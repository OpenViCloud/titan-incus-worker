package vmkeyaddition

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
)

// Fixed paths the VM's vendor-data boothook writes at create (VmVendorData.cs, control plane): the key file sshd
// reads for the `ubuntu` user, and the sshd config that points at it. The boothook now writes the config for every
// VM, with or without a key, so a VM created earlier without one is the only case missing it.
const (
	keyFilePath    = "/etc/ssh/openvicloud/ubuntu"
	sshdConfigPath = "/etc/ssh/sshd_config.d/10-openvicloud.conf"
	// The exact line VmVendorData.Build writes; a worker that writes the config must write the same line a VM created
	// with a key already got, so sshd behaves the same whichever path put it there.
	sshdConfigLine = "AuthorizedKeysFile .ssh/authorized_keys .ssh/authorized_keys2 /etc/ssh/openvicloud/%u\n"
)

// createPayload mirrors ProviderCommandQueue.EnqueueVmKeyAdditionCreateAsync (control plane branch
// feat/vm-key-additions). KeyLines are rebuilt from parsed keys there, never customer input verbatim.
type createPayload struct {
	KeyAdditionId string `json:"keyAdditionId"`
	// RequestId names the request this command ends (titan-ops architecture/resource-lifecycle.md#callback-routes).
	RequestId    string   `json:"requestId"`
	VmId         string   `json:"vmId"`
	ExternalVmId string   `json:"externalVmId"`
	ProjectName  string   `json:"projectName"`
	KeyLines     []string `json:"keyLines"`
}

// Create runs vm-key-addition.create and ends its request Done, carrying whether the VM needs a restart for sshd to
// read the config this run had to write.
type Create struct {
	cp       *controlplane.Client
	clusters cluster.Clusters
}

func NewCreate(cp *controlplane.Client, clusters cluster.Clusters) *Create {
	return &Create{cp: cp, clusters: clusters}
}

func (h *Create) Run(ctx context.Context, leased command.Leased, logger *slog.Logger) error {
	payload, err := decodeComplete(leased)
	if err != nil {
		return err
	}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return err
	}
	restartNeeded, err := apply(ctx, cl.Provider, payload.ProjectName, payload.ExternalVmId, payload.KeyLines)
	if err != nil {
		return err
	}
	return endRequest(ctx, h.cp, payload.VmId, payload.KeyAdditionId, payload.RequestId, done(restartNeeded))
}

// GiveUp always ends the request Failed: the run is safe to repeat whole from the start (appending a key already
// there changes nothing, titan-ops architecture/intent-and-observation.md#provider-create-idempotency), so unlike a
// create that makes a named object, there is nothing on Incus worth reading to tell a partial append from none.
func (h *Create) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	var payload createPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	if payload.RequestId == "" {
		logger.Warn("no request id; nothing to end")
		return nil
	}
	return endRequest(ctx, h.cp, payload.VmId, payload.KeyAdditionId, payload.RequestId, failed(cause))
}

func decodeComplete(leased command.Leased) (createPayload, error) {
	var payload createPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return createPayload{}, err
	}
	isComplete := payload.KeyAdditionId != "" && payload.RequestId != "" && payload.VmId != "" &&
		payload.ExternalVmId != "" && payload.ProjectName != "" && len(payload.KeyLines) > 0
	if !isComplete {
		return createPayload{}, command.Permanent(fmt.Errorf(
			"vm-key-addition.create payload lacks keyAdditionId, requestId, vmId, externalVmId, projectName or keyLines"))
	}
	return payload, nil
}

// apply appends every key line the VM's key file lacks, writes the sshd config only when the VM predates it, and
// answers whether the config was just written: the tenant must restart the VM for sshd to read it.
func apply(ctx context.Context, p provider.Provider, project string, instance string, keyLines []string) (bool, error) {
	existing, err := pullLines(ctx, p, project, instance, keyFilePath)
	if err != nil {
		return false, err
	}
	merged := appendMissing(existing, keyLines)
	if err := pushFile(ctx, p, project, instance, keyFilePath, linesToContent(merged)); err != nil {
		return false, err
	}

	_, err = classify(p.GetInstanceFile(ctx, project, instance, sshdConfigPath))
	if err == nil {
		return false, nil
	}
	if !provider.IsInstanceFileNotFound(err) {
		return false, err
	}
	if err := pushFile(ctx, p, project, instance, sshdConfigPath, []byte(sshdConfigLine)); err != nil {
		return false, err
	}
	return true, nil
}

// pullLines reads path as newline-terminated lines, in order; an absent path is no lines, not an error.
func pullLines(ctx context.Context, p provider.Provider, project string, instance string, path string) ([]string, error) {
	content, err := classify(p.GetInstanceFile(ctx, project, instance, path))
	if provider.IsInstanceFileNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	text := string(content)
	if text == "" {
		return nil, nil
	}
	return strings.Split(strings.TrimSuffix(text, "\n"), "\n"), nil
}

// appendMissing keeps every existing line, in order, then adds each new line not already there.
func appendMissing(existing []string, add []string) []string {
	has := map[string]bool{}
	for _, line := range existing {
		has[line] = true
	}
	merged := append([]string{}, existing...)
	for _, line := range add {
		if !has[line] {
			merged = append(merged, line)
			has[line] = true
		}
	}
	return merged
}

func linesToContent(lines []string) []byte {
	if len(lines) == 0 {
		return []byte{}
	}
	return []byte(strings.Join(lines, "\n") + "\n")
}

// pushFile ensures path's parent directory exists, then writes content as root, mode 0644, overwriting whatever was
// there (as measured on the dev lab: `incus file push --uid 0 --gid 0 --mode 0644 --create-dirs`).
func pushFile(ctx context.Context, p provider.Provider, project string, instance string, path string, content []byte) error {
	if err := ensureDir(ctx, p, project, instance, parentDir(path)); err != nil {
		return err
	}
	return classifyErr(p.CreateInstanceFile(ctx, project, instance, path, content, 0, 0, 0o644))
}

// ensureDir makes dir only when Incus does not already have it.
func ensureDir(ctx context.Context, p provider.Provider, project string, instance string, dir string) error {
	_, err := classify(p.GetInstanceFile(ctx, project, instance, dir))
	if err == nil {
		return nil
	}
	if !provider.IsInstanceFileNotFound(err) {
		return err
	}
	return classifyErr(p.CreateInstanceDirectory(ctx, project, instance, dir))
}

func parentDir(path string) string {
	idx := strings.LastIndex(path, "/")
	if idx <= 0 {
		return "/"
	}
	return path[:idx]
}

// classify marks "Instance is not running" permanent (the tenant restarting it is not a retry's job); every other
// error, including the agent on a Running VM not yet up, is not measured on the dev lab and stays retryable by the
// loop's default (command.IsRetryable).
func classify(content []byte, err error) ([]byte, error) {
	return content, classifyErr(err)
}

func classifyErr(err error) error {
	if err == nil {
		return nil
	}
	if provider.IsInstanceFileNotFound(err) {
		return err
	}
	if provider.IsInstanceNotRunning(err) {
		return command.Permanent(err)
	}
	return err
}
