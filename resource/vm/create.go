package vm

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
)

const (
	displayNameKey = "user.titan-name"
	userDataKey    = "cloud-init.user-data"
	vendorDataKey  = "cloud-init.vendor-data"
	defaultPool    = "default"
)

// createPayload mirrors ProviderCommandQueue.EnqueueVmCreateAsync. templateId is already the cluster's image alias.
type createPayload struct {
	VmId              string  `json:"vmId"`
	Name              string  `json:"name"`
	CpuUnits          float64 `json:"cpuUnits"`
	VcpuCount         int     `json:"vcpuCount"`
	MemoryMb          int     `json:"memoryMb"`
	TemplateId        string  `json:"templateId"`
	ExternalNetworkId string  `json:"externalNetworkId"`
	ProjectName       string  `json:"projectName"`
	DiskSizeGb        int     `json:"diskSizeGb"`
	UserData          string  `json:"userData"`
	VendorData        string  `json:"vendorData"`
}

// Create runs vm.create. It reports only the provider id (Created) or the give-up; the cluster's reconciler reports
// Running (https://github.com/OpenViCloud/titan-ops/blob/main/docs/architecture/desired-and-current.md#desired-and-current).
type Create struct {
	cp       *controlplane.Client
	clusters cluster.Clusters
}

func NewCreate(cp *controlplane.Client, clusters cluster.Clusters) *Create {
	return &Create{cp: cp, clusters: clusters}
}

func (h *Create) Run(ctx context.Context, leased command.Leased, logger *slog.Logger) error {
	payload, err := decodeCreate(leased)
	if err != nil {
		return err
	}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return err
	}
	if err := provider.EnsureProject(ctx, cl.Provider, payload.ProjectName); err != nil {
		return err
	}

	request, err := InstanceRequest(payload, cl.StoragePool)
	if err != nil {
		return command.Permanent(err)
	}
	err = cl.Provider.CreateInstance(ctx, payload.ProjectName, request)
	// A lease that ran out can overlap a second run of the same create; the name is ours, so the instance is too.
	isOurs := err != nil && provider.IsInstanceAlreadyExists(err)
	if err != nil && !isOurs {
		return err
	}
	if isOurs {
		logger.Info("already exists; treating it as ours", "object", request.Name)
		// The earlier run may have died between creating and starting it: a stopped instance here would leave the
		// VM waiting for a Running that nothing asks for.
		if err := startIfStopped(ctx, cl.Provider, payload.ProjectName, request.Name, logger); err != nil {
			return err
		}
	}
	return sendStatus(ctx, h.cp, payload.VmId, statusReport{Report: reportCreated, ExternalId: request.Name})
}

// GiveUp reports the request failure. It carries the provider id when the instance exists, from this run or an earlier
// one, so the VM's delete still reaches Incus.
func (h *Create) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	// Not decodeCreate: an incomplete payload is a reason to give up, not a reason to stay silent about it.
	var payload createPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	report := statusReport{
		Report:     reportRequestFailed,
		ExternalId: h.existingInstance(ctx, leased.ClusterId, payload),
		Reason:     reasonProvisionFailed,
		Detail:     cause.Error(),
	}
	return sendStatus(ctx, h.cp, payload.VmId, report)
}

// existingInstance returns the instance name when it is on Incus, "" when it is not or cannot be read.
func (h *Create) existingInstance(ctx context.Context, clusterId string, payload createPayload) string {
	if payload.ProjectName == "" {
		return ""
	}
	cl, err := h.clusters.Cluster(ctx, clusterId)
	if err != nil {
		return ""
	}
	name, err := provider.InstanceName(payload.VmId)
	if err != nil {
		return ""
	}
	_, err = cl.Provider.GetInstance(ctx, payload.ProjectName, name)
	if err != nil {
		return ""
	}
	return name
}

func decodeCreate(leased command.Leased) (createPayload, error) {
	var payload createPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return createPayload{}, err
	}
	// A VM without its network would run with no NIC while the control plane reports it Running.
	isComplete := payload.VmId != "" && payload.ProjectName != "" && payload.TemplateId != "" &&
		payload.ExternalNetworkId != "" && payload.VcpuCount > 0 && payload.MemoryMb > 0
	if !isComplete {
		return createPayload{}, command.Permanent(fmt.Errorf("vm.create payload lacks vmId, projectName, templateId, externalNetworkId, vcpuCount or memoryMb"))
	}
	return payload, nil
}

// InstanceRequest builds the instance. Sizing comes from the VM offering; the image carries none.
func InstanceRequest(payload createPayload, storagePool string) (api.InstancesPost, error) {
	name, err := provider.InstanceName(payload.VmId)
	if err != nil {
		return api.InstancesPost{}, err
	}
	config := map[string]string{
		"limits.cpu":    strconv.Itoa(payload.VcpuCount),
		"limits.memory": strconv.Itoa(payload.MemoryMb) + "MiB",
		displayNameKey:  payload.Name,
	}
	// A cloud-config with nothing but comments is an empty YAML stream, which fails the create (lab 04).
	if HasUserDataContent(payload.UserData) {
		config[userDataKey] = payload.UserData
	}
	// Titan's own script, built by the control plane; empty when no key was picked
	// (https://github.com/OpenViCloud/titan-ops/blob/main/docs/features/vm/design.md#ssh-key).
	if payload.VendorData != "" {
		config[vendorDataKey] = payload.VendorData
	}

	pool := storagePool
	if pool == "" {
		pool = defaultPool
	}
	root := map[string]string{"type": "disk", "pool": pool, "path": "/"}
	if payload.DiskSizeGb > 0 {
		root["size"] = strconv.Itoa(payload.DiskSizeGb) + "GiB"
	}
	devices := map[string]map[string]string{"root": root}
	if payload.ExternalNetworkId != "" {
		devices["eth0"] = map[string]string{"type": "nic", "network": payload.ExternalNetworkId}
	}

	return api.InstancesPost{
		Name:        name,
		Type:        api.InstanceTypeVM,
		Source:      api.InstanceSource{Type: "image", Alias: payload.TemplateId},
		Start:       true,
		InstancePut: api.InstancePut{Config: config, Devices: devices},
	}, nil
}

// HasUserDataContent: true when some line is neither blank nor a comment ("#cloud-config" is a comment).
func HasUserDataContent(userData string) bool {
	for _, line := range strings.Split(userData, "\n") {
		trimmed := strings.TrimSpace(line)
		isContent := trimmed != "" && !strings.HasPrefix(trimmed, "#")
		if isContent {
			return true
		}
	}
	return false
}

func startIfStopped(ctx context.Context, p provider.Provider, project string, name string, logger *slog.Logger) error {
	instance, err := p.GetInstance(ctx, project, name)
	if err != nil {
		return err
	}
	isStopped := instance.StatusCode == api.Stopped
	if !isStopped {
		return nil
	}
	logger.Info("found our instance stopped; starting it", "object", name)
	err = p.UpdateInstanceState(ctx, project, name, api.InstanceStatePut{Action: "start"})
	isAlreadyRunning := provider.IsOperationFailureContaining(err, alreadyRunning)
	if err != nil && !isAlreadyRunning {
		return err
	}
	return nil
}
