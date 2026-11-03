package volume

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"slices"
	"strings"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
)

// attachPayload mirrors EnqueueVolumeAttachAsync and EnqueueVolumeDetachAsync (detach has no externalVmId).
type attachPayload struct {
	VolumeId string `json:"volumeId"`
	// RequestId names the request this command ends (titan-ops architecture/resource-lifecycle.md#callback-routes). Empty
	// on a command of the old control plane that the migration gave no id: it keeps the old reporting until rollout
	// phase 6 (titan-ops architecture/resource-lifecycle.md#rollout).
	RequestId        string `json:"requestId"`
	ExternalVolumeId string `json:"externalVolumeId"`
	ExternalVmId     string `json:"externalVmId"`
	ProjectName      string `json:"projectName"`
}

// Attach runs volume.attach: a disk device named after the volume, hot-plugged by PATCH (lab-storage 04: works on a
// running VM, and the same PATCH again is a no-op). It reports what Incus then shows: the instances using the volume.
// With a request id that report is the attach's Done; without one, an observation on the old route.
type Attach struct {
	cp       *controlplane.Client
	clusters cluster.Clusters
}

func NewAttach(cp *controlplane.Client, clusters cluster.Clusters) *Attach {
	return &Attach{cp: cp, clusters: clusters}
}

func (h *Attach) Run(ctx context.Context, leased command.Leased, logger *slog.Logger) error {
	var payload attachPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	isComplete := payload.VolumeId != "" && payload.ExternalVolumeId != "" && payload.ExternalVmId != "" && payload.ProjectName != ""
	if !isComplete {
		return command.Permanent(fmt.Errorf("volume.attach payload lacks volumeId, externalVolumeId, externalVmId or projectName"))
	}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return err
	}
	device := map[string]string{
		"type":   "disk",
		"pool":   provider.StoragePoolOrDefault(cl.StoragePool),
		"source": payload.ExternalVolumeId,
	}
	patch := api.InstancePut{Devices: map[string]map[string]string{payload.ExternalVolumeId: device}}
	err = cl.Provider.PatchInstance(ctx, payload.ProjectName, payload.ExternalVmId, patch)
	if provider.IsNotFound(err) {
		// The VM is gone: no later lease can attach to it.
		return command.Permanent(err)
	}
	if err != nil {
		return err
	}
	return reportWhatIncusShows(ctx, h.cp, cl.Provider, payload, provider.StoragePoolOrDefault(cl.StoragePool), logger)
}

// GiveUp ends the request with what Incus shows: an attach that happened while its report failed is Done, one that did
// not is Failed (see giveUp).
func (h *Attach) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	return giveUp(ctx, h.cp, h.clusters, leased, cause, reasonAttachFailed, func(names []string, payload attachPayload) bool {
		return slices.Contains(names, payload.ExternalVmId)
	})
}

// reportWhatIncusShows reads the volume after an attach or detach and reports it. On the old route a failed read is
// retried, since that report is the only one the request gets; with a request id the Done goes without it.
func reportWhatIncusShows(ctx context.Context, cp *controlplane.Client, p provider.Provider, payload attachPayload, pool string, logger *slog.Logger) error {
	if payload.RequestId != "" {
		observed := ReadObservation(ctx, p, payload.ProjectName, pool, payload.ExternalVolumeId, logger)
		return EndRequest(ctx, cp, payload.VolumeId, payload.RequestId, Done("", observed))
	}
	volume, err := p.GetVolume(ctx, payload.ProjectName, pool, payload.ExternalVolumeId)
	if err != nil {
		return err
	}
	return SendStatus(ctx, cp, payload.VolumeId, Observed(true, InstanceNames(volume.UsedBy)))
}

// giveUp is the attach's and the detach's. With a request id: Incus showing what was asked (reached) makes the report
// the request's Done — the work happened, only its report was lost. That is an exception to every give-up sending
// Failed (titan-ops architecture/resource-lifecycle.md#callback-routes), kept from the volume's rule that a give-up
// showing the attach or detach done is that request done (titan-ops features/volume/design.md#volume-reports);
// otherwise Failed, with the instances using the volume when it can be read, alone when it cannot or the volume is gone.
// Without one, the old route: the request failed, plus what Incus shows when it can be read — the volume gone, or the
// instances using it.
func giveUp(ctx context.Context, cp *controlplane.Client, clusters cluster.Clusters, leased command.Leased, cause error,
	reason string, reached func(names []string, payload attachPayload) bool) error {
	var payload attachPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	observation, read := readAttachment(ctx, clusters, leased.ClusterId, payload)
	if payload.RequestId != "" {
		isThere := read && *observation.Exists
		if !isThere {
			return EndRequest(ctx, cp, payload.VolumeId, payload.RequestId, Failed("", nil, reason, cause))
		}
		observed := &Observation{AttachedTo: observation.AttachedTo}
		if reached(observation.AttachedTo, payload) {
			return EndRequest(ctx, cp, payload.VolumeId, payload.RequestId, Done("", observed))
		}
		return EndRequest(ctx, cp, payload.VolumeId, payload.RequestId, Failed("", observed, reason, cause))
	}
	report := StatusReport{Report: ReportRequestFailed, Reason: reason, Detail: cause.Error()}
	if read {
		report.Exists = observation.Exists
		report.AttachedTo = observation.AttachedTo
	}
	return SendStatus(ctx, cp, payload.VolumeId, report)
}

// readAttachment reads whether the volume exists and the instances using it; false when it cannot be read.
func readAttachment(ctx context.Context, clusters cluster.Clusters, clusterId string, payload attachPayload) (StatusReport, bool) {
	if payload.ProjectName == "" || payload.ExternalVolumeId == "" {
		return StatusReport{}, false
	}
	cl, err := clusters.Cluster(ctx, clusterId)
	if err != nil {
		return StatusReport{}, false
	}
	pool := provider.StoragePoolOrDefault(cl.StoragePool)
	volume, err := cl.Provider.GetVolume(ctx, payload.ProjectName, pool, payload.ExternalVolumeId)
	if provider.IsNotFound(err) {
		return Observed(false, nil), true
	}
	if err != nil {
		return StatusReport{}, false
	}
	return Observed(true, InstanceNames(volume.UsedBy)), true
}

// InstanceNames is the instance names of a used_by, in order.
func InstanceNames(usedBy []string) []string {
	names := []string{}
	for _, instance := range InstancesUsing(usedBy) {
		names = append(names, instance.Name)
	}
	return names
}

// InstanceRef is one instance named in a volume's used_by.
type InstanceRef struct {
	Project string
	Name    string
}

// InstancesUsing reads used_by ("/1.0/instances/<name>?project=<project>", lab-storage 04). Entries for anything
// else (a snapshot, a profile) are not instances a detach can edit.
func InstancesUsing(usedBy []string) []InstanceRef {
	instances := []InstanceRef{}
	for _, entry := range usedBy {
		parsed, err := url.Parse(entry)
		if err != nil {
			continue
		}
		name, isInstance := strings.CutPrefix(parsed.Path, "/1.0/instances/")
		if !isInstance || name == "" || strings.Contains(name, "/") {
			continue
		}
		project := parsed.Query().Get("project")
		if project == "" {
			project = "default"
		}
		instances = append(instances, InstanceRef{Project: project, Name: name})
	}
	return instances
}
