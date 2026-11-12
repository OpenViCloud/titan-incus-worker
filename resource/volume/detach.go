package volume

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
)

// Detach runs volume.detach. The command names no VM: the volume's used_by does. Each instance using it gets a PUT
// without the volume's disk device (lab-storage 04b). It reports what Incus then shows, as Attach does.
type Detach struct {
	cp       *controlplane.Client
	clusters cluster.Clusters
}

func NewDetach(cp *controlplane.Client, clusters cluster.Clusters) *Detach {
	return &Detach{cp: cp, clusters: clusters}
}

func (h *Detach) Run(ctx context.Context, leased command.Leased, logger *slog.Logger) error {
	var payload attachPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	isComplete := payload.VolumeId != "" && payload.RequestId != "" && payload.ExternalVolumeId != "" && payload.ProjectName != ""
	if !isComplete {
		return command.Permanent(fmt.Errorf("volume.detach payload lacks volumeId, requestId, externalVolumeId or projectName"))
	}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return err
	}
	pool := provider.StoragePoolOrDefault(cl.StoragePool)
	volume, err := cl.Provider.GetVolume(ctx, payload.ProjectName, pool, payload.ExternalVolumeId)
	if provider.IsNotFound(err) {
		return command.Permanent(err)
	}
	if err != nil {
		return err
	}
	for _, instance := range InstancesUsing(volume.UsedBy) {
		if err := removeDisk(ctx, cl.Provider, instance.Project, instance.Name, payload.ExternalVolumeId); err != nil {
			return err
		}
	}
	observed := ReadObservation(ctx, cl.Provider, payload.ProjectName, pool, payload.ExternalVolumeId, logger)
	return EndRequest(ctx, h.cp, payload.VolumeId, payload.RequestId, Done("", observed))
}

// GiveUp ends the request with what Incus shows: a detach that happened while its report failed is Done, one that did
// not is Failed (see giveUp).
func (h *Detach) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	return giveUp(ctx, h.cp, h.clusters, leased, cause, reasonDetachFailed, logger, func(names []string, _ attachPayload) bool {
		return len(names) == 0
	})
}

// removeDisk drops every disk device of the instance whose source is the volume. An instance gone has no device left.
// The PUT replaces the whole device list, so it carries the read's ETag: a run that overlaps this one (a lease that ran
// out while its run went on, titan-ops features/provider/design.md#tenant-lane) may have written the instance since, and
// Incus then refuses the stale list (412: EtagCheck in cmd/incusd/instance_put.go of v7.0.1; the ETag covers devices and
// config, volatile keys too, so a state change in between also refuses it). That error is retryable: the next run reads
// the instance again.
func removeDisk(ctx context.Context, p provider.Provider, project string, name string, volume string) error {
	instance, etag, err := p.GetInstanceWithEtag(ctx, project, name)
	if provider.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	put := instance.Writable()
	removed := false
	for device, settings := range put.Devices {
		isThisVolume := settings["type"] == "disk" && settings["source"] == volume
		if isThisVolume {
			delete(put.Devices, device)
			removed = true
		}
	}
	if !removed {
		return nil
	}
	return p.UpdateInstance(ctx, project, name, put, etag)
}
