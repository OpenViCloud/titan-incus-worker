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
// without the volume's disk device (lab-storage 04b). It reports what Incus then shows.
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
	isComplete := payload.VolumeId != "" && payload.ExternalVolumeId != "" && payload.ProjectName != ""
	if !isComplete {
		return command.Permanent(fmt.Errorf("volume.detach payload lacks volumeId, externalVolumeId or projectName"))
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
	return reportWhatIncusShows(ctx, h.cp, cl.Provider, payload, pool)
}

// GiveUp reports the request failed, carrying what Incus shows: a detach that happened while its report failed is then
// fulfilled by it, and only one that did not is withdrawn.
func (h *Detach) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	return giveUp(ctx, h.cp, h.clusters, leased, cause, reasonDetachFailed)
}

// removeDisk drops every disk device of the instance whose source is the volume. An instance gone has no device left.
func removeDisk(ctx context.Context, p provider.Provider, project string, name string, volume string) error {
	instance, err := p.GetInstance(ctx, project, name)
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
	return p.UpdateInstance(ctx, project, name, put)
}
