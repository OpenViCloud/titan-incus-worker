package volsnapshot

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/resource/volume"
)

// restorePayload mirrors EnqueueVolumeSnapshotRestoreAsync.
type restorePayload struct {
	SnapshotId         string `json:"snapshotId"`
	NewVolumeId        string `json:"newVolumeId"`
	ExternalSnapshotId string `json:"externalSnapshotId"`
	Name               string `json:"name"`
	ProjectName        string `json:"projectName"`
}

// Restore runs volume-snapshot.restore: a copy of the snapshot as a new volume (lab-storage 03). It reports on the new
// volume, like a volume create: Created with its provider id, once the copy has finished.
type Restore struct {
	cp       *controlplane.Client
	clusters cluster.Clusters
}

func NewRestore(cp *controlplane.Client, clusters cluster.Clusters) *Restore {
	return &Restore{cp: cp, clusters: clusters}
}

func (h *Restore) Run(ctx context.Context, leased command.Leased, logger *slog.Logger) error {
	var payload restorePayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	isComplete := payload.NewVolumeId != "" && payload.ExternalSnapshotId != "" && payload.ProjectName != ""
	if !isComplete {
		return command.Permanent(fmt.Errorf("volume-snapshot.restore payload lacks newVolumeId, externalSnapshotId or projectName"))
	}
	name, err := provider.VolumeName(payload.NewVolumeId)
	if err != nil {
		return command.Permanent(err)
	}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return err
	}
	pool := provider.StoragePoolOrDefault(cl.StoragePool)
	// The copy keeps only the config in this request (lab-storage 03): the display name goes here or is lost.
	request := api.StorageVolumesPost{
		Name:             name,
		Type:             "custom",
		ContentType:      "block",
		Source:           api.StorageVolumeSource{Type: "copy", Pool: pool, Name: payload.ExternalSnapshotId},
		StorageVolumePut: api.StorageVolumePut{Config: map[string]string{volume.DisplayNameKey: payload.Name}},
	}
	err = cl.Provider.CreateVolume(ctx, payload.ProjectName, pool, request)
	isOurs := err != nil && provider.IsConflict(err)
	if err != nil && !isOurs {
		return err
	}
	if isOurs {
		logger.Info("already exists; treating it as ours", "object", name)
	}
	return volume.SendStatus(ctx, h.cp, payload.NewVolumeId, volume.StatusReport{Report: volume.ReportCreated, ExternalId: name})
}

func (h *Restore) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	var payload restorePayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	report := volume.StatusReport{Report: volume.ReportRequestFailed, Reason: volume.ReasonCreateFailed, Detail: cause.Error()}
	report.ExternalId = volume.ExistingVolume(ctx, h.clusters, leased.ClusterId, payload.ProjectName, payload.NewVolumeId)
	return volume.SendStatus(ctx, h.cp, payload.NewVolumeId, report)
}
