package backup

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
)

// volumePayload mirrors ProviderCommandQueue.EnqueueVolumeBackupAsync.
type volumePayload struct {
	VolumeId         string `json:"volumeId"`
	ExternalVolumeId string `json:"externalVolumeId"`
	ProjectName      string `json:"projectName"`
	TenantId         string `json:"tenantId"`
}

// Volume runs volume.backup the way Vm does: a copy of a snapshot is what Incus uploads, then both go.
type Volume struct {
	clusters cluster.Clusters
	target   Target
	objects  Objects
	arrival  Arrival
	now      func() time.Time
}

func NewVolume(clusters cluster.Clusters, target Target, objects Objects) *Volume {
	return &Volume{clusters: clusters, target: target, objects: objects, arrival: DefaultArrival, now: time.Now}
}

func (h *Volume) Run(ctx context.Context, leased command.Leased, logger *slog.Logger) error {
	payload, err := decodeVolume(leased)
	if err != nil {
		return err
	}
	if err := h.target.usable(); err != nil {
		return err
	}
	copyName, err := provider.BackupVolumeCopyName(payload.VolumeId)
	if err != nil {
		return command.Permanent(err)
	}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return err
	}
	p := cl.Provider
	pool := provider.StoragePoolOrDefault(cl.StoragePool)
	project, volume := payload.ProjectName, payload.ExternalVolumeId

	_, err = p.GetVolume(ctx, project, pool, volume)
	if provider.IsNotFound(err) {
		return command.Permanent(fmt.Errorf("volume %s is not in project %s: nothing to back up", volume, project))
	}
	if err != nil {
		return err
	}
	if err := removeVolumeLeftovers(ctx, p, project, pool, volume, copyName); err != nil {
		return err
	}

	if err := p.CreateVolumeSnapshot(ctx, project, pool, volume, snapshotName); err != nil {
		return err
	}
	key := ObjectKey(leased.ClusterId, payload.TenantId, "volumes", payload.VolumeId, h.now())
	runErr := copyAndUploadVolume(ctx, p, project, pool, volume, copyName, h.target.upload(key))
	cleanUp(logger,
		func(ctx context.Context) error { return deleteVolumeIfThere(ctx, p, project, pool, copyName) },
		func(ctx context.Context) error {
			return p.DeleteVolumeSnapshot(ctx, project, pool, volume, snapshotName)
		},
	)
	if runErr != nil {
		return runErr
	}
	if err := confirmArrived(ctx, h.objects, h.arrival, key); err != nil {
		return err
	}
	logger.Info("backup uploaded", "object", key)
	return nil
}

func copyAndUploadVolume(ctx context.Context, p provider.Provider, project string, pool string, volume string,
	copyName string, upload api.BackupTarget) error {
	copyRequest := api.StorageVolumesPost{
		Name:        copyName,
		Type:        "custom",
		ContentType: "block",
		Source:      api.StorageVolumeSource{Type: "copy", Pool: pool, Name: provider.SnapshotId(volume, snapshotName)},
	}
	if err := p.CreateVolume(ctx, project, pool, copyRequest); err != nil {
		return err
	}
	return p.BackupVolume(ctx, project, pool, copyName, upload)
}

// GiveUp removes what a run left; the command has no callback.
func (h *Volume) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	payload, err := decodeVolume(leased)
	if err != nil {
		return nil
	}
	copyName, err := provider.BackupVolumeCopyName(payload.VolumeId)
	if err != nil {
		return nil
	}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return nil
	}
	pool := provider.StoragePoolOrDefault(cl.StoragePool)
	err = removeVolumeLeftovers(ctx, cl.Provider, payload.ProjectName, pool, payload.ExternalVolumeId, copyName)
	if err != nil {
		logger.Warn("removing the backup's leftovers failed; the next run removes them", "error", err)
	}
	return nil
}

func removeVolumeLeftovers(ctx context.Context, p provider.Provider, project string, pool string, volume string,
	copyName string) error {
	if err := deleteVolumeIfThere(ctx, p, project, pool, copyName); err != nil {
		return err
	}
	snapshots, err := p.ListVolumeSnapshots(ctx, project, pool, volume)
	if err != nil {
		return err
	}
	leftover := provider.SnapshotId(volume, snapshotName)
	for _, snapshot := range snapshots {
		if snapshot.Name == leftover {
			return p.DeleteVolumeSnapshot(ctx, project, pool, volume, snapshotName)
		}
	}
	return nil
}

func deleteVolumeIfThere(ctx context.Context, p provider.Provider, project string, pool string, name string) error {
	_, err := p.GetVolume(ctx, project, pool, name)
	if provider.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return p.DeleteVolume(ctx, project, pool, name)
}

func decodeVolume(leased command.Leased) (volumePayload, error) {
	var payload volumePayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return volumePayload{}, err
	}
	isComplete := payload.VolumeId != "" && payload.ExternalVolumeId != "" && payload.ProjectName != "" && payload.TenantId != ""
	if !isComplete {
		return volumePayload{}, command.Permanent(fmt.Errorf("volume.backup payload lacks volumeId, externalVolumeId, projectName or tenantId"))
	}
	return payload, nil
}
