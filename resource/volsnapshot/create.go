package volsnapshot

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/resource/volume"
)

// createPayload mirrors ProviderCommandQueue.EnqueueVolumeSnapshotCreateAsync.
type createPayload struct {
	SnapshotId string `json:"snapshotId"`
	// RequestId names the request this command ends (titan-ops architecture/resource-lifecycle.md#callback-routes).
	RequestId        string `json:"requestId"`
	VolumeId         string `json:"volumeId"`
	ExternalVolumeId string `json:"externalVolumeId"`
	ProjectName      string `json:"projectName"`
}

// Create runs volume-snapshot.create. Incus has made it once the operation returns, so the create is done: Done with
// the provider id "<volume>/<snapshot>".
type Create struct {
	cp       *controlplane.Client
	clusters cluster.Clusters
}

func NewCreate(cp *controlplane.Client, clusters cluster.Clusters) *Create {
	return &Create{cp: cp, clusters: clusters}
}

func (h *Create) Run(ctx context.Context, leased command.Leased, logger *slog.Logger) error {
	var payload createPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	isComplete := payload.SnapshotId != "" && payload.RequestId != "" && payload.VolumeId != "" &&
		payload.ExternalVolumeId != "" && payload.ProjectName != ""
	if !isComplete {
		return command.Permanent(fmt.Errorf("volume-snapshot.create payload lacks snapshotId, requestId, volumeId, externalVolumeId or projectName"))
	}
	name, err := provider.SnapshotName(payload.SnapshotId)
	if err != nil {
		return command.Permanent(err)
	}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return err
	}
	pool := provider.StoragePoolOrDefault(cl.StoragePool)
	err = cl.Provider.CreateVolumeSnapshot(ctx, payload.ProjectName, pool, payload.ExternalVolumeId, name)
	// 409: an earlier run of this command made it (lab-storage 03); the name is ours.
	isOurs := err != nil && provider.IsConflict(err)
	if err != nil && !isOurs {
		return err
	}
	externalId := provider.SnapshotId(payload.ExternalVolumeId, name)
	if isOurs {
		logger.Info("already exists; treating it as ours", "object", externalId)
	}
	return volume.EndSnapshotRequest(ctx, h.cp, payload.VolumeId, payload.SnapshotId, payload.RequestId, volume.Done(externalId, nil))
}

// GiveUp reports the request failed, with the provider id when the snapshot exists: then the create did happen.
func (h *Create) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	var payload createPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	if payload.RequestId == "" {
		logger.Warn("no request id; nothing to end")
		return nil
	}
	existing := h.existingSnapshot(ctx, leased.ClusterId, payload)
	end := volume.Failed(existing, nil, reasonCreateFailed, cause)
	return volume.EndSnapshotRequest(ctx, h.cp, payload.VolumeId, payload.SnapshotId, payload.RequestId, end)
}

func (h *Create) existingSnapshot(ctx context.Context, clusterId string, payload createPayload) string {
	name, err := provider.SnapshotName(payload.SnapshotId)
	if err != nil || payload.ProjectName == "" || payload.ExternalVolumeId == "" {
		return ""
	}
	cl, err := h.clusters.Cluster(ctx, clusterId)
	if err != nil {
		return ""
	}
	pool := provider.StoragePoolOrDefault(cl.StoragePool)
	snapshots, err := cl.Provider.ListVolumeSnapshots(ctx, payload.ProjectName, pool, payload.ExternalVolumeId)
	if err != nil {
		return ""
	}
	if !volume.SnapshotNames(payload.ExternalVolumeId, snapshots)[name] {
		return ""
	}
	return provider.SnapshotId(payload.ExternalVolumeId, name)
}
