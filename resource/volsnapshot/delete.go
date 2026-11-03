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

// deletePayload mirrors EnqueueVolumeSnapshotDeleteAsync.
type deletePayload struct {
	SnapshotId string `json:"snapshotId"`
	// RequestId names the request this command ends (titan-ops architecture/resource-lifecycle.md#callback-routes). Empty
	// on a command of the old control plane that the migration gave no id: it keeps the old reporting until rollout
	// phase 6 (titan-ops architecture/resource-lifecycle.md#rollout).
	RequestId          string `json:"requestId"`
	VolumeId           string `json:"volumeId"`
	ExternalSnapshotId string `json:"externalSnapshotId"`
	ProjectName        string `json:"projectName"`
}

// Delete runs volume-snapshot.delete and ends its request Done (Deleted on the old route); giving up ends it Failed,
// which gives the snapshot back (titan-ops architecture/resource-lifecycle.md#kinds).
type Delete struct {
	cp       *controlplane.Client
	clusters cluster.Clusters
}

func NewDelete(cp *controlplane.Client, clusters cluster.Clusters) *Delete {
	return &Delete{cp: cp, clusters: clusters}
}

func (h *Delete) Run(ctx context.Context, leased command.Leased, logger *slog.Logger) error {
	var payload deletePayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	isComplete := payload.SnapshotId != "" && payload.VolumeId != "" && payload.ProjectName != ""
	if !isComplete {
		return command.Permanent(fmt.Errorf("volume-snapshot.delete payload lacks snapshotId, volumeId or projectName"))
	}
	parent, name, err := provider.SplitSnapshotId(payload.ExternalSnapshotId)
	if err != nil {
		return command.Permanent(err)
	}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return err
	}
	pool := provider.StoragePoolOrDefault(cl.StoragePool)
	err = cl.Provider.DeleteVolumeSnapshot(ctx, payload.ProjectName, pool, parent, name)
	// 404: an earlier run deleted it, or its volume is gone and took it along (lab-storage 03, 07).
	isAlreadyGone := provider.IsNotFound(err)
	if err != nil && !isAlreadyGone {
		return err
	}
	if isAlreadyGone {
		logger.Info("already gone; nothing to delete", "object", provider.SnapshotId(parent, name))
	}
	if payload.RequestId != "" {
		return volume.EndSnapshotRequest(ctx, h.cp, payload.VolumeId, payload.SnapshotId, payload.RequestId, volume.Done("", nil))
	}
	report := volume.StatusReport{Report: volume.ReportDeleted}
	return volume.SendSnapshotStatus(ctx, h.cp, payload.VolumeId, payload.SnapshotId, report)
}

func (h *Delete) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	var payload deletePayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	if payload.RequestId != "" {
		end := volume.Failed("", nil, reasonDeleteFailed, cause)
		return volume.EndSnapshotRequest(ctx, h.cp, payload.VolumeId, payload.SnapshotId, payload.RequestId, end)
	}
	report := volume.StatusReport{Report: volume.ReportRequestFailed, Reason: reasonDeleteFailed, Detail: cause.Error()}
	return volume.SendSnapshotStatus(ctx, h.cp, payload.VolumeId, payload.SnapshotId, report)
}
