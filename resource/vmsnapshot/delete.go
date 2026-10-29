package vmsnapshot

import (
	"context"
	"log/slog"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
)

// Delete runs vm-snapshot.delete and reports Deleted; giving up reports the request failed, which gives the snapshot back
// (titan-ops architecture/desired-and-current.md#abandoned-request).
type Delete struct {
	cp       *controlplane.Client
	clusters cluster.Clusters
}

func NewDelete(cp *controlplane.Client, clusters cluster.Clusters) *Delete {
	return &Delete{cp: cp, clusters: clusters}
}

func (h *Delete) Run(ctx context.Context, leased command.Leased, logger *slog.Logger) error {
	payload, instance, snapshot, err := decodeSnapshot(leased)
	if err != nil {
		return err
	}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return err
	}
	err = cl.Provider.DeleteInstanceSnapshot(ctx, payload.ProjectName, instance, snapshot)
	// 404: an earlier run deleted it, or its VM is gone and took it along (lab-storage 05, 06).
	isAlreadyGone := provider.IsNotFound(err)
	if err != nil && !isAlreadyGone {
		return err
	}
	if isAlreadyGone {
		logger.Info("already gone; nothing to delete", "object", provider.SnapshotId(instance, snapshot))
	}
	return sendStatus(ctx, h.cp, payload.VmId, payload.SnapshotId, statusReport{Report: reportDeleted})
}

func (h *Delete) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	var payload snapshotPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	return sendStatus(ctx, h.cp, payload.VmId, payload.SnapshotId, requestFailed(reasonDeleteFailed, cause))
}
