package vmsnapshot

import (
	"context"
	"log/slog"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
)

// Delete runs vm-snapshot.delete and ends its request Done (Deleted on the old route). Giving up ends it Failed, which
// gives the snapshot back (titan-ops architecture/desired-and-current.md#abandoned-request), unless Incus no longer has
// the snapshot.
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
	if payload.RequestId == "" {
		return sendStatus(ctx, h.cp, payload.VmId, payload.SnapshotId, statusReport{Report: reportDeleted})
	}
	return endRequest(ctx, h.cp, payload.VmId, payload.SnapshotId, payload.RequestId, done(""))
}

func (h *Delete) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	var payload snapshotPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	if payload.RequestId == "" {
		return sendStatus(ctx, h.cp, payload.VmId, payload.SnapshotId, requestFailed(reasonDeleteFailed, cause))
	}
	gone, err := h.snapshotIsGone(ctx, leased.ClusterId, payload)
	if err != nil {
		return err
	}
	// The delete is the only write, and a 404 read proves it happened: the request is Done
	// (titan-ops architecture/resource-lifecycle.md#callback-routes).
	if gone {
		logger.Info("already gone; the give-up ends the request done", "object", payload.ExternalSnapshotId)
		return endRequest(ctx, h.cp, payload.VmId, payload.SnapshotId, payload.RequestId, done(""))
	}
	end := failed("", reasonDeleteFailed, cause)
	return endRequest(ctx, h.cp, payload.VmId, payload.SnapshotId, payload.RequestId, end)
}

// snapshotIsGone is true only when Incus answers 404, which a VM gone along with its snapshots answers too. A payload or
// cluster that cannot be read from tells nothing, which is not gone.
func (h *Delete) snapshotIsGone(ctx context.Context, clusterId string, payload snapshotPayload) (bool, error) {
	instance, snapshot, err := provider.SplitSnapshotId(payload.ExternalSnapshotId)
	if err != nil || payload.ProjectName == "" {
		return false, nil
	}
	exists, known, err := snapshotExists(ctx, h.clusters, clusterId, payload.ProjectName, instance, snapshot)
	return known && !exists, err
}
