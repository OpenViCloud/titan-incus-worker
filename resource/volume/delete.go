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

type deletePayload struct {
	VolumeId string `json:"volumeId"`
	// RequestId names the request this command ends (titan-ops architecture/resource-lifecycle.md#callback-routes). Empty
	// on a command of the old control plane that the migration gave no id: it keeps the old reporting until rollout
	// phase 6 (titan-ops architecture/resource-lifecycle.md#rollout).
	RequestId        string `json:"requestId"`
	ExternalVolumeId string `json:"externalVolumeId"`
	ProjectName      string `json:"projectName"`
}

// Delete runs volume.delete and ends its request Done (Deleted on the old route). Incus refuses while the volume is
// attached (400, lab-storage 04): an error the next lease may get past. Giving up ends it Failed, which gives the volume
// back (titan-ops architecture/resource-lifecycle.md#kinds).
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
	isComplete := payload.VolumeId != "" && payload.ExternalVolumeId != "" && payload.ProjectName != ""
	if !isComplete {
		return command.Permanent(fmt.Errorf("volume.delete payload lacks volumeId, externalVolumeId or projectName"))
	}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return err
	}
	pool := provider.StoragePoolOrDefault(cl.StoragePool)
	err = cl.Provider.DeleteVolume(ctx, payload.ProjectName, pool, payload.ExternalVolumeId)
	// 404: an earlier run deleted it, or its project is gone (lab-storage 07).
	isAlreadyGone := provider.IsNotFound(err)
	if err != nil && !isAlreadyGone {
		return err
	}
	if isAlreadyGone {
		logger.Info("already gone; nothing to delete", "object", payload.ExternalVolumeId)
	}
	if payload.RequestId == "" {
		return SendStatus(ctx, h.cp, payload.VolumeId, StatusReport{Report: ReportDeleted})
	}
	return EndRequest(ctx, h.cp, payload.VolumeId, payload.RequestId, Done("", nil))
}

func (h *Delete) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	var payload deletePayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	if payload.RequestId != "" {
		return EndRequest(ctx, h.cp, payload.VolumeId, payload.RequestId, Failed("", nil, reasonDeleteFailed, cause))
	}
	report := StatusReport{Report: ReportRequestFailed, Reason: reasonDeleteFailed, Detail: cause.Error()}
	return SendStatus(ctx, h.cp, payload.VolumeId, report)
}
