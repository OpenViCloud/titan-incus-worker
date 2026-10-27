package vmsnapshot

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
)

// createPayload mirrors ProviderCommandQueue.EnqueueVmSnapshotCreateAsync. name is the tenant's; Incus gets Titan's.
type createPayload struct {
	SnapshotId   string `json:"snapshotId"`
	VmId         string `json:"vmId"`
	ExternalVmId string `json:"externalVmId"`
	Name         string `json:"name"`
	ProjectName  string `json:"projectName"`
}

// Create runs vm-snapshot.create. Incus has made it once the operation returns, so it reports Created with the
// provider id "<instance>/<snapshot>": the create is done.
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
	isComplete := payload.SnapshotId != "" && payload.VmId != "" && payload.ExternalVmId != "" && payload.ProjectName != ""
	if !isComplete {
		return command.Permanent(fmt.Errorf("vm-snapshot.create payload lacks snapshotId, vmId, externalVmId or projectName"))
	}
	name, err := provider.SnapshotName(payload.SnapshotId)
	if err != nil {
		return command.Permanent(err)
	}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return err
	}
	err = cl.Provider.CreateInstanceSnapshot(ctx, payload.ProjectName, payload.ExternalVmId, name)
	// A duplicate fails in the operation, not with a 409 (lab-storage 05); the name is ours.
	isOurs := err != nil && provider.IsInstanceSnapshotAlreadyExists(err)
	if err != nil && !isOurs {
		return err
	}
	externalId := provider.SnapshotId(payload.ExternalVmId, name)
	if isOurs {
		logger.Info("already exists; treating it as ours", "object", externalId)
	}
	report := statusReport{Report: reportCreated, ExternalId: externalId}
	return sendStatus(ctx, h.cp, payload.VmId, payload.SnapshotId, report)
}

// GiveUp reports the request failed, with the provider id when Incus has the snapshot: then the create did happen, and
// its delete reaches Incus.
func (h *Create) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	var payload createPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	report := requestFailed(reasonCreateFailed, cause)
	report.ExternalId = h.existingSnapshot(ctx, leased.ClusterId, payload)
	return sendStatus(ctx, h.cp, payload.VmId, payload.SnapshotId, report)
}

// existingSnapshot returns the snapshot's provider id when Incus has it, "" when it does not or cannot be read.
func (h *Create) existingSnapshot(ctx context.Context, clusterId string, payload createPayload) string {
	name, err := provider.SnapshotName(payload.SnapshotId)
	if err != nil || payload.ProjectName == "" || payload.ExternalVmId == "" {
		return ""
	}
	cl, err := h.clusters.Cluster(ctx, clusterId)
	if err != nil {
		return ""
	}
	if err := cl.Provider.GetInstanceSnapshot(ctx, payload.ProjectName, payload.ExternalVmId, name); err != nil {
		return ""
	}
	return provider.SnapshotId(payload.ExternalVmId, name)
}
