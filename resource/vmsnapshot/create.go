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
	SnapshotId string `json:"snapshotId"`
	// RequestId names the request this command ends (titan-ops architecture/resource-lifecycle.md#callback-routes). Empty
	// on a command of the old control plane that the migration gave no id: it keeps the old reporting until rollout
	// phase 6 (titan-ops architecture/resource-lifecycle.md#rollout).
	RequestId    string `json:"requestId"`
	VmId         string `json:"vmId"`
	ExternalVmId string `json:"externalVmId"`
	Name         string `json:"name"`
	ProjectName  string `json:"projectName"`
}

// Create runs vm-snapshot.create. Incus has made it once the operation returns, so the create is done: it ends the
// request Done with the provider id "<instance>/<snapshot>" (Created on the old route).
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
	if payload.RequestId == "" {
		report := statusReport{Report: reportCreated, ExternalId: externalId}
		return sendStatus(ctx, h.cp, payload.VmId, payload.SnapshotId, report)
	}
	return endRequest(ctx, h.cp, payload.VmId, payload.SnapshotId, payload.RequestId, done(externalId))
}

// GiveUp ends the request Failed (RequestFailed on the old route), with the provider id when Incus has the snapshot:
// then the create did happen, and its delete reaches Incus.
func (h *Create) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	var payload createPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	if payload.RequestId == "" {
		report := requestFailed(reasonCreateFailed, cause)
		report.ExternalId, _ = h.existingSnapshot(ctx, leased.ClusterId, payload)
		return sendStatus(ctx, h.cp, payload.VmId, payload.SnapshotId, report)
	}
	externalId, err := h.existingSnapshot(ctx, leased.ClusterId, payload)
	if err != nil {
		return err
	}
	end := failed(externalId, reasonCreateFailed, cause)
	return endRequest(ctx, h.cp, payload.VmId, payload.SnapshotId, payload.RequestId, end)
}

// existingSnapshot returns the snapshot's provider id when Incus has it, "" when it does not, when the payload lacks what
// the read needs, or when the worker cannot reach the cluster for good; a read that fails is the error. The old route
// reads the error as no snapshot.
func (h *Create) existingSnapshot(ctx context.Context, clusterId string, payload createPayload) (string, error) {
	name, err := provider.SnapshotName(payload.SnapshotId)
	if err != nil || payload.ProjectName == "" || payload.ExternalVmId == "" {
		return "", nil
	}
	exists, _, err := snapshotExists(ctx, h.clusters, clusterId, payload.ProjectName, payload.ExternalVmId, name)
	if err != nil || !exists {
		return "", err
	}
	return provider.SnapshotId(payload.ExternalVmId, name), nil
}
