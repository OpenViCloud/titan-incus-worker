package vmsnapshot

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
)

// ReconcilableVmSnapshot mirrors ReconcilableVmSnapshotQueryResult: what Titan last recorded of a snapshot.
type ReconcilableVmSnapshot struct {
	Id               string `json:"id"`
	VmId             string `json:"vmId"`
	ExternalId       string `json:"externalId"`
	CurrentExistence string `json:"currentExistence"`
}

// The CurrentExistence words of the control plane (Titan.Domain.Values.CurrentExistence) the reconciler compares to.
const (
	existenceExists   = "Exists"
	existenceNotFound = "NotFound"
)

// Reconcile observes one cluster's VM snapshots and reports what differs; it never acts on Incus, and it never reports
// Deleted: a snapshot gone is NotFound (titan-ops architecture/desired-and-current.md#existence-axis). The all-projects
// instance list (recursion=2) carries each instance's snapshots (lab-storage 05), so one read covers them all.
func Reconcile(ctx context.Context, cp *controlplane.Client, p provider.Provider, clusterId string, logger *slog.Logger) error {
	var rows []ReconcilableVmSnapshot
	if err := cp.GetJSON(ctx, "/internal/reconcile/vm-snapshots?clusterId="+url.QueryEscape(clusterId), &rows); err != nil {
		return fmt.Errorf("list reconcilable VM snapshots of cluster %s: %w", clusterId, err)
	}
	if len(rows) == 0 {
		return nil
	}
	instances, err := p.ListVirtualMachinesAllProjects(ctx)
	if err != nil {
		return fmt.Errorf("observe VM snapshots of cluster %s: %w", clusterId, err)
	}
	snapshotsByInstance := SnapshotsByInstance(instances)

	for _, row := range rows {
		report, shouldReport := DecideReport(row, snapshotsByInstance)
		if !shouldReport {
			continue
		}
		err := sendStatus(ctx, cp, row.VmId, row.Id, report)
		if err != nil {
			logger.Warn("VM snapshot report failed; next cycle sends it again", "snapshotId", row.Id, "error", err)
			continue
		}
		logger.Info("reconcile report sent", "snapshotId", row.Id, "vmId", row.VmId,
			"recordedExistence", row.CurrentExistence, "exists", *report.Exists)
	}
	return nil
}

// SnapshotsByInstance keys each instance's snapshot names by the instance name.
func SnapshotsByInstance(instances []api.InstanceFull) map[string]map[string]bool {
	byInstance := map[string]map[string]bool{}
	for _, instance := range instances {
		names := map[string]bool{}
		for _, snapshot := range instance.Snapshots {
			names[snapshot.Name] = true
		}
		byInstance[instance.Name] = names
	}
	return byInstance
}

// DecideReport: seeing a snapshot only says it exists, so it can never end a revert that has not happened. A VM gone
// says nothing: its delete settles its snapshots. A snapshot gone from a VM still there is NotFound unless already
// recorded.
func DecideReport(row ReconcilableVmSnapshot, snapshotsByInstance map[string]map[string]bool) (statusReport, bool) {
	instance, snapshot, err := provider.SplitSnapshotId(row.ExternalId)
	if err != nil {
		return statusReport{}, false
	}
	snapshots, instancePresent := snapshotsByInstance[instance]
	if !instancePresent {
		return statusReport{}, false
	}
	if snapshots[snapshot] {
		return observed(true), row.CurrentExistence != existenceExists
	}
	return observed(false), row.CurrentExistence != existenceNotFound
}
