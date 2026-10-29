// Package vmsnapshot runs the VM snapshot provider commands (create, revert, delete) and the cluster's VM snapshot
// reconciler.
package vmsnapshot

import (
	"context"
	"fmt"
	"net/url"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
)

// The reports UpdateVmSnapshotStatusCommandHandler accepts. Incus has no status word for an instance snapshot
// (titan-ops features/vm/design.md#vm-snapshot-is-existence): an observation says whether it exists, the revert command
// says when the VM's disk is back at it, and a give-up is a request failure
// (titan-ops architecture/desired-and-current.md#request-failure).
const (
	reportObserved      = "Observed"
	reportCreated       = "Created"
	reportReverted      = "Reverted"
	reportRequestFailed = "RequestFailed"
	reportDeleted       = "Deleted"
)

// The give-up reasons reach the tenant as the snapshot's requestFailureReason, so they are fixed texts; the raw error
// goes to the admin-only detail.
const (
	reasonCreateFailed = "Snapshot creation failed due to a system error. Please try again or contact support."
	reasonRevertFailed = "Reverting to the snapshot failed due to a system error. Please try again or contact support."
	reasonDeleteFailed = "Snapshot deletion failed due to a system error. Please try again or contact support."
)

// statusReport is the body of PATCH /internal/vms/{vmId}/snapshots/{snapshotId}/status. Exists is a pointer so an
// observation always says true or false, and the other reports leave it out.
type statusReport struct {
	Report     string `json:"report"`
	Exists     *bool  `json:"exists,omitempty"`
	ExternalId string `json:"externalId,omitempty"`
	Reason     string `json:"reason,omitempty"`
	Detail     string `json:"detail,omitempty"`
}

func observed(exists bool) statusReport {
	return statusReport{Report: reportObserved, Exists: &exists}
}

func requestFailed(reason string, cause error) statusReport {
	return statusReport{Report: reportRequestFailed, Reason: reason, Detail: cause.Error()}
}

func sendStatus(ctx context.Context, cp *controlplane.Client, vmId string, snapshotId string, report statusReport) error {
	path := "/internal/vms/" + url.PathEscape(vmId) + "/snapshots/" + url.PathEscape(snapshotId) + "/status"
	return cp.Patch(ctx, path, report)
}

// snapshotPayload mirrors EnqueueVmSnapshotRevertAsync and EnqueueVmSnapshotDeleteAsync.
type snapshotPayload struct {
	SnapshotId         string `json:"snapshotId"`
	VmId               string `json:"vmId"`
	ExternalSnapshotId string `json:"externalSnapshotId"`
	ProjectName        string `json:"projectName"`
}

func decodeSnapshot(leased command.Leased) (snapshotPayload, string, string, error) {
	var payload snapshotPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return snapshotPayload{}, "", "", err
	}
	isComplete := payload.SnapshotId != "" && payload.VmId != "" && payload.ProjectName != ""
	if !isComplete {
		return snapshotPayload{}, "", "", command.Permanent(fmt.Errorf("%s payload lacks snapshotId, vmId or projectName", leased.Kind))
	}
	instance, snapshot, err := provider.SplitSnapshotId(payload.ExternalSnapshotId)
	if err != nil {
		return snapshotPayload{}, "", "", command.Permanent(err)
	}
	return payload, instance, snapshot, nil
}
