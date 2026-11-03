// Package vmsnapshot runs the VM snapshot provider commands (create, revert, delete) and the cluster's VM snapshot
// reconciler.
package vmsnapshot

import (
	"context"
	"fmt"
	"net/url"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
)

// The reports of the old route, which UpdateVmSnapshotStatusCommandHandler maps onto the waiting request until rollout
// phase 6 (titan-ops architecture/resource-lifecycle.md#rollout). Incus has no status word for an instance snapshot
// (titan-ops features/vm/design.md#vm-snapshot-is-existence), and a give-up is a request failure
// (titan-ops architecture/desired-and-current.md#request-failure).
const (
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

// statusReport is the body of the old PATCH /internal/vms/{vmId}/snapshots/{snapshotId}/status, which a command without
// a request id still uses.
type statusReport struct {
	Report     string `json:"report"`
	ExternalId string `json:"externalId,omitempty"`
	Reason     string `json:"reason,omitempty"`
	Detail     string `json:"detail,omitempty"`
}

func requestFailed(reason string, cause error) statusReport {
	return statusReport{Report: reportRequestFailed, Reason: reason, Detail: cause.Error()}
}

func sendStatus(ctx context.Context, cp *controlplane.Client, vmId string, snapshotId string, report statusReport) error {
	return cp.Patch(ctx, snapshotPath(vmId, snapshotId)+"/status", report)
}

// RequestEnd is the body of PATCH /internal/vms/{vmId}/snapshots/{snapshotId}/requests/{requestId}: the report that
// ends the request a command carries (titan-ops architecture/resource-lifecycle.md#callback-routes). A snapshot has no
// state to observe, so it carries none.
type RequestEnd struct {
	Outcome    string `json:"outcome"`
	ExternalId string `json:"externalId,omitempty"`
	Reason     string `json:"reason,omitempty"`
	Detail     string `json:"detail,omitempty"`
}

func done(externalId string) RequestEnd {
	return RequestEnd{Outcome: controlplane.OutcomeDone, ExternalId: externalId}
}

// failed carries the fixed reason the tenant reads, the raw error cut for the admin, and the provider id a create's
// give-up found.
func failed(externalId string, reason string, cause error) RequestEnd {
	return RequestEnd{Outcome: controlplane.OutcomeFailed, ExternalId: externalId, Reason: reason,
		Detail: controlplane.Detail(cause)}
}

func endRequest(ctx context.Context, cp *controlplane.Client, vmId string, snapshotId string, requestId string,
	end RequestEnd) error {
	return cp.Patch(ctx, controlplane.RequestPath(snapshotPath(vmId, snapshotId), requestId), end)
}

// ObservationBody is the body of PATCH /internal/vms/{vmId}/snapshots/{snapshotId}/observation: whether Incus has the
// snapshot, all there is to see of one (titan-ops architecture/desired-and-current.md#existence-axis).
type ObservationBody struct {
	Exists bool `json:"exists"`
}

func sendObservation(ctx context.Context, cp *controlplane.Client, vmId string, snapshotId string,
	body ObservationBody) error {
	return cp.Patch(ctx, controlplane.ObservationPath(snapshotPath(vmId, snapshotId)), body)
}

func snapshotPath(vmId string, snapshotId string) string {
	return "/internal/vms/" + url.PathEscape(vmId) + "/snapshots/" + url.PathEscape(snapshotId)
}

// snapshotPayload mirrors EnqueueVmSnapshotRevertAsync and EnqueueVmSnapshotDeleteAsync.
type snapshotPayload struct {
	SnapshotId string `json:"snapshotId"`
	// RequestId names the request this command ends (titan-ops architecture/resource-lifecycle.md#callback-routes). Empty
	// on a command of the old control plane that the migration gave no id: it keeps the old reporting until rollout
	// phase 6 (titan-ops architecture/resource-lifecycle.md#rollout).
	RequestId          string `json:"requestId"`
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

// snapshotExists reads the snapshot: only Incus answering 404 is absent. A read that fails is returned, so the next
// lease gives up again instead of guessing. A cluster the worker is not configured for (a permanent error) answers
// nothing: known is false with no error.
func snapshotExists(ctx context.Context, clusters cluster.Clusters, clusterId string, project string, instance string,
	snapshot string) (exists bool, known bool, err error) {
	cl, err := clusters.Cluster(ctx, clusterId)
	if err != nil {
		if !command.IsRetryable(err) {
			return false, false, nil
		}
		return false, false, err
	}
	_, err = cl.Provider.GetInstanceSnapshot(ctx, project, instance, snapshot)
	if provider.IsNotFound(err) {
		return false, true, nil
	}
	if err != nil {
		return false, false, err
	}
	return true, true, nil
}
