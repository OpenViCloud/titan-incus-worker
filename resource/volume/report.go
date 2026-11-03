// Package volume runs the volume provider commands and the cluster's storage reconciler (volumes and volume snapshots).
package volume

import (
	"context"
	"net/url"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
)

// The reports of the old route, which UpdateVolumeStatusCommandHandler and UpdateVolumeSnapshotStatusCommandHandler map onto
// the waiting request until rollout phase 6 (titan-ops architecture/resource-lifecycle.md#rollout). Incus has no status
// word for a custom volume or its snapshots (titan-ops features/volume/design.md#volume-state-is-existence): an observation
// says whether it exists and, for a volume, which instances use it; a give-up is a request failure
// (titan-ops architecture/desired-and-current.md#request-failure).
const (
	ReportObserved      = "Observed"
	ReportCreated       = "Created"
	ReportRequestFailed = "RequestFailed"
	ReportDeleted       = "Deleted"
)

// Reason is returned verbatim to the tenant, so it carries only these fixed texts; the raw error goes to the
// admin-only detail (titan-ops features/public-api/design.md#status-reason).
const (
	ReasonCreateFailed = "Volume creation failed due to a system error. Please try again or contact support."
	reasonAttachFailed = "Attaching the volume failed due to a system error. Please try again or contact support."
	reasonDetachFailed = "Detaching the volume failed due to a system error. Please try again or contact support."
	reasonDeleteFailed = "Volume deletion failed due to a system error. Please try again or contact support."
)

// StatusReport is the body of PATCH /internal/volumes/{volumeId}/status and of its snapshots' callback. Exists is a
// pointer so an observation always says true or false, and the other reports leave it out. AttachedTo is the names of
// the instances in the volume's used_by, as Incus lists them: which are Titan VMs is the control plane's to say
// (titan-ops features/volume/design.md#attachment-from-used-by).
type StatusReport struct {
	Report     string   `json:"report"`
	Exists     *bool    `json:"exists,omitempty"`
	AttachedTo []string `json:"attachedTo,omitempty"`
	ExternalId string   `json:"externalId,omitempty"`
	Reason     string   `json:"reason,omitempty"`
	Detail     string   `json:"detail,omitempty"`
}

// Observed builds an observation: whether Incus has the object and, for a volume, the instances using it.
func Observed(exists bool, attachedTo []string) StatusReport {
	return StatusReport{Report: ReportObserved, Exists: &exists, AttachedTo: attachedTo}
}

// RequestEnd is the body of PATCH /internal/volumes/{volumeId}/requests/{requestId}, and without Observed of a snapshot's
// .../snapshots/{snapshotId}/requests/{requestId}: the report that ends the request a command carries
// (titan-ops architecture/resource-lifecycle.md#callback-routes).
type RequestEnd struct {
	Outcome    string       `json:"outcome"`
	ExternalId string       `json:"externalId,omitempty"`
	Observed   *Observation `json:"observed,omitempty"`
	Reason     string       `json:"reason,omitempty"`
	Detail     string       `json:"detail,omitempty"`
}

// Observation is the volume as the worker read it: there, and used by these instances (none when it is free). A volume
// the worker did not find has no observation.
type Observation struct {
	AttachedTo []string `json:"attachedTo"`
}

// Done ends a request with what the worker saw: the provider id when the resource had none yet, the volume's users
// when it read them.
func Done(externalId string, observed *Observation) RequestEnd {
	return RequestEnd{Outcome: controlplane.OutcomeDone, ExternalId: externalId, Observed: observed}
}

// Failed ends a request that gave up: the fixed reason the tenant reads, the raw error cut for the admin, and what the
// worker saw.
func Failed(externalId string, observed *Observation, reason string, cause error) RequestEnd {
	return RequestEnd{Outcome: controlplane.OutcomeFailed, ExternalId: externalId, Observed: observed, Reason: reason,
		Detail: controlplane.Detail(cause)}
}

// EndRequest reports on one volume's request; a restore's is the new volume's create.
func EndRequest(ctx context.Context, cp *controlplane.Client, volumeId string, requestId string, end RequestEnd) error {
	return cp.Patch(ctx, controlplane.RequestPath(volumePath(volumeId), requestId), end)
}

// EndSnapshotRequest reports on one snapshot's request; the snapshot's report carries no observation.
func EndSnapshotRequest(ctx context.Context, cp *controlplane.Client, volumeId string, snapshotId string, requestId string, end RequestEnd) error {
	return cp.Patch(ctx, controlplane.RequestPath(snapshotPath(volumeId, snapshotId), requestId), end)
}

// observationBody is the body of PATCH /internal/volumes/{volumeId}/observation and, without Observed, of a snapshot's.
type observationBody struct {
	Exists   bool         `json:"exists"`
	Observed *Observation `json:"observed,omitempty"`
}

func sendObservation(ctx context.Context, cp *controlplane.Client, volumeId string, exists bool, attachedTo []string) error {
	body := observationBody{Exists: exists}
	if exists {
		body.Observed = &Observation{AttachedTo: attachedTo}
	}
	return cp.Patch(ctx, controlplane.ObservationPath(volumePath(volumeId)), body)
}

func sendSnapshotObservation(ctx context.Context, cp *controlplane.Client, volumeId string, snapshotId string, exists bool) error {
	return cp.Patch(ctx, controlplane.ObservationPath(snapshotPath(volumeId, snapshotId)), observationBody{Exists: exists})
}

func volumePath(volumeId string) string {
	return "/internal/volumes/" + url.PathEscape(volumeId)
}

func snapshotPath(volumeId string, snapshotId string) string {
	return volumePath(volumeId) + "/snapshots/" + url.PathEscape(snapshotId)
}

// SendStatus reports on one volume on the old route, which a command without a request id still uses; a restore reports
// on the volume it makes.
func SendStatus(ctx context.Context, cp *controlplane.Client, volumeId string, report StatusReport) error {
	return cp.Patch(ctx, "/internal/volumes/"+url.PathEscape(volumeId)+"/status", report)
}

// SendSnapshotStatus is the old PATCH /internal/volumes/{volumeId}/snapshots/{snapshotId}/status; the body is a StatusReport.
func SendSnapshotStatus(ctx context.Context, cp *controlplane.Client, volumeId string, snapshotId string, report StatusReport) error {
	return cp.Patch(ctx, snapshotPath(volumeId, snapshotId)+"/status", report)
}
