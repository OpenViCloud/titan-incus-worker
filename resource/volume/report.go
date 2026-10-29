// Package volume runs the volume provider commands and the cluster's storage reconciler (volumes and volume snapshots).
package volume

import (
	"context"
	"net/url"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
)

// The reports UpdateVolumeStatusCommandHandler and UpdateVolumeSnapshotStatusCommandHandler accept. Incus has no status
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

// SendStatus reports on one volume; a restore reports on the volume it makes.
func SendStatus(ctx context.Context, cp *controlplane.Client, volumeId string, report StatusReport) error {
	return cp.Patch(ctx, "/internal/volumes/"+url.PathEscape(volumeId)+"/status", report)
}

// SendSnapshotStatus is PATCH /internal/volumes/{volumeId}/snapshots/{snapshotId}/status; the body is a StatusReport.
func SendSnapshotStatus(ctx context.Context, cp *controlplane.Client, volumeId string, snapshotId string, report StatusReport) error {
	path := "/internal/volumes/" + url.PathEscape(volumeId) + "/snapshots/" + url.PathEscape(snapshotId) + "/status"
	return cp.Patch(ctx, path, report)
}
