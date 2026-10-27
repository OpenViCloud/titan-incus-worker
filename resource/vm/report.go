// Package vm runs the VM provider commands and the cluster's VM reconciler.
package vm

import (
	"context"
	"net/url"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
)

// The reports UpdateVmStatusCommandHandler accepts. An observation carries the Incus status word for word
// (https://github.com/OpenViCloud/titan-ops/blob/main/docs/architecture/desired-and-current.md#provider-state-is-verbatim); whether the instance exists is its own field
// (https://github.com/OpenViCloud/titan-ops/blob/main/docs/architecture/desired-and-current.md#existence-axis), and a give-up is a request failure, not a state
// (https://github.com/OpenViCloud/titan-ops/blob/main/docs/architecture/desired-and-current.md#request-failure).
const (
	reportObserved      = "Observed"
	reportCreated       = "Created"
	reportRequestFailed = "RequestFailed"
	reportDeleted       = "Deleted"
)

// Reason is returned verbatim to the tenant, so it carries only these fixed texts; the raw error goes to the
// admin-only detail (https://github.com/OpenViCloud/titan-ops/blob/main/docs/features/public-api/design.md#status-reason).
const (
	reasonProvisionFailed = "VM provisioning failed due to a system error. Please try again or contact support."
)

// statusReport is the body of PATCH /internal/vms/{vmId}/status.
// Exists is a pointer so an Observed report always says true or false, and the other reports leave it out.
type statusReport struct {
	Report     string `json:"report"`
	Exists     *bool  `json:"exists,omitempty"`
	Status     string `json:"status,omitempty"`
	ExternalId string `json:"externalId,omitempty"`
	IpAddress  string `json:"ipAddress,omitempty"`
	// Left out when the VM has none: the control plane records that as no public IPv6.
	PublicIpv6Address string `json:"publicIpv6Address,omitempty"`
	Reason            string `json:"reason,omitempty"`
	Detail            string `json:"detail,omitempty"`
}

func sendStatus(ctx context.Context, cp *controlplane.Client, vmId string, report statusReport) error {
	return cp.Patch(ctx, "/internal/vms/"+url.PathEscape(vmId)+"/status", report)
}
