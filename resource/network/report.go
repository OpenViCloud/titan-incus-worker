// Package network runs the network provider commands and the cluster's network reconciler.
package network

import (
	"context"
	"net/url"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
)

// The reports UpdateNetworkStatusCommandHandler accepts. An observation carries the Incus network status word for
// word (https://github.com/OpenViCloud/titan-ops/blob/main/docs/architecture/desired-and-current.md#provider-state-is-verbatim); whether the network exists is its own
// field (https://github.com/OpenViCloud/titan-ops/blob/main/docs/architecture/desired-and-current.md#existence-axis), and a give-up is a request failure
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
	reasonCreateFailed = "Network creation failed due to a system error. Please try again or contact support."
	reasonDeleteFailed = "Network deletion failed due to a system error. Please try again or contact support."
)

// statusReport is the body of PATCH /internal/networks/{networkId}/status.
// Exists is a pointer so an Observed report always says true or false, and the other reports leave it out.
type statusReport struct {
	Report     string `json:"report"`
	Exists     *bool  `json:"exists,omitempty"`
	Status     string `json:"status,omitempty"`
	ExternalId string `json:"externalId,omitempty"`
	Cidr       string `json:"cidr,omitempty"`
	Gateway    string `json:"gateway,omitempty"`
	Reason     string `json:"reason,omitempty"`
	Detail     string `json:"detail,omitempty"`
}

func sendStatus(ctx context.Context, cp *controlplane.Client, networkId string, report statusReport) error {
	return cp.Patch(ctx, "/internal/networks/"+url.PathEscape(networkId)+"/status", report)
}
