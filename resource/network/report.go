// Package network runs the network provider commands and the cluster's network reconciler.
package network

import (
	"context"
	"net/url"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
)

// The reports of the old route, which UpdateNetworkStatusCommandHandler maps onto the waiting request until rollout
// phase 6 (titan-ops architecture/resource-lifecycle.md#rollout); a give-up is a request failure
// (titan-ops architecture/desired-and-current.md#request-failure).
const (
	reportCreated       = "Created"
	reportRequestFailed = "RequestFailed"
	reportDeleted       = "Deleted"
)

// Reason is returned verbatim to the tenant, so it carries only these fixed texts; the raw error goes to the
// admin-only detail (titan-ops features/public-api/design.md#status-reason).
const (
	reasonCreateFailed = "Network creation failed due to a system error. Please try again or contact support."
	reasonDeleteFailed = "Network deletion failed due to a system error. Please try again or contact support."
)

// statusReport is the body of the old PATCH /internal/networks/{networkId}/status, which a command without a request
// id still uses.
type statusReport struct {
	Report     string `json:"report"`
	ExternalId string `json:"externalId,omitempty"`
	Cidr       string `json:"cidr,omitempty"`
	Gateway    string `json:"gateway,omitempty"`
	Reason     string `json:"reason,omitempty"`
	Detail     string `json:"detail,omitempty"`
}

func sendStatus(ctx context.Context, cp *controlplane.Client, networkId string, report statusReport) error {
	return cp.Patch(ctx, networkPath(networkId)+"/status", report)
}

// RequestEnd is the body of PATCH /internal/networks/{networkId}/requests/{requestId}: the report that ends the request
// a command carries (titan-ops architecture/resource-lifecycle.md#callback-routes).
type RequestEnd struct {
	Outcome    string       `json:"outcome"`
	ExternalId string       `json:"externalId,omitempty"`
	Observed   *Observation `json:"observed,omitempty"`
	Reason     string       `json:"reason,omitempty"`
	Detail     string       `json:"detail,omitempty"`
}

// Observation is the network as the worker read it: the Incus status word for word
// (titan-ops architecture/desired-and-current.md#provider-state-is-verbatim), and the IPv4 range and gateway. The
// reconciler sends the status only.
type Observation struct {
	Status  string `json:"status,omitempty"`
	Cidr    string `json:"cidr,omitempty"`
	Gateway string `json:"gateway,omitempty"`
}

func done(externalId string, observed *Observation) RequestEnd {
	return RequestEnd{Outcome: controlplane.OutcomeDone, ExternalId: externalId, Observed: observed}
}

// failed carries the fixed reason the tenant reads, the raw error cut for the admin, and what the worker saw.
func failed(externalId string, observed *Observation, reason string, cause error) RequestEnd {
	return RequestEnd{Outcome: controlplane.OutcomeFailed, ExternalId: externalId, Observed: observed, Reason: reason,
		Detail: controlplane.Detail(cause)}
}

func endRequest(ctx context.Context, cp *controlplane.Client, networkId string, requestId string,
	end RequestEnd) error {
	return cp.Patch(ctx, controlplane.RequestPath(networkPath(networkId), requestId), end)
}

// ObservationBody is the body of PATCH /internal/networks/{networkId}/observation. A network Incus does not have has
// no observation.
type ObservationBody struct {
	Exists   bool         `json:"exists"`
	Observed *Observation `json:"observed,omitempty"`
}

func sendObservation(ctx context.Context, cp *controlplane.Client, networkId string, body ObservationBody) error {
	return cp.Patch(ctx, controlplane.ObservationPath(networkPath(networkId)), body)
}

func networkPath(networkId string) string {
	return "/internal/networks/" + url.PathEscape(networkId)
}
