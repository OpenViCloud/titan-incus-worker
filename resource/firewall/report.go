// Package firewall runs the firewall rule provider commands and the cluster's firewall rule reconciler. A Titan rule is
// one entry of its network's ACL, found by its description (titan-ops features/network/design.md#firewall-on-incus).
package firewall

import (
	"context"
	"net/url"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
)

// A give-up is a request failure (titan-ops architecture/desired-and-current.md#request-failure). Its reason is
// returned verbatim to the tenant, so it carries only these fixed texts; the raw error goes to the admin-only detail
// (titan-ops features/public-api/design.md#status-reason).
const (
	reasonCreateFailed = "Firewall rule creation failed due to a system error. Please try again or contact support."
	reasonDeleteFailed = "Firewall rule deletion failed due to a system error. Please try again or contact support."
)

// RequestEnd is the body of PATCH /internal/networks/{networkId}/firewall-rules/{ruleId}/requests/{requestId}: the
// report that ends the request a command carries (titan-ops architecture/resource-lifecycle.md#callback-routes).
type RequestEnd struct {
	Outcome    string       `json:"outcome"`
	ExternalId string       `json:"externalId,omitempty"`
	Observed   *Observation `json:"observed,omitempty"`
	Reason     string       `json:"reason,omitempty"`
	Detail     string       `json:"detail,omitempty"`
}

// Observation is the rule as the worker read it in its ACL: the Incus rule state word for word
// (titan-ops architecture/desired-and-current.md#provider-state-is-verbatim). A rule not in the ACL has none.
type Observation struct {
	State string `json:"state,omitempty"`
}

func done(externalId string, observed *Observation) RequestEnd {
	return RequestEnd{Outcome: controlplane.OutcomeDone, ExternalId: externalId, Observed: observed}
}

// failed carries the fixed reason the tenant reads, the raw error cut for the admin, and the provider id when the rule
// is in the ACL.
func failed(externalId string, reason string, cause error) RequestEnd {
	return RequestEnd{Outcome: controlplane.OutcomeFailed, ExternalId: externalId, Reason: reason,
		Detail: controlplane.Detail(cause)}
}

func endRequest(ctx context.Context, cp *controlplane.Client, networkId string, ruleId string, requestId string,
	end RequestEnd) error {
	return cp.Patch(ctx, controlplane.RequestPath(rulePath(networkId, ruleId), requestId), end)
}

// ObservationBody is the body of PATCH /internal/networks/{networkId}/firewall-rules/{ruleId}/observation; whether the
// rule is in the ACL is its own field (titan-ops architecture/desired-and-current.md#existence-axis).
type ObservationBody struct {
	Exists   bool         `json:"exists"`
	Observed *Observation `json:"observed,omitempty"`
}

func sendObservation(ctx context.Context, cp *controlplane.Client, networkId string, ruleId string,
	body ObservationBody) error {
	return cp.Patch(ctx, controlplane.ObservationPath(rulePath(networkId, ruleId)), body)
}

func rulePath(networkId string, ruleId string) string {
	return "/internal/networks/" + url.PathEscape(networkId) + "/firewall-rules/" + url.PathEscape(ruleId)
}
