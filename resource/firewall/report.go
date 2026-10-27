// Package firewall runs the firewall rule provider commands and the cluster's firewall rule reconciler. A Titan rule is
// one entry of its network's ACL, found by its description (https://github.com/OpenViCloud/titan-ops/blob/main/docs/features/network/design.md#firewall-on-incus).
package firewall

import (
	"context"
	"net/url"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
)

// The reports UpdateFirewallRuleStatusCommandHandler accepts. An observation carries the Incus ACL rule state word
// for word (https://github.com/OpenViCloud/titan-ops/blob/main/docs/architecture/desired-and-current.md#provider-state-is-verbatim); whether the rule is in the ACL is
// its own field (https://github.com/OpenViCloud/titan-ops/blob/main/docs/architecture/desired-and-current.md#existence-axis), and a give-up is a request failure
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
	reasonCreateFailed = "Firewall rule creation failed due to a system error. Please try again or contact support."
	reasonDeleteFailed = "Firewall rule deletion failed due to a system error. Please try again or contact support."
)

// statusReport is the body of PATCH /internal/networks/{networkId}/firewall-rules/{ruleId}.
// Exists is a pointer so an Observed report always says true or false, and the other reports leave it out.
type statusReport struct {
	Report         string `json:"report"`
	Exists         *bool  `json:"exists,omitempty"`
	State          string `json:"state,omitempty"`
	ExternalRuleId string `json:"externalRuleId,omitempty"`
	Reason         string `json:"reason,omitempty"`
	Detail         string `json:"detail,omitempty"`
}

func sendStatus(ctx context.Context, cp *controlplane.Client, networkId string, ruleId string, report statusReport) error {
	path := "/internal/networks/" + url.PathEscape(networkId) + "/firewall-rules/" + url.PathEscape(ruleId)
	return cp.Patch(ctx, path, report)
}
