// Package loadbalancer runs the load balancer provider commands and the cluster's load balancer reconciler. A Titan load
// balancer is one Incus network load balancer on its network, named by its VIP
// (titan-ops features/load-balancer/design.md#lb-resource-model).
package loadbalancer

import (
	"context"
	"net/url"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
)

// The reports UpdateLoadBalancerStatusCommandHandler accepts (titan-ops features/load-balancer/design.md#lb-lifecycle).
const (
	reportObserved      = "Observed"
	reportCreated       = "Created"
	reportUpdated       = "Updated"
	reportRequestFailed = "RequestFailed"
	reportDeleted       = "Deleted"
)

// Reason is returned verbatim to the tenant, so it carries only these fixed texts; the raw error goes to the
// admin-only detail (titan-ops features/public-api/design.md#status-reason).
const (
	reasonCreateFailed = "Load balancer creation failed due to a system error. Please try again or contact support."
	reasonUpdateFailed = "Load balancer update failed due to a system error. Please try again or contact support."
	reasonDeleteFailed = "Load balancer deletion failed due to a system error. Please try again or contact support."
)

// statusReport is the body of PATCH /internal/load-balancers/{id}/status. Exists is a pointer so an Observed report
// always says true or false, and the other reports leave it out.
type statusReport struct {
	Report     string `json:"report"`
	Exists     *bool  `json:"exists,omitempty"`
	ExternalId string `json:"externalId,omitempty"`
	IntentId   string `json:"intentId,omitempty"`
	Reason     string `json:"reason,omitempty"`
	Detail     string `json:"detail,omitempty"`
}

func sendStatus(ctx context.Context, cp *controlplane.Client, loadBalancerId string, report statusReport) error {
	return cp.Patch(ctx, "/internal/load-balancers/"+url.PathEscape(loadBalancerId)+"/status", report)
}
