// Package loadbalancer runs the load balancer provider commands and the cluster's load balancer reconciler. A Titan load
// balancer is one Incus network load balancer on its network, named by its VIP
// (titan-ops features/load-balancer/design.md#lb-resource-model).
package loadbalancer

import (
	"context"
	"net/url"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
)

// The reports of the old route, which UpdateLoadBalancerStatusCommandHandler maps onto the waiting request until rollout
// phase 6 (titan-ops architecture/resource-lifecycle.md#rollout; titan-ops features/load-balancer/design.md#lb-lifecycle).
const (
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

// statusReport is the body of the old PATCH /internal/load-balancers/{id}/status, which a command without a request id
// still uses.
type statusReport struct {
	Report     string `json:"report"`
	ExternalId string `json:"externalId,omitempty"`
	IntentId   string `json:"intentId,omitempty"`
	Reason     string `json:"reason,omitempty"`
	Detail     string `json:"detail,omitempty"`
}

func sendStatus(ctx context.Context, cp *controlplane.Client, loadBalancerId string, report statusReport) error {
	return cp.Patch(ctx, loadBalancerPath(loadBalancerId)+"/status", report)
}

// RequestEnd is the body of PATCH /internal/load-balancers/{id}/requests/{requestId}: the report that ends the request
// a command carries (titan-ops architecture/resource-lifecycle.md#callback-routes).
type RequestEnd struct {
	Outcome    string `json:"outcome"`
	ExternalId string `json:"externalId,omitempty"`
	Reason     string `json:"reason,omitempty"`
	Detail     string `json:"detail,omitempty"`
}

func done(externalId string) RequestEnd {
	return RequestEnd{Outcome: controlplane.OutcomeDone, ExternalId: externalId}
}

// failed carries the fixed reason the tenant reads, the raw error cut for the admin, and the VIP a create's give-up
// found.
func failed(externalId string, reason string, cause error) RequestEnd {
	return RequestEnd{Outcome: controlplane.OutcomeFailed, ExternalId: externalId, Reason: reason,
		Detail: controlplane.Detail(cause)}
}

func endRequest(ctx context.Context, cp *controlplane.Client, loadBalancerId string, requestId string, end RequestEnd) error {
	return cp.Patch(ctx, controlplane.RequestPath(loadBalancerPath(loadBalancerId), requestId), end)
}

// ObservationBody is the body of PATCH /internal/load-balancers/{id}/observation: whether Incus has the VIP
// (titan-ops architecture/desired-and-current.md#existence-axis).
type ObservationBody struct {
	Exists bool `json:"exists"`
}

func sendObservation(ctx context.Context, cp *controlplane.Client, loadBalancerId string, body ObservationBody) error {
	return cp.Patch(ctx, controlplane.ObservationPath(loadBalancerPath(loadBalancerId)), body)
}

func loadBalancerPath(loadBalancerId string) string {
	return "/internal/load-balancers/" + url.PathEscape(loadBalancerId)
}

// vipExists reads the VIP: only Incus answering 404 is absent. A read that fails is returned, so the next lease gives
// up again instead of guessing. A cluster the worker cannot reach for good (not configured, or its connection refused
// by the control plane: a permanent error) answers nothing: known is false with no error.
func vipExists(ctx context.Context, clusters cluster.Clusters, clusterId string, project string, network string,
	vip string) (exists bool, known bool, err error) {
	cl, err := clusters.Cluster(ctx, clusterId)
	if err != nil {
		if !command.IsRetryable(err) {
			return false, false, nil
		}
		return false, false, err
	}
	_, _, err = cl.Provider.GetNetworkLoadBalancer(ctx, project, network, vip)
	if provider.IsNotFound(err) {
		return false, true, nil
	}
	if err != nil {
		return false, false, err
	}
	return true, true, nil
}
