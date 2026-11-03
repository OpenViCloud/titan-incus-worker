// Package template runs template.sync — a template on Incus is an image alias in the default project
// (titan-ops features/catalog/design.md#template-is-an-image-alias) — and the cluster's template reconciler.
package template

import (
	"context"
	"net/url"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
)

// The reports of the old route, which UpdateTemplateStatusCommandHandler maps onto the waiting sync until rollout
// phase 6 (titan-ops architecture/resource-lifecycle.md#rollout). Incus has no status word for an image alias
// (titan-ops features/catalog/design.md#template-state-is-existence). A template is never deleted, so there is no
// Deleted report; the reconciler's observations use the observation route.
const (
	reportCreated       = "Created"
	reportRequestFailed = "RequestFailed"
)

// The fixed reasons of a give-up, one per cause an operator acts on differently; the raw error goes to the detail.
const (
	reasonWrongArchitecture = "The image server has no VM image of this cluster's architecture."
	reasonNoCluster         = "The cluster is not configured."
	reasonSyncFailed        = "Template sync failed due to a system error."
)

// statusReport is the body of the old PATCH /internal/templates/{templateId}/status, which a command without a request
// id still uses; clusterId is required.
type statusReport struct {
	Report     string `json:"report"`
	ClusterId  string `json:"clusterId"`
	ExternalId string `json:"externalId,omitempty"`
	Reason     string `json:"reason,omitempty"`
	Detail     string `json:"detail,omitempty"`
}

func sendStatus(ctx context.Context, cp *controlplane.Client, templateId string, report statusReport) error {
	return cp.Patch(ctx, templatePath(templateId)+"/status", report)
}

// RequestEnd is the body of PATCH /internal/templates/{templateId}/clusters/{clusterId}/requests/{requestId}: the
// report that ends the sync request a command carries (titan-ops architecture/resource-lifecycle.md#callback-routes).
type RequestEnd struct {
	Outcome    string `json:"outcome"`
	ExternalId string `json:"externalId,omitempty"`
	Reason     string `json:"reason,omitempty"`
	Detail     string `json:"detail,omitempty"`
}

// done carries the alias: it is the template's id on the cluster, and a Done without it is refused while none is
// recorded.
func done(alias string) RequestEnd {
	return RequestEnd{Outcome: controlplane.OutcomeDone, ExternalId: alias}
}

// failed carries the fixed reason, the raw error cut for the admin, and the alias when a give-up found it usable.
func failed(alias string, cause error) RequestEnd {
	return RequestEnd{Outcome: controlplane.OutcomeFailed, ExternalId: alias, Reason: giveUpReason(cause),
		Detail: controlplane.Detail(cause)}
}

func endRequest(ctx context.Context, cp *controlplane.Client, templateId string, clusterId string, requestId string,
	end RequestEnd) error {
	return cp.Patch(ctx, controlplane.RequestPath(templateClusterPath(templateId, clusterId), requestId), end)
}

// ObservationBody is the body of PATCH /internal/templates/{templateId}/clusters/{clusterId}/observation: whether the
// cluster has the alias (titan-ops architecture/desired-and-current.md#existence-axis).
type ObservationBody struct {
	Exists bool `json:"exists"`
}

func sendObservation(ctx context.Context, cp *controlplane.Client, templateId string, clusterId string,
	body ObservationBody) error {
	return cp.Patch(ctx, controlplane.ObservationPath(templateClusterPath(templateId, clusterId)), body)
}

func templatePath(templateId string) string {
	return "/internal/templates/" + url.PathEscape(templateId)
}

// templateClusterPath is the template in one cluster, the child resource a sync request belongs to.
func templateClusterPath(templateId string, clusterId string) string {
	return templatePath(templateId) + "/clusters/" + url.PathEscape(clusterId)
}
