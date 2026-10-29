// Package template runs template.sync — a template on Incus is an image alias in the default project
// (titan-ops features/catalog/design.md#template-is-an-image-alias) — and the cluster's template reconciler.
package template

import (
	"context"
	"net/url"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
)

// The reports UpdateTemplateStatusCommandHandler accepts. Incus has no status word for an image alias
// (titan-ops features/catalog/design.md#template-state-is-existence): an observation says whether the alias exists; a
// give-up is a request failure. A template is never deleted, so there is no Deleted report.
const (
	reportObserved      = "Observed"
	reportCreated       = "Created"
	reportRequestFailed = "RequestFailed"
)

// The fixed reasons of a give-up, one per cause an operator acts on differently; the raw error goes to the detail.
const (
	reasonWrongArchitecture = "The image server has no VM image of this cluster's architecture."
	reasonNoCluster         = "The cluster is not configured."
	reasonSyncFailed        = "Template sync failed due to a system error."
)

// statusReport is the body of PATCH /internal/templates/{templateId}/status; clusterId is required. Exists is a pointer
// so an observation always says true or false, and the other reports leave it out.
type statusReport struct {
	Report     string `json:"report"`
	ClusterId  string `json:"clusterId"`
	Exists     *bool  `json:"exists,omitempty"`
	ExternalId string `json:"externalId,omitempty"`
	Reason     string `json:"reason,omitempty"`
	Detail     string `json:"detail,omitempty"`
}

func sendStatus(ctx context.Context, cp *controlplane.Client, templateId string, report statusReport) error {
	return cp.Patch(ctx, "/internal/templates/"+url.PathEscape(templateId)+"/status", report)
}
