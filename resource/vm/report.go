// Package vm runs the VM provider commands and the cluster's VM reconciler.
package vm

import (
	"context"
	"log/slog"
	"net/url"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
)

// The words of the old route, UpdateVmStatusCommandHandler. Observed goes no further than DecideReport and the log:
// sendObservation does not send it. An observation carries the Incus status word for word
// (titan-ops architecture/desired-and-current.md#provider-state-is-verbatim); whether the instance exists is its own field
// (titan-ops architecture/desired-and-current.md#existence-axis), and a give-up is a request failure, not a state
// (titan-ops architecture/desired-and-current.md#request-failure).
const (
	reportObserved      = "Observed"
	reportCreated       = "Created"
	reportRequestFailed = "RequestFailed"
	reportDeleted       = "Deleted"
)

// Reason is returned verbatim to the tenant, so it carries only these fixed texts; the raw error goes to the
// admin-only detail (titan-ops features/public-api/design.md#status-reason).
const (
	reasonProvisionFailed = "VM provisioning failed due to a system error. Please try again or contact support."
	reasonStartFailed     = "The VM could not be started. Please try again or contact support."
	reasonStopFailed      = "The VM could not be stopped. Please try again or contact support."
	reasonDeleteFailed    = "The VM could not be deleted. Please try again or contact support."
)

// requestEnd is the body of PATCH /internal/vms/{vmId}/requests/{requestId}: the report that ends the request it names
// (titan-ops architecture/resource-lifecycle.md#callback-routes).
type requestEnd struct {
	Outcome    string        `json:"outcome"`
	ExternalId string        `json:"externalId,omitempty"`
	Observed   *observedBody `json:"observed,omitempty"`
	Reason     string        `json:"reason,omitempty"`
	Detail     string        `json:"detail,omitempty"`
}

// observedBody is the instance as the worker read it.
type observedBody struct {
	Status    string `json:"status"`
	IpAddress string `json:"ipAddress,omitempty"`
	// Left out when the VM has none: the control plane records that as no public IPv6.
	PublicIpv6Address string `json:"publicIpv6Address,omitempty"`
}

// observationBody is the body of PATCH /internal/vms/{vmId}/observation.
type observationBody struct {
	Exists   bool          `json:"exists"`
	Observed *observedBody `json:"observed,omitempty"`
}

func sendRequestEnd(ctx context.Context, cp *controlplane.Client, vmId string, requestId string, end requestEnd) error {
	return cp.Patch(ctx, controlplane.RequestPath(vmPath(vmId), requestId), end)
}

func sendObservation(ctx context.Context, cp *controlplane.Client, vmId string, report statusReport) error {
	body := observationBody{Exists: report.Exists != nil && *report.Exists}
	if body.Exists {
		body.Observed = &observedBody{Status: report.Status, IpAddress: report.IpAddress, PublicIpv6Address: report.PublicIpv6Address}
	}
	return cp.Patch(ctx, controlplane.ObservationPath(vmPath(vmId)), body)
}

func vmPath(vmId string) string {
	return "/internal/vms/" + url.PathEscape(vmId)
}

// readObserved reads the instance once after a command did its work. A read that fails does not undo that work, so the
// report goes without it and the reconciler reports the instance next cycle.
func readObserved(ctx context.Context, p provider.Provider, project string, name string, logger *slog.Logger) *observedBody {
	instance, err := p.GetInstanceFull(ctx, project, name)
	if err != nil {
		logger.Warn("instance not read after its command; the reconciler reports it", "object", name, "error", err)
		return nil
	}
	return &observedBody{Status: instance.Status, IpAddress: firstGlobalIpv4(instance.State), PublicIpv6Address: publicIpv6(instance.State)}
}

func failedEnd(externalId string, reason string, cause error) requestEnd {
	return requestEnd{Outcome: controlplane.OutcomeFailed, ExternalId: externalId, Reason: reason, Detail: controlplane.Detail(cause)}
}

// statusReport is the body of the old PATCH /internal/vms/{vmId}/status, which a command without a request id still
// uses, and what DecideReport hands to sendObservation.
type statusReport struct {
	Report            string `json:"report"`
	Exists            *bool  `json:"exists,omitempty"`
	Status            string `json:"status,omitempty"`
	ExternalId        string `json:"externalId,omitempty"`
	IpAddress         string `json:"ipAddress,omitempty"`
	PublicIpv6Address string `json:"publicIpv6Address,omitempty"`
	Reason            string `json:"reason,omitempty"`
	Detail            string `json:"detail,omitempty"`
}

func sendStatus(ctx context.Context, cp *controlplane.Client, vmId string, report statusReport) error {
	return cp.Patch(ctx, "/internal/vms/"+url.PathEscape(vmId)+"/status", report)
}
