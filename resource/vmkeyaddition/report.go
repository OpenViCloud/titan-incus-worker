// Package vmkeyaddition runs vm-key-addition.create: it appends keys to the sshd key file a VM's vendor-data writes,
// writing the sshd config too when the VM predates it. The addition is a child of the VM, like a VmSnapshot
// (titan-ops architecture/resource-lifecycle.md#callback-routes): its own request, independent of the VM's status.
package vmkeyaddition

import (
	"context"
	"net/url"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
)

// The tenant reads one of these fixed reasons; the raw error goes to the admin-only detail
// (titan-ops architecture/desired-and-current.md#request-failure).
const (
	reasonCreateFailed = "Adding the key failed due to a system error. Please try again or contact support."
	// reasonAgentNotRunning: the give-up's last error was the guest agent not up yet (titan-ops
	// features/vm/design.md#vm-key-addition), the one case measured on dev where the cause is worth naming.
	reasonAgentNotRunning = "The VM is still starting, so the key could not be added yet. Try again in a few minutes."
)

// RequestEnd is the body of PATCH /internal/vms/{vmId}/key-additions/{keyAdditionId}/requests/{requestId}
// (EndVmKeyAdditionRequestEndpoint.cs, control plane branch feat/vm-key-additions — titan-ops
// architecture/resource-lifecycle.md#callback-routes). RestartNeeded is a pointer so Done always carries it (true or
// false), while Failed carries none.
type RequestEnd struct {
	Outcome       string `json:"outcome"`
	RestartNeeded *bool  `json:"restartNeeded,omitempty"`
	Reason        string `json:"reason,omitempty"`
	Detail        string `json:"detail,omitempty"`
}

func done(restartNeeded bool) RequestEnd {
	return RequestEnd{Outcome: controlplane.OutcomeDone, RestartNeeded: &restartNeeded}
}

func failed(cause error) RequestEnd {
	reason := reasonCreateFailed
	if provider.IsAgentNotRunning(cause) {
		reason = reasonAgentNotRunning
	}
	return RequestEnd{Outcome: controlplane.OutcomeFailed, Reason: reason, Detail: controlplane.Detail(cause)}
}

func endRequest(ctx context.Context, cp *controlplane.Client, vmId string, keyAdditionId string, requestId string,
	end RequestEnd) error {
	return cp.Patch(ctx, controlplane.RequestPath(keyAdditionPath(vmId, keyAdditionId), requestId), end)
}

func keyAdditionPath(vmId string, keyAdditionId string) string {
	return "/internal/vms/" + url.PathEscape(vmId) + "/key-additions/" + url.PathEscape(keyAdditionId)
}
