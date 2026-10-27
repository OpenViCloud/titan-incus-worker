package vm

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
)

// The operation errors Incus gives an action that is already done (lab 05).
const (
	alreadyStopped = "already stopped"
	alreadyRunning = "already running"
)

type actionPayload struct {
	VmId         string `json:"vmId"`
	IntentId     string `json:"intentId"`
	ExternalVmId string `json:"externalVmId"`
	ProjectName  string `json:"projectName"`
}

// Action runs vm.stop or vm.start. Success reports nothing: the reconciler observes Stopped/Running. Giving up
// withdraws the intent (https://github.com/OpenViCloud/titan-ops/blob/main/docs/features/vm/design.md#intent-failed-callback).
type Action struct {
	cp          *controlplane.Client
	clusters    cluster.Clusters
	state       api.InstanceStatePut
	alreadyDone string
	// Incus counts a Frozen instance as running: action "start" answers "already running" and leaves it Frozen,
	// so a start reads the instance first and resumes a Frozen one with "unfreeze"
	// (https://github.com/OpenViCloud/titan-ops/blob/main/docs/features/vm/design.md#requests-follow-incus).
	unfreezesFrozen bool
}

func NewStop(cp *controlplane.Client, clusters cluster.Clusters) *Action {
	stop := api.InstanceStatePut{Action: "stop", Force: true, Timeout: forceStopTimeoutSeconds}
	return &Action{cp: cp, clusters: clusters, state: stop, alreadyDone: alreadyStopped}
}

func NewStart(cp *controlplane.Client, clusters cluster.Clusters) *Action {
	start := api.InstanceStatePut{Action: "start"}
	return &Action{cp: cp, clusters: clusters, state: start, alreadyDone: alreadyRunning, unfreezesFrozen: true}
}

func (h *Action) Run(ctx context.Context, leased command.Leased, logger *slog.Logger) error {
	var payload actionPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	isComplete := payload.VmId != "" && payload.ExternalVmId != "" && payload.ProjectName != ""
	if !isComplete {
		return command.Permanent(fmt.Errorf("vm.%s payload lacks vmId, externalVmId or projectName", h.state.Action))
	}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return err
	}

	state := h.state
	if h.unfreezesFrozen {
		instance, err := cl.Provider.GetInstance(ctx, payload.ProjectName, payload.ExternalVmId)
		if err != nil {
			if provider.IsNotFound(err) {
				return command.Permanent(err)
			}
			return err
		}
		if instance.StatusCode == api.Frozen {
			state = api.InstanceStatePut{Action: "unfreeze"}
		}
	}

	err = cl.Provider.UpdateInstanceState(ctx, payload.ProjectName, payload.ExternalVmId, state)
	if err == nil {
		return nil
	}
	if provider.IsOperationFailureContaining(err, h.alreadyDone) {
		logger.Info("already in the asked state", "object", payload.ExternalVmId, "action", state.Action)
		return nil
	}
	if provider.IsNotFound(err) {
		return command.Permanent(err)
	}
	return err
}

func (h *Action) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	var payload actionPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	isComplete := payload.VmId != "" && payload.IntentId != ""
	if !isComplete {
		return command.Permanent(fmt.Errorf("vm.%s payload lacks vmId or intentId: no intent to withdraw", h.state.Action))
	}
	path := "/internal/vms/" + url.PathEscape(payload.VmId) + "/intents/" + url.PathEscape(payload.IntentId) + "/failed"
	return h.cp.Patch(ctx, path, nil)
}
