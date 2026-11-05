package vm

import (
	"context"
	"fmt"
	"log/slog"

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
	RequestId    string `json:"requestId"`
	ExternalVmId string `json:"externalVmId"`
	ProjectName  string `json:"projectName"`
}

// Action runs vm.stop or vm.start and ends the request it names, Done with what it read of the instance, Failed when it
// gives up (titan-ops architecture/resource-lifecycle.md#callback-routes).
type Action struct {
	cp          *controlplane.Client
	clusters    cluster.Clusters
	state       api.InstanceStatePut
	alreadyDone string
	reason      string
	// Incus counts a Frozen instance as running: action "start" answers "already running" and leaves it Frozen,
	// so a start reads the instance first and resumes a Frozen one with "unfreeze"
	// (titan-ops features/vm/design.md#requests-follow-incus).
	unfreezesFrozen bool
}

func NewStop(cp *controlplane.Client, clusters cluster.Clusters) *Action {
	stop := api.InstanceStatePut{Action: "stop", Force: true, Timeout: forceStopTimeoutSeconds}
	return &Action{cp: cp, clusters: clusters, state: stop, alreadyDone: alreadyStopped, reason: reasonStopFailed}
}

func NewStart(cp *controlplane.Client, clusters cluster.Clusters) *Action {
	start := api.InstanceStatePut{Action: "start"}
	return &Action{cp: cp, clusters: clusters, state: start, alreadyDone: alreadyRunning, reason: reasonStartFailed, unfreezesFrozen: true}
}

func (h *Action) Run(ctx context.Context, leased command.Leased, logger *slog.Logger) error {
	var payload actionPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	isComplete := payload.VmId != "" && payload.RequestId != "" && payload.ExternalVmId != "" && payload.ProjectName != ""
	if !isComplete {
		return command.Permanent(fmt.Errorf("vm.%s payload lacks vmId, requestId, externalVmId or projectName", h.state.Action))
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
	if err != nil {
		if provider.IsNotFound(err) {
			return command.Permanent(err)
		}
		if !provider.IsOperationFailureContaining(err, h.alreadyDone) {
			return err
		}
		logger.Info("already in the asked state", "object", payload.ExternalVmId, "action", state.Action)
	}
	observed := readObserved(ctx, cl.Provider, payload.ProjectName, payload.ExternalVmId, logger)
	return sendRequestEnd(ctx, h.cp, payload.VmId, payload.RequestId, requestEnd{Outcome: controlplane.OutcomeDone, Observed: observed})
}

func (h *Action) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	var payload actionPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	if payload.RequestId == "" {
		logger.Warn("no request id; nothing to end")
		return nil
	}
	if payload.VmId == "" {
		return command.Permanent(fmt.Errorf("vm.%s payload lacks vmId: nobody to tell", h.state.Action))
	}
	return sendRequestEnd(ctx, h.cp, payload.VmId, payload.RequestId, failedEnd("", h.reason, cause))
}
