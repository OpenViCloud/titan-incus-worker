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

const forceStopTimeoutSeconds = 30

type deletePayload struct {
	VmId         string `json:"vmId"`
	RequestId    string `json:"requestId"`
	ExternalVmId string `json:"externalVmId"`
	ProjectName  string `json:"projectName"`
}

// Delete runs vm.delete and ends the request it names once Incus no longer has the instance
// (titan-ops architecture/desired-and-current.md#forget-on-provider-confirm), Failed when it gives up.
type Delete struct {
	cp       *controlplane.Client
	clusters cluster.Clusters
}

func NewDelete(cp *controlplane.Client, clusters cluster.Clusters) *Delete {
	return &Delete{cp: cp, clusters: clusters}
}

func (h *Delete) Run(ctx context.Context, leased command.Leased, logger *slog.Logger) error {
	var payload deletePayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	isComplete := payload.VmId != "" && payload.RequestId != "" && payload.ExternalVmId != "" && payload.ProjectName != ""
	if !isComplete {
		return command.Permanent(fmt.Errorf("vm.delete payload lacks vmId, requestId, externalVmId or projectName"))
	}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return err
	}
	if err := removeInstance(ctx, cl.Provider, payload.ProjectName, payload.ExternalVmId, logger); err != nil {
		return err
	}
	return sendRequestEnd(ctx, h.cp, payload.VmId, payload.RequestId, requestEnd{Outcome: controlplane.OutcomeDone})
}

func (h *Delete) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	var payload deletePayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	if payload.RequestId == "" {
		logger.Warn("no request id; nothing to end")
		return nil
	}
	if payload.VmId == "" {
		return command.Permanent(fmt.Errorf("vm.delete payload lacks vmId: nobody to tell"))
	}
	return sendRequestEnd(ctx, h.cp, payload.VmId, payload.RequestId, failedEnd("", reasonDeleteFailed, cause))
}

// removeInstance stops then deletes; Incus refuses to delete a running instance (lab 05). A 404 at any step — the
// instance or its whole project gone — means the delete is already done.
func removeInstance(ctx context.Context, p provider.Provider, project string, name string, logger *slog.Logger) error {
	instance, err := p.GetInstance(ctx, project, name)
	if provider.IsNotFound(err) {
		logger.Info("already gone; nothing to delete", "object", name)
		return nil
	}
	if err != nil {
		return err
	}

	isStopped := instance.StatusCode == api.Stopped
	if !isStopped {
		stop := api.InstanceStatePut{Action: "stop", Force: true, Timeout: forceStopTimeoutSeconds}
		err := p.UpdateInstanceState(ctx, project, name, stop)
		if provider.IsNotFound(err) {
			logger.Info("already gone; nothing to delete", "object", name)
			return nil
		}
		isAlreadyStopped := provider.IsOperationFailureContaining(err, alreadyStopped)
		if err != nil && !isAlreadyStopped {
			return err
		}
	}

	err = p.DeleteInstance(ctx, project, name)
	if provider.IsNotFound(err) {
		logger.Info("already gone; nothing to delete", "object", name)
		return nil
	}
	return err
}
