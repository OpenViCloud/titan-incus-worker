package loadbalancer

import (
	"context"
	"errors"
	"log/slog"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
)

type deletePayload struct {
	LoadBalancerId    string `json:"loadBalancerId"`
	ProjectName       string `json:"projectName"`
	ExternalNetworkId string `json:"externalNetworkId"`
	Vip               string `json:"vip"`
}

// Delete runs load-balancer.delete: the VIP leaves its network; one already gone is done. Only this command reports
// Deleted (https://github.com/OpenViCloud/titan-ops/blob/main/docs/architecture/desired-and-current.md#forget-on-provider-confirm).
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
	isComplete := payload.LoadBalancerId != "" && payload.ProjectName != "" && payload.ExternalNetworkId != "" && payload.Vip != ""
	if !isComplete {
		return command.Permanent(errors.New("load-balancer.delete payload lacks loadBalancerId, projectName, externalNetworkId or vip"))
	}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return err
	}
	err = cl.Provider.DeleteNetworkLoadBalancer(ctx, payload.ProjectName, payload.ExternalNetworkId, payload.Vip)
	alreadyGone := provider.IsNotFound(err)
	if err != nil && !alreadyGone {
		return err
	}
	if alreadyGone {
		logger.Info("already gone; nothing to delete", "object", payload.Vip)
	}
	return sendStatus(ctx, h.cp, payload.LoadBalancerId, statusReport{Report: reportDeleted})
}

// GiveUp gives the load balancer back to the tenant (https://github.com/OpenViCloud/titan-ops/blob/main/docs/architecture/desired-and-current.md#abandoned-request).
func (h *Delete) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	var payload deletePayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	if payload.LoadBalancerId == "" {
		return command.Permanent(errors.New("load-balancer.delete payload lacks loadBalancerId: nobody to tell"))
	}
	report := statusReport{Report: reportRequestFailed, Reason: reasonDeleteFailed, Detail: cause.Error()}
	return sendStatus(ctx, h.cp, payload.LoadBalancerId, report)
}
