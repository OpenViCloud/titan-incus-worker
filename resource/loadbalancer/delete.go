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
	LoadBalancerId string `json:"loadBalancerId"`
	// RequestId names the request this command ends (titan-ops architecture/resource-lifecycle.md#callback-routes). Empty
	// on a command of the old control plane that the migration gave no id: it keeps the old reporting until rollout
	// phase 6 (titan-ops architecture/resource-lifecycle.md#rollout).
	RequestId         string `json:"requestId"`
	ProjectName       string `json:"projectName"`
	ExternalNetworkId string `json:"externalNetworkId"`
	Vip               string `json:"vip"`
}

// Delete runs load-balancer.delete: the VIP leaves its network; one already gone is done. It ends its request Done
// (Deleted on the old route; only this command reports it, titan-ops architecture/desired-and-current.md#forget-on-provider-confirm).
// Giving up ends it Failed, which gives the load balancer back
// (titan-ops architecture/desired-and-current.md#abandoned-request), unless Incus no longer has the VIP (RequestFailed
// on the old route, which reads nothing).
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
	if payload.RequestId == "" {
		return sendStatus(ctx, h.cp, payload.LoadBalancerId, statusReport{Report: reportDeleted})
	}
	return endRequest(ctx, h.cp, payload.LoadBalancerId, payload.RequestId, done(""))
}

func (h *Delete) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	var payload deletePayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	if payload.LoadBalancerId == "" {
		return command.Permanent(errors.New("load-balancer.delete payload lacks loadBalancerId: nobody to tell"))
	}
	if payload.RequestId == "" {
		report := statusReport{Report: reportRequestFailed, Reason: reasonDeleteFailed, Detail: cause.Error()}
		return sendStatus(ctx, h.cp, payload.LoadBalancerId, report)
	}
	gone, err := h.vipIsGone(ctx, leased.ClusterId, payload)
	if err != nil {
		return err
	}
	// The delete is the only write, and a 404 read proves it happened: the request is Done
	// (titan-ops architecture/resource-lifecycle.md#callback-routes).
	if gone {
		logger.Info("already gone; the give-up ends the request done", "object", payload.Vip)
		return endRequest(ctx, h.cp, payload.LoadBalancerId, payload.RequestId, done(""))
	}
	return endRequest(ctx, h.cp, payload.LoadBalancerId, payload.RequestId, failed("", reasonDeleteFailed, cause))
}

// vipIsGone is true only when Incus answers 404. A payload without what the read needs, or a cluster the worker cannot
// reach for good, tells nothing, which is not gone.
func (h *Delete) vipIsGone(ctx context.Context, clusterId string, payload deletePayload) (bool, error) {
	if payload.ProjectName == "" || payload.ExternalNetworkId == "" || payload.Vip == "" {
		return false, nil
	}
	exists, known, err := vipExists(ctx, h.clusters, clusterId, payload.ProjectName, payload.ExternalNetworkId, payload.Vip)
	return known && !exists, err
}
