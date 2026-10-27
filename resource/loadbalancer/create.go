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

// Create runs load-balancer.create: the VIP on the network with the asked configuration, then Created with the VIP as
// the provider id.
type Create struct {
	cp       *controlplane.Client
	clusters cluster.Clusters
}

func NewCreate(cp *controlplane.Client, clusters cluster.Clusters) *Create {
	return &Create{cp: cp, clusters: clusters}
}

func (h *Create) Run(ctx context.Context, leased command.Leased, logger *slog.Logger) error {
	var payload specPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	if err := payload.check(); err != nil {
		return err
	}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return err
	}
	put, err := incusSpec(ctx, cl.Provider, payload)
	if err != nil {
		return err
	}
	if err := apply(ctx, cl.Provider, payload, put); err != nil {
		return err
	}
	return sendStatus(ctx, h.cp, payload.LoadBalancerId, statusReport{Report: reportCreated, ExternalId: payload.Vip})
}

// GiveUp tells the control plane the create gave up; if Incus has the VIP anyway, the report carries it and the
// create counts as done. A failed read sends no id: the control plane keeps the create failed.
func (h *Create) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	var payload specPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	if payload.LoadBalancerId == "" {
		return command.Permanent(errors.New("load-balancer.create payload lacks loadBalancerId: nobody to tell"))
	}
	report := statusReport{Report: reportRequestFailed, Reason: reasonCreateFailed, Detail: cause.Error()}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err == nil {
		_, _, err = cl.Provider.GetNetworkLoadBalancer(ctx, payload.ProjectName, payload.ExternalNetworkId, payload.Vip)
		if err == nil {
			report.ExternalId = payload.Vip
		} else if !provider.IsNotFound(err) {
			logger.Warn("could not read the load balancer while giving up; reporting it not created", "error", err)
		}
	}
	return sendStatus(ctx, h.cp, payload.LoadBalancerId, report)
}
