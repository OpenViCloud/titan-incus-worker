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

// Create runs load-balancer.create: the VIP on the network with the asked configuration, then it ends the request Done
// with the VIP as the provider id (Created on the old route).
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
	if payload.RequestId == "" {
		return sendStatus(ctx, h.cp, payload.LoadBalancerId, statusReport{Report: reportCreated, ExternalId: payload.Vip})
	}
	return endRequest(ctx, h.cp, payload.LoadBalancerId, payload.RequestId, done(payload.Vip))
}

// GiveUp ends the request Failed (RequestFailed on the old route), with the VIP when Incus has it: then the create did
// happen, and its delete reaches Incus.
func (h *Create) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	var payload specPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	if payload.LoadBalancerId == "" {
		return command.Permanent(errors.New("load-balancer.create payload lacks loadBalancerId: nobody to tell"))
	}
	if payload.RequestId == "" {
		return h.giveUpOnTheOldRoute(ctx, leased, payload, cause, logger)
	}
	externalId, err := h.existingVip(ctx, leased.ClusterId, payload)
	if err != nil {
		return err
	}
	return endRequest(ctx, h.cp, payload.LoadBalancerId, payload.RequestId, failed(externalId, reasonCreateFailed, cause))
}

// giveUpOnTheOldRoute keeps the old give-up: a VIP that cannot be read goes without its id, and the raw detail is sent
// whole.
func (h *Create) giveUpOnTheOldRoute(ctx context.Context, leased command.Leased, payload specPayload, cause error,
	logger *slog.Logger) error {
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

// existingVip returns the VIP when Incus has it, "" when it does not, when the payload lacks what the read needs, or
// when the worker cannot reach the cluster for good; a read that fails is the error.
func (h *Create) existingVip(ctx context.Context, clusterId string, payload specPayload) (string, error) {
	if payload.ProjectName == "" || payload.ExternalNetworkId == "" || payload.Vip == "" {
		return "", nil
	}
	exists, _, err := vipExists(ctx, h.clusters, clusterId, payload.ProjectName, payload.ExternalNetworkId, payload.Vip)
	if err != nil || !exists {
		return "", err
	}
	return payload.Vip, nil
}
