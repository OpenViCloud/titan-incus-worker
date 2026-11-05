package loadbalancer

import (
	"context"
	"errors"
	"log/slog"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
)

// Create runs load-balancer.create: the VIP on the network with the asked configuration, then it ends the request Done
// with the VIP as the provider id.
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
	return endRequest(ctx, h.cp, payload.LoadBalancerId, payload.RequestId, done(payload.Vip))
}

// GiveUp ends the request Failed, with the VIP when Incus has it: then the create did happen, and its delete reaches
// Incus.
func (h *Create) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	var payload specPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	if payload.RequestId == "" {
		logger.Warn("no request id; nothing to end")
		return nil
	}
	if payload.LoadBalancerId == "" {
		return command.Permanent(errors.New("load-balancer.create payload lacks loadBalancerId: nobody to tell"))
	}
	externalId, err := h.existingVip(ctx, leased.ClusterId, payload)
	if err != nil {
		return err
	}
	return endRequest(ctx, h.cp, payload.LoadBalancerId, payload.RequestId, failed(externalId, reasonCreateFailed, cause))
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
