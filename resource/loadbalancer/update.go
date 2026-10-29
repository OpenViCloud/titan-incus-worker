package loadbalancer

import (
	"context"
	"errors"
	"log/slog"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
)

// Update runs load-balancer.update: the whole asked configuration in one write, then Updated with the intent it ends.
// A VIP Incus lost is made again rather than left failing.
type Update struct {
	cp       *controlplane.Client
	clusters cluster.Clusters
}

func NewUpdate(cp *controlplane.Client, clusters cluster.Clusters) *Update {
	return &Update{cp: cp, clusters: clusters}
}

func (h *Update) Run(ctx context.Context, leased command.Leased, logger *slog.Logger) error {
	var payload specPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	if err := payload.check(); err != nil {
		return err
	}
	if payload.IntentId == "" {
		return command.Permanent(errors.New("load-balancer.update payload lacks intentId"))
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
	return sendStatus(ctx, h.cp, payload.LoadBalancerId, statusReport{Report: reportUpdated, IntentId: payload.IntentId})
}

// GiveUp names the intent, so a give-up that a delete has since replaced is read as stale
// (titan-ops features/load-balancer/design.md#lb-lifecycle).
func (h *Update) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	var payload specPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	if payload.LoadBalancerId == "" || payload.IntentId == "" {
		return command.Permanent(errors.New("load-balancer.update payload lacks loadBalancerId or intentId: nobody to tell"))
	}
	report := statusReport{Report: reportRequestFailed, IntentId: payload.IntentId, Reason: reasonUpdateFailed, Detail: cause.Error()}
	return sendStatus(ctx, h.cp, payload.LoadBalancerId, report)
}
