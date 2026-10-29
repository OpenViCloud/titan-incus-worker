package loadbalancer

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
)

// The CurrentExistence values Reconcile reads.
const (
	existenceExists   = "Exists"
	existenceNotFound = "NotFound"
)

// ReconcilableLoadBalancer mirrors ReconcilableLoadBalancerQueryResult: where to find it on Incus, and what Titan last
// recorded, so only a difference is reported.
type ReconcilableLoadBalancer struct {
	Id                string `json:"id"`
	ProjectName       string `json:"projectName"`
	ExternalNetworkId string `json:"externalNetworkId"`
	Vip               string `json:"vip"`
	CurrentExistence  string `json:"currentExistence"`
}

// Reconcile observes one cluster's load balancers — one read per network — and reports what differs; it never acts on
// Incus. A network Incus no longer has holds none. A failed read of a network skips that network this cycle: reading
// it as "none" would report every one of them gone. Absent is NotFound, never Deleted
// (titan-ops architecture/desired-and-current.md#existence-axis).
func Reconcile(ctx context.Context, cp *controlplane.Client, p provider.Provider, clusterId string, logger *slog.Logger) error {
	var rows []ReconcilableLoadBalancer
	if err := cp.GetJSON(ctx, "/internal/reconcile/load-balancers?clusterId="+url.QueryEscape(clusterId), &rows); err != nil {
		return fmt.Errorf("list reconcilable load balancers of cluster %s: %w", clusterId, err)
	}

	type networkKey struct{ project, network string }
	present := map[networkKey]map[string]bool{}
	for _, row := range rows {
		key := networkKey{row.ProjectName, row.ExternalNetworkId}
		if _, read := present[key]; read {
			continue
		}
		loadBalancers, err := p.ListNetworkLoadBalancers(ctx, row.ProjectName, row.ExternalNetworkId)
		if provider.IsNotFound(err) {
			present[key] = map[string]bool{}
			continue
		}
		if err != nil {
			logger.Warn("could not read the load balancers of a network; next cycle reads it again",
				"network", row.ExternalNetworkId, "project", row.ProjectName, "error", err)
			present[key] = nil
			continue
		}
		vips := map[string]bool{}
		for _, loadBalancer := range loadBalancers {
			vips[loadBalancer.ListenAddress] = true
		}
		present[key] = vips
	}

	for _, row := range rows {
		vips := present[networkKey{row.ProjectName, row.ExternalNetworkId}]
		if vips == nil {
			continue
		}
		exists := vips[row.Vip]
		alreadyRecorded := (exists && row.CurrentExistence == existenceExists) ||
			(!exists && row.CurrentExistence == existenceNotFound)
		if alreadyRecorded {
			continue
		}
		if err := sendStatus(ctx, cp, row.Id, statusReport{Report: reportObserved, Exists: &exists}); err != nil {
			logger.Warn("load balancer report failed; next cycle sends it again", "loadBalancerId", row.Id, "error", err)
			continue
		}
		logger.Info("reconcile report sent", "loadBalancerId", row.Id, "recordedExistence", row.CurrentExistence, "exists", exists)
	}
	return nil
}
