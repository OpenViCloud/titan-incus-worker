// Package reconcile is the reconciler loop: every interval, each kind in each cluster the control plane configures
// (https://github.com/OpenViCloud/titan-ops/blob/main/docs/architecture/provider-and-worker.md#reconciler-is-a-worker-ticker). An error skips that cluster and kind for the
// cycle; the loop never exits on one.
package reconcile

import (
	"context"
	"log/slog"
	"time"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
)

// clusterInfo mirrors the control plane's InternalClusterResponse on GET /internal/clusters.
type clusterInfo struct {
	Id   string `json:"id"`
	Name string `json:"name"`
}

// Kind is one resource's reconciler. main.go registers them, so this loop knows no resource.
type Kind struct {
	Name      string
	Reconcile func(ctx context.Context, cp *controlplane.Client, cl *cluster.Cluster, clusterId string, logger *slog.Logger) error
}

type Runner struct {
	cp       *controlplane.Client
	clusters cluster.Clusters
	interval time.Duration
	logger   *slog.Logger
	// kinds run in this order in each cluster.
	kinds []Kind
}

func NewRunner(cp *controlplane.Client, clusters cluster.Clusters, kinds []Kind, interval time.Duration, logger *slog.Logger) *Runner {
	return &Runner{cp: cp, clusters: clusters, interval: interval, logger: logger, kinds: kinds}
}

// Run reconciles at once, then every interval, until ctx ends.
func (r *Runner) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		r.Cycle(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Cycle runs every kind once in every cluster.
func (r *Runner) Cycle(ctx context.Context) {
	var clusters []clusterInfo
	if err := r.cp.GetJSON(ctx, "/internal/clusters", &clusters); err != nil {
		if ctx.Err() == nil {
			r.logger.Warn("reconcile: listing clusters failed; skipping this cycle", "error", err)
		}
		return
	}
	for _, cl := range clusters {
		if ctx.Err() != nil {
			return
		}
		r.reconcileCluster(ctx, cl.Id)
	}
}

func (r *Runner) reconcileCluster(ctx context.Context, clusterId string) {
	cl, err := r.clusters.Cluster(ctx, clusterId)
	if err != nil {
		r.logger.Warn("reconcile: cannot reach the cluster; skipping it this cycle", "clusterId", clusterId, "error", err)
		return
	}

	// One line per cluster and cycle says how long each kind took: a kind stuck on a slow Incus read shows here.
	clusterStartedAt := time.Now()
	durations := []any{}
	for _, kind := range r.kinds {
		kindLogger := r.logger.With("kind", kind.Name, "clusterId", clusterId)
		kindStartedAt := time.Now()
		err := kind.Reconcile(ctx, r.cp, cl, clusterId, kindLogger)
		durations = append(durations, slog.Int64(kind.Name, time.Since(kindStartedAt).Milliseconds()))
		if err != nil && ctx.Err() == nil {
			kindLogger.Warn("reconcile failed; skipping it this cycle", "error", err)
		}
	}
	r.logger.Info("reconcile cluster done", "clusterId", clusterId, "totalMs", time.Since(clusterStartedAt).Milliseconds(),
		slog.Group("durationsMs", durations...))
}
