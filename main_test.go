package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/loop/reconcile"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

// The reconcile loop knows no resource; this is where every resource's reconciler must be registered.
func TestReconcilers_ObserveEveryResourceKind_OfEveryConfiguredCluster(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, "/internal/clusters", http.StatusOK, `[{"id":"mock","name":"Mock"},{"id":"gone","name":"Gone"}]`)
	clusters := cluster.Fixed{"mock": {Provider: testsupport.NewProvider()}}
	runner := reconcile.NewRunner(cp.Client(), clusters, reconcilers(), 0, slog.New(slog.NewTextHandler(io.Discard, nil)))

	runner.Cycle(context.Background())

	for _, kind := range []string{"vms", "networks", "firewall-rules", "templates", "volumes", "vm-snapshots", "load-balancers"} {
		if len(cp.RequestsTo("GET /internal/reconcile/"+kind+"?clusterId=mock")) != 1 {
			t.Errorf("%s of mock not reconciled", kind)
		}
		if len(cp.RequestsTo("GET /internal/reconcile/"+kind+"?clusterId=gone")) != 0 {
			t.Errorf("%s reconciled in a cluster it cannot reach", kind)
		}
	}
}
