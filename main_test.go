package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/loop/reconcile"
	"github.com/OpenViCloud/titan-incus-worker/resource/backup"
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

// Metrics run on their own runner, every METRICS_INTERVAL: one read of each reachable cluster, and none of the state
// reconcilers (titan-ops features/vm/design.md#vm-metrics).
func TestMetricsReconcilers_ReadEachReachableClusterOnce_AndNothingElse(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, "/internal/clusters", http.StatusOK, `[{"id":"mock","name":"Mock"},{"id":"gone","name":"Gone"}]`)
	mock := testsupport.NewProvider()
	mock.Metrics = `incus_cpu_seconds_total{cpu="0",mode="user",name="vmaaaaaaaaaaaaaaaaaaaaaaaaaa",type="virtual-machine"} 1`
	clusters := cluster.Fixed{"mock": {Provider: mock}}
	runner := reconcile.NewRunner(cp.Client(), clusters, metricsReconcilers(), 0, slog.New(slog.NewTextHandler(io.Discard, nil)))

	runner.Cycle(context.Background())

	if mock.Calls != 1 {
		t.Errorf("want one Incus read of mock, got %d", mock.Calls)
	}
	if len(cp.RequestsTo("POST /internal/clusters/mock/vm-metrics")) != 1 || len(cp.RequestsTo("POST /internal/clusters/gone/")) != 0 {
		t.Errorf("want one batch, of mock only: %+v", cp.Requests())
	}
	if len(cp.RequestsTo("GET /internal/reconcile/")) != 0 {
		t.Errorf("the metrics runner ran a state reconciler: %+v", cp.Requests())
	}
	for _, kind := range reconcilers() {
		if kind.Name == "vm-metrics" {
			t.Error("vm-metrics also runs on the state reconciler's interval")
		}
	}
}

type namedRunner string

func (namedRunner) Run(context.Context) {}

// The command pod leases (customer commands, backups and restore drills) and sends no reconcile report; the inventory pod only
// reconciles and reads metrics, so each cluster is read once per cycle however many command pods run
// (titan-ops architecture/worker-structure.md#worker-roles).
func TestLoopsOf_EachRole_RunsItsLoopsOnly(t *testing.T) {
	all := loops{commands: namedRunner("commands"), backups: namedRunner("backups"), drills: namedRunner("drills"),
		reconciler: namedRunner("reconciler"), metrics: namedRunner("metrics")}
	want := map[role]string{
		roleAll:       "commands,backups,drills,reconciler,metrics",
		roleCommands:  "commands,backups,drills",
		roleInventory: "reconciler,metrics",
	}

	for role, names := range want {
		got := []string{}
		for _, running := range all.of(role) {
			got = append(got, string(running.(namedRunner)))
		}
		if strings.Join(got, ",") != names {
			t.Errorf("%s runs %v, want %s", role, got, names)
		}
	}
}

// Each of the three lease loops leases its own kinds: a drill holds its loop for as long as it imports a disk, so it
// shares a loop with neither a tenant's command nor a backup (titan-ops features/backup/design.md#restore-drill).
func TestSplitRoutes_BackupsAndDrills_LeaveTheTenantLoop_EachOnItsOwn(t *testing.T) {
	all := routes(nil, cluster.Fixed{}, "", backup.Target{})

	tenant, backups, drills := splitRoutes(all)

	for _, kind := range backupKinds {
		if _, ok := backups[kind]; !ok || tenant[kind].Handler != nil || drills[kind].Handler != nil {
			t.Errorf("%s is not on the backup loop alone", kind)
		}
	}
	for _, kind := range drillKinds {
		if _, ok := drills[kind]; !ok || tenant[kind].Handler != nil || backups[kind].Handler != nil {
			t.Errorf("%s is not on the drill loop alone", kind)
		}
	}
	if _, ok := tenant["vm.create"]; !ok {
		t.Error("vm.create left the tenant loop")
	}
	if len(drills) != 1 || drills["restore.drill"].Timeout != drillTimeout {
		t.Errorf("drill routes %+v, want restore.drill alone with the drill timeout", drills)
	}
}
