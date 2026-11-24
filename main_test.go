package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
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

// The command pod leases (customer commands, backups, and the drills and restores that import an archive) and sends no
// reconcile report; the inventory pod only reconciles and reads metrics, so each cluster is read once per cycle however
// many command pods run (titan-ops architecture/worker-structure.md#worker-roles).
func TestLoopsOf_EachRole_RunsItsLoopsOnly(t *testing.T) {
	all := loops{commands: namedRunner("commands"), backups: namedRunner("backups"), imports: namedRunner("imports"),
		reconciler: namedRunner("reconciler"), metrics: namedRunner("metrics")}
	want := map[role]string{
		roleAll:       "commands,backups,imports,reconciler,metrics",
		roleCommands:  "commands,backups,imports",
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

func kindsOf(routes map[string]command.Route) string {
	kinds := []string{}
	for kind := range routes {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	return strings.Join(kinds, ",")
}

// Each of the three lease loops leases its own kinds: a drill or a restore holds its loop for as long as it imports a
// disk, so it shares a loop with neither a tenant's command nor a backup; drills and restores wait for each other
// (titan-ops features/backup/design.md#restore-as-new-resource).
func TestSplitRoutes_BackupsAndImports_LeaveTheTenantLoop_EachOnItsOwn(t *testing.T) {
	all := routes(nil, cluster.Fixed{}, cluster.Fixed{}, "", backup.Target{})

	tenant, backups, imports := splitRoutes(all)

	for _, kind := range backupKinds {
		if backups[kind].Handler == nil || tenant[kind].Handler != nil || imports[kind].Handler != nil {
			t.Errorf("%s is not on the backup loop alone", kind)
		}
	}
	for _, kind := range importKinds {
		if imports[kind].Handler == nil || tenant[kind].Handler != nil || backups[kind].Handler != nil {
			t.Errorf("%s is not on the import loop alone", kind)
		}
	}
	if _, ok := tenant["vm.create"]; !ok {
		t.Error("vm.create left the tenant loop")
	}
	if kindsOf(imports) != "restore.drill,vm.restore,volume.restore" {
		t.Errorf("import loop: %s", kindsOf(imports))
	}
	// A whole disk goes through one import: hours for a large one.
	for kind, route := range imports {
		if route.Timeout != 6*time.Hour {
			t.Errorf("%s may run for %s, want six hours", kind, route.Timeout)
		}
	}
}

// askedClusters connects to nothing: it keeps the ids it was asked for.
type askedClusters struct {
	asked []string
}

func (a *askedClusters) Cluster(ctx context.Context, clusterId string) (*cluster.Cluster, error) {
	a.asked = append(a.asked, clusterId)
	return nil, errors.New("no cluster behind this test")
}

// An archive import is one request as long as the upload of a whole disk, and the client every other kind reaches its
// cluster with cuts a request at two minutes: a kind of the import loop is built over the resolver whose client does
// not (titan-ops features/backup/design.md#restore-as-new-resource).
func TestRoutes_AKindOfTheImportLoop_ReachesItsClusterWithoutTheRequestTimeout(t *testing.T) {
	payloads := map[string]string{
		"restore.drill": `{"kind":"volume","resourceId":"11111111-1111-4111-8111-111111111111","externalId":"vol-a",
			"projectName":"t-1","tenantId":"tenant-1"}`,
		"volume.restore": `{"newVolumeId":"22222222-2222-4222-8222-222222222222","requestId":"r-1","name":"copy",
			"sourceVolumeId":"11111111-1111-4111-8111-111111111111","backupKey":"20261123T010203Z","tenantId":"tenant-1",
			"projectName":"t-1","sizeGb":1}`,
		"vm.restore": `{"vmId":"44444444-4444-4444-8444-444444444444","requestId":"r-1","name":"copy",
			"sourceVmId":"33333333-3333-4333-8333-333333333333","backupKey":"20261123T010203Z","tenantId":"tenant-1",
			"projectName":"t-1","diskSizeGb":1}`,
	}

	for _, kind := range importKinds {
		payload, known := payloads[kind]
		if !known {
			t.Errorf("%s: no payload in this test, so the resolver it asks is not known", kind)
			continue
		}
		withTimeout, withoutTimeout := &askedClusters{}, &askedClusters{}
		route := routes(nil, withTimeout, withoutTimeout, "", backup.Target{})[kind]
		if route.Handler == nil {
			t.Errorf("%s has no route", kind)
			continue
		}

		route.Handler.Run(context.Background(), command.Leased{Kind: kind, ClusterId: "c1", Payload: []byte(payload)},
			testsupport.DiscardLogger())

		if !slices.Equal(withoutTimeout.asked, []string{"c1"}) || len(withTimeout.asked) != 0 {
			t.Errorf("%s asked the resolver without the request timeout for %v and the one with it for %v, want c1 of the first alone",
				kind, withoutTimeout.asked, withTimeout.asked)
		}
	}
}

// connectOf is the function a resolver connects with, read as a code pointer. Every resolver keeps it in a field named
// connect; one that does not stops the test, which then needs another way to find it.
func connectOf(t *testing.T, resolver reflect.Value) uintptr {
	t.Helper()
	if resolver.Kind() != reflect.Pointer || resolver.Type() != reflect.TypeFor[*cluster.Resolver]() || resolver.IsNil() {
		t.Fatalf("%v is no *cluster.Resolver", resolver.Type())
	}
	connect := resolver.Elem().FieldByName("connect")
	if !connect.IsValid() || connect.Kind() != reflect.Func {
		t.Fatalf("the resolver keeps no connect function in a field named connect")
	}
	return connect.Pointer()
}

func connectFunc(connect cluster.ConnectFunc) uintptr {
	return reflect.ValueOf(connect).Pointer()
}

// The import loop's own client is what lets an upload take hours and still ends a request that stops moving: the
// resolver the loop's kinds are built over connects with it, not with the cluster restore's, which limits no request
// once it has waited on an operation.
func TestImportClusters_ConnectWithTheClientMadeForTheImportLoop(t *testing.T) {
	resolver := importClusters(nil, testsupport.DiscardLogger())

	if connectOf(t, reflect.ValueOf(resolver)) != connectFunc(cluster.ConnectIncusForImports) {
		t.Error("the import loop's resolver does not connect with cluster.ConnectIncusForImports")
	}
}

// The cluster restore keeps the client it was measured with, which puts no limit of its own on a request. The import
// loop's ten minutes are not its limit.
func TestRestoreClusters_ConnectWithTheClientOfTheClusterRestore(t *testing.T) {
	resolver := restoreClusters(nil, testsupport.DiscardLogger())

	if connectOf(t, reflect.ValueOf(resolver)) != connectFunc(cluster.ConnectIncusWithoutRequestTimeout) {
		t.Error("the restore role's resolver does not connect with cluster.ConnectIncusWithoutRequestTimeout")
	}
}

// resolverOf is the resolver a handler was built over, as clustersOf finds it.
func resolverOf(t *testing.T, kind string, handler command.Handler) reflect.Value {
	t.Helper()
	clustersOf(t, kind, handler)
	return reflect.ValueOf(handler).Elem().FieldByName("clusters").Elem()
}

// What a worker runs is put together in one place: the kinds of the import loop over the resolver made for it, every
// other kind over the one whose client cuts a request at two minutes.
func TestCommandRoutes_BuildTheImportKindsOverTheImportLoopsResolver_AndEveryOtherKindOverTheOneGiven(t *testing.T) {
	clusters := &askedClusters{}

	tenant, backups, imports := commandRoutes(nil, clusters, config{}, testsupport.DiscardLogger())

	if kindsOf(imports) != "restore.drill,vm.restore,volume.restore" || kindsOf(backups) != "cluster.backup,vm.backup,volume.backup" || len(tenant) == 0 {
		t.Fatalf("import loop %q, backup loop %q, %d tenant kinds", kindsOf(imports), kindsOf(backups), len(tenant))
	}
	for kind, route := range imports {
		if connectOf(t, resolverOf(t, kind, route.Handler)) != connectFunc(cluster.ConnectIncusForImports) {
			t.Errorf("%s is not built over a resolver that connects with cluster.ConnectIncusForImports", kind)
		}
	}
	for _, loop := range []map[string]command.Route{tenant, backups} {
		for kind, route := range loop {
			if clustersOf(t, kind, route.Handler) != reflect.ValueOf(clusters).Pointer() {
				t.Errorf("%s is not built over the resolver every other loop uses", kind)
			}
		}
	}
}

// kindsLeasedBy polls once and answers the kinds the loop asked the control plane for. The fake control plane has no
// command to lease, so the poll ends there, with an error that is not this test's.
func kindsLeasedBy(t *testing.T, cp *testsupport.ControlPlane, loop runner) string {
	t.Helper()
	single, isALoop := loop.(*command.Loop)
	if !isALoop {
		t.Fatalf("%T is not one loop, which takes one command at a time", loop)
	}
	before := len(cp.RequestsTo("PUT /internal/provider-commands/leases/"))
	_, _ = single.PollOnce(context.Background())
	asked := cp.RequestsTo("PUT /internal/provider-commands/leases/")
	if len(asked) != before+1 {
		t.Fatalf("the poll asked for %d lease(s), want one", len(asked)-before)
	}
	var body struct {
		Kinds []string `json:"kinds"`
	}
	if err := json.Unmarshal([]byte(asked[before].Body), &body); err != nil {
		t.Fatalf("lease request %q: %v", asked[before].Body, err)
	}
	return strings.Join(body.Kinds, ",")
}

// A restore or a drill holds its loop for as long as it imports a disk: they run one at a time, on a loop that leases
// their kinds alone, and the backups on another. A pool there would run several imports at once.
func TestNewLoops_TheImportLoopAndTheBackupLoop_EachTakeOneCommandAtATime_OfTheirOwnKindsAlone(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	loaded := config{pollInterval: time.Hour, reconcileInterval: time.Hour, metricsInterval: time.Hour, commandSlots: 3}

	running := newLoops(cp.Client(), loaded, testsupport.DiscardLogger())

	if got := kindsLeasedBy(t, cp, running.imports); got != "restore.drill,vm.restore,volume.restore" {
		t.Errorf("the import loop leases %q", got)
	}
	if got := kindsLeasedBy(t, cp, running.backups); got != "cluster.backup,vm.backup,volume.backup" {
		t.Errorf("the backup loop leases %q", got)
	}
	if _, isAPool := running.commands.(*command.Pool); !isAPool {
		t.Errorf("the tenant's commands run on a %T, want the pool of command slots", running.commands)
	}
	if running.reconciler == nil || running.metrics == nil {
		t.Errorf("reconciler %v, metrics %v: a loop is missing", running.reconciler, running.metrics)
	}
}

// clustersOf is the resolver a handler was built over. Every handler keeps it in a field named clusters; one that does
// not stops the test, which then needs another way to find it.
func clustersOf(t *testing.T, kind string, handler command.Handler) uintptr {
	t.Helper()
	value := reflect.ValueOf(handler)
	if value.Kind() != reflect.Pointer || value.Elem().Kind() != reflect.Struct {
		t.Fatalf("%s: handler %T is no pointer to a struct", kind, handler)
	}
	field := value.Elem().FieldByName("clusters")
	if !field.IsValid() || field.Kind() != reflect.Interface || field.IsNil() || field.Elem().Kind() != reflect.Pointer {
		t.Fatalf("%s: handler %T keeps no resolver in a field named clusters", kind, handler)
	}
	return field.Elem().Pointer()
}

// The Incus client's calls take no context, so the two minutes its requests are cut at are all that ends a request a
// server never answers: no kind of the tenant loop or the backup loop gives them up.
func TestRoutes_NoKindOfTheTenantOrBackupLoop_GivesUpTheRequestTimeout(t *testing.T) {
	withTimeout, withoutTimeout := &askedClusters{}, &askedClusters{}

	tenant, backups, _ := splitRoutes(routes(nil, withTimeout, withoutTimeout, "", backup.Target{}))

	if len(tenant) == 0 || len(backups) == 0 {
		t.Fatalf("%d tenant kinds and %d backup kinds: nothing to check", len(tenant), len(backups))
	}
	for _, loop := range []map[string]command.Route{tenant, backups} {
		for kind, route := range loop {
			if clustersOf(t, kind, route.Handler) != reflect.ValueOf(withTimeout).Pointer() {
				t.Errorf("%s is built over the resolver without the request timeout", kind)
			}
		}
	}
}
