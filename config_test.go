package main

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/resource/backup"
)

func lookupFrom(values map[string]string) func(string) string {
	return func(key string) string {
		return values[key]
	}
}

func TestLoadConfig_WithoutTheControlPlane_Fails(t *testing.T) {
	for _, values := range []map[string]string{
		{},
		{"CP_BASE_URL": "http://cp"},
		{"CP_INTERNAL_TOKEN": "t"},
	} {
		if _, err := loadConfig(lookupFrom(values)); err == nil {
			t.Errorf("%v: started without the control plane", values)
		}
	}
}

func TestLoadConfig_Defaults(t *testing.T) {
	loaded, err := loadConfig(lookupFrom(map[string]string{"CP_BASE_URL": "http://cp", "CP_INTERNAL_TOKEN": "t"}))

	if err != nil {
		t.Fatal(err)
	}
	if loaded.pollInterval != 2*time.Second || loaded.reconcileInterval != 2*time.Minute || loaded.metricsInterval != time.Minute ||
		loaded.imageServer != "https://images.linuxcontainers.org" || loaded.healthAddr != ":8080" {
		t.Fatalf("defaults: %+v", loaded)
	}
}

func TestLoadConfig_BadDuration_Fails(t *testing.T) {
	for _, key := range []string{"POLL_INTERVAL", "RECONCILE_INTERVAL", "METRICS_INTERVAL"} {
		_, err := loadConfig(lookupFrom(map[string]string{"CP_BASE_URL": "http://cp", "CP_INTERNAL_TOKEN": "t", key: "soon"}))

		if err == nil {
			t.Errorf("accepted %s=soon", key)
		}
	}
}

func TestLoadConfig_ReadsTheMetricsInterval(t *testing.T) {
	loaded, err := loadConfig(lookupFrom(map[string]string{"CP_BASE_URL": "http://cp", "CP_INTERNAL_TOKEN": "t", "METRICS_INTERVAL": "30s"}))

	if err != nil || loaded.metricsInterval != 30*time.Second {
		t.Fatalf("err %v, metrics interval %s", err, loaded.metricsInterval)
	}
}

// The control plane's kinds, read from its C# source as copied by scripts/sync-kinds.sh rather than retyped: a kind
// added there and not routed here would stay Pending forever
// (titan-ops architecture/provider-and-worker.md#provider-command-table).
const providerCommandKindsSource = "contract/ProviderCommandKinds.cs"

var kindConstant = regexp.MustCompile(`public const string \w+ = "([^"]+)";`)

func controlPlaneKinds(t *testing.T) []string {
	t.Helper()
	source, err := os.ReadFile(providerCommandKindsSource)
	if err != nil {
		t.Fatalf("read %s: %v", providerCommandKindsSource, err)
	}
	kinds := []string{}
	for _, match := range kindConstant.FindAllStringSubmatch(string(source), -1) {
		kinds = append(kinds, match[1])
	}
	if len(kinds) == 0 {
		t.Fatalf("no kind constant found in %s", providerCommandKindsSource)
	}
	return kinds
}

// isFunctionWorkerKind: the kinds of the function service, `function.*` and `function-runtime.*`, which the function
// worker leases and this worker never asks for (titan-ops architecture/provider-commands.md#provider-command-table).
func isFunctionWorkerKind(kind string) bool {
	return strings.HasPrefix(kind, "function.") || strings.HasPrefix(kind, "function-runtime.")
}

// uncoveredKinds: the control plane's kinds, other than the function worker's, that have no route with a timeout here.
func uncoveredKinds(routes map[string]command.Route, kinds []string) []string {
	uncovered := []string{}
	for _, kind := range kinds {
		if isFunctionWorkerKind(kind) {
			continue
		}
		if route, found := routes[kind]; !found || route.Timeout <= 0 {
			uncovered = append(uncovered, kind)
		}
	}
	return uncovered
}

// A route with no kind yet is allowed: the worker ships a new kind before the control plane enqueues it, and keeps a
// removed one until the control plane has stopped
// (titan-ops architecture/provider-and-worker.md#contract-changes).
func TestRoutes_CoverTheControlPlanesKinds(t *testing.T) {
	routes := routes(nil, nil, nil, "", backup.Target{})
	kinds := controlPlaneKinds(t)

	for _, kind := range uncoveredKinds(routes, kinds) {
		t.Errorf("%s: no route, or a route without a timeout", kind)
	}
}

// A kind of the function service is the function worker's, the one other worker that leases from the control plane:
// each kind belongs to exactly one worker (titan-ops architecture/provider-commands.md#provider-command-table).
func TestUncoveredKinds_LeavesTheFunctionKindsToTheFunctionWorker(t *testing.T) {
	routes := map[string]command.Route{"vm.create": {Timeout: time.Minute}}

	uncovered := uncoveredKinds(routes, []string{"vm.create", "function-runtime.sync", "function.deploy", "vm.delete"})

	if len(uncovered) != 1 || uncovered[0] != "vm.delete" {
		t.Fatalf("uncovered %v, want only vm.delete", uncovered)
	}
}

func TestRoutes_RouteNoKindOfTheFunctionWorker(t *testing.T) {
	for kind := range routes(nil, nil, nil, "", backup.Target{}) {
		if isFunctionWorkerKind(kind) {
			t.Errorf("%s: a kind of the function worker routed here", kind)
		}
	}
}

func TestIsFunctionWorkerKind_IsThePrefixOfTheFunctionService(t *testing.T) {
	for kind, want := range map[string]bool{
		"function.deploy": true, "function-runtime.sync": true, "function.": true,
		"functions.deploy": false, "function-secret.create": false, "vm.create": false, "template.sync": false,
	} {
		if got := isFunctionWorkerKind(kind); got != want {
			t.Errorf("%s: %v, want %v", kind, got, want)
		}
	}
}

// A stop may wait the longest stop timeout a VM takes, 1800 s, before it powers the VM off
// (titan-ops features/vm/design.md#vm-stop-timeout).
func TestRoutes_AStopOutlastsTheLongestStopTimeout(t *testing.T) {
	stop := routes(nil, nil, nil, "", backup.Target{})["vm.stop"]

	if stop.Timeout < 1800*time.Second+5*time.Minute {
		t.Fatalf("vm.stop gives up after %s", stop.Timeout)
	}
}

func TestLoadConfig_ReadsTheBackupTarget_AndStartsWithoutIt(t *testing.T) {
	loaded, err := loadConfig(lookupFrom(map[string]string{
		"CP_BASE_URL": "http://cp", "CP_INTERNAL_TOKEN": "t",
		"BACKUP_S3_URL": "https://r2", "BACKUP_S3_BUCKET": "b", "BACKUP_S3_ACCESS_KEY_ID": "k", "BACKUP_S3_SECRET_ACCESS_KEY": "s",
	}))
	if err != nil {
		t.Fatal(err)
	}
	want := backup.Target{Url: "https://r2", Bucket: "b", AccessKeyId: "k", SecretAccessKey: "s"}
	if loaded.backupTarget != want {
		t.Fatalf("target: %+v", loaded.backupTarget)
	}

	withoutTarget, err := loadConfig(lookupFrom(map[string]string{"CP_BASE_URL": "http://cp", "CP_INTERNAL_TOKEN": "t"}))
	if err != nil || len(withoutTarget.backupTarget.Missing()) != 4 {
		t.Fatalf("err %v, missing %v", err, withoutTarget.backupTarget.Missing())
	}
}

// A backup holds its loop for the whole upload, a drill or a restore from an archive for the whole import; tenant
// commands must be leased by a loop that never runs one.
func TestBackupKinds_HaveALoopOfTheirOwn(t *testing.T) {
	tenant, backups, imports := splitRoutes(routes(nil, nil, nil, "", backup.Target{}))

	names := []string{}
	for kind := range backups {
		names = append(names, kind)
	}
	sort.Strings(names)
	if strings.Join(names, ",") != "cluster.backup,vm.backup,volume.backup" {
		t.Fatalf("backup loop: %v", names)
	}
	// Rolling a volume back to a snapshot of its own imports no archive: it is a tenant's command like any other.
	const snapshotRestore = "volume-snapshot.restore"
	for kind := range tenant {
		isLong := strings.HasSuffix(kind, ".backup") || strings.HasSuffix(kind, ".drill") || strings.HasSuffix(kind, ".restore")
		if isLong && kind != snapshotRestore {
			t.Errorf("%s is leased by the tenant loop", kind)
		}
	}
	if tenant[snapshotRestore].Handler == nil {
		t.Errorf("%s left the tenant loop", snapshotRestore)
	}
	if len(tenant)+len(backups)+len(imports) != len(routes(nil, nil, nil, "", backup.Target{})) {
		t.Fatalf("a route was lost: %d + %d + %d", len(tenant), len(backups), len(imports))
	}
	for kind, route := range backups {
		if route.Handler == nil || route.Timeout <= 0 {
			t.Errorf("%s: no handler or no timeout", kind)
		}
	}
}

// Without the two variables a worker runs every loop (titan-ops architecture/worker-structure.md#worker-roles).
func TestLoadConfig_RunsEveryLoop_WithFourCommandSlots_ByDefault(t *testing.T) {
	loaded, err := loadConfig(lookupFrom(map[string]string{"CP_BASE_URL": "http://cp", "CP_INTERNAL_TOKEN": "t"}))

	if err != nil || loaded.role != roleAll || loaded.commandSlots != 4 {
		t.Fatalf("err %v, role %q, slots %d", err, loaded.role, loaded.commandSlots)
	}
}

func TestLoadConfig_ReadsTheRoleAndTheCommandSlots(t *testing.T) {
	for _, role := range []role{roleAll, roleCommands, roleInventory} {
		loaded, err := loadConfig(lookupFrom(map[string]string{"CP_BASE_URL": "http://cp", "CP_INTERNAL_TOKEN": "t",
			"WORKER_ROLE": string(role), "COMMAND_SLOTS": "7"}))

		if err != nil || loaded.role != role || loaded.commandSlots != 7 {
			t.Errorf("%s: err %v, role %q, slots %d", role, err, loaded.role, loaded.commandSlots)
		}
	}
}

// A typo must not start a pod that runs nothing, or every loop twice.
func TestLoadConfig_UnknownRoleOrBadSlots_Fails(t *testing.T) {
	for _, values := range []map[string]string{
		{"WORKER_ROLE": "reconciler"},
		{"WORKER_ROLE": "Commands"},
		{"COMMAND_SLOTS": "0"},
		{"COMMAND_SLOTS": "-1"},
		{"COMMAND_SLOTS": "four"},
	} {
		values["CP_BASE_URL"] = "http://cp"
		values["CP_INTERNAL_TOKEN"] = "t"

		if _, err := loadConfig(lookupFrom(values)); err == nil {
			t.Errorf("%v: started", values)
		}
	}
}

func TestLoadConfig_Restore_NeedsTheClusterId(t *testing.T) {
	_, err := loadConfig(lookupFrom(map[string]string{"CP_BASE_URL": "http://cp", "CP_INTERNAL_TOKEN": "t", "WORKER_ROLE": "restore"}))

	if err == nil {
		t.Fatal("started a restore without RESTORE_CLUSTER_ID")
	}
}

func TestLoadConfig_Restore_ReadsTheClusterIdAndPoint(t *testing.T) {
	loaded, err := loadConfig(lookupFrom(map[string]string{
		"CP_BASE_URL": "http://cp", "CP_INTERNAL_TOKEN": "t", "WORKER_ROLE": "restore",
		"RESTORE_CLUSTER_ID": "c1", "RESTORE_POINT": "20261118T120000Z",
	}))

	if err != nil || loaded.role != roleRestore || loaded.restoreClusterId != "c1" || loaded.restorePoint != "20261118T120000Z" {
		t.Fatalf("err %v, loaded %+v", err, loaded)
	}
}

func TestLoadConfig_Restore_WithoutAPoint_IsFine(t *testing.T) {
	loaded, err := loadConfig(lookupFrom(map[string]string{
		"CP_BASE_URL": "http://cp", "CP_INTERNAL_TOKEN": "t", "WORKER_ROLE": "restore", "RESTORE_CLUSTER_ID": "c1",
	}))

	if err != nil || loaded.restorePoint != "" {
		t.Fatalf("err %v, point %q", err, loaded.restorePoint)
	}
}

func TestLoadConfig_Restore_BadPoint_Fails(t *testing.T) {
	_, err := loadConfig(lookupFrom(map[string]string{
		"CP_BASE_URL": "http://cp", "CP_INTERNAL_TOKEN": "t", "WORKER_ROLE": "restore",
		"RESTORE_CLUSTER_ID": "c1", "RESTORE_POINT": "2026-11-18",
	}))

	if err == nil {
		t.Fatal("accepted a RESTORE_POINT not in backup.TimeLayout")
	}
}
