package main

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

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
	if loaded.pollInterval != 2*time.Second || loaded.reconcileInterval != 2*time.Minute ||
		loaded.imageServer != "https://images.linuxcontainers.org" || loaded.healthAddr != ":8080" {
		t.Fatalf("defaults: %+v", loaded)
	}
}

func TestLoadConfig_BadDuration_Fails(t *testing.T) {
	_, err := loadConfig(lookupFrom(map[string]string{"CP_BASE_URL": "http://cp", "CP_INTERNAL_TOKEN": "t", "POLL_INTERVAL": "soon"}))

	if err == nil {
		t.Fatal("accepted POLL_INTERVAL=soon")
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

// A route with no kind yet is allowed: the worker ships a new kind before the control plane enqueues it, and keeps a
// removed one until the control plane has stopped
// (titan-ops architecture/provider-and-worker.md#contract-changes).
func TestRoutes_CoverTheControlPlanesKinds(t *testing.T) {
	routes := routes(nil, nil, "", backup.Target{})
	kinds := controlPlaneKinds(t)

	for _, kind := range kinds {
		route, found := routes[kind]
		if !found || route.Timeout <= 0 {
			t.Errorf("%s: no route, or a route without a timeout", kind)
		}
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

// A backup holds its loop for the whole upload; tenant commands must be leased by a loop that never runs one.
func TestBackupKinds_HaveALoopOfTheirOwn(t *testing.T) {
	tenant, backups := splitBackupRoutes(routes(nil, nil, "", backup.Target{}))

	names := []string{}
	for kind := range backups {
		names = append(names, kind)
	}
	sort.Strings(names)
	if strings.Join(names, ",") != "vm.backup,volume.backup" {
		t.Fatalf("backup loop: %v", names)
	}
	for kind := range tenant {
		if strings.HasSuffix(kind, ".backup") {
			t.Errorf("%s is leased by the tenant loop", kind)
		}
	}
	if len(tenant)+len(backups) != len(routes(nil, nil, "", backup.Target{})) {
		t.Fatalf("a route was lost: %d + %d", len(tenant), len(backups))
	}
	for kind, route := range backups {
		if route.Handler == nil || route.Timeout <= 0 {
			t.Errorf("%s: no handler or no timeout", kind)
		}
	}
}
