package restore

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/lxc/incus/v7/shared/api"
	"go.yaml.in/yaml/v4"

	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/resource/backup"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

const clusterId = "c1"

var configTakenAt = time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)

// fakeObjects is an in-memory Objects: a restore test writes archives and config objects to it directly, never
// building a real tar.gz or S3 call.
type fakeObjects struct {
	data map[string]string
}

func newFakeObjects() *fakeObjects {
	return &fakeObjects{data: map[string]string{}}
}

func (f *fakeObjects) put(key string, body string) {
	f.data[key] = body
}

func (f *fakeObjects) List(ctx context.Context, prefix string) ([]string, error) {
	keys := []string{}
	for key := range f.data {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (f *fakeObjects) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	body, found := f.data[key]
	if !found {
		return nil, fmt.Errorf("fake bucket: no object %s", key)
	}
	return io.NopCloser(strings.NewReader(body)), nil
}

// orderedProvider wraps the fake Provider to record the calls restore_test.go's order assertions care about, since
// testsupport.Provider's own Order/BackupOrder are for other packages' tests (resource/network, resource/backup) and
// are not to carry restore-only entries.
type orderedProvider struct {
	*testsupport.Provider
	order *[]string
}

func newOrderedProvider() (*orderedProvider, *[]string) {
	order := []string{}
	return &orderedProvider{Provider: testsupport.NewProvider(), order: &order}, &order
}

func (p *orderedProvider) ImportVolume(ctx context.Context, project string, pool string, name string, body io.Reader) error {
	*p.order = append(*p.order, "import-volume "+name)
	return p.Provider.ImportVolume(ctx, project, pool, name, body)
}

func (p *orderedProvider) ImportInstance(ctx context.Context, project string, pool string, name string, body io.Reader, overrides provider.InstanceOverrides) error {
	*p.order = append(*p.order, "import-instance "+name)
	if err := p.Provider.ImportInstance(ctx, project, pool, name, body, overrides); err != nil {
		return err
	}
	// An imported vm has the NIC its archive declares, as on Incus.
	p.Provider.Instances[project+"/"+name].Devices = map[string]map[string]string{
		"eth0": {"type": "nic", "network": "net-net1"},
	}
	return nil
}

func (p *orderedProvider) UpdateInstanceState(ctx context.Context, project string, name string, state api.InstanceStatePut) error {
	*p.order = append(*p.order, "start "+name)
	return p.Provider.UpdateInstanceState(ctx, project, name, state)
}

func (p *orderedProvider) PatchInstance(ctx context.Context, project string, name string, instance api.InstancePut) error {
	if eth0, ok := instance.Devices["eth0"]; ok {
		*p.order = append(*p.order, "pin-ipv4 "+name+" "+eth0["ipv4.address"])
	}
	return p.Provider.PatchInstance(ctx, project, name, instance)
}

func (p *orderedProvider) CreateNetworkLoadBalancer(ctx context.Context, project string, network string, loadBalancer api.NetworkLoadBalancersPost) error {
	*p.order = append(*p.order, "create-lb "+loadBalancer.ListenAddress)
	return p.Provider.CreateNetworkLoadBalancer(ctx, project, network, loadBalancer)
}

func logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func putConfig(t *testing.T, objects *fakeObjects, config backup.ClusterConfig) {
	t.Helper()
	body, err := yaml.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	objects.put(backup.ConfigObjectKey(clusterId, configTakenAt), string(body))
}

func archiveKeyFor(collection string, id string, takenAt time.Time) string {
	return backup.ObjectKey(clusterId, "any-tenant", collection, id, takenAt)
}

// setupTitanLists answers every reconcile list the restore reads, "[]" (or the empty storage shape) unless overrides
// names a body for that path; a test queues only the responses it cares about, by path exactly as Run() builds it.
func setupTitanLists(cp *testsupport.ControlPlane, overrides map[string]string) {
	defaults := map[string]string{
		"/internal/reconcile/vms?clusterId=" + clusterId:            "[]",
		"/internal/reconcile/volumes?clusterId=" + clusterId:        `{"volumes":[],"snapshots":[]}`,
		"/internal/reconcile/vm-snapshots?clusterId=" + clusterId:   "[]",
		"/internal/reconcile/networks?clusterId=" + clusterId:       "[]",
		"/internal/reconcile/firewall-rules?clusterId=" + clusterId: "[]",
		"/internal/reconcile/load-balancers?clusterId=" + clusterId: "[]",
		"/internal/reconcile/templates?clusterId=" + clusterId:      "[]",
	}
	for path, body := range defaults {
		if override, ok := overrides[path]; ok {
			body = override
		}
		cp.Respond(http.MethodGet, path, http.StatusOK, body)
	}
}

// baseConfig is one project with a default profile, an extra profile, one network with its ACL, and one load
// balancer on that network.
func baseConfig() backup.ClusterConfig {
	return backup.ClusterConfig{
		ClusterId: clusterId,
		TakenAt:   configTakenAt,
		Projects: []backup.ProjectConfig{
			{
				Project: api.Project{Name: "t-proj1", ProjectPut: api.ProjectPut{Config: map[string]string{"features.networks": "true"}}},
				Profiles: []api.Profile{
					{Name: "default", ProfilePut: api.ProfilePut{Devices: api.DevicesMap{"root": {"type": "disk", "pool": "default", "path": "/"}}}},
					{Name: "extra", ProfilePut: api.ProfilePut{Description: "extra profile"}},
				},
				Networks: []api.Network{
					{Name: "net-net1", Type: "ovn", NetworkPut: api.NetworkPut{
						Config: map[string]string{
							"ipv4.address":                  "10.0.0.1/24",
							"volatile.network.ipv6.address": "fd00:1::1",
							"volatile.last_state.ready":     "true",
						},
						Description: "network one",
					}},
					{Name: "net-net2", Type: "ovn", NetworkPut: api.NetworkPut{Config: map[string]string{"ipv4.address": "10.0.1.1/24"}, Description: "network two, gone"}},
				},
				NetworkAcls: []api.NetworkACL{
					{
						NetworkACLPost: api.NetworkACLPost{Name: "acl-net1"},
						NetworkACLPut: api.NetworkACLPut{
							Ingress: []api.NetworkACLRule{
								{Action: "allow", State: "enabled", Description: "rule-live"},
								{Action: "allow", State: "enabled", Description: "rule-dead"},
								{Action: "allow", State: "enabled"},
							},
						},
					},
				},
				LoadBalancers: []backup.NetworkLoadBalancers{
					{Network: "net-net1", LoadBalancers: []api.NetworkLoadBalancer{
						{ListenAddress: "fd00:1::10", NetworkLoadBalancerPut: api.NetworkLoadBalancerPut{Description: "lb"}},
					}},
				},
				InstanceAddresses: []backup.NetworkInstanceAddresses{
					{Network: "net-net1", Addresses: map[string]string{"vmaaaa": "10.0.0.50"}},
				},
			},
		},
	}
}

func TestRun_HappyPath_FullOrder_AndFiltering(t *testing.T) {
	objects := newFakeObjects()
	putConfig(t, objects, baseConfig())
	archiveAt := configTakenAt.Add(-time.Hour)
	objects.put(archiveKeyFor("volumes", "vol1", archiveAt), testsupport.ArchiveBody(backup.SnapshotName, "keep", "stale"))
	objects.put(archiveKeyFor("vms", "vm1", archiveAt), testsupport.ArchiveBody(backup.SnapshotName, "keep", "stale"))
	// vm2 has an archive but Titan no longer lists it: never imported.
	objects.put(archiveKeyFor("vms", "vm2", archiveAt), testsupport.ArchiveBody(backup.SnapshotName))

	p, order := newOrderedProvider()
	cp := testsupport.NewControlPlane(t)
	setupTitanLists(cp, map[string]string{
		"/internal/reconcile/vms?clusterId=" + clusterId: `[{"id":"vm1","externalId":"vmaaaa","currentState":"Running","projectName":"t-proj1"}]`,
		"/internal/reconcile/volumes?clusterId=" + clusterId: `{"volumes":[{"id":"vol1","externalId":"volaaaa","projectName":"t-proj1"}],` +
			`"snapshots":[{"id":"s1","volumeId":"vol1","externalId":"volaaaa/keep","projectName":"t-proj1"}]}`,
		"/internal/reconcile/vm-snapshots?clusterId=" + clusterId:   `[{"id":"s2","vmId":"vm1","externalId":"vmaaaa/keep","projectName":"t-proj1"}]`,
		"/internal/reconcile/networks?clusterId=" + clusterId:       `[{"id":"net1","externalId":"net-net1","projectName":"t-proj1"}]`,
		"/internal/reconcile/firewall-rules?clusterId=" + clusterId: `[{"id":"fw1","networkId":"net1","externalId":"acl-net1/rule-live"}]`,
		"/internal/reconcile/load-balancers?clusterId=" + clusterId: `[{"id":"lb1","projectName":"t-proj1","externalNetworkId":"net-net1","vip":"fd00:1::10"}]`,
	})

	clusters := cluster.Fixed{clusterId: {Provider: p}}
	summary, err := New(cp.Client(), clusters, objects).Run(context.Background(), clusterId, "", logger())

	if err != nil {
		t.Fatalf("Run: %v, summary %+v", err, summary)
	}
	if summary.Failed != 0 {
		t.Fatalf("summary: %+v", summary)
	}

	// (1) order: project, profile, acl, network, volume (import, rollback+delete backup), vm (same, pin its ipv4,
	// start), load balancer last: OVN refuses a load balancer whose target has no logical switch port yet, which only
	// exists once the vm is imported (titan-ops features/backup/design.md#cluster-restore).
	gotOrder := strings.Join(*order, ",")
	wantOrder := "import-volume volaaaa,import-instance vmaaaa,pin-ipv4 vmaaaa 10.0.0.50,start vmaaaa,create-lb fd00:1::10"
	if gotOrder != wantOrder {
		t.Fatalf("order: got %q, want %q", gotOrder, wantOrder)
	}
	// Incus's PATCH replaces a whole device, so the pin carries the NIC's other keys along.
	wantNic := map[string]string{"type": "nic", "network": "net-net1", "ipv4.address": "10.0.0.50"}
	if len(p.PatchedInstances) != 1 || !maps.Equal(p.PatchedInstances[0].Devices["eth0"], wantNic) {
		t.Fatalf("ipv4 pin: %+v", p.PatchedInstances)
	}
	if len(p.CreatedProjects) != 1 || p.CreatedProjects[0].Name != "t-proj1" {
		t.Fatalf("projects: %+v", p.CreatedProjects)
	}
	if len(p.UpdatedProfiles) != 1 || p.UpdatedProfiles[0].Name != "default" {
		t.Fatalf("default profile not updated: %+v", p.UpdatedProfiles)
	}
	if len(p.CreatedProfiles) != 1 || p.CreatedProfiles[0].Name != "extra" {
		t.Fatalf("extra profile not created: %+v", p.CreatedProfiles)
	}

	// (2) net-net2 (titan no longer lists network "net2") is never created.
	for _, created := range p.CreatedNetworks {
		if created.Name == "net-net2" {
			t.Fatalf("a network titan no longer lists was created: %+v", created)
		}
	}
	if len(p.CreatedNetworks) != 1 || p.CreatedNetworks[0].Name != "net-net1" {
		t.Fatalf("network net-net1 not restored: %+v", p.CreatedNetworks)
	}
	if p.CreatedNetworks[0].Config["volatile.last_state.ready"] != "" {
		t.Fatalf("a stale volatile key was kept: %+v", p.CreatedNetworks[0].Config)
	}
	if p.CreatedNetworks[0].Config["volatile.network.ipv6.address"] != "fd00:1::1" {
		t.Fatalf("the public IPv6 router address was dropped: %+v", p.CreatedNetworks[0].Config)
	}

	// (4) the ACL rule whose description is not a live rule id is dropped; the one with no description is kept.
	if len(p.CreatedACLs) != 1 {
		t.Fatalf("acl: %+v", p.CreatedACLs)
	}
	descriptions := []string{}
	for _, rule := range p.CreatedACLs[0].Ingress {
		descriptions = append(descriptions, rule.Description)
	}
	if strings.Join(descriptions, ",") != "rule-live," {
		t.Fatalf("acl rules kept: %v", descriptions)
	}

	if len(p.CreatedLoadBalancers) != 1 || p.CreatedLoadBalancers[0].ListenAddress != "fd00:1::10" {
		t.Fatalf("load balancer: %+v", p.CreatedLoadBalancers)
	}

	// (1) volume and vm were imported, rolled back to backup and had it deleted.
	if len(p.ImportedVolumes) != 1 || p.ImportedVolumes[0].Name != "volaaaa" {
		t.Fatalf("volume import: %+v", p.ImportedVolumes)
	}
	if len(p.RestoredVolumes) != 1 || p.RestoredVolumes[0] != "t-proj1/volaaaa/backup" {
		t.Fatalf("volume restore: %v", p.RestoredVolumes)
	}
	if len(p.ImportedInstances) != 1 || p.ImportedInstances[0].Name != "vmaaaa" {
		t.Fatalf("vm import: %+v", p.ImportedInstances)
	}

	// (3) the "stale" snapshot titan no longer lists was deleted; "keep", which it lists, was not; "backup" is gone.
	volumeSnapshots, _ := p.ListVolumeSnapshots(context.Background(), "t-proj1", "default", "volaaaa")
	if len(volumeSnapshots) != 1 || volumeSnapshots[0].Name != "volaaaa/keep" {
		t.Fatalf("volume snapshots left: %+v", volumeSnapshots)
	}
	vmSnapshots, _ := p.ListInstanceSnapshots(context.Background(), "t-proj1", "vmaaaa")
	if len(vmSnapshots) != 1 || vmSnapshots[0].Name != "keep" {
		t.Fatalf("vm snapshots left: %+v", vmSnapshots)
	}

	// vm2 is never touched: titan no longer lists it.
	for _, imported := range p.ImportedInstances {
		if imported.Name == "vmbbbb" {
			t.Fatalf("a vm titan no longer lists was imported: %+v", imported)
		}
	}
}

// A config with no InstanceAddresses (an older restore point, or a project whose network had no lease yet when the
// backup ran) leaves the vm's NIC alone: nothing to pin it to.
func TestRun_NoRecordedIpv4_LeavesTheNicAlone(t *testing.T) {
	objects := newFakeObjects()
	config := baseConfig()
	config.Projects[0].InstanceAddresses = nil
	putConfig(t, objects, config)
	archiveAt := configTakenAt.Add(-time.Hour)
	objects.put(archiveKeyFor("vms", "vm1", archiveAt), testsupport.ArchiveBody(backup.SnapshotName))

	p, _ := newOrderedProvider()
	cp := testsupport.NewControlPlane(t)
	setupTitanLists(cp, map[string]string{
		"/internal/reconcile/vms?clusterId=" + clusterId: `[{"id":"vm1","externalId":"vmaaaa","currentState":"Stopped","projectName":"t-proj1"}]`,
	})

	clusters := cluster.Fixed{clusterId: {Provider: p}}
	summary, err := New(cp.Client(), clusters, objects).Run(context.Background(), clusterId, "", logger())

	if err != nil || summary.Failed != 0 {
		t.Fatalf("err %v, summary %+v", err, summary)
	}
	if len(p.PatchedInstances) != 0 {
		t.Fatalf("a vm with no recorded address was pinned: %+v", p.PatchedInstances)
	}
}

// Only a VM Titan last recorded Running or Ready is started.
func TestRun_StartsOnlyRunningOrReady(t *testing.T) {
	objects := newFakeObjects()
	config := backup.ClusterConfig{ClusterId: clusterId, TakenAt: configTakenAt}
	putConfig(t, objects, config)
	archiveAt := configTakenAt.Add(-time.Hour)
	for _, id := range []string{"vm1", "vm2"} {
		objects.put(archiveKeyFor("vms", id, archiveAt), testsupport.ArchiveBody(backup.SnapshotName))
	}

	p, order := newOrderedProvider()
	cp := testsupport.NewControlPlane(t)
	setupTitanLists(cp, map[string]string{
		"/internal/reconcile/vms?clusterId=" + clusterId: `[{"id":"vm1","externalId":"vmrunning","currentState":"Running","projectName":"t-proj1"},` +
			`{"id":"vm2","externalId":"vmstopped","currentState":"Stopped","projectName":"t-proj1"}]`,
	})

	clusters := cluster.Fixed{clusterId: {Provider: p}}
	summary, err := New(cp.Client(), clusters, objects).Run(context.Background(), clusterId, "", logger())

	if err != nil || summary.Failed != 0 {
		t.Fatalf("err %v, summary %+v", err, summary)
	}
	started := []string{}
	for _, entry := range *order {
		if strings.HasPrefix(entry, "start ") {
			started = append(started, strings.TrimPrefix(entry, "start "))
		}
	}
	if strings.Join(started, ",") != "vmrunning" {
		t.Fatalf("started: %v", started)
	}
}

// The newest archive at or before the restore point's time is used; a later one is ignored.
func TestRun_PicksTheNewestArchiveAtOrBeforeThePoint(t *testing.T) {
	objects := newFakeObjects()
	putConfig(t, objects, backup.ClusterConfig{ClusterId: clusterId, TakenAt: configTakenAt})
	before := configTakenAt.Add(-2 * time.Hour)
	atPoint := configTakenAt.Add(-time.Hour)
	after := configTakenAt.Add(time.Hour)
	objects.put(archiveKeyFor("volumes", "vol1", before), testsupport.ArchiveBody(backup.SnapshotName, "too-old"))
	objects.put(archiveKeyFor("volumes", "vol1", atPoint), testsupport.ArchiveBody(backup.SnapshotName, "the-right-one"))
	objects.put(archiveKeyFor("volumes", "vol1", after), testsupport.ArchiveBody(backup.SnapshotName, "too-new"))

	p, _ := newOrderedProvider()
	cp := testsupport.NewControlPlane(t)
	setupTitanLists(cp, map[string]string{
		"/internal/reconcile/volumes?clusterId=" + clusterId: `{"volumes":[{"id":"vol1","externalId":"volaaaa","projectName":"t-proj1"}],"snapshots":[]}`,
	})

	clusters := cluster.Fixed{clusterId: {Provider: p}}
	summary, err := New(cp.Client(), clusters, objects).Run(context.Background(), clusterId, "", logger())

	if err != nil || summary.Failed != 0 {
		t.Fatalf("err %v, summary %+v", err, summary)
	}
	if len(p.ImportedVolumes) != 1 || !strings.Contains(p.ImportedVolumes[0].Body, "the-right-one") {
		t.Fatalf("imported: %+v", p.ImportedVolumes)
	}
}

// An archive without the backup snapshot is an old-format archive: reported failed, the import undone, and the run
// fails overall, but other resources still restore.
func TestRun_OldFormatArchive_FailsButOthersStillRestore(t *testing.T) {
	objects := newFakeObjects()
	putConfig(t, objects, backup.ClusterConfig{ClusterId: clusterId, TakenAt: configTakenAt})
	archiveAt := configTakenAt.Add(-time.Hour)
	// vol1's archive has no "backup" snapshot: old format.
	objects.put(archiveKeyFor("volumes", "vol1", archiveAt), testsupport.ArchiveBody("some-other-snapshot"))
	objects.put(archiveKeyFor("volumes", "vol2", archiveAt), testsupport.ArchiveBody(backup.SnapshotName))

	p, _ := newOrderedProvider()
	cp := testsupport.NewControlPlane(t)
	setupTitanLists(cp, map[string]string{
		"/internal/reconcile/volumes?clusterId=" + clusterId: `{"volumes":[{"id":"vol1","externalId":"volaaaa","projectName":"t-proj1"},` +
			`{"id":"vol2","externalId":"volbbbb","projectName":"t-proj1"}],"snapshots":[]}`,
	})

	clusters := cluster.Fixed{clusterId: {Provider: p}}
	summary, err := New(cp.Client(), clusters, objects).Run(context.Background(), clusterId, "", logger())

	if err == nil {
		t.Fatal("want the run to fail: an old-format archive was restored")
	}
	if summary.Failed != 1 || summary.Restored != 1 {
		t.Fatalf("summary: %+v", summary)
	}
	if _, found := p.Volumes["t-proj1/volaaaa"]; found {
		t.Fatalf("the old-format import was not undone: %+v", p.Volumes)
	}
	if _, found := p.Volumes["t-proj1/volbbbb"]; !found {
		t.Fatalf("the other volume was not restored: %+v", p.Volumes)
	}
}

// Every template row is reported missing, then one sync is asked for.
func TestRun_Templates_ReportsEachMissing_ThenOneSync(t *testing.T) {
	objects := newFakeObjects()
	putConfig(t, objects, backup.ClusterConfig{ClusterId: clusterId, TakenAt: configTakenAt})

	p, _ := newOrderedProvider()
	cp := testsupport.NewControlPlane(t)
	setupTitanLists(cp, map[string]string{
		"/internal/reconcile/templates?clusterId=" + clusterId: `[{"id":"tpl1","externalTemplateId":"tpl-tpl1"},{"id":"tpl2","externalTemplateId":"tpl-tpl2"}]`,
	})

	clusters := cluster.Fixed{clusterId: {Provider: p}}
	summary, err := New(cp.Client(), clusters, objects).Run(context.Background(), clusterId, "", logger())

	if err != nil || summary.Failed != 0 {
		t.Fatalf("err %v, summary %+v", err, summary)
	}
	observed := cp.RequestsTo("PATCH /internal/templates/")
	if len(observed) != 2 {
		t.Fatalf("observations sent: %+v", observed)
	}
	for _, request := range observed {
		if !strings.Contains(request.Body, `"exists":false`) {
			t.Fatalf("observation body: %s", request.Body)
		}
	}
	sync := cp.RequestsTo("POST /internal/templates/sync")
	if len(sync) != 1 {
		t.Fatalf("sync requests: %+v", sync)
	}
}

// No config object at all in the bucket is a failure, and nothing is created.
func TestRun_NoConfigObject_Fails_NothingCreated(t *testing.T) {
	objects := newFakeObjects()
	p, _ := newOrderedProvider()
	cp := testsupport.NewControlPlane(t)
	setupTitanLists(cp, nil)

	clusters := cluster.Fixed{clusterId: {Provider: p}}
	summary, err := New(cp.Client(), clusters, objects).Run(context.Background(), clusterId, "", logger())

	if err == nil {
		t.Fatal("want a failure: no config object in the bucket")
	}
	if summary.Restored != 0 || summary.Skipped != 0 || summary.Failed != 0 {
		t.Fatalf("summary: %+v", summary)
	}
	if p.Calls != 0 {
		t.Fatalf("incus was touched: %d calls", p.Calls)
	}
	if len(cp.Requests()) != 0 {
		t.Fatalf("the control plane was touched: %+v", cp.Requests())
	}
}

// A restore point before every config object leaves none to pick: the same failure as no config at all.
func TestRun_PointBeforeEveryConfig_Fails(t *testing.T) {
	objects := newFakeObjects()
	putConfig(t, objects, backup.ClusterConfig{ClusterId: clusterId, TakenAt: configTakenAt})
	p, _ := newOrderedProvider()
	cp := testsupport.NewControlPlane(t)
	setupTitanLists(cp, nil)

	clusters := cluster.Fixed{clusterId: {Provider: p}}
	_, err := New(cp.Client(), clusters, objects).Run(context.Background(), clusterId, "20200101T000000Z", logger())

	if err == nil {
		t.Fatal("want a failure: the point is before every config object")
	}
}
