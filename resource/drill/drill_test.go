package drill

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/resource/backup"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

const (
	clusterId = "mock"
	tenantId  = "0123abcd-0000-4000-8000-000000000001"
	tenant    = "t-0123abcd000040008000000000000001"
	vmId      = "11111111-2222-3333-4444-555555555555"
	volumeId  = "99999999-8888-4777-8666-555555555555"
	networkId = "a0331ba7-9723-4ae4-b0df-6c1a43272584"
	gib       = uint64(1) << 30
	pool      = "default"
	drillName = "drill-20261123"
)

var (
	today       = time.Date(2026, 11, 23, 8, 0, 0, 0, time.UTC)
	vmName      = mustName(provider.InstanceName(vmId))
	volumeName  = mustName(provider.VolumeName(volumeId))
	networkName = provider.NetworkName(networkId)
)

func mustName(name string, err error) string {
	if err != nil {
		panic(err)
	}
	return name
}

// fakeObjects is a bucket of strings: a fake archive's body is the list of snapshots it carries
// (testsupport.ArchiveBody).
type fakeObjects map[string]string

func (f fakeObjects) List(ctx context.Context, prefix string) ([]string, error) {
	keys := []string{}
	for key := range f {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (f fakeObjects) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	body, found := f[key]
	if !found {
		return nil, fmt.Errorf("fake bucket: no object %s", key)
	}
	return io.NopCloser(strings.NewReader(body)), nil
}

// recorded wraps the fake Provider to keep every write in call order, "<verb> <project>/<name>": the drill's first
// rule is that it writes in its own project and nowhere else.
type recorded struct {
	*testsupport.Provider
	writes []string
	// onImportInstance runs before an instance import answers.
	onImportInstance func() error
}

func (r *recorded) write(verb string, project string, name string) {
	r.writes = append(r.writes, verb+" "+project+"/"+name)
}

func (r *recorded) CreateProject(ctx context.Context, project api.ProjectsPost) error {
	r.write("create-project", "", project.Name)
	return r.Provider.CreateProject(ctx, project)
}

func (r *recorded) CreateNetwork(ctx context.Context, project string, network api.NetworksPost) error {
	r.write("create-network", project, network.Name)
	return r.Provider.CreateNetwork(ctx, project, network)
}

func (r *recorded) ImportVolume(ctx context.Context, project string, pool string, name string, body io.Reader) error {
	r.write("import-volume", project, name)
	return r.Provider.ImportVolume(ctx, project, pool, name, body)
}

func (r *recorded) ImportInstance(ctx context.Context, project string, pool string, name string, body io.Reader) error {
	r.write("import-vm", project, name)
	if r.onImportInstance != nil {
		if err := r.onImportInstance(); err != nil {
			return err
		}
	}
	return r.Provider.ImportInstance(ctx, project, pool, name, body)
}

func (r *recorded) RestoreVolume(ctx context.Context, project string, pool string, name string, snapshot string) error {
	r.write("rollback-volume", project, name)
	return r.Provider.RestoreVolume(ctx, project, pool, name, snapshot)
}

func (r *recorded) UpdateInstance(ctx context.Context, project string, name string, instance api.InstancePut, etag string) error {
	r.write("update-vm", project, name)
	return r.Provider.UpdateInstance(ctx, project, name, instance, etag)
}

func (r *recorded) DeleteInstanceSnapshot(ctx context.Context, project string, instance string, snapshot string) error {
	r.write("delete-vm-snapshot", project, instance+"/"+snapshot)
	return r.Provider.DeleteInstanceSnapshot(ctx, project, instance, snapshot)
}

func (r *recorded) DeleteVolumeSnapshot(ctx context.Context, project string, pool string, volume string, snapshot string) error {
	r.write("delete-volume-snapshot", project, volume+"/"+snapshot)
	return r.Provider.DeleteVolumeSnapshot(ctx, project, pool, volume, snapshot)
}

func (r *recorded) DeleteInstance(ctx context.Context, project string, name string) error {
	r.write("delete-vm", project, name)
	return r.Provider.DeleteInstance(ctx, project, name)
}

func (r *recorded) DeleteVolume(ctx context.Context, project string, pool string, name string) error {
	r.write("delete-volume", project, name)
	return r.Provider.DeleteVolume(ctx, project, pool, name)
}

func (r *recorded) DeleteNetwork(ctx context.Context, project string, name string) error {
	r.write("delete-network", project, name)
	return r.Provider.DeleteNetwork(ctx, project, name)
}

func (r *recorded) DeleteProject(ctx context.Context, name string) error {
	r.write("delete-project", "", name)
	return r.Provider.DeleteProject(ctx, name)
}

type scene struct {
	provider *recorded
	objects  fakeObjects
	drill    *Drill
}

// newScene is a cluster with a 40 GiB pool 4 GiB used, a tenant VM of a 5 GiB root disk on one network with a 1 GiB
// volume attached, and a day-old archive of the VM and of the volume (each with the backup snapshot).
func newScene() *scene {
	fake := testsupport.NewProvider()
	fake.PoolSpace = api.ResourcesStoragePoolSpace{Total: 40 * gib, Used: 4 * gib}
	fake.Projects[tenant] = true
	fake.Instances[tenant+"/"+vmName] = &api.Instance{
		Name: vmName,
		ExpandedDevices: map[string]map[string]string{
			"root":     {"type": "disk", "path": "/", "pool": pool, "size": "5GiB"},
			"eth0":     {"type": "nic", "network": networkName},
			volumeName: {"type": "disk", "pool": pool, "source": volumeName},
		},
	}
	fake.Volumes[tenant+"/"+volumeName] = &api.StorageVolume{
		Name: volumeName, Type: "custom", StorageVolumePut: api.StorageVolumePut{Config: map[string]string{"size": "1GiB"}},
	}
	objects := fakeObjects{
		backup.ObjectKey(clusterId, tenantId, "vms", vmId, today.Add(-24*time.Hour)):         testsupport.ArchiveBody("backup"),
		backup.ObjectKey(clusterId, tenantId, "volumes", volumeId, today.Add(-24*time.Hour)): testsupport.ArchiveBody("backup"),
	}
	wrapped := &recorded{Provider: fake}
	handler := New(cluster.Fixed{clusterId: {Provider: wrapped, StoragePool: pool}}, objects)
	handler.now = func() time.Time { return today }
	return &scene{provider: wrapped, objects: objects, drill: handler}
}

func leased(payload map[string]any) command.Leased {
	encoded, _ := json.Marshal(payload)
	return command.Leased{Key: "restore-drill-mock-20261123", Kind: "restore.drill", ClusterId: clusterId, Payload: encoded, Attempt: 1, MaxAttempts: 5}
}

func vmLeased() command.Leased {
	return leased(map[string]any{"kind": "vm", "resourceId": vmId, "externalId": vmName, "projectName": tenant, "tenantId": tenantId})
}

func volumeLeased() command.Leased {
	return leased(map[string]any{"kind": "volume", "resourceId": volumeId, "externalId": volumeName, "projectName": tenant, "tenantId": tenantId})
}

func (s *scene) run(l command.Leased) error {
	return s.drill.Run(context.Background(), l, testsupport.DiscardLogger())
}

func isPermanent(err error) bool {
	var permanent interface{ Permanent() bool }
	return errors.As(err, &permanent) && permanent.Permanent()
}

// An end-to-end run: volumes first, then the VM with the network its NIC names, every step in the drill's own project,
// and nothing of it left (AC-2, AC-3, AC-4, AC-9).
func TestRun_Vm_ImportsTheVolumeBeforeTheVm_AndLeavesNothing(t *testing.T) {
	s := newScene()

	err := s.run(vmLeased())

	if err != nil {
		t.Fatalf("a good drill failed: %v", err)
	}
	want := []string{
		"create-project /" + drillName,
		"create-network " + drillName + "/" + networkName,
		"import-volume " + drillName + "/" + volumeName,
		"rollback-volume " + drillName + "/" + volumeName,
		"delete-volume-snapshot " + drillName + "/" + volumeName + "/backup",
		"import-vm " + drillName + "/" + vmName,
		"update-vm " + drillName + "/" + vmName,
		"delete-vm-snapshot " + drillName + "/" + vmName + "/backup",
		"delete-vm " + drillName + "/" + vmName,
		"delete-volume " + drillName + "/" + volumeName,
		"delete-network " + drillName + "/" + networkName,
		"delete-project /" + drillName,
	}
	if !slices.Equal(s.provider.writes, want) {
		t.Errorf("writes\n got %q\nwant %q", s.provider.writes, want)
	}
}

func TestRun_Volume_ImportsJustTheVolume(t *testing.T) {
	s := newScene()

	err := s.run(volumeLeased())

	if err != nil {
		t.Fatalf("a good drill failed: %v", err)
	}
	want := []string{
		"create-project /" + drillName,
		"import-volume " + drillName + "/" + volumeName,
		"rollback-volume " + drillName + "/" + volumeName,
		"delete-volume-snapshot " + drillName + "/" + volumeName + "/backup",
		"delete-volume " + drillName + "/" + volumeName,
		"delete-project /" + drillName,
	}
	if !slices.Equal(s.provider.writes, want) {
		t.Errorf("writes\n got %q\nwant %q", s.provider.writes, want)
	}
}

// AC-3 and AC-7: a drill writes in its own project and in no other, and never in a tenant's.
func TestRun_EveryWrite_NamesTheDrillProject_NeverATenantsOrAnyOther(t *testing.T) {
	for kind, l := range map[string]command.Leased{"vm": vmLeased(), "volume": volumeLeased()} {
		s := newScene()

		_ = s.run(l)

		for _, write := range s.provider.writes {
			_, target, _ := strings.Cut(write, " ")
			project, name, _ := strings.Cut(target, "/")
			if project == "" {
				project = name // a project-level write: "create-project /<project>"
			}
			if project != drillName {
				t.Errorf("%s drill wrote outside %s: %q", kind, drillName, write)
			}
		}
		if !strings.HasPrefix(drillName, "drill-") || strings.HasPrefix(drillName, "t-") {
			t.Fatalf("project %q is not outside the tenant namespace", drillName)
		}
	}
}

// The tenant's own VM and volume are read, never changed (AC-7).
func TestRun_TheTenantsVmAndVolume_AreUntouched(t *testing.T) {
	s := newScene()

	_ = s.run(vmLeased())

	if _, found := s.provider.Instances[tenant+"/"+vmName]; !found {
		t.Error("the tenant's VM is gone")
	}
	if _, found := s.provider.Volumes[tenant+"/"+volumeName]; !found {
		t.Error("the tenant's volume is gone")
	}
	if len(s.provider.StateChanges) != 0 || len(s.provider.PatchedInstances) != 0 {
		t.Errorf("a state change or patch reached an instance: %+v %+v", s.provider.StateChanges, s.provider.PatchedInstances)
	}
}

func TestRun_TheNewestArchive_IsTheOneImported(t *testing.T) {
	s := newScene()
	older := backup.ObjectKey(clusterId, tenantId, "vms", vmId, today.Add(-48*time.Hour))
	newest := backup.ObjectKey(clusterId, tenantId, "vms", vmId, today.Add(-1*time.Hour))
	s.objects[older] = testsupport.ArchiveBody("backup", "older")
	s.objects[newest] = testsupport.ArchiveBody("backup", "newest")

	_ = s.run(vmLeased())

	if len(s.provider.ImportedInstances) != 1 || !strings.Contains(s.provider.ImportedInstances[0].Body, "newest") {
		t.Errorf("imported %+v, want the newest archive", s.provider.ImportedInstances)
	}
}

// AC-5: not enough room is a permanent failure that says what was measured, before anything is made.
func TestRun_NotEnoughPoolSpace_FailsPermanentBeforeAnythingIsCreated(t *testing.T) {
	s := newScene()
	s.provider.PoolSpace = api.ResourcesStoragePoolSpace{Total: 40 * gib, Used: 34 * gib}

	err := s.run(vmLeased())

	if err == nil || !isPermanent(err) {
		t.Fatalf("want a permanent error, got %v", err)
	}
	for _, text := range []string{"free", "6.0 GiB", "needs"} {
		if !strings.Contains(err.Error(), text) {
			t.Errorf("%q does not say %q (measured and required space)", err, text)
		}
	}
	if len(s.provider.writes) != 0 {
		t.Errorf("a drill without room wrote %q", s.provider.writes)
	}
}

// The rule of titan-ops features/backup/design.md#restore-drill: free >= 1.5 x (root disk + attached volumes) + 10% of the pool.
func TestRun_FreeSpace_IsExactlyTheRequirement_Passes_AndOneByteLess_Fails(t *testing.T) {
	required := 9*gib + 4*gib // 1.5 x 6 GiB + 10% of 40 GiB
	for name, tc := range map[string]struct {
		free uint64
		ok   bool
	}{"exactly": {required, true}, "one byte less": {required - 1, false}} {
		s := newScene()
		s.provider.PoolSpace = api.ResourcesStoragePoolSpace{Total: 40 * gib, Used: 40*gib - tc.free}

		err := s.run(vmLeased())

		if tc.ok != (err == nil) {
			t.Errorf("%s: err = %v, want success=%v", name, err, tc.ok)
		}
	}
}

// AC-2: every step that fails fails the drill with the step named, and AC-4: the project goes anyway.
func TestRun_AStepThatFails_NamesTheStep_AndStillRemovesTheProject(t *testing.T) {
	for name, tc := range map[string]struct {
		break_ func(*scene)
		step   string
	}{
		"vm import":       {func(s *scene) { s.provider.ImportInstanceErr = errors.New("disk full") }, "import vm"},
		"volume import":   {func(s *scene) { s.provider.ImportVolumeErr = errors.New("bad tar") }, "import volume"},
		"vm rollback":     {func(s *scene) { s.provider.UpdateInstanceErr = errors.New("network gone") }, "roll vm"},
		"volume rollback": {func(s *scene) { s.provider.RestoreVolumeErr = errors.New("busy") }, "roll volume"},
		"snapshot delete": {func(s *scene) { s.provider.DeleteSnapshotErr = errors.New("locked") }, "delete the \"backup\" snapshot"},
		"no backup snapshot": {func(s *scene) {
			for key := range s.objects {
				s.objects[key] = testsupport.ArchiveBody()
			}
		}, "no \"backup\" snapshot"},
	} {
		s := newScene()
		tc.break_(s)

		err := s.run(vmLeased())

		if err == nil || !strings.Contains(err.Error(), tc.step) {
			t.Errorf("%s: err = %v, want it to name %q", name, err, tc.step)
		}
		if !slices.Contains(s.provider.DeletedProjects, drillName) {
			t.Errorf("%s: project %s was not removed (deleted %v)", name, drillName, s.provider.DeletedProjects)
		}
		if s.provider.Projects[drillName] {
			t.Errorf("%s: project %s is still there", name, drillName)
		}
	}
}

// AC-2 again: a project that cannot be deleted is a failed drill, and the next drill finds it (AC-4).
func TestRun_AProjectThatWillNotDelete_FailsTheDrill(t *testing.T) {
	s := newScene()
	s.provider.DeleteProjectErr = errors.New("project not empty")

	err := s.run(vmLeased())

	if err == nil || !strings.Contains(err.Error(), "delete project") {
		t.Fatalf("err = %v, want it to name the project delete", err)
	}
}

// AC-4 on cancel: the drill's own context is gone, the cleanup still runs.
func TestRun_WhenTheLeaseIsCancelledMidImport_TheProjectIsStillRemoved(t *testing.T) {
	s := newScene()
	ctx, cancel := context.WithCancel(context.Background())
	s.provider.onImportInstance = func() error {
		cancel()
		return ctx.Err()
	}

	err := s.drill.Run(ctx, vmLeased(), testsupport.DiscardLogger())

	if err == nil {
		t.Fatal("a cancelled drill succeeded")
	}
	if !slices.Contains(s.provider.DeletedProjects, drillName) || s.provider.Projects[drillName] {
		t.Errorf("project not removed after cancel: deleted %v, still there %v", s.provider.DeletedProjects, s.provider.Projects[drillName])
	}
}

// AC-4: a project an earlier run died in is removed with what it holds, and a tenant's project never is.
func TestRun_ALeftoverDrillProject_IsRemovedFirst_WithItsContents_AndATenantsIsNot(t *testing.T) {
	s := newScene()
	left := "drill-20261120"
	s.provider.Projects[left] = true
	s.provider.VirtualMachines = []api.InstanceFull{{Instance: api.Instance{Name: "vmleft", Project: left}}}
	s.provider.AllVolumes = []api.StorageVolume{{Name: "volleft", Type: "custom", Project: left}, {Name: "volother", Type: "custom", Project: tenant}}
	s.provider.Networks = []api.Network{{Name: "net-left", Project: left}, {Name: "net-tenant", Project: tenant}}

	err := s.run(vmLeased())

	if err != nil {
		t.Fatalf("drill failed: %v", err)
	}
	first := s.provider.writes[:4]
	want := []string{"delete-vm " + left + "/vmleft", "delete-volume " + left + "/volleft", "delete-network " + left + "/net-left", "delete-project /" + left}
	if !slices.Equal(first, want) {
		t.Errorf("first writes\n got %q\nwant %q", first, want)
	}
	for _, write := range s.provider.writes {
		if strings.Contains(write, "volother") || strings.Contains(write, "net-tenant") || strings.Contains(write, "delete-project /"+tenant) {
			t.Errorf("a tenant's object was touched: %q", write)
		}
	}
}

// AC-9: a VM whose attached volume has no archive fails with the volume named, and imports nothing.
func TestRun_AnAttachedVolumeWithoutAnArchive_FailsNamingItBeforeImportingAnything(t *testing.T) {
	s := newScene()
	for key := range s.objects {
		if strings.Contains(key, "/volumes/") {
			delete(s.objects, key)
		}
	}

	err := s.run(vmLeased())

	if err == nil || !strings.Contains(err.Error(), volumeName) || !strings.Contains(err.Error(), "archive") {
		t.Fatalf("err = %v, want it to name %s and its missing archive", err, volumeName)
	}
	if len(s.provider.writes) != 0 {
		t.Errorf("a drill with a missing archive wrote %q", s.provider.writes)
	}
}

func TestRun_ASourceThatIsGone_FailsPermanentWithoutWriting(t *testing.T) {
	s := newScene()
	delete(s.provider.Instances, tenant+"/"+vmName)

	err := s.run(vmLeased())

	if err == nil || !isPermanent(err) || len(s.provider.writes) != 0 {
		t.Fatalf("err = %v, writes %q; want a permanent error and no write", err, s.provider.writes)
	}
}

func TestRun_APayloadThatIsNotUsable_IsPermanent(t *testing.T) {
	for name, l := range map[string]command.Leased{
		"not json":     {Kind: "restore.drill", ClusterId: clusterId, Payload: []byte("{")},
		"unknown kind": leased(map[string]any{"kind": "network", "resourceId": vmId, "externalId": vmName, "projectName": tenant, "tenantId": tenantId}),
		"no tenant":    leased(map[string]any{"kind": "vm", "resourceId": vmId, "externalId": vmName, "projectName": tenant}),
	} {
		s := newScene()

		err := s.run(l)

		if err == nil || !isPermanent(err) {
			t.Errorf("%s: err = %v, want a permanent error", name, err)
		}
	}
}

// GiveUp removes what a run that died left, and nothing else.
func TestGiveUp_RemovesTheDrillProjects_AndNoTenantProject(t *testing.T) {
	s := newScene()
	s.provider.Projects["drill-20261123"] = true

	err := s.drill.GiveUp(context.Background(), vmLeased(), errors.New("lease lost"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(s.provider.DeletedProjects, []string{"drill-20261123"}) {
		t.Errorf("deleted %v, want only the drill project", s.provider.DeletedProjects)
	}
}
