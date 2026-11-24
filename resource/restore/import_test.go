package restore

import (
	"context"
	"errors"
	"io"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

const (
	tenantProject  = "t-0123abcd000040008000000000000001"
	storagePool    = "pool1"
	originalVm     = "vmoriginal"
	copiedVm       = "vmcopied"
	originalVolume = "voloriginal"
	copiedVolume   = "volcopied"
	vmArchiveKey   = "incus/c1/tenants/t1/vms/vm1/20261122T020000Z.tar.gz"
	volArchiveKey  = "incus/c1/tenants/t1/volumes/vol1/20261122T020000Z.tar.gz"
)

// writes wraps the fake Provider to keep every write an import step makes, in call order, "<verb> <project>/<name>".
type writes struct {
	*testsupport.Provider
	made []string
}

func (w *writes) write(verb string, project string, name string) {
	w.made = append(w.made, verb+" "+project+"/"+name)
}

func (w *writes) ImportInstance(ctx context.Context, project string, pool string, name string, body io.Reader, overrides provider.InstanceOverrides) error {
	w.write("import-vm", project, name)
	return w.Provider.ImportInstance(ctx, project, pool, name, body, overrides)
}

func (w *writes) ImportVolume(ctx context.Context, project string, pool string, name string, body io.Reader) error {
	w.write("import-volume", project, name)
	return w.Provider.ImportVolume(ctx, project, pool, name, body)
}

func (w *writes) UpdateInstance(ctx context.Context, project string, name string, instance api.InstancePut, etag string) error {
	w.write("update-vm", project, name)
	return w.Provider.UpdateInstance(ctx, project, name, instance, etag)
}

func (w *writes) RestoreVolume(ctx context.Context, project string, pool string, name string, snapshot string) error {
	w.write("rollback-volume", project, name+"/"+snapshot)
	return w.Provider.RestoreVolume(ctx, project, pool, name, snapshot)
}

func (w *writes) DeleteInstanceSnapshot(ctx context.Context, project string, instance string, snapshot string) error {
	w.write("delete-vm-snapshot", project, instance+"/"+snapshot)
	return w.Provider.DeleteInstanceSnapshot(ctx, project, instance, snapshot)
}

func (w *writes) DeleteVolumeSnapshot(ctx context.Context, project string, pool string, volume string, snapshot string) error {
	w.write("delete-volume-snapshot", project, volume+"/"+snapshot)
	return w.Provider.DeleteVolumeSnapshot(ctx, project, pool, volume, snapshot)
}

func (w *writes) DeleteInstance(ctx context.Context, project string, name string) error {
	w.write("delete-vm", project, name)
	return w.Provider.DeleteInstance(ctx, project, name)
}

func (w *writes) DeleteVolume(ctx context.Context, project string, pool string, name string) error {
	w.write("delete-volume", project, name)
	return w.Provider.DeleteVolume(ctx, project, pool, name)
}

// archivedVm is a VM with a volume attached, backed up with the backup snapshot and one of the tenant's own.
func archivedVm(snapshots ...string) string {
	return testsupport.ArchiveDescription{
		Name: originalVm, Snapshots: snapshots,
		Devices: map[string]map[string]string{
			"root":         {"type": "disk", "path": "/", "pool": storagePool},
			"eth0":         {"type": "nic", "network": "net-1"},
			originalVolume: {"type": "disk", "pool": storagePool, "source": originalVolume},
		},
		Config: map[string]string{"volatile.uuid": "6d3f6f6a-52b5-4bd5-9d3c-0d2f6a1f0a11", "volatile.eth0.hwaddr": "10:66:6a:9c:6a:01"},
	}.TarGz()
}

// copyScene is a tenant's project where the original VM runs with its volume attached, a volume restored for the copy
// stands ready, and the bucket holds the VM's and the volume's archive.
func copyScene(vmSnapshots []string, volumeSnapshots []string) (*writes, *fakeObjects) {
	fake := testsupport.NewProvider()
	fake.Instances[tenantProject+"/"+originalVm] = &api.Instance{Name: originalVm, StatusCode: api.Running, InstancePut: api.InstancePut{
		Devices: map[string]map[string]string{originalVolume: {"type": "disk", "pool": storagePool, "source": originalVolume}},
	}}
	for _, name := range []string{originalVolume, copiedVolume} {
		fake.Volumes[tenantProject+"/"+name] = &api.StorageVolume{Name: name, Type: "custom", Project: tenantProject}
	}
	objects := newFakeObjects()
	objects.put(vmArchiveKey, archivedVm(vmSnapshots...))
	objects.put(volArchiveKey, testsupport.ArchiveBody(volumeSnapshots...))
	return &writes{Provider: fake}, objects
}

var copyOverrides = provider.InstanceOverrides{
	Devices: []string{originalVolume + ",source=" + copiedVolume},
	Config:  []string{"volatile.eth0.hwaddr=", "volatile.uuid="},
}

func importCopy(p provider.Provider, objects Objects) error {
	return ImportVmWithOverrides(context.Background(), p, storagePool, tenantProject, copiedVm, vmArchiveKey, copyOverrides, objects, logger())
}

// A copy beside the original is rolled back disk-only: a full rollback puts the archived devices and volatile keys
// back, and with them the original's volume, address and port (titan-ops features/backup/design.md#restore-as-new-resource).
func TestImportVmWithOverrides_ImportsUnderTheNameWithTheOverrides_RollsBackDiskOnly_AndRemovesTheBackupSnapshot(t *testing.T) {
	p, objects := copyScene([]string{"backup", "sold"}, nil)

	err := importCopy(p, objects)

	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"import-vm " + tenantProject + "/" + copiedVm,
		"update-vm " + tenantProject + "/" + copiedVm,
		"delete-vm-snapshot " + tenantProject + "/" + copiedVm + "/backup",
	}
	if !slices.Equal(p.made, want) {
		t.Errorf("writes\n got %q\nwant %q", p.made, want)
	}
	imported := p.ImportedInstances
	isTheArchiveWithTheOverrides := len(imported) == 1 && imported[0].Pool == storagePool && imported[0].Body == archivedVm("backup", "sold") &&
		slices.Equal(imported[0].Overrides.Devices, copyOverrides.Devices) && slices.Equal(imported[0].Overrides.Config, copyOverrides.Config)
	if !isTheArchiveWithTheOverrides {
		t.Errorf("import: %+v", imported)
	}
	wantRollback := api.InstancePut{Restore: "backup", DiskOnly: true}
	if len(p.UpdatedInstances) != 1 || p.UpdatedInstances[0].Restore != wantRollback.Restore || p.UpdatedInstances[0].DiskOnly != wantRollback.DiskOnly ||
		len(p.UpdatedInstances[0].Devices) != 0 || len(p.UpdatedInstances[0].Config) != 0 {
		t.Errorf("rollback: %+v", p.UpdatedInstances)
	}
	left, _ := p.ListInstanceSnapshots(context.Background(), tenantProject, copiedVm)
	if len(left) != 1 || left[0].Name != "sold" {
		t.Errorf("snapshots left: %+v", left)
	}
	copied, _ := p.GetInstance(context.Background(), tenantProject, copiedVm)
	if copied.Devices[originalVolume]["source"] != copiedVolume {
		t.Errorf("the copy's volume device: %v", copied.Devices[originalVolume])
	}
}

func TestImportVmWithOverrides_NeverWritesTheOriginal(t *testing.T) {
	p, objects := copyScene([]string{"backup"}, nil)
	before := maps.Clone(p.Instances[tenantProject+"/"+originalVm].Devices)

	if err := importCopy(p, objects); err != nil {
		t.Fatal(err)
	}

	for _, write := range p.made {
		if strings.HasSuffix(write, "/"+originalVm) || strings.Contains(write, "/"+originalVm+"/") || strings.Contains(write, originalVolume) {
			t.Errorf("a write names the original: %q", write)
		}
	}
	if !maps.EqualFunc(p.Instances[tenantProject+"/"+originalVm].Devices, before, maps.Equal[map[string]string]) {
		t.Errorf("the original's devices changed: %v", p.Instances[tenantProject+"/"+originalVm].Devices)
	}
}

// An archive without the backup snapshot cannot be rolled back to the moment it was taken: the import is undone and
// the caller is told which kind of failure it is.
func TestImportVmWithOverrides_AnArchiveWithoutTheBackupSnapshot_IsOldFormat_AndItsImportIsRemoved(t *testing.T) {
	p, objects := copyScene([]string{"sold"}, nil)

	err := importCopy(p, objects)

	if !errors.Is(err, ErrOldFormat) {
		t.Fatalf("err = %v, want ErrOldFormat", err)
	}
	wantText := "archive " + vmArchiveKey + ` has no "backup" snapshot: an old-format archive, not usable`
	if err.Error() != wantText {
		t.Errorf("error\n got %q\nwant %q", err, wantText)
	}
	want := []string{"import-vm " + tenantProject + "/" + copiedVm, "delete-vm " + tenantProject + "/" + copiedVm}
	if !slices.Equal(p.made, want) {
		t.Errorf("writes\n got %q\nwant %q", p.made, want)
	}
	if _, left := p.Instances[tenantProject+"/"+copiedVm]; left {
		t.Error("the old-format import is still there")
	}
}

func TestImportVmWithOverrides_AStepThatFails_NamesTheStep_AndIsNotOldFormat(t *testing.T) {
	for name, tc := range map[string]struct {
		break_ func(*writes, *fakeObjects)
		step   string
	}{
		"no archive":      {func(_ *writes, objects *fakeObjects) { delete(objects.data, vmArchiveKey) }, "read archive " + vmArchiveKey},
		"import":          {func(p *writes, _ *fakeObjects) { p.ImportInstanceErr = errors.New("disk full") }, "import vm " + copiedVm},
		"snapshot read":   {func(p *writes, _ *fakeObjects) { p.GetSnapshotErr = errors.New("busy") }, `look for the "backup" snapshot of vm ` + copiedVm},
		"rollback":        {func(p *writes, _ *fakeObjects) { p.UpdateInstanceErr = errors.New("locked") }, "roll vm " + copiedVm + ` back to "backup"`},
		"snapshot delete": {func(p *writes, _ *fakeObjects) { p.DeleteSnapshotErr = errors.New("locked") }, `delete the "backup" snapshot of vm ` + copiedVm},
	} {
		p, objects := copyScene([]string{"backup"}, nil)
		tc.break_(p, objects)

		err := importCopy(p, objects)

		if err == nil || !strings.Contains(err.Error(), tc.step) || errors.Is(err, ErrOldFormat) {
			t.Errorf("%s: err = %v, want it to name %q and not be ErrOldFormat", name, err, tc.step)
		}
		if slices.Contains(p.made, "delete-vm "+tenantProject+"/"+copiedVm) {
			t.Errorf("%s: the import was removed on a failure that is not the archive's: %q", name, p.made)
		}
	}
}

// The whole-cluster restore puts a VM back as it was at the backup, config and devices included: its rollback is full.
func TestImportVm_ImportsWithoutOverrides_AndRollsBackInFull(t *testing.T) {
	p, objects := copyScene([]string{"backup"}, nil)
	delete(p.Instances, tenantProject+"/"+originalVm)

	err := ImportVm(context.Background(), p, storagePool, tenantProject, originalVm, vmArchiveKey, objects, logger())

	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"import-vm " + tenantProject + "/" + originalVm,
		"update-vm " + tenantProject + "/" + originalVm,
		"delete-vm-snapshot " + tenantProject + "/" + originalVm + "/backup",
	}
	if !slices.Equal(p.made, want) {
		t.Errorf("writes\n got %q\nwant %q", p.made, want)
	}
	overrides := p.ImportedInstances[0].Overrides
	if len(overrides.Devices) != 0 || len(overrides.Config) != 0 {
		t.Errorf("overrides: %+v", overrides)
	}
	if len(p.UpdatedInstances) != 1 || p.UpdatedInstances[0].Restore != "backup" || p.UpdatedInstances[0].DiskOnly {
		t.Errorf("rollback: %+v", p.UpdatedInstances)
	}
}

// A run that finds the copy already imported takes the rollback up where the run before left it: the backup snapshot
// still there means the rollback is still owed, gone means it was done.
func TestRollBackVmDiskOnly_WithTheBackupSnapshot_RollsBackAndRemovesIt_AndWithout_WritesNothing(t *testing.T) {
	for name, tc := range map[string]struct {
		snapshots []string
		rolled    bool
		writes    []string
	}{
		"the snapshot is there": {[]string{"backup", "sold"}, true, []string{
			"update-vm " + tenantProject + "/" + copiedVm, "delete-vm-snapshot " + tenantProject + "/" + copiedVm + "/backup"}},
		"the snapshot is gone": {[]string{"sold"}, false, nil},
	} {
		p, _ := copyScene(nil, nil)
		p.Instances[tenantProject+"/"+copiedVm] = &api.Instance{Name: copiedVm, StatusCode: api.Stopped}
		for _, snapshot := range tc.snapshots {
			p.InstanceSnapshots[tenantProject+"/"+copiedVm+"/"+snapshot] = true
		}

		rolled, err := RollBackVmDiskOnly(context.Background(), p, tenantProject, copiedVm)

		if err != nil || rolled != tc.rolled {
			t.Errorf("%s: rolled back %v, err %v", name, rolled, err)
		}
		if !slices.Equal(p.made, tc.writes) {
			t.Errorf("%s: writes\n got %q\nwant %q", name, p.made, tc.writes)
		}
		if tc.rolled && (len(p.UpdatedInstances) != 1 || !p.UpdatedInstances[0].DiskOnly || p.UpdatedInstances[0].Restore != "backup") {
			t.Errorf("%s: rollback: %+v", name, p.UpdatedInstances)
		}
		if _, kept := p.Instances[tenantProject+"/"+copiedVm]; !kept {
			t.Errorf("%s: the instance was removed", name)
		}
	}
}

func TestRollBackVmDiskOnly_AStepThatFails_IsAnError_NotASnapshotThatIsGone(t *testing.T) {
	for name, tc := range map[string]struct {
		break_ func(*writes)
		step   string
	}{
		"snapshot read":   {func(p *writes) { p.GetSnapshotErr = errors.New("busy") }, "look for"},
		"rollback":        {func(p *writes) { p.UpdateInstanceErr = errors.New("locked") }, "roll vm"},
		"snapshot delete": {func(p *writes) { p.DeleteSnapshotErr = errors.New("locked") }, "delete the"},
	} {
		p, _ := copyScene(nil, nil)
		p.InstanceSnapshots[tenantProject+"/"+copiedVm+"/backup"] = true
		tc.break_(p)

		_, err := RollBackVmDiskOnly(context.Background(), p, tenantProject, copiedVm)

		if err == nil || !strings.Contains(err.Error(), tc.step) {
			t.Errorf("%s: err = %v, want it to name %q", name, err, tc.step)
		}
	}
}

func TestRollBackVolume_WithTheBackupSnapshot_RollsBackAndRemovesIt_AndWithout_WritesNothing(t *testing.T) {
	for name, tc := range map[string]struct {
		snapshots []string
		rolled    bool
		writes    []string
	}{
		"the snapshot is there": {[]string{"backup", "sold"}, true, []string{
			"rollback-volume " + tenantProject + "/" + copiedVolume + "/backup", "delete-volume-snapshot " + tenantProject + "/" + copiedVolume + "/backup"}},
		"the snapshot is gone": {[]string{"sold"}, false, nil},
	} {
		p, _ := copyScene(nil, nil)
		for _, snapshot := range tc.snapshots {
			p.VolumeSnapshots[tenantProject+"/"+copiedVolume+"/"+snapshot] = true
		}

		rolled, err := RollBackVolume(context.Background(), p, storagePool, tenantProject, copiedVolume)

		if err != nil || rolled != tc.rolled {
			t.Errorf("%s: rolled back %v, err %v", name, rolled, err)
		}
		if !slices.Equal(p.made, tc.writes) {
			t.Errorf("%s: writes\n got %q\nwant %q", name, p.made, tc.writes)
		}
		if _, kept := p.Volumes[tenantProject+"/"+copiedVolume]; !kept {
			t.Errorf("%s: the volume was removed", name)
		}
	}
}

func TestRollBackVolume_AStepThatFails_IsAnError_NotASnapshotThatIsGone(t *testing.T) {
	for name, tc := range map[string]struct {
		break_ func(*writes)
		step   string
	}{
		"snapshot list":   {func(p *writes) { p.ListVolumeSnapshotsErr = errors.New("busy") }, "look for"},
		"rollback":        {func(p *writes) { p.RestoreVolumeErr = errors.New("locked") }, "roll volume"},
		"snapshot delete": {func(p *writes) { p.DeleteSnapshotErr = errors.New("locked") }, "delete the"},
	} {
		p, _ := copyScene(nil, nil)
		p.VolumeSnapshots[tenantProject+"/"+copiedVolume+"/backup"] = true
		tc.break_(p)

		_, err := RollBackVolume(context.Background(), p, storagePool, tenantProject, copiedVolume)

		if err == nil || !strings.Contains(err.Error(), tc.step) {
			t.Errorf("%s: err = %v, want it to name %q", name, err, tc.step)
		}
	}
}

// ImportVolume under a name of its own is all a restore of one volume needs of the import: the same steps, and the
// same word for an archive too old to use.
func TestImportVolume_UnderAnotherName_RollsBack_AndAnOldFormatArchive_IsErrOldFormat_WithItsImportRemoved(t *testing.T) {
	p, objects := copyScene(nil, []string{"backup", "sold"})
	delete(p.Volumes, tenantProject+"/"+copiedVolume)

	err := ImportVolume(context.Background(), p, storagePool, tenantProject, copiedVolume, volArchiveKey, objects, logger())

	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"import-volume " + tenantProject + "/" + copiedVolume,
		"rollback-volume " + tenantProject + "/" + copiedVolume + "/backup",
		"delete-volume-snapshot " + tenantProject + "/" + copiedVolume + "/backup",
	}
	if !slices.Equal(p.made, want) {
		t.Errorf("writes\n got %q\nwant %q", p.made, want)
	}

	old, oldObjects := copyScene(nil, []string{"sold"})
	delete(old.Volumes, tenantProject+"/"+copiedVolume)

	err = ImportVolume(context.Background(), old, storagePool, tenantProject, copiedVolume, volArchiveKey, oldObjects, logger())

	wantText := "archive " + volArchiveKey + ` has no "backup" snapshot: an old-format archive, not usable`
	if !errors.Is(err, ErrOldFormat) || err.Error() != wantText {
		t.Errorf("err = %v, want ErrOldFormat worded %q", err, wantText)
	}
	wantOld := []string{"import-volume " + tenantProject + "/" + copiedVolume, "delete-volume " + tenantProject + "/" + copiedVolume}
	if !slices.Equal(old.made, wantOld) {
		t.Errorf("writes of an old-format import\n got %q\nwant %q", old.made, wantOld)
	}
}
