package vm

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

// A restore makes a VM of its own from one archive, beside the VM the archive was taken of
// (titan-ops features/backup/design.md#restore-as-new-resource). In these tests the VM made is vmId, whose create
// request the command ends; the original is sourceVmId.
const (
	sourceVmId = "99999999-8888-4777-8666-555555555555"
	// Oracles: python3 base32 of each id, as in provider/names_test.go.
	sourceVmName = "vmtgmztgmirbdxpbtgkvkvkvkvku"
	// The volume the original has attached, and its copy, which volume.restore made before this command runs.
	sourceVolumeId   = "22222222-3333-4444-8555-666666666666"
	sourceVolumeName = "voleirceirtgncejbkvmztgmztgmy"
	newVolumeId      = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
	newVolumeName    = "volvkvkvkv3xngmzdo553xo53xo5y"
	// The request the copy's own volume.restore ends, which that command leaves on the copy once it has finished it.
	volumeRequestId = "44444444-5555-4666-8777-888888888888"
	// The request of an earlier restore, of the VM or volume an archive was taken of.
	earlierRequestId = "66666666-5555-4444-8333-222222222222"
	// A volume the original had attached when the backup was taken and has detached since: nothing restores it, unless
	// a test restores it too, as secondNewVolumeId, by a volume.restore that ends secondVolumeRequestId.
	detachedVolumeId      = "33333333-4444-4555-8666-777777777777"
	detachedVolumeName    = "volgmztgm2eircvlbtgo53xo53xo4"
	secondNewVolumeId     = "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff"
	secondNewVolumeName   = "volxo53xo6mzrg53dxo7777777774"
	secondVolumeRequestId = "55555555-6666-4777-8888-999999999999"

	restoreTenantId  = "0123abcd-0000-4000-8000-000000000001"
	restoreBackupKey = "20261122T020000Z"
	// The key vm.backup wrote: incus/<cluster>/<tenant>/vms/<the original's id>/<time>.tar.gz.
	restoreArchiveKey = "incus/mock/0123abcd-0000-4000-8000-000000000001/vms/99999999-8888-4777-8666-555555555555/20261122T020000Z.tar.gz"
	restorePool       = "pool1"
	restoredName      = "web-1-copy"
	restoreFailedText = "Restoring the VM from its backup failed due to a system error. Please contact support."
	archivedUuid      = "6d3f6f6a-52b5-4bd5-9d3c-0d2f6a1f0a11"
	archivedMac       = "10:66:6a:9c:6a:01"
	placeholder0      = "ph0-" + instanceName
	placeholder1      = "ph1-" + instanceName
	gib               = uint64(1) << 30
)

var sourceToNewVolume = []map[string]string{{"sourceVolumeId": sourceVolumeId, "newVolumeId": newVolumeId, "requestId": volumeRequestId}}

// bothVolumes restores the detached volume with the VM too, beside the one still attached.
var bothVolumes = []map[string]string{
	{"sourceVolumeId": sourceVolumeId, "newVolumeId": newVolumeId, "requestId": volumeRequestId},
	{"sourceVolumeId": detachedVolumeId, "newVolumeId": secondNewVolumeId, "requestId": secondVolumeRequestId},
}

func rootDisk() map[string]string {
	return map[string]string{"type": "disk", "path": "/", "pool": restorePool, "size": "10GiB"}
}

func volumeDisk(volume string) map[string]string {
	return map[string]string{"type": "disk", "pool": restorePool, "source": volume}
}

// archiveOf is the original's archive: its root disk, its NIC and a disk device for each volume given, named after
// the volume as volume.attach names it, with the "backup" snapshot and one the tenant took.
func archiveOf(volumes ...string) testsupport.ArchiveDescription {
	devices := map[string]map[string]string{"root": rootDisk(), "eth0": {"type": "nic", "network": "net-1"}}
	for _, volume := range volumes {
		devices[volume] = volumeDisk(volume)
	}
	return testsupport.ArchiveDescription{
		Name: sourceVmName, Snapshots: []string{"backup", "sold1"}, Devices: devices,
		Config: map[string]string{
			"volatile.uuid": archivedUuid, "volatile.eth0.hwaddr": archivedMac,
			"limits.cpu": "2", "limits.memory": "2048MiB", "user.titan-name": "web-1",
		},
	}
}

type restoreScene struct {
	cp      *testsupport.ControlPlane
	p       *testsupport.Recorder
	bucket  *testsupport.Bucket
	handler *Restore
}

// newRestoreScene is a tenant's project where the original VM runs with its volume attached and a snapshot the
// tenant took; the volume's copy, which its own volume.restore has finished, and a volume detached since the backup,
// both free; a 40 GiB pool 4 GiB used; and the bucket with the archive given as the original's. The cluster's pool is
// not the default one, and the fake answers a call whatever pool it names: when the test ends, every pool a call named
// must have been the cluster's.
func newRestoreScene(t *testing.T, archive testsupport.ArchiveDescription) *restoreScene {
	t.Helper()
	p := testsupport.NewRecorder()
	t.Cleanup(func() {
		for _, pool := range p.Pools {
			if pool != restorePool {
				t.Errorf("a call named pool %q, want the cluster's %q in every call: %q", pool, restorePool, p.Pools)
				return
			}
		}
	})
	p.PoolSpace = api.ResourcesStoragePoolSpace{Total: 40 * gib, Used: 4 * gib}
	p.Projects[project] = true
	p.Instances[project+"/"+sourceVmName] = &api.Instance{
		Name: sourceVmName, Status: "Running", StatusCode: api.Running,
		InstancePut: api.InstancePut{
			Devices: map[string]map[string]string{
				"root": rootDisk(), "eth0": {"type": "nic", "network": "net-1"}, sourceVolumeName: volumeDisk(sourceVolumeName),
			},
			Config: map[string]string{"volatile.uuid": archivedUuid, "volatile.eth0.hwaddr": archivedMac, "user.titan-name": "web-1"},
		},
	}
	p.InstanceSnapshots[project+"/"+sourceVmName+"/skept"] = true
	for _, volume := range []string{sourceVolumeName, newVolumeName, detachedVolumeName} {
		p.Volumes[project+"/"+volume] = &api.StorageVolume{Name: volume, Type: "custom", Project: project}
	}
	p.Volumes[project+"/"+newVolumeName].Config = map[string]string{"user.titan-name": "data-1-copy", "user.titan-restore": volumeRequestId}
	// What a full read answers of the copy: an import leaves it stopped.
	p.VirtualMachines = []api.InstanceFull{{Instance: api.Instance{Name: instanceName, Status: "Stopped", StatusCode: api.Stopped}}}
	bucket := testsupport.NewBucket()
	bucket.Objects[restoreArchiveKey] = archive.TarGz()
	cp := testsupport.NewControlPlane(t)
	handler := NewRestore(cp.Client(), cluster.Fixed{clusterId: {Provider: p, StoragePool: restorePool}}, bucket)
	return &restoreScene{cp: cp, p: p, bucket: bucket, handler: handler}
}

// restoreLeased is the command of a VM with a 10 GiB root disk; a nil value in changes leaves that field out.
func restoreLeased(volumes []map[string]string, changes map[string]any) command.Leased {
	if volumes == nil {
		volumes = []map[string]string{}
	}
	payload := map[string]any{
		"vmId": vmId, "requestId": requestId, "name": restoredName, "sourceVmId": sourceVmId, "backupKey": restoreBackupKey,
		"tenantId": restoreTenantId, "projectName": project, "diskSizeGb": 10, "volumes": volumes,
	}
	for field, value := range changes {
		if value == nil {
			delete(payload, field)
			continue
		}
		payload[field] = value
	}
	return leased("vm.restore", payload)
}

func (s *restoreScene) run(l command.Leased) error {
	return s.handler.Run(context.Background(), l, testsupport.DiscardLogger())
}

func (s *restoreScene) giveUp(l command.Leased, cause error) error {
	return s.handler.GiveUp(context.Background(), l, cause, testsupport.DiscardLogger())
}

// alreadyImported puts the copy on Incus as an earlier run left it: stopped, under the original's display name, with
// the devices and snapshots given.
func (s *restoreScene) alreadyImported(devices map[string]map[string]string, snapshots ...string) {
	s.p.Instances[project+"/"+instanceName] = &api.Instance{
		Name: instanceName, Status: "Stopped", StatusCode: api.Stopped,
		InstancePut: api.InstancePut{
			Devices: devices,
			Config: map[string]string{
				"volatile.uuid": "00000000-0000-4000-8000-00000000cafe", "volatile.eth0.hwaddr": "10:66:6a:00:ca:fe",
				"limits.cpu": "2", "limits.memory": "2048MiB", "user.titan-name": "web-1",
			},
		},
	}
	for _, snapshot := range snapshots {
		s.p.InstanceSnapshots[project+"/"+instanceName+"/"+snapshot] = true
	}
}

// usedSinceItWasRestored makes the copy what a VM the tenant has had for a while is: running, with another volume
// attached, a snapshot the tenant took and the "backup" snapshot the daily backup takes on the instance itself.
func (s *restoreScene) usedSinceItWasRestored(t *testing.T) {
	t.Helper()
	copied := s.copyOnIncus(t)
	copied.Status, copied.StatusCode = "Running", api.Running
	copied.Devices[detachedVolumeName] = volumeDisk(detachedVolumeName)
	s.p.InstanceSnapshots[project+"/"+instanceName+"/backup"] = true
	s.p.InstanceSnapshots[project+"/"+instanceName+"/stenant"] = true
	s.p.VirtualMachines[0].Status, s.p.VirtualMachines[0].StatusCode = "Running", api.Running
}

// isAsTheTenantHasIt says whether the copy is still what usedSinceItWasRestored left, with the devices and config
// given.
func (s *restoreScene) isAsTheTenantHasIt(t *testing.T, devices map[string]map[string]string, config map[string]string) bool {
	t.Helper()
	copied := s.copyOnIncus(t)
	return copied.StatusCode == api.Running && sameDevices(copied.Devices, devices) && maps.Equal(copied.Config, config) &&
		slices.Equal(s.snapshotsOf(t, instanceName), []string{"backup", "stenant"})
}

// secondCopyFinished puts on Incus the copy of the detached volume, as its own volume.restore leaves it.
func (s *restoreScene) secondCopyFinished() {
	s.p.Volumes[project+"/"+secondNewVolumeName] = &api.StorageVolume{
		Name: secondNewVolumeName, Type: "custom", Project: project,
		StorageVolumePut: api.StorageVolumePut{Config: map[string]string{"user.titan-name": "data-2-copy", "user.titan-restore": secondVolumeRequestId}},
	}
}

// importStillRunning leaves on Incus the operation of an import of the copy nobody waits on any more.
func (s *restoreScene) importStillRunning() {
	s.p.RunningOperations[project+"/"+instanceName] = []api.Operation{
		{ID: "op-import", Class: "task", Description: "Restoring backup", Status: "Running", StatusCode: api.Running},
	}
}

func (s *restoreScene) putPlaceholders(names ...string) {
	for _, name := range names {
		s.p.Volumes[project+"/"+name] = &api.StorageVolume{Name: name, Type: "custom", Project: project}
	}
}

// copyOnIncus is the copy as Incus holds it.
func (s *restoreScene) copyOnIncus(t *testing.T) *api.Instance {
	t.Helper()
	instance, found := s.p.Instances[project+"/"+instanceName]
	if !found {
		t.Fatalf("the copy is not on Incus; instances: %v", slices.Sorted(maps.Keys(s.p.Instances)))
	}
	return instance
}

func (s *restoreScene) snapshotsOf(t *testing.T, instance string) []string {
	t.Helper()
	snapshots, err := s.p.ListInstanceSnapshots(context.Background(), project, instance)
	if err != nil {
		t.Fatal(err)
	}
	names := []string{}
	for _, snapshot := range snapshots {
		names = append(names, snapshot.Name)
	}
	return names
}

// reportBody is the one report on the copy's create request, as sent.
func (s *restoreScene) reportBody(t *testing.T) string {
	t.Helper()
	requests := s.cp.RequestsTo(requestRoute)
	if len(requests) != 1 || len(s.cp.Requests()) != 1 {
		t.Fatalf("want one report, on %s; got %+v", requestRoute, s.cp.Requests())
	}
	return requests[0].Body
}

const (
	doneStopped = `{"outcome":"Done","externalId":"` + instanceName + `","observed":{"status":"Stopped"}}`
	doneRunning = `{"outcome":"Done","externalId":"` + instanceName + `","observed":{"status":"Running"}}`
)

func sameDevices(got map[string]map[string]string, want map[string]map[string]string) bool {
	return maps.EqualFunc(got, want, maps.Equal[map[string]string])
}

func isPermanent(err error) bool {
	return err != nil && !command.IsRetryable(err)
}

func isRetryable(err error) bool {
	return err != nil && command.IsRetryable(err)
}

func TestRestore_ImportsTheArchiveOntoTheVolumesCopy_RollsBackDiskOnly_DropsItsSnapshots_SetsItsDevicesAndName_AndEndsTheCreateDoneStopped(t *testing.T) {
	s := newRestoreScene(t, archiveOf(sourceVolumeName))

	err := s.run(restoreLeased(sourceToNewVolume, nil))

	if err != nil {
		t.Fatal(err)
	}
	target := project + "/" + instanceName
	want := []string{
		"import-vm " + target,
		"rollback-vm " + target + "/backup",
		"delete-vm-snapshot " + target + "/backup",
		"delete-vm-snapshot " + target + "/sold1",
		"update-vm " + target,
	}
	if !slices.Equal(s.p.Writes, want) {
		t.Errorf("writes\n got %q\nwant %q", s.p.Writes, want)
	}
	if !slices.Equal(s.bucket.Opened, []string{restoreArchiveKey, restoreArchiveKey}) || s.bucket.Open != 0 {
		t.Errorf("archives opened %q, %d left open; want %s for its index and for the import, closed", s.bucket.Opened, s.bucket.Open, restoreArchiveKey)
	}

	// The archived device names the original's volume, which the original still holds: the import must point it at
	// the copy, and must not bring the original's uuid (its OVN port is named after it) or its MAC.
	imported := s.p.ImportedInstances
	if len(imported) != 1 {
		t.Fatalf("imports: %+v", imported)
	}
	isTheArchiveInTheClustersPool := imported[0].Pool == restorePool && imported[0].Body == s.bucket.Objects[restoreArchiveKey]
	wantDeviceOverrides := []string{sourceVolumeName + ",source=" + newVolumeName}
	wantConfigOverrides := []string{"volatile.eth0.hwaddr=", "volatile.uuid="}
	if !isTheArchiveInTheClustersPool || !slices.Equal(imported[0].Overrides.Devices, wantDeviceOverrides) ||
		!slices.Equal(imported[0].Overrides.Config, wantConfigOverrides) {
		t.Errorf("import: pool %q, overrides %+v; want %q and %q", imported[0].Pool, imported[0].Overrides, wantDeviceOverrides, wantConfigOverrides)
	}

	if len(s.p.UpdatedInstances) != 2 {
		t.Fatalf("updates: %+v", s.p.UpdatedInstances)
	}
	// A full rollback would put the archived devices and volatile keys back.
	rollback := s.p.UpdatedInstances[0]
	isDiskOnly := rollback.Restore == "backup" && rollback.DiskOnly && len(rollback.Devices) == 0 && len(rollback.Config) == 0
	if !isDiskOnly {
		t.Errorf("rollback: %+v", rollback)
	}
	wantDevices := map[string]map[string]string{
		"root": rootDisk(), "eth0": {"type": "nic", "network": "net-1"}, newVolumeName: volumeDisk(newVolumeName),
	}
	fixed := s.p.UpdatedInstances[1]
	if !sameDevices(fixed.Devices, wantDevices) {
		t.Errorf("devices set\n got %v\nwant %v", fixed.Devices, wantDevices)
	}
	uuid, mac := fixed.Config["volatile.uuid"], fixed.Config["volatile.eth0.hwaddr"]
	hasItsOwnIdentity := uuid != "" && uuid != archivedUuid && mac != "" && mac != archivedMac
	keepsItsSizing := fixed.Config["limits.cpu"] == "2" && fixed.Config["limits.memory"] == "2048MiB"
	// The same write, the last one of the copy, says which request finished it.
	isNamedAndFinished := fixed.Config["user.titan-name"] == restoredName && fixed.Config["user.titan-restore"] == requestId
	if !isNamedAndFinished || !hasItsOwnIdentity || !keepsItsSizing || len(fixed.Config) != 6 {
		t.Errorf("config set: %v", fixed.Config)
	}

	if !sameDevices(s.copyOnIncus(t).Devices, wantDevices) {
		t.Errorf("the copy's devices: %v", s.copyOnIncus(t).Devices)
	}
	if left := s.snapshotsOf(t, instanceName); len(left) != 0 {
		t.Errorf("snapshots left on the copy: %q", left)
	}
	if len(s.p.StateChanges) != 0 {
		t.Errorf("the copy was started or stopped: %+v", s.p.StateChanges)
	}
	if got := s.reportBody(t); got != doneStopped {
		t.Errorf("report\n got %s\nwant %s", got, doneStopped)
	}
}

// Every write of a run, up to its last one of the copy, which fails here, and the give-up's removal after it.
func TestRestore_LeavesTheOriginalVmAndTheOriginalVolumesAsTheyAre_AndNoWriteNamesThem(t *testing.T) {
	s := newRestoreScene(t, archiveOf(sourceVolumeName, detachedVolumeName))
	original := s.p.Instances[project+"/"+sourceVmName]
	devicesBefore, configBefore := cloneDevices(original.Devices), maps.Clone(original.Config)
	// The copy is first read once it is imported and rolled back: the update after that read sets its devices.
	s.p.AfterGetInstance = func() { s.p.UpdateInstanceErr = errors.New("locked") }

	if err := s.run(restoreLeased(sourceToNewVolume, nil)); err == nil {
		t.Fatal("the run passed although its last write of the copy failed")
	}
	if err := s.giveUp(restoreLeased(sourceToNewVolume, nil), errors.New("set the devices: locked")); err != nil {
		t.Fatal(err)
	}

	target := project + "/" + instanceName
	want := []string{
		"create-volume " + project + "/" + placeholder0,
		"import-vm " + target,
		"rollback-vm " + target + "/backup",
		"delete-vm-snapshot " + target + "/backup",
		"delete-vm-snapshot " + target + "/sold1",
		"update-vm " + target,
		"delete-vm " + target,
		"delete-volume " + project + "/" + placeholder0,
	}
	if !slices.Equal(s.p.Writes, want) {
		t.Fatalf("writes\n got %q\nwant %q: the check below needs the whole run and the give-up's removal", s.p.Writes, want)
	}
	for _, write := range s.p.Writes {
		for _, original := range []string{sourceVmName, sourceVolumeName, detachedVolumeName} {
			if strings.Contains(write, original) {
				t.Errorf("a write names %s: %q", original, write)
			}
		}
	}
	kept := s.p.Instances[project+"/"+sourceVmName]
	isUntouched := kept == original && kept.StatusCode == api.Running && sameDevices(kept.Devices, devicesBefore) &&
		maps.Equal(kept.Config, configBefore) && slices.Equal(s.snapshotsOf(t, sourceVmName), []string{"skept"})
	if !isUntouched {
		t.Errorf("the original changed: %+v, snapshots %q", kept, s.snapshotsOf(t, sourceVmName))
	}
	for _, volume := range []string{sourceVolumeName, detachedVolumeName} {
		if _, there := s.p.Volumes[project+"/"+volume]; !there {
			t.Errorf("volume %s is gone", volume)
		}
	}
	if len(s.p.StateChanges) != 0 || len(s.p.PatchedInstances) != 0 || len(s.p.PatchedVolumeConfigs) != 0 {
		t.Errorf("a state change or a patch was made: %+v %+v %+v", s.p.StateChanges, s.p.PatchedInstances, s.p.PatchedVolumeConfigs)
	}
}

func cloneDevices(devices map[string]map[string]string) map[string]map[string]string {
	cloned := map[string]map[string]string{}
	for name, settings := range devices {
		cloned[name] = maps.Clone(settings)
	}
	return cloned
}

// A volume attached when the backup was taken and not restored with the VM: the import fails on a device whose volume is not
// there, and would take the tenant's own volume if it is there and free. A placeholder stands in for the import, and
// goes with the device.
func TestRestore_AVolumeOfTheArchiveThatIsNotRestored_GetsAPlaceholderForTheImport_RemovedWithItsDevice(t *testing.T) {
	for name, archive := range map[string]testsupport.ArchiveDescription{
		"still attached to the original": archiveOf(sourceVolumeName),
		"detached since, and free":       archiveOf(detachedVolumeName),
		"gone since":                     archiveOf("volgone"),
	} {
		s := newRestoreScene(t, archive)
		archivedVolume := slices.Sorted(maps.Keys(archive.Devices))[2]

		err := s.run(restoreLeased(nil, nil))

		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		target := project + "/" + instanceName
		want := []string{
			"create-volume " + project + "/" + placeholder0,
			"import-vm " + target,
			"rollback-vm " + target + "/backup",
			"delete-vm-snapshot " + target + "/backup",
			"delete-vm-snapshot " + target + "/sold1",
			"update-vm " + target,
			"delete-volume " + project + "/" + placeholder0,
		}
		if !slices.Equal(s.p.Writes, want) {
			t.Errorf("%s: writes\n got %q\nwant %q", name, s.p.Writes, want)
		}
		made := s.p.CreatedVolumes
		isASmallBlockVolume := len(made) == 1 && made[0].Name == placeholder0 && made[0].Type == "custom" && made[0].ContentType == "block" &&
			maps.Equal(made[0].Config, map[string]string{"size": "1GiB"})
		if !isASmallBlockVolume {
			t.Errorf("%s: placeholder made: %+v", name, made)
		}
		wantOverrides := []string{archivedVolume + ",source=" + placeholder0}
		if got := s.p.ImportedInstances[0].Overrides.Devices; !slices.Equal(got, wantOverrides) {
			t.Errorf("%s: device overrides %q, want %q", name, got, wantOverrides)
		}
		wantDevices := map[string]map[string]string{"root": rootDisk(), "eth0": {"type": "nic", "network": "net-1"}}
		if !sameDevices(s.copyOnIncus(t).Devices, wantDevices) {
			t.Errorf("%s: the copy's devices: %v", name, s.copyOnIncus(t).Devices)
		}
		if _, left := s.p.Volumes[project+"/"+placeholder0]; left {
			t.Errorf("%s: the placeholder is still there", name)
		}
		for _, write := range s.p.Writes {
			if strings.Contains(write, archivedVolume) {
				t.Errorf("%s: a write names the volume that is not restored: %q", name, write)
			}
		}
	}
}

func TestRestore_TwoVolumesOfTheArchiveThatAreNotRestored_GetAPlaceholderEach_InTheOrderOfTheirDevices(t *testing.T) {
	s := newRestoreScene(t, archiveOf(sourceVolumeName, detachedVolumeName))

	err := s.run(restoreLeased(nil, nil))

	if err != nil {
		t.Fatal(err)
	}
	target := project + "/" + instanceName
	want := []string{
		"create-volume " + project + "/" + placeholder0,
		"create-volume " + project + "/" + placeholder1,
		"import-vm " + target,
		"rollback-vm " + target + "/backup",
		"delete-vm-snapshot " + target + "/backup",
		"delete-vm-snapshot " + target + "/sold1",
		"update-vm " + target,
		"delete-volume " + project + "/" + placeholder1,
		"delete-volume " + project + "/" + placeholder0,
	}
	if !slices.Equal(s.p.Writes, want) {
		t.Errorf("writes\n got %q\nwant %q", s.p.Writes, want)
	}
	wantOverrides := []string{sourceVolumeName + ",source=" + placeholder0, detachedVolumeName + ",source=" + placeholder1}
	if got := s.p.ImportedInstances[0].Overrides.Devices; !slices.Equal(got, wantOverrides) {
		t.Errorf("device overrides %q, want %q", got, wantOverrides)
	}
}

// One volume restored with the VM, one attached at the backup and not restored: the first goes to its copy, the
// second to a placeholder, and only the copy stays attached.
func TestRestore_ARestoredVolumeBesideOneThatIsNot_KeepsTheCopyAttached_AndDropsThePlaceholder(t *testing.T) {
	s := newRestoreScene(t, archiveOf(sourceVolumeName, detachedVolumeName))

	err := s.run(restoreLeased(sourceToNewVolume, nil))

	if err != nil {
		t.Fatal(err)
	}
	wantOverrides := []string{sourceVolumeName + ",source=" + newVolumeName, detachedVolumeName + ",source=" + placeholder0}
	if got := s.p.ImportedInstances[0].Overrides.Devices; !slices.Equal(got, wantOverrides) {
		t.Errorf("device overrides %q, want %q", got, wantOverrides)
	}
	wantDevices := map[string]map[string]string{
		"root": rootDisk(), "eth0": {"type": "nic", "network": "net-1"}, newVolumeName: volumeDisk(newVolumeName),
	}
	if !sameDevices(s.copyOnIncus(t).Devices, wantDevices) {
		t.Errorf("the copy's devices: %v", s.copyOnIncus(t).Devices)
	}
	_, placeholderLeft := s.p.Volumes[project+"/"+placeholder0]
	_, copyKept := s.p.Volumes[project+"/"+newVolumeName]
	if placeholderLeft || !copyKept {
		t.Errorf("placeholder left %v, the volume's copy kept %v", placeholderLeft, copyKept)
	}
}

// A volume attached after the backup was taken: the archive has no device for it, the update adds one.
func TestRestore_ARestoredVolumeTheArchiveHasNoDeviceFor_IsAttachedByTheUpdate(t *testing.T) {
	s := newRestoreScene(t, archiveOf())

	err := s.run(restoreLeased(sourceToNewVolume, nil))

	if err != nil {
		t.Fatal(err)
	}
	if got := s.p.ImportedInstances[0].Overrides.Devices; len(got) != 0 {
		t.Errorf("device overrides %q, want none", got)
	}
	if len(s.p.CreatedVolumes) != 0 {
		t.Errorf("a placeholder was made: %+v", s.p.CreatedVolumes)
	}
	wantDevices := map[string]map[string]string{
		"root": rootDisk(), "eth0": {"type": "nic", "network": "net-1"}, newVolumeName: volumeDisk(newVolumeName),
	}
	if !sameDevices(s.copyOnIncus(t).Devices, wantDevices) {
		t.Errorf("the copy's devices: %v", s.copyOnIncus(t).Devices)
	}
}

// An archive taken after a whole-cluster restore carries the address that restore pinned: a copy importing it would
// take the original's address.
func TestRestore_ANicWithAPinnedAddress_IsImportedWithoutIt(t *testing.T) {
	archive := archiveOf()
	archive.Devices["eth0"]["ipv4.address"] = "10.189.217.2"
	s := newRestoreScene(t, archive)

	err := s.run(restoreLeased(nil, nil))

	if err != nil {
		t.Fatal(err)
	}
	overrides := s.p.ImportedInstances[0].Overrides
	if !slices.Equal(overrides.Devices, []string{"eth0,ipv4.address="}) || !slices.Equal(overrides.Config, []string{"volatile.eth0.hwaddr=", "volatile.uuid="}) {
		t.Errorf("overrides: %+v", overrides)
	}
	if nic := s.copyOnIncus(t).Devices["eth0"]; !maps.Equal(nic, map[string]string{"type": "nic", "network": "net-1"}) {
		t.Errorf("the copy's NIC: %v", nic)
	}
}

// Each NIC has a MAC in the archive, and each copy of one needs its own; an address pinned on any of them is the
// original's to keep.
func TestRestore_EveryNic_IsImportedWithoutItsMacOrItsPinnedAddress_InTheOrderOfTheNames(t *testing.T) {
	archive := archiveOf()
	archive.Devices["eth0"]["ipv4.address"] = "10.189.217.2"
	archive.Devices["eth1"] = map[string]string{"type": "nic", "network": "net-2", "ipv4.address": "10.189.218.2"}
	archive.Devices["eth2"] = map[string]string{"type": "nic", "network": "net-3"}
	archive.Config["volatile.eth1.hwaddr"] = "10:66:6a:9c:6a:02"
	archive.Config["volatile.eth2.hwaddr"] = "10:66:6a:9c:6a:03"
	s := newRestoreScene(t, archive)

	err := s.run(restoreLeased(nil, nil))

	if err != nil {
		t.Fatal(err)
	}
	wantConfig := []string{"volatile.eth0.hwaddr=", "volatile.eth1.hwaddr=", "volatile.eth2.hwaddr=", "volatile.uuid="}
	wantDevices := []string{"eth0,ipv4.address=", "eth1,ipv4.address="}
	if got := s.p.ImportedInstances[0].Overrides; !slices.Equal(got.Config, wantConfig) || !slices.Equal(got.Devices, wantDevices) {
		t.Errorf("overrides %+v, want config %q and devices %q", got, wantConfig, wantDevices)
	}
	copied := s.copyOnIncus(t)
	if copied.Config["volatile.eth1.hwaddr"] == "" || copied.Config["volatile.eth1.hwaddr"] == "10:66:6a:9c:6a:02" {
		t.Errorf("the copy's second MAC: %q", copied.Config["volatile.eth1.hwaddr"])
	}
	if nic := copied.Devices["eth1"]; !maps.Equal(nic, map[string]string{"type": "nic", "network": "net-2"}) {
		t.Errorf("the copy's second NIC: %v", nic)
	}
}

// Two volumes restored with the VM: each archived device goes to its own volume's copy, and each copy ends under a
// device named after it.
func TestRestore_TwoVolumesRestoredWithTheVm_EachGoesToItsOwnCopy_AndNoPlaceholderIsMade(t *testing.T) {
	s := newRestoreScene(t, archiveOf(sourceVolumeName, detachedVolumeName))
	s.secondCopyFinished()

	err := s.run(restoreLeased(bothVolumes, nil))

	if err != nil {
		t.Fatal(err)
	}
	target := project + "/" + instanceName
	want := []string{
		"import-vm " + target,
		"rollback-vm " + target + "/backup",
		"delete-vm-snapshot " + target + "/backup",
		"delete-vm-snapshot " + target + "/sold1",
		"update-vm " + target,
	}
	if !slices.Equal(s.p.Writes, want) {
		t.Errorf("writes\n got %q\nwant %q", s.p.Writes, want)
	}
	wantOverrides := []string{sourceVolumeName + ",source=" + newVolumeName, detachedVolumeName + ",source=" + secondNewVolumeName}
	if got := s.p.ImportedInstances[0].Overrides.Devices; !slices.Equal(got, wantOverrides) {
		t.Errorf("device overrides %q, want %q", got, wantOverrides)
	}
	wantDevices := map[string]map[string]string{
		"root": rootDisk(), "eth0": {"type": "nic", "network": "net-1"},
		newVolumeName: volumeDisk(newVolumeName), secondNewVolumeName: volumeDisk(secondNewVolumeName),
	}
	if len(s.p.UpdatedInstances) != 2 || !sameDevices(s.p.UpdatedInstances[1].Devices, wantDevices) {
		t.Errorf("devices set: %+v, want %v", s.p.UpdatedInstances, wantDevices)
	}
	if !sameDevices(s.copyOnIncus(t).Devices, wantDevices) {
		t.Errorf("the copy's devices\n got %v\nwant %v", s.copyOnIncus(t).Devices, wantDevices)
	}
	if len(s.p.CreatedVolumes) != 0 {
		t.Errorf("a placeholder was made: %+v", s.p.CreatedVolumes)
	}
	if got := s.reportBody(t); got != doneStopped {
		t.Errorf("report: %s", got)
	}
}

// Every copy is asked about, not the first alone: the VM waits for the one that is not finished, whichever it is.
func TestRestore_OneOfTwoVolumesCopiesNotFinished_IsRetryable_NamingIt_BeforeTheArchiveIsOpenedOrAnythingWritten(t *testing.T) {
	for name, tc := range map[string]struct {
		leave      func(*restoreScene)
		unfinished string
	}{
		"the second is not on Incus yet": {func(s *restoreScene) {}, secondNewVolumeName},
		"the second is imported, and not finished": {func(s *restoreScene) {
			s.secondCopyFinished()
			delete(s.p.Volumes[project+"/"+secondNewVolumeName].Config, "user.titan-restore")
		}, secondNewVolumeName},
		"the first is not finished": {func(s *restoreScene) {
			s.secondCopyFinished()
			delete(s.p.Volumes[project+"/"+newVolumeName].Config, "user.titan-restore")
		}, newVolumeName},
		// Each copy is finished by the request of its own restore, not by the other's.
		"each carries the other's request": {func(s *restoreScene) {
			s.secondCopyFinished()
			s.p.Volumes[project+"/"+newVolumeName].Config["user.titan-restore"] = secondVolumeRequestId
			s.p.Volumes[project+"/"+secondNewVolumeName].Config["user.titan-restore"] = volumeRequestId
		}, newVolumeName},
	} {
		s := newRestoreScene(t, archiveOf(sourceVolumeName, detachedVolumeName))
		tc.leave(s)

		err := s.run(restoreLeased(bothVolumes, nil))

		if !isRetryable(err) || !strings.Contains(err.Error(), "volume "+tc.unfinished+" is not restored yet") {
			t.Errorf("%s: err = %v, want a retryable error that names %s", name, err, tc.unfinished)
		}
		if len(s.p.Writes) != 0 || len(s.bucket.Opened) != 0 || len(s.cp.Requests()) != 0 {
			t.Errorf("%s: wrote %q, opened %q, reported %+v; want nothing", name, s.p.Writes, s.bucket.Opened, s.cp.Requests())
		}
	}
}

// A volume's device is a disk with a source that is not the root disk; no other disk is pointed elsewhere for the
// import or removed after it.
func TestRestore_ADiskThatIsNoVolumes_IsImportedAsArchived_AndKept(t *testing.T) {
	archive := archiveOf()
	archive.Devices["root"]["source"] = "rootsource"
	archive.Devices["scratch"] = map[string]string{"type": "disk", "pool": restorePool, "path": "/scratch"}
	s := newRestoreScene(t, archive)
	s.p.Volumes[project+"/rootsource"] = &api.StorageVolume{Name: "rootsource", Type: "custom", Project: project}

	err := s.run(restoreLeased(nil, nil))

	if err != nil {
		t.Fatal(err)
	}
	if got := s.p.ImportedInstances[0].Overrides.Devices; len(got) != 0 || len(s.p.CreatedVolumes) != 0 {
		t.Errorf("device overrides %q, placeholders %+v; want none", got, s.p.CreatedVolumes)
	}
	if !sameDevices(s.copyOnIncus(t).Devices, archive.Devices) {
		t.Errorf("the copy's devices\n got %v\nwant %v", s.copyOnIncus(t).Devices, archive.Devices)
	}
}

func TestRestore_AVmWithoutVolumes_IsImportedAndNamed_WithNoPlaceholder(t *testing.T) {
	s := newRestoreScene(t, archiveOf())

	err := s.run(restoreLeased(nil, map[string]any{"volumes": nil}))

	if err != nil {
		t.Fatal(err)
	}
	target := project + "/" + instanceName
	want := []string{
		"import-vm " + target,
		"rollback-vm " + target + "/backup",
		"delete-vm-snapshot " + target + "/backup",
		"delete-vm-snapshot " + target + "/sold1",
		"update-vm " + target,
	}
	if !slices.Equal(s.p.Writes, want) {
		t.Errorf("writes\n got %q\nwant %q", s.p.Writes, want)
	}
	if s.copyOnIncus(t).Config["user.titan-name"] != restoredName {
		t.Errorf("the copy's config: %v", s.copyOnIncus(t).Config)
	}
	if got := s.reportBody(t); got != doneStopped {
		t.Errorf("report: %s", got)
	}
}

// Each volume's copy has a command of its own, which normally ends first. Until it has, the VM waits: the import
// would fail on a volume that is not there, after the whole upload, and a volume imported but not rolled back yet holds
// what the original held after its backup. Only the copy's own restore says it is finished, with its request on the
// copy; another request there came with the archive. The VM waits before it asks the pool for room, which the
// volume's own import is about to take.
func TestRestore_AVolumesCopyItsOwnRestoreHasNotFinished_IsRetryable_BeforeTheArchiveIsOpenedOrAnythingWritten(t *testing.T) {
	for name, leave := range map[string]func(*restoreScene){
		"not on Incus yet": func(s *restoreScene) { delete(s.p.Volumes, project+"/"+newVolumeName) },
		"imported, and no request on it": func(s *restoreScene) {
			s.p.Volumes[project+"/"+newVolumeName].Config = map[string]string{"user.titan-name": "data-1"}
		},
		"imported, with no config at all": func(s *restoreScene) { s.p.Volumes[project+"/"+newVolumeName].Config = nil },
		"imported, with the request of an earlier restore": func(s *restoreScene) {
			s.p.Volumes[project+"/"+newVolumeName].Config = map[string]string{"user.titan-restore": earlierRequestId}
		},
		// The VM's own request finishes the VM's copy, never a volume's.
		"imported, with the request of the vm's restore": func(s *restoreScene) {
			s.p.Volumes[project+"/"+newVolumeName].Config = map[string]string{"user.titan-restore": requestId}
		},
	} {
		s := newRestoreScene(t, archiveOf(sourceVolumeName))
		leave(s)
		s.p.PoolSpace = api.ResourcesStoragePoolSpace{Total: 40 * gib, Used: 40 * gib}

		err := s.run(restoreLeased(sourceToNewVolume, nil))

		if !isRetryable(err) || !strings.Contains(err.Error(), "volume "+newVolumeName+" is not restored yet") {
			t.Errorf("%s: err = %v, want a retryable error that names the volume", name, err)
		}
		if len(s.p.Writes) != 0 || len(s.bucket.Opened) != 0 || len(s.cp.Requests()) != 0 {
			t.Errorf("%s: wrote %q, opened %q, reported %+v; want nothing", name, s.p.Writes, s.bucket.Opened, s.cp.Requests())
		}
	}
}

// An archive from before backups took the "backup" snapshot cannot be put back to the moment it was taken.
func TestRestore_AnArchiveWithoutTheBackupSnapshot_IsPermanent_AndItsImportAndPlaceholderAreRemoved(t *testing.T) {
	archive := archiveOf(detachedVolumeName)
	archive.Snapshots = []string{"sold1"}
	s := newRestoreScene(t, archive)

	err := s.run(restoreLeased(nil, nil))

	if !isPermanent(err) {
		t.Fatalf("want a permanent error, got %v", err)
	}
	wantText := "archive " + restoreArchiveKey + ` has no "backup" snapshot: an old-format archive, not usable`
	if err.Error() != wantText {
		t.Errorf("error\n got %q\nwant %q", err, wantText)
	}
	want := []string{
		"create-volume " + project + "/" + placeholder0,
		"import-vm " + project + "/" + instanceName,
		"delete-vm " + project + "/" + instanceName,
		"delete-volume " + project + "/" + placeholder0,
	}
	if !slices.Equal(s.p.Writes, want) {
		t.Errorf("writes\n got %q\nwant %q", s.p.Writes, want)
	}
	if len(s.cp.Requests()) != 0 {
		t.Errorf("reported on a run that failed: %+v", s.cp.Requests())
	}
}

// The rule of restore.CheckPoolRoom, on the root disk the command carries: 1.5 x 10 GiB + a tenth of the 40 GiB pool.
func TestRestore_APoolOneByteShort_IsPermanentBeforeAnythingIsWritten_AndOneExactlyLargeEnough_Imports(t *testing.T) {
	required := 15*gib + 4*gib
	for name, tc := range map[string]struct {
		free uint64
		ok   bool
	}{"exactly": {required, true}, "one byte less": {required - 1, false}} {
		s := newRestoreScene(t, archiveOf(detachedVolumeName))
		s.p.PoolSpace = api.ResourcesStoragePoolSpace{Total: 40 * gib, Used: 40*gib - tc.free}

		err := s.run(restoreLeased(nil, nil))

		if tc.ok {
			if err != nil || len(s.p.ImportedInstances) != 1 {
				t.Errorf("%s: err %v, imports %+v; want the import", name, err, s.p.ImportedInstances)
			}
			continue
		}
		if !isPermanent(err) || !strings.Contains(err.Error(), "importing 10.0 GiB needs 19.0 GiB") {
			t.Errorf("%s: err = %v, want a permanent refusal that says the size and the need", name, err)
		}
		if len(s.p.Writes) != 0 || len(s.bucket.Opened) != 0 || len(s.cp.Requests()) != 0 {
			t.Errorf("%s: wrote %q, opened %q, reported %+v; want nothing, not even a placeholder", name, s.p.Writes, s.bucket.Opened, s.cp.Requests())
		}
	}
}

// A pool that could not be read says nothing of its room: the next lease asks again.
func TestRestore_APoolThatCannotBeRead_IsRetryable_AndWritesNothing(t *testing.T) {
	s := newRestoreScene(t, archiveOf(detachedVolumeName))
	s.p.PoolResourcesErr = errors.New("incus is busy")

	err := s.run(restoreLeased(nil, nil))

	if !isRetryable(err) || len(s.p.Writes) != 0 {
		t.Fatalf("err = %v, wrote %q; want a retryable error and no write", err, s.p.Writes)
	}
}

func TestRestore_APayloadThatIsNotUsable_IsPermanent_AndCallsNothing(t *testing.T) {
	pair := func(source string, copied string) map[string]string {
		return map[string]string{"sourceVolumeId": source, "newVolumeId": copied, "requestId": volumeRequestId}
	}
	const anotherVolumeId = "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff"
	commands := map[string]command.Leased{
		"not json": {Kind: "vm.restore", ClusterId: clusterId, Payload: []byte("{")},
		// Whatever is under the copy's name is taken for this command's own import, and written.
		"the original's own id":           restoreLeased(nil, map[string]any{"vmId": sourceVmId}),
		"a disk size below zero":          restoreLeased(nil, map[string]any{"diskSizeGb": -1}),
		"a pair without its source":       restoreLeased([]map[string]string{{"newVolumeId": newVolumeId, "requestId": volumeRequestId}}, nil),
		"a pair without its copy":         restoreLeased([]map[string]string{{"sourceVolumeId": sourceVolumeId, "requestId": volumeRequestId}}, nil),
		"a pair without its request":      restoreLeased([]map[string]string{{"sourceVolumeId": sourceVolumeId, "newVolumeId": newVolumeId}}, nil),
		"a pair with an empty request":    restoreLeased([]map[string]string{{"sourceVolumeId": sourceVolumeId, "newVolumeId": newVolumeId, "requestId": ""}}, nil),
		"a pair whose source is no id":    restoreLeased([]map[string]string{pair("data", newVolumeId)}, nil),
		"a pair whose copy is no id":      restoreLeased([]map[string]string{pair(sourceVolumeId, "data")}, nil),
		"a volume copied onto itself":     restoreLeased([]map[string]string{pair(sourceVolumeId, sourceVolumeId)}, nil),
		"a copy that another pair copies": restoreLeased([]map[string]string{pair(sourceVolumeId, newVolumeId), pair(newVolumeId, anotherVolumeId)}, nil),
		"one volume copied twice":         restoreLeased([]map[string]string{pair(sourceVolumeId, newVolumeId), pair(sourceVolumeId, anotherVolumeId)}, nil),
		"two volumes with one copy":       restoreLeased([]map[string]string{pair(sourceVolumeId, newVolumeId), pair(anotherVolumeId, newVolumeId)}, nil),
	}
	for _, field := range []string{"vmId", "requestId", "sourceVmId", "backupKey", "tenantId", "projectName"} {
		commands["no "+field] = restoreLeased(nil, map[string]any{field: nil})
	}
	for field, value := range map[string]string{"vmId": "not-a-titan-id", "sourceVmId": "../other", "backupKey": "20261122T020000Z.tar.gz"} {
		commands["a "+field+" that is "+value] = restoreLeased(nil, map[string]any{field: value})
	}
	for name, l := range commands {
		s := newRestoreScene(t, archiveOf())

		err := s.run(l)

		if !isPermanent(err) {
			t.Errorf("%s: err = %v, want a permanent error", name, err)
		}
		if s.p.Calls != 0 || len(s.bucket.Opened) != 0 || len(s.cp.Requests()) != 0 {
			t.Errorf("%s: %d call(s) on Incus, opened %q, reported %+v; want nothing", name, s.p.Calls, s.bucket.Opened, s.cp.Requests())
		}
	}
}

// A run that died after its import left the copy with the "backup" snapshot: the rollback is still owed. It is found
// by what Incus holds, without the archive, the volumes' readiness or the pool's room being asked again.
func TestRestore_ASecondRun_FindingTheCopyImported_RollsItBackDiskOnly_WithoutImportingAgain(t *testing.T) {
	s := newRestoreScene(t, archiveOf(sourceVolumeName))
	s.alreadyImported(map[string]map[string]string{
		"root": rootDisk(), "eth0": {"type": "nic", "network": "net-1"}, sourceVolumeName: volumeDisk(newVolumeName),
	}, "backup", "sold1")
	s.p.PoolSpace = api.ResourcesStoragePoolSpace{Total: 40 * gib, Used: 40 * gib}
	delete(s.p.Volumes[project+"/"+newVolumeName].Config, "user.titan-restore")

	err := s.run(restoreLeased(sourceToNewVolume, nil))

	if err != nil {
		t.Fatal(err)
	}
	target := project + "/" + instanceName
	want := []string{
		"rollback-vm " + target + "/backup",
		"delete-vm-snapshot " + target + "/backup",
		"delete-vm-snapshot " + target + "/sold1",
		"update-vm " + target,
	}
	if !slices.Equal(s.p.Writes, want) {
		t.Errorf("writes\n got %q\nwant %q", s.p.Writes, want)
	}
	if rollback := s.p.UpdatedInstances[0]; rollback.Restore != "backup" || !rollback.DiskOnly {
		t.Errorf("rollback: %+v", rollback)
	}
	if len(s.bucket.Opened) != 0 || len(s.p.ImportedInstances) != 0 {
		t.Errorf("the archive was opened %q, imported %+v", s.bucket.Opened, s.p.ImportedInstances)
	}
	wantDevices := map[string]map[string]string{
		"root": rootDisk(), "eth0": {"type": "nic", "network": "net-1"}, newVolumeName: volumeDisk(newVolumeName),
	}
	if !sameDevices(s.copyOnIncus(t).Devices, wantDevices) {
		t.Errorf("the copy's devices: %v", s.copyOnIncus(t).Devices)
	}
	if got := s.reportBody(t); got != doneStopped {
		t.Errorf("report: %s", got)
	}
}

// No "backup" snapshot on a copy this run did not import: the run before rolled it back. Its placeholder is found by
// its name.
func TestRestore_ASecondRun_FindingTheCopyRolledBack_SetsItsDevices_AndRemovesThePlaceholderLeft(t *testing.T) {
	s := newRestoreScene(t, archiveOf(sourceVolumeName, detachedVolumeName))
	s.putPlaceholders(placeholder0)
	s.alreadyImported(map[string]map[string]string{
		"root": rootDisk(), "eth0": {"type": "nic", "network": "net-1"},
		sourceVolumeName: volumeDisk(newVolumeName), detachedVolumeName: volumeDisk(placeholder0),
	})
	// Neither the pool's room nor the volumes' copies are asked about again.
	s.p.PoolSpace = api.ResourcesStoragePoolSpace{Total: 40 * gib, Used: 40 * gib}
	delete(s.p.Volumes[project+"/"+newVolumeName].Config, "user.titan-restore")

	err := s.run(restoreLeased(sourceToNewVolume, nil))

	if err != nil {
		t.Fatal(err)
	}
	want := []string{"update-vm " + project + "/" + instanceName, "delete-volume " + project + "/" + placeholder0}
	if !slices.Equal(s.p.Writes, want) {
		t.Errorf("writes\n got %q\nwant %q", s.p.Writes, want)
	}
	if _, kept := s.p.Instances[project+"/"+instanceName]; !kept {
		t.Error("the copy was removed as an old-format import")
	}
	wantDevices := map[string]map[string]string{
		"root": rootDisk(), "eth0": {"type": "nic", "network": "net-1"}, newVolumeName: volumeDisk(newVolumeName),
	}
	if !sameDevices(s.copyOnIncus(t).Devices, wantDevices) {
		t.Errorf("the copy's devices: %v", s.copyOnIncus(t).Devices)
	}
	if got := s.reportBody(t); got != doneStopped {
		t.Errorf("report: %s", got)
	}
}

// The device named after a volume's copy is that copy's, whatever a run before left under the name.
func TestRestore_ADeviceNamedAfterAVolumesCopyThatPointsElsewhere_IsSetToTheCopy(t *testing.T) {
	s := newRestoreScene(t, archiveOf())
	s.alreadyImported(map[string]map[string]string{
		"root": rootDisk(), "eth0": {"type": "nic", "network": "net-1"}, newVolumeName: volumeDisk(detachedVolumeName),
	})

	err := s.run(restoreLeased(sourceToNewVolume, nil))

	if err != nil {
		t.Fatal(err)
	}
	wantDevices := map[string]map[string]string{
		"root": rootDisk(), "eth0": {"type": "nic", "network": "net-1"}, newVolumeName: volumeDisk(newVolumeName),
	}
	if !sameDevices(s.copyOnIncus(t).Devices, wantDevices) {
		t.Errorf("the copy's devices: %v", s.copyOnIncus(t).Devices)
	}
}

// withoutMaps answers an instance as a body with neither "devices" nor "config" decodes: both maps nil.
type withoutMaps struct {
	*testsupport.Recorder
}

func (w withoutMaps) GetInstanceWithEtag(ctx context.Context, project string, name string) (*api.Instance, string, error) {
	instance, etag, err := w.Recorder.GetInstanceWithEtag(ctx, project, name)
	if err == nil {
		instance.Devices, instance.Config = nil, nil
	}
	return instance, etag, err
}

func TestRestore_ACopyReadWithoutDevicesOrConfig_StillGetsItsVolumeAndItsName(t *testing.T) {
	s := newRestoreScene(t, archiveOf())
	s.alreadyImported(nil)
	handler := NewRestore(s.cp.Client(), cluster.Fixed{clusterId: {Provider: withoutMaps{s.p}, StoragePool: restorePool}}, s.bucket)

	err := handler.Run(context.Background(), restoreLeased(sourceToNewVolume, nil), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	set := s.p.UpdatedInstances
	isSet := len(set) == 1 && sameDevices(set[0].Devices, map[string]map[string]string{newVolumeName: volumeDisk(newVolumeName)}) &&
		maps.Equal(set[0].Config, map[string]string{"user.titan-name": restoredName, "user.titan-restore": requestId})
	if !isSet {
		t.Errorf("update: %+v", set)
	}
}

// A command leased again after its report was accepted (the loop's own completion was lost) finds a VM the tenant
// uses. The "backup" snapshot on it is then the daily backup's: a rollback to it, the removal of the tenant's
// snapshots or of the device of a volume they attached would destroy what they have. Only a placeholder is this
// command's still: the run that finished the copy may have died before it removed it.
func TestRestore_ARunAfterTheRestoreFinished_WritesNothingButTheRemovalOfAPlaceholderLeft_AndEndsTheCreateDoneAgain(t *testing.T) {
	for name, tc := range map[string]struct {
		placeholders []string
		writes       []string
	}{
		"nothing left":       {nil, nil},
		"a placeholder left": {[]string{placeholder0}, []string{"delete-volume " + project + "/" + placeholder0}},
	} {
		s := newRestoreScene(t, archiveOf(sourceVolumeName, detachedVolumeName))
		if err := s.run(restoreLeased(sourceToNewVolume, nil)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		s.usedSinceItWasRestored(t)
		s.putPlaceholders(tc.placeholders...)
		devices, config := cloneDevices(s.copyOnIncus(t).Devices), maps.Clone(s.copyOnIncus(t).Config)
		// Neither the pool's room nor the volumes' copies are asked about again.
		s.p.PoolSpace = api.ResourcesStoragePoolSpace{Total: 40 * gib, Used: 40 * gib}
		delete(s.p.Volumes[project+"/"+newVolumeName].Config, "user.titan-restore")
		s.p.Writes, s.bucket.Opened = nil, nil

		err := s.run(restoreLeased(sourceToNewVolume, nil))

		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !slices.Equal(s.p.Writes, tc.writes) || len(s.bucket.Opened) != 0 {
			t.Errorf("%s: the second run wrote %q and opened %q; want %q and nothing opened", name, s.p.Writes, s.bucket.Opened, tc.writes)
		}
		if !s.isAsTheTenantHasIt(t, devices, config) {
			t.Errorf("%s: the copy changed: %+v, snapshots %q", name, s.copyOnIncus(t), s.snapshotsOf(t, instanceName))
		}
		if len(s.p.StateChanges) != 0 {
			t.Errorf("%s: the copy was started or stopped: %+v", name, s.p.StateChanges)
		}
		reports := s.cp.RequestsTo(requestRoute)
		if len(reports) != 2 || reports[1].Body != doneRunning {
			t.Errorf("%s: reports: %+v", name, reports)
		}
	}
}

// A placeholder left beside a finished copy is still this command's to remove: one it cannot remove, or look for, is
// not "none left", whatever the tenant has made of the copy since.
func TestRestore_ARunAfterTheRestoreFinished_ThatCannotRemoveAPlaceholderLeft_IsRetryable_AndReportsNothingMore(t *testing.T) {
	for name, tc := range map[string]struct {
		break_ func(*restoreScene)
		step   string
		writes []string
	}{
		"the delete fails": {func(s *restoreScene) { s.p.DeleteVolumeErr = errors.New("locked") },
			"delete placeholder " + placeholder0, []string{"delete-volume " + project + "/" + placeholder0}},
		"the look fails": {func(s *restoreScene) { s.p.GetVolumeErr = errors.New("busy") }, "look for placeholder " + placeholder0, nil},
	} {
		s := newRestoreScene(t, archiveOf(sourceVolumeName, detachedVolumeName))
		if err := s.run(restoreLeased(sourceToNewVolume, nil)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		s.usedSinceItWasRestored(t)
		s.putPlaceholders(placeholder0)
		tc.break_(s)
		s.p.Writes = nil

		err := s.run(restoreLeased(sourceToNewVolume, nil))

		if !isRetryable(err) || !strings.Contains(err.Error(), tc.step) {
			t.Errorf("%s: err = %v, want a retryable error that names %q", name, err, tc.step)
		}
		if !slices.Equal(s.p.Writes, tc.writes) {
			t.Errorf("%s: writes\n got %q\nwant %q", name, s.p.Writes, tc.writes)
		}
		if reports := s.cp.RequestsTo(requestRoute); len(reports) != 1 {
			t.Errorf("%s: reported again on a run that failed: %+v", name, reports)
		}
	}
}

// An import goes on in Incus after the run that asked for it stopped waiting on it. Incus makes the instance only
// once its disk is unpacked: until then a run finds no copy, and a second import beside the first would be refused
// once its whole upload is in. A copy that is there while an operation still names it is not ready to be rolled back
// either.
func TestRestore_ACopyIncusIsStillImporting_IsRetryable_AndNothingIsWrittenOrReported(t *testing.T) {
	devices := map[string]map[string]string{"root": rootDisk(), "eth0": {"type": "nic", "network": "net-1"}, sourceVolumeName: volumeDisk(newVolumeName)}
	for name, leave := range map[string]func(*restoreScene){
		"the copy is not there yet":             func(s *restoreScene) {},
		"the copy is there, with its snapshots": func(s *restoreScene) { s.alreadyImported(devices, "backup", "sold1") },
		"the copy is there, rolled back":        func(s *restoreScene) { s.alreadyImported(devices) },
		"the copy carries another restore's request": func(s *restoreScene) {
			s.alreadyImported(devices, "backup")
			s.copyOnIncus(t).Config["user.titan-restore"] = earlierRequestId
		},
	} {
		s := newRestoreScene(t, archiveOf(sourceVolumeName, detachedVolumeName))
		leave(s)
		s.importStillRunning()

		err := s.run(restoreLeased(sourceToNewVolume, nil))

		wantText := "import of " + instanceName + " is still running: incus operation op-import (Restoring backup)"
		if !isRetryable(err) || err.Error() != wantText {
			t.Errorf("%s: err = %v, want the retryable %q", name, err, wantText)
		}
		if len(s.p.Writes) != 0 || len(s.bucket.Opened) != 0 || len(s.cp.Requests()) != 0 {
			t.Errorf("%s: wrote %q, opened %q, reported %+v; want nothing, not even a placeholder", name, s.p.Writes, s.bucket.Opened, s.cp.Requests())
		}
	}
}

// Operations that cannot be listed are not "none running".
func TestRestore_OperationsThatCannotBeListed_AreRetryable_AndNothingIsWrittenOrReported(t *testing.T) {
	for name, leave := range map[string]func(*restoreScene){
		"the copy is there": func(s *restoreScene) {
			s.alreadyImported(map[string]map[string]string{"root": rootDisk()}, "backup")
		},
		"the copy is not there": func(s *restoreScene) {},
	} {
		s := newRestoreScene(t, archiveOf(detachedVolumeName))
		leave(s)
		s.p.ListOperationsErr = errors.New("incus is busy")

		err := s.run(restoreLeased(nil, nil))

		if !isRetryable(err) || !strings.Contains(err.Error(), "look for an operation still running on vm "+instanceName) {
			t.Errorf("%s: err = %v, want a retryable error that names the step", name, err)
		}
		if len(s.p.Writes) != 0 || len(s.bucket.Opened) != 0 || len(s.cp.Requests()) != 0 {
			t.Errorf("%s: wrote %q, opened %q, reported %+v; want nothing", name, s.p.Writes, s.bucket.Opened, s.cp.Requests())
		}
	}
}

// A finished copy is the tenant's, and an operation on it is theirs: a start, a snapshot, the daily backup. Neither a
// run nor a give-up that comes after waits for it, or asks.
func TestRestore_AfterTheRestoreFinished_AnOperationOnTheCopy_IsNotWaitedFor(t *testing.T) {
	for name, again := range map[string]func(*restoreScene) error{
		"a run":     func(s *restoreScene) error { return s.run(restoreLeased(nil, nil)) },
		"a give-up": func(s *restoreScene) error { return s.giveUp(restoreLeased(nil, nil), errors.New("lease lost")) },
	} {
		s := newRestoreScene(t, archiveOf())
		if err := s.run(restoreLeased(nil, nil)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		s.p.RunningOperations[project+"/"+instanceName] = []api.Operation{{ID: "op-backup", Description: "Backing up instance", StatusCode: api.Running}}
		s.p.ListOperationsErr = errors.New("not to be asked")
		s.p.Writes = nil

		err := again(s)

		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		reports := s.cp.RequestsTo(requestRoute)
		if len(s.p.Writes) != 0 || len(reports) != 2 || reports[1].Body != doneStopped {
			t.Errorf("%s: wrote %q, reports %+v; want no write and the Done again", name, s.p.Writes, reports)
		}
	}
}

// The archive of a VM that was itself restored carries that restore's request in its config, and the import keeps it:
// the copy is not finished by a request that is not this command's.
func TestRestore_ACopyThatCarriesAnotherRestoresRequest_IsTakenUpAsNotFinished_AndGetsItsOwn(t *testing.T) {
	s := newRestoreScene(t, archiveOf(sourceVolumeName))
	s.alreadyImported(map[string]map[string]string{
		"root": rootDisk(), "eth0": {"type": "nic", "network": "net-1"}, sourceVolumeName: volumeDisk(newVolumeName),
	}, "backup", "sold1")
	s.copyOnIncus(t).Config["user.titan-restore"] = earlierRequestId

	err := s.run(restoreLeased(sourceToNewVolume, nil))

	if err != nil {
		t.Fatal(err)
	}
	target := project + "/" + instanceName
	want := []string{
		"rollback-vm " + target + "/backup",
		"delete-vm-snapshot " + target + "/backup",
		"delete-vm-snapshot " + target + "/sold1",
		"update-vm " + target,
	}
	if !slices.Equal(s.p.Writes, want) {
		t.Errorf("writes\n got %q\nwant %q", s.p.Writes, want)
	}
	config := s.copyOnIncus(t).Config
	if config["user.titan-restore"] != requestId || config["user.titan-name"] != restoredName {
		t.Errorf("the copy's config: %v", config)
	}
	if got := s.reportBody(t); got != doneStopped {
		t.Errorf("report: %s", got)
	}
}

// Two runs of one command may overlap when a lease ran out. The one that finishes the copy hands it to the tenant; the
// other learns of it from the read its update is built on, and makes no update: it would drop what the tenant attached.
func TestRestore_ACopyAnotherRunFinishedSinceThisOneLookedForIt_IsNotUpdated_AndTheCreateEndsDone(t *testing.T) {
	s := newRestoreScene(t, archiveOf())
	s.alreadyImported(map[string]map[string]string{"root": rootDisk(), "eth0": {"type": "nic", "network": "net-1"}})
	reads := 0
	s.p.AfterGetInstance = func() {
		reads++
		if reads > 1 {
			return
		}
		s.copyOnIncus(t).Config["user.titan-name"] = restoredName
		s.copyOnIncus(t).Config["user.titan-restore"] = requestId
		s.p.WriteInstance(project, instanceName, func(devices map[string]map[string]string) {
			devices[detachedVolumeName] = volumeDisk(detachedVolumeName)
		})
	}

	err := s.run(restoreLeased(nil, nil))

	if err != nil {
		t.Fatal(err)
	}
	if len(s.p.Writes) != 0 {
		t.Errorf("wrote %q", s.p.Writes)
	}
	if _, kept := s.copyOnIncus(t).Devices[detachedVolumeName]; !kept {
		t.Errorf("the volume the tenant attached is gone: %v", s.copyOnIncus(t).Devices)
	}
	if got := s.reportBody(t); got != doneStopped {
		t.Errorf("report: %s", got)
	}
}

// The update replaces the whole device list, so it carries the ETag of its read: a write in between is not undone.
func TestRestore_TheCopyWrittenBetweenTheReadAndTheUpdate_IsRetryable_AndNotOverwritten(t *testing.T) {
	s := newRestoreScene(t, archiveOf())
	s.alreadyImported(map[string]map[string]string{"root": rootDisk(), "eth0": {"type": "nic", "network": "net-1"}})
	s.p.AfterGetInstance = func() {
		s.p.WriteInstance(project, instanceName, func(devices map[string]map[string]string) { devices["other"] = volumeDisk("volother") })
	}

	err := s.run(restoreLeased(nil, nil))

	if !isRetryable(err) || !strings.Contains(err.Error(), "ETag") {
		t.Fatalf("err = %v, want the retryable ETag mismatch", err)
	}
	if _, kept := s.copyOnIncus(t).Devices["other"]; !kept || s.copyOnIncus(t).Config["user.titan-name"] != "web-1" {
		t.Errorf("the stale update was applied: %+v", s.copyOnIncus(t).InstancePut)
	}
	if len(s.cp.Requests()) != 0 {
		t.Errorf("reported on a run that failed: %+v", s.cp.Requests())
	}
}

// A placeholder an earlier run made, whose import then failed, is this command's: its name is made from the copy's.
func TestRestore_APlaceholderAnEarlierRunMade_IsTakenForItsOwn(t *testing.T) {
	s := newRestoreScene(t, archiveOf(detachedVolumeName))
	s.putPlaceholders(placeholder0)

	err := s.run(restoreLeased(nil, nil))

	if err != nil {
		t.Fatalf("a placeholder already there failed the restore: %v", err)
	}
	if _, left := s.p.Volumes[project+"/"+placeholder0]; left {
		t.Error("the placeholder is still there")
	}
	if got := s.reportBody(t); got != doneStopped {
		t.Errorf("report: %s", got)
	}
}

// Every step may pass on the next lease, and the next run takes the copy up where this one left it: nothing is
// reported, and what was imported stays.
func TestRestore_AStepThatFails_IsRetryable_ReportsNothing_AndKeepsWhatWasImported(t *testing.T) {
	imported := func(s *restoreScene) {
		s.alreadyImported(map[string]map[string]string{"root": rootDisk(), "eth0": {"type": "nic", "network": "net-1"}})
	}
	for name, tc := range map[string]struct {
		break_ func(*restoreScene)
		step   string
	}{
		"copy read":          {func(s *restoreScene) { s.p.GetInstanceErr = errors.New("busy") }, "look for vm " + instanceName},
		"volume read":        {func(s *restoreScene) { s.p.GetVolumeErr = errors.New("busy") }, "look for volume " + newVolumeName},
		"index read":         {func(s *restoreScene) { s.bucket.GetErr = errors.New("bucket unreachable") }, "read archive " + restoreArchiveKey},
		"placeholder create": {func(s *restoreScene) { s.p.CreateVolumeErr = errors.New("pool busy") }, "create placeholder " + placeholder0},
		"import":             {func(s *restoreScene) { s.p.ImportInstanceErr = errors.New("disk full") }, "import vm " + instanceName},
		"snapshot read":      {func(s *restoreScene) { s.p.GetSnapshotErr = errors.New("busy") }, `look for the "backup" snapshot of vm ` + instanceName},
		"rollback":           {func(s *restoreScene) { s.p.UpdateInstanceErr = errors.New("locked") }, "roll vm " + instanceName},
		"snapshot delete":    {func(s *restoreScene) { s.p.DeleteSnapshotErr = errors.New("locked") }, `delete the "backup" snapshot of vm ` + instanceName},
		"snapshot list":      {func(s *restoreScene) { s.p.ListInstanceSnapshotsErr = errors.New("busy") }, "list the snapshots of vm " + instanceName},
		"device update": {func(s *restoreScene) {
			imported(s)
			s.p.UpdateInstanceErr = errors.New("locked")
		}, "set the devices of vm " + instanceName},
		"placeholder delete": {func(s *restoreScene) { s.p.DeleteVolumeErr = errors.New("locked") }, "delete placeholder " + placeholder0},
		"report": {func(s *restoreScene) {
			s.cp.Respond("PATCH", "/internal/vms/"+vmId+"/requests/"+requestId, 503, "")
		}, "503"},
	} {
		s := newRestoreScene(t, archiveOf(sourceVolumeName, detachedVolumeName))
		tc.break_(s)

		err := s.run(restoreLeased(sourceToNewVolume, nil))

		if !isRetryable(err) || !strings.Contains(err.Error(), tc.step) {
			t.Errorf("%s: err = %v, want a retryable error that names %q", name, err, tc.step)
		}
		if slices.Contains(s.p.Writes, "delete-vm "+project+"/"+instanceName) {
			t.Errorf("%s: the import was removed: %q", name, s.p.Writes)
		}
		if name != "report" && len(s.cp.Requests()) != 0 {
			t.Errorf("%s: reported on a run that failed: %+v", name, s.cp.Requests())
		}
	}
}

// Placeholders are found by looking for their names: a look that fails is not "none left".
func TestRestore_APlaceholderThatCannotBeLookedFor_IsRetryable_AndReportsNothing(t *testing.T) {
	s := newRestoreScene(t, archiveOf())
	s.p.GetVolumeErr = errors.New("busy")

	err := s.run(restoreLeased(nil, nil))

	if !isRetryable(err) || !strings.Contains(err.Error(), "look for placeholder "+placeholder0) {
		t.Fatalf("err = %v, want a retryable error that names the placeholder", err)
	}
	if len(s.cp.Requests()) != 0 {
		t.Errorf("reported on a run that failed: %+v", s.cp.Requests())
	}
}

// Another run of the same command may have removed the placeholder between the look and the delete.
func TestRestore_APlaceholderAlreadyGoneWhenItIsDeleted_IsNotAFailure(t *testing.T) {
	s := newRestoreScene(t, archiveOf(detachedVolumeName))
	s.p.DeleteVolumeErr = testsupport.NotFound("Storage pool volume not found")

	err := s.run(restoreLeased(nil, nil))

	if err != nil {
		t.Fatalf("a placeholder already gone failed the restore: %v", err)
	}
	if got := s.reportBody(t); got != doneStopped {
		t.Errorf("report: %s", got)
	}
}

// Another run of the same command may have removed the snapshot between the list and the delete.
func TestRestore_ASnapshotAlreadyGoneWhenItIsDeleted_IsNotAFailure(t *testing.T) {
	s := newRestoreScene(t, archiveOf())
	s.alreadyImported(map[string]map[string]string{"root": rootDisk(), "eth0": {"type": "nic", "network": "net-1"}}, "sold1")
	s.p.DeleteSnapshotErr = testsupport.NotFound("Instance_snapshot not found")

	err := s.run(restoreLeased(nil, nil))

	if err != nil {
		t.Fatalf("a snapshot already gone failed the restore: %v", err)
	}
	if got := s.reportBody(t); got != doneStopped {
		t.Errorf("report: %s", got)
	}
}

// The copy is on Incus; a read that fails afterwards does not undo it, and the reconciler reports its status.
func TestRestore_ACopyThatCannotBeReadForTheReport_StillEndsTheCreateDone_WithNothingObserved(t *testing.T) {
	s := newRestoreScene(t, archiveOf())
	s.p.GetInstanceFullErr = errors.New("incus is busy")

	err := s.run(restoreLeased(nil, nil))

	if err != nil {
		t.Fatal(err)
	}
	if got := s.reportBody(t); got != `{"outcome":"Done","externalId":"`+instanceName+`"}` {
		t.Errorf("report: %s", got)
	}
}

// The copy was never started, so it is deleted as it stands. The volumes' copies are not the VM's to remove: each
// ended a request of its own.
func TestRestore_GiveUp_RemovesTheCopyAndItsPlaceholders_NotTheVolumesCopies_AndEndsTheCreateFailed_WithoutAnId(t *testing.T) {
	s := newRestoreScene(t, archiveOf(sourceVolumeName, detachedVolumeName))
	s.putPlaceholders(placeholder0, placeholder1)
	s.alreadyImported(map[string]map[string]string{
		"root": rootDisk(), "eth0": {"type": "nic", "network": "net-1"},
		sourceVolumeName: volumeDisk(placeholder0), detachedVolumeName: volumeDisk(placeholder1), newVolumeName: volumeDisk(newVolumeName),
	}, "backup", "sold1")

	err := s.giveUp(restoreLeased(sourceToNewVolume, nil), errors.New("roll vm back: locked"))

	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"delete-vm " + project + "/" + instanceName,
		"delete-volume " + project + "/" + placeholder1,
		"delete-volume " + project + "/" + placeholder0,
	}
	if !slices.Equal(s.p.Writes, want) {
		t.Errorf("writes\n got %q\nwant %q", s.p.Writes, want)
	}
	if _, kept := s.p.Volumes[project+"/"+newVolumeName]; !kept {
		t.Error("the volume's copy was removed")
	}
	if len(s.p.StateChanges) != 0 {
		t.Errorf("the copy was started or stopped: %+v", s.p.StateChanges)
	}
	wantReport := `{"outcome":"Failed","reason":"` + restoreFailedText + `","detail":"roll vm back: locked"}`
	if got := s.reportBody(t); got != wantReport {
		t.Errorf("report\n got %s\nwant %s", got, wantReport)
	}
}

// A give-up that found no copy while Incus still unpacks its disk would say nothing is left, and the import would then
// leave a VM no row in Titan names; one that removed a copy an operation is still at work on would remove it under
// that operation. It comes again once the operation has ended, and writes nothing until then: the placeholders are
// what the import's devices point at.
func TestRestore_GiveUp_OfACopyIncusIsStillImporting_IsRetryable_AndNothingIsRemovedOrReported(t *testing.T) {
	for name, leave := range map[string]func(*restoreScene){
		"the copy is not there yet": func(s *restoreScene) {},
		"the copy is there": func(s *restoreScene) {
			s.alreadyImported(map[string]map[string]string{"root": rootDisk(), detachedVolumeName: volumeDisk(placeholder0)}, "backup")
		},
	} {
		s := newRestoreScene(t, archiveOf(detachedVolumeName))
		s.putPlaceholders(placeholder0)
		leave(s)
		s.importStillRunning()

		err := s.giveUp(restoreLeased(nil, nil), errors.New("waiting for incus operation op-import: connection reset by peer"))

		wantText := "import of " + instanceName + " is still running: incus operation op-import (Restoring backup)"
		if !isRetryable(err) || err.Error() != wantText {
			t.Errorf("%s: err = %v, want the retryable %q", name, err, wantText)
		}
		if len(s.p.Writes) != 0 || len(s.cp.Requests()) != 0 {
			t.Errorf("%s: wrote %q, reported %+v; want nothing", name, s.p.Writes, s.cp.Requests())
		}
	}
}

// The restore never starts the copy; one found running all the same, and not finished, is still what the command made.
func TestRestore_GiveUp_OfAnUnfinishedCopyThatIsRunning_RemovesItAllTheSame(t *testing.T) {
	s := newRestoreScene(t, archiveOf())
	s.alreadyImported(map[string]map[string]string{"root": rootDisk()}, "backup")
	s.copyOnIncus(t).Status, s.copyOnIncus(t).StatusCode = "Running", api.Running

	err := s.giveUp(restoreLeased(nil, nil), errors.New("roll vm back: locked"))

	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"delete-vm " + project + "/" + instanceName}; !slices.Equal(s.p.Writes, want) {
		t.Errorf("writes\n got %q\nwant %q", s.p.Writes, want)
	}
	wantReport := `{"outcome":"Failed","reason":"` + restoreFailedText + `","detail":"roll vm back: locked"}`
	if got := s.reportBody(t); got != wantReport {
		t.Errorf("report\n got %s\nwant %s", got, wantReport)
	}
}

// Nothing removes a placeholder once the command is over: Titan's delete of the failed VM knows the copy by its id
// alone. The line that says the give-up left some is all an admin has to find them by, so it names each.
func TestRestore_GiveUp_ThatLeavesPlaceholders_NamesEachInItsWarning(t *testing.T) {
	bothLeft := []any{placeholder0, placeholder1}
	for name, tc := range map[string]struct {
		break_  func(*restoreScene)
		message string
		named   any
	}{
		"the copy will not delete": {func(s *restoreScene) { s.p.DeleteInstanceErr = errors.New("instance is busy") },
			"removing the vm of a failed restore failed; it is left with its placeholders", bothLeft},
		"the copy cannot be read": {func(s *restoreScene) { s.p.GetInstanceErr = errors.New("connection refused") },
			"the vm of a failed restore could not be read; it may be left with its placeholders", bothLeft},
		"the copy's operations cannot be listed": {func(s *restoreScene) { s.p.ListOperationsErr = errors.New("connection refused") },
			"the operations on the vm of a failed restore could not be listed; it may be left with its placeholders", bothLeft},
		"a placeholder will not delete": {func(s *restoreScene) { s.p.DeleteVolumeErr = errors.New("locked") },
			"removing the placeholders of a restore failed; they are left", bothLeft},
		// Looked for by their names, which are made from the copy's: the line says how, when the look itself fails.
		"the placeholders cannot be looked for": {func(s *restoreScene) {
			s.p.DeleteInstanceErr = errors.New("instance is busy")
			s.p.GetVolumeErr = errors.New("connection refused")
		}, "removing the vm of a failed restore failed; it is left with its placeholders",
			"not read (look for placeholder " + placeholder0 + ": connection refused); named ph<n>-" + instanceName},
	} {
		s := newRestoreScene(t, archiveOf(sourceVolumeName, detachedVolumeName))
		s.putPlaceholders(placeholder0, placeholder1)
		s.alreadyImported(map[string]map[string]string{
			"root": rootDisk(), sourceVolumeName: volumeDisk(placeholder0), detachedVolumeName: volumeDisk(placeholder1),
		})
		tc.break_(s)
		logger, logs := testsupport.NewLogCapture(t)

		err := s.handler.GiveUp(context.Background(), restoreLeased(nil, nil), errors.New("lease lost"), logger)

		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		lines := logs.Records(tc.message)
		if len(lines) != 1 {
			t.Errorf("%s: want one line %q, got %v", name, tc.message, logs.All())
			continue
		}
		line := lines[0]
		if named := line["placeholders"]; !reflect.DeepEqual(named, tc.named) {
			t.Errorf("%s: the line names %v, want %v", name, named, tc.named)
		}
		if line["level"] != "WARN" || line["vm"] != instanceName || line["error"] == nil {
			t.Errorf("%s: line %v; want a warning that names the copy and the error", name, line)
		}
	}
}

// With the cluster gone nothing can be looked for: the line says how the placeholders are named.
func TestRestore_GiveUp_InAClusterThatCannotBeReached_SaysHowThePlaceholdersItMayLeaveAreNamed(t *testing.T) {
	s := newRestoreScene(t, archiveOf())
	logger, logs := testsupport.NewLogCapture(t)
	l := restoreLeased(nil, nil)
	l.ClusterId = "elsewhere"

	err := s.handler.GiveUp(context.Background(), l, errors.New("lease lost"), logger)

	if err != nil {
		t.Fatal(err)
	}
	line := logs.One("the cluster of a failed restore is not reachable; its vm may be left with its placeholders")
	if line["level"] != "WARN" || line["vm"] != instanceName || line["placeholders"] != "not read; named ph<n>-"+instanceName {
		t.Errorf("line: %v", line)
	}
}

// The restore did finish, whatever the loop gives up on afterwards: the VM is the tenant's, and telling Titan its
// create failed would be as wrong as removing it. A placeholder left is still this command's to remove.
func TestRestore_GiveUp_AfterTheRestoreFinished_RemovesNothingButAPlaceholderLeft_AndEndsTheCreateDone(t *testing.T) {
	for name, tc := range map[string]struct {
		placeholders []string
		writes       []string
	}{
		"nothing left":       {nil, nil},
		"a placeholder left": {[]string{placeholder0}, []string{"delete-volume " + project + "/" + placeholder0}},
	} {
		s := newRestoreScene(t, archiveOf(sourceVolumeName, detachedVolumeName))
		if err := s.run(restoreLeased(sourceToNewVolume, nil)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		s.usedSinceItWasRestored(t)
		s.putPlaceholders(tc.placeholders...)
		devices, config := cloneDevices(s.copyOnIncus(t).Devices), maps.Clone(s.copyOnIncus(t).Config)
		s.p.Writes = nil

		err := s.giveUp(restoreLeased(sourceToNewVolume, nil), errors.New("lease lost"))

		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !slices.Equal(s.p.Writes, tc.writes) {
			t.Errorf("%s: the give-up wrote %q, want %q", name, s.p.Writes, tc.writes)
		}
		if !s.isAsTheTenantHasIt(t, devices, config) {
			t.Errorf("%s: the copy changed: %+v, snapshots %q", name, s.copyOnIncus(t), s.snapshotsOf(t, instanceName))
		}
		if len(s.p.StateChanges) != 0 {
			t.Errorf("%s: the copy was started or stopped: %+v", name, s.p.StateChanges)
		}
		reports := s.cp.RequestsTo(requestRoute)
		if len(reports) != 2 || reports[1].Body != doneRunning {
			t.Errorf("%s: reports: %+v", name, reports)
		}
	}
}

// A copy that carries the request of an earlier restore, as its archive did, is this command's unfinished import.
func TestRestore_GiveUp_OfACopyThatCarriesAnotherRestoresRequest_RemovesIt_AndEndsTheCreateFailed(t *testing.T) {
	s := newRestoreScene(t, archiveOf())
	s.alreadyImported(map[string]map[string]string{"root": rootDisk()}, "backup")
	s.copyOnIncus(t).Config["user.titan-restore"] = earlierRequestId

	err := s.giveUp(restoreLeased(nil, nil), errors.New("roll vm back: locked"))

	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"delete-vm " + project + "/" + instanceName}; !slices.Equal(s.p.Writes, want) {
		t.Errorf("writes\n got %q\nwant %q", s.p.Writes, want)
	}
	wantReport := `{"outcome":"Failed","reason":"` + restoreFailedText + `","detail":"roll vm back: locked"}`
	if got := s.reportBody(t); got != wantReport {
		t.Errorf("report\n got %s\nwant %s", got, wantReport)
	}
}

// A run that made its placeholder and failed before the import leaves no copy, and one volume to remove.
func TestRestore_GiveUp_WithNoCopyOnIncus_RemovesAPlaceholderLeft_AndEndsTheCreateFailed_WithoutAnId(t *testing.T) {
	for name, tc := range map[string]struct {
		placeholders []string
		writes       []string
	}{
		"nothing was made":       {nil, nil},
		"a placeholder was made": {[]string{placeholder0}, []string{"delete-volume " + project + "/" + placeholder0}},
	} {
		s := newRestoreScene(t, archiveOf(detachedVolumeName))
		s.putPlaceholders(tc.placeholders...)

		err := s.giveUp(restoreLeased(nil, nil), errors.New("import vm: disk full"))

		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !slices.Equal(s.p.Writes, tc.writes) {
			t.Errorf("%s: writes\n got %q\nwant %q", name, s.p.Writes, tc.writes)
		}
		wantReport := `{"outcome":"Failed","reason":"` + restoreFailedText + `","detail":"import vm: disk full"}`
		if got := s.reportBody(t); got != wantReport {
			t.Errorf("%s: report\n got %s\nwant %s", name, got, wantReport)
		}
	}
}

// A copy still on Incus, or one nobody could look for, is told to Titan by its id: Titan's own delete of the failed VM
// then reaches it. Its placeholders stay with it: Incus refuses to delete a volume an instance holds.
func TestRestore_GiveUp_WhenTheCopyMayStillBeThere_EndsTheCreateFailed_WithItsId_AndLeavesItsPlaceholders(t *testing.T) {
	for name, tc := range map[string]struct {
		break_  func(*restoreScene)
		cluster string
		writes  []string
	}{
		"the delete fails":            {func(s *restoreScene) { s.p.DeleteInstanceErr = errors.New("instance is busy") }, clusterId, []string{"delete-vm " + project + "/" + instanceName}},
		"the copy cannot be read":     {func(s *restoreScene) { s.p.GetInstanceErr = errors.New("connection refused") }, clusterId, nil},
		"the cluster cannot be found": {func(s *restoreScene) {}, "elsewhere", nil},
		// An operation may still be at work on it: it is not removed under one.
		"its operations cannot be listed": {func(s *restoreScene) { s.p.ListOperationsErr = errors.New("connection refused") }, clusterId, nil},
		"no copy yet, and its operations cannot be listed": {func(s *restoreScene) {
			delete(s.p.Instances, project+"/"+instanceName)
			s.p.ListOperationsErr = errors.New("connection refused")
		}, clusterId, nil},
	} {
		s := newRestoreScene(t, archiveOf(detachedVolumeName))
		s.putPlaceholders(placeholder0)
		s.alreadyImported(map[string]map[string]string{"root": rootDisk(), detachedVolumeName: volumeDisk(placeholder0)})
		tc.break_(s)
		l := restoreLeased(nil, nil)
		l.ClusterId = tc.cluster

		err := s.giveUp(l, errors.New("lease lost"))

		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !slices.Equal(s.p.Writes, tc.writes) {
			t.Errorf("%s: writes\n got %q\nwant %q", name, s.p.Writes, tc.writes)
		}
		wantReport := `{"outcome":"Failed","externalId":"` + instanceName + `","reason":"` + restoreFailedText + `","detail":"lease lost"}`
		if got := s.reportBody(t); got != wantReport {
			t.Errorf("%s: report\n got %s\nwant %s", name, got, wantReport)
		}
	}
}

// Another run's give-up may have removed the copy between the look and the delete: it is gone all the same.
func TestRestore_GiveUp_WhenTheCopyIsGoneByTheTimeItIsDeleted_EndsTheCreateFailed_WithoutAnId(t *testing.T) {
	s := newRestoreScene(t, archiveOf())
	s.alreadyImported(map[string]map[string]string{"root": rootDisk()})
	s.p.DeleteInstanceErr = testsupport.NotFound("Instance not found")

	err := s.giveUp(restoreLeased(nil, nil), errors.New("lease lost"))

	if err != nil {
		t.Fatal(err)
	}
	wantReport := `{"outcome":"Failed","reason":"` + restoreFailedText + `","detail":"lease lost"}`
	if got := s.reportBody(t); got != wantReport {
		t.Errorf("report\n got %s\nwant %s", got, wantReport)
	}
}

// A command without a request has nothing to end: the give-up passes, so the loop fails the command for good instead of
// leaving its lease to run out and the command to come back.
func TestRestore_GiveUp_WithoutARequest_CallsNothing_SaysSo_AndPasses(t *testing.T) {
	for name, changes := range map[string]map[string]any{
		"no request":           {"requestId": nil},
		"an empty request":     {"requestId": ""},
		"no request and no vm": {"requestId": nil, "vmId": nil},
	} {
		s := newRestoreScene(t, archiveOf())
		s.alreadyImported(map[string]map[string]string{"root": rootDisk()})
		logger, logs := testsupport.NewLogCapture(t)

		err := s.handler.GiveUp(context.Background(), restoreLeased(nil, changes), errors.New("boom"), logger)

		if err != nil {
			t.Errorf("%s: err = %v, want none", name, err)
		}
		if s.p.Calls != 0 || len(s.cp.Requests()) != 0 {
			t.Errorf("%s: %d call(s) on Incus, reported %+v; want nothing", name, s.p.Calls, s.cp.Requests())
		}
		if got := logs.Records("no request id; nothing to end"); len(got) != 1 {
			t.Errorf("%s: want one line that says there is no request, got %v", name, got)
		}
	}
}

// A request with no VM to end it on can never be reported: retrying the give-up changes nothing.
func TestRestore_GiveUp_WithARequestAndNoVmToReportOn_IsPermanent_AndCallsNothing(t *testing.T) {
	s := newRestoreScene(t, archiveOf())
	s.alreadyImported(map[string]map[string]string{"root": rootDisk()})

	err := s.giveUp(restoreLeased(nil, map[string]any{"vmId": nil}), errors.New("boom"))

	if !isPermanent(err) || !strings.Contains(err.Error(), "nobody to tell") {
		t.Errorf("err = %v, want a permanent error that says nobody can be told", err)
	}
	if s.p.Calls != 0 || len(s.cp.Requests()) != 0 {
		t.Errorf("%d call(s) on Incus, reported %+v; want nothing", s.p.Calls, s.cp.Requests())
	}
}

// A payload that is not JSON names no request: nothing can be ended, and no retry reads it better.
func TestRestore_GiveUp_OfAPayloadThatDoesNotDecode_IsPermanent_AndCallsNothing(t *testing.T) {
	s := newRestoreScene(t, archiveOf())

	err := s.giveUp(command.Leased{Kind: "vm.restore", ClusterId: clusterId, Payload: []byte("{")}, errors.New("boom"))

	if !isPermanent(err) || s.p.Calls != 0 || len(s.cp.Requests()) != 0 {
		t.Errorf("err = %v, %d call(s) on Incus, reported %+v; want a permanent error and nothing done", err, s.p.Calls, s.cp.Requests())
	}
}

// The detail is the admin's; it is cut as every Failed cuts it.
func TestRestore_GiveUp_CutsTheDetail(t *testing.T) {
	s := newRestoreScene(t, archiveOf())

	if err := s.giveUp(restoreLeased(nil, nil), errors.New(strings.Repeat("x", 1500))); err != nil {
		t.Fatal(err)
	}

	var body map[string]any
	if err := json.Unmarshal([]byte(s.reportBody(t)), &body); err != nil {
		t.Fatal(err)
	}
	if detail, _ := body["detail"].(string); len(detail) != 1000 {
		t.Errorf("detail is %d characters, want 1000", len(detail))
	}
}

// An admin finds a restore in the log by its archive and by the VM it was taken of; the command loop's own keys stay
// the loop's.
func TestRestore_LogsWhereTheImportStartsAndWhereItIsDone_WithTheArchiveTheSourceAndTheCopy(t *testing.T) {
	s := newRestoreScene(t, archiveOf())
	logger, logs := testsupport.NewLogCapture(t)

	err := s.handler.Run(context.Background(), restoreLeased(nil, nil), logger.With(loopKeys...))

	if err != nil {
		t.Fatal(err)
	}
	for _, line := range []map[string]any{logs.One("vm restore started"), logs.One("vm restored")} {
		isFindable := line["object"] == restoreArchiveKey && line["source"] == sourceVmId && line["vm"] == instanceName
		if !isFindable {
			t.Errorf("log line: %v", line)
		}
	}
	assertTheLoopsKeysAreKept(t, "a first run", logs)
}

// loopKeys are the keys the command loop puts on the logger it hands a handler, each with a value no handler would
// write.
var loopKeys = []any{"commandId", "the-loop's-commandId", "leaseId", "the-loop's-leaseId", "kind", "the-loop's-kind",
	"key", "the-loop's-key", "clusterId", "the-loop's-clusterId", "attempt", "the-loop's-attempt",
	"maxAttempts", "the-loop's-maxAttempts"}

// assertTheLoopsKeysAreKept reads every line the handler wrote: a key set again on a line replaces the loop's value in
// the record as it is read back.
func assertTheLoopsKeysAreKept(t *testing.T, name string, logs *testsupport.LogCapture) {
	t.Helper()
	lines := logs.All()
	if len(lines) == 0 {
		t.Errorf("%s: the handler logged nothing: no line to read the loop's keys on", name)
	}
	for _, line := range lines {
		for i := 0; i < len(loopKeys); i += 2 {
			if key, want := loopKeys[i].(string), loopKeys[i+1]; line[key] != want {
				t.Errorf("%s: log line %q has %s = %v, want the loop's %v", name, line["msg"], key, line[key], want)
			}
		}
	}
}

// Every line of a run that takes a copy up, of a run after the copy was finished and of each give-up, beside the two
// of a first run.
func TestRestore_NoLineOfARunOrAGiveUp_SetsAKeyTheLoopSet(t *testing.T) {
	imported := func(s *restoreScene) {
		s.putPlaceholders(placeholder0)
		s.alreadyImported(map[string]map[string]string{"root": rootDisk(), detachedVolumeName: volumeDisk(placeholder0)}, "backup")
	}
	for name, act := range map[string]func(*restoreScene, *slog.Logger){
		"a run that takes the copy up": func(s *restoreScene, logger *slog.Logger) {
			imported(s)
			_ = s.handler.Run(context.Background(), restoreLeased(nil, nil), logger)
		},
		"a run of an old-format archive": func(s *restoreScene, logger *slog.Logger) {
			archive := archiveOf(detachedVolumeName)
			archive.Snapshots = nil
			s.bucket.Objects[restoreArchiveKey] = archive.TarGz()
			s.p.DeleteVolumeErr = errors.New("locked")
			_ = s.handler.Run(context.Background(), restoreLeased(nil, nil), logger)
		},
		"a run after the copy was finished": func(s *restoreScene, logger *slog.Logger) {
			_ = s.run(restoreLeased(nil, nil))
			_ = s.handler.Run(context.Background(), restoreLeased(nil, nil), logger)
		},
		"a give-up that removes the copy": func(s *restoreScene, logger *slog.Logger) {
			imported(s)
			_ = s.handler.GiveUp(context.Background(), restoreLeased(nil, nil), errors.New("boom"), logger)
		},
		"a give-up that cannot remove it": func(s *restoreScene, logger *slog.Logger) {
			imported(s)
			s.p.DeleteInstanceErr = errors.New("busy")
			_ = s.handler.GiveUp(context.Background(), restoreLeased(nil, nil), errors.New("boom"), logger)
		},
		"a give-up that cannot remove its placeholder": func(s *restoreScene, logger *slog.Logger) {
			imported(s)
			s.p.DeleteVolumeErr = errors.New("busy")
			_ = s.handler.GiveUp(context.Background(), restoreLeased(nil, nil), errors.New("boom"), logger)
		},
		"a give-up after the copy was finished": func(s *restoreScene, logger *slog.Logger) {
			_ = s.run(restoreLeased(nil, nil))
			_ = s.handler.GiveUp(context.Background(), restoreLeased(nil, nil), errors.New("boom"), logger)
		},
		"a give-up that cannot read the copy": func(s *restoreScene, logger *slog.Logger) {
			s.p.GetInstanceErr = errors.New("busy")
			_ = s.handler.GiveUp(context.Background(), restoreLeased(nil, nil), errors.New("boom"), logger)
		},
		"a give-up that cannot list the operations": func(s *restoreScene, logger *slog.Logger) {
			s.p.ListOperationsErr = errors.New("busy")
			_ = s.handler.GiveUp(context.Background(), restoreLeased(nil, nil), errors.New("boom"), logger)
		},
		"a give-up in a cluster that is gone": func(s *restoreScene, logger *slog.Logger) {
			l := restoreLeased(nil, nil)
			l.ClusterId = "elsewhere"
			_ = s.handler.GiveUp(context.Background(), l, errors.New("boom"), logger)
		},
		"a give-up without a request": func(s *restoreScene, logger *slog.Logger) {
			_ = s.handler.GiveUp(context.Background(), restoreLeased(nil, map[string]any{"requestId": nil}), errors.New("boom"), logger)
		},
	} {
		s := newRestoreScene(t, archiveOf(detachedVolumeName))
		logger, logs := testsupport.NewLogCapture(t)

		act(s, logger.With(loopKeys...))

		assertTheLoopsKeysAreKept(t, name, logs)
	}
}

// A payload Run refuses made nothing on Incus, and what is under the name it gives is not this command's: a give-up
// that looked there would take whatever it found for its own import and delete it.
func TestRestore_GiveUp_OfAPayloadRunRefuses_MakesNoCallOnIncus_AndEndsTheCreateFailed_WithoutAnId(t *testing.T) {
	pair := func(source string, copied string) map[string]string {
		return map[string]string{"sourceVolumeId": source, "newVolumeId": copied, "requestId": volumeRequestId}
	}
	refused := map[string]command.Leased{
		"the original's own id":           restoreLeased(nil, map[string]any{"vmId": sourceVmId}),
		"an id that names no vm":          restoreLeased(nil, map[string]any{"vmId": "not-a-titan-id"}),
		"a source that names no vm":       restoreLeased(nil, map[string]any{"sourceVmId": "../other"}),
		"a backupKey that is no moment":   restoreLeased(nil, map[string]any{"backupKey": "20261122T020000Z.tar.gz"}),
		"a disk size below zero":          restoreLeased(nil, map[string]any{"diskSizeGb": -1}),
		"a pair without its request":      restoreLeased([]map[string]string{{"sourceVolumeId": sourceVolumeId, "newVolumeId": newVolumeId}}, nil),
		"a pair whose copy is no id":      restoreLeased([]map[string]string{pair(sourceVolumeId, "data")}, nil),
		"a volume copied onto itself":     restoreLeased([]map[string]string{pair(sourceVolumeId, sourceVolumeId)}, nil),
		"a copy that another pair copies": restoreLeased([]map[string]string{pair(sourceVolumeId, newVolumeId), pair(newVolumeId, secondNewVolumeId)}, nil),
		"one volume copied twice":         restoreLeased([]map[string]string{pair(sourceVolumeId, newVolumeId), pair(sourceVolumeId, secondNewVolumeId)}, nil),
	}
	for _, field := range []string{"sourceVmId", "backupKey", "tenantId", "projectName"} {
		refused["no "+field] = restoreLeased(nil, map[string]any{field: nil})
	}
	for name, l := range refused {
		s := newRestoreScene(t, archiveOf())
		s.putPlaceholders(placeholder0)
		s.alreadyImported(map[string]map[string]string{"root": rootDisk()}, "backup")
		if err := s.run(l); !isPermanent(err) || s.p.Calls != 0 {
			t.Fatalf("%s: Run answered %v after %d call(s) on Incus: not a payload Run refuses", name, err, s.p.Calls)
		}
		var payload struct {
			VmId string `json:"vmId"`
		}
		if err := json.Unmarshal(l.Payload, &payload); err != nil {
			t.Fatal(err)
		}

		err := s.giveUp(l, errors.New("payload refused"))

		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if s.p.Calls != 0 || len(s.p.Writes) != 0 {
			t.Errorf("%s: %d call(s) on Incus, writes %q; want none", name, s.p.Calls, s.p.Writes)
		}
		reports := s.cp.RequestsTo("PATCH /internal/vms/" + payload.VmId + "/requests/" + requestId)
		wantReport := `{"outcome":"Failed","reason":"` + restoreFailedText + `","detail":"payload refused"}`
		if len(reports) != 1 || len(s.cp.Requests()) != 1 || reports[0].Body != wantReport {
			t.Errorf("%s: reports %+v; want one Failed without an id, on the request of vm %s", name, s.cp.Requests(), payload.VmId)
		}
	}
}

// The copy's id equal to the original's makes the copy's name the original's: Run refuses it, and the give-up after
// that refusal must not find the original under "the copy's name" and remove it.
func TestRestore_GiveUp_OfAPayloadThatRestoresAVmOntoItself_LeavesTheOriginalAsItIs(t *testing.T) {
	s := newRestoreScene(t, archiveOf(sourceVolumeName))
	original := s.p.Instances[project+"/"+sourceVmName]
	// Stopped, as Incus deletes an instance only then.
	original.Status, original.StatusCode = "Stopped", api.Stopped
	devicesBefore, configBefore := cloneDevices(original.Devices), maps.Clone(original.Config)
	l := restoreLeased(nil, map[string]any{"vmId": sourceVmId})

	runErr := s.run(l)
	err := s.giveUp(l, runErr)

	if !isPermanent(runErr) || !strings.Contains(runErr.Error(), "onto itself") {
		t.Fatalf("Run: err = %v, want the permanent refusal of a vm restored onto itself", runErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(s.p.Writes) != 0 || s.p.Calls != 0 {
		t.Errorf("writes %q after %d call(s) on Incus; want no call at all", s.p.Writes, s.p.Calls)
	}
	kept := s.p.Instances[project+"/"+sourceVmName]
	isUntouched := kept == original && sameDevices(kept.Devices, devicesBefore) && maps.Equal(kept.Config, configBefore) &&
		slices.Equal(s.snapshotsOf(t, sourceVmName), []string{"skept"})
	if !isUntouched {
		t.Errorf("the original changed: %+v, snapshots %q", kept, s.snapshotsOf(t, sourceVmName))
	}
	reports := s.cp.Requests()
	if len(reports) != 1 || strings.Contains(reports[0].Body, "externalId") || !strings.Contains(reports[0].Body, `"outcome":"Failed"`) {
		t.Errorf("reports %+v; want one Failed that names no provider id", reports)
	}
}
