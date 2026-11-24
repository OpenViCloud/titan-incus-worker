package testsupport

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"maps"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
)

const (
	fakeProject   = "t-0123456789abcdef0123456789abcdef"
	fakePool      = "default"
	archivedUuid  = "11111111-1111-4111-8111-111111111111"
	archivedMac   = "10:66:6a:aa:aa:aa"
	archivedDisk  = "volarchived"
	restoredDisk  = "volrestored"
	importedName  = "vmimported"
	archivedName  = "vmarchived"
	otherInstance = "vmother"
)

// vmArchive is a VM with a root disk, one NIC and one attached volume, backed up with two snapshots.
func vmArchive() ArchiveDescription {
	return ArchiveDescription{
		Name:      archivedName,
		Snapshots: []string{"backup", "sold"},
		Devices: map[string]map[string]string{
			"root":       {"type": "disk", "path": "/", "pool": fakePool, "size": "5GiB"},
			"eth0":       {"type": "nic", "network": "net-1", "ipv4.address": "10.0.0.5"},
			archivedDisk: {"type": "disk", "pool": fakePool, "source": archivedDisk},
		},
		Config: map[string]string{
			"volatile.uuid":        archivedUuid,
			"volatile.eth0.hwaddr": archivedMac,
			"limits.cpu":           "2",
			"user.titan-name":      "web-1",
		},
	}
}

func withVolumes(names ...string) *Provider {
	p := NewProvider()
	for _, name := range names {
		p.Volumes[fakeProject+"/"+name] = &api.StorageVolume{Name: name, Type: "custom", Project: fakeProject}
	}
	return p
}

func importVm(t *testing.T, p *Provider, name string, body string, overrides provider.InstanceOverrides) *api.Instance {
	t.Helper()
	if err := p.ImportInstance(context.Background(), fakeProject, fakePool, name, strings.NewReader(body), overrides); err != nil {
		t.Fatalf("import %s: %v", name, err)
	}
	instance, err := p.GetInstance(context.Background(), fakeProject, name)
	if err != nil {
		t.Fatalf("read %s after its import: %v", name, err)
	}
	return instance
}

func snapshotNames(t *testing.T, p *Provider, instance string) []string {
	t.Helper()
	snapshots, err := p.ListInstanceSnapshots(context.Background(), fakeProject, instance)
	if err != nil {
		t.Fatal(err)
	}
	names := []string{}
	for _, snapshot := range snapshots {
		names = append(names, snapshot.Name)
	}
	slices.Sort(names)
	return names
}

// What Incus reads first in an archive is what a handler reads too: the bytes must be a gzipped tar that opens on
// backup/index.yaml.
func TestTarGz_IsAGzippedTar_WhoseFirstEntryIsTheIndex(t *testing.T) {
	unzipped, err := gzip.NewReader(strings.NewReader(vmArchive().TarGz()))
	if err != nil {
		t.Fatalf("not gzip: %v", err)
	}
	header, err := tar.NewReader(unzipped).Next()
	if err != nil {
		t.Fatalf("not a tar: %v", err)
	}
	if header.Name != "backup/index.yaml" {
		t.Fatalf("first entry is %q", header.Name)
	}
}

func TestImportInstance_AnArchiveWithAnIndex_MakesTheInstanceItDescribes(t *testing.T) {
	p := withVolumes(archivedDisk)
	archive := vmArchive()

	instance := importVm(t, p, importedName, archive.TarGz(), provider.InstanceOverrides{})

	if len(instance.Devices) != 3 {
		t.Fatalf("devices: %v", instance.Devices)
	}
	for name, want := range archive.Devices {
		if !maps.Equal(instance.Devices[name], want) {
			t.Errorf("device %s: got %v, want %v", name, instance.Devices[name], want)
		}
	}
	if !maps.Equal(instance.Config, archive.Config) {
		t.Errorf("config: got %v, want %v", instance.Config, archive.Config)
	}
	if instance.StatusCode != api.Stopped {
		t.Errorf("an imported instance is stopped, got %v", instance.StatusCode)
	}
	if got := snapshotNames(t, p, importedName); !slices.Equal(got, []string{"backup", "sold"}) {
		t.Errorf("snapshots: %v", got)
	}
	recorded := p.ImportedInstances
	if len(recorded) != 1 || recorded[0].Name != importedName || recorded[0].Body != archive.TarGz() {
		t.Errorf("recorded imports: %d", len(recorded))
	}
}

// The override rules of cmd/incusd/api_internal.go of v7.0.1 (internalImportFromBackup): one key of one device set, a
// device the archive lacks added, a config key set. Incus keeps no key with an empty value.
func TestImportInstance_Overrides_ChangeOneKeyOfADevice_AddADevice_SetAConfigKey_AndDropAKeyLeftEmpty(t *testing.T) {
	p := withVolumes(archivedDisk, restoredDisk, "volextra")
	overrides := provider.InstanceOverrides{
		Devices: []string{archivedDisk + ",source=" + restoredDisk, "eth0,ipv4.address=", "extra,type=disk", "extra,pool=" + fakePool, "extra,source=volextra"},
		Config:  []string{"limits.cpu=4", "user.titan-name="},
	}

	instance := importVm(t, p, importedName, vmArchive().TarGz(), overrides)

	wantDisk := map[string]string{"type": "disk", "pool": fakePool, "source": restoredDisk}
	if !maps.Equal(instance.Devices[archivedDisk], wantDisk) {
		t.Errorf("overridden device: %v", instance.Devices[archivedDisk])
	}
	wantNic := map[string]string{"type": "nic", "network": "net-1"}
	if !maps.Equal(instance.Devices["eth0"], wantNic) {
		t.Errorf("nic after its address was emptied: %v", instance.Devices["eth0"])
	}
	wantExtra := map[string]string{"type": "disk", "pool": fakePool, "source": "volextra"}
	if !maps.Equal(instance.Devices["extra"], wantExtra) {
		t.Errorf("added device: %v", instance.Devices["extra"])
	}
	if instance.Config["limits.cpu"] != "4" {
		t.Errorf("config override: %v", instance.Config)
	}
	if _, kept := instance.Config["user.titan-name"]; kept {
		t.Errorf("a config key emptied is still there: %v", instance.Config)
	}
	if instance.Config["volatile.uuid"] != archivedUuid || instance.Config["volatile.eth0.hwaddr"] != archivedMac {
		t.Errorf("keys no override named changed: %v", instance.Config)
	}
	if !slices.Equal(p.ImportedInstances[0].Overrides.Devices, overrides.Devices) || !slices.Equal(p.ImportedInstances[0].Overrides.Config, overrides.Config) {
		t.Errorf("recorded overrides: %+v", p.ImportedInstances[0].Overrides)
	}
}

// An emptied volatile.uuid and an emptied volatile.<nic>.hwaddr come back filled with a value of the instance's own
// (measured on Incus 7.0.2, titan-ops features/backup/design.md#restore-as-new-resource).
func TestImportInstance_AnEmptiedUuidOrNicAddress_IsMadeAfresh_ForEachImport(t *testing.T) {
	for emptied, archived := range map[string]string{"volatile.uuid": archivedUuid, "volatile.eth0.hwaddr": archivedMac} {
		p := withVolumes("volfirst", "volsecond")
		made := []string{}
		for _, which := range []string{"first", "second"} {
			overrides := provider.InstanceOverrides{
				Devices: []string{archivedDisk + ",source=vol" + which},
				Config:  []string{emptied + "=", "volatile.eth9.hwaddr="},
			}

			instance := importVm(t, p, "vm"+which, vmArchive().TarGz(), overrides)

			value := instance.Config[emptied]
			if value == "" || value == archived {
				t.Errorf("%s of the %s import is %q, not a value of its own", emptied, which, value)
			}
			if _, hasAddress := instance.Config["volatile.eth9.hwaddr"]; hasAddress {
				t.Errorf("an address was made for a NIC the instance does not have: %v", instance.Config)
			}
			made = append(made, value)
		}
		if made[0] == made[1] {
			t.Errorf("two imports share the %s %q", emptied, made[0])
		}
	}
}

func TestImportInstance_ASnapshotListBody_ImportsWithThoseSnapshots_AndTakesOverrides(t *testing.T) {
	p := NewProvider()

	instance := importVm(t, p, importedName, ArchiveBody("backup", "keep"), provider.InstanceOverrides{Config: []string{"user.titan-name=copy"}})

	if got := snapshotNames(t, p, importedName); !slices.Equal(got, []string{"backup", "keep"}) {
		t.Errorf("snapshots: %v", got)
	}
	if len(instance.Devices) != 0 || !maps.Equal(instance.Config, map[string]string{"user.titan-name": "copy"}) {
		t.Errorf("instance: devices %v, config %v", instance.Devices, instance.Config)
	}
	if p.ImportedInstances[0].Body != "backup\nkeep" {
		t.Errorf("recorded body: %q", p.ImportedInstances[0].Body)
	}
}

// The refusals measured on Incus 7.0.2 (titan-ops features/backup/design.md#restore-as-new-resource): a name already
// taken, a block volume another instance has attached, and a device naming a volume the project lacks.
func TestImportInstance_RefusesWhatIncusRefuses_AndMakesNoInstance(t *testing.T) {
	attachedElsewhere := func(p *Provider) {
		p.Instances[fakeProject+"/"+otherInstance] = &api.Instance{Name: otherInstance, InstancePut: api.InstancePut{
			Devices: map[string]map[string]string{archivedDisk: {"type": "disk", "pool": fakePool, "source": archivedDisk}},
		}}
	}
	for name, tc := range map[string]struct {
		arrange   func(*Provider)
		overrides provider.InstanceOverrides
		says      string
	}{
		"the name is taken": {
			arrange: func(p *Provider) { p.Instances[fakeProject+"/"+importedName] = &api.Instance{Name: importedName} },
			says:    "Cannot restore volume, already exists on target",
		},
		"the volume is attached to another instance": {
			arrange: attachedElsewhere,
			says:    "Cannot add un-shared custom storage block volume to more than one instance",
		},
		"an override names a volume that is not there": {
			arrange:   attachedElsewhere,
			overrides: provider.InstanceOverrides{Devices: []string{archivedDisk + ",source=volmissing"}},
			says:      `Failed add validation for device "` + archivedDisk + `": Failed loading custom volume: Storage volume not found`,
		},
	} {
		p := withVolumes(archivedDisk)
		tc.arrange(p)
		before := len(p.Instances)

		err := p.ImportInstance(context.Background(), fakeProject, fakePool, importedName, strings.NewReader(vmArchive().TarGz()), tc.overrides)

		if !provider.IsOperationFailureContaining(err, tc.says) {
			t.Errorf("%s: want a failed operation saying %q, got %v", name, tc.says, err)
		}
		if len(p.Instances) != before || len(snapshotNames(t, p, importedName)) != 0 {
			t.Errorf("%s: a refused import left something: %v", name, p.Instances)
		}
		if len(p.ImportedInstances) != 1 {
			t.Errorf("%s: the import call was not recorded", name)
		}
	}
}

func TestImportInstance_AnOverrideIncusCannotParse_IsABadRequest(t *testing.T) {
	for name, overrides := range map[string]provider.InstanceOverrides{
		"a config entry without =": {Config: []string{"volatile.uuid"}},
		"a device entry without ,": {Devices: []string{"source=vol"}},
		"a device entry without =": {Devices: []string{"vol,source"}},
	} {
		p := withVolumes(archivedDisk)

		err := p.ImportInstance(context.Background(), fakeProject, fakePool, importedName, strings.NewReader(vmArchive().TarGz()), overrides)

		if !api.StatusErrorCheck(err, http.StatusBadRequest) || len(p.Instances) != 0 {
			t.Errorf("%s: want a 400 and no instance, got %v, %v", name, err, p.Instances)
		}
	}
}

func TestImportVolume_ReadsEitherArchiveForm_AndRefusesANameThatIsTaken(t *testing.T) {
	indexed := ArchiveDescription{Name: archivedDisk, Type: "custom", Snapshots: []string{"backup", "s1"}}.TarGz()
	for name, body := range map[string]string{"snapshot list": ArchiveBody("backup", "s1"), "index": indexed} {
		p := NewProvider()

		err := p.ImportVolume(context.Background(), fakeProject, fakePool, restoredDisk, strings.NewReader(body))

		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		snapshots, _ := p.ListVolumeSnapshots(context.Background(), fakeProject, fakePool, restoredDisk)
		if _, found := p.Volumes[fakeProject+"/"+restoredDisk]; !found || len(snapshots) != 2 {
			t.Errorf("%s: volume %v, snapshots %v", name, p.Volumes, snapshots)
		}

		again := p.ImportVolume(context.Background(), fakeProject, fakePool, restoredDisk, strings.NewReader(body))

		want := `Create custom volume from backup: Volume "` + restoredDisk + `" already exists in pool "` + fakePool + `"`
		if !provider.IsOperationFailureContaining(again, want) || len(p.ImportedVolumes) != 2 {
			t.Errorf("%s: a second import of the name: %v (%d calls recorded)", name, again, len(p.ImportedVolumes))
		}
	}
}

func storedInstance(p *Provider) {
	p.Instances[fakeProject+"/"+importedName] = &api.Instance{Name: importedName, InstancePut: api.InstancePut{
		Devices: map[string]map[string]string{
			"root":       {"type": "disk", "path": "/", "pool": fakePool},
			archivedDisk: {"type": "disk", "pool": fakePool, "source": archivedDisk},
		},
		Config: map[string]string{"limits.cpu": "2", "user.titan-name": "web-1"},
	}}
}

// A PUT replaces the instance's writable part: what it leaves out is gone at the next read.
func TestUpdateInstance_ReplacesTheDevicesAndConfig_AndTheNextReadSeesThem(t *testing.T) {
	p := NewProvider()
	storedInstance(p)
	read, etag, err := p.GetInstanceWithEtag(context.Background(), fakeProject, importedName)
	if err != nil {
		t.Fatal(err)
	}
	put := read.Writable()
	delete(put.Devices, archivedDisk)
	put.Devices[restoredDisk] = map[string]string{"type": "disk", "pool": fakePool, "source": restoredDisk}
	put.Config["user.titan-name"] = "web-1-copy"

	if err := p.UpdateInstance(context.Background(), fakeProject, importedName, put, etag); err != nil {
		t.Fatal(err)
	}
	// The caller's maps are its own: a change after the write is not a write.
	put.Devices["later"] = map[string]string{"type": "disk"}
	put.Config["later"] = "x"

	after, newEtag, _ := p.GetInstanceWithEtag(context.Background(), fakeProject, importedName)
	if len(after.Devices) != 2 || after.Devices[restoredDisk]["source"] != restoredDisk || after.Devices["root"]["path"] != "/" {
		t.Errorf("devices after the PUT: %v", after.Devices)
	}
	if !maps.Equal(after.Config, map[string]string{"limits.cpu": "2", "user.titan-name": "web-1-copy"}) {
		t.Errorf("config after the PUT: %v", after.Config)
	}
	if newEtag == etag {
		t.Error("the ETag did not change with the write")
	}
	if len(p.UpdatedInstances) != 1 {
		t.Errorf("recorded PUTs: %+v", p.UpdatedInstances)
	}
}

// A rollback is a PUT that carries only the snapshot's name; the fake records it and keeps the instance's devices and
// config, as a disk-only rollback does.
func TestUpdateInstance_ARollback_AStaleEtag_OrAFailedWrite_LeaveTheStoredInstanceAsItWas(t *testing.T) {
	for name, write := range map[string]func(p *Provider) error{
		"a rollback": func(p *Provider) error {
			return p.UpdateInstance(context.Background(), fakeProject, importedName, api.InstancePut{Restore: "backup", DiskOnly: true}, "")
		},
		"a stale etag": func(p *Provider) error {
			err := p.UpdateInstance(context.Background(), fakeProject, importedName, api.InstancePut{}, "stale")
			if !provider.IsEtagMismatch(err) {
				return err
			}
			return nil
		},
		"a failed write": func(p *Provider) error {
			p.UpdateInstanceErr = io.ErrUnexpectedEOF
			_ = p.UpdateInstance(context.Background(), fakeProject, importedName, api.InstancePut{}, "")
			return nil
		},
	} {
		p := NewProvider()
		storedInstance(p)

		if err := write(p); err != nil {
			t.Fatalf("%s: %v", name, err)
		}

		after, _ := p.GetInstance(context.Background(), fakeProject, importedName)
		if len(after.Devices) != 2 || len(after.Config) != 2 {
			t.Errorf("%s: the instance changed: devices %v, config %v", name, after.Devices, after.Config)
		}
	}
}

// Incus's PATCH replaces each device it names whole and sets each config key it names; the rest stays.
func TestPatchInstance_MergesItsDevicesAndConfigIntoTheStoredInstance(t *testing.T) {
	p := NewProvider()
	storedInstance(p)
	patch := api.InstancePut{
		Devices: map[string]map[string]string{archivedDisk: {"type": "disk", "pool": "other", "source": restoredDisk}},
		Config:  map[string]string{"user.titan-name": "renamed"},
	}

	if err := p.PatchInstance(context.Background(), fakeProject, importedName, patch); err != nil {
		t.Fatal(err)
	}

	after, _ := p.GetInstance(context.Background(), fakeProject, importedName)
	if !maps.Equal(after.Devices[archivedDisk], patch.Devices[archivedDisk]) || after.Devices["root"]["path"] != "/" {
		t.Errorf("devices after the PATCH: %v", after.Devices)
	}
	if !maps.Equal(after.Config, map[string]string{"limits.cpu": "2", "user.titan-name": "renamed"}) {
		t.Errorf("config after the PATCH: %v", after.Config)
	}
}

func TestUpdateAndPatchInstance_OfAnInstanceTheFakeDoesNotHold_AreStillRecorded(t *testing.T) {
	p := NewProvider()

	updateErr := p.UpdateInstance(context.Background(), fakeProject, "vmunknown", api.InstancePut{Restore: "s1", DiskOnly: true}, "")
	patchErr := p.PatchInstance(context.Background(), fakeProject, "vmunknown", api.InstancePut{Config: map[string]string{"a": "b"}})

	if updateErr != nil || patchErr != nil || len(p.UpdatedInstances) != 1 || len(p.PatchedInstances) != 1 || len(p.Instances) != 0 {
		t.Fatalf("errors %v %v, PUTs %d, PATCHes %d, instances %v", updateErr, patchErr, len(p.UpdatedInstances), len(p.PatchedInstances), p.Instances)
	}
}

func TestPatchVolumeConfig_RecordsThePatch_AndTheNextReadSeesItsKeysBesideTheOthers(t *testing.T) {
	p := NewProvider()
	p.Volumes[fakeProject+"/"+restoredDisk] = &api.StorageVolume{Name: restoredDisk, Type: "custom",
		StorageVolumePut: api.StorageVolumePut{Config: map[string]string{"size": "1GiB", "user.titan-name": "data-1"}}}

	err := p.PatchVolumeConfig(context.Background(), fakeProject, fakePool, restoredDisk, map[string]string{"user.titan-name": "data-1-copy"})

	if err != nil {
		t.Fatal(err)
	}
	volume, _ := p.GetVolume(context.Background(), fakeProject, fakePool, restoredDisk)
	if !maps.Equal(volume.Config, map[string]string{"size": "1GiB", "user.titan-name": "data-1-copy"}) {
		t.Errorf("config after the patch: %v", volume.Config)
	}
	want := VolumeConfigPatch{Project: fakeProject, Pool: fakePool, Name: restoredDisk, Config: map[string]string{"user.titan-name": "data-1-copy"}}
	if len(p.PatchedVolumeConfigs) != 1 || p.PatchedVolumeConfigs[0].Project != want.Project || p.PatchedVolumeConfigs[0].Pool != want.Pool ||
		p.PatchedVolumeConfigs[0].Name != want.Name || !maps.Equal(p.PatchedVolumeConfigs[0].Config, want.Config) {
		t.Errorf("recorded patches: %+v", p.PatchedVolumeConfigs)
	}
}

func TestPatchVolumeConfig_AVolumeThatIsNotThere_OrAnInjectedError_ChangesNothing(t *testing.T) {
	p := NewProvider()

	missing := p.PatchVolumeConfig(context.Background(), fakeProject, fakePool, "volgone", map[string]string{"user.titan-name": "x"})

	if !provider.IsNotFound(missing) || len(p.Volumes) != 0 {
		t.Fatalf("a volume that is not there: %v, %v", missing, p.Volumes)
	}

	p.Volumes[fakeProject+"/"+restoredDisk] = &api.StorageVolume{Name: restoredDisk, Type: "custom"}
	p.PatchVolumeConfigErr = io.ErrUnexpectedEOF
	injected := p.PatchVolumeConfig(context.Background(), fakeProject, fakePool, restoredDisk, map[string]string{"user.titan-name": "x"})

	if injected != io.ErrUnexpectedEOF || len(p.Volumes[fakeProject+"/"+restoredDisk].Config) != 0 || len(p.PatchedVolumeConfigs) != 2 {
		t.Fatalf("an injected error: %v, config %v, %d calls recorded", injected, p.Volumes[fakeProject+"/"+restoredDisk].Config, len(p.PatchedVolumeConfigs))
	}
}

// A handler that deletes every snapshot it lists does so in the order of the list: the fake's must not change from run
// to run.
func TestListSnapshots_AnswerInTheOrderOfTheirNames(t *testing.T) {
	p := NewProvider()
	names := []string{"s7", "s2", "backup", "s5", "s1", "s9", "s3", "s8", "s4", "s6"}
	for _, name := range names {
		p.InstanceSnapshots[fakeProject+"/"+importedName+"/"+name] = true
		p.VolumeSnapshots[fakeProject+"/"+restoredDisk+"/"+name] = true
	}
	slices.Sort(names)

	ofInstance, _ := p.ListInstanceSnapshots(context.Background(), fakeProject, importedName)
	ofVolume, _ := p.ListVolumeSnapshots(context.Background(), fakeProject, fakePool, restoredDisk)

	listed, listedOfVolume := []string{}, []string{}
	for _, snapshot := range ofInstance {
		listed = append(listed, snapshot.Name)
	}
	for _, snapshot := range ofVolume {
		listedOfVolume = append(listedOfVolume, strings.TrimPrefix(snapshot.Name, restoredDisk+"/"))
	}
	if !slices.Equal(listed, names) || !slices.Equal(listedOfVolume, names) {
		t.Errorf("instance snapshots %v, volume snapshots %v, want both %v", listed, listedOfVolume, names)
	}
}

// Incus answers 409 to a custom volume under a name already there (lab-storage 02): a command that makes its volume
// twice must take the second answer for its own.
func TestCreateVolume_UnderANameAlreadyThere_IsAConflict_AndKeepsTheVolumeThatWasThere(t *testing.T) {
	p := withVolumes(restoredDisk)
	p.Volumes[fakeProject+"/"+restoredDisk].Config = map[string]string{"size": "5GiB"}

	err := p.CreateVolume(context.Background(), fakeProject, fakePool, api.StorageVolumesPost{Name: restoredDisk})

	if !provider.IsConflict(err) {
		t.Fatalf("err = %v, want a 409", err)
	}
	if p.Volumes[fakeProject+"/"+restoredDisk].Config["size"] != "5GiB" || len(p.CreatedVolumes) != 1 {
		t.Errorf("volume %+v, creates recorded %d; want the volume kept and the call recorded", p.Volumes[fakeProject+"/"+restoredDisk], len(p.CreatedVolumes))
	}
	if err := p.CreateVolume(context.Background(), "t-another", fakePool, api.StorageVolumesPost{Name: restoredDisk}); err != nil {
		t.Errorf("the same name in another project: %v", err)
	}
}

// Incus refuses to delete a volume an instance has as a disk (400, lab-storage 04): a command that removes a volume
// before the device naming it fails here as it would there.
func TestDeleteVolume_OfAVolumeAnInstanceOfTheProjectHasAsADisk_IsRefused_AndKeepsIt(t *testing.T) {
	p := withVolumes(archivedDisk)
	p.Instances[fakeProject+"/"+otherInstance] = &api.Instance{Name: otherInstance, InstancePut: api.InstancePut{
		Devices: map[string]map[string]string{"data": {"type": "disk", "pool": fakePool, "source": archivedDisk}},
	}}
	p.Instances["t-another/"+otherInstance] = &api.Instance{Name: otherInstance}

	err := p.DeleteVolume(context.Background(), fakeProject, fakePool, archivedDisk)

	if !api.StatusErrorCheck(err, http.StatusBadRequest) || !strings.Contains(err.Error(), "still in use") {
		t.Fatalf("err = %v, want a 400 that says the volume is in use", err)
	}
	if _, kept := p.Volumes[fakeProject+"/"+archivedDisk]; !kept || len(p.DeletedVolumes) != 1 {
		t.Errorf("kept %v, deletes recorded %d; want the volume kept and the call recorded", kept, len(p.DeletedVolumes))
	}

	delete(p.Instances[fakeProject+"/"+otherInstance].Devices, "data")

	if err := p.DeleteVolume(context.Background(), fakeProject, fakePool, archivedDisk); err != nil {
		t.Fatalf("once the device is gone: %v", err)
	}
	if _, kept := p.Volumes[fakeProject+"/"+archivedDisk]; kept {
		t.Error("the volume is still there")
	}
}

// An instance that cannot be read is neither there nor gone: a command must not take the error for a 404.
func TestGetInstance_WithAnInjectedError_AnswersIt_WithOrWithoutTheEtag(t *testing.T) {
	unreadable := errors.New("connection refused")
	p := NewProvider()
	p.Instances[fakeProject+"/"+importedName] = &api.Instance{Name: importedName}
	p.GetInstanceErr = unreadable

	_, err := p.GetInstance(context.Background(), fakeProject, importedName)
	_, _, errWithEtag := p.GetInstanceWithEtag(context.Background(), fakeProject, importedName)

	if !errors.Is(err, unreadable) || !errors.Is(errWithEtag, unreadable) {
		t.Fatalf("errs = %v, %v; want the injected error from both", err, errWithEtag)
	}
}

// The fake holds a volume by its project and name, whatever pool a call names: a handler that asks the wrong pool is
// answered as if it had asked the right one, and only the pools kept here tell.
func TestProvider_KeepsThePoolOfEveryCallThatNamesOne_InCallOrder_AndOfNoOther(t *testing.T) {
	p := NewProvider()
	ctx := context.Background()

	_, _ = p.GetStoragePoolResources(ctx, "pool01")
	_, _ = p.GetVolume(ctx, fakeProject, "pool02", restoredDisk)
	_ = p.CreateVolume(ctx, fakeProject, "pool03", api.StorageVolumesPost{Name: restoredDisk})
	_ = p.PatchVolumeConfig(ctx, fakeProject, "pool04", restoredDisk, map[string]string{"user.titan-name": "data"})
	_ = p.CreateVolumeSnapshot(ctx, fakeProject, "pool05", restoredDisk, "backup")
	_, _ = p.ListVolumeSnapshots(ctx, fakeProject, "pool06", restoredDisk)
	_ = p.RestoreVolume(ctx, fakeProject, "pool07", restoredDisk, "backup")
	_ = p.DeleteVolumeSnapshot(ctx, fakeProject, "pool08", restoredDisk, "backup")
	_ = p.BackupVolume(ctx, fakeProject, "pool09", restoredDisk, api.BackupTarget{})
	_, _ = p.ListRunningVolumeOperations(ctx, fakeProject, "pool10", restoredDisk)
	_ = p.DeleteVolume(ctx, fakeProject, "pool11", restoredDisk)
	_ = p.ImportVolume(ctx, fakeProject, "pool12", restoredDisk, strings.NewReader(ArchiveBody("backup")))
	_ = p.ImportInstance(ctx, fakeProject, "pool13", importedName, strings.NewReader(ArchiveBody("backup")), provider.InstanceOverrides{})
	_, _ = p.ListVolumesAllProjects(ctx, "pool14")
	_, _ = p.GetInstance(ctx, fakeProject, importedName)
	_, _ = p.ListRunningInstanceOperations(ctx, fakeProject, importedName)
	_ = p.DeleteInstance(ctx, fakeProject, importedName)

	want := []string{"pool01", "pool02", "pool03", "pool04", "pool05", "pool06", "pool07", "pool08", "pool09", "pool10",
		"pool11", "pool12", "pool13", "pool14"}
	if !slices.Equal(p.Pools, want) {
		t.Errorf("pools\n got %q\nwant %q", p.Pools, want)
	}
	if p.Calls != 17 {
		t.Errorf("%d calls counted, want 17", p.Calls)
	}
}

// What Incus still runs is what a test says it runs, on one instance or volume of one project.
func TestListRunningOperations_AnswerWhatATestHoldsForThatInstanceOrVolume_AndNothingForAnother(t *testing.T) {
	p := NewProvider()
	ctx := context.Background()
	importing := api.Operation{ID: "op-1", Description: "Restoring backup", StatusCode: api.Running}
	p.RunningOperations[fakeProject+"/"+importedName] = []api.Operation{importing}
	p.RunningOperations[fakeProject+"/"+restoredDisk] = []api.Operation{{ID: "op-2", Description: "Restoring custom volume backup", StatusCode: api.Running}}

	onInstance, instanceErr := p.ListRunningInstanceOperations(ctx, fakeProject, importedName)
	onVolume, volumeErr := p.ListRunningVolumeOperations(ctx, fakeProject, fakePool, restoredDisk)
	onAnotherInstance, _ := p.ListRunningInstanceOperations(ctx, fakeProject, otherInstance)
	onAnotherVolume, _ := p.ListRunningVolumeOperations(ctx, fakeProject, fakePool, archivedDisk)
	inAnotherProject, _ := p.ListRunningInstanceOperations(ctx, "t-another", importedName)
	volumeInAnotherProject, _ := p.ListRunningVolumeOperations(ctx, "t-another", fakePool, restoredDisk)

	if instanceErr != nil || len(onInstance) != 1 || onInstance[0].ID != "op-1" {
		t.Errorf("on the instance: %+v, %v", onInstance, instanceErr)
	}
	if volumeErr != nil || len(onVolume) != 1 || onVolume[0].ID != "op-2" {
		t.Errorf("on the volume: %+v, %v", onVolume, volumeErr)
	}
	if len(onAnotherInstance)+len(onAnotherVolume)+len(inAnotherProject)+len(volumeInAnotherProject) != 0 {
		t.Errorf("operations answered for what none names: %+v %+v %+v %+v", onAnotherInstance, onAnotherVolume, inAnotherProject, volumeInAnotherProject)
	}
}

// Operations that cannot be listed are not "none running".
func TestListRunningOperations_WithAnInjectedError_AnswerIt_AndNoOperation(t *testing.T) {
	unreadable := errors.New("connection refused")
	p := NewProvider()
	p.RunningOperations[fakeProject+"/"+importedName] = []api.Operation{{ID: "op-1"}}
	p.RunningOperations[fakeProject+"/"+restoredDisk] = []api.Operation{{ID: "op-2"}}
	p.ListOperationsErr = unreadable

	onInstance, instanceErr := p.ListRunningInstanceOperations(context.Background(), fakeProject, importedName)
	onVolume, volumeErr := p.ListRunningVolumeOperations(context.Background(), fakeProject, fakePool, restoredDisk)

	if !errors.Is(instanceErr, unreadable) || !errors.Is(volumeErr, unreadable) || onInstance != nil || onVolume != nil {
		t.Fatalf("errs = %v, %v; answers %+v, %+v; want the injected error and nothing", instanceErr, volumeErr, onInstance, onVolume)
	}
}
