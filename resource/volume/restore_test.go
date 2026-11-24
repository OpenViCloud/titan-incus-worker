package volume

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

// A restore makes a volume of its own from one archive, beside the volume the archive was taken of
// (titan-ops features/backup/design.md#restore-as-new-resource). In these tests the volume made is volumeId, whose
// create request the command ends; the original is sourceVolumeId.
const (
	sourceVolumeId = "99999999-8888-4777-8666-555555555555"
	// Oracle: python3 base32 of sourceVolumeId, as in provider/names_test.go.
	sourceVolumeName = "voltgmztgmirbdxpbtgkvkvkvkvku"
	restoreTenantId  = "0123abcd-0000-4000-8000-000000000001"
	restoreBackupKey = "20261122T020000Z"
	// The key volume.backup wrote: incus/<cluster>/<tenant>/volumes/<the original's id>/<time>.tar.gz.
	restoreArchiveKey = "incus/mock/0123abcd-0000-4000-8000-000000000001/volumes/99999999-8888-4777-8666-555555555555/20261122T020000Z.tar.gz"
	restorePool       = "pool1"
	restoredName      = "data-1-copy"
	restoreFailedText = "Restoring the volume from its backup failed due to a system error. Please contact support."
	// The request of an earlier restore, of the volume the archive was taken of.
	earlierRequestId = "66666666-5555-4444-8333-222222222222"
	doneReport       = `{"outcome":"Done","externalId":"` + volumeName + `"}`
	gib              = uint64(1) << 30
)

type restoreScene struct {
	cp      *testsupport.ControlPlane
	p       *testsupport.Recorder
	bucket  *testsupport.Bucket
	handler *Restore
}

// newRestoreScene is a tenant's project where the original volume, with a snapshot the tenant took, is attached to a
// running VM; a 40 GiB pool 4 GiB used; and the bucket with the original's archive carrying the snapshots given. The
// cluster's pool is not the default one, and the fake answers a call whatever pool it names: when the test ends, every
// pool a call named must have been the cluster's.
func newRestoreScene(t *testing.T, archived ...string) *restoreScene {
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
	p.Volumes[project+"/"+sourceVolumeName] = &api.StorageVolume{
		Name: sourceVolumeName, Type: "custom", Project: project, UsedBy: []string{usedByInstance},
		StorageVolumePut: api.StorageVolumePut{Config: map[string]string{"size": "20GiB", DisplayNameKey: "data-1"}},
	}
	p.VolumeSnapshots[project+"/"+sourceVolumeName+"/skept"] = true
	bucket := testsupport.NewBucket()
	bucket.Objects[restoreArchiveKey] = testsupport.ArchiveDescription{Name: sourceVolumeName, Type: "custom", Snapshots: archived}.TarGz()
	cp := testsupport.NewControlPlane(t)
	handler := NewRestore(cp.Client(), cluster.Fixed{clusterId: {Provider: p, StoragePool: restorePool}}, bucket)
	return &restoreScene{cp: cp, p: p, bucket: bucket, handler: handler}
}

// restoreLeased is the command of a 20 GiB volume; a nil value in changes leaves that field out.
func restoreLeased(changes map[string]any) command.Leased {
	payload := map[string]any{
		"newVolumeId": volumeId, "requestId": requestId, "name": restoredName, "sourceVolumeId": sourceVolumeId,
		"backupKey": restoreBackupKey, "tenantId": restoreTenantId, "projectName": project, "sizeGb": 20,
	}
	for field, value := range changes {
		if value == nil {
			delete(payload, field)
			continue
		}
		payload[field] = value
	}
	return leased("volume.restore", payload)
}

func (s *restoreScene) run(l command.Leased) error {
	return s.handler.Run(context.Background(), l, testsupport.DiscardLogger())
}

func (s *restoreScene) giveUp(l command.Leased, cause error) error {
	return s.handler.GiveUp(context.Background(), l, cause, testsupport.DiscardLogger())
}

// alreadyImported puts the copy on Incus as an earlier run of the command left it, with these snapshots.
func (s *restoreScene) alreadyImported(snapshots ...string) {
	s.p.Volumes[project+"/"+volumeName] = &api.StorageVolume{Name: volumeName, Type: "custom", Project: project}
	for _, snapshot := range snapshots {
		s.p.VolumeSnapshots[project+"/"+volumeName+"/"+snapshot] = true
	}
}

// usedSinceItWasRestored makes the copy what a volume the tenant has had for a while is: attached to a running VM,
// with a snapshot the tenant took and the "backup" snapshot the daily backup takes on the volume itself.
func (s *restoreScene) usedSinceItWasRestored() {
	s.p.Volumes[project+"/"+volumeName].UsedBy = []string{usedByInstance}
	s.p.VolumeSnapshots[project+"/"+volumeName+"/backup"] = true
	s.p.VolumeSnapshots[project+"/"+volumeName+"/stenant"] = true
}

// importStillRunning leaves on Incus the operation of an import of the copy nobody waits on any more.
func (s *restoreScene) importStillRunning() {
	s.p.RunningOperations[project+"/"+volumeName] = []api.Operation{
		{ID: "op-import", Class: "task", Description: "Restoring custom volume backup", Status: "Running", StatusCode: api.Running},
	}
}

func (s *restoreScene) snapshotsOf(t *testing.T, volume string) []string {
	t.Helper()
	snapshots, err := s.p.ListVolumeSnapshots(context.Background(), project, restorePool, volume)
	if err != nil {
		t.Fatal(err)
	}
	names := []string{}
	for _, snapshot := range snapshots {
		names = append(names, strings.TrimPrefix(snapshot.Name, volume+"/"))
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

func isPermanent(err error) bool {
	return err != nil && !command.IsRetryable(err)
}

func isRetryable(err error) bool {
	return err != nil && command.IsRetryable(err)
}

func TestRestore_ImportsTheArchiveAsAVolumeOfItsOwn_RollsItBack_DropsItsSnapshots_NamesIt_AndEndsTheCreateDone(t *testing.T) {
	s := newRestoreScene(t, "backup", "sold1", "sold2")

	err := s.run(restoreLeased(nil))

	if err != nil {
		t.Fatal(err)
	}
	target := project + "/" + volumeName
	want := []string{
		"import-volume " + target,
		"rollback-volume " + target + "/backup",
		"delete-volume-snapshot " + target + "/backup",
		"delete-volume-snapshot " + target + "/sold1",
		"delete-volume-snapshot " + target + "/sold2",
		"patch-volume " + target,
	}
	if !slices.Equal(s.p.Writes, want) {
		t.Errorf("writes\n got %q\nwant %q", s.p.Writes, want)
	}
	if !slices.Equal(s.bucket.Opened, []string{restoreArchiveKey}) || s.bucket.Open != 0 {
		t.Errorf("archives opened %q, %d left open; want %s alone, closed", s.bucket.Opened, s.bucket.Open, restoreArchiveKey)
	}
	imported := s.p.ImportedVolumes
	isTheArchiveInTheClustersPool := len(imported) == 1 && imported[0].Pool == restorePool && imported[0].Body == s.bucket.Objects[restoreArchiveKey]
	if !isTheArchiveInTheClustersPool {
		t.Errorf("import: %+v", imported)
	}
	// The import keeps the original's name; the copy is known by the one the admin typed. The same write, the last one
	// of the copy, says which request finished it.
	named := s.p.PatchedVolumeConfigs
	isNamedAndFinished := len(named) == 1 && named[0].Pool == restorePool &&
		maps.Equal(named[0].Config, map[string]string{"user.titan-name": restoredName, "user.titan-restore": requestId})
	if !isNamedAndFinished {
		t.Errorf("name and finish marker: %+v", named)
	}
	if left := s.snapshotsOf(t, volumeName); len(left) != 0 {
		t.Errorf("snapshots left on the copy: %q", left)
	}
	// Where it is attached is the reconciler's to say: a copy that goes with a VM is reported before that VM exists.
	if got := s.reportBody(t); got != doneReport {
		t.Errorf("report\n got %s\nwant %s", got, doneReport)
	}
}

// Every write of a run, up to the last one, which fails here, and the give-up's removal after it.
func TestRestore_LeavesTheOriginalVolumeAsItIs_AndNoWriteNamesIt(t *testing.T) {
	s := newRestoreScene(t, "backup", "sold1")
	original := s.p.Volumes[project+"/"+sourceVolumeName]
	configBefore := maps.Clone(original.Config)
	s.p.PatchVolumeConfigErr = errors.New("locked")

	if err := s.run(restoreLeased(nil)); err == nil {
		t.Fatal("the run passed although its last write failed")
	}
	if err := s.giveUp(restoreLeased(nil), errors.New("name volume: locked")); err != nil {
		t.Fatal(err)
	}

	target := project + "/" + volumeName
	if len(s.p.Writes) != 6 || s.p.Writes[4] != "patch-volume "+target || s.p.Writes[5] != "delete-volume "+target {
		t.Fatalf("writes %q: the check below needs the whole run and the give-up's removal", s.p.Writes)
	}
	for _, write := range s.p.Writes {
		if strings.Contains(write, sourceVolumeName) {
			t.Errorf("a write names the original: %q", write)
		}
	}
	kept := s.p.Volumes[project+"/"+sourceVolumeName]
	isUntouched := kept == original && maps.Equal(kept.Config, configBefore) && slices.Equal(kept.UsedBy, []string{usedByInstance}) &&
		slices.Equal(s.snapshotsOf(t, sourceVolumeName), []string{"skept"})
	if !isUntouched {
		t.Errorf("the original changed: %+v, snapshots %q", kept, s.snapshotsOf(t, sourceVolumeName))
	}
}

// An archive from before backups took the "backup" snapshot cannot be put back to the moment it was taken.
func TestRestore_AnArchiveWithoutTheBackupSnapshot_IsPermanent_AndItsImportIsRemoved(t *testing.T) {
	s := newRestoreScene(t, "sold1")

	err := s.run(restoreLeased(nil))

	if !isPermanent(err) {
		t.Fatalf("want a permanent error, got %v", err)
	}
	wantText := "archive " + restoreArchiveKey + ` has no "backup" snapshot: an old-format archive, not usable`
	if err.Error() != wantText {
		t.Errorf("error\n got %q\nwant %q", err, wantText)
	}
	want := []string{"import-volume " + project + "/" + volumeName, "delete-volume " + project + "/" + volumeName}
	if !slices.Equal(s.p.Writes, want) {
		t.Errorf("writes\n got %q\nwant %q", s.p.Writes, want)
	}
	if len(s.cp.Requests()) != 0 {
		t.Errorf("reported on a run that failed: %+v", s.cp.Requests())
	}
}

// The rule of restore.CheckPoolRoom, on the size the command carries: 1.5 x 20 GiB + a tenth of the 40 GiB pool.
func TestRestore_APoolOneByteShort_IsPermanentBeforeAnythingIsImported_AndOneExactlyLargeEnough_Imports(t *testing.T) {
	required := 30*gib + 4*gib
	for name, tc := range map[string]struct {
		free uint64
		ok   bool
	}{"exactly": {required, true}, "one byte less": {required - 1, false}} {
		s := newRestoreScene(t, "backup")
		s.p.PoolSpace = api.ResourcesStoragePoolSpace{Total: 40 * gib, Used: 40*gib - tc.free}

		err := s.run(restoreLeased(nil))

		if tc.ok {
			if err != nil || len(s.p.ImportedVolumes) != 1 {
				t.Errorf("%s: err %v, imports %+v; want the import", name, err, s.p.ImportedVolumes)
			}
			continue
		}
		if !isPermanent(err) || !strings.Contains(err.Error(), "importing 20.0 GiB needs 34.0 GiB") {
			t.Errorf("%s: err = %v, want a permanent refusal that says the size and the need", name, err)
		}
		if len(s.p.Writes) != 0 || len(s.bucket.Opened) != 0 || len(s.cp.Requests()) != 0 {
			t.Errorf("%s: wrote %q, opened %q, reported %+v; want nothing", name, s.p.Writes, s.bucket.Opened, s.cp.Requests())
		}
	}
}

// A pool that could not be read says nothing of its room: the next lease asks again.
func TestRestore_APoolThatCannotBeRead_IsRetryable_AndImportsNothing(t *testing.T) {
	s := newRestoreScene(t, "backup")
	s.p.PoolResourcesErr = errors.New("incus is busy")

	err := s.run(restoreLeased(nil))

	if !isRetryable(err) || len(s.p.Writes) != 0 || len(s.bucket.Opened) != 0 {
		t.Fatalf("err = %v, wrote %q, opened %q; want a retryable error and nothing done", err, s.p.Writes, s.bucket.Opened)
	}
}

func TestRestore_APayloadThatIsNotUsable_IsPermanent_AndCallsNothing(t *testing.T) {
	commands := map[string]command.Leased{
		"not json": {Kind: "volume.restore", ClusterId: clusterId, Payload: []byte("{")},
		// The copy under the original's own name would be taken for an import already made, and written.
		"the original's own id": restoreLeased(map[string]any{"newVolumeId": sourceVolumeId}),
		"a size below zero":     restoreLeased(map[string]any{"sizeGb": -1}),
	}
	for _, field := range []string{"newVolumeId", "requestId", "sourceVolumeId", "backupKey", "tenantId", "projectName"} {
		commands["no "+field] = restoreLeased(map[string]any{field: nil})
	}
	for field, value := range map[string]string{
		"newVolumeId": "not-a-titan-id", "sourceVolumeId": "../other", "backupKey": "20261122T020000Z.tar.gz",
	} {
		commands["a "+field+" that is "+value] = restoreLeased(map[string]any{field: value})
	}
	for name, l := range commands {
		s := newRestoreScene(t, "backup")

		err := s.run(l)

		if !isPermanent(err) {
			t.Errorf("%s: err = %v, want a permanent error", name, err)
		}
		if s.p.Calls != 0 || len(s.bucket.Opened) != 0 || len(s.cp.Requests()) != 0 {
			t.Errorf("%s: %d call(s) on Incus, opened %q, reported %+v; want nothing", name, s.p.Calls, s.bucket.Opened, s.cp.Requests())
		}
	}
}

// A run that died after its import left the volume with the "backup" snapshot: the rollback is still owed, and the
// room the import needed is already the volume's.
func TestRestore_ASecondRun_FindingTheVolumeImported_RollsItBack_WithoutImportingAgainOrAskingForRoom(t *testing.T) {
	s := newRestoreScene(t, "backup", "sold1")
	s.alreadyImported("backup", "sold1")
	s.p.PoolSpace = api.ResourcesStoragePoolSpace{Total: 40 * gib, Used: 40 * gib}

	err := s.run(restoreLeased(nil))

	if err != nil {
		t.Fatal(err)
	}
	target := project + "/" + volumeName
	want := []string{
		"rollback-volume " + target + "/backup",
		"delete-volume-snapshot " + target + "/backup",
		"delete-volume-snapshot " + target + "/sold1",
		"patch-volume " + target,
	}
	if !slices.Equal(s.p.Writes, want) {
		t.Errorf("writes\n got %q\nwant %q", s.p.Writes, want)
	}
	if len(s.bucket.Opened) != 0 {
		t.Errorf("the archive was opened again: %q", s.bucket.Opened)
	}
	if got := s.reportBody(t); got != doneReport {
		t.Errorf("report: %s", got)
	}
}

// No "backup" snapshot on a volume this run did not import: the run before rolled it back and removed it.
func TestRestore_ASecondRun_FindingTheVolumeRolledBack_GoesOnFromThere(t *testing.T) {
	s := newRestoreScene(t, "backup", "sold1")
	s.alreadyImported("sold1")

	err := s.run(restoreLeased(nil))

	if err != nil {
		t.Fatal(err)
	}
	target := project + "/" + volumeName
	want := []string{"delete-volume-snapshot " + target + "/sold1", "patch-volume " + target}
	if !slices.Equal(s.p.Writes, want) {
		t.Errorf("writes\n got %q\nwant %q", s.p.Writes, want)
	}
	if _, kept := s.p.Volumes[target]; !kept {
		t.Error("the volume was removed as an old-format import")
	}
	if got := s.reportBody(t); got != doneReport {
		t.Errorf("report: %s", got)
	}
}

// A command leased again after its report was accepted (the loop's own completion was lost) finds a volume the tenant
// uses. The "backup" snapshot on it is then the daily backup's, and a rollback to it, or the removal of the tenant's
// snapshots, would destroy what they wrote since.
func TestRestore_ARunAfterTheRestoreFinished_WritesNothing_AndEndsTheCreateDoneAgain(t *testing.T) {
	s := newRestoreScene(t, "backup", "sold1")
	if err := s.run(restoreLeased(nil)); err != nil {
		t.Fatal(err)
	}
	s.usedSinceItWasRestored()
	s.p.PoolSpace = api.ResourcesStoragePoolSpace{Total: 40 * gib, Used: 40 * gib}
	s.p.Writes, s.bucket.Opened = nil, nil

	err := s.run(restoreLeased(nil))

	if err != nil {
		t.Fatal(err)
	}
	if len(s.p.Writes) != 0 || len(s.bucket.Opened) != 0 {
		t.Errorf("the second run wrote %q and opened %q; want nothing", s.p.Writes, s.bucket.Opened)
	}
	if left := s.snapshotsOf(t, volumeName); !slices.Equal(left, []string{"backup", "stenant"}) {
		t.Errorf("snapshots left on the copy: %q", left)
	}
	reports := s.cp.RequestsTo(requestRoute)
	if len(reports) != 2 || reports[1].Body != doneReport {
		t.Errorf("reports: %+v", reports)
	}
}

// The archive of a volume that was itself restored carries that restore's request, and the import keeps it: the copy
// is not finished by a request that is not this command's.
func TestRestore_ACopyThatCarriesAnotherRestoresRequest_IsTakenUpAsNotFinished_AndGetsItsOwn(t *testing.T) {
	s := newRestoreScene(t, "backup", "sold1")
	s.alreadyImported("backup", "sold1")
	s.p.Volumes[project+"/"+volumeName].Config = map[string]string{"user.titan-name": "data-1", "user.titan-restore": earlierRequestId}

	err := s.run(restoreLeased(nil))

	if err != nil {
		t.Fatal(err)
	}
	target := project + "/" + volumeName
	want := []string{
		"rollback-volume " + target + "/backup",
		"delete-volume-snapshot " + target + "/backup",
		"delete-volume-snapshot " + target + "/sold1",
		"patch-volume " + target,
	}
	if !slices.Equal(s.p.Writes, want) {
		t.Errorf("writes\n got %q\nwant %q", s.p.Writes, want)
	}
	wantConfig := map[string]string{"user.titan-name": restoredName, "user.titan-restore": requestId}
	if got := s.p.Volumes[target].Config; !maps.Equal(got, wantConfig) {
		t.Errorf("the copy's config: %v, want %v", got, wantConfig)
	}
	if got := s.reportBody(t); got != doneReport {
		t.Errorf("report: %s", got)
	}
}

// Another run of the same command may have removed the snapshot between the list and the delete.
func TestRestore_ASnapshotAlreadyGoneWhenItIsDeleted_IsNotAFailure(t *testing.T) {
	s := newRestoreScene(t, "backup")
	s.alreadyImported("sold1")
	s.p.DeleteSnapshotErr = testsupport.NotFound("Storage volume snapshot not found")

	err := s.run(restoreLeased(nil))

	if err != nil {
		t.Fatalf("a snapshot already gone failed the restore: %v", err)
	}
	if got := s.reportBody(t); got != doneReport {
		t.Errorf("report: %s", got)
	}
}

// Every step may pass on the next lease, and the next run takes the volume up where this one left it: nothing is
// reported, and what was imported stays.
func TestRestore_AStepThatFails_IsRetryable_ReportsNothing_AndKeepsWhatWasImported(t *testing.T) {
	for name, tc := range map[string]struct {
		break_ func(*restoreScene)
		step   string
	}{
		"volume read":     {func(s *restoreScene) { s.p.GetVolumeErr = errors.New("busy") }, "look for volume " + volumeName},
		"archive read":    {func(s *restoreScene) { s.bucket.GetErr = errors.New("bucket unreachable") }, "read archive " + restoreArchiveKey},
		"import":          {func(s *restoreScene) { s.p.ImportVolumeErr = errors.New("bad tar") }, "import volume " + volumeName},
		"rollback":        {func(s *restoreScene) { s.p.RestoreVolumeErr = errors.New("busy") }, "roll volume " + volumeName},
		"snapshot list":   {func(s *restoreScene) { s.p.ListVolumeSnapshotsErr = errors.New("busy") }, "snapshot"},
		"snapshot delete": {func(s *restoreScene) { s.p.DeleteSnapshotErr = errors.New("locked") }, "delete the"},
		"name":            {func(s *restoreScene) { s.p.PatchVolumeConfigErr = errors.New("locked") }, "name volume " + volumeName},
		"report": {func(s *restoreScene) {
			s.cp.Respond("PATCH", "/internal/volumes/"+volumeId+"/requests/"+requestId, 503, "")
		}, "503"},
	} {
		s := newRestoreScene(t, "backup", "sold1")
		tc.break_(s)

		err := s.run(restoreLeased(nil))

		if !isRetryable(err) || !strings.Contains(err.Error(), tc.step) {
			t.Errorf("%s: err = %v, want a retryable error that names %q", name, err, tc.step)
		}
		if slices.Contains(s.p.Writes, "delete-volume "+project+"/"+volumeName) {
			t.Errorf("%s: the import was removed: %q", name, s.p.Writes)
		}
		if name != "report" && len(s.cp.Requests()) != 0 {
			t.Errorf("%s: reported on a run that failed: %+v", name, s.cp.Requests())
		}
	}
}

// secondListFails answers the first list of a volume's snapshots, the rollback's, and fails the next.
type secondListFails struct {
	*testsupport.Recorder
	lists int
}

func (f *secondListFails) ListVolumeSnapshots(ctx context.Context, project string, pool string, volume string) ([]api.StorageVolumeSnapshot, error) {
	f.lists++
	if f.lists > 1 {
		return nil, errors.New("busy")
	}
	return f.Recorder.ListVolumeSnapshots(ctx, project, pool, volume)
}

// Snapshots that cannot be listed are not "none": the copy would be reported with the original's snapshots on it.
func TestRestore_TheCopysSnapshotsThatCannotBeListed_AreRetryable_AndNothingIsReported(t *testing.T) {
	s := newRestoreScene(t, "backup", "sold1")
	handler := NewRestore(s.cp.Client(), cluster.Fixed{clusterId: {Provider: &secondListFails{Recorder: s.p}, StoragePool: restorePool}}, s.bucket)

	err := handler.Run(context.Background(), restoreLeased(nil), testsupport.DiscardLogger())

	if !isRetryable(err) || !strings.Contains(err.Error(), "list the snapshots of volume "+volumeName) {
		t.Fatalf("err = %v, want a retryable error that names the step", err)
	}
	if len(s.cp.Requests()) != 0 {
		t.Errorf("reported on a run that failed: %+v", s.cp.Requests())
	}
}

// An import goes on in Incus after the run that asked for it stopped waiting on it, and Incus makes the volume and
// its snapshots before it unpacks the archive into them: a run that took the volume it finds for imported would roll
// it back, drop its snapshots and report it while the import still writes it. With no volume there yet, a second
// import beside the first would be refused once its whole upload is in.
func TestRestore_ACopyIncusIsStillImporting_IsRetryable_AndNothingIsWrittenOrReported(t *testing.T) {
	for name, leave := range map[string]func(*restoreScene){
		"the volume is there, with its snapshots": func(s *restoreScene) { s.alreadyImported("backup", "sold1") },
		"the volume is there, rolled back":        func(s *restoreScene) { s.alreadyImported() },
		"the volume is not there yet":             func(s *restoreScene) {},
		"the volume carries another restore's request": func(s *restoreScene) {
			s.alreadyImported("backup")
			s.p.Volumes[project+"/"+volumeName].Config = map[string]string{"user.titan-restore": earlierRequestId}
		},
	} {
		s := newRestoreScene(t, "backup", "sold1")
		leave(s)
		s.importStillRunning()

		err := s.run(restoreLeased(nil))

		wantText := "import of " + volumeName + " is still running: incus operation op-import (Restoring custom volume backup)"
		if !isRetryable(err) || err.Error() != wantText {
			t.Errorf("%s: err = %v, want the retryable %q", name, err, wantText)
		}
		if len(s.p.Writes) != 0 || len(s.bucket.Opened) != 0 || len(s.cp.Requests()) != 0 {
			t.Errorf("%s: wrote %q, opened %q, reported %+v; want nothing", name, s.p.Writes, s.bucket.Opened, s.cp.Requests())
		}
	}
}

// Operations that cannot be listed are not "none running".
func TestRestore_OperationsThatCannotBeListed_AreRetryable_AndNothingIsWrittenOrReported(t *testing.T) {
	for name, leave := range map[string]func(*restoreScene){
		"the volume is there":     func(s *restoreScene) { s.alreadyImported("backup") },
		"the volume is not there": func(s *restoreScene) {},
	} {
		s := newRestoreScene(t, "backup")
		leave(s)
		s.p.ListOperationsErr = errors.New("incus is busy")

		err := s.run(restoreLeased(nil))

		if !isRetryable(err) || !strings.Contains(err.Error(), "look for an operation still running on volume "+volumeName) {
			t.Errorf("%s: err = %v, want a retryable error that names the step", name, err)
		}
		if len(s.p.Writes) != 0 || len(s.bucket.Opened) != 0 || len(s.cp.Requests()) != 0 {
			t.Errorf("%s: wrote %q, opened %q, reported %+v; want nothing", name, s.p.Writes, s.bucket.Opened, s.cp.Requests())
		}
	}
}

// A finished copy is the tenant's, and an operation on it is theirs: the daily backup's, say. Neither a run nor a
// give-up that comes after waits for it, or asks.
func TestRestore_AfterTheRestoreFinished_AnOperationOnTheCopy_IsNotWaitedFor(t *testing.T) {
	for name, again := range map[string]func(*restoreScene) error{
		"a run":     func(s *restoreScene) error { return s.run(restoreLeased(nil)) },
		"a give-up": func(s *restoreScene) error { return s.giveUp(restoreLeased(nil), errors.New("lease lost")) },
	} {
		s := newRestoreScene(t, "backup")
		if err := s.run(restoreLeased(nil)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		s.p.RunningOperations[project+"/"+volumeName] = []api.Operation{{ID: "op-backup", Description: "Creating custom volume backup", StatusCode: api.Running}}
		s.p.ListOperationsErr = errors.New("not to be asked")
		s.p.Writes = nil

		err := again(s)

		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		reports := s.cp.RequestsTo(requestRoute)
		if len(s.p.Writes) != 0 || len(reports) != 2 || reports[1].Body != doneReport {
			t.Errorf("%s: wrote %q, reports %+v; want no write and the Done again", name, s.p.Writes, reports)
		}
	}
}

// A give-up that removed a volume Incus is still importing would have it removed under the import, or made again by
// it afterwards with no row in Titan to name it; one that found no volume yet would say nothing is left. It comes
// again once the operation has ended.
func TestRestore_GiveUp_OfACopyIncusIsStillImporting_IsRetryable_AndNothingIsRemovedOrReported(t *testing.T) {
	for name, leave := range map[string]func(*restoreScene){
		"the volume is there":         func(s *restoreScene) { s.alreadyImported("backup") },
		"the volume is not there yet": func(s *restoreScene) {},
	} {
		s := newRestoreScene(t, "backup")
		leave(s)
		s.importStillRunning()

		err := s.giveUp(restoreLeased(nil), errors.New("waiting for incus operation op-import: connection reset by peer"))

		wantText := "import of " + volumeName + " is still running: incus operation op-import (Restoring custom volume backup)"
		if !isRetryable(err) || err.Error() != wantText {
			t.Errorf("%s: err = %v, want the retryable %q", name, err, wantText)
		}
		if len(s.p.Writes) != 0 || len(s.cp.Requests()) != 0 {
			t.Errorf("%s: wrote %q, reported %+v; want nothing", name, s.p.Writes, s.cp.Requests())
		}
	}
}

func TestRestore_GiveUp_RemovesTheImportedVolume_AndEndsTheCreateFailed_WithoutAnId(t *testing.T) {
	s := newRestoreScene(t, "backup")
	s.alreadyImported("backup", "sold1")

	err := s.giveUp(restoreLeased(nil), errors.New("roll volume back: busy"))

	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"delete-volume " + project + "/" + volumeName}; !slices.Equal(s.p.Writes, want) {
		t.Errorf("writes\n got %q\nwant %q", s.p.Writes, want)
	}
	if len(s.p.DeletedVolumes) != 1 || s.p.DeletedVolumes[0] != project+"/"+volumeName {
		t.Errorf("deleted: %q", s.p.DeletedVolumes)
	}
	wantReport := `{"outcome":"Failed","reason":"` + restoreFailedText + `","detail":"roll volume back: busy"}`
	if got := s.reportBody(t); got != wantReport {
		t.Errorf("report\n got %s\nwant %s", got, wantReport)
	}
}

// The restore did finish, whatever the loop gives up on afterwards: the volume is the tenant's, and telling Titan its
// create failed would be as wrong as removing it.
func TestRestore_GiveUp_AfterTheRestoreFinished_RemovesNothing_AndEndsTheCreateDone(t *testing.T) {
	s := newRestoreScene(t, "backup", "sold1")
	if err := s.run(restoreLeased(nil)); err != nil {
		t.Fatal(err)
	}
	s.usedSinceItWasRestored()
	s.p.Writes = nil

	err := s.giveUp(restoreLeased(nil), errors.New("lease lost"))

	if err != nil {
		t.Fatal(err)
	}
	if len(s.p.Writes) != 0 {
		t.Errorf("the give-up wrote %q", s.p.Writes)
	}
	_, kept := s.p.Volumes[project+"/"+volumeName]
	if left := s.snapshotsOf(t, volumeName); !kept || !slices.Equal(left, []string{"backup", "stenant"}) {
		t.Errorf("the copy kept: %v, its snapshots: %q", kept, left)
	}
	reports := s.cp.RequestsTo(requestRoute)
	if len(reports) != 2 || reports[1].Body != doneReport {
		t.Errorf("reports: %+v", reports)
	}
}

// A copy that carries the request of an earlier restore, as its archive did, is this command's unfinished import.
func TestRestore_GiveUp_OfACopyThatCarriesAnotherRestoresRequest_RemovesIt_AndEndsTheCreateFailed(t *testing.T) {
	s := newRestoreScene(t, "backup")
	s.alreadyImported("backup")
	s.p.Volumes[project+"/"+volumeName].Config = map[string]string{"user.titan-restore": earlierRequestId}

	err := s.giveUp(restoreLeased(nil), errors.New("roll volume back: busy"))

	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"delete-volume " + project + "/" + volumeName}; !slices.Equal(s.p.Writes, want) {
		t.Errorf("writes\n got %q\nwant %q", s.p.Writes, want)
	}
	wantReport := `{"outcome":"Failed","reason":"` + restoreFailedText + `","detail":"roll volume back: busy"}`
	if got := s.reportBody(t); got != wantReport {
		t.Errorf("report\n got %s\nwant %s", got, wantReport)
	}
}

func TestRestore_GiveUp_WithNothingImported_WritesNothing_AndEndsTheCreateFailed_WithoutAnId(t *testing.T) {
	s := newRestoreScene(t, "backup")

	err := s.giveUp(restoreLeased(nil), errors.New("not enough space"))

	if err != nil {
		t.Fatal(err)
	}
	if len(s.p.Writes) != 0 {
		t.Errorf("wrote %q with nothing to remove", s.p.Writes)
	}
	wantReport := `{"outcome":"Failed","reason":"` + restoreFailedText + `","detail":"not enough space"}`
	if got := s.reportBody(t); got != wantReport {
		t.Errorf("report\n got %s\nwant %s", got, wantReport)
	}
}

// A volume still on Incus, or one nobody could look for, is told to Titan by its id: Titan's own delete of the failed
// volume then reaches it.
func TestRestore_GiveUp_WhenTheVolumeMayStillBeThere_EndsTheCreateFailed_WithItsId(t *testing.T) {
	for name, tc := range map[string]struct {
		break_  func(*restoreScene)
		cluster string
		writes  []string
	}{
		"the delete fails": {func(s *restoreScene) { s.p.DeleteVolumeErr = errors.New("The storage volume is still in use") }, clusterId,
			[]string{"delete-volume " + project + "/" + volumeName}},
		"the volume cannot be read":   {func(s *restoreScene) { s.p.GetVolumeErr = errors.New("connection refused") }, clusterId, nil},
		"the cluster cannot be found": {func(s *restoreScene) {}, "elsewhere", nil},
		// An import may still be at work on it: it is not removed under one.
		"its operations cannot be listed": {func(s *restoreScene) { s.p.ListOperationsErr = errors.New("connection refused") }, clusterId, nil},
		"no volume yet, and its operations cannot be listed": {func(s *restoreScene) {
			delete(s.p.Volumes, project+"/"+volumeName)
			s.p.ListOperationsErr = errors.New("connection refused")
		}, clusterId, nil},
	} {
		s := newRestoreScene(t, "backup")
		s.alreadyImported()
		tc.break_(s)
		l := restoreLeased(nil)
		l.ClusterId = tc.cluster

		err := s.giveUp(l, errors.New("lease lost"))

		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !slices.Equal(s.p.Writes, tc.writes) {
			t.Errorf("%s: writes\n got %q\nwant %q", name, s.p.Writes, tc.writes)
		}
		wantReport := `{"outcome":"Failed","externalId":"` + volumeName + `","reason":"` + restoreFailedText + `","detail":"lease lost"}`
		if got := s.reportBody(t); got != wantReport {
			t.Errorf("%s: report\n got %s\nwant %s", name, got, wantReport)
		}
	}
}

// Another run's give-up may have removed the volume between the look and the delete: it is gone all the same.
func TestRestore_GiveUp_WhenTheVolumeIsGoneByTheTimeItIsDeleted_EndsTheCreateFailed_WithoutAnId(t *testing.T) {
	s := newRestoreScene(t, "backup")
	s.alreadyImported()
	s.p.DeleteVolumeErr = testsupport.NotFound("Storage pool volume not found")

	err := s.giveUp(restoreLeased(nil), errors.New("lease lost"))

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
		"no request":               {"requestId": nil},
		"an empty request":         {"requestId": ""},
		"no request and no volume": {"requestId": nil, "newVolumeId": nil},
	} {
		s := newRestoreScene(t, "backup")
		s.alreadyImported()
		logger, logs := testsupport.NewLogCapture(t)

		err := s.handler.GiveUp(context.Background(), restoreLeased(changes), errors.New("boom"), logger)

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

// A request with no volume to end it on can never be reported: retrying the give-up changes nothing.
func TestRestore_GiveUp_WithARequestAndNoVolumeToReportOn_IsPermanent_AndCallsNothing(t *testing.T) {
	s := newRestoreScene(t, "backup")
	s.alreadyImported()

	err := s.giveUp(restoreLeased(map[string]any{"newVolumeId": nil}), errors.New("boom"))

	if !isPermanent(err) || !strings.Contains(err.Error(), "nobody to tell") {
		t.Errorf("err = %v, want a permanent error that says nobody can be told", err)
	}
	if s.p.Calls != 0 || len(s.cp.Requests()) != 0 {
		t.Errorf("%d call(s) on Incus, reported %+v; want nothing", s.p.Calls, s.cp.Requests())
	}
}

// A payload that is not JSON names no request: nothing can be ended, and no retry reads it better.
func TestRestore_GiveUp_OfAPayloadThatDoesNotDecode_IsPermanent_AndCallsNothing(t *testing.T) {
	s := newRestoreScene(t, "backup")

	err := s.giveUp(command.Leased{Kind: "volume.restore", ClusterId: clusterId, Payload: []byte("{")}, errors.New("boom"))

	if !isPermanent(err) || s.p.Calls != 0 || len(s.cp.Requests()) != 0 {
		t.Errorf("err = %v, %d call(s) on Incus, reported %+v; want a permanent error and nothing done", err, s.p.Calls, s.cp.Requests())
	}
}

// The detail is the admin's; it is cut as every Failed cuts it.
func TestRestore_GiveUp_CutsTheDetail(t *testing.T) {
	s := newRestoreScene(t, "backup")

	if err := s.giveUp(restoreLeased(nil), errors.New(strings.Repeat("x", 1500))); err != nil {
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

// An admin finds a restore in the log by its archive and by the volume it was taken of; the command loop's own keys
// stay the loop's.
func TestRestore_LogsWhereTheImportStartsAndWhereItIsDone_WithTheArchiveTheSourceAndTheCopy(t *testing.T) {
	s := newRestoreScene(t, "backup")
	logger, logs := testsupport.NewLogCapture(t)

	err := s.handler.Run(context.Background(), restoreLeased(nil), logger.With(loopKeys...))

	if err != nil {
		t.Fatal(err)
	}
	for _, line := range []map[string]any{logs.One("volume restore started"), logs.One("volume restored")} {
		isFindable := line["object"] == restoreArchiveKey && line["source"] == sourceVolumeId && line["volume"] == volumeName
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
	for name, act := range map[string]func(*restoreScene, *slog.Logger){
		"a run that takes the copy up": func(s *restoreScene, logger *slog.Logger) {
			s.alreadyImported("backup")
			_ = s.handler.Run(context.Background(), restoreLeased(nil), logger)
		},
		"a run after the copy was finished": func(s *restoreScene, logger *slog.Logger) {
			_ = s.run(restoreLeased(nil))
			_ = s.handler.Run(context.Background(), restoreLeased(nil), logger)
		},
		"a give-up that removes the copy": func(s *restoreScene, logger *slog.Logger) {
			s.alreadyImported("backup")
			_ = s.handler.GiveUp(context.Background(), restoreLeased(nil), errors.New("boom"), logger)
		},
		"a give-up that cannot remove it": func(s *restoreScene, logger *slog.Logger) {
			s.alreadyImported("backup")
			s.p.DeleteVolumeErr = errors.New("busy")
			_ = s.handler.GiveUp(context.Background(), restoreLeased(nil), errors.New("boom"), logger)
		},
		"a give-up after the copy was finished": func(s *restoreScene, logger *slog.Logger) {
			_ = s.run(restoreLeased(nil))
			_ = s.handler.GiveUp(context.Background(), restoreLeased(nil), errors.New("boom"), logger)
		},
		"a give-up that cannot read the copy": func(s *restoreScene, logger *slog.Logger) {
			s.p.GetVolumeErr = errors.New("busy")
			_ = s.handler.GiveUp(context.Background(), restoreLeased(nil), errors.New("boom"), logger)
		},
		"a give-up that cannot list the operations": func(s *restoreScene, logger *slog.Logger) {
			s.p.ListOperationsErr = errors.New("busy")
			_ = s.handler.GiveUp(context.Background(), restoreLeased(nil), errors.New("boom"), logger)
		},
		"a give-up in a cluster that is gone": func(s *restoreScene, logger *slog.Logger) {
			l := restoreLeased(nil)
			l.ClusterId = "elsewhere"
			_ = s.handler.GiveUp(context.Background(), l, errors.New("boom"), logger)
		},
		"a give-up without a request": func(s *restoreScene, logger *slog.Logger) {
			_ = s.handler.GiveUp(context.Background(), restoreLeased(map[string]any{"requestId": nil}), errors.New("boom"), logger)
		},
	} {
		s := newRestoreScene(t, "backup")
		logger, logs := testsupport.NewLogCapture(t)

		act(s, logger.With(loopKeys...))

		assertTheLoopsKeysAreKept(t, name, logs)
	}
}

// A payload Run refuses made nothing on Incus, and what is under the name it gives is not this command's: a give-up
// that looked there would take whatever it found for its own import and delete it.
func TestRestore_GiveUp_OfAPayloadRunRefuses_MakesNoCallOnIncus_AndEndsTheCreateFailed_WithoutAnId(t *testing.T) {
	refused := map[string]map[string]any{
		"the original's own id":         {"newVolumeId": sourceVolumeId},
		"an id that names no volume":    {"newVolumeId": "not-a-titan-id"},
		"a source that names no volume": {"sourceVolumeId": "../other"},
		"a backupKey that is no moment": {"backupKey": "20261122T020000Z.tar.gz"},
		"a size below zero":             {"sizeGb": -1},
	}
	for _, field := range []string{"sourceVolumeId", "backupKey", "tenantId", "projectName"} {
		refused["no "+field] = map[string]any{field: nil}
	}
	for name, changes := range refused {
		s := newRestoreScene(t, "backup")
		s.alreadyImported("backup")
		l := restoreLeased(changes)
		if err := s.run(l); !isPermanent(err) || s.p.Calls != 0 {
			t.Fatalf("%s: Run answered %v after %d call(s) on Incus: not a payload Run refuses", name, err, s.p.Calls)
		}
		told := volumeId
		if given, isText := changes["newVolumeId"].(string); isText {
			told = given
		}

		err := s.giveUp(l, errors.New("payload refused"))

		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if s.p.Calls != 0 || len(s.p.Writes) != 0 {
			t.Errorf("%s: %d call(s) on Incus, writes %q; want none", name, s.p.Calls, s.p.Writes)
		}
		reports := s.cp.RequestsTo("PATCH /internal/volumes/" + told + "/requests/" + requestId)
		wantReport := `{"outcome":"Failed","reason":"` + restoreFailedText + `","detail":"payload refused"}`
		if len(reports) != 1 || len(s.cp.Requests()) != 1 || reports[0].Body != wantReport {
			t.Errorf("%s: reports %+v; want one Failed without an id, on the request of volume %s", name, s.cp.Requests(), told)
		}
	}
}

// The copy's id equal to the original's makes the copy's name the original's: Run refuses it, and the give-up after
// that refusal must not find the original under "the copy's name" and remove it.
func TestRestore_GiveUp_OfAPayloadThatRestoresAVolumeOntoItself_LeavesTheOriginalAsItIs(t *testing.T) {
	s := newRestoreScene(t, "backup")
	// Free, as a detached volume is: Incus would delete it.
	original := s.p.Volumes[project+"/"+sourceVolumeName]
	original.UsedBy = nil
	configBefore := maps.Clone(original.Config)
	l := restoreLeased(map[string]any{"newVolumeId": sourceVolumeId})

	runErr := s.run(l)
	err := s.giveUp(l, runErr)

	if !isPermanent(runErr) || !strings.Contains(runErr.Error(), "onto itself") {
		t.Fatalf("Run: err = %v, want the permanent refusal of a volume restored onto itself", runErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(s.p.Writes) != 0 || s.p.Calls != 0 {
		t.Errorf("writes %q after %d call(s) on Incus; want no call at all", s.p.Writes, s.p.Calls)
	}
	kept := s.p.Volumes[project+"/"+sourceVolumeName]
	if kept != original || !maps.Equal(kept.Config, configBefore) || !slices.Equal(s.snapshotsOf(t, sourceVolumeName), []string{"skept"}) {
		t.Errorf("the original changed: %+v, snapshots %q", kept, s.snapshotsOf(t, sourceVolumeName))
	}
	reports := s.cp.Requests()
	if len(reports) != 1 || strings.Contains(reports[0].Body, "externalId") || !strings.Contains(reports[0].Body, `"outcome":"Failed"`) {
		t.Errorf("reports %+v; want one Failed that names no provider id", reports)
	}
}
