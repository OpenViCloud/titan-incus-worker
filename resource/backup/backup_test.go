package backup

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

const (
	vmId     = "11111111-2222-3333-4444-555555555555"
	volumeId = "11111111-2222-3333-4444-555555555555"
	// Oracle: python3 base32 of the id, as in provider/names_test.go.
	encodedId    = "ceircejceiztgrcekvkvkvkvku"
	instanceName = "vmsource"
	volumeName   = "volsource"
	tenantId     = "0123abcd-0000-4000-8000-000000000001"
	project      = "t-0123abcd000040008000000000000001"
	zoneId       = "mock"
	pool         = "default"
)

var takenAt = time.Date(2026, 10, 26, 2, 7, 9, 0, time.UTC)

var target = Target{
	Url:             "https://account.r2.cloudflarestorage.com",
	Bucket:          "titan-backup-dev",
	AccessKeyId:     "key-id",
	SecretAccessKey: "secret-key",
}

func zones(p *testsupport.Provider) cluster.Fixed {
	return cluster.Fixed{zoneId: {Provider: p}}
}

func leased(kind string, payload map[string]any) command.Leased {
	encoded, _ := json.Marshal(payload)
	return command.Leased{Kind: kind, ZoneId: zoneId, Payload: encoded, Attempt: 1, MaxAttempts: 5}
}

func vmLeased() command.Leased {
	return leased("vm.backup", map[string]any{
		"vmId": vmId, "externalVmId": instanceName, "projectName": project, "tenantId": tenantId,
	})
}

func volumeLeased() command.Leased {
	return leased("volume.backup", map[string]any{
		"volumeId": volumeId, "externalVolumeId": volumeName, "projectName": project, "tenantId": tenantId,
	})
}

func runningVm(p *testsupport.Provider) {
	p.Instances[project+"/"+instanceName] = &api.Instance{Name: instanceName, Status: "Running", StatusCode: api.Running}
}

// fakeObjects answers Has from answers in turn, then with the last one; it records each key asked.
type fakeObjects struct {
	answers []bool
	err     error
	asked   []string
}

func (f *fakeObjects) Has(ctx context.Context, key string) (bool, error) {
	f.asked = append(f.asked, key)
	if f.err != nil {
		return false, f.err
	}
	answer := f.answers[0]
	if len(f.answers) > 1 {
		f.answers = f.answers[1:]
	}
	return answer, nil
}

func arrived() *fakeObjects {
	return &fakeObjects{answers: []bool{true}}
}

var quickArrival = Arrival{Every: time.Millisecond, Within: 20 * time.Millisecond}

func newVm(p *testsupport.Provider, configured Target) *Vm {
	return newVmWith(p, configured, arrived())
}

func newVmWith(p *testsupport.Provider, configured Target, objects *fakeObjects) *Vm {
	h := NewVm(zones(p), configured, objects)
	h.now = func() time.Time { return takenAt }
	h.arrival = quickArrival
	return h
}

func newVolume(p *testsupport.Provider, configured Target) *Volume {
	h := NewVolume(zones(p), configured, arrived())
	h.now = func() time.Time { return takenAt }
	h.arrival = quickArrival
	return h
}

func TestVm_BacksUpACopyOfASnapshot_NotTheRunningDisk_ToTheTargetsKey(t *testing.T) {
	p := testsupport.NewProvider()
	runningVm(p)

	err := newVm(p, target).Run(context.Background(), vmLeased(), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	wantOrder := []string{
		"snapshot " + project + "/" + instanceName + "/backup",
		"copy " + project + "/" + instanceName + "/backup -> bk" + encodedId,
		"backup-instance " + project + "/bk" + encodedId,
		"delete-instance " + project + "/bk" + encodedId,
		"delete-snapshot " + project + "/" + instanceName + "/backup",
	}
	if strings.Join(p.BackupOrder, "\n") != strings.Join(wantOrder, "\n") {
		t.Fatalf("order:\n%s\nwant:\n%s", strings.Join(p.BackupOrder, "\n"), strings.Join(wantOrder, "\n"))
	}
	if len(p.InstanceBackups) != 1 {
		t.Fatalf("backups: %+v", p.InstanceBackups)
	}
	sent := p.InstanceBackups[0].Target
	wantKey := "incus/mock/" + tenantId + "/vms/" + vmId + "/20261026T020709Z.tar.gz"
	isTarget := sent.Protocol == "s3" && sent.URL == target.Url && sent.BucketName == target.Bucket &&
		sent.Path == wantKey && sent.AccessKey == target.AccessKeyId && sent.SecretKey == target.SecretAccessKey
	if !isTarget {
		t.Fatalf("target: %+v, want path %s", sent, wantKey)
	}
}

// Leftovers of a run that died midway (worker restart, lease lost) are removed first, or the snapshot and the copy
// would refuse to be made again.
func TestVm_ARunAfterOneThatDiedMidway_RemovesItsLeftoversFirst(t *testing.T) {
	p := testsupport.NewProvider()
	runningVm(p)
	p.InstanceSnapshots[project+"/"+instanceName+"/backup"] = true
	p.Instances[project+"/bk"+encodedId] = &api.Instance{Name: "bk" + encodedId, StatusCode: api.Stopped}

	err := newVm(p, target).Run(context.Background(), vmLeased(), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(strings.Join(p.BackupOrder, "\n"), "delete-instance "+project+"/bk"+encodedId+"\ndelete-snapshot "+project+"/"+instanceName+"/backup\nsnapshot ") {
		t.Fatalf("order: %v", p.BackupOrder)
	}
	if len(p.InstanceBackups) != 1 {
		t.Fatalf("backups: %+v", p.InstanceBackups)
	}
}

// A failed upload still removes the copy and the snapshot, and is retried.
func TestVm_UploadFails_CleansUp_AndIsRetryable(t *testing.T) {
	p := testsupport.NewProvider()
	runningVm(p)
	p.BackupErr = errors.New("incus operation \"Backing up instance\" ended Failure: AccessDenied")

	err := newVm(p, target).Run(context.Background(), vmLeased(), testsupport.DiscardLogger())

	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want a retryable error, got %v", err)
	}
	order := strings.Join(p.BackupOrder, "\n")
	if !strings.HasSuffix(order, "delete-instance "+project+"/bk"+encodedId+"\ndelete-snapshot "+project+"/"+instanceName+"/backup") {
		t.Fatalf("order: %v", p.BackupOrder)
	}
}

func TestVm_GoneFromIncus_IsPermanent_AndMakesNothing(t *testing.T) {
	p := testsupport.NewProvider()

	err := newVm(p, target).Run(context.Background(), vmLeased(), testsupport.DiscardLogger())

	if err == nil || command.IsRetryable(err) {
		t.Fatalf("want a permanent error, got %v", err)
	}
	if len(p.BackupOrder) != 0 {
		t.Fatalf("made: %v", p.BackupOrder)
	}
}

func TestVm_NoTargetConfigured_IsPermanent_AndNamesWhatIsMissing(t *testing.T) {
	p := testsupport.NewProvider()
	runningVm(p)
	missingSecret := target
	missingSecret.SecretAccessKey = ""

	err := newVm(p, missingSecret).Run(context.Background(), vmLeased(), testsupport.DiscardLogger())

	if err == nil || command.IsRetryable(err) || !strings.Contains(err.Error(), "BACKUP_S3_SECRET_ACCESS_KEY") {
		t.Fatalf("want a permanent error naming the setting, got %v", err)
	}
	if strings.Contains(err.Error(), target.AccessKeyId) || len(p.BackupOrder) != 0 {
		t.Fatalf("error %q, made %v", err, p.BackupOrder)
	}
}

func TestVm_PayloadWithoutTenant_IsPermanent(t *testing.T) {
	p := testsupport.NewProvider()
	runningVm(p)
	incomplete := leased("vm.backup", map[string]any{"vmId": vmId, "externalVmId": instanceName, "projectName": project})

	err := newVm(p, target).Run(context.Background(), incomplete, testsupport.DiscardLogger())

	if err == nil || command.IsRetryable(err) {
		t.Fatalf("want a permanent error, got %v", err)
	}
}

func TestVm_GiveUp_RemovesLeftovers(t *testing.T) {
	p := testsupport.NewProvider()
	p.InstanceSnapshots[project+"/"+instanceName+"/backup"] = true
	p.Instances[project+"/bk"+encodedId] = &api.Instance{Name: "bk" + encodedId, StatusCode: api.Stopped}

	err := newVm(p, target).GiveUp(context.Background(), vmLeased(), errors.New("cause"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(p.BackupOrder, "\n") != "delete-instance "+project+"/bk"+encodedId+"\ndelete-snapshot "+project+"/"+instanceName+"/backup" {
		t.Fatalf("order: %v", p.BackupOrder)
	}
}

func TestVolume_BacksUpACopyOfASnapshot_ToTheTargetsKey(t *testing.T) {
	p := testsupport.NewProvider()
	p.Volumes[project+"/"+volumeName] = &api.StorageVolume{Name: volumeName, Type: "custom", Project: project}

	err := newVolume(p, target).Run(context.Background(), volumeLeased(), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	wantOrder := []string{
		"snapshot-volume " + project + "/" + volumeName + "/backup",
		"copy-volume " + project + "/" + volumeName + "/backup -> bkv" + encodedId,
		"backup-volume " + project + "/bkv" + encodedId,
		"delete-volume " + project + "/bkv" + encodedId,
		"delete-volume-snapshot " + project + "/" + volumeName + "/backup",
	}
	if strings.Join(p.BackupOrder, "\n") != strings.Join(wantOrder, "\n") {
		t.Fatalf("order:\n%s\nwant:\n%s", strings.Join(p.BackupOrder, "\n"), strings.Join(wantOrder, "\n"))
	}
	if len(p.VolumeBackups) != 1 || p.VolumeBackups[0].Pool != pool ||
		p.VolumeBackups[0].Target.Path != "incus/mock/"+tenantId+"/volumes/"+volumeId+"/20261026T020709Z.tar.gz" {
		t.Fatalf("backups: %+v", p.VolumeBackups)
	}
}

func TestVolume_GoneFromIncus_IsPermanent(t *testing.T) {
	p := testsupport.NewProvider()

	err := newVolume(p, target).Run(context.Background(), volumeLeased(), testsupport.DiscardLogger())

	if err == nil || command.IsRetryable(err) || len(p.BackupOrder) != 0 {
		t.Fatalf("err %v, made %v", err, p.BackupOrder)
	}
}

func TestTarget_Missing_NamesEverySettingThatIsEmpty(t *testing.T) {
	missing := Target{Url: "https://x"}.Missing()

	if strings.Join(missing, ",") != "BACKUP_S3_BUCKET,BACKUP_S3_ACCESS_KEY_ID,BACKUP_S3_SECRET_ACCESS_KEY" {
		t.Fatalf("missing: %v", missing)
	}
	if len(target.Missing()) != 0 {
		t.Fatalf("a full target misses %v", target.Missing())
	}
}

// The worker's clock may run in any zone; the key is always in UTC, so one instant has one key.
func TestObjectKey_IsInUtc_WhateverTheClocksZone(t *testing.T) {
	saigon := time.FixedZone("Asia/Saigon", 7*60*60)

	key := ObjectKey("mock", tenantId, "vms", vmId, time.Date(2026, 10, 26, 9, 7, 9, 0, saigon))

	if key != "incus/mock/"+tenantId+"/vms/"+vmId+"/20261026T020709Z.tar.gz" {
		t.Fatalf("key: %s", key)
	}
}

// Incus ends its backup operation once the archive is written into the upload, before the upload has finished
// (cmd/incusd/instance_backup.go of v7.0.1): done is when the object is in the bucket.
func TestVm_DoneOnlyOnceTheObjectIsInTheBucket_AskingAgainMeanwhile(t *testing.T) {
	p := testsupport.NewProvider()
	runningVm(p)
	objects := &fakeObjects{answers: []bool{false, false, true}}

	err := newVmWith(p, target, objects).Run(context.Background(), vmLeased(), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	wantKey := "incus/mock/" + tenantId + "/vms/" + vmId + "/20261026T020709Z.tar.gz"
	if len(objects.asked) != 3 || objects.asked[2] != wantKey {
		t.Fatalf("asked: %v", objects.asked)
	}
}

func TestVm_ObjectNeverArrives_IsRetryable_AndCleansUp(t *testing.T) {
	p := testsupport.NewProvider()
	runningVm(p)

	err := newVmWith(p, target, &fakeObjects{answers: []bool{false}}).Run(context.Background(), vmLeased(), testsupport.DiscardLogger())

	if err == nil || !command.IsRetryable(err) || !strings.Contains(err.Error(), "not in the bucket") {
		t.Fatalf("want a retryable error saying the object is missing, got %v", err)
	}
	if !strings.HasSuffix(strings.Join(p.BackupOrder, "\n"), "delete-snapshot "+project+"/"+instanceName+"/backup") {
		t.Fatalf("order: %v", p.BackupOrder)
	}
}

func TestVm_BucketUnreadable_IsRetryable(t *testing.T) {
	p := testsupport.NewProvider()
	runningVm(p)
	objects := &fakeObjects{err: errors.New("read object: AccessDenied")}

	err := newVmWith(p, target, objects).Run(context.Background(), vmLeased(), testsupport.DiscardLogger())

	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want a retryable error, got %v", err)
	}
}

func TestVolume_ObjectNeverArrives_IsRetryable(t *testing.T) {
	p := testsupport.NewProvider()
	p.Volumes[project+"/"+volumeName] = &api.StorageVolume{Name: volumeName, Type: "custom", Project: project}
	h := NewVolume(zones(p), target, &fakeObjects{answers: []bool{false}})
	h.now = func() time.Time { return takenAt }
	h.arrival = quickArrival

	err := h.Run(context.Background(), volumeLeased(), testsupport.DiscardLogger())

	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want a retryable error, got %v", err)
	}
}

// A volume run that died midway left its copy and its snapshot: the next run removes both before snapshotting again.
func TestVolume_ARunAfterOneThatDiedMidway_RemovesItsLeftoversFirst(t *testing.T) {
	p := testsupport.NewProvider()
	p.Volumes[project+"/"+volumeName] = &api.StorageVolume{Name: volumeName, Type: "custom", Project: project}
	p.Volumes[project+"/bkv"+encodedId] = &api.StorageVolume{Name: "bkv" + encodedId, Type: "custom", Project: project}
	p.VolumeSnapshots[project+"/"+volumeName+"/backup"] = true

	err := newVolume(p, target).Run(context.Background(), volumeLeased(), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	wantStart := "delete-volume " + project + "/bkv" + encodedId + "\ndelete-volume-snapshot " + project + "/" + volumeName +
		"/backup\nsnapshot-volume "
	if !strings.HasPrefix(strings.Join(p.BackupOrder, "\n"), wantStart) {
		t.Fatalf("order: %v", p.BackupOrder)
	}
}

func TestVolume_GiveUp_RemovesLeftovers(t *testing.T) {
	p := testsupport.NewProvider()
	p.Volumes[project+"/bkv"+encodedId] = &api.StorageVolume{Name: "bkv" + encodedId, Type: "custom", Project: project}
	p.VolumeSnapshots[project+"/"+volumeName+"/backup"] = true

	err := newVolume(p, target).GiveUp(context.Background(), volumeLeased(), errors.New("cause"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	want := "delete-volume " + project + "/bkv" + encodedId + "\ndelete-volume-snapshot " + project + "/" + volumeName + "/backup"
	if strings.Join(p.BackupOrder, "\n") != want {
		t.Fatalf("order: %v", p.BackupOrder)
	}
}
