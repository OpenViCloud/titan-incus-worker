package volsnapshot

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/resource/volume"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

const (
	snapshotId  = "11111111-2222-3333-4444-555555555555"
	newVolumeId = "11111111-2222-3333-4444-555555555555"
	// Oracle: python3 base32 of the id, as in provider/names_test.go.
	encodedId  = "ceircejceiztgrcekvkvkvkvku"
	volumeId   = "99999999-8888-7777-6666-555555555555"
	volumeName = "volsource"
	project    = "t-0123456789abcdef0123456789abcdef"
	clusterId  = "mock"
)

func clusters(p *testsupport.Provider) cluster.Fixed {
	return cluster.Fixed{clusterId: {Provider: p}}
}

func leased(kind string, payload map[string]any) command.Leased {
	encoded, _ := json.Marshal(payload)
	return command.Leased{Kind: kind, ClusterId: clusterId, Payload: encoded, Attempt: 1, MaxAttempts: 5}
}

func onlyBody(t *testing.T, cp *testsupport.ControlPlane, prefix string) map[string]any {
	t.Helper()
	requests := cp.RequestsTo(prefix)
	if len(requests) != 1 {
		t.Fatalf("want one %s, got %d: %+v", prefix, len(requests), cp.Requests())
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(requests[0].Body), &body); err != nil {
		t.Fatal(err)
	}
	return body
}

const snapshotPath = "PATCH /internal/volumes/" + volumeId + "/snapshots/" + snapshotId + "/status"

func createLeased() command.Leased {
	return leased("volume-snapshot.create", map[string]any{
		"snapshotId": snapshotId, "volumeId": volumeId, "externalVolumeId": volumeName, "projectName": project,
	})
}

func TestCreate_SnapshotsTheVolume_AndReportsCreatedWithVolumeSlashSnapshot(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), createLeased(), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if len(p.CreatedVolumeSnapshots) != 1 || p.CreatedVolumeSnapshots[0] != project+"/"+volumeName+"/s"+encodedId {
		t.Fatalf("snapshots: %v", p.CreatedVolumeSnapshots)
	}
	body := onlyBody(t, cp, snapshotPath)
	if body["report"] != "Created" || body["externalId"] != volumeName+"/s"+encodedId {
		t.Fatalf("report: %v", body)
	}
}

func TestCreate_SnapshotAlreadyThere_ReportsCreated(t *testing.T) {
	logger, logs := testsupport.NewLogCapture(t)
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.CreateSnapshotErr = api.StatusErrorf(http.StatusConflict, `Snapshot "s%s" already in use`, encodedId)

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), createLeased(), logger)

	if err != nil {
		t.Fatalf("a duplicate must not fail the create: %v", err)
	}
	if onlyBody(t, cp, snapshotPath)["report"] != "Created" {
		t.Fatal("want Created")
	}
	if got := logs.Objects("already exists; treating it as ours"); len(got) != 1 {
		t.Fatalf("want 1 line(s) %q, got %v", "already exists; treating it as ours", got)
	}
}

func TestCreate_GiveUp_CarriesTheIdOnlyWhenTheSnapshotExists(t *testing.T) {
	for _, exists := range []bool{true, false} {
		cp := testsupport.NewControlPlane(t)
		p := testsupport.NewProvider()
		if exists {
			p.VolumeSnapshots[project+"/"+volumeName+"/s"+encodedId] = true
		}

		err := NewCreate(cp.Client(), clusters(p)).GiveUp(context.Background(), createLeased(), errors.New("raw"), testsupport.DiscardLogger())

		if err != nil {
			t.Fatal(err)
		}
		body := onlyBody(t, cp, snapshotPath)
		wantId := ""
		if exists {
			wantId = volumeName + "/s" + encodedId
		}
		gotId, _ := body["externalId"].(string)
		if body["report"] != "RequestFailed" || body["reason"] != reasonCreateFailed || body["detail"] != "raw" || gotId != wantId {
			t.Fatalf("exists=%v: report %v", exists, body)
		}
	}
}

func deleteLeased(externalSnapshotId string) command.Leased {
	return leased("volume-snapshot.delete", map[string]any{
		"snapshotId": snapshotId, "volumeId": volumeId, "externalSnapshotId": externalSnapshotId, "projectName": project,
	})
}

func TestDelete_DeletesTheNamedSnapshot_AndAGoneOne_ReportsDeleted(t *testing.T) {
	for _, gone := range []bool{false, true} {
		cp := testsupport.NewControlPlane(t)
		logger, logs := testsupport.NewLogCapture(t)
		p := testsupport.NewProvider()
		if gone {
			p.DeleteSnapshotErr = testsupport.NotFound("Storage pool volume not found")
		}

		err := NewDelete(cp.Client(), clusters(p)).Run(context.Background(), deleteLeased(volumeName+"/sone"), logger)

		if err != nil {
			t.Fatal(err)
		}
		if len(p.DeletedVolumeSnapshots) != 1 || p.DeletedVolumeSnapshots[0] != project+"/"+volumeName+"/sone" {
			t.Fatalf("deleted: %v", p.DeletedVolumeSnapshots)
		}
		if onlyBody(t, cp, snapshotPath)["report"] != "Deleted" {
			t.Fatalf("gone=%v: want Deleted", gone)
		}
		wantGoneLines := 0
		if gone {
			wantGoneLines = 1
		}
		if got := logs.Objects("already gone; nothing to delete"); len(got) != wantGoneLines {
			t.Fatalf("%v: want %d line(s) %q, got %v", gone, wantGoneLines, "already gone; nothing to delete", got)
		}
	}
}

func TestDelete_IdWithoutAVolume_IsPermanent(t *testing.T) {
	cp := testsupport.NewControlPlane(t)

	err := NewDelete(cp.Client(), clusters(testsupport.NewProvider())).Run(context.Background(), deleteLeased("sone"), testsupport.DiscardLogger())

	if err == nil || command.IsRetryable(err) {
		t.Fatalf("want a permanent error, got %v", err)
	}
}

func TestDelete_GiveUp_ReportsRequestFailed(t *testing.T) {
	cp := testsupport.NewControlPlane(t)

	err := NewDelete(cp.Client(), clusters(testsupport.NewProvider())).GiveUp(context.Background(), deleteLeased(volumeName+"/sone"), errors.New("raw"), testsupport.DiscardLogger())

	body := onlyBody(t, cp, snapshotPath)
	if err != nil || body["report"] != "RequestFailed" || body["reason"] != reasonDeleteFailed || body["detail"] != "raw" {
		t.Fatalf("err %v, report %v", err, body)
	}
}

func restoreLeased() command.Leased {
	return leased("volume-snapshot.restore", map[string]any{
		"snapshotId": "77777777-7777-7777-7777-777777777777", "newVolumeId": newVolumeId, "externalSnapshotId": volumeName + "/sone",
		"name": "restored-1", "projectName": project,
	})
}

const newVolumePath = "PATCH /internal/volumes/" + newVolumeId + "/status"

func TestRestore_CopiesTheSnapshotIntoTheNewVolume_KeepingTheDisplayName_AndReportsOnTheNewVolume(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	err := NewRestore(cp.Client(), clusters(p)).Run(context.Background(), restoreLeased(), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if len(p.CreatedVolumes) != 1 {
		t.Fatalf("volumes: %+v", p.CreatedVolumes)
	}
	copied := p.CreatedVolumes[0]
	isAsAsked := copied.Name == "vol"+encodedId && copied.Source.Type == "copy" && copied.Source.Name == volumeName+"/sone" &&
		copied.Source.Pool == "default" && copied.Config[volume.DisplayNameKey] == "restored-1"
	if !isAsAsked {
		t.Fatalf("copy: %+v", copied)
	}
	body := onlyBody(t, cp, newVolumePath)
	if body["report"] != "Created" || body["externalId"] != "vol"+encodedId {
		t.Fatalf("report: %v", body)
	}
	if len(cp.RequestsTo("PATCH /internal/volumes/"+volumeId)) != 0 {
		t.Fatal("reported on the source volume")
	}
}

func TestRestore_NewVolumeAlreadyThere_ReportsCreated(t *testing.T) {
	logger, logs := testsupport.NewLogCapture(t)
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.CreateVolumeErr = api.StatusErrorf(http.StatusConflict, "Volume by that name already exists")

	err := NewRestore(cp.Client(), clusters(p)).Run(context.Background(), restoreLeased(), logger)

	if err != nil || onlyBody(t, cp, newVolumePath)["report"] != "Created" {
		t.Fatalf("err %v", err)
	}
	if got := logs.Objects("already exists; treating it as ours"); len(got) != 1 {
		t.Fatalf("want 1 line(s) %q, got %v", "already exists; treating it as ours", got)
	}
}

func TestRestore_GiveUp_ReportsRequestFailedOnTheNewVolume(t *testing.T) {
	cp := testsupport.NewControlPlane(t)

	err := NewRestore(cp.Client(), clusters(testsupport.NewProvider())).GiveUp(context.Background(), restoreLeased(), errors.New("raw"), testsupport.DiscardLogger())

	body := onlyBody(t, cp, newVolumePath)
	if err != nil || body["report"] != "RequestFailed" || body["reason"] != volume.ReasonCreateFailed || body["detail"] != "raw" {
		t.Fatalf("err %v, report %v", err, body)
	}
}
