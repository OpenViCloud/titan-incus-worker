package volsnapshot

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/resource/volume"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

// A command that carries a requestId ends that request on its own route
// (titan-ops architecture/resource-lifecycle.md#callback-routes); a restore's request is the new volume's create.
// volsnapshot_test.go keeps the commands without one, on the old routes.

const requestId = "77777777-6666-5555-4444-333333333333"

const snapshotRequestRoute = "PATCH /internal/volumes/" + volumeId + "/snapshots/" + snapshotId + "/requests/" + requestId

func withRequest(l command.Leased) command.Leased {
	var payload map[string]any
	_ = json.Unmarshal(l.Payload, &payload)
	payload["requestId"] = requestId
	return leased(l.Kind, payload)
}

// requestEnd reads the one report on route, and checks nothing went to an old /status route.
func requestEnd(t *testing.T, cp *testsupport.ControlPlane, route string) map[string]any {
	t.Helper()
	for _, request := range cp.RequestsTo("PATCH") {
		if len(request.Path) > 7 && request.Path[len(request.Path)-7:] == "/status" {
			t.Fatalf("a command with a requestId used an old route: %+v", cp.Requests())
		}
	}
	return onlyBody(t, cp, route)
}

func TestCreate_WithARequest_EndsItDone_WithVolumeSlashSnapshot(t *testing.T) {
	cp := testsupport.NewControlPlane(t)

	err := NewCreate(cp.Client(), clusters(testsupport.NewProvider())).Run(context.Background(), withRequest(createLeased()), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body := requestEnd(t, cp, snapshotRequestRoute)
	if body["outcome"] != "Done" || body["externalId"] != volumeName+"/s"+encodedId {
		t.Fatalf("report: %v", body)
	}
}

func TestCreate_WithARequest_GiveUp_EndsItFailed_CarryingTheIdOnlyWhenTheSnapshotExists(t *testing.T) {
	for _, exists := range []bool{true, false} {
		cp := testsupport.NewControlPlane(t)
		p := testsupport.NewProvider()
		if exists {
			p.VolumeSnapshots[project+"/"+volumeName+"/s"+encodedId] = true
		}

		err := NewCreate(cp.Client(), clusters(p)).GiveUp(context.Background(), withRequest(createLeased()), errors.New("raw"), testsupport.DiscardLogger())

		if err != nil {
			t.Fatal(err)
		}
		body := requestEnd(t, cp, snapshotRequestRoute)
		wantId := ""
		if exists {
			wantId = volumeName + "/s" + encodedId
		}
		gotId, _ := body["externalId"].(string)
		if body["outcome"] != "Failed" || body["reason"] != reasonCreateFailed || body["detail"] != "raw" || gotId != wantId {
			t.Errorf("snapshot there %v: report %v", exists, body)
		}
	}
}

func TestDelete_WithARequest_EndsItDone_AndGiveUpEndsItFailed(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	if err := NewDelete(cp.Client(), clusters(p)).Run(context.Background(), withRequest(deleteLeased(volumeName+"/sone")), testsupport.DiscardLogger()); err != nil {
		t.Fatal(err)
	}
	if body := requestEnd(t, cp, snapshotRequestRoute); body["outcome"] != "Done" {
		t.Fatalf("report: %v", body)
	}

	cp = testsupport.NewControlPlane(t)
	err := NewDelete(cp.Client(), clusters(p)).GiveUp(context.Background(), withRequest(deleteLeased(volumeName+"/sone")), errors.New("raw"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if body := requestEnd(t, cp, snapshotRequestRoute); body["outcome"] != "Failed" || body["reason"] != reasonDeleteFailed {
		t.Fatalf("report: %v", body)
	}
}

// The restore makes a new volume: its request is that volume's create, ended on the new volume's route.
func TestRestore_WithARequest_EndsTheNewVolumesCreateDone_AndGiveUpEndsItFailed(t *testing.T) {
	newVolumeRoute := "PATCH /internal/volumes/" + newVolumeId + "/requests/" + requestId
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	if err := NewRestore(cp.Client(), clusters(p)).Run(context.Background(), withRequest(restoreLeased()), testsupport.DiscardLogger()); err != nil {
		t.Fatal(err)
	}
	body := requestEnd(t, cp, newVolumeRoute)
	if _, observed := body["observed"]; body["outcome"] != "Done" || body["externalId"] != "vol"+encodedId || !observed {
		t.Fatalf("report: %v", body)
	}

	cp = testsupport.NewControlPlane(t)
	err := NewRestore(cp.Client(), clusters(testsupport.NewProvider())).GiveUp(context.Background(), withRequest(restoreLeased()), errors.New("raw"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body = requestEnd(t, cp, newVolumeRoute)
	if _, carriesId := body["externalId"]; body["outcome"] != "Failed" || body["reason"] != volume.ReasonCreateFailed || carriesId {
		t.Fatalf("report: %v", body)
	}
}
