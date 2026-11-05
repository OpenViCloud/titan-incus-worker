package volsnapshot

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/resource/volume"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

// A command ends the request it carries on that request's route
// (titan-ops architecture/resource-lifecycle.md#callback-routes); a restore's request is the new volume's create.

const requestId = "77777777-6666-5555-4444-333333333333"

const snapshotRequestRoute = "PATCH /internal/volumes/" + volumeId + "/snapshots/" + snapshotId + "/requests/" + requestId

func TestCreate_WithARequest_EndsItDone_WithVolumeSlashSnapshot(t *testing.T) {
	cp := testsupport.NewControlPlane(t)

	err := NewCreate(cp.Client(), clusters(testsupport.NewProvider())).Run(context.Background(), createLeased(), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body := onlyBody(t, cp, snapshotRequestRoute)
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

		err := NewCreate(cp.Client(), clusters(p)).GiveUp(context.Background(), createLeased(), errors.New("raw"), testsupport.DiscardLogger())

		if err != nil {
			t.Fatal(err)
		}
		body := onlyBody(t, cp, snapshotRequestRoute)
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

	if err := NewDelete(cp.Client(), clusters(p)).Run(context.Background(), deleteLeased(volumeName+"/sone"), testsupport.DiscardLogger()); err != nil {
		t.Fatal(err)
	}
	if body := onlyBody(t, cp, snapshotRequestRoute); body["outcome"] != "Done" {
		t.Fatalf("report: %v", body)
	}

	cp = testsupport.NewControlPlane(t)
	err := NewDelete(cp.Client(), clusters(p)).GiveUp(context.Background(), deleteLeased(volumeName+"/sone"), errors.New("raw"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if body := onlyBody(t, cp, snapshotRequestRoute); body["outcome"] != "Failed" || body["reason"] != reasonDeleteFailed {
		t.Fatalf("report: %v", body)
	}
}

// The restore makes a new volume: its request is that volume's create, ended on the new volume's route.
func TestRestore_WithARequest_EndsTheNewVolumesCreateDone_AndGiveUpEndsItFailed(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	if err := NewRestore(cp.Client(), clusters(p)).Run(context.Background(), restoreLeased(), testsupport.DiscardLogger()); err != nil {
		t.Fatal(err)
	}
	body := onlyBody(t, cp, newVolumeRoute)
	if _, observed := body["observed"]; body["outcome"] != "Done" || body["externalId"] != "vol"+encodedId || !observed {
		t.Fatalf("report: %v", body)
	}

	cp = testsupport.NewControlPlane(t)
	err := NewRestore(cp.Client(), clusters(testsupport.NewProvider())).GiveUp(context.Background(), restoreLeased(), errors.New("raw"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body = onlyBody(t, cp, newVolumeRoute)
	if _, carriesId := body["externalId"]; body["outcome"] != "Failed" || body["reason"] != volume.ReasonCreateFailed || carriesId {
		t.Fatalf("report: %v", body)
	}
}

// The control plane puts a requestId in every command (titan-ops architecture/resource-lifecycle.md#callback-routes):
// one without it is malformed. Run fails it for good before Incus or the control plane hear of it; GiveUp has no
// route to end anything on.
func TestCommands_WithoutARequest_FailForGood_AndCallNothing(t *testing.T) {
	build := map[string]func(cp *testsupport.ControlPlane, p *testsupport.Provider) command.Handler{
		"volume-snapshot.create": func(cp *testsupport.ControlPlane, p *testsupport.Provider) command.Handler {
			return NewCreate(cp.Client(), clusters(p))
		},
		"volume-snapshot.delete": func(cp *testsupport.ControlPlane, p *testsupport.Provider) command.Handler {
			return NewDelete(cp.Client(), clusters(p))
		},
		"volume-snapshot.restore": func(cp *testsupport.ControlPlane, p *testsupport.Provider) command.Handler {
			return NewRestore(cp.Client(), clusters(p))
		},
	}
	commands := []command.Leased{createLeased(), deleteLeased(volumeName + "/sone"), restoreLeased()}
	for _, full := range commands {
		for _, absent := range []bool{true, false} {
			var payload map[string]any
			_ = json.Unmarshal(full.Payload, &payload)
			payload["requestId"] = ""
			if absent {
				delete(payload, "requestId")
			}
			withoutRequest := leased(full.Kind, payload)
			cp := testsupport.NewControlPlane(t)
			p := testsupport.NewProvider()
			p.Volumes[project+"/vol"+encodedId] = &api.StorageVolume{Name: "vol" + encodedId}
			p.VolumeSnapshots[project+"/"+volumeName+"/s"+encodedId] = true
			handler := build[full.Kind](cp, p)

			err := handler.Run(context.Background(), withoutRequest, testsupport.DiscardLogger())
			giveUpErr := handler.GiveUp(context.Background(), withoutRequest, errors.New("x"), testsupport.DiscardLogger())

			var permanent interface{ Permanent() bool }
			isForGood := errors.As(err, &permanent) && permanent.Permanent()
			if !isForGood || giveUpErr != nil || len(cp.Requests()) != 0 || p.Calls != 0 {
				t.Errorf("%s, key absent %v: run %v, give-up %v, requests %+v, Incus calls %d",
					full.Kind, absent, err, giveUpErr, cp.Requests(), p.Calls)
			}
		}
	}
}
