package vmsnapshot

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

const (
	snapshotId = "11111111-2222-3333-4444-555555555555"
	// Oracle: python3 base32 of snapshotId, as in provider/names_test.go.
	snapshotName = "sceircejceiztgrcekvkvkvkvku"
	vmId         = "99999999-8888-7777-6666-555555555555"
	instance     = "vminstance"
	project      = "t-0123456789abcdef0123456789abcdef"
	clusterId    = "mock"
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

func createLeased() command.Leased {
	return leased("vm-snapshot.create", map[string]any{
		"snapshotId": snapshotId, "requestId": requestId, "vmId": vmId, "externalVmId": instance, "name": "before-upgrade", "projectName": project,
	})
}

func snapshotLeased(kind string) command.Leased {
	return leased(kind, map[string]any{
		"snapshotId": snapshotId, "requestId": requestId, "vmId": vmId, "externalSnapshotId": instance + "/" + snapshotName, "projectName": project,
	})
}

func TestCreate_SnapshotsTheInstanceUnderTitansName_AndEndsTheRequestDoneWithInstanceSlashSnapshot(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), createLeased(), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if !p.InstanceSnapshots[project+"/"+instance+"/"+snapshotName] || len(p.InstanceSnapshots) != 1 {
		t.Fatalf("snapshots: %v", p.InstanceSnapshots)
	}
	body := requestEnd(t, cp)
	if body["outcome"] != "Done" || body["externalId"] != instance+"/"+snapshotName {
		t.Fatalf("report: %v", body)
	}
}

// A duplicate instance snapshot fails in its operation (lab-storage 05), not with a 409: still ours, still created.
func TestCreate_SnapshotAlreadyThere_EndsTheRequestDone(t *testing.T) {
	logger, logs := testsupport.NewLogCapture(t)
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.CreateSnapshotErr = &provider.OperationFailedError{
		Description: "Snapshotting instance",
		Status:      "Failure",
		Message:     `Failed creating instance snapshot record "` + snapshotName + `": Add snapshot info to the database: This "Instance_snapshot" entry already exists`,
	}

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), createLeased(), logger)

	if err != nil {
		t.Fatalf("a duplicate must not fail the create: %v", err)
	}
	if body := requestEnd(t, cp); body["outcome"] != "Done" || body["externalId"] != providerId {
		t.Fatalf("report: %v", body)
	}
	if got := logs.Objects("already exists; treating it as ours"); len(got) != 1 {
		t.Fatalf("want 1 line(s) %q, got %v", "already exists; treating it as ours", got)
	}
}

func TestCreate_OtherOperationFailure_FailsTheRun(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.CreateSnapshotErr = &provider.OperationFailedError{Description: "Snapshotting instance", Status: "Failure", Message: "injected snapshot failure"}

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), createLeased(), testsupport.DiscardLogger())

	if err == nil || len(cp.RequestsTo("PATCH")) != 0 {
		t.Fatalf("err %v, reports %+v", err, cp.RequestsTo("PATCH"))
	}
}

// The tenant reads the fixed reason; the raw error goes to the operator's detail
// (titan-ops architecture/desired-and-current.md#request-failure).
func TestGiveUps_EndTheRequestFailedWithAFixedReason_AndTheRawErrorAsDetail(t *testing.T) {
	cases := []struct {
		handler func(cp *testsupport.ControlPlane) command.Handler
		leased  command.Leased
		reason  string
	}{
		{func(cp *testsupport.ControlPlane) command.Handler {
			return NewCreate(cp.Client(), clusters(testsupport.NewProvider()))
		},
			createLeased(), reasonCreateFailed},
		{func(cp *testsupport.ControlPlane) command.Handler {
			return NewRevert(cp.Client(), clusters(testsupport.NewProvider()))
		},
			snapshotLeased("vm-snapshot.revert"), reasonRevertFailed},
		{func(cp *testsupport.ControlPlane) command.Handler {
			// Incus still has the snapshot: one gone would end the delete Done.
			p := testsupport.NewProvider()
			p.InstanceSnapshots[snapshotKey] = true
			return NewDelete(cp.Client(), clusters(p))
		},
			snapshotLeased("vm-snapshot.delete"), reasonDeleteFailed},
	}
	for _, c := range cases {
		cp := testsupport.NewControlPlane(t)

		err := c.handler(cp).GiveUp(context.Background(), c.leased, errors.New("raw: lvcreate exit status 5"), testsupport.DiscardLogger())

		body := requestEnd(t, cp)
		isTheGiveUp := body["outcome"] == "Failed" && body["reason"] == c.reason &&
			body["detail"] == "raw: lvcreate exit status 5"
		if err != nil || !isTheGiveUp {
			t.Errorf("%s: err %v, report %v", c.leased.Kind, err, body)
		}
	}
}

// Disk only: a full restore also puts back the snapshot's devices, attaching or detaching volumes behind Titan's back.
func TestRevert_RestoresTheSnapshotsDiskOnly_AndEndsTheRequestDone(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	err := NewRevert(cp.Client(), clusters(p)).Run(context.Background(), snapshotLeased("vm-snapshot.revert"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if len(p.UpdatedInstances) != 1 || p.UpdatedInstances[0].Restore != snapshotName || !p.UpdatedInstances[0].DiskOnly {
		t.Fatalf("updates: %+v", p.UpdatedInstances)
	}
	if body := requestEnd(t, cp); body["outcome"] != "Done" || len(body) != 1 {
		t.Fatalf("report: %v", body)
	}
}

func TestDelete_SnapshotThereOrGone_EndsTheRequestDone(t *testing.T) {
	for _, exists := range []bool{true, false} {
		cp := testsupport.NewControlPlane(t)
		logger, logs := testsupport.NewLogCapture(t)
		p := testsupport.NewProvider()
		if exists {
			p.InstanceSnapshots[project+"/"+instance+"/"+snapshotName] = true
		}

		err := NewDelete(cp.Client(), clusters(p)).Run(context.Background(), snapshotLeased("vm-snapshot.delete"), logger)

		if err != nil || len(p.InstanceSnapshots) != 0 {
			t.Fatalf("exists=%v: err %v, left %v", exists, err, p.InstanceSnapshots)
		}
		if body := requestEnd(t, cp); body["outcome"] != "Done" || len(body) != 1 {
			t.Fatalf("exists=%v: report %v", exists, body)
		}
		wantGoneLines := 0
		if !exists {
			wantGoneLines = 1
		}
		if got := logs.Objects("already gone; nothing to delete"); len(got) != wantGoneLines {
			t.Fatalf("%v: want %d line(s) %q, got %v", exists, wantGoneLines, "already gone; nothing to delete", got)
		}
	}
}

func TestDelete_VmGone_EndsTheRequestDone(t *testing.T) {
	logger, logs := testsupport.NewLogCapture(t)
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.DeleteSnapshotErr = testsupport.NotFound(`Failed to fetch instance "` + instance + `" in project "` + project + `": Instance not found`)

	err := NewDelete(cp.Client(), clusters(p)).Run(context.Background(), snapshotLeased("vm-snapshot.delete"), logger)

	if body := requestEnd(t, cp); err != nil || body["outcome"] != "Done" {
		t.Fatalf("err %v, report %v", err, body)
	}
	if got := logs.Objects("already gone; nothing to delete"); len(got) != 1 {
		t.Fatalf("want 1 line(s) %q, got %v", "already gone; nothing to delete", got)
	}
}

func TestDecideReport(t *testing.T) {
	byInstance := SnapshotsByInstance([]api.InstanceFull{
		{Instance: api.Instance{Name: "vma"}, Snapshots: []api.InstanceSnapshot{{Name: "sone"}}},
		{Instance: api.Instance{Name: "vmb"}},
	})
	cases := []struct {
		name      string
		row       ReconcilableVmSnapshot
		wantSend  bool
		wantExist bool
	}{
		{"seen first", ReconcilableVmSnapshot{ExternalId: "vma/sone", CurrentExistence: "NotCreated"}, true, true},
		{"found again", ReconcilableVmSnapshot{ExternalId: "vma/sone", CurrentExistence: "NotFound"}, true, true},
		{"unchanged", ReconcilableVmSnapshot{ExternalId: "vma/sone", CurrentExistence: "Exists"}, false, true},
		{"gone from its VM", ReconcilableVmSnapshot{ExternalId: "vma/stwo", CurrentExistence: "Exists"}, true, false},
		{"gone, already recorded", ReconcilableVmSnapshot{ExternalId: "vmb/sone", CurrentExistence: "NotFound"}, false, false},
		// A VM gone says nothing: its delete settles its snapshots.
		{"its VM gone", ReconcilableVmSnapshot{ExternalId: "vmgone/sone", CurrentExistence: "Exists"}, false, false},
		{"not a snapshot id", ReconcilableVmSnapshot{ExternalId: "sone", CurrentExistence: "Exists"}, false, false},
	}
	for _, c := range cases {
		report, send := DecideReport(c.row, byInstance)
		if send != c.wantSend {
			t.Errorf("%s: send %v, want %v", c.name, send, c.wantSend)
			continue
		}
		if send && report.Exists != c.wantExist {
			t.Errorf("%s: report %+v", c.name, report)
		}
	}
}

// The reconciler never reports Deleted, nor ends a revert: a snapshot gone is NotFound
// (titan-ops architecture/desired-and-current.md#existence-axis).
func TestReconcile_ReportsOnTheSnapshotsVm_AGoneSnapshotAsNotFound(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond("GET", "/internal/reconcile/vm-snapshots?clusterId=mock", 200,
		`[{"id":"`+snapshotId+`","vmId":"`+vmId+`","externalId":"vma/sone","currentExistence":"Exists"}]`)
	p := testsupport.NewProvider()
	p.VirtualMachines = []api.InstanceFull{{Instance: api.Instance{Name: "vma"}}}
	logger, logs := testsupport.NewLogCapture(t)

	err := Reconcile(context.Background(), cp.Client(), p, clusterId, logger)

	body := onlyBody(t, cp, observationRoute)
	if err != nil || body["exists"] != false {
		t.Fatalf("err %v, report %v", err, body)
	}
	sent := logs.One("reconcile report sent")
	isLogged := sent["snapshotId"] == snapshotId && sent["vmId"] == vmId && sent["recordedExistence"] == "Exists" &&
		sent["exists"] == false
	if !isLogged {
		t.Fatalf("report line: %v", sent)
	}
}
