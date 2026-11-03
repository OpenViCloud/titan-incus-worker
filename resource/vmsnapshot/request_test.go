package vmsnapshot

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

// A command that carries a requestId ends that request on its own route
// (titan-ops architecture/resource-lifecycle.md#callback-routes): Done after its one write, or a delete's give-up that
// finds the snapshot gone; Failed with a fixed reason and the raw error, cut, as detail. The tests named
// WithoutARequest here, and vmsnapshot_test.go, keep the commands without one on the old route.

const requestId = "77777777-6666-5555-4444-333333333333"

const (
	requestRoute     = "PATCH /internal/vms/" + vmId + "/snapshots/" + snapshotId + "/requests/" + requestId
	observationRoute = "PATCH /internal/vms/" + vmId + "/snapshots/" + snapshotId + "/observation"
	snapshotKey      = project + "/" + instance + "/" + snapshotName
	providerId       = instance + "/" + snapshotName
)

func withRequest(l command.Leased) command.Leased {
	var payload map[string]any
	_ = json.Unmarshal(l.Payload, &payload)
	payload["requestId"] = requestId
	return leased(l.Kind, payload)
}

func runCommand(h command.Handler, l command.Leased) error {
	return h.Run(context.Background(), l, testsupport.DiscardLogger())
}

func giveUpCommand(h command.Handler, l command.Leased, cause error) error {
	return h.GiveUp(context.Background(), l, cause, testsupport.DiscardLogger())
}

// requestEnd reads the one report on the request route, and checks nothing went to the old one.
func requestEnd(t *testing.T, cp *testsupport.ControlPlane) map[string]any {
	t.Helper()
	if len(cp.RequestsTo(statusPath)) != 0 {
		t.Fatalf("a command with a requestId used the old route: %+v", cp.Requests())
	}
	return onlyBody(t, cp, requestRoute)
}

func TestRun_WithARequest_EndsItDone_OnTheRequestRoute(t *testing.T) {
	cases := []struct {
		name           string
		handler        func(cp *testsupport.ControlPlane, p *testsupport.Provider) command.Handler
		leased         command.Leased
		snapshotThere  bool
		wantExternalId string
	}{
		{"create", func(cp *testsupport.ControlPlane, p *testsupport.Provider) command.Handler {
			return NewCreate(cp.Client(), clusters(p))
		}, createLeased(), false, providerId},
		{"revert", func(cp *testsupport.ControlPlane, p *testsupport.Provider) command.Handler {
			return NewRevert(cp.Client(), clusters(p))
		}, snapshotLeased("vm-snapshot.revert"), true, ""},
		{"delete", func(cp *testsupport.ControlPlane, p *testsupport.Provider) command.Handler {
			return NewDelete(cp.Client(), clusters(p))
		}, snapshotLeased("vm-snapshot.delete"), true, ""},
		// 404: an earlier run deleted it; the delete is done all the same.
		{"delete, already gone", func(cp *testsupport.ControlPlane, p *testsupport.Provider) command.Handler {
			return NewDelete(cp.Client(), clusters(p))
		}, snapshotLeased("vm-snapshot.delete"), false, ""},
	}
	for _, c := range cases {
		cp := testsupport.NewControlPlane(t)
		p := testsupport.NewProvider()
		if c.snapshotThere {
			p.InstanceSnapshots[snapshotKey] = true
		}

		err := runCommand(c.handler(cp, p), withRequest(c.leased))

		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		body := requestEnd(t, cp)
		gotId, _ := body["externalId"].(string)
		_, hasReason := body["reason"]
		wantKeys := 1
		if c.wantExternalId != "" {
			wantKeys = 2
		}
		if body["outcome"] != "Done" || gotId != c.wantExternalId || hasReason || len(body) != wantKeys {
			t.Errorf("%s: report %v", c.name, body)
		}
	}
}

func TestCreate_WithARequest_PutsTheSnapshotOnIncus(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	err := runCommand(NewCreate(cp.Client(), clusters(p)), withRequest(createLeased()))

	if err != nil || !p.InstanceSnapshots[snapshotKey] {
		t.Fatalf("err %v, snapshots %v", err, p.InstanceSnapshots)
	}
}

func TestRevert_WithARequest_RestoresTheDiskOnly(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	err := runCommand(NewRevert(cp.Client(), clusters(p)), withRequest(snapshotLeased("vm-snapshot.revert")))

	if err != nil || len(p.UpdatedInstances) != 1 || p.UpdatedInstances[0].Restore != snapshotName || !p.UpdatedInstances[0].DiskOnly {
		t.Fatalf("err %v, updates %+v", err, p.UpdatedInstances)
	}
}

func TestRevert_WithARequest_VmGone_IsPermanent_AndReportsNothing(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.UpdateInstanceErr = testsupport.NotFound("Instance not found")

	err := runCommand(NewRevert(cp.Client(), clusters(p)), withRequest(snapshotLeased("vm-snapshot.revert")))

	if err == nil || command.IsRetryable(err) {
		t.Fatalf("want a permanent error, got %v", err)
	}
	if len(cp.Requests()) != 0 {
		t.Fatalf("reported %+v", cp.Requests())
	}
}

// A give-up that carries the id tells the control plane the create did happen, so a delete of it reaches Incus.
func TestCreate_WithARequest_GiveUp_EndsItFailed_CarryingTheIdOnlyWhenTheSnapshotExists(t *testing.T) {
	for _, exists := range []bool{true, false} {
		cp := testsupport.NewControlPlane(t)
		p := testsupport.NewProvider()
		if exists {
			p.InstanceSnapshots[snapshotKey] = true
		}

		err := giveUpCommand(NewCreate(cp.Client(), clusters(p)), withRequest(createLeased()), errors.New("raw"))

		if err != nil {
			t.Fatal(err)
		}
		body := requestEnd(t, cp)
		wantId := ""
		if exists {
			wantId = providerId
		}
		gotId, _ := body["externalId"].(string)
		if body["outcome"] != "Failed" || body["reason"] != reasonCreateFailed || body["detail"] != "raw" || gotId != wantId {
			t.Errorf("snapshot there %v: report %v", exists, body)
		}
	}
}

// A read that fails is not "no snapshot": the next lease gives up again.
func TestCreate_WithARequest_GiveUp_SnapshotReadFails_IsRetryable_AndReportsNothing(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.InstanceSnapshots[snapshotKey] = true
	p.GetSnapshotErr = errors.New("incus unreachable")

	err := giveUpCommand(NewCreate(cp.Client(), clusters(p)), withRequest(createLeased()), errors.New("raw"))

	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want retryable, got %v", err)
	}
	if len(cp.Requests()) != 0 {
		t.Fatalf("reported %+v", cp.Requests())
	}
}

// No read can prove a restore done, so a revert's give-up is Failed whatever Incus shows, and it reads nothing.
func TestRevert_WithARequest_GiveUp_IsAlwaysFailed_WithoutReadingIncus(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.InstanceSnapshots[snapshotKey] = true
	p.GetSnapshotErr = errors.New("incus unreachable")

	err := giveUpCommand(NewRevert(cp.Client(), clusters(p)), withRequest(snapshotLeased("vm-snapshot.revert")), errors.New("raw"))

	if err != nil {
		t.Fatal(err)
	}
	body := requestEnd(t, cp)
	_, hasId := body["externalId"]
	if body["outcome"] != "Failed" || body["reason"] != reasonRevertFailed || body["detail"] != "raw" || hasId {
		t.Errorf("report %v", body)
	}
	if p.Calls != 0 {
		t.Errorf("the give-up called Incus %d time(s)", p.Calls)
	}
}

// A delete whose snapshot Incus no longer has gives nothing back to the tenant: its give-up is the request's Done.
func TestDelete_WithARequest_GiveUp_IsDone_WhenIncusNoLongerHasTheSnapshot_ElseFailed(t *testing.T) {
	for _, exists := range []bool{true, false} {
		cp := testsupport.NewControlPlane(t)
		logger, logs := testsupport.NewLogCapture(t)
		p := testsupport.NewProvider()
		if exists {
			p.InstanceSnapshots[snapshotKey] = true
		}

		err := NewDelete(cp.Client(), clusters(p)).GiveUp(context.Background(), withRequest(snapshotLeased("vm-snapshot.delete")),
			errors.New("in use"), logger)

		if err != nil {
			t.Fatal(err)
		}
		body := requestEnd(t, cp)
		isFailed := body["outcome"] == "Failed" && body["reason"] == reasonDeleteFailed && body["detail"] == "in use"
		_, hasReason := body["reason"]
		isDone := body["outcome"] == "Done" && !hasReason && len(body) == 1
		if (exists && !isFailed) || (!exists && !isDone) {
			t.Errorf("snapshot there %v: report %v", exists, body)
		}
		wantGoneLines := 0
		if !exists {
			wantGoneLines = 1
		}
		if got := logs.Objects("already gone; the give-up ends the request done"); len(got) != wantGoneLines {
			t.Errorf("snapshot there %v: want %d gone line(s), got %v", exists, wantGoneLines, got)
		}
	}
}

// A cluster the worker is not configured for says nothing about the snapshot: not gone, so the delete gives it back.
func TestDelete_WithARequest_GiveUp_OnAClusterItCannotRead_IsFailed(t *testing.T) {
	cp := testsupport.NewControlPlane(t)

	err := giveUpCommand(NewDelete(cp.Client(), cluster.Fixed{}), withRequest(snapshotLeased("vm-snapshot.delete")), errors.New("in use"))

	if err != nil {
		t.Fatal(err)
	}
	if body := requestEnd(t, cp); body["outcome"] != "Failed" || body["reason"] != reasonDeleteFailed {
		t.Errorf("report %v", body)
	}
}

func TestDelete_WithARequest_GiveUp_SnapshotReadFails_IsRetryable_AndReportsNothing(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.GetSnapshotErr = errors.New("incus unreachable")

	err := giveUpCommand(NewDelete(cp.Client(), clusters(p)), withRequest(snapshotLeased("vm-snapshot.delete")), errors.New("in use"))

	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want retryable, got %v", err)
	}
	if len(cp.Requests()) != 0 {
		t.Fatalf("reported %+v", cp.Requests())
	}
}

func TestGiveUps_WithARequest_CutTheDetailAt1000Runes_NotBytes(t *testing.T) {
	cases := []struct {
		name    string
		handler func(cp *testsupport.ControlPlane, p *testsupport.Provider) command.Handler
		leased  command.Leased
	}{
		{"create", func(cp *testsupport.ControlPlane, p *testsupport.Provider) command.Handler {
			return NewCreate(cp.Client(), clusters(p))
		}, createLeased()},
		{"revert", func(cp *testsupport.ControlPlane, p *testsupport.Provider) command.Handler {
			return NewRevert(cp.Client(), clusters(p))
		}, snapshotLeased("vm-snapshot.revert")},
		{"delete", func(cp *testsupport.ControlPlane, p *testsupport.Provider) command.Handler {
			return NewDelete(cp.Client(), clusters(p))
		}, snapshotLeased("vm-snapshot.delete")},
	}
	long := strings.Repeat("é", controlplane.MaxDetail+10)
	for _, c := range cases {
		cp := testsupport.NewControlPlane(t)
		p := testsupport.NewProvider()
		p.InstanceSnapshots[snapshotKey] = true

		err := giveUpCommand(c.handler(cp, p), withRequest(c.leased), errors.New(long))

		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		body := requestEnd(t, cp)
		detail, _ := body["detail"].(string)
		if body["outcome"] != "Failed" || len([]rune(detail)) != controlplane.MaxDetail {
			t.Errorf("%s: outcome %v, detail of %d runes, want %d", c.name, body["outcome"], len([]rune(detail)), controlplane.MaxDetail)
		}
	}
}

// A command the old control plane enqueued without a requestId keeps the old route until rollout phase 6
// (titan-ops architecture/resource-lifecycle.md#rollout).
func TestCommands_WithoutARequest_ReportOnTheOldRoute(t *testing.T) {
	cases := []struct {
		name       string
		run        func(cp *testsupport.ControlPlane, p *testsupport.Provider) error
		wantReport string
	}{
		{"create", func(cp *testsupport.ControlPlane, p *testsupport.Provider) error {
			return runCommand(NewCreate(cp.Client(), clusters(p)), createLeased())
		}, "Created"},
		{"create give-up", func(cp *testsupport.ControlPlane, p *testsupport.Provider) error {
			return giveUpCommand(NewCreate(cp.Client(), clusters(p)), createLeased(), errors.New("x"))
		}, "RequestFailed"},
		{"revert", func(cp *testsupport.ControlPlane, p *testsupport.Provider) error {
			return runCommand(NewRevert(cp.Client(), clusters(p)), snapshotLeased("vm-snapshot.revert"))
		}, "Reverted"},
		{"revert give-up", func(cp *testsupport.ControlPlane, p *testsupport.Provider) error {
			return giveUpCommand(NewRevert(cp.Client(), clusters(p)), snapshotLeased("vm-snapshot.revert"), errors.New("x"))
		}, "RequestFailed"},
		{"delete", func(cp *testsupport.ControlPlane, p *testsupport.Provider) error {
			return runCommand(NewDelete(cp.Client(), clusters(p)), snapshotLeased("vm-snapshot.delete"))
		}, "Deleted"},
		// The old route keeps the old give-up: no read, RequestFailed even with the snapshot gone.
		{"delete give-up", func(cp *testsupport.ControlPlane, p *testsupport.Provider) error {
			return giveUpCommand(NewDelete(cp.Client(), clusters(p)), snapshotLeased("vm-snapshot.delete"), errors.New("x"))
		}, "RequestFailed"},
	}
	for _, c := range cases {
		cp := testsupport.NewControlPlane(t)

		if err := c.run(cp, testsupport.NewProvider()); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		requestRoutes := cp.RequestsTo("PATCH /internal/vms/" + vmId + "/snapshots/" + snapshotId + "/requests/")
		if len(requestRoutes) != 0 {
			t.Errorf("%s: reports %+v", c.name, cp.Requests())
			continue
		}
		if got := onlyBody(t, cp, statusPath)["report"]; got != c.wantReport {
			t.Errorf("%s: report %v, want %s", c.name, got, c.wantReport)
		}
	}
}

// The old route keeps the old create give-up too: a snapshot that cannot be read goes without its id.
func TestCreate_WithoutARequest_GiveUp_SnapshotReadFails_StillReportsRequestFailed_WithoutTheId(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.InstanceSnapshots[snapshotKey] = true
	p.GetSnapshotErr = errors.New("incus unreachable")

	err := giveUpCommand(NewCreate(cp.Client(), clusters(p)), createLeased(), errors.New("raw"))

	body := onlyBody(t, cp, statusPath)
	if _, hasId := body["externalId"]; err != nil || body["report"] != "RequestFailed" || hasId {
		t.Fatalf("err %v, report %v", err, body)
	}
}

func TestReconcile_SendsOnTheObservationRoute_NeverTheOldOne(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond("GET", "/internal/reconcile/vm-snapshots?clusterId=mock", 200,
		`[{"id":"`+snapshotId+`","vmId":"`+vmId+`","externalId":"vma/sone","currentExistence":"NotCreated"}]`)
	p := testsupport.NewProvider()
	p.VirtualMachines = []api.InstanceFull{{Instance: api.Instance{Name: "vma"}, Snapshots: []api.InstanceSnapshot{{Name: "sone"}}}}

	err := Reconcile(context.Background(), cp.Client(), p, clusterId, testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if len(cp.RequestsTo(statusPath)) != 0 {
		t.Fatalf("the reconciler used the old route: %+v", cp.Requests())
	}
	body := onlyBody(t, cp, observationRoute)
	if body["exists"] != true || len(body) != 1 {
		t.Fatalf("observation: %v", body)
	}
}
