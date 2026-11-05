package vmsnapshot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

// A command ends the request it carries on that request's own route
// (titan-ops architecture/resource-lifecycle.md#callback-routes): Done after its one write, or a delete's give-up that
// finds the snapshot gone; Failed with a fixed reason and the raw error, cut, as detail.

const requestId = "77777777-6666-5555-4444-333333333333"

const (
	requestRoute     = "PATCH /internal/vms/" + vmId + "/snapshots/" + snapshotId + "/requests/" + requestId
	observationRoute = "PATCH /internal/vms/" + vmId + "/snapshots/" + snapshotId + "/observation"
	snapshotKey      = project + "/" + instance + "/" + snapshotName
	providerId       = instance + "/" + snapshotName
)

// withoutRequest drops the payload's requestId, or leaves it empty when keepTheKey.
func withoutRequest(l command.Leased, keepTheKey bool) command.Leased {
	var payload map[string]any
	_ = json.Unmarshal(l.Payload, &payload)
	delete(payload, "requestId")
	if keepTheKey {
		payload["requestId"] = ""
	}
	return leased(l.Kind, payload)
}

func runCommand(h command.Handler, l command.Leased) error {
	return h.Run(context.Background(), l, testsupport.DiscardLogger())
}

func giveUpCommand(h command.Handler, l command.Leased, cause error) error {
	return h.GiveUp(context.Background(), l, cause, testsupport.DiscardLogger())
}

// requestEnd reads the one report, which must be on the request route.
func requestEnd(t *testing.T, cp *testsupport.ControlPlane) map[string]any {
	t.Helper()
	if len(cp.Requests()) != 1 {
		t.Fatalf("want one report, got %+v", cp.Requests())
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

		err := runCommand(c.handler(cp, p), c.leased)

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

func TestRevert_WithARequest_VmGone_IsPermanent_AndReportsNothing(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.UpdateInstanceErr = testsupport.NotFound("Instance not found")

	err := runCommand(NewRevert(cp.Client(), clusters(p)), snapshotLeased("vm-snapshot.revert"))

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

		err := giveUpCommand(NewCreate(cp.Client(), clusters(p)), createLeased(), errors.New("raw"))

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

	err := giveUpCommand(NewCreate(cp.Client(), clusters(p)), createLeased(), errors.New("raw"))

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

	err := giveUpCommand(NewRevert(cp.Client(), clusters(p)), snapshotLeased("vm-snapshot.revert"), errors.New("raw"))

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

		err := NewDelete(cp.Client(), clusters(p)).GiveUp(context.Background(), snapshotLeased("vm-snapshot.delete"),
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
func TestDelete_WithARequest_GiveUp_OnAClusterTheWorkerIsNotConfiguredFor_IsFailed(t *testing.T) {
	cp := testsupport.NewControlPlane(t)

	err := giveUpCommand(NewDelete(cp.Client(), cluster.Fixed{}), snapshotLeased("vm-snapshot.delete"), errors.New("in use"))

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

	err := giveUpCommand(NewDelete(cp.Client(), clusters(p)), snapshotLeased("vm-snapshot.delete"), errors.New("in use"))

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

		err := giveUpCommand(c.handler(cp, p), c.leased, errors.New(long))

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

// A command without a requestId has no request to end: Run fails for good before touching Incus, and a give-up has
// nothing to say (titan-ops architecture/resource-lifecycle.md#callback-routes).
func TestCommands_WithoutARequest_FailForGood_AndCallNothing(t *testing.T) {
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
	for _, c := range cases {
		for _, keepTheKey := range []bool{false, true} {
			name := fmt.Sprintf("%s, empty key %v", c.name, keepTheKey)
			cp := testsupport.NewControlPlane(t)
			p := testsupport.NewProvider()
			p.InstanceSnapshots[snapshotKey] = true
			logger, logs := testsupport.NewLogCapture(t)
			h := c.handler(cp, p)
			l := withoutRequest(c.leased, keepTheKey)

			runErr := h.Run(context.Background(), l, logger)
			giveUpErr := h.GiveUp(context.Background(), l, errors.New("raw"), logger)

			if runErr == nil || command.IsRetryable(runErr) {
				t.Errorf("%s: Run wants a permanent error, got %v", name, runErr)
			}
			if giveUpErr != nil {
				t.Errorf("%s: give-up: %v", name, giveUpErr)
			}
			if len(cp.Requests()) != 0 || p.Calls != 0 {
				t.Errorf("%s: reports %+v, Incus calls %d", name, cp.Requests(), p.Calls)
			}
			if got := logs.Records("no request id; nothing to end"); len(got) != 1 {
				t.Errorf("%s: want one give-up line, got %v", name, got)
			}
		}
	}
}

func TestReconcile_SendsOnTheObservationRoute(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond("GET", "/internal/reconcile/vm-snapshots?clusterId=mock", 200,
		`[{"id":"`+snapshotId+`","vmId":"`+vmId+`","externalId":"vma/sone","currentExistence":"NotCreated"}]`)
	p := testsupport.NewProvider()
	p.VirtualMachines = []api.InstanceFull{{Instance: api.Instance{Name: "vma"}, Snapshots: []api.InstanceSnapshot{{Name: "sone"}}}}

	err := Reconcile(context.Background(), cp.Client(), p, clusterId, testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if len(cp.RequestsTo("PATCH")) != 1 {
		t.Fatalf("want one report, got %+v", cp.Requests())
	}
	body := onlyBody(t, cp, observationRoute)
	if body["exists"] != true || len(body) != 1 {
		t.Fatalf("observation: %v", body)
	}
}
