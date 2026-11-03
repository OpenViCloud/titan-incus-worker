package volume

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

// A command that carries a requestId ends that request on its own route
// (titan-ops architecture/resource-lifecycle.md#callback-routes): Done with what the worker read of the volume, Failed
// with a fixed reason and the raw error as detail. volume_test.go keeps the commands without one, on the old route.

const requestId = "77777777-6666-5555-4444-333333333333"

const requestRoute = "PATCH /internal/volumes/" + volumeId + "/requests/" + requestId

func withRequest(l command.Leased) command.Leased {
	var payload map[string]any
	_ = json.Unmarshal(l.Payload, &payload)
	payload["requestId"] = requestId
	return leased(l.Kind, payload)
}

// requestEnd reads the one report on the request route, and checks nothing went to the old one.
func requestEnd(t *testing.T, cp *testsupport.ControlPlane) map[string]any {
	t.Helper()
	if len(cp.RequestsTo(statusPath)) != 0 {
		t.Fatalf("a command with a requestId used the old route: %+v", cp.Requests())
	}
	return onlyBody(t, cp, requestRoute)
}

// observedNames reads the instances of a report's observation; ok is false when the report carries none.
func observedNames(body map[string]any) ([]string, bool) {
	observed, ok := body["observed"].(map[string]any)
	if !ok {
		return nil, false
	}
	return attachedTo(observed), true
}

func TestCreate_WithARequest_EndsItDone_WithTheNameAndWhatItRead(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	err := NewCreate(cp.Client(), clusters(p, "")).Run(context.Background(), withRequest(createLeased()), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body := requestEnd(t, cp)
	names, observed := observedNames(body)
	if body["outcome"] != "Done" || body["externalId"] != volumeName || !observed || len(names) != 0 {
		t.Fatalf("report: %v", body)
	}
}

func TestCreate_WithARequest_GiveUp_EndsItFailed_CarryingTheNameOnlyWhenTheVolumeExists(t *testing.T) {
	for _, exists := range []bool{true, false} {
		cp := testsupport.NewControlPlane(t)
		p := testsupport.NewProvider()
		if exists {
			p.Volumes[project+"/"+volumeName] = usedVolume()
		}

		err := NewCreate(cp.Client(), clusters(p, "")).GiveUp(context.Background(), withRequest(createLeased()), errors.New("pool full"), testsupport.DiscardLogger())

		if err != nil {
			t.Fatal(err)
		}
		body := requestEnd(t, cp)
		wantId := ""
		if exists {
			wantId = volumeName
		}
		gotId, _ := body["externalId"].(string)
		isTheGiveUp := body["outcome"] == "Failed" && body["reason"] == ReasonCreateFailed && body["detail"] == "pool full" && gotId == wantId
		if !isTheGiveUp {
			t.Errorf("volume there %v: report %v", exists, body)
		}
	}
}

func TestAttach_WithARequest_EndsItDone_WithTheInstancesUsingIt(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Volumes[project+"/"+volumeName] = usedVolume(usedByInstance)

	err := NewAttach(cp.Client(), clusters(p, "")).Run(context.Background(), withRequest(attachLeased("volume.attach")), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body := requestEnd(t, cp)
	names, _ := observedNames(body)
	if body["outcome"] != "Done" || len(names) != 1 || names[0] != instance {
		t.Fatalf("report: %v", body)
	}
}

// The attach did its work; a read that fails afterwards does not undo it: Done without an observation, and the
// reconciler reports the volume next cycle.
func TestAttach_WithARequest_UnreadableAfterward_EndsItDone_WithoutAnObservation(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.GetVolumeErr = errors.New("connection refused")

	err := NewAttach(cp.Client(), clusters(p, "")).Run(context.Background(), withRequest(attachLeased("volume.attach")), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body := requestEnd(t, cp)
	if _, observed := observedNames(body); body["outcome"] != "Done" || observed {
		t.Fatalf("report: %v", body)
	}
}

// An attach or detach whose work happened while its report failed five times: Incus shows what was asked, so the give-up
// is the request's Done. One that did not happen is Failed, with what Incus shows; a volume gone is Failed alone.
func TestGiveUps_WithARequest_AreDone_WhenIncusShowsWhatWasAsked_ElseFailed(t *testing.T) {
	cases := []struct {
		kind        string
		volumeThere bool
		usedBy      []string
		wantOutcome string
		wantNames   int
		wantReason  string
	}{
		{"volume.attach", true, []string{usedByInstance}, "Done", 1, ""},
		{"volume.attach", true, nil, "Failed", 0, reasonAttachFailed},
		{"volume.detach", true, nil, "Done", 0, ""},
		{"volume.detach", true, []string{usedByInstance}, "Failed", 1, reasonDetachFailed},
		{"volume.attach", false, nil, "Failed", -1, reasonAttachFailed},
	}
	for _, c := range cases {
		cp := testsupport.NewControlPlane(t)
		p := testsupport.NewProvider()
		if c.volumeThere {
			p.Volumes[project+"/"+volumeName] = usedVolume(c.usedBy...)
		}
		var handler command.Handler = NewAttach(cp.Client(), clusters(p, ""))
		if c.kind == "volume.detach" {
			handler = NewDetach(cp.Client(), clusters(p, ""))
		}

		err := handler.GiveUp(context.Background(), withRequest(attachLeased(c.kind)), errors.New("callback refused"), testsupport.DiscardLogger())

		if err != nil {
			t.Fatal(err)
		}
		body := requestEnd(t, cp)
		names, observed := observedNames(body)
		gotReason, _ := body["reason"].(string)
		isAsWanted := body["outcome"] == c.wantOutcome && gotReason == c.wantReason &&
			observed == (c.wantNames >= 0) && (c.wantNames < 0 || len(names) == c.wantNames)
		if !isAsWanted {
			t.Errorf("%s, volume there %v, used_by %v: report %v", c.kind, c.volumeThere, c.usedBy, body)
		}
	}
}

func TestDelete_WithARequest_EndsItDone_AndGiveUpEndsItFailed(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	handler := NewDelete(cp.Client(), clusters(p, ""))

	if err := handler.Run(context.Background(), withRequest(deleteLeased()), testsupport.DiscardLogger()); err != nil {
		t.Fatal(err)
	}
	if body := requestEnd(t, cp); body["outcome"] != "Done" {
		t.Fatalf("report: %v", body)
	}

	cp = testsupport.NewControlPlane(t)
	err := NewDelete(cp.Client(), clusters(p, "")).GiveUp(context.Background(), withRequest(deleteLeased()), errors.New("busy"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if body := requestEnd(t, cp); body["outcome"] != "Failed" || body["reason"] != reasonDeleteFailed || body["detail"] != "busy" {
		t.Fatalf("report: %v", body)
	}
}

func TestGiveUp_WithARequest_CutsTheDetailAt1000Runes_NotBytes(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	long := strings.Repeat("é", controlplane.MaxDetail+10)

	err := NewDelete(cp.Client(), clusters(testsupport.NewProvider(), "")).GiveUp(context.Background(), withRequest(deleteLeased()), errors.New(long), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	detail, _ := requestEnd(t, cp)["detail"].(string)
	if len([]rune(detail)) != controlplane.MaxDetail {
		t.Fatalf("detail is %d runes, want %d", len([]rune(detail)), controlplane.MaxDetail)
	}
}

// The control plane records only that something outside Titan uses a volume, not what: the reconciler sends such a
// volume once, then again only when the instances using it change (titan-ops architecture/resource-lifecycle.md#reconciler).
func TestReconcile_AVolumeUsedOutsideTitan_IsSentOnce_UntilItsInstancesChange(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond("GET", "/internal/reconcile/volumes?clusterId=mock", 200,
		`{"volumes":[{"id":"v1","externalId":"vola","currentExistence":"Exists","currentAttachedExternalVmId":null,"currentAttachedElsewhere":true}],"snapshots":[]}`)
	p := testsupport.NewProvider()
	p.AllVolumes = []api.StorageVolume{{Name: "vola", Type: "custom", Project: project, UsedBy: []string{usedByInstance}}}
	reconciler := NewReconciler()
	cycle := func() {
		if err := reconciler.Reconcile(context.Background(), cp.Client(), p, clusterId, "", slog.Default()); err != nil {
			t.Fatal(err)
		}
	}

	cycle()
	cycle()
	if sent := len(cp.RequestsTo("PATCH /internal/volumes/v1/observation")); sent != 1 {
		t.Fatalf("same instances twice: %d reports, want 1", sent)
	}
	p.AllVolumes[0].UsedBy = []string{usedByInstance, "/1.0/instances/other?project=" + project}
	cycle()

	if sent := len(cp.RequestsTo("PATCH /internal/volumes/v1/observation")); sent != 2 {
		t.Fatalf("after the instances changed: %d reports, want 2", sent)
	}
}
