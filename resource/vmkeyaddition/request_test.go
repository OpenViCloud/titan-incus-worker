package vmkeyaddition

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

// agentNotRunningError is the text IsAgentNotRunning matches (titan-ops features/vm/design.md#vm-key-addition),
// as GetInstanceFile wraps it on the dev lab.
func agentNotRunningError() error {
	return errors.New("get file /etc/ssh/openvicloud/ubuntu of instance " + instance + " in project " + project +
		": VM agent isn't currently running")
}

// A command ends the request it carries on that request's own route, under the VM (resource-lifecycle.md#callback-routes):
// Done with restartNeeded, or Failed with a fixed reason and the raw error, cut, as detail.

const requestRoute = "PATCH /internal/vms/" + vmId + "/key-additions/" + keyAdditionId + "/requests/" + requestId

// requestEnd reads the one report, which must be on the request route.
func requestEnd(t *testing.T, cp *testsupport.ControlPlane) map[string]any {
	t.Helper()
	requests := cp.RequestsTo(requestRoute)
	if len(requests) != 1 {
		t.Fatalf("want one %s, got %d: %+v", requestRoute, len(requests), cp.Requests())
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(requests[0].Body), &body); err != nil {
		t.Fatal(err)
	}
	return body
}

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

func TestGiveUp_EndsTheRequestFailed_WithAFixedReason_AndTheRawErrorAsDetail(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	err := NewCreate(cp.Client(), clusters(p)).GiveUp(context.Background(), createLeased(), errors.New("raw: agent timeout"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body := requestEnd(t, cp)
	_, hasRestart := body["restartNeeded"]
	if body["outcome"] != "Failed" || body["reason"] != reasonCreateFailed || body["detail"] != "raw: agent timeout" || hasRestart {
		t.Errorf("report %v", body)
	}
}

// A give-up whose last error is the guest agent not up yet reads a reason naming that, not the generic one
// (titan-ops features/vm/design.md#vm-key-addition): the raw error still goes to detail.
func TestGiveUp_AgentNotRunning_EndsTheRequestFailed_WithTheAgentReason(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	cause := agentNotRunningError()

	err := NewCreate(cp.Client(), clusters(p)).GiveUp(context.Background(), createLeased(), cause, testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body := requestEnd(t, cp)
	if body["outcome"] != "Failed" || body["reason"] != reasonAgentNotRunning || body["detail"] != cause.Error() {
		t.Errorf("report %v", body)
	}
}

func TestGiveUp_CutsTheDetailAt1000Runes_NotBytes(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	long := strings.Repeat("é", controlplane.MaxDetail+10)

	err := NewCreate(cp.Client(), clusters(p)).GiveUp(context.Background(), createLeased(), errors.New(long), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body := requestEnd(t, cp)
	detail, _ := body["detail"].(string)
	if body["outcome"] != "Failed" || len([]rune(detail)) != controlplane.MaxDetail {
		t.Errorf("detail of %d runes, want %d", len([]rune(detail)), controlplane.MaxDetail)
	}
}

// A command without a requestId has no request to end: Run fails for good before touching Incus, and a give-up has
// nothing to say (titan-ops architecture/resource-lifecycle.md#callback-routes).
func TestCommands_WithoutARequest_FailForGood_AndCallNothing(t *testing.T) {
	for _, keepTheKey := range []bool{false, true} {
		cp := testsupport.NewControlPlane(t)
		p := testsupport.NewProvider()
		logger, logs := testsupport.NewLogCapture(t)
		h := NewCreate(cp.Client(), clusters(p))
		l := withoutRequest(createLeased(), keepTheKey)

		runErr := h.Run(context.Background(), l, logger)
		giveUpErr := h.GiveUp(context.Background(), l, errors.New("raw"), logger)

		if runErr == nil || command.IsRetryable(runErr) {
			t.Errorf("empty key %v: Run wants a permanent error, got %v", keepTheKey, runErr)
		}
		if giveUpErr != nil {
			t.Errorf("empty key %v: give-up: %v", keepTheKey, giveUpErr)
		}
		if len(cp.Requests()) != 0 || p.Calls != 0 {
			t.Errorf("empty key %v: reports %+v, Incus calls %d", keepTheKey, cp.Requests(), p.Calls)
		}
		if got := logs.Records("no request id; nothing to end"); len(got) != 1 {
			t.Errorf("empty key %v: want one give-up line, got %v", keepTheKey, got)
		}
	}
}

// A payload missing any other required field never reaches Incus either.
func TestRun_IncompletePayload_FailsForGood_AndCallsNothing(t *testing.T) {
	cases := []struct {
		name string
		drop string
	}{
		{"no keyAdditionId", "keyAdditionId"},
		{"no vmId", "vmId"},
		{"no externalVmId", "externalVmId"},
		{"no projectName", "projectName"},
		{"no keyLines", "keyLines"},
	}
	for _, c := range cases {
		cp := testsupport.NewControlPlane(t)
		p := testsupport.NewProvider()
		var payload map[string]any
		_ = json.Unmarshal(createLeased().Payload, &payload)
		delete(payload, c.drop)
		l := leased("vm-key-addition.create", payload)

		err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), l, testsupport.DiscardLogger())

		if err == nil || command.IsRetryable(err) {
			t.Errorf("%s: want a permanent error, got %v", c.name, err)
		}
		if p.Calls != 0 {
			t.Errorf("%s: Incus calls %d", c.name, p.Calls)
		}
	}
}
