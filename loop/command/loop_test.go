package command

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

const (
	leaseId   = "11111111-2222-3333-4444-555555555555"
	leasePath = "/internal/provider-commands/leases/" + leaseId
	giveUpURL = "/internal/give-up"
)

// fakeHandler sends its give-up callback to the control plane, so the request order shows give-up before failure.
type fakeHandler struct {
	cp        *controlplane.Client
	runErr    error
	runs      int
	giveUps   int
	waitOnCtx bool
}

func (h *fakeHandler) Run(ctx context.Context, leased Leased, logger *slog.Logger) error {
	h.runs++
	if h.waitOnCtx {
		<-ctx.Done()
		return ctx.Err()
	}
	return h.runErr
}

func (h *fakeHandler) GiveUp(ctx context.Context, leased Leased, cause error, logger *slog.Logger) error {
	h.giveUps++
	return h.cp.Patch(ctx, giveUpURL, map[string]string{"cause": cause.Error()})
}

func newTestLoop(cp *testsupport.ControlPlane, handler *fakeHandler) *Loop {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	loop := NewLoop(cp.Client(), map[string]Route{"vm.create": {Handler: handler, Timeout: time.Minute}}, time.Second, logger)
	loop.newLeaseId = func() string { return leaseId }
	return loop
}

func leasedJSON(attempt int, maxAttempts int) string {
	encoded, _ := json.Marshal(map[string]any{
		"id": "c1", "key": "vm-provision-x", "kind": "vm.create", "clusterId": "mock",
		"payload": map[string]any{"vmId": "x"}, "attempt": attempt, "maxAttempts": maxAttempts,
		"leaseUntil": "2026-10-08T00:00:00Z",
	})
	return string(encoded)
}

// sequence lists "METHOD path" of every request after the lease PUT and GET.
func sequence(cp *testsupport.ControlPlane) []string {
	steps := []string{}
	for _, request := range cp.Requests()[2:] {
		steps = append(steps, request.Method+" "+request.Path)
	}
	return steps
}

func failureBody(t *testing.T, cp *testsupport.ControlPlane) map[string]any {
	t.Helper()
	failures := cp.RequestsTo("PUT " + leasePath + "/failure")
	if len(failures) != 1 {
		t.Fatalf("want one failure report, got %d", len(failures))
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(failures[0].Body), &body); err != nil {
		t.Fatal(err)
	}
	return body
}

// A GET that fails in transit may hide a command the control plane already leased to this id: the next poll asks
// again under the same id, and only an answered poll moves on to a fresh one.
func TestPollOnce_UnansweredLease_IsAskedAgainUnderTheSameId(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, leasePath, http.StatusBadGateway, `bad gateway`)
	cp.Respond(http.MethodGet, leasePath, http.StatusNotFound, `{"code":"PROVIDER_COMMAND_LEASE_NOT_FOUND"}`)
	handler := &fakeHandler{cp: cp.Client()}
	loop := newTestLoop(cp, handler)
	minted := 0
	loop.newLeaseId = func() string {
		minted++
		return leaseId
	}

	if _, err := loop.PollOnce(context.Background()); err == nil {
		t.Fatal("want the failed GET reported")
	}
	if _, err := loop.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if minted != 1 {
		t.Fatalf("minted %d lease ids, want 1 (the unanswered one is reused)", minted)
	}
	if loop.unansweredLeaseId != "" {
		t.Fatal("an answered poll must let the next one mint a fresh id")
	}
}

func TestPollOnce_NoWork_LeasesTheKindsAndStops(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, leasePath, http.StatusNotFound, `{"code":"PROVIDER_COMMAND_LEASE_NOT_FOUND"}`)
	handler := &fakeHandler{cp: cp.Client()}

	ran, err := newTestLoop(cp, handler).PollOnce(context.Background())

	if err != nil || ran {
		t.Fatalf("want no work and no error, got ran=%v err=%v", ran, err)
	}
	requests := cp.Requests()
	if len(requests) != 2 || requests[0].Method != http.MethodPut || requests[1].Method != http.MethodGet {
		t.Fatalf("want PUT then GET of the lease, got %+v", requests)
	}
	if requests[0].Body != `{"kinds":["vm.create"]}` || requests[0].Token != testsupport.Token {
		t.Fatalf("lease body/token: %+v", requests[0])
	}
	if handler.runs != 0 {
		t.Fatal("ran a command with no work")
	}
}

func TestPollOnce_Success_ReportsCompletion(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, leasePath, http.StatusOK, leasedJSON(1, 5))
	handler := &fakeHandler{cp: cp.Client()}

	ran, err := newTestLoop(cp, handler).PollOnce(context.Background())

	if err != nil || !ran {
		t.Fatalf("ran=%v err=%v", ran, err)
	}
	want := []string{"PUT " + leasePath + "/completion"}
	if strings.Join(sequence(cp), ",") != strings.Join(want, ",") {
		t.Fatalf("want %v, got %v", want, sequence(cp))
	}
}

func TestPollOnce_RetryableFailureBeforeTheLastAttempt_FailsRetryable_WithoutGivingUp(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, leasePath, http.StatusOK, leasedJSON(2, 5))
	handler := &fakeHandler{cp: cp.Client(), runErr: errors.New("incus said no")}

	newTestLoop(cp, handler).PollOnce(context.Background())

	if handler.giveUps != 0 {
		t.Fatal("gave up before the last attempt")
	}
	body := failureBody(t, cp)
	if body["retryable"] != true || body["error"] != "incus said no" {
		t.Fatalf("failure body: %v", body)
	}
}

func TestPollOnce_LastAttempt_SendsTheGiveUpBeforeTheFailure(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, leasePath, http.StatusOK, leasedJSON(5, 5))
	handler := &fakeHandler{cp: cp.Client(), runErr: errors.New("incus said no")}

	newTestLoop(cp, handler).PollOnce(context.Background())

	want := []string{"PATCH " + giveUpURL, "PUT " + leasePath + "/failure"}
	if strings.Join(sequence(cp), ",") != strings.Join(want, ",") {
		t.Fatalf("want %v, got %v", want, sequence(cp))
	}
	if failureBody(t, cp)["retryable"] != false {
		t.Fatal("the last failure must not be retryable")
	}
}

func TestPollOnce_PermanentError_GivesUpAtTheFirstAttempt(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, leasePath, http.StatusOK, leasedJSON(1, 5))
	handler := &fakeHandler{cp: cp.Client(), runErr: Permanent(errors.New("payload names a template that does not exist"))}

	newTestLoop(cp, handler).PollOnce(context.Background())

	want := []string{"PATCH " + giveUpURL, "PUT " + leasePath + "/failure"}
	if strings.Join(sequence(cp), ",") != strings.Join(want, ",") {
		t.Fatalf("want %v, got %v", want, sequence(cp))
	}
	if failureBody(t, cp)["retryable"] != false {
		t.Fatal("a permanent error must not be retryable")
	}
}

func TestPollOnce_PastTheLastAttempt_DoesNotRun_OnlyGivesUpAndFails(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, leasePath, http.StatusOK, leasedJSON(6, 5))
	handler := &fakeHandler{cp: cp.Client()}

	newTestLoop(cp, handler).PollOnce(context.Background())

	if handler.runs != 0 {
		t.Fatal("redid the provider work past the last attempt")
	}
	want := []string{"PATCH " + giveUpURL, "PUT " + leasePath + "/failure"}
	if strings.Join(sequence(cp), ",") != strings.Join(want, ",") {
		t.Fatalf("want %v, got %v", want, sequence(cp))
	}
	if failureBody(t, cp)["retryable"] != false {
		t.Fatal("want retryable false")
	}
}

func TestPollOnce_GiveUpCallbackFailsTransiently_LeavesTheLeaseToRunOut(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, leasePath, http.StatusOK, leasedJSON(5, 5))
	cp.Respond(http.MethodPatch, giveUpURL, http.StatusServiceUnavailable, "")
	handler := &fakeHandler{cp: cp.Client(), runErr: errors.New("incus said no")}

	newTestLoop(cp, handler).PollOnce(context.Background())

	if len(cp.RequestsTo("PUT "+leasePath+"/failure")) != 0 {
		t.Fatal("failed the command although its give-up never reached the control plane")
	}
}

func TestPollOnce_GiveUpCallbackRefused_StillFailsTheCommand(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, leasePath, http.StatusOK, leasedJSON(5, 5))
	cp.Respond(http.MethodPatch, giveUpURL, http.StatusNotFound, `{"code":"VM_NOT_FOUND"}`)
	handler := &fakeHandler{cp: cp.Client(), runErr: errors.New("incus said no")}

	newTestLoop(cp, handler).PollOnce(context.Background())

	if failureBody(t, cp)["retryable"] != false {
		t.Fatal("want retryable false")
	}
}

func TestPollOnce_LeaseLostWhileRunning_CancelsTheWork_AndReportsNothing(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, leasePath, http.StatusOK, leasedJSON(1, 5))
	cp.Respond(http.MethodPatch, leasePath, http.StatusConflict, `{"code":"PROVIDER_COMMAND_LEASE_LOST"}`)
	handler := &fakeHandler{cp: cp.Client(), waitOnCtx: true}
	loop := newTestLoop(cp, handler)
	loop.SetExtendEvery(10 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		loop.PollOnce(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the work was not cancelled when the lease was lost")
	}

	want := []string{"PATCH " + leasePath}
	if strings.Join(sequence(cp), ",") != strings.Join(want, ",") {
		t.Fatalf("want only the extend, got %v", sequence(cp))
	}
}

func TestPollOnce_LongRun_ExtendsTheLease(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, leasePath, http.StatusOK, leasedJSON(1, 5))
	handler := &fakeHandler{cp: cp.Client()}
	loop := newTestLoop(cp, handler)
	loop.SetExtendEvery(5 * time.Millisecond)
	loop.routes["vm.create"] = Route{Handler: slowHandler{fakeHandler: handler, delay: 50 * time.Millisecond}, Timeout: time.Minute}

	loop.PollOnce(context.Background())

	if len(cp.RequestsTo("PATCH "+leasePath)) == 0 {
		t.Fatal("never extended the lease")
	}
	if len(cp.RequestsTo("PUT "+leasePath+"/completion")) != 1 {
		t.Fatal("want completion")
	}
}

type slowHandler struct {
	*fakeHandler
	delay time.Duration
}

func (h slowHandler) Run(ctx context.Context, leased Leased, logger *slog.Logger) error {
	time.Sleep(h.delay)
	return nil
}

func TestTruncate_KeepsWholeCharacters(t *testing.T) {
	text := strings.Repeat("a", 4095) + "é"

	cut := truncate(text, 4096)

	if cut != strings.Repeat("a", 4095) {
		t.Fatalf("want the 2-byte character dropped whole, got length %d", len(cut))
	}
}
