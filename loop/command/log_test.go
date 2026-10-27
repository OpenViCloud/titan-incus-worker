package command

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

// decidingHandler logs one decision with the logger the loop hands it.
type decidingHandler struct {
	runErr error
}

func (h *decidingHandler) Run(ctx context.Context, leased Leased, logger *slog.Logger) error {
	logger.Info("handler decision")
	return h.runErr
}

func (h *decidingHandler) GiveUp(ctx context.Context, leased Leased, cause error, logger *slog.Logger) error {
	return nil
}

func newLoggingLoop(cp *testsupport.ControlPlane, handler Handler, logger *slog.Logger) *Loop {
	loop := NewLoop(cp.Client(), map[string]Route{"vm.create": {Handler: handler, Timeout: time.Minute}}, time.Second, logger)
	loop.newLeaseId = func() string { return leaseId }
	return loop
}

// The fields that find the command's row in core.provider_commands and the run in the pod log.
func assertCommandFields(t *testing.T, record map[string]any) {
	t.Helper()
	want := map[string]any{
		"commandId": "c1", "leaseId": leaseId, "kind": "vm.create", "key": "vm-provision-x", "clusterId": "mock",
		"attempt": float64(1), "maxAttempts": float64(5),
	}
	for field, value := range want {
		if record[field] != value {
			t.Errorf("%q: field %s = %v, want %v", record["msg"], field, record[field], value)
		}
	}
}

func assertHasDuration(t *testing.T, record map[string]any) {
	t.Helper()
	_, isNumber := record["durationMs"].(float64)
	if !isNumber {
		t.Errorf("%q: durationMs = %v, want a number", record["msg"], record["durationMs"])
	}
}

func TestPollOnce_Done_LogsStartAndEnd_WithTheCommandFields_AndHandsTheLoggerToTheHandler(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, leasePath, http.StatusOK, leasedJSON(1, 5))
	logger, logs := testsupport.NewLogCapture(t)

	if _, err := newLoggingLoop(cp, &decidingHandler{}, logger).PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	started := logs.One("provider command started")
	assertCommandFields(t, started)
	done := logs.One("provider command done")
	assertCommandFields(t, done)
	assertHasDuration(t, done)
	assertCommandFields(t, logs.One("handler decision"))
}

func TestPollOnce_RetryableFailure_LogsTheEnd_WithTheCommandFields(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, leasePath, http.StatusOK, leasedJSON(1, 5))
	logger, logs := testsupport.NewLogCapture(t)

	if _, err := newLoggingLoop(cp, &decidingHandler{runErr: errors.New("operation failed")}, logger).PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	failed := logs.One("provider command failed; it will be retried")
	assertCommandFields(t, failed)
	assertHasDuration(t, failed)
}

func TestPollOnce_GivingUp_LogsTheEnd_WithTheCommandFields(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, leasePath, http.StatusOK, leasedJSON(1, 5))
	logger, logs := testsupport.NewLogCapture(t)

	if _, err := newLoggingLoop(cp, &decidingHandler{runErr: Permanent(errors.New("bad payload"))}, logger).PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	gaveUp := logs.One("provider command gave up")
	assertCommandFields(t, gaveUp)
	assertHasDuration(t, gaveUp)
}
