package command

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

// holdingHandler says which command started, then holds the run until the loop's context ends.
type holdingHandler struct {
	started chan string
}

func (h *holdingHandler) Run(ctx context.Context, leased Leased, logger *slog.Logger) error {
	h.started <- leased.Id
	<-ctx.Done()
	return ctx.Err()
}

func (h *holdingHandler) GiveUp(ctx context.Context, leased Leased, cause error, logger *slog.Logger) error {
	return nil
}

func leasedJSONWithId(id string) string {
	encoded, _ := json.Marshal(map[string]any{
		"id": id, "key": "vm-provision-" + id, "kind": "vm.create", "clusterId": "mock",
		"payload": map[string]any{"vmId": id}, "attempt": 1, "maxAttempts": 5,
	})
	return string(encoded)
}

// A command that runs long holds one slot, and the other slots go on taking commands: a slow command of one tenant does
// not delay another tenant's (titan-ops features/provider/design.md#tenant-lane).
func TestPool_RunsACommandInEverySlotAtOnce(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	leaseIds := []string{"11111111-0000-0000-0000-000000000001", "11111111-0000-0000-0000-000000000002",
		"11111111-0000-0000-0000-000000000003"}
	for i, id := range leaseIds {
		cp.Respond(http.MethodGet, "/internal/provider-commands/leases/"+id, http.StatusOK, leasedJSONWithId(string(rune('a'+i))))
	}
	handler := &holdingHandler{started: make(chan string, 16)}
	routes := map[string]Route{"vm.create": {Handler: handler, Timeout: time.Minute}}
	pool := NewPool(cp.Client(), routes, time.Second, len(leaseIds), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if len(pool.loops) != len(leaseIds) {
		t.Fatalf("want %d slots, got %d", len(leaseIds), len(pool.loops))
	}
	for i, loop := range pool.loops {
		id := leaseIds[i]
		loop.newLeaseId = func() string { return id }
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		pool.Run(ctx)
		close(stopped)
	}()
	defer func() {
		cancel()
		<-stopped
	}()

	running := map[string]bool{}
	for len(running) < len(leaseIds) {
		select {
		case id := <-handler.started:
			running[id] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("want %d commands running at once, got %v", len(leaseIds), running)
		}
	}
}
