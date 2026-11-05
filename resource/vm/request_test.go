package vm

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

// A command ends the request it names on its own route (titan-ops architecture/resource-lifecycle.md#callback-routes):
// Done with what the worker read of an instance still there, Failed with a fixed reason and the raw error as detail.

func instanceWith(status string, code api.StatusCode) api.InstanceFull {
	return api.InstanceFull{
		Instance: api.Instance{Name: instanceName, Status: status, StatusCode: code},
		State: &api.InstanceState{Network: map[string]api.InstanceStateNetwork{
			"eth0": {Addresses: []api.InstanceStateNetworkAddress{{Family: "inet", Scope: "global", Address: "10.40.0.9"}}},
		}},
	}
}

func observedOf(t *testing.T, body map[string]any) map[string]any {
	t.Helper()
	observed, ok := body["observed"].(map[string]any)
	if !ok {
		t.Fatalf("report carries no observed: %v", body)
	}
	return observed
}

func TestCreate_WithARequest_EndsItDone_WithTheNameAndWhatItRead(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.VirtualMachines = []api.InstanceFull{instanceWith("Running", api.Running)}

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), createLeased(nil), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body := onlyBody(t, cp, requestRoute)
	if body["outcome"] != "Done" || body["externalId"] != instanceName {
		t.Fatalf("report: %v", body)
	}
	observed := observedOf(t, body)
	if observed["status"] != "Running" || observed["ipAddress"] != "10.40.0.9" {
		t.Fatalf("observed: %v", observed)
	}
}

// The instance is made; a read that fails must not repeat the command, and the reconciler reports the status next cycle.
func TestCreate_WithARequest_AReadThatFails_StillEndsItDone_WithNothingObserved(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.GetInstanceFullErr = errors.New("incus is busy")

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), createLeased(nil), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body := onlyBody(t, cp, requestRoute)
	if _, has := body["observed"]; has || body["outcome"] != "Done" || body["externalId"] != instanceName {
		t.Fatalf("report: %v", body)
	}
}

func TestCreate_WithARequest_InstanceAlreadyExistsButStopped_IsStarted_AndEndsItDone(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.CreateInstanceErr = api.StatusErrorf(http.StatusInternalServerError, `Instance "%s" already exists`, instanceName)
	p.Instances[project+"/"+instanceName] = &api.Instance{Name: instanceName, StatusCode: api.Stopped}
	p.VirtualMachines = []api.InstanceFull{instanceWith("Running", api.Running)}

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), createLeased(nil), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if len(p.StateChanges) != 1 || p.StateChanges[0].Action != "start" {
		t.Fatalf("state changes: %+v", p.StateChanges)
	}
	body := onlyBody(t, cp, requestRoute)
	if body["outcome"] != "Done" || body["externalId"] != instanceName || observedOf(t, body)["status"] != "Running" {
		t.Fatalf("report: %v", body)
	}
}

func TestCreate_WithARequest_GiveUp_EndsItFailed_WithTheIdOfAnInstanceThatExists(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Instances[project+"/"+instanceName] = &api.Instance{Name: instanceName}

	err := NewCreate(cp.Client(), clusters(p)).GiveUp(context.Background(), createLeased(nil), errors.New("raw error"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body := onlyBody(t, cp, requestRoute)
	if body["outcome"] != "Failed" || body["externalId"] != instanceName || body["reason"] != reasonProvisionFailed || body["detail"] != "raw error" {
		t.Fatalf("give-up: %v", body)
	}
}

func TestStop_WithARequest_EndsItDone_WithWhatItRead(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.VirtualMachines = []api.InstanceFull{instanceWith("Stopped", api.Stopped)}

	err := NewStop(cp.Client(), clusters(p)).Run(context.Background(), actionLeased("vm.stop"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body := onlyBody(t, cp, requestRoute)
	if body["outcome"] != "Done" || observedOf(t, body)["status"] != "Stopped" {
		t.Fatalf("report: %v", body)
	}
}

// "Already stopped" is what was asked: a lease run twice ends the request all the same.
func TestStop_WithARequest_AlreadyStopped_EndsItDone(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.UpdateStateErr = &provider.OperationFailedError{Status: "Failure", Message: "The instance is already stopped"}
	p.VirtualMachines = []api.InstanceFull{instanceWith("Stopped", api.Stopped)}

	err := NewStop(cp.Client(), clusters(p)).Run(context.Background(), actionLeased("vm.stop"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if onlyBody(t, cp, requestRoute)["outcome"] != "Done" {
		t.Fatalf("requests: %+v", cp.Requests())
	}
}

func TestStart_WithARequest_EndsItDone_WithWhatItRead(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Instances[project+"/"+instanceName] = &api.Instance{Name: instanceName, StatusCode: api.Stopped}
	p.VirtualMachines = []api.InstanceFull{instanceWith("Running", api.Running)}

	err := NewStart(cp.Client(), clusters(p)).Run(context.Background(), actionLeased("vm.start"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body := onlyBody(t, cp, requestRoute)
	if body["outcome"] != "Done" || observedOf(t, body)["status"] != "Running" {
		t.Fatalf("report: %v", body)
	}
}

// The command did not happen, so nothing ends: the lease runs again.
func TestStop_WithARequest_ACommandThatFails_ReportsNothing(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.UpdateStateErr = errors.New("incus is busy")

	err := NewStop(cp.Client(), clusters(p)).Run(context.Background(), actionLeased("vm.stop"), testsupport.DiscardLogger())

	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want retryable, got %v", err)
	}
	if len(cp.RequestsTo("PATCH ")) != 0 {
		t.Fatalf("reported a stop that failed: %+v", cp.Requests())
	}
}

// A Done that does not arrive fails the run, so the lease runs again and sends it again.
func TestStop_WithARequest_ADoneThatDoesNotArrive_IsRetryable(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodPatch, "/internal/vms/"+vmId+"/requests/"+requestId, http.StatusServiceUnavailable, "")
	p := testsupport.NewProvider()
	p.VirtualMachines = []api.InstanceFull{instanceWith("Stopped", api.Stopped)}

	err := NewStop(cp.Client(), clusters(p)).Run(context.Background(), actionLeased("vm.stop"), testsupport.DiscardLogger())

	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want retryable, got %v", err)
	}
}

func TestGiveUps_WithARequest_EndItFailed_WithTheFixedReasonOfTheirKind(t *testing.T) {
	cases := []struct {
		name   string
		giveUp func(cp *testsupport.ControlPlane, p *testsupport.Provider) error
		reason string
	}{
		{"stop", func(cp *testsupport.ControlPlane, p *testsupport.Provider) error {
			return NewStop(cp.Client(), clusters(p)).GiveUp(context.Background(), actionLeased("vm.stop"), errors.New("raw"), testsupport.DiscardLogger())
		}, reasonStopFailed},
		{"start", func(cp *testsupport.ControlPlane, p *testsupport.Provider) error {
			return NewStart(cp.Client(), clusters(p)).GiveUp(context.Background(), actionLeased("vm.start"), errors.New("raw"), testsupport.DiscardLogger())
		}, reasonStartFailed},
		{"delete", func(cp *testsupport.ControlPlane, p *testsupport.Provider) error {
			return NewDelete(cp.Client(), clusters(p)).GiveUp(context.Background(), deleteLeased(), errors.New("raw"), testsupport.DiscardLogger())
		}, reasonDeleteFailed},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cp := testsupport.NewControlPlane(t)

			if err := c.giveUp(cp, testsupport.NewProvider()); err != nil {
				t.Fatal(err)
			}

			body := onlyBody(t, cp, requestRoute)
			if body["outcome"] != "Failed" || body["reason"] != c.reason || body["detail"] != "raw" {
				t.Fatalf("give-up: %v", body)
			}
		})
	}
}

func TestGiveUp_CutsTheDetailAt1000Runes_NotBytes(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	long := errors.New(strings.Repeat("ệ", 1001))

	err := NewDelete(cp.Client(), clusters(testsupport.NewProvider())).GiveUp(context.Background(), deleteLeased(), long, testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	detail, _ := onlyBody(t, cp, requestRoute)["detail"].(string)
	if len([]rune(detail)) != 1000 {
		t.Fatalf("want 1000 runes, got %d", len([]rune(detail)))
	}
}

func TestDelete_WithARequest_DeletesTheInstance_AndEndsItDone(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Instances[project+"/"+instanceName] = &api.Instance{Name: instanceName, StatusCode: api.Stopped}

	err := NewDelete(cp.Client(), clusters(p)).Run(context.Background(), deleteLeased(), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if len(p.DeletedInstances) != 1 {
		t.Fatalf("deleted: %v", p.DeletedInstances)
	}
	body := onlyBody(t, cp, requestRoute)
	if _, has := body["observed"]; has || body["outcome"] != "Done" {
		t.Fatalf("report: %v", body)
	}
}

func TestDelete_WithARequest_AnInstanceAlreadyGone_EndsItDone(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	err := NewDelete(cp.Client(), clusters(p)).Run(context.Background(), deleteLeased(), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body := onlyBody(t, cp, requestRoute)
	if _, has := body["observed"]; has || body["outcome"] != "Done" {
		t.Fatalf("report: %v", body)
	}
}

type handler interface {
	Run(ctx context.Context, leased command.Leased, logger *slog.Logger) error
	GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error
}

// Every tenant command names the request it ends; one that does not has no route to report on
// (titan-ops architecture/resource-lifecycle.md#callback-routes).
func TestCommands_WithoutARequest_FailForGood_AndCallNothing(t *testing.T) {
	kinds := []struct {
		kind    string
		leased  command.Leased
		handler func(cp *testsupport.ControlPlane, p *testsupport.Provider) handler
	}{
		{"vm.create", createLeased(nil), func(cp *testsupport.ControlPlane, p *testsupport.Provider) handler {
			return NewCreate(cp.Client(), clusters(p))
		}},
		{"vm.delete", deleteLeased(), func(cp *testsupport.ControlPlane, p *testsupport.Provider) handler {
			return NewDelete(cp.Client(), clusters(p))
		}},
		{"vm.stop", actionLeased("vm.stop"), func(cp *testsupport.ControlPlane, p *testsupport.Provider) handler {
			return NewStop(cp.Client(), clusters(p))
		}},
		{"vm.start", actionLeased("vm.start"), func(cp *testsupport.ControlPlane, p *testsupport.Provider) handler {
			return NewStart(cp.Client(), clusters(p))
		}},
	}
	withoutRequest := map[string]func(payload map[string]any){
		"absent": func(payload map[string]any) { delete(payload, "requestId") },
		"empty":  func(payload map[string]any) { payload["requestId"] = "" },
	}
	for _, k := range kinds {
		for shape, strip := range withoutRequest {
			t.Run(k.kind+"/"+shape, func(t *testing.T) {
				var payload map[string]any
				if err := json.Unmarshal(k.leased.Payload, &payload); err != nil {
					t.Fatal(err)
				}
				strip(payload)
				cmd := leased(k.kind, payload)
				cp := testsupport.NewControlPlane(t)
				p := testsupport.NewProvider()
				p.Instances[project+"/"+instanceName] = &api.Instance{Name: instanceName, StatusCode: api.Running}
				h := k.handler(cp, p)

				runErr := h.Run(context.Background(), cmd, testsupport.DiscardLogger())
				giveUpErr := h.GiveUp(context.Background(), cmd, errors.New("raw"), testsupport.DiscardLogger())

				if runErr == nil || command.IsRetryable(runErr) {
					t.Fatalf("Run: want a permanent error, got %v", runErr)
				}
				if giveUpErr != nil {
					t.Fatalf("GiveUp: want nil, got %v", giveUpErr)
				}
				if len(cp.Requests()) != 0 {
					t.Fatalf("called the control plane: %+v", cp.Requests())
				}
				if p.Calls != 0 {
					t.Fatalf("touched Incus: %d call(s)", p.Calls)
				}
			})
		}
	}
}
