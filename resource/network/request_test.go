package network

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

// A command that carries a requestId ends that request on its own route
// (titan-ops architecture/resource-lifecycle.md#callback-routes): Done with what the worker read of the network, Failed
// with a fixed reason and the raw error as detail. network_test.go keeps the commands without one, on the old route.

const requestId = "77777777-6666-5555-4444-333333333333"

const (
	statusRoute      = "PATCH /internal/networks/" + networkId + "/status"
	requestRoute     = "PATCH /internal/networks/" + networkId + "/requests/" + requestId
	observationRoute = "PATCH /internal/networks/" + networkId + "/observation"
	networkName      = "net-" + networkId
)

var quickSettling = Settling{Every: time.Millisecond, Within: time.Second}

func runCommand(h command.Handler, l command.Leased) error {
	return h.Run(context.Background(), l, testsupport.DiscardLogger())
}

func giveUpCommand(h command.Handler, l command.Leased, cause error) error {
	return h.GiveUp(context.Background(), l, cause, testsupport.DiscardLogger())
}

func newDelete(cp *testsupport.ControlPlane, p *testsupport.Provider) *Delete {
	return NewDelete(cp.Client(), cluster.Fixed{clusterId: {Provider: p}})
}

func withRequest(l command.Leased) command.Leased {
	var payload map[string]any
	_ = json.Unmarshal(l.Payload, &payload)
	payload["requestId"] = requestId
	return leased(l.Kind, payload)
}

func bodyOn(t *testing.T, cp *testsupport.ControlPlane, route string) map[string]any {
	t.Helper()
	requests := cp.RequestsTo(route)
	if len(requests) != 1 {
		t.Fatalf("want one report on %s, got %+v", route, cp.Requests())
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(requests[0].Body), &body); err != nil {
		t.Fatal(err)
	}
	return body
}

// requestEnd reads the one report on the request route, and checks nothing went to the old one.
func requestEnd(t *testing.T, cp *testsupport.ControlPlane) map[string]any {
	t.Helper()
	if len(cp.RequestsTo(statusRoute)) != 0 {
		t.Fatalf("a command with a requestId used the old route: %+v", cp.Requests())
	}
	return bodyOn(t, cp, requestRoute)
}

func observationBody(t *testing.T, cp *testsupport.ControlPlane) map[string]any {
	t.Helper()
	if len(cp.RequestsTo(statusRoute)) != 0 {
		t.Fatalf("the reconciler used the old route: %+v", cp.Requests())
	}
	return bodyOn(t, cp, observationRoute)
}

func observedOf(body map[string]any) (map[string]any, bool) {
	observed, ok := body["observed"].(map[string]any)
	return observed, ok
}

func quickCreate(cp *testsupport.ControlPlane, p *testsupport.Provider) *Create {
	h := NewCreate(cp.Client(), cluster.Fixed{clusterId: {Provider: p}})
	h.settling = quickSettling
	return h
}

func TestCreate_WithARequest_EndsItDone_WithTheNameTheSubnetAndTheStatus(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	err := runCommand(quickCreate(cp, p), withRequest(createLeased("10.40.0.0/24")))

	if err != nil {
		t.Fatal(err)
	}
	body := requestEnd(t, cp)
	observed, _ := observedOf(body)
	isDone := body["outcome"] == "Done" && body["externalId"] == networkName && observed["status"] == "Created" &&
		observed["cidr"] == "10.40.0.0/24" && observed["gateway"] == "10.40.0.1"
	if !isDone {
		t.Fatalf("report: %v", body)
	}
}

// The control plane counts a network usable by its status, so the create waits while Incus shows it Pending
// (titan-ops architecture/resource-lifecycle.md#callback-routes).
func TestCreate_WithARequest_WaitsWhileTheNetworkIsPending_ThenEndsItDone_WithTheStatusItReached(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.NetworkStatuses = []string{"Pending", "Pending", "Created"}

	err := runCommand(quickCreate(cp, p), withRequest(createLeased("10.40.0.0/24")))

	if err != nil {
		t.Fatal(err)
	}
	observed, _ := observedOf(requestEnd(t, cp))
	if observed["status"] != "Created" || p.GetNetworkReads != 3 {
		t.Fatalf("status %v after %d reads, want Created after 3", observed["status"], p.GetNetworkReads)
	}
}

// The create is done once Incus has the network: a status still Pending when the wait runs out goes as it is, and the
// reconciler reports the rest.
func TestCreate_WithARequest_StillPendingWhenTheWaitRunsOut_EndsItDone_Pending(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.NetworkStatuses = []string{"Pending"}
	h := quickCreate(cp, p)
	h.settling = Settling{Every: time.Millisecond, Within: 20 * time.Millisecond}

	err := h.Run(context.Background(), withRequest(createLeased("10.40.0.0/24")), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body := requestEnd(t, cp)
	observed, _ := observedOf(body)
	if body["outcome"] != "Done" || observed["status"] != "Pending" || p.GetNetworkReads < 2 {
		t.Fatalf("report %v after %d reads", body, p.GetNetworkReads)
	}
}

func TestCreate_WithARequest_StatusReadFails_IsRetryable_AndReportsNothing(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.GetNetworkErr = errors.New("incus unreachable")

	err := runCommand(quickCreate(cp, p), withRequest(createLeased("10.40.0.0/24")))

	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want retryable, got %v", err)
	}
	if len(cp.Requests()) != 0 {
		t.Fatalf("reported %v", cp.Requests())
	}
}

func TestCreate_WithARequest_WithoutACidr_EndsItDone_WithTheSubnetIncusPicked(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Networks = []api.Network{{Name: networkName, Project: project, Status: "Created",
		NetworkPut: api.NetworkPut{Config: map[string]string{"ipv4.address": "10.101.0.1/24"}}}}

	err := runCommand(quickCreate(cp, p), withRequest(createLeased("")))

	if err != nil {
		t.Fatal(err)
	}
	observed, _ := observedOf(requestEnd(t, cp))
	if observed["cidr"] != "10.101.0.0/24" || observed["gateway"] != "10.101.0.1" || observed["status"] != "Created" {
		t.Fatalf("observed: %v", observed)
	}
}

// What a give-up carries decides whether the control plane keeps the network's /64: the provider id and subnet when
// Incus has the network, nothing when it does not.
func TestCreate_WithARequest_GiveUp_EndsItFailed_CarryingTheNetworkOnlyWhenItExists(t *testing.T) {
	for _, exists := range []bool{true, false} {
		cp := testsupport.NewControlPlane(t)
		p := testsupport.NewProvider()
		if exists {
			p.Networks = []api.Network{{Name: networkName, Project: project}}
		}

		err := giveUpCommand(quickCreate(cp, p), withRequest(createLeased("10.40.0.0/24")), errors.New("gave up"))

		if err != nil {
			t.Fatal(err)
		}
		body := requestEnd(t, cp)
		observed, hasObservation := observedOf(body)
		gotId, _ := body["externalId"].(string)
		isFailed := body["outcome"] == "Failed" && body["reason"] == reasonCreateFailed && body["detail"] == "gave up"
		carriesTheNetwork := gotId == networkName && hasObservation && observed["cidr"] == "10.40.0.0/24" &&
			observed["gateway"] == "10.40.0.1"
		carriesNothing := gotId == "" && !hasObservation
		if !isFailed || (exists && !carriesTheNetwork) || (!exists && !carriesNothing) {
			t.Errorf("network there %v: report %v", exists, body)
		}
	}
}

func TestCreate_WithARequest_GiveUp_CannotListNetworks_IsRetryable_AndReportsNothing(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.ListNetworksErr = errors.New("incus unreachable")

	err := giveUpCommand(quickCreate(cp, p), withRequest(createLeased("10.40.0.0/24")), errors.New("gave up"))

	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want retryable, got %v", err)
	}
	if len(cp.Requests()) != 0 {
		t.Fatalf("reported %v", cp.Requests())
	}
}

func TestDelete_WithARequest_EndsItDone_WithoutAnObservation(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	err := runCommand(newDelete(cp, p), withRequest(deleteLeased()))

	if err != nil {
		t.Fatal(err)
	}
	body := requestEnd(t, cp)
	if _, hasObservation := observedOf(body); body["outcome"] != "Done" || hasObservation {
		t.Fatalf("report: %v", body)
	}
}

// A delete whose network Incus no longer has gives nothing back to the tenant: its give-up is the request's Done. One
// whose network is still there is Failed.
func TestDelete_WithARequest_GiveUp_IsDone_WhenIncusNoLongerHasTheNetwork_ElseFailed(t *testing.T) {
	for _, exists := range []bool{true, false} {
		cp := testsupport.NewControlPlane(t)
		p := testsupport.NewProvider()
		if exists {
			p.Networks = []api.Network{{Name: networkName, Project: project}}
		}

		err := giveUpCommand(newDelete(cp, p), withRequest(deleteLeased()), errors.New("in use"))

		if err != nil {
			t.Fatal(err)
		}
		body := requestEnd(t, cp)
		isFailed := body["outcome"] == "Failed" && body["reason"] == reasonDeleteFailed && body["detail"] == "in use"
		_, hasReason := body["reason"]
		isDone := body["outcome"] == "Done" && !hasReason
		if (exists && !isFailed) || (!exists && !isDone) {
			t.Errorf("network there %v: report %v", exists, body)
		}
	}
}

func TestDelete_WithARequest_GiveUp_NetworkGoneButItsAclStays_IsFailed(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.DeleteACLErr = errors.New("acl in use")

	err := giveUpCommand(newDelete(cp, p), withRequest(deleteLeased()), errors.New("acl in use"))

	if err != nil {
		t.Fatal(err)
	}
	if body := requestEnd(t, cp); body["outcome"] != "Failed" || body["reason"] != reasonDeleteFailed {
		t.Errorf("report %v", body)
	}
}

func TestDelete_WithARequest_GiveUp_NetworkReadFails_IsRetryable_AndReportsNothing(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.GetNetworkErr = errors.New("incus unreachable")

	err := giveUpCommand(newDelete(cp, p), withRequest(deleteLeased()), errors.New("in use"))

	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want retryable, got %v", err)
	}
	if len(cp.Requests()) != 0 {
		t.Fatalf("reported %v", cp.Requests())
	}
}

func TestGiveUp_WithARequest_CutsTheDetailAt1000Runes_NotBytes(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Networks = []api.Network{{Name: networkName, Project: project}}
	long := strings.Repeat("é", controlplane.MaxDetail+10)

	err := giveUpCommand(newDelete(cp, p), withRequest(deleteLeased()), errors.New(long))

	if err != nil {
		t.Fatal(err)
	}
	detail, _ := requestEnd(t, cp)["detail"].(string)
	if len([]rune(detail)) != controlplane.MaxDetail {
		t.Fatalf("detail is %d runes, want %d", len([]rune(detail)), controlplane.MaxDetail)
	}
}

// A command the old control plane enqueued without a requestId keeps the old route until rollout phase 6
// (titan-ops architecture/resource-lifecycle.md#rollout).
func TestCommands_WithoutARequest_ReportOnTheOldRoute(t *testing.T) {
	cases := []struct {
		name string
		run  func(cp *testsupport.ControlPlane, p *testsupport.Provider) error
	}{
		{"create", func(cp *testsupport.ControlPlane, p *testsupport.Provider) error {
			return runCommand(quickCreate(cp, p), createLeased("10.40.0.0/24"))
		}},
		{"create give-up", func(cp *testsupport.ControlPlane, p *testsupport.Provider) error {
			return giveUpCommand(quickCreate(cp, p), createLeased("10.40.0.0/24"), errors.New("x"))
		}},
		{"delete", func(cp *testsupport.ControlPlane, p *testsupport.Provider) error {
			return runCommand(newDelete(cp, p), deleteLeased())
		}},
		{"delete give-up", func(cp *testsupport.ControlPlane, p *testsupport.Provider) error {
			return giveUpCommand(newDelete(cp, p), deleteLeased(), errors.New("x"))
		}},
	}
	for _, c := range cases {
		cp := testsupport.NewControlPlane(t)

		if err := c.run(cp, testsupport.NewProvider()); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		requestRoutes := cp.RequestsTo("PATCH /internal/networks/" + networkId + "/requests/")
		if len(cp.RequestsTo(statusRoute)) != 1 || len(requestRoutes) != 0 {
			t.Errorf("%s: reports %+v", c.name, cp.Requests())
		}
	}
}

func TestReconcile_SendsOnTheObservationRoute_NeverTheOldOne(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, "/internal/reconcile/networks?clusterId=mock", http.StatusOK,
		`[{"id":"`+networkId+`","externalId":"net-a","currentState":"Pending","currentExistence":"Exists"}]`)
	p := testsupport.NewProvider()
	p.Networks = []api.Network{{Name: "net-a", Status: "Errored", Project: project}}

	err := Reconcile(context.Background(), cp.Client(), p, "mock", testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body := observationBody(t, cp)
	observed, _ := observedOf(body)
	if body["exists"] != true || len(observed) != 1 || observed["status"] != "Errored" {
		t.Fatalf("observation: %v", body)
	}
}
