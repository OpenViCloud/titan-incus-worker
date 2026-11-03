package loadbalancer

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
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
// finds the VIP gone; Failed with a fixed reason and the raw error, cut, as detail. The tests named WithoutARequest
// here, and loadbalancer_test.go, keep the commands without one on the old route.

const requestId = "77777777-6666-5555-4444-333333333333"

const (
	statusRoute      = "PATCH /internal/load-balancers/" + loadBalancerId + "/status"
	requestRoute     = "PATCH /internal/load-balancers/" + loadBalancerId + "/requests/" + requestId
	anyRequestRoute  = "PATCH /internal/load-balancers/" + loadBalancerId + "/requests/"
	observationRoute = "PATCH /internal/load-balancers/" + loadBalancerId + "/observation"
)

func createLeased() command.Leased { return leased("load-balancer.create", specFields()) }
func updateLeased() command.Leased { return leased("load-balancer.update", updateFields()) }
func deleteLeased() command.Leased { return leased("load-balancer.delete", deleteFields()) }

func withRequest(l command.Leased) command.Leased {
	var payload map[string]any
	_ = json.Unmarshal(l.Payload, &payload)
	payload["requestId"] = requestId
	return leased(l.Kind, payload)
}

func runCommand(h command.Handler, l command.Leased) error {
	return h.Run(context.Background(), l, discard)
}

func giveUpCommand(h command.Handler, l command.Leased, cause error) error {
	return h.GiveUp(context.Background(), l, cause, discard)
}

func withVip(p *testsupport.Provider) *testsupport.Provider {
	p.PutLoadBalancer(project, network, api.NetworkLoadBalancer{ListenAddress: vip})
	return p
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

// requestEnd reads the one report on the request route, and checks nothing went to the old one.
func requestEnd(t *testing.T, cp *testsupport.ControlPlane) map[string]any {
	t.Helper()
	if len(cp.RequestsTo(statusRoute)) != 0 {
		t.Fatalf("a command with a requestId used the old route: %+v", cp.Requests())
	}
	return onlyBody(t, cp, requestRoute)
}

type handlerOf func(cp *testsupport.ControlPlane, p *testsupport.Provider) command.Handler

func createHandler(cp *testsupport.ControlPlane, p *testsupport.Provider) command.Handler {
	return NewCreate(cp.Client(), clusters(p))
}

func updateHandler(cp *testsupport.ControlPlane, p *testsupport.Provider) command.Handler {
	return NewUpdate(cp.Client(), clusters(p))
}

func deleteHandler(cp *testsupport.ControlPlane, p *testsupport.Provider) command.Handler {
	return NewDelete(cp.Client(), clusters(p))
}

func TestRun_WithARequest_EndsItDone_OnTheRequestRoute(t *testing.T) {
	cases := []struct {
		name           string
		handler        handlerOf
		leased         command.Leased
		vipThere       bool
		wantExternalId string
	}{
		{"create", createHandler, createLeased(), false, vip},
		{"update", updateHandler, updateLeased(), true, ""},
		{"delete", deleteHandler, deleteLeased(), true, ""},
		// 404: an earlier run deleted it; the delete is done all the same.
		{"delete, already gone", deleteHandler, deleteLeased(), false, ""},
	}
	for _, c := range cases {
		cp := testsupport.NewControlPlane(t)
		p := withVms()
		if c.vipThere {
			withVip(p)
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

func TestRun_WithARequest_WritesIncus(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := withVms()

	if err := runCommand(NewCreate(cp.Client(), clusters(p)), withRequest(createLeased())); err != nil {
		t.Fatal(err)
	}
	if err := runCommand(NewUpdate(cp.Client(), clusters(p)), withRequest(updateLeased())); err != nil {
		t.Fatal(err)
	}
	if len(p.CreatedLoadBalancers) != 1 || len(p.UpdatedLoadBalancers) != 1 || len(p.UpdatedLoadBalancers[0].Backends) != 4 {
		t.Fatalf("created %+v, updated %+v", p.CreatedLoadBalancers, p.UpdatedLoadBalancers)
	}
	if err := runCommand(NewDelete(cp.Client(), clusters(p)), withRequest(deleteLeased())); err != nil {
		t.Fatal(err)
	}
	if len(p.LoadBalancers) != 0 {
		t.Fatalf("left %v", p.LoadBalancers)
	}
}

func TestRun_WithARequest_AFailedWrite_ReportsNothing(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	fresh := withVms()
	fresh.CreateLoadBalancerErr = api.StatusErrorf(http.StatusInternalServerError, "busy")
	p := withVip(withVms())
	p.UpdateLoadBalancerErr = api.StatusErrorf(http.StatusInternalServerError, "busy")
	p.DeleteLoadBalancerErr = api.StatusErrorf(http.StatusInternalServerError, "busy")

	createErr := runCommand(NewCreate(cp.Client(), clusters(fresh)), withRequest(createLeased()))
	updateErr := runCommand(NewUpdate(cp.Client(), clusters(p)), withRequest(updateLeased()))
	deleteErr := runCommand(NewDelete(cp.Client(), clusters(p)), withRequest(deleteLeased()))

	if createErr == nil || updateErr == nil || deleteErr == nil || len(cp.Requests()) != 0 {
		t.Fatalf("create %v, update %v, delete %v, requests %+v", createErr, updateErr, deleteErr, cp.Requests())
	}
}

// A give-up that carries the VIP tells the control plane the create did happen, so a delete of it reaches Incus.
func TestCreate_WithARequest_GiveUp_EndsItFailed_CarryingTheVipOnlyWhenIncusHasIt(t *testing.T) {
	for _, exists := range []bool{true, false} {
		cp := testsupport.NewControlPlane(t)
		p := withVms()
		if exists {
			withVip(p)
		}

		err := giveUpCommand(NewCreate(cp.Client(), clusters(p)), withRequest(createLeased()), errors.New("raw"))

		if err != nil {
			t.Fatal(err)
		}
		body := requestEnd(t, cp)
		wantId := ""
		if exists {
			wantId = vip
		}
		gotId, _ := body["externalId"].(string)
		if body["outcome"] != "Failed" || body["reason"] != reasonCreateFailed || body["detail"] != "raw" || gotId != wantId {
			t.Errorf("VIP there %v: report %v", exists, body)
		}
	}
}

// A read that fails is not "no VIP": the next lease gives up again.
func TestGiveUps_WithARequest_VipReadFails_AreRetryable_AndReportNothing(t *testing.T) {
	cases := []struct {
		name    string
		handler handlerOf
		leased  command.Leased
	}{
		{"create", createHandler, createLeased()},
		{"delete", deleteHandler, deleteLeased()},
	}
	for _, c := range cases {
		cp := testsupport.NewControlPlane(t)
		p := withVip(withVms())
		p.GetLoadBalancerErr = api.StatusErrorf(http.StatusInternalServerError, "busy")

		err := giveUpCommand(c.handler(cp, p), withRequest(c.leased), errors.New("raw"))

		if err == nil || !command.IsRetryable(err) {
			t.Errorf("%s: want retryable, got %v", c.name, err)
		}
		if len(cp.Requests()) != 0 {
			t.Errorf("%s: reported %+v", c.name, cp.Requests())
		}
	}
}

// A cluster the worker is not configured for says nothing about the VIP: a create gives up without it, and a delete
// is not gone, so it gives the load balancer back.
func TestGiveUps_WithARequest_OnAClusterTheWorkerIsNotConfiguredFor_AreFailed_WithoutAnId(t *testing.T) {
	cases := []struct {
		name       string
		handler    func(cp *controlplane.Client) command.Handler
		leased     command.Leased
		wantReason string
	}{
		{"create", func(cp *controlplane.Client) command.Handler { return NewCreate(cp, cluster.Fixed{}) }, createLeased(), reasonCreateFailed},
		{"delete", func(cp *controlplane.Client) command.Handler { return NewDelete(cp, cluster.Fixed{}) }, deleteLeased(), reasonDeleteFailed},
	}
	for _, c := range cases {
		cp := testsupport.NewControlPlane(t)

		err := giveUpCommand(c.handler(cp.Client()), withRequest(c.leased), errors.New("raw"))

		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		body := requestEnd(t, cp)
		_, hasId := body["externalId"]
		if body["outcome"] != "Failed" || body["reason"] != c.wantReason || hasId {
			t.Errorf("%s: report %v", c.name, body)
		}
	}
}

// Proving an update done would mean comparing the whole spec, so its give-up is Failed whatever Incus shows, and it
// reads nothing.
func TestUpdate_WithARequest_GiveUp_IsAlwaysFailed_WithoutReadingIncus(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := withVip(withVms())
	p.GetLoadBalancerErr = api.StatusErrorf(http.StatusInternalServerError, "busy")

	err := giveUpCommand(NewUpdate(cp.Client(), clusters(p)), withRequest(updateLeased()), errors.New("raw"))

	if err != nil {
		t.Fatal(err)
	}
	body := requestEnd(t, cp)
	_, hasId := body["externalId"]
	if body["outcome"] != "Failed" || body["reason"] != reasonUpdateFailed || body["detail"] != "raw" || hasId {
		t.Errorf("report %v", body)
	}
	if p.Calls != 0 {
		t.Errorf("the give-up called Incus %d time(s)", p.Calls)
	}
}

// A delete whose VIP Incus no longer has gives nothing back to the tenant: its give-up is the request's Done.
func TestDelete_WithARequest_GiveUp_IsDone_WhenIncusNoLongerHasTheVip_ElseFailed(t *testing.T) {
	for _, exists := range []bool{true, false} {
		cp := testsupport.NewControlPlane(t)
		logger, logs := testsupport.NewLogCapture(t)
		p := withVms()
		if exists {
			withVip(p)
		}

		err := NewDelete(cp.Client(), clusters(p)).GiveUp(context.Background(), withRequest(deleteLeased()), errors.New("in use"), logger)

		if err != nil {
			t.Fatal(err)
		}
		body := requestEnd(t, cp)
		isFailed := body["outcome"] == "Failed" && body["reason"] == reasonDeleteFailed && body["detail"] == "in use"
		_, hasReason := body["reason"]
		isDone := body["outcome"] == "Done" && !hasReason && len(body) == 1
		if (exists && !isFailed) || (!exists && !isDone) {
			t.Errorf("VIP there %v: report %v", exists, body)
		}
		wantGoneLines := 0
		if !exists {
			wantGoneLines = 1
		}
		if got := logs.Objects("already gone; the give-up ends the request done"); len(got) != wantGoneLines {
			t.Errorf("VIP there %v: want %d gone line(s), got %v", exists, wantGoneLines, got)
		}
	}
}

func TestGiveUps_WithARequest_CutTheDetailAt1000Runes_NotBytes(t *testing.T) {
	cases := []struct {
		name    string
		handler handlerOf
		leased  command.Leased
	}{
		{"create", createHandler, createLeased()},
		{"update", updateHandler, updateLeased()},
		{"delete", deleteHandler, deleteLeased()},
	}
	long := strings.Repeat("é", controlplane.MaxDetail+10)
	for _, c := range cases {
		cp := testsupport.NewControlPlane(t)
		p := withVip(withVms())

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
		{"update", func(cp *testsupport.ControlPlane, p *testsupport.Provider) error {
			return runCommand(NewUpdate(cp.Client(), clusters(p)), updateLeased())
		}, "Updated"},
		{"update give-up", func(cp *testsupport.ControlPlane, p *testsupport.Provider) error {
			return giveUpCommand(NewUpdate(cp.Client(), clusters(p)), updateLeased(), errors.New("x"))
		}, "RequestFailed"},
		{"delete", func(cp *testsupport.ControlPlane, p *testsupport.Provider) error {
			return runCommand(NewDelete(cp.Client(), clusters(p)), deleteLeased())
		}, "Deleted"},
		// The old route keeps the old give-up: no read, RequestFailed even with the VIP gone.
		{"delete give-up", func(cp *testsupport.ControlPlane, p *testsupport.Provider) error {
			return giveUpCommand(NewDelete(cp.Client(), clusters(p)), deleteLeased(), errors.New("x"))
		}, "RequestFailed"},
	}
	for _, c := range cases {
		cp := testsupport.NewControlPlane(t)

		if err := c.run(cp, withVms()); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if len(cp.RequestsTo(anyRequestRoute)) != 0 {
			t.Errorf("%s: reports %+v", c.name, cp.Requests())
			continue
		}
		if got := onlyBody(t, cp, statusRoute)["report"]; got != c.wantReport {
			t.Errorf("%s: report %v, want %s", c.name, got, c.wantReport)
		}
	}
}

// The old route keeps the old create give-up too: a VIP that cannot be read goes without its id, and the raw detail
// is not cut.
func TestCreate_WithoutARequest_GiveUp_VipReadFails_StillReportsRequestFailed_WithoutTheId(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := withVip(withVms())
	p.GetLoadBalancerErr = api.StatusErrorf(http.StatusInternalServerError, "busy")
	long := strings.Repeat("é", controlplane.MaxDetail+10)

	err := giveUpCommand(NewCreate(cp.Client(), clusters(p)), createLeased(), errors.New(long))

	body := onlyBody(t, cp, statusRoute)
	if _, hasId := body["externalId"]; err != nil || body["report"] != "RequestFailed" || hasId || body["detail"] != long {
		t.Fatalf("err %v, report %v", err, body["report"])
	}
}

func TestReconcile_SendsOnTheObservationRoute_NeverTheOldOne(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	rows := []ReconcilableLoadBalancer{{Id: loadBalancerId, ProjectName: project, ExternalNetworkId: network, Vip: vip, CurrentExistence: "NotCreated"}}
	encoded, _ := json.Marshal(rows)
	cp.Respond("GET", "/internal/reconcile/load-balancers?clusterId="+clusterId, http.StatusOK, string(encoded))
	p := withVip(testsupport.NewProvider())

	err := Reconcile(context.Background(), cp.Client(), p, clusterId, discard)

	if err != nil {
		t.Fatal(err)
	}
	if len(cp.RequestsTo(statusRoute)) != 0 {
		t.Fatalf("the reconciler used the old route: %+v", cp.Requests())
	}
	body := onlyBody(t, cp, observationRoute)
	if body["exists"] != true || len(body) != 1 {
		t.Fatalf("observation: %v", body)
	}
}
