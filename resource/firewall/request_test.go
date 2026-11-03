package firewall

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

// A command that carries a requestId ends that request on its own route
// (titan-ops architecture/resource-lifecycle.md#callback-routes): Done with what the worker read of the rule, Failed
// with a fixed reason and the raw error as detail. firewall_test.go keeps the commands without one, on the old route.

const requestId = "77777777-6666-5555-4444-333333333333"

const (
	ruleRoute    = "PATCH /internal/networks/" + networkId + "/firewall-rules/" + ruleId
	requestRoute = ruleRoute + "/requests/" + requestId
)

func runCommand(h command.Handler, l command.Leased) error {
	return h.Run(context.Background(), l, testsupport.DiscardLogger())
}

func giveUpCommand(h command.Handler, l command.Leased, cause error) error {
	return h.GiveUp(context.Background(), l, cause, testsupport.DiscardLogger())
}

func withRequest(l command.Leased) command.Leased {
	var payload map[string]any
	_ = json.Unmarshal(l.Payload, &payload)
	payload["requestId"] = requestId
	return leased(l.Kind, payload)
}

// requestsExactlyTo returns the requests to exactly "METHOD path", not to the routes under it.
func requestsExactlyTo(cp *testsupport.ControlPlane, route string) []testsupport.Request {
	matching := []testsupport.Request{}
	for _, request := range cp.Requests() {
		if request.Method+" "+request.Path == route {
			matching = append(matching, request)
		}
	}
	return matching
}

func decoded(t *testing.T, requests []testsupport.Request, cp *testsupport.ControlPlane) map[string]any {
	t.Helper()
	if len(requests) != 1 {
		t.Fatalf("want one report, got %+v", cp.Requests())
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
	if len(requestsExactlyTo(cp, ruleRoute)) != 0 {
		t.Fatalf("a command with a requestId used the old route: %+v", cp.Requests())
	}
	return decoded(t, requestsExactlyTo(cp, requestRoute), cp)
}

// observationOf reads the one observation of a rule, and checks nothing went to its old route.
func observationOf(t *testing.T, cp *testsupport.ControlPlane, id string) map[string]any {
	t.Helper()
	route := "PATCH /internal/networks/" + networkId + "/firewall-rules/" + id
	if len(requestsExactlyTo(cp, route)) != 0 {
		t.Fatalf("the reconciler used the old route: %+v", cp.Requests())
	}
	return decoded(t, requestsExactlyTo(cp, route+"/observation"), cp)
}

func stateOf(body map[string]any) (any, bool) {
	observed, ok := body["observed"].(map[string]any)
	if !ok {
		return nil, false
	}
	return observed["state"], true
}

func TestCreate_WithARequest_EndsItDone_WithTheIdAndTheStateItReadBack(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := withAcl()

	err := runCommand(NewCreate(cp.Client(), clusters(p)), withRequest(createLeased()))

	if err != nil {
		t.Fatal(err)
	}
	body := requestEnd(t, cp)
	state, observed := stateOf(body)
	if body["outcome"] != "Done" || body["externalId"] != aclName+"/"+ruleId || !observed || state != "enabled" {
		t.Fatalf("report: %v", body)
	}
}

// An earlier run wrote the rule: its state is the one the read found, as Incus spells it.
func TestCreate_WithARequest_RuleAlreadyThere_EndsItDone_WithTheStateItFound(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := withAcl(api.NetworkACLRule{Action: "allow", Description: ruleId, State: "logged"})

	err := runCommand(NewCreate(cp.Client(), clusters(p)), withRequest(createLeased()))

	if err != nil {
		t.Fatal(err)
	}
	body := requestEnd(t, cp)
	if state, _ := stateOf(body); body["outcome"] != "Done" || state != "logged" || len(p.UpdatedACLs) != 0 {
		t.Fatalf("report %v, writes %d", body, len(p.UpdatedACLs))
	}
}

// What a give-up carries decides whether the control plane keeps an id to delete the rule through.
func TestCreate_WithARequest_GiveUp_EndsItFailed_CarryingTheIdOnlyWhenTheRuleIsThere(t *testing.T) {
	cases := []struct {
		name   string
		p      *testsupport.Provider
		wantId string
	}{
		{"in the ACL", withAcl(api.NetworkACLRule{Description: ruleId, State: "enabled"}), aclName + "/" + ruleId},
		{"not in the ACL", withAcl(), ""},
		{"no ACL", testsupport.NewProvider(), ""},
	}
	for _, c := range cases {
		cp := testsupport.NewControlPlane(t)

		err := giveUpCommand(NewCreate(cp.Client(), clusters(c.p)), withRequest(createLeased()), errors.New("gave up"))

		if err != nil {
			t.Fatal(err)
		}
		body := requestEnd(t, cp)
		gotId, _ := body["externalId"].(string)
		isFailed := body["outcome"] == "Failed" && body["reason"] == reasonCreateFailed && body["detail"] == "gave up"
		if !isFailed || gotId != c.wantId {
			t.Errorf("%s: report %v", c.name, body)
		}
	}
}

// A read that failed says neither "there" nor "not there": the next lease gives up again.
func TestCreate_WithARequest_GiveUp_AclReadFails_IsRetryable_AndReportsNothing(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := withAcl(api.NetworkACLRule{Description: ruleId, State: "enabled"})
	p.GetACLErr = errors.New("incus unreachable")

	err := giveUpCommand(NewCreate(cp.Client(), clusters(p)), withRequest(createLeased()), errors.New("gave up"))

	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want retryable, got %v", err)
	}
	if len(cp.Requests()) != 0 {
		t.Fatalf("reported %v", cp.Requests())
	}
}

// The old route keeps today's give-up: a read that fails reads as "not there".
func TestCreate_WithoutARequest_GiveUp_AclReadFails_ReportsWithoutAnId(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := withAcl(api.NetworkACLRule{Description: ruleId, State: "enabled"})
	p.GetACLErr = errors.New("incus unreachable")

	err := giveUpCommand(NewCreate(cp.Client(), clusters(p)), createLeased(), errors.New("gave up"))

	if err != nil {
		t.Fatal(err)
	}
	body := decoded(t, requestsExactlyTo(cp, ruleRoute), cp)
	if _, hasId := body["externalRuleId"]; body["report"] != reportRequestFailed || hasId {
		t.Fatalf("report: %v", body)
	}
}

func TestDelete_WithARequest_EndsItDone_WithoutAnObservation(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := withAcl(api.NetworkACLRule{Description: ruleId, State: "enabled"})

	err := runCommand(NewDelete(cp.Client(), clusters(p)), withRequest(deleteLeased()))

	if err != nil {
		t.Fatal(err)
	}
	body := requestEnd(t, cp)
	if _, observed := stateOf(body); body["outcome"] != "Done" || observed {
		t.Fatalf("report: %v", body)
	}
}

// A delete whose rule the ACL no longer has gives nothing back to the tenant: its give-up is the request's Done. One
// whose rule is still there is Failed.
func TestDelete_WithARequest_GiveUp_IsDone_WhenTheAclNoLongerHasTheRule_ElseFailed(t *testing.T) {
	cases := []struct {
		name        string
		p           *testsupport.Provider
		wantOutcome string
	}{
		{"in the ACL", withAcl(api.NetworkACLRule{Description: ruleId, State: "enabled"}), "Failed"},
		{"not in the ACL", withAcl(api.NetworkACLRule{Description: otherRule, State: "enabled"}), "Done"},
		{"no ACL", testsupport.NewProvider(), "Done"},
	}
	for _, c := range cases {
		cp := testsupport.NewControlPlane(t)

		err := giveUpCommand(NewDelete(cp.Client(), clusters(c.p)), withRequest(deleteLeased()), errors.New("refused"))

		if err != nil {
			t.Fatal(err)
		}
		body := requestEnd(t, cp)
		gotReason, _ := body["reason"].(string)
		wantReason := ""
		if c.wantOutcome == "Failed" {
			wantReason = reasonDeleteFailed
		}
		if body["outcome"] != c.wantOutcome || gotReason != wantReason {
			t.Errorf("%s: report %v", c.name, body)
		}
	}
}

func TestDelete_WithARequest_GiveUp_AclReadFails_IsRetryable_AndReportsNothing(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := withAcl(api.NetworkACLRule{Description: ruleId, State: "enabled"})
	p.GetACLErr = errors.New("incus unreachable")

	err := giveUpCommand(NewDelete(cp.Client(), clusters(p)), withRequest(deleteLeased()), errors.New("refused"))

	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want retryable, got %v", err)
	}
	if len(cp.Requests()) != 0 {
		t.Fatalf("reported %v", cp.Requests())
	}
}

func TestGiveUp_WithARequest_CutsTheDetailAt1000Runes_NotBytes(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := withAcl(api.NetworkACLRule{Description: ruleId, State: "enabled"})
	long := strings.Repeat("é", controlplane.MaxDetail+10)

	err := giveUpCommand(NewDelete(cp.Client(), clusters(p)), withRequest(deleteLeased()), errors.New(long))

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
		run  func(cp *testsupport.ControlPlane) error
	}{
		{"create", func(cp *testsupport.ControlPlane) error {
			return runCommand(NewCreate(cp.Client(), clusters(withAcl())), createLeased())
		}},
		{"create give-up", func(cp *testsupport.ControlPlane) error {
			return giveUpCommand(NewCreate(cp.Client(), clusters(withAcl())), createLeased(), errors.New("x"))
		}},
		{"delete", func(cp *testsupport.ControlPlane) error {
			return runCommand(NewDelete(cp.Client(), clusters(withAcl())), deleteLeased())
		}},
		{"delete give-up", func(cp *testsupport.ControlPlane) error {
			return giveUpCommand(NewDelete(cp.Client(), clusters(withAcl())), deleteLeased(), errors.New("x"))
		}},
	}
	for _, c := range cases {
		cp := testsupport.NewControlPlane(t)

		if err := c.run(cp); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if len(requestsExactlyTo(cp, ruleRoute)) != 1 || len(cp.RequestsTo(ruleRoute+"/requests/")) != 0 {
			t.Errorf("%s: reports %+v", c.name, cp.Requests())
		}
	}
}

func TestReconcile_SendsOnTheObservationRoute_NeverTheOldOne(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, "/internal/reconcile/firewall-rules?clusterId=mock", http.StatusOK,
		`[{"id":"`+ruleId+`","networkId":"`+networkId+`","externalId":"`+aclName+`/`+ruleId+`",`+
			`"currentState":"enabled","currentExistence":"Exists"}]`)
	p := withAcl(api.NetworkACLRule{Action: "allow", Description: ruleId, State: "disabled"})

	err := Reconcile(context.Background(), cp.Client(), p, "mock", testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body := observationOf(t, cp, ruleId)
	if state, observed := stateOf(body); body["exists"] != true || !observed || state != "disabled" {
		t.Fatalf("observation: %v", body)
	}
}
