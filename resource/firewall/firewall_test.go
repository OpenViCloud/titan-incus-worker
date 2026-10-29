package firewall

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

const (
	ruleId    = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	otherRule = "99999999-8888-7777-6666-555555555555"
	networkId = "11111111-2222-3333-4444-555555555555"
	project   = "t-0123456789abcdef0123456789abcdef"
	clusterId = "mock"
	aclName   = "acl-" + networkId
)

func leased(kind string, payload map[string]any) command.Leased {
	encoded, _ := json.Marshal(payload)
	return command.Leased{Kind: kind, ClusterId: clusterId, Payload: encoded, Attempt: 1, MaxAttempts: 5}
}

func createPayload(id string, direction string, protocol string, startPort any, endPort any, cidr string) map[string]any {
	return map[string]any{
		"ruleId": id, "networkId": networkId, "direction": direction, "protocol": protocol,
		"startPort": startPort, "endPort": endPort, "cidr": cidr, "externalNetworkId": "net-" + networkId,
		"projectName": project,
	}
}

func createLeased() command.Leased {
	return leased("firewall-rule.create", createPayload(ruleId, "Ingress", "TCP", 22, 22, "10.40.0.0/24"))
}

func deleteLeased() command.Leased {
	return leased("firewall-rule.delete", map[string]any{
		"ruleId": ruleId, "networkId": networkId, "externalRuleId": aclName + "/" + ruleId,
		"projectName": project,
	})
}

func withAcl(rules ...api.NetworkACLRule) *testsupport.Provider {
	p := testsupport.NewProvider()
	acl := api.NetworkACL{NetworkACLPost: api.NetworkACLPost{Name: aclName}}
	for _, rule := range rules {
		acl.Ingress = append(acl.Ingress, rule)
	}
	p.PutACL(project, acl)
	return p
}

func onlyBody(t *testing.T, cp *testsupport.ControlPlane, id string) map[string]any {
	t.Helper()
	requests := cp.RequestsTo("PATCH /internal/networks/" + networkId + "/firewall-rules/" + id)
	if len(requests) != 1 {
		t.Fatalf("want one report, got %+v", cp.Requests())
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(requests[0].Body), &body); err != nil {
		t.Fatal(err)
	}
	return body
}

func clusters(p *testsupport.Provider) cluster.Fixed {
	return cluster.Fixed{clusterId: {Provider: p}}
}

func TestAclRule_MapsTitansRule(t *testing.T) {
	cases := []struct {
		name          string
		direction     string
		protocol      string
		startPort     *int
		endPort       *int
		wantDirection string
		want          api.NetworkACLRule
	}{
		{"ingress tcp one port", "Ingress", "TCP", intOf(22), intOf(22), "ingress",
			api.NetworkACLRule{Action: "allow", Source: "10.40.0.0/24", Protocol: "tcp", DestinationPort: "22", Description: ruleId, State: "enabled"}},
		{"egress udp range", "Egress", "UDP", intOf(5000), intOf(5010), "egress",
			api.NetworkACLRule{Action: "allow", Destination: "10.40.0.0/24", Protocol: "udp", DestinationPort: "5000-5010", Description: ruleId, State: "enabled"}},
		{"start port only", "Ingress", "TCP", intOf(443), nil, "ingress",
			api.NetworkACLRule{Action: "allow", Source: "10.40.0.0/24", Protocol: "tcp", DestinationPort: "443", Description: ruleId, State: "enabled"}},
		{"icmp", "Ingress", "ICMP", nil, nil, "ingress",
			api.NetworkACLRule{Action: "allow", Source: "10.40.0.0/24", Protocol: "icmp4", Description: ruleId, State: "enabled"}},
		{"all", "Egress", "All", nil, nil, "egress",
			api.NetworkACLRule{Action: "allow", Destination: "10.40.0.0/24", Description: ruleId, State: "enabled"}},
	}
	for _, c := range cases {
		payload := CreatePayload{RuleId: ruleId, Direction: c.direction, Protocol: c.protocol, StartPort: c.startPort, EndPort: c.endPort, Cidr: "10.40.0.0/24"}

		direction, rule, err := AclRule(payload)

		if err != nil || direction != c.wantDirection || rule != c.want {
			t.Errorf("%s: want %s %+v, got %s %+v (%v)", c.name, c.wantDirection, c.want, direction, rule, err)
		}
	}
}

func TestAclRule_RefusesWhatItCannotMap(t *testing.T) {
	cases := []CreatePayload{
		{RuleId: ruleId, Direction: "Sideways", Protocol: "TCP", StartPort: intOf(22), Cidr: "10.0.0.0/8"},
		{RuleId: ruleId, Direction: "Ingress", Protocol: "SCTP", Cidr: "10.0.0.0/8"},
		{RuleId: ruleId, Direction: "Ingress", Protocol: "TCP", StartPort: intOf(22), Cidr: "not-a-cidr"},
		// An IPv4 range written in IPv6 form: which ICMP it means is a guess.
		{RuleId: ruleId, Direction: "Ingress", Protocol: "ICMP", Cidr: "::ffff:10.0.0.0/104"},
	}
	for _, payload := range cases {
		if _, _, err := AclRule(payload); err == nil {
			t.Errorf("%+v accepted", payload)
		}
	}
}

// A network with public IPv6 needs rules for it; ICMP takes the family of the rule's CIDR, since Incus has one protocol
// word per family.
func TestAclRule_MapsAnIpv6Cidr_AndIcmpFollowsTheCidrsFamily(t *testing.T) {
	cases := []struct {
		name          string
		direction     string
		protocol      string
		startPort     *int
		cidr          string
		wantDirection string
		want          api.NetworkACLRule
	}{
		{"ipv6 tcp", "Ingress", "TCP", intOf(22), "2001:db8:0:1::/64", "ingress",
			api.NetworkACLRule{Action: "allow", Source: "2001:db8:0:1::/64", Protocol: "tcp", DestinationPort: "22", Description: ruleId, State: "enabled"}},
		{"ipv6 any address", "Ingress", "TCP", intOf(443), "::/0", "ingress",
			api.NetworkACLRule{Action: "allow", Source: "::/0", Protocol: "tcp", DestinationPort: "443", Description: ruleId, State: "enabled"}},
		{"icmp over ipv6", "Ingress", "ICMP", nil, "::/0", "ingress",
			api.NetworkACLRule{Action: "allow", Source: "::/0", Protocol: "icmp6", Description: ruleId, State: "enabled"}},
		{"icmp over ipv6 egress", "Egress", "ICMP", nil, "fd00::/64", "egress",
			api.NetworkACLRule{Action: "allow", Destination: "fd00::/64", Protocol: "icmp6", Description: ruleId, State: "enabled"}},
		{"icmp over ipv4", "Ingress", "ICMP", nil, "0.0.0.0/0", "ingress",
			api.NetworkACLRule{Action: "allow", Source: "0.0.0.0/0", Protocol: "icmp4", Description: ruleId, State: "enabled"}},
	}
	for _, c := range cases {
		payload := CreatePayload{RuleId: ruleId, Direction: c.direction, Protocol: c.protocol, StartPort: c.startPort, Cidr: c.cidr}

		direction, rule, err := AclRule(payload)

		if err != nil || direction != c.wantDirection || rule != c.want {
			t.Errorf("%s: want %s %+v, got %s %+v (%v)", c.name, c.wantDirection, c.want, direction, rule, err)
		}
	}
}

func TestCreate_AddsTheRuleToTheNetworksAcl_AndReportsCreated(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := withAcl()

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), createLeased(), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	acl := p.ACLs[project+"/"+aclName]
	if len(acl.Ingress) != 1 || acl.Ingress[0].Description != ruleId || acl.Ingress[0].DestinationPort != "22" {
		t.Fatalf("ACL: %+v", acl.NetworkACLPut)
	}
	body := onlyBody(t, cp, ruleId)
	if body["report"] != reportCreated || body["externalRuleId"] != aclName+"/"+ruleId {
		t.Fatalf("report: %v", body)
	}
}

func TestCreate_TwoRules_BothStay(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := withAcl()
	create := NewCreate(cp.Client(), clusters(p))

	first := create.Run(context.Background(), createLeased(), testsupport.DiscardLogger())
	second := create.Run(context.Background(), leased("firewall-rule.create", createPayload(otherRule, "Egress", "UDP", 53, 53, "10.40.0.0/24")), testsupport.DiscardLogger())

	if first != nil || second != nil {
		t.Fatal(first, second)
	}
	acl := p.ACLs[project+"/"+aclName]
	if len(acl.Ingress) != 1 || len(acl.Egress) != 1 || acl.Egress[0].Description != otherRule {
		t.Fatalf("ACL: %+v", acl.NetworkACLPut)
	}
}

// A lease after a run that wrote the rule: it is there already, nothing is written twice.
func TestCreate_RuleAlreadyThere_WritesNothing_AndReportsCreated(t *testing.T) {
	logger, logs := testsupport.NewLogCapture(t)
	cp := testsupport.NewControlPlane(t)
	p := withAcl(api.NetworkACLRule{Action: "allow", Source: "10.40.0.0/24", Protocol: "tcp", DestinationPort: "22", Description: ruleId, State: "enabled"})

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), createLeased(), logger)

	if err != nil {
		t.Fatal(err)
	}
	if len(p.UpdatedACLs) != 0 {
		t.Fatalf("wrote again: %+v", p.UpdatedACLs)
	}
	if onlyBody(t, cp, ruleId)["report"] != reportCreated {
		t.Fatal("want Created")
	}
	if got := logs.Objects("already exists; treating it as ours"); len(got) != 1 {
		t.Fatalf("want 1 line(s) %q, got %v", "already exists; treating it as ours", got)
	}
}

// The ACL is made with its network; without it there is no default action to set, so the rule does not make one.
func TestCreate_NetworkHasNoAcl_IsPermanent(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), createLeased(), testsupport.DiscardLogger())

	if err == nil || command.IsRetryable(err) {
		t.Fatalf("want permanent, got %v", err)
	}
	if len(p.CreatedACLs) != 0 || len(cp.Requests()) != 0 {
		t.Fatal("made an ACL or reported")
	}
}

func TestCreate_AclChangedSinceTheRead_IsRetryable(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := withAcl()
	p.UpdateACLErr = api.StatusErrorf(http.StatusPreconditionFailed, "ETag doesn't match")

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), createLeased(), testsupport.DiscardLogger())

	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want retryable, got %v", err)
	}
	if len(cp.Requests()) != 0 {
		t.Fatal("reported")
	}
}

// Incus refusing the ACL write for any other reason is retried too; the command gives up after its last lease.
func TestCreate_AclWriteRefused_IsRetryable_AndReportsNothing(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := withAcl()
	p.UpdateACLErr = api.StatusErrorf(http.StatusInternalServerError, "injected firewall rule create failure")

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), createLeased(), testsupport.DiscardLogger())

	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want retryable, got %v", err)
	}
	if len(cp.Requests()) != 0 {
		t.Fatal("reported")
	}
}

func TestCreate_PayloadWithoutProject_IsPermanent(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := withAcl()
	payload := createPayload(ruleId, "Ingress", "TCP", 22, 22, "10.40.0.0/24")
	delete(payload, "projectName")

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), leased("firewall-rule.create", payload), testsupport.DiscardLogger())

	if err == nil || command.IsRetryable(err) {
		t.Fatalf("want permanent, got %v", err)
	}
	if p.Calls != 0 {
		t.Fatal("touched Incus")
	}
}

func TestCreate_GiveUp_ReportsRequestFailed_WithTheIdWhenTheRuleIsThere(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := withAcl(api.NetworkACLRule{Action: "allow", Description: ruleId, State: "enabled"})

	err := NewCreate(cp.Client(), clusters(p)).GiveUp(context.Background(), createLeased(), errors.New("gave up"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body := onlyBody(t, cp, ruleId)
	if body["report"] != reportRequestFailed || body["externalRuleId"] != aclName+"/"+ruleId ||
		body["reason"] != reasonCreateFailed || body["detail"] != "gave up" {
		t.Fatalf("report: %v", body)
	}
}

func TestCreate_GiveUp_RuleNotThere_ReportsWithoutAnId(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := withAcl()

	err := NewCreate(cp.Client(), clusters(p)).GiveUp(context.Background(), createLeased(), errors.New("gave up"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body := onlyBody(t, cp, ruleId)
	if body["report"] != reportRequestFailed || body["externalRuleId"] != nil || body["reason"] != reasonCreateFailed {
		t.Fatalf("report: %v", body)
	}
}

func TestDelete_RemovesOnlyThatRule_AndReportsDeleted(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := withAcl(
		api.NetworkACLRule{Action: "allow", Description: ruleId, State: "enabled"},
		api.NetworkACLRule{Action: "allow", Description: otherRule, State: "enabled"},
	)

	err := NewDelete(cp.Client(), clusters(p)).Run(context.Background(), deleteLeased(), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	acl := p.ACLs[project+"/"+aclName]
	if len(acl.Ingress) != 1 || acl.Ingress[0].Description != otherRule {
		t.Fatalf("ACL: %+v", acl.Ingress)
	}
	if onlyBody(t, cp, ruleId)["report"] != reportDeleted {
		t.Fatal("want Deleted")
	}
}

func TestDelete_RuleOrAclGone_ReportsDeleted_WithoutWriting(t *testing.T) {
	for name, p := range map[string]*testsupport.Provider{"rule gone": withAcl(), "ACL gone": testsupport.NewProvider()} {
		cp := testsupport.NewControlPlane(t)
		logger, logs := testsupport.NewLogCapture(t)

		err := NewDelete(cp.Client(), clusters(p)).Run(context.Background(), deleteLeased(), logger)

		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(p.UpdatedACLs) != 0 {
			t.Fatalf("%s: wrote %+v", name, p.UpdatedACLs)
		}
		if onlyBody(t, cp, ruleId)["report"] != reportDeleted {
			t.Fatalf("%s: want Deleted", name)
		}
		if got := logs.Objects("already gone; nothing to delete"); len(got) != 1 {
			t.Fatalf("%v: want one line %q, got %v", name, "already gone; nothing to delete", got)
		}
	}
}

func TestDelete_WriteFails_DoesNotReportDeleted(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := withAcl(api.NetworkACLRule{Action: "allow", Description: ruleId, State: "enabled"})
	p.UpdateACLErr = errors.New("boom")

	err := NewDelete(cp.Client(), clusters(p)).Run(context.Background(), deleteLeased(), testsupport.DiscardLogger())

	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want retryable, got %v", err)
	}
	if len(cp.Requests()) != 0 {
		t.Fatal("reported")
	}
}

func TestDelete_BadProviderId_IsPermanent(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := withAcl()
	payload := map[string]any{"ruleId": ruleId, "networkId": networkId, "externalRuleId": "no-slash", "projectName": project}

	err := NewDelete(cp.Client(), clusters(p)).Run(context.Background(), leased("firewall-rule.delete", payload), testsupport.DiscardLogger())

	if err == nil || command.IsRetryable(err) {
		t.Fatalf("want permanent, got %v", err)
	}
}

func TestDelete_GiveUp_ReportsRequestFailed(t *testing.T) {
	cp := testsupport.NewControlPlane(t)

	err := NewDelete(cp.Client(), cluster.Fixed{}).GiveUp(context.Background(), deleteLeased(), errors.New("refused"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body := onlyBody(t, cp, ruleId)
	if body["report"] != reportRequestFailed || body["reason"] != reasonDeleteFailed || body["detail"] != "refused" {
		t.Fatalf("report: %v", body)
	}
}

func TestDecideReport(t *testing.T) {
	enabled := "enabled"
	disabled := "disabled"
	stateById := map[string]string{aclName + "/" + ruleId: "enabled", aclName + "/" + otherRule: "logged"}
	id := aclName + "/" + ruleId
	cases := []struct {
		name       string
		row        ReconcilableRule
		send       bool
		wantExists bool
		wantState  string
	}{
		{"first seen after create", ReconcilableRule{ExternalId: id, CurrentExistence: "Exists"}, true, true, "enabled"},
		{"unchanged", ReconcilableRule{ExternalId: id, CurrentState: &enabled, CurrentExistence: "Exists"}, false, true, "enabled"},
		{"state changed outside Titan", ReconcilableRule{ExternalId: id, CurrentState: &disabled, CurrentExistence: "Exists"}, true, true, "enabled"},
		{"logged sent as Incus spells it", ReconcilableRule{ExternalId: aclName + "/" + otherRule, CurrentState: &enabled, CurrentExistence: "Exists"}, true, true, "logged"},
		{"back after not found", ReconcilableRule{ExternalId: id, CurrentState: nil, CurrentExistence: "NotFound"}, true, true, "enabled"},
		{"gone", ReconcilableRule{ExternalId: aclName + "/x", CurrentState: &enabled, CurrentExistence: "Exists"}, true, false, ""},
		{"gone and recorded", ReconcilableRule{ExternalId: aclName + "/x", CurrentExistence: "NotFound"}, false, false, ""},
	}
	for _, c := range cases {
		got, send := DecideReport(c.row, stateById)
		if send != c.send {
			t.Errorf("%s: want send %v, got %v", c.name, c.send, send)
			continue
		}
		if !send {
			continue
		}
		isRightReport := got.Report == reportObserved && got.Exists != nil && *got.Exists == c.wantExists && got.State == c.wantState
		if !isRightReport {
			t.Errorf("%s: got %+v", c.name, got)
		}
	}
}

func TestReconcile_SendsTheIncusStateAsIs_AndGoneAsNotFound(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, "/internal/reconcile/firewall-rules?clusterId=mock", http.StatusOK,
		`[{"id":"`+ruleId+`","networkId":"`+networkId+`","externalId":"`+aclName+`/`+ruleId+`","currentState":null,"currentExistence":"Exists"},`+
			`{"id":"`+otherRule+`","networkId":"`+networkId+`","externalId":"`+aclName+`/`+otherRule+`","currentState":"enabled","currentExistence":"Exists"}]`)
	p := withAcl(api.NetworkACLRule{Action: "allow", Description: ruleId, State: "logged"})
	logger, logs := testsupport.NewLogCapture(t)

	err := Reconcile(context.Background(), cp.Client(), p, "mock", logger)

	if err != nil {
		t.Fatal(err)
	}
	present := onlyBody(t, cp, ruleId)
	if present["report"] != reportObserved || present["exists"] != true || present["state"] != "logged" {
		t.Fatalf("present rule: %v", present)
	}
	gone := onlyBody(t, cp, otherRule)
	if gone["report"] != reportObserved || gone["exists"] != false || gone["state"] != nil {
		t.Fatalf("gone rule: %v", gone)
	}
	lines := map[any]map[string]any{}
	for _, sent := range logs.Records("reconcile report sent") {
		lines[sent["ruleId"]] = sent
	}
	isPresentLogged := lines[ruleId]["recordedState"] == "" && lines[ruleId]["state"] == "logged" && lines[ruleId]["exists"] == true
	isGoneLogged := lines[otherRule]["recordedState"] == "enabled" && lines[otherRule]["exists"] == false
	if len(lines) != 2 || !isPresentLogged || !isGoneLogged {
		t.Fatalf("report lines: %v", lines)
	}
}

// The reconciler never reports Deleted (titan-ops architecture/desired-and-current.md#existence-axis): even a rule whose
// delete waits is only not found; the delete command confirms it.
func TestReconcile_RuleGone_NeverReportsDeleted(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, "/internal/reconcile/firewall-rules?clusterId=mock", http.StatusOK,
		`[{"id":"`+ruleId+`","networkId":"`+networkId+`","externalId":"`+aclName+`/`+ruleId+`","currentState":"enabled","currentExistence":"Exists"}]`)
	p := withAcl()

	err := Reconcile(context.Background(), cp.Client(), p, "mock", testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if onlyBody(t, cp, ruleId)["report"] != reportObserved {
		t.Fatal("want Observed, never Deleted")
	}
}

// A failed read is not "no rule": reading it so would report every rule gone.
func TestReconcile_AclReadFails_ReportsNothing(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, "/internal/reconcile/firewall-rules?clusterId=mock", http.StatusOK,
		`[{"id":"`+ruleId+`","networkId":"`+networkId+`","externalId":"`+aclName+`/`+ruleId+`","currentState":"enabled","currentExistence":"Exists"}]`)
	p := testsupport.NewProvider()
	p.ListACLsErr = errors.New("boom")

	err := Reconcile(context.Background(), cp.Client(), p, "mock", slog.New(slog.NewTextHandler(io.Discard, nil)))

	if err == nil {
		t.Fatal("want the error")
	}
	if len(cp.RequestsTo("PATCH /internal/networks/"+networkId+"/firewall-rules/"+ruleId)) != 0 {
		t.Fatal("reported")
	}
}

func intOf(value int) *int {
	return &value
}
