package loadbalancer

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
	loadBalancerId = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	intentId       = "11111111-2222-3333-4444-555555555555"
	project        = "t-0123456789abcdef0123456789abcdef"
	clusterId      = "local-mac"
	network        = "net-99999999-8888-7777-6666-555555555555"
	subnet         = "2001:db8:42:101::/64"
	vip            = "2001:db8:42:101::1:1"
	vmA            = "vmaaaaaaaaaaaaaaaaaaaaaaaaaa"
	vmB            = "vmbbbbbbbbbbbbbbbbbbbbbbbbbb"
	// Measured on local-mac (NETWORK-LOAD-BALANCER Stage 0, M-L4): the guest of this MAC uses this address.
	macA     = "10:66:6a:ed:72:93"
	addressA = "2001:db8:42:101:1266:6aff:feed:7293"
	macB     = "10:66:6a:20:d3:c6"
	addressB = "2001:db8:42:101:1266:6aff:fe20:d3c6"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

func leased(kind string, payload map[string]any) command.Leased {
	encoded, _ := json.Marshal(payload)
	return command.Leased{Kind: kind, ClusterId: clusterId, Payload: encoded, Attempt: 1, MaxAttempts: 5}
}

func specFields() map[string]any {
	return map[string]any{
		"loadBalancerId": loadBalancerId, "projectName": project, "externalNetworkId": network,
		"publicIpv6Subnet": subnet, "vip": vip,
		"listeners": []map[string]any{
			{"protocol": "tcp", "port": 80, "targetPort": 8080},
			{"protocol": "udp", "port": 53, "targetPort": 53},
		},
		"backends": []map[string]any{
			{"vmId": "a", "externalVmId": vmA},
			{"vmId": "b", "externalVmId": vmB},
		},
	}
}

func updateFields() map[string]any {
	payload := specFields()
	payload["intentId"] = intentId
	return payload
}

func deleteFields() map[string]any {
	return map[string]any{
		"loadBalancerId": loadBalancerId, "projectName": project, "externalNetworkId": network, "vip": vip,
	}
}

func withVms() *testsupport.Provider {
	p := testsupport.NewProvider()
	p.Instances[project+"/"+vmA] = &api.Instance{Name: vmA, InstancePut: api.InstancePut{Config: map[string]string{"volatile.eth0.hwaddr": macA}}}
	p.Instances[project+"/"+vmB] = &api.Instance{Name: vmB, InstancePut: api.InstancePut{Config: map[string]string{"volatile.eth0.hwaddr": macB}}}
	return p
}

func clusters(p *testsupport.Provider) cluster.Fixed {
	return cluster.Fixed{clusterId: {Provider: p}}
}

func reports(t *testing.T, cp *testsupport.ControlPlane, id string) []map[string]any {
	t.Helper()
	bodies := []map[string]any{}
	for _, request := range cp.RequestsTo("PATCH /internal/load-balancers/" + id + "/status") {
		var body map[string]any
		if err := json.Unmarshal([]byte(request.Body), &body); err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, body)
	}
	return bodies
}

func onlyReport(t *testing.T, cp *testsupport.ControlPlane) map[string]any {
	t.Helper()
	all := reports(t, cp, loadBalancerId)
	if len(all) != 1 {
		t.Fatalf("want one report, got %+v", cp.Requests())
	}
	return all[0]
}

// ---- the Incus shape (titan-ops features/load-balancer/design.md#lb-resource-model)

func TestEui64_IsTheAddressTheGuestOfThatMacUses(t *testing.T) {
	got, err := Eui64(subnet, macA)
	if err != nil {
		t.Fatal(err)
	}
	if got != addressA {
		t.Fatalf("got %s, want %s (measured)", got, addressA)
	}
	if _, err := Eui64(subnet, "not-a-mac"); err == nil {
		t.Fatal("a bad MAC must be refused")
	}
	if _, err := Eui64("2001:db8:42:101::/56", macA); err == nil {
		t.Fatal("only a /64 has room for an EUI-64 interface id")
	}
}

func TestCreate_PostsOneBackendPerListenerAndVm_WithHealthCheck_AndReportsCreated(t *testing.T) {
	p := withVms()
	cp := testsupport.NewControlPlane(t)

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), leased("load-balancer.create", specFields()), discard)

	if err != nil {
		t.Fatal(err)
	}
	if len(p.CreatedLoadBalancers) != 1 {
		t.Fatalf("want one POST, got %+v", p.CreatedLoadBalancers)
	}
	posted := p.CreatedLoadBalancers[0]
	if posted.ListenAddress != vip || posted.Config["healthcheck"] != "true" {
		t.Fatalf("listen %q config %v", posted.ListenAddress, posted.Config)
	}
	want := []api.NetworkLoadBalancerBackend{
		{Name: "l0-" + vmA, TargetAddress: addressA, TargetPort: "8080"},
		{Name: "l0-" + vmB, TargetAddress: addressB, TargetPort: "8080"},
		{Name: "l1-" + vmA, TargetAddress: addressA, TargetPort: "53"},
		{Name: "l1-" + vmB, TargetAddress: addressB, TargetPort: "53"},
	}
	if len(posted.Backends) != len(want) {
		t.Fatalf("backends %+v", posted.Backends)
	}
	for i := range want {
		if posted.Backends[i] != want[i] {
			t.Fatalf("backend %d: got %+v, want %+v", i, posted.Backends[i], want[i])
		}
	}
	wantPorts := []api.NetworkLoadBalancerPort{
		{Protocol: "tcp", ListenPort: "80", TargetBackend: []string{"l0-" + vmA, "l0-" + vmB}},
		{Protocol: "udp", ListenPort: "53", TargetBackend: []string{"l1-" + vmA, "l1-" + vmB}},
	}
	if len(posted.Ports) != 2 {
		t.Fatalf("ports %+v", posted.Ports)
	}
	for i := range wantPorts {
		got := posted.Ports[i]
		if got.Protocol != wantPorts[i].Protocol || got.ListenPort != wantPorts[i].ListenPort ||
			len(got.TargetBackend) != 2 || got.TargetBackend[0] != wantPorts[i].TargetBackend[0] || got.TargetBackend[1] != wantPorts[i].TargetBackend[1] {
			t.Fatalf("port %d: got %+v, want %+v", i, got, wantPorts[i])
		}
	}
	report := onlyReport(t, cp)
	if report["report"] != "Created" || report["externalId"] != vip {
		t.Fatalf("report %v", report)
	}
}

// A second lease of the same create (the first one's report got lost) finds the VIP and writes it again.
func TestCreate_WhenIncusAlreadyHasTheVip_PutsIt_AndStillReportsCreated(t *testing.T) {
	p := withVms()
	p.PutLoadBalancer(project, network, api.NetworkLoadBalancer{ListenAddress: vip})
	cp := testsupport.NewControlPlane(t)

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), leased("load-balancer.create", specFields()), discard)

	if err != nil {
		t.Fatal(err)
	}
	if len(p.CreatedLoadBalancers) != 0 || len(p.UpdatedLoadBalancers) != 1 {
		t.Fatalf("created %d updated %d", len(p.CreatedLoadBalancers), len(p.UpdatedLoadBalancers))
	}
	if onlyReport(t, cp)["report"] != "Created" {
		t.Fatal("want Created")
	}
}

func TestCreate_ABackendWhoseInstanceIsGone_IsPermanent_AndWritesNothing(t *testing.T) {
	p := withVms()
	delete(p.Instances, project+"/"+vmB)
	cp := testsupport.NewControlPlane(t)

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), leased("load-balancer.create", specFields()), discard)

	if err == nil || command.IsRetryable(err) {
		t.Fatalf("want permanent, got %v", err)
	}
	if len(p.CreatedLoadBalancers)+len(p.UpdatedLoadBalancers) != 0 {
		t.Fatal("nothing must reach Incus")
	}
}

// Incus sets volatile.eth0.hwaddr when it creates the instance; without it there is no address to send to yet.
func TestCreate_ABackendWithNoMacYet_IsRetryable(t *testing.T) {
	p := withVms()
	p.Instances[project+"/"+vmB].Config = map[string]string{}
	cp := testsupport.NewControlPlane(t)

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), leased("load-balancer.create", specFields()), discard)

	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want a retryable error, got %v", err)
	}
}

func TestCreate_APayloadMissingTheVip_IsPermanent_AndTouchesNothing(t *testing.T) {
	p := withVms()
	payload := specFields()
	delete(payload, "vip")
	cp := testsupport.NewControlPlane(t)

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), leased("load-balancer.create", payload), discard)

	if err == nil || command.IsRetryable(err) || p.Calls != 0 {
		t.Fatalf("err %v, provider calls %d", err, p.Calls)
	}
}

func TestCreate_GiveUp_WithTheVipOnIncus_SaysTheCreateHappened(t *testing.T) {
	p := withVms()
	p.PutLoadBalancer(project, network, api.NetworkLoadBalancer{ListenAddress: vip})
	cp := testsupport.NewControlPlane(t)

	err := NewCreate(cp.Client(), clusters(p)).GiveUp(context.Background(), leased("load-balancer.create", specFields()), errors.New("boom"), discard)

	if err != nil {
		t.Fatal(err)
	}
	report := onlyReport(t, cp)
	if report["report"] != "RequestFailed" || report["externalId"] != vip || report["detail"] != "boom" {
		t.Fatalf("report %v", report)
	}
}

func TestCreate_GiveUp_WithNothingOnIncus_SendsNoId(t *testing.T) {
	p := withVms()
	cp := testsupport.NewControlPlane(t)

	err := NewCreate(cp.Client(), clusters(p)).GiveUp(context.Background(), leased("load-balancer.create", specFields()), errors.New("boom"), discard)

	if err != nil {
		t.Fatal(err)
	}
	if _, carriesAnId := onlyReport(t, cp)["externalId"]; carriesAnId {
		t.Fatal("no VIP on Incus: no id")
	}
}

// ---- update

func TestUpdate_PutsTheWholeSpec_AndReportsUpdatedWithItsIntent(t *testing.T) {
	p := withVms()
	p.PutLoadBalancer(project, network, api.NetworkLoadBalancer{ListenAddress: vip})
	cp := testsupport.NewControlPlane(t)

	err := NewUpdate(cp.Client(), clusters(p)).Run(context.Background(), leased("load-balancer.update", updateFields()), discard)

	if err != nil {
		t.Fatal(err)
	}
	if len(p.UpdatedLoadBalancers) != 1 || len(p.UpdatedLoadBalancers[0].Backends) != 4 {
		t.Fatalf("updated %+v", p.UpdatedLoadBalancers)
	}
	report := onlyReport(t, cp)
	if report["report"] != "Updated" || report["intentId"] != intentId {
		t.Fatalf("report %v", report)
	}
}

// Incus lost the VIP (drift): the update makes it again rather than fail for ever.
func TestUpdate_WhenTheVipIsGone_CreatesIt(t *testing.T) {
	p := withVms()
	cp := testsupport.NewControlPlane(t)

	err := NewUpdate(cp.Client(), clusters(p)).Run(context.Background(), leased("load-balancer.update", updateFields()), discard)

	if err != nil {
		t.Fatal(err)
	}
	if len(p.CreatedLoadBalancers) != 1 || onlyReport(t, cp)["report"] != "Updated" {
		t.Fatalf("created %+v, requests %+v", p.CreatedLoadBalancers, cp.Requests())
	}
}

func TestUpdate_WithoutAnIntent_IsPermanent(t *testing.T) {
	p := withVms()
	cp := testsupport.NewControlPlane(t)

	err := NewUpdate(cp.Client(), clusters(p)).Run(context.Background(), leased("load-balancer.update", specFields()), discard)

	if err == nil || command.IsRetryable(err) {
		t.Fatalf("want permanent, got %v", err)
	}
}

func TestUpdate_GiveUp_NamesItsIntent(t *testing.T) {
	p := withVms()
	cp := testsupport.NewControlPlane(t)

	err := NewUpdate(cp.Client(), clusters(p)).GiveUp(context.Background(), leased("load-balancer.update", updateFields()), errors.New("boom"), discard)

	if err != nil {
		t.Fatal(err)
	}
	report := onlyReport(t, cp)
	if report["report"] != "RequestFailed" || report["intentId"] != intentId || report["reason"] != reasonUpdateFailed {
		t.Fatalf("report %v", report)
	}
}

// ---- delete

func TestDelete_RemovesTheVip_AndReportsDeleted(t *testing.T) {
	p := withVms()
	p.PutLoadBalancer(project, network, api.NetworkLoadBalancer{ListenAddress: vip})
	cp := testsupport.NewControlPlane(t)

	err := NewDelete(cp.Client(), clusters(p)).Run(context.Background(), leased("load-balancer.delete", deleteFields()), discard)

	if err != nil {
		t.Fatal(err)
	}
	if len(p.LoadBalancers) != 0 || onlyReport(t, cp)["report"] != "Deleted" {
		t.Fatalf("left %v, requests %+v", p.LoadBalancers, cp.Requests())
	}
}

func TestDelete_AlreadyGone_StillReportsDeleted(t *testing.T) {
	p := withVms()
	cp := testsupport.NewControlPlane(t)

	err := NewDelete(cp.Client(), clusters(p)).Run(context.Background(), leased("load-balancer.delete", deleteFields()), discard)

	if err != nil || onlyReport(t, cp)["report"] != "Deleted" {
		t.Fatalf("err %v requests %+v", err, cp.Requests())
	}
}

func TestDelete_IncusRefuses_ReportsNothing_AndIsRetried(t *testing.T) {
	p := withVms()
	p.PutLoadBalancer(project, network, api.NetworkLoadBalancer{ListenAddress: vip})
	p.DeleteLoadBalancerErr = api.StatusErrorf(http.StatusInternalServerError, "busy")
	cp := testsupport.NewControlPlane(t)

	err := NewDelete(cp.Client(), clusters(p)).Run(context.Background(), leased("load-balancer.delete", deleteFields()), discard)

	if err == nil || !command.IsRetryable(err) || len(reports(t, cp, loadBalancerId)) != 0 {
		t.Fatalf("err %v, requests %+v", err, cp.Requests())
	}
}

func TestDelete_GiveUp_ReportsTheFailure(t *testing.T) {
	cp := testsupport.NewControlPlane(t)

	err := NewDelete(cp.Client(), clusters(withVms())).GiveUp(context.Background(), leased("load-balancer.delete", deleteFields()), errors.New("boom"), discard)

	if err != nil {
		t.Fatal(err)
	}
	report := onlyReport(t, cp)
	if report["report"] != "RequestFailed" || report["reason"] != reasonDeleteFailed {
		t.Fatalf("report %v", report)
	}
}

// ---- reconciler

func TestReconcile_ReportsOnlyWhatDiffers_AbsentIsNotFound(t *testing.T) {
	const (
		kept     = "00000000-0000-0000-0000-000000000001"
		vanished = "00000000-0000-0000-0000-000000000002"
		back     = "00000000-0000-0000-0000-000000000003"
		orphaned = "00000000-0000-0000-0000-000000000004"
		other    = "net-other"
		gone     = "net-gone"
	)
	p := testsupport.NewProvider()
	p.PutLoadBalancer(project, network, api.NetworkLoadBalancer{ListenAddress: "2001:db8:42:101::1:1"})
	p.PutLoadBalancer(project, other, api.NetworkLoadBalancer{ListenAddress: "2001:db8:42:102::1:1"})
	p.MissingNetworks[project+"/"+gone] = true
	cp := testsupport.NewControlPlane(t)
	rows := []ReconcilableLoadBalancer{
		{Id: kept, ProjectName: project, ExternalNetworkId: network, Vip: "2001:db8:42:101::1:1", CurrentExistence: "Exists"},
		{Id: vanished, ProjectName: project, ExternalNetworkId: network, Vip: "2001:db8:42:101::1:2", CurrentExistence: "Exists"},
		{Id: back, ProjectName: project, ExternalNetworkId: other, Vip: "2001:db8:42:102::1:1", CurrentExistence: "NotFound"},
		{Id: orphaned, ProjectName: project, ExternalNetworkId: gone, Vip: "2001:db8:42:103::1:1", CurrentExistence: "Exists"},
	}
	encoded, _ := json.Marshal(rows)
	cp.Respond("GET", "/internal/reconcile/load-balancers?clusterId="+clusterId, http.StatusOK, string(encoded))

	if err := Reconcile(context.Background(), cp.Client(), p, clusterId, discard); err != nil {
		t.Fatal(err)
	}

	if len(reports(t, cp, kept)) != 0 {
		t.Fatal("unchanged: no report")
	}
	for id, want := range map[string]bool{vanished: false, back: true, orphaned: false} {
		got := reports(t, cp, id)
		if len(got) != 1 || got[0]["report"] != "Observed" || got[0]["exists"] != want {
			t.Fatalf("%s: got %v, want exists=%v", id, got, want)
		}
	}
}

// A failed read is not "nothing there": reporting it so would mark every load balancer of the network NotFound.
func TestReconcile_AFailedListOfOneNetwork_ReportsNothingForIt(t *testing.T) {
	p := testsupport.NewProvider()
	p.ListLoadBalancersErr = api.StatusErrorf(http.StatusInternalServerError, "busy")
	cp := testsupport.NewControlPlane(t)
	rows := []ReconcilableLoadBalancer{{Id: loadBalancerId, ProjectName: project, ExternalNetworkId: network, Vip: vip, CurrentExistence: "Exists"}}
	encoded, _ := json.Marshal(rows)
	cp.Respond("GET", "/internal/reconcile/load-balancers?clusterId="+clusterId, http.StatusOK, string(encoded))

	if err := Reconcile(context.Background(), cp.Client(), p, clusterId, discard); err != nil {
		t.Fatal(err)
	}
	if len(reports(t, cp, loadBalancerId)) != 0 {
		t.Fatalf("requests %+v", cp.Requests())
	}
}
