package network

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

const (
	networkId = "11111111-2222-3333-4444-555555555555"
	project   = "t-0123456789abcdef0123456789abcdef"
	clusterId = "mock"
)

func leased(kind string, payload map[string]any) command.Leased {
	encoded, _ := json.Marshal(payload)
	return command.Leased{Kind: kind, ClusterId: clusterId, Payload: encoded, Attempt: 1, MaxAttempts: 5}
}

func createLeased(cidr string) command.Leased {
	return createLeasedWithIpv6(cidr, "", "")
}

func createLeasedWithIpv6(cidr string, subnet string, router string) command.Leased {
	return leased("network.create", map[string]any{
		"networkId": networkId, "name": "backend", "projectName": project, "cidr": cidr,
		"defaultEgressPolicy": "Allow", "publicIpv6Subnet": subnet, "publicIpv6RouterAddress": router,
	})
}

const (
	ipv6Subnet = "2001:41d0:d00:d470::/64"
	ipv6Router = "2001:41d0:d00:d400::170"
)

// ipv6Network is the network as Incus holds it after a create with public IPv6.
func ipv6Network(config map[string]string) api.Network {
	held := map[string]string{
		"network": "ovnup", "ipv4.address": "10.40.0.1/24", "ipv4.nat": "false",
		"ipv6.address": "2001:41d0:d00:d470::1/64", "ipv6.nat": "false",
		"volatile.network.ipv6.address":        ipv6Router,
		"security.acls":                        "acl-" + networkId,
		"security.acls.default.ingress.action": "allow", "security.acls.default.egress.action": "allow",
	}
	for key, value := range config {
		held[key] = value
	}
	return api.Network{Name: "net-" + networkId, Project: project, Status: "Created", NetworkPut: api.NetworkPut{Config: held}}
}

func ovnUplinkCluster(p *testsupport.Provider, uplink string) cluster.Fixed {
	return cluster.Fixed{clusterId: {Provider: p, OvnUplinkNetwork: uplink}}
}

func onlyBody(t *testing.T, cp *testsupport.ControlPlane) map[string]any {
	t.Helper()
	requests := cp.RequestsTo("PATCH /internal/networks/" + networkId + "/status")
	if len(requests) != 1 {
		t.Fatalf("want one status report, got %+v", cp.Requests())
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(requests[0].Body), &body); err != nil {
		t.Fatal(err)
	}
	return body
}

func TestRouterAddress_IsTheFirstHostOfTheCidr(t *testing.T) {
	cases := map[string]Router{
		"10.40.0.0/24":   {Cidr: "10.40.0.1/24", Gateway: "10.40.0.1"},
		"172.20.0.0/16":  {Cidr: "172.20.0.1/16", Gateway: "172.20.0.1"},
		"192.168.8.0/22": {Cidr: "192.168.8.1/22", Gateway: "192.168.8.1"},
		"":               {Cidr: "auto", Gateway: ""},
	}
	for cidr, want := range cases {
		got, err := RouterAddress(cidr)
		if err != nil || got != want {
			t.Errorf("%q: want %+v, got %+v (%v)", cidr, want, got, err)
		}
	}
}

func TestRouterAddress_RefusesWhatCannotHoldARouterAndAVm(t *testing.T) {
	for _, cidr := range []string{"10.0.0.0/31", "10.0.0.0/32", "fd00::/64", "not-a-cidr"} {
		if _, err := RouterAddress(cidr); err == nil {
			t.Errorf("%q accepted", cidr)
		}
	}
}

func createLeasedWithPolicies(ingress string, egress string) command.Leased {
	payload := map[string]any{
		"networkId": networkId, "name": "backend", "projectName": project, "cidr": "10.40.0.0/24",
		"defaultEgressPolicy": egress,
	}
	if ingress != "" {
		payload["defaultIngressPolicy"] = ingress
	}
	return leased("network.create", payload)
}

// A network without public IPv6 has no uplink, even where the cluster has one: OVN keeps each one to itself and lets two
// tenants use the same CIDR (https://github.com/OpenViCloud/titan-ops/blob/main/docs/architecture/vm-network.md#no-ipv4-egress).
func TestCreate_MakesAnIsolatedOvnNetwork_WithItsAcl_AndReportsCreated(t *testing.T) {
	for _, uplink := range []string{"ovnup", ""} {
		cp := testsupport.NewControlPlane(t)
		p := testsupport.NewProvider()
		clusters := cluster.Fixed{clusterId: {Provider: p, OvnUplinkNetwork: uplink}}

		err := NewCreate(cp.Client(), clusters).Run(context.Background(), createLeased("10.40.0.0/24"), testsupport.DiscardLogger())

		if err != nil {
			t.Fatal(err)
		}
		created := p.CreatedNetworks[0]
		if created.Name != "net-"+networkId || created.Type != "ovn" {
			t.Fatalf("network: %+v", created)
		}
		want := map[string]string{
			"ipv4.address": "10.40.0.1/24", "ipv4.nat": "false", "ipv6.address": "none", "network": "none",
			"security.acls":                        "acl-" + networkId,
			"security.acls.default.ingress.action": "allow", "security.acls.default.egress.action": "allow",
		}
		if len(created.Config) != len(want) {
			t.Errorf("uplink %q: want exactly %v, got %v", uplink, want, created.Config)
		}
		for key, value := range want {
			if created.Config[key] != value {
				t.Errorf("uplink %q, %s: want %q, got %q", uplink, key, value, created.Config[key])
			}
		}
		wantOrder := []string{"create-acl " + project + "/acl-" + networkId, "create-network " + project + "/net-" + networkId}
		if strings.Join(p.Order, ",") != strings.Join(wantOrder, ",") {
			t.Fatalf("order: want %v, got %v", wantOrder, p.Order)
		}
		body := onlyBody(t, cp)
		if body["report"] != "Created" || body["externalId"] != "net-"+networkId || body["cidr"] != "10.40.0.0/24" || body["gateway"] != "10.40.0.1" {
			t.Fatalf("report: %v", body)
		}
	}
}

// A network with public IPv6 goes on the cluster's uplink and carries its /64; its router takes the address the static
// OVH route of that /64 points to (https://github.com/OpenViCloud/titan-ops/blob/main/docs/infra/incus/design.md#ipv6-ovn-uplink). IPv4 stays as on any other network.
func TestCreate_WithPublicIpv6_UsesTheClustersUplink_AndPinsTheRouter(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	// Incus may write an address its own way; the read-back compares addresses, not text.
	p.Networks = []api.Network{ipv6Network(map[string]string{"volatile.network.ipv6.address": "2001:41d0:0d00:d400:0:0:0:170"})}

	err := NewCreate(cp.Client(), ovnUplinkCluster(p, "ovnup")).Run(context.Background(), createLeasedWithIpv6("10.40.0.0/24", ipv6Subnet, ipv6Router), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"network": "ovnup", "ipv6.address": "2001:41d0:d00:d470::1/64", "ipv6.nat": "false",
		"volatile.network.ipv6.address": ipv6Router,
		"ipv4.address":                  "10.40.0.1/24", "ipv4.nat": "false",
		"security.acls":                        "acl-" + networkId,
		"security.acls.default.ingress.action": "allow", "security.acls.default.egress.action": "allow",
	}
	created := p.CreatedNetworks[0].Config
	if len(created) != len(want) {
		t.Fatalf("config: want %v, got %v", want, created)
	}
	for key, value := range want {
		if created[key] != value {
			t.Errorf("%s: want %q, got %q", key, value, created[key])
		}
	}
	body := onlyBody(t, cp)
	if body["report"] != "Created" || body["externalId"] != "net-"+networkId || body["cidr"] != "10.40.0.0/24" {
		t.Fatalf("report: %v", body)
	}
}

func TestCreate_WithPublicIpv6_ClusterWithoutUplink_IsPermanent_AndTouchesNothing(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	err := NewCreate(cp.Client(), ovnUplinkCluster(p, "")).Run(context.Background(), createLeasedWithIpv6("10.40.0.0/24", ipv6Subnet, ipv6Router), testsupport.DiscardLogger())

	if err == nil || command.IsRetryable(err) || !strings.Contains(err.Error(), "uplink") {
		t.Fatalf("want a permanent error naming the uplink, got %v", err)
	}
	if p.Calls != 0 {
		t.Fatal("touched Incus")
	}
}

func TestCreate_WithPublicIpv6_WhatCannotBeApplied_IsPermanent_AndTouchesNothing(t *testing.T) {
	cases := []struct {
		name   string
		subnet string
		router string
	}{
		{"subnet without router", ipv6Subnet, ""},
		{"router without subnet", "", ipv6Router},
		{"subnet not a prefix", "2001:41d0:d00:d470::", ipv6Router},
		{"subnet IPv4", "10.50.0.0/24", ipv6Router},
		{"subnet larger than a /64", "2001:41d0:d00:d400::/56", ipv6Router},
		{"subnet smaller than a /64", "2001:41d0:d00:d470::/80", ipv6Router},
		{"subnet is a host, not the /64", "2001:41d0:d00:d470::5/64", ipv6Router},
		{"subnet IPv4 in IPv6 form", "::ffff:0:0/64", ipv6Router},
		{"router not an address", ipv6Subnet, "router"},
		{"router IPv4", ipv6Subnet, "10.0.0.1"},
		{"router a prefix", ipv6Subnet, "2001:41d0:d00:d400::170/64"},
	}
	for _, c := range cases {
		cp := testsupport.NewControlPlane(t)
		p := testsupport.NewProvider()

		err := NewCreate(cp.Client(), ovnUplinkCluster(p, "ovnup")).Run(context.Background(), createLeasedWithIpv6("10.40.0.0/24", c.subnet, c.router), testsupport.DiscardLogger())

		if err == nil || command.IsRetryable(err) {
			t.Errorf("%s: want permanent, got %v", c.name, err)
		}
		if p.Calls != 0 {
			t.Errorf("%s: touched Incus", c.name)
		}
	}
}

// A router at another address gets nothing from the static route of the /64, and the network would look healthy: a
// read-back that differs is a permanent failure, not a network to report Created.
func TestCreate_WithPublicIpv6_IncusHoldsSomethingElse_IsPermanent(t *testing.T) {
	cases := map[string]map[string]string{
		"router elsewhere":       {"volatile.network.ipv6.address": "2001:41d0:d00:d400::171"},
		"no router address":      {"volatile.network.ipv6.address": ""},
		"another /64":            {"ipv6.address": "2001:41d0:d00:d471::1/64"},
		"no IPv6":                {"ipv6.address": "none"},
		"another uplink":         {"network": "uplink2"},
		"router address garbled": {"volatile.network.ipv6.address": "router"},
	}
	for name, held := range cases {
		cp := testsupport.NewControlPlane(t)
		p := testsupport.NewProvider()
		p.Networks = []api.Network{ipv6Network(held)}

		err := NewCreate(cp.Client(), ovnUplinkCluster(p, "ovnup")).Run(context.Background(), createLeasedWithIpv6("10.40.0.0/24", ipv6Subnet, ipv6Router), testsupport.DiscardLogger())

		if err == nil || command.IsRetryable(err) {
			t.Errorf("%s: want permanent, got %v", name, err)
		}
		if len(cp.Requests()) != 0 {
			t.Errorf("%s: reported %v", name, cp.Requests())
		}
		// Left in place, the reconciler would see it Created and VMs would land on a /64 nothing routes to.
		if len(p.DeletedNetworks) != 1 || p.DeletedNetworks[0] != project+"/net-"+networkId {
			t.Errorf("%s: want the wrong network deleted, deleted %v", name, p.DeletedNetworks)
		}
	}
}

// The wrong network could not be removed: try the whole create again rather than give up with it still there.
func TestCreate_WithPublicIpv6_WrongNetworkCannotBeDeleted_IsRetryable(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Networks = []api.Network{ipv6Network(map[string]string{"volatile.network.ipv6.address": "2001:41d0:d00:d400::171"})}
	p.DeleteNetworkErr = errors.New("incus unreachable")

	err := NewCreate(cp.Client(), ovnUplinkCluster(p, "ovnup")).Run(context.Background(), createLeasedWithIpv6("10.40.0.0/24", ipv6Subnet, ipv6Router), testsupport.DiscardLogger())

	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want retryable, got %v", err)
	}
}

// Whether Incus holds the network decides whether the control plane keeps or frees its /64: a list that failed
// answers neither, so the give-up is sent again by the next lease instead of guessing "absent".
func TestCreate_GiveUp_CannotListNetworks_IsRetryable_AndTellsNothing(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.PutACL(project, api.NetworkACL{NetworkACLPost: api.NetworkACLPost{Name: "acl-" + networkId}})
	p.ListNetworksErr = errors.New("incus unreachable")

	err := NewCreate(cp.Client(), cluster.Fixed{clusterId: {Provider: p}}).GiveUp(context.Background(), createLeased("10.40.0.0/24"), errors.New("gave up"), testsupport.DiscardLogger())

	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want retryable, got %v", err)
	}
	if len(cp.Requests()) != 0 {
		t.Fatalf("reported %v", cp.Requests())
	}
	if len(p.DeletedACLs) != 0 {
		t.Fatal("deleted the ACL without knowing whether the network exists")
	}
}

func TestCreate_WithPublicIpv6_NetworkReadFails_IsRetryable(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	err := NewCreate(cp.Client(), ovnUplinkCluster(p, "ovnup")).Run(context.Background(), createLeasedWithIpv6("10.40.0.0/24", ipv6Subnet, ipv6Router), testsupport.DiscardLogger())

	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want retryable, got %v", err)
	}
	if len(cp.Requests()) != 0 {
		t.Fatalf("reported %v", cp.Requests())
	}
}

// A lease after a run that made the network: it is ours, its ACL keys are set back, and its IPv6 is checked like a
// fresh one's.
func TestCreate_WithPublicIpv6_AlreadyOurs_IsCheckedToo(t *testing.T) {
	cases := []struct {
		name    string
		held    map[string]string
		wantErr bool
	}{
		{"as asked", nil, false},
		{"router elsewhere", map[string]string{"volatile.network.ipv6.address": "2001:41d0:d00:d400::171"}, true},
	}
	for _, c := range cases {
		cp := testsupport.NewControlPlane(t)
		p := testsupport.NewProvider()
		p.Networks = []api.Network{ipv6Network(c.held)}
		p.CreateNetworkErr = api.StatusErrorf(http.StatusConflict, `Network "net-%s" already exists`, networkId)

		err := NewCreate(cp.Client(), ovnUplinkCluster(p, "ovnup")).Run(context.Background(), createLeasedWithIpv6("10.40.0.0/24", ipv6Subnet, ipv6Router), testsupport.DiscardLogger())

		if c.wantErr {
			if err == nil || command.IsRetryable(err) {
				t.Errorf("%s: want permanent, got %v", c.name, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if len(p.UpdatedNetworks) != 0 {
			t.Errorf("%s: rewrote a network that was right: %+v", c.name, p.UpdatedNetworks)
		}
		if onlyBody(t, cp)["report"] != "Created" {
			t.Errorf("%s: want Created", c.name)
		}
	}
}

// On an isolated network every packet is one VM's egress and another's ingress, and Incus applies only the egress
// default between them (https://github.com/OpenViCloud/titan-ops/blob/main/docs/features/network/design.md#firewall-default-policy): either Deny blocks, both Allow lets
// through.
func TestCreate_MergesTheTwoPolicies(t *testing.T) {
	cases := []struct {
		ingress string
		egress  string
		want    string
	}{
		{"Allow", "Allow", "allow"},
		{"Deny", "Allow", "drop"},
		{"Allow", "Deny", "drop"},
		{"Deny", "Deny", "drop"},
		{"", "Allow", "allow"},
		{"", "Deny", "drop"},
	}
	for _, c := range cases {
		cp := testsupport.NewControlPlane(t)
		p := testsupport.NewProvider()

		err := NewCreate(cp.Client(), cluster.Fixed{clusterId: {Provider: p}}).Run(context.Background(), createLeasedWithPolicies(c.ingress, c.egress), testsupport.DiscardLogger())

		if err != nil {
			t.Fatal(err)
		}
		config := p.CreatedNetworks[0].Config
		gotIngress := config["security.acls.default.ingress.action"]
		gotEgress := config["security.acls.default.egress.action"]
		if gotIngress != c.want || gotEgress != c.want {
			t.Errorf("ingress %q egress %q: want %q/%q, got %q/%q", c.ingress, c.egress, c.want, c.want, gotIngress, gotEgress)
		}
	}
}

func TestCreate_UnknownPolicy_IsPermanent(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	err := NewCreate(cp.Client(), cluster.Fixed{clusterId: {Provider: p}}).Run(context.Background(), createLeasedWithPolicies("Maybe", "Allow"), testsupport.DiscardLogger())

	if err == nil || command.IsRetryable(err) {
		t.Fatalf("want permanent, got %v", err)
	}
	if p.Calls != 0 {
		t.Fatal("touched Incus")
	}
}

// The tenant chose no CIDR: Incus picks one ("auto") and Titan must record the subnet the network really has.
func TestCreate_WithoutACidr_ReportsTheSubnetIncusPicked(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Networks = []api.Network{{Name: "net-" + networkId, Project: project, NetworkPut: api.NetworkPut{Config: map[string]string{"ipv4.address": "10.101.0.1/24"}}}}

	err := NewCreate(cp.Client(), cluster.Fixed{clusterId: {Provider: p}}).Run(context.Background(), createLeased(""), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if p.CreatedNetworks[0].Config["ipv4.address"] != "auto" {
		t.Fatalf("asked for %q, want auto", p.CreatedNetworks[0].Config["ipv4.address"])
	}
	body := onlyBody(t, cp)
	if body["cidr"] != "10.101.0.0/24" || body["gateway"] != "10.101.0.1" {
		t.Fatalf("report: %v", body)
	}
}

func TestCreate_GiveUp_WithAnIncusChosenSubnet_ReportsThatSubnet(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Networks = []api.Network{{Name: "net-" + networkId, Project: project, NetworkPut: api.NetworkPut{Config: map[string]string{"ipv4.address": "10.102.0.1/24"}}}}

	err := NewCreate(cp.Client(), cluster.Fixed{clusterId: {Provider: p}}).GiveUp(context.Background(), createLeased(""), errors.New("gave up"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body := onlyBody(t, cp)
	if body["report"] != "RequestFailed" || body["cidr"] != "10.102.0.0/24" || body["gateway"] != "10.102.0.1" ||
		body["reason"] != reasonCreateFailed || body["detail"] != "gave up" {
		t.Fatalf("report: %v", body)
	}
}

// A create that gives up before its network exists leaves no ACL behind: the control plane deletes such a network
// locally, so no network.delete would ever come for it.
func TestCreate_GiveUp_WithoutTheNetwork_DeletesTheAcl(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.PutACL(project, api.NetworkACL{NetworkACLPost: api.NetworkACLPost{Name: "acl-" + networkId}})

	err := NewCreate(cp.Client(), cluster.Fixed{clusterId: {Provider: p}}).GiveUp(context.Background(), createLeased("10.40.0.0/24"), errors.New("gave up"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if len(p.ACLs) != 0 {
		t.Fatal("ACL left behind")
	}
	if onlyBody(t, cp)["report"] != "RequestFailed" {
		t.Fatal("want RequestFailed")
	}
}

func TestCreate_GiveUp_WithTheNetwork_KeepsTheAcl(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.PutACL(project, api.NetworkACL{NetworkACLPost: api.NetworkACLPost{Name: "acl-" + networkId}})
	p.Networks = []api.Network{{Name: "net-" + networkId, Project: project, NetworkPut: api.NetworkPut{Config: map[string]string{"ipv4.address": "10.40.0.1/24"}}}}

	err := NewCreate(cp.Client(), cluster.Fixed{clusterId: {Provider: p}}).GiveUp(context.Background(), createLeased("10.40.0.0/24"), errors.New("gave up"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if len(p.ACLs) != 1 || len(p.DeletedACLs) != 0 {
		t.Fatal("deleted the ACL of a network that exists")
	}
	if onlyBody(t, cp)["externalId"] != "net-"+networkId {
		t.Fatal("want the network's id")
	}
}

func TestSubnetOf(t *testing.T) {
	subnet, ok := SubnetOf("10.40.7.1/16")
	if !ok || subnet.Cidr != "10.40.0.0/16" || subnet.Gateway != "10.40.7.1" {
		t.Fatalf("got %+v %v", subnet, ok)
	}
	for _, notASubnet := range []string{"auto", "none", "", "fd42::1/64"} {
		if _, ok := SubnetOf(notASubnet); ok {
			t.Errorf("%q read as a subnet", notASubnet)
		}
	}
}

// A lease after a run that got as far as the network: both "already exists" answers are ours, and the network is set back to what this
// command wants if it differs.
func TestCreate_NetworkAndAclExist_IsOurs_AndTheNetworkGetsItsAclSettings(t *testing.T) {
	logger, logs := testsupport.NewLogCapture(t)
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.PutACL(project, api.NetworkACL{NetworkACLPost: api.NetworkACLPost{Name: "acl-" + networkId}})
	p.Networks = []api.Network{{Name: "net-" + networkId, Project: project, NetworkPut: api.NetworkPut{
		Description: "backend",
		Config:      map[string]string{"ipv4.address": "10.40.0.1/24", "network": "none"},
	}}}
	p.CreateNetworkErr = api.StatusErrorf(http.StatusConflict, `Network "net-%s" already exists`, networkId)

	err := NewCreate(cp.Client(), cluster.Fixed{clusterId: {Provider: p}}).Run(context.Background(), createLeasedWithPolicies("Deny", "Allow"), logger)

	if err != nil {
		t.Fatal(err)
	}
	if len(p.UpdatedNetworks) != 1 {
		t.Fatalf("want one network update, got %d", len(p.UpdatedNetworks))
	}
	updated := p.UpdatedNetworks[0]
	if updated.Config["security.acls"] != "acl-"+networkId || updated.Config["security.acls.default.egress.action"] != "drop" ||
		updated.Config["security.acls.default.ingress.action"] != "drop" || updated.Config["ipv4.address"] != "10.40.0.1/24" {
		t.Fatalf("update: %+v", updated.Config)
	}
	if onlyBody(t, cp)["report"] != "Created" {
		t.Fatal("want Created")
	}
	if got := logs.Objects("already exists; treating it as ours"); len(got) != 2 {
		t.Fatalf("want 2 line(s) %q, got %v", "already exists; treating it as ours", got)
	}
}

func TestCreate_NetworkExistsAlreadyRight_IsNotWritten(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Networks = []api.Network{{Name: "net-" + networkId, Project: project, NetworkPut: api.NetworkPut{Config: map[string]string{
		"security.acls": "acl-" + networkId, "security.acls.default.ingress.action": "allow", "security.acls.default.egress.action": "allow",
	}}}}
	p.CreateNetworkErr = api.StatusErrorf(http.StatusConflict, `Network "net-%s" already exists`, networkId)

	err := NewCreate(cp.Client(), cluster.Fixed{clusterId: {Provider: p}}).Run(context.Background(), createLeased("10.40.0.0/24"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if len(p.UpdatedNetworks) != 0 {
		t.Fatalf("rewrote a network that was right: %+v", p.UpdatedNetworks)
	}
}

func TestCreate_AclCreateFails_NoNetwork(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.CreateACLErr = errors.New("boom")

	err := NewCreate(cp.Client(), cluster.Fixed{clusterId: {Provider: p}}).Run(context.Background(), createLeased("10.40.0.0/24"), testsupport.DiscardLogger())

	if err == nil {
		t.Fatal("want the error")
	}
	if len(p.CreatedNetworks) != 0 || len(cp.Requests()) != 0 {
		t.Fatalf("went on: networks %v, reports %v", p.CreatedNetworks, cp.Requests())
	}
}

func TestCreate_InvalidCidr_IsPermanent(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	err := NewCreate(cp.Client(), cluster.Fixed{clusterId: {Provider: p}}).Run(context.Background(), createLeased("10.0.0.0/32"), testsupport.DiscardLogger())

	if err == nil || command.IsRetryable(err) {
		t.Fatalf("want permanent, got %v", err)
	}
	if p.Calls != 0 {
		t.Fatal("touched Incus")
	}
}

func deleteLeased() command.Leased {
	return leased("network.delete", map[string]any{
		"networkId": networkId, "externalNetworkId": "net-" + networkId, "projectName": project,
	})
}

// Incus keeps a network's ACL after the network goes and refuses to delete one in use: the
// network first, then its ACL.
func TestDelete_DeletesTheNetworkThenItsAcl(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.PutACL(project, api.NetworkACL{NetworkACLPost: api.NetworkACLPost{Name: "acl-" + networkId}})

	err := NewDelete(cp.Client(), cluster.Fixed{clusterId: {Provider: p}}).Run(context.Background(), deleteLeased(), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	wantOrder := []string{"delete-network " + project + "/net-" + networkId, "delete-acl " + project + "/acl-" + networkId}
	if strings.Join(p.Order, ",") != strings.Join(wantOrder, ",") {
		t.Fatalf("order: want %v, got %v", wantOrder, p.Order)
	}
	if len(p.ACLs) != 0 {
		t.Fatal("ACL left behind")
	}
	if onlyBody(t, cp)["report"] != "Deleted" {
		t.Fatal("want Deleted")
	}
}

func TestDelete_Gone_ReportsDeleted(t *testing.T) {
	logger, logs := testsupport.NewLogCapture(t)
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.DeleteNetworkErr = testsupport.NotFound("Network not found")

	err := NewDelete(cp.Client(), cluster.Fixed{clusterId: {Provider: p}}).Run(context.Background(), deleteLeased(), logger)

	if err != nil {
		t.Fatal(err)
	}
	if onlyBody(t, cp)["report"] != "Deleted" {
		t.Fatal("want Deleted")
	}
	if got := logs.Objects("already gone; nothing to delete"); len(got) != 2 {
		t.Fatalf("want 2 line(s) %q, got %v", "already gone; nothing to delete", got)
	}
}

func TestDelete_AclDeleteFails_DoesNotReportDeleted(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.DeleteACLErr = errors.New("boom")

	err := NewDelete(cp.Client(), cluster.Fixed{clusterId: {Provider: p}}).Run(context.Background(), deleteLeased(), testsupport.DiscardLogger())

	if err == nil {
		t.Fatal("want the error")
	}
	if len(cp.Requests()) != 0 {
		t.Fatalf("reported: %v", cp.Requests())
	}
}

func TestDelete_GiveUp_ReportsRequestFailed(t *testing.T) {
	cp := testsupport.NewControlPlane(t)

	err := NewDelete(cp.Client(), cluster.Fixed{}).GiveUp(context.Background(), deleteLeased(), errors.New("in use"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body := onlyBody(t, cp)
	if body["report"] != "RequestFailed" || body["reason"] != reasonDeleteFailed || body["detail"] != "in use" {
		t.Fatalf("report: %v", body)
	}
}

func strp(value string) *string { return &value }

// https://github.com/OpenViCloud/titan-ops/blob/main/docs/architecture/desired-and-current.md#provider-state-is-verbatim: whatever word Incus has, it goes as it is.
func TestDecideReport(t *testing.T) {
	observed := map[string]string{"net-a": "Created", "net-b": "Pending", "net-c": "Errored", "net-u": "Unknown", "net-v": "Unavailable"}
	cases := []struct {
		name       string
		row        ReconcilableNetwork
		wantExists bool
		wantStatus string
		send       bool
	}{
		{"created now", ReconcilableNetwork{ExternalId: "net-a", CurrentExistence: "Exists"}, true, "Created", true},
		{"unchanged", ReconcilableNetwork{ExternalId: "net-a", CurrentState: strp("Created"), CurrentExistence: "Exists"}, true, "", false},
		{"pending", ReconcilableNetwork{ExternalId: "net-b", CurrentExistence: "Exists"}, true, "Pending", true},
		{"errored", ReconcilableNetwork{ExternalId: "net-c", CurrentState: strp("Created"), CurrentExistence: "Exists"}, true, "Errored", true},
		{"unknown", ReconcilableNetwork{ExternalId: "net-u", CurrentState: strp("Created"), CurrentExistence: "Exists"}, true, "Unknown", true},
		{"unavailable", ReconcilableNetwork{ExternalId: "net-v", CurrentState: strp("Created"), CurrentExistence: "Exists"}, true, "Unavailable", true},
		{"found again", ReconcilableNetwork{ExternalId: "net-a", CurrentExistence: "NotFound"}, true, "Created", true},
		{"lost", ReconcilableNetwork{ExternalId: "net-x", CurrentState: strp("Created"), CurrentExistence: "Exists"}, false, "", true},
		{"lost and recorded", ReconcilableNetwork{ExternalId: "net-x", CurrentExistence: "NotFound"}, false, "", false},
	}
	for _, c := range cases {
		got, send := DecideReport(c.row, observed)
		if send != c.send {
			t.Errorf("%s: want send %v, got %v", c.name, c.send, send)
			continue
		}
		if !send {
			continue
		}
		if got.Report != reportObserved || got.Exists == nil || *got.Exists != c.wantExists || got.Status != c.wantStatus {
			t.Errorf("%s: want exists %v status %q, got %+v", c.name, c.wantExists, c.wantStatus, got)
		}
	}
}

func TestReconcile_SendsTheIncusStatusAsIs(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, "/internal/reconcile/networks?clusterId=mock", http.StatusOK,
		`[{"id":"`+networkId+`","externalId":"net-a","currentState":null,"currentExistence":"Exists"}]`)
	p := testsupport.NewProvider()
	p.Networks = []api.Network{{Name: "net-a", Status: "Created", Project: project}}
	logger, logs := testsupport.NewLogCapture(t)

	err := Reconcile(context.Background(), cp.Client(), p, "mock", logger)

	if err != nil {
		t.Fatal(err)
	}
	body := onlyBody(t, cp)
	if body["report"] != "Observed" || body["exists"] != true || body["status"] != "Created" {
		t.Fatalf("report: %v", body)
	}
	sent := logs.One("reconcile report sent")
	if sent["networkId"] != networkId || sent["recordedState"] != "" || sent["status"] != "Created" {
		t.Fatalf("report line: %v", sent)
	}
}

func TestReconcile_NetworkGone_IsNotFound_NeverDeleted(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, "/internal/reconcile/networks?clusterId=mock", http.StatusOK,
		`[{"id":"`+networkId+`","externalId":"net-gone","currentState":"Created","currentExistence":"Exists"}]`)
	p := testsupport.NewProvider()

	err := Reconcile(context.Background(), cp.Client(), p, "mock", testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body := onlyBody(t, cp)
	if body["report"] != "Observed" || body["exists"] != false {
		t.Fatalf("report: %v", body)
	}
	if _, hasStatus := body["status"]; hasStatus {
		t.Fatalf("an absent network has no status: %v", body)
	}
}

func TestReconcile_NetworkReadFails_ReportsNothing(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, "/internal/reconcile/networks?clusterId=mock", http.StatusOK,
		`[{"id":"`+networkId+`","externalId":"net-a","currentState":"Created","currentExistence":"Exists"}]`)
	p := testsupport.NewProvider()
	p.ListNetworksErr = errors.New("incus unreachable")

	err := Reconcile(context.Background(), cp.Client(), p, "mock", testsupport.DiscardLogger())

	if err == nil {
		t.Fatal("want the read error")
	}
	reports := cp.RequestsTo("PATCH /internal/networks/" + networkId + "/status")
	if len(reports) != 0 {
		t.Fatalf("reported on a failed read: %+v", reports)
	}
}
