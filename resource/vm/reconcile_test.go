package vm

import (
	"context"
	"net/http"
	"testing"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

func running(name string, addresses map[string][]api.InstanceStateNetworkAddress) api.InstanceFull {
	network := map[string]api.InstanceStateNetwork{}
	for iface, list := range addresses {
		network[iface] = api.InstanceStateNetwork{Addresses: list}
	}
	return api.InstanceFull{
		Instance: api.Instance{Name: name, Status: "Running", StatusCode: api.Running},
		State:    &api.InstanceState{Status: "Running", StatusCode: api.Running, Network: network},
	}
}

func ipv4(address string) api.InstanceStateNetworkAddress {
	return api.InstanceStateNetworkAddress{Family: "inet", Address: address, Netmask: "24", Scope: "global"}
}

func ipv6(address string, scope string) api.InstanceStateNetworkAddress {
	return api.InstanceStateNetworkAddress{Family: "inet6", Address: address, Netmask: "64", Scope: scope}
}

func byName(instances ...api.InstanceFull) map[string]api.InstanceFull {
	indexed := map[string]api.InstanceFull{}
	for _, instance := range instances {
		indexed[instance.Name] = instance
	}
	return indexed
}

func TestDecideReport_Running_CarriesTheFirstGlobalIpv4_SkippingLoopback(t *testing.T) {
	row := ReconcilableVm{Id: "a", ExternalId: "vm-a", CurrentExistence: "Exists"}
	// "lo" sorts before "zz0" and carries a global-looking address: only the name check keeps it out. "eth0" sorts
	// first but has no IPv4, so the address comes from the first interface that has one.
	instance := running("vm-a", map[string][]api.InstanceStateNetworkAddress{
		"eth0": {{Family: "inet6", Address: "fe80::1", Scope: "link"}},
		"lo":   {ipv4("10.255.0.1")},
		"zz0":  {{Family: "inet6", Address: "fe80::2", Scope: "link"}, ipv4("10.40.0.2")},
	})

	report, send := DecideReport(row, byName(instance))

	if !send || report.Report != reportObserved || !*report.Exists || report.Status != "Running" || report.IpAddress != "10.40.0.2" {
		t.Fatalf("got %+v send=%v", report, send)
	}
}

// Incus says Running before DHCP answers; that is Running (titan-ops architecture/desired-and-current.md#provider-state-is-verbatim).
// The address follows in a later cycle as an address change.
func TestDecideReport_RunningWithoutAnIpv4Yet_IsReportedRunning(t *testing.T) {
	row := ReconcilableVm{Id: "a", ExternalId: "vm-a", CurrentExistence: "Exists"}

	report, send := DecideReport(row, byName(running("vm-a", nil)))

	if !send || report.Status != "Running" || report.IpAddress != "" {
		t.Fatalf("got %+v send=%v", report, send)
	}
}

// The VM's public IPv6 is a global inet6 address; with several (SLAAC plus a privacy address, or two NICs) the
// smallest is reported, so the same VM gives the same address every cycle and the reconciler does not flap.
func TestDecideReport_Running_CarriesTheSmallestGlobalIpv6(t *testing.T) {
	row := ReconcilableVm{Id: "a", ExternalId: "vm-a", CurrentExistence: "Exists"}
	instance := running("vm-a", map[string][]api.InstanceStateNetworkAddress{
		"eth0": {ipv4("10.40.0.2"), ipv6("fe80::216:3eff:fe00:1", "link"), ipv6("2001:41d0:d00:d470:216:3eff:fe00:9", "global")},
		"eth1": {ipv6("2001:41d0:d00:d470::10", "global")},
		"lo":   {ipv6("::1", "local"), ipv6("2001:41d0:d00:d470::1", "global")},
	})

	report, send := DecideReport(row, byName(instance))

	if !send || report.IpAddress != "10.40.0.2" || report.PublicIpv6Address != "2001:41d0:d00:d470::10" {
		t.Fatalf("got %+v send=%v", report, send)
	}
}

// SLAAC from the NIC's MAC (EUI-64) is the address that stays; a privacy address rotates. When the guest has the
// EUI-64 one, it is reported even though a privacy address sorts first.
func TestDecideReport_PrefersTheEui64AddressOfTheNicsMac_OverAPrivacyAddress(t *testing.T) {
	row := ReconcilableVm{Id: "a", ExternalId: "vm-a", CurrentExistence: "Exists"}
	instance := running("vm-a", map[string][]api.InstanceStateNetworkAddress{
		"eth0": {ipv6("2001:41d0:d00:d470::10", "global"), ipv6("2001:41d0:d00:d470:216:3eff:fe00:9", "global")},
	})
	nic := instance.State.Network["eth0"]
	nic.Hwaddr = "00:16:3e:00:00:09"
	instance.State.Network["eth0"] = nic

	report, _ := DecideReport(row, byName(instance))

	if report.PublicIpv6Address != "2001:41d0:d00:d470:216:3eff:fe00:9" {
		t.Fatalf("got %+v", report)
	}
}

// Unique local addresses (fc00::/7, e.g. a container runtime inside the guest) carry scope global but are not public.
func TestDecideReport_AUniqueLocalAddress_IsNotThePublicIpv6(t *testing.T) {
	row := ReconcilableVm{Id: "a", ExternalId: "vm-a", CurrentExistence: "Exists"}
	instance := running("vm-a", map[string][]api.InstanceStateNetworkAddress{
		"eth0":    {ipv4("10.40.0.2")},
		"docker0": {ipv6("fd00::1", "global")},
	})

	report, _ := DecideReport(row, byName(instance))

	if report.PublicIpv6Address != "" {
		t.Fatalf("got %+v", report)
	}
}

func TestDecideReport_NoGlobalIpv6_ReportsNone(t *testing.T) {
	row := ReconcilableVm{Id: "a", ExternalId: "vm-a", CurrentExistence: "Exists"}
	instance := running("vm-a", map[string][]api.InstanceStateNetworkAddress{
		"eth0": {ipv4("10.40.0.2"), ipv6("fe80::1", "link"), ipv6("not-an-address", "global")},
	})

	report, _ := DecideReport(row, byName(instance))

	if report.PublicIpv6Address != "" {
		t.Fatalf("got %+v", report)
	}
}

func TestDecideReport_APublicIpv6Change_IsSent_TheSameOneIsNot(t *testing.T) {
	running_ := "Running"
	recorded := ReconcilableVm{Id: "a", ExternalId: "vm-a", CurrentState: &running_, CurrentExistence: "Exists",
		IpAddress: "10.40.0.2", PublicIpv6Address: "2001:41d0:d00:d470::10"}
	same := running("vm-a", map[string][]api.InstanceStateNetworkAddress{"eth0": {ipv4("10.40.0.2"), ipv6("2001:41d0:d00:d470::10", "global")}})
	moved := running("vm-a", map[string][]api.InstanceStateNetworkAddress{"eth0": {ipv4("10.40.0.2"), ipv6("2001:41d0:d00:d470::11", "global")}})
	gone := running("vm-a", map[string][]api.InstanceStateNetworkAddress{"eth0": {ipv4("10.40.0.2")}})

	_, sendSame := DecideReport(recorded, byName(same))
	movedReport, sendMoved := DecideReport(recorded, byName(moved))
	goneReport, sendGone := DecideReport(recorded, byName(gone))

	if sendSame {
		t.Fatal("reported an unchanged public IPv6")
	}
	if !sendMoved || movedReport.PublicIpv6Address != "2001:41d0:d00:d470::11" {
		t.Fatalf("missed a public IPv6 change: %+v", movedReport)
	}
	if !sendGone || goneReport.PublicIpv6Address != "" {
		t.Fatalf("missed a public IPv6 that went away: %+v", goneReport)
	}
}

func TestDecideReport_Unchanged_SendsNothing_ButANewAddressIsSent(t *testing.T) {
	running_ := "Running"
	row := ReconcilableVm{Id: "a", ExternalId: "vm-a", CurrentState: &running_, CurrentExistence: "Exists", IpAddress: "10.40.0.2"}
	same := running("vm-a", map[string][]api.InstanceStateNetworkAddress{"eth0": {ipv4("10.40.0.2")}})
	moved := running("vm-a", map[string][]api.InstanceStateNetworkAddress{"eth0": {ipv4("10.40.0.9")}})

	_, sendSame := DecideReport(row, byName(same))
	report, sendMoved := DecideReport(row, byName(moved))

	if sendSame {
		t.Fatal("reported an unchanged VM")
	}
	if !sendMoved || report.IpAddress != "10.40.0.9" {
		t.Fatalf("missed an address change: %+v", report)
	}
}

// The five statuses Incus gives a virtual machine (driver_qemu.go statusCode) go to the control
// plane word for word; the right-hand words are the control plane's VmState names, so a rename on either side is red.
func TestDecideReport_EveryQemuStatus_IsSentVerbatim(t *testing.T) {
	cases := map[api.StatusCode]string{
		api.Running: "Running",
		api.Ready:   "Ready",
		api.Stopped: "Stopped",
		api.Frozen:  "Frozen",
		api.Error:   "Error",
	}
	for code, word := range cases {
		row := ReconcilableVm{Id: "a", ExternalId: "vm-a", CurrentExistence: "Exists"}
		instance := api.InstanceFull{Instance: api.Instance{Name: "vm-a", Status: code.String(), StatusCode: code}}

		report, send := DecideReport(row, byName(instance))

		if !send || report.Status != word || report.Reason != "" {
			t.Fatalf("%d: got %+v send=%v", code, report, send)
		}
	}
}

// A word the control plane does not know is still sent: the control plane refuses it and the refusal is logged,
// rather than the worker hiding a status nobody mapped.
func TestDecideReport_AStatusOutsideTheFive_IsSentAsIs(t *testing.T) {
	row := ReconcilableVm{Id: "a", ExternalId: "vm-a", CurrentExistence: "Exists"}
	stopping := api.InstanceFull{Instance: api.Instance{Name: "vm-a", Status: "Stopping", StatusCode: api.Stopping}}

	report, send := DecideReport(row, byName(stopping))

	if !send || report.Status != "Stopping" {
		t.Fatalf("got %+v send=%v", report, send)
	}
}

func TestDecideReport_Absent_IsNotFound_NeverDeleted_AndNotRepeated(t *testing.T) {
	running_ := "Running"
	row := ReconcilableVm{Id: "a", ExternalId: "vm-a", CurrentState: &running_, CurrentExistence: "Exists"}
	recorded := ReconcilableVm{Id: "a", ExternalId: "vm-a", CurrentExistence: "NotFound"}

	report, send := DecideReport(row, byName())
	_, sendAgain := DecideReport(recorded, byName())

	if !send || report.Report != reportObserved || *report.Exists || report.Status != "" {
		t.Fatalf("got %+v", report)
	}
	if sendAgain {
		t.Fatal("repeated a NotFound already recorded")
	}
}

func TestDecideReport_SeenAgainAfterNotFound_IsReported(t *testing.T) {
	recorded := ReconcilableVm{Id: "a", ExternalId: "vm-a", CurrentExistence: "NotFound"}
	stopped := api.InstanceFull{Instance: api.Instance{Name: "vm-a", Status: "Stopped", StatusCode: api.Stopped}}

	report, send := DecideReport(recorded, byName(stopped))

	if !send || !*report.Exists || report.Status != "Stopped" {
		t.Fatalf("got %+v send=%v", report, send)
	}
}

func TestReconcile_ReportsOnlyChanges_AndSkipsTheProviderWhenTheListIsEmpty(t *testing.T) {
	logger, logs := testsupport.NewLogCapture(t)
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, "/internal/reconcile/vms?clusterId=mock", http.StatusOK,
		`[{"id":"a","externalId":"vm-a","currentState":"Running","currentExistence":"Exists","ipAddress":"10.40.0.2","publicIpv6Address":null},
		  {"id":"b","externalId":"vm-b","currentState":null,"currentExistence":"Exists","ipAddress":null,"publicIpv6Address":null}]`)
	p := testsupport.NewProvider()
	p.VirtualMachines = []api.InstanceFull{
		running("vm-a", map[string][]api.InstanceStateNetworkAddress{"eth0": {ipv4("10.40.0.2")}}),
		running("vm-b", map[string][]api.InstanceStateNetworkAddress{"eth0": {ipv4("10.40.0.3")}}),
	}

	if err := Reconcile(context.Background(), cp.Client(), p, "mock", logger); err != nil {
		t.Fatal(err)
	}

	if len(cp.RequestsTo("PATCH /internal/vms/a/")) != 0 {
		t.Fatal("reported an unchanged VM")
	}
	body := onlyBody(t, cp, "PATCH /internal/vms/b/status")
	if body["report"] != "Observed" || body["exists"] != true || body["status"] != "Running" || body["ipAddress"] != "10.40.0.3" {
		t.Fatalf("want vm b Running with its address, got %v", body)
	}
	if _, hasIpv6 := body["publicIpv6Address"]; hasIpv6 {
		t.Fatalf("a VM without public IPv6 sends none: %v", body)
	}
	sent := logs.One("reconcile report sent")
	isVmB := sent["vmId"] == "b" && sent["recordedState"] == "" && sent["recordedExistence"] == "Exists"
	isRunning := sent["report"] == "Observed" && sent["status"] == "Running" && sent["ipAddress"] == "10.40.0.3"
	if !isVmB || !isRunning {
		t.Fatalf("report line: %v", sent)
	}

	empty := testsupport.NewControlPlane(t)
	untouched := testsupport.NewProvider()
	if err := Reconcile(context.Background(), empty.Client(), untouched, "mock", logger); err != nil {
		t.Fatal(err)
	}
	if untouched.Calls != 0 {
		t.Fatal("read Incus with nothing to reconcile")
	}
}

func TestReconcile_SendsThePublicIpv6_AndLogsIt(t *testing.T) {
	logger, logs := testsupport.NewLogCapture(t)
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, "/internal/reconcile/vms?clusterId=mock", http.StatusOK,
		`[{"id":"a","externalId":"vm-a","currentState":"Running","currentExistence":"Exists","ipAddress":"10.40.0.2","publicIpv6Address":null}]`)
	p := testsupport.NewProvider()
	p.VirtualMachines = []api.InstanceFull{
		running("vm-a", map[string][]api.InstanceStateNetworkAddress{"eth0": {ipv4("10.40.0.2"), ipv6("2001:41d0:d00:d470::10", "global")}}),
	}

	if err := Reconcile(context.Background(), cp.Client(), p, "mock", logger); err != nil {
		t.Fatal(err)
	}

	body := onlyBody(t, cp, "PATCH /internal/vms/a/status")
	if body["status"] != "Running" || body["ipAddress"] != "10.40.0.2" || body["publicIpv6Address"] != "2001:41d0:d00:d470::10" {
		t.Fatalf("want vm a with its public IPv6, got %v", body)
	}
	if sent := logs.One("reconcile report sent"); sent["publicIpv6Address"] != "2001:41d0:d00:d470::10" {
		t.Fatalf("report line: %v", sent)
	}
}
