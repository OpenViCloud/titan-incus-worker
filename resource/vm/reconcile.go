package vm

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"net/url"
	"sort"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
)

// The CurrentExistence values DecideReport reads.
const (
	existenceExists   = "Exists"
	existenceNotFound = "NotFound"
)

// ReconcilableVm mirrors ReconcilableVmQueryResult: what Titan last recorded, so only a difference is reported.
// CurrentState is null while Titan has seen no instance status (created, not yet observed, or not found).
type ReconcilableVm struct {
	Id                string  `json:"id"`
	ExternalId        string  `json:"externalId"`
	CurrentState      *string `json:"currentState"`
	CurrentExistence  string  `json:"currentExistence"`
	IpAddress         string  `json:"ipAddress"`
	PublicIpv6Address string  `json:"publicIpv6Address"`
}

// Reconcile observes one cluster's VMs and reports what differs. It never acts on Incus. A failed read skips the cycle:
// reading it as "no instance" would report every VM lost.
func Reconcile(ctx context.Context, cp *controlplane.Client, p provider.Provider, clusterId string, logger *slog.Logger) error {
	var rows []ReconcilableVm
	if err := cp.GetJSON(ctx, "/internal/reconcile/vms?clusterId="+url.QueryEscape(clusterId), &rows); err != nil {
		return fmt.Errorf("list reconcilable VMs of cluster %s: %w", clusterId, err)
	}
	if len(rows) == 0 {
		return nil
	}
	instances, err := p.ListVirtualMachinesAllProjects(ctx)
	if err != nil {
		return fmt.Errorf("observe VMs of cluster %s: %w", clusterId, err)
	}
	byName := map[string]api.InstanceFull{}
	for _, instance := range instances {
		byName[instance.Name] = instance
	}

	for _, row := range rows {
		report, shouldReport := DecideReport(row, byName)
		if !shouldReport {
			continue
		}
		err := sendStatus(ctx, cp, row.Id, report)
		if err != nil {
			// One VM refused must not cost the rest their report; nothing changed on the Titan side, so the next
			// cycle sends it again.
			logger.Warn("VM report failed; next cycle sends it again", "vmId", row.Id, "report", report.Report, "status", report.Status, "error", err)
			continue
		}
		recordedState := ""
		if row.CurrentState != nil {
			recordedState = *row.CurrentState
		}
		logger.Info("reconcile report sent", "vmId", row.Id, "recordedState", recordedState,
			"recordedExistence", row.CurrentExistence, "report", report.Report, "status", report.Status,
			"ipAddress", report.IpAddress, "publicIpv6Address", report.PublicIpv6Address, "reason", report.Reason)
	}
	return nil
}

// DecideReport compares one VM with Incus: a report when the status, the existence or an address differs from what
// Titan recorded. Absent is NotFound, never Deleted: only the delete command reports that, since a reconciler pointed
// at the wrong cluster also finds nothing (titan-ops architecture/desired-and-current.md#reconciler-never-dies).
func DecideReport(row ReconcilableVm, byName map[string]api.InstanceFull) (statusReport, bool) {
	instance, present := byName[row.ExternalId]
	if !present {
		absent := false
		alreadyRecorded := row.CurrentExistence == existenceNotFound
		return statusReport{Report: reportObserved, Exists: &absent}, !alreadyRecorded
	}

	exists := true
	observed := statusReport{
		Report:            reportObserved,
		Exists:            &exists,
		Status:            instance.Status,
		IpAddress:         firstGlobalIpv4(instance.State),
		PublicIpv6Address: publicIpv6(instance.State),
	}
	recordedState := ""
	if row.CurrentState != nil {
		recordedState = *row.CurrentState
	}
	existenceChanged := row.CurrentExistence != existenceExists
	stateChanged := observed.Status != recordedState
	addressChanged := observed.IpAddress != row.IpAddress
	publicIpv6Changed := observed.PublicIpv6Address != row.PublicIpv6Address
	return observed, existenceChanged || stateChanged || addressChanged || publicIpv6Changed
}

// firstGlobalIpv4 reads the interfaces in name order so the same VM always yields the same address.
func firstGlobalIpv4(state *api.InstanceState) string {
	if state == nil {
		return ""
	}
	names := []string{}
	for name := range state.Network {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if name == "lo" {
			continue
		}
		for _, address := range state.Network[name].Addresses {
			isGlobalIpv4 := address.Family == "inet" && address.Scope == "global"
			if isGlobalIpv4 {
				return address.Address
			}
		}
	}
	return ""
}

// publicIpv6 is the VM's public IPv6: a global inet6 address, which only a network with public IPv6 gives
// (titan-ops infra/incus/design.md#ipv6-ovn-uplink). Unique local addresses (fc00::/7) carry scope global too and are
// left out. A guest may hold several: the one SLAAC built from the NIC's MAC (EUI-64) stays while privacy addresses
// rotate, so it is taken when present; otherwise the smallest, so every cycle picks the same one while the set holds.
func publicIpv6(state *api.InstanceState) string {
	if state == nil {
		return ""
	}
	uniqueLocal := netip.MustParsePrefix("fc00::/7")
	var smallest netip.Addr
	var smallestFromMac netip.Addr
	for name, network := range state.Network {
		if name == "lo" {
			continue
		}
		fromMac, hasMac := eui64InterfaceId(network.Hwaddr)
		for _, address := range network.Addresses {
			isGlobalIpv6 := address.Family == "inet6" && address.Scope == "global"
			if !isGlobalIpv6 {
				continue
			}
			parsed, err := netip.ParseAddr(address.Address)
			if err != nil || uniqueLocal.Contains(parsed) {
				continue
			}
			if !smallest.IsValid() || parsed.Less(smallest) {
				smallest = parsed
			}
			bytes := parsed.As16()
			isFromMac := hasMac && [8]byte(bytes[8:]) == fromMac
			if isFromMac && (!smallestFromMac.IsValid() || parsed.Less(smallestFromMac)) {
				smallestFromMac = parsed
			}
		}
	}
	if smallestFromMac.IsValid() {
		return smallestFromMac.String()
	}
	if !smallest.IsValid() {
		return ""
	}
	return smallest.String()
}

// eui64InterfaceId is the interface id SLAAC builds from a MAC: ff:fe in the middle, the universal/local bit flipped.
func eui64InterfaceId(hwaddr string) ([8]byte, bool) {
	mac, err := net.ParseMAC(hwaddr)
	if err != nil || len(mac) != 6 {
		return [8]byte{}, false
	}
	return [8]byte{mac[0] ^ 0x02, mac[1], mac[2], 0xff, 0xfe, mac[3], mac[4], mac[5]}, true
}
