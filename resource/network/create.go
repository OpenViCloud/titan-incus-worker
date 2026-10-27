package network

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
)

// The network config keys that attach the ACL and set what happens to traffic no rule allows.
const (
	keyAcls           = "security.acls"
	keyDefaultIngress = "security.acls.default.ingress.action"
	keyDefaultEgress  = "security.acls.default.egress.action"
)

// The network config keys that carry public IPv6.
const (
	keyUplink             = "network"
	keyIpv6Address        = "ipv6.address"
	keyIpv6Nat            = "ipv6.nat"
	keyIpv6RouterOnUplink = "volatile.network.ipv6.address"
)

// DefaultAction merges the network's two policies into the one action Incus applies between VMs of an isolated
// network: there every packet is one VM's egress and another's ingress, and only the egress default takes effect
// (https://github.com/OpenViCloud/titan-ops/blob/main/docs/features/network/design.md#firewall-default-policy). Either Deny blocks; a missing policy (a command enqueued
// before the ingress one was sent) is Allow.
func DefaultAction(ingressPolicy string, egressPolicy string) (string, error) {
	denies := false
	for _, policy := range []string{ingressPolicy, egressPolicy} {
		switch policy {
		case "", "Allow":
		case "Deny":
			denies = true
		default:
			return "", fmt.Errorf("unknown network policy %q", policy)
		}
	}
	if denies {
		return "drop", nil
	}
	return "allow", nil
}

type createPayload struct {
	NetworkId            string `json:"networkId"`
	Name                 string `json:"name"`
	ProjectName          string `json:"projectName"`
	Cidr                 string `json:"cidr"`
	DefaultIngressPolicy string `json:"defaultIngressPolicy"`
	DefaultEgressPolicy  string `json:"defaultEgressPolicy"`
	// Both empty when the network has no public IPv6.
	PublicIpv6Subnet        string `json:"publicIpv6Subnet"`
	PublicIpv6RouterAddress string `json:"publicIpv6RouterAddress"`
}

// Create runs network.create: the network's ACL, then an isolated OVN network in the tenant's project that carries
// it. It reports the provider id (Created); the reconciler reports Active.
type Create struct {
	cp       *controlplane.Client
	clusters cluster.Clusters
}

func NewCreate(cp *controlplane.Client, clusters cluster.Clusters) *Create {
	return &Create{cp: cp, clusters: clusters}
}

func (h *Create) Run(ctx context.Context, leased command.Leased, logger *slog.Logger) error {
	payload, err := decodeCreate(leased)
	if err != nil {
		return err
	}
	address, err := RouterAddress(payload.Cidr)
	if err != nil {
		return command.Permanent(err)
	}
	publicIpv6, err := PublicIpv6Of(payload.PublicIpv6Subnet, payload.PublicIpv6RouterAddress)
	if err != nil {
		return command.Permanent(err)
	}
	defaultAction, err := DefaultAction(payload.DefaultIngressPolicy, payload.DefaultEgressPolicy)
	if err != nil {
		return command.Permanent(err)
	}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return err
	}
	request, err := NetworkRequest(payload, address.Cidr, defaultAction, cl.OvnUplinkNetwork, publicIpv6)
	if err != nil {
		return command.Permanent(fmt.Errorf("cluster %s: %w", leased.ClusterId, err))
	}
	if err := provider.EnsureProject(ctx, cl.Provider, payload.ProjectName); err != nil {
		return err
	}

	// The ACL first: a network cannot name an ACL that does not exist. Already there: an earlier run made it; the
	// name is ours.
	acl := api.NetworkACLsPost{NetworkACLPost: api.NetworkACLPost{Name: provider.NetworkAclName(payload.NetworkId)}}
	err = cl.Provider.CreateNetworkACL(ctx, payload.ProjectName, acl)
	aclIsOurs := err != nil && provider.IsAclAlreadyExists(err)
	if err != nil && !aclIsOurs {
		return err
	}
	if aclIsOurs {
		logger.Info("already exists; treating it as ours", "object", acl.Name)
	}

	err = cl.Provider.CreateNetwork(ctx, payload.ProjectName, request)
	// 409: an earlier run made it; the name is ours, but that run may have made it with other ACL settings.
	isOurs := err != nil && provider.IsConflict(err)
	if err != nil && !isOurs {
		return err
	}
	if isOurs {
		logger.Info("already exists; treating it as ours", "object", request.Name)
		if err := ensureAclSettings(ctx, cl.Provider, payload.ProjectName, request); err != nil {
			return err
		}
	}
	if err := checkPublicIpv6(ctx, cl.Provider, payload.ProjectName, request.Name, cl.OvnUplinkNetwork, publicIpv6); err != nil {
		return err
	}
	report := statusReport{
		Report:     reportCreated,
		ExternalId: request.Name,
		Cidr:       payload.Cidr,
		Gateway:    address.Gateway,
	}
	// Titan chose no CIDR: Incus picked one ("auto"); read it back so Titan records the subnet the VMs get.
	cidrWasChosenByIncus := payload.Cidr == ""
	if cidrWasChosenByIncus {
		picked, err := h.pickedSubnet(ctx, cl, payload)
		if err != nil {
			return err
		}
		report.Cidr = picked.Cidr
		report.Gateway = picked.Gateway
	}
	return sendStatus(ctx, h.cp, payload.NetworkId, report)
}

func (h *Create) pickedSubnet(ctx context.Context, cl *cluster.Cluster, payload createPayload) (Subnet, error) {
	networks, err := cl.Provider.ListNetworksAllProjects(ctx)
	if err != nil {
		return Subnet{}, err
	}
	name := provider.NetworkName(payload.NetworkId)
	for _, network := range networks {
		isThisOne := network.Name == name && network.Project == payload.ProjectName
		if !isThisOne {
			continue
		}
		subnet, ok := SubnetOf(network.Config["ipv4.address"])
		if !ok {
			return Subnet{}, fmt.Errorf("network %s has no IPv4 subnet (ipv4.address %q)", name, network.Config["ipv4.address"])
		}
		return subnet, nil
	}
	return Subnet{}, fmt.Errorf("network %s is not on the cluster after its create", name)
}

// GiveUp reports the give-up status, with the provider id when the network exists so its delete still reaches Incus.
func (h *Create) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	var payload createPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	if payload.NetworkId == "" {
		return command.Permanent(errors.New("network.create payload lacks networkId: nobody to tell"))
	}
	report := statusReport{Report: reportRequestFailed, Reason: reasonCreateFailed, Detail: cause.Error()}
	// Whether Incus holds the network decides whether the control plane keeps the network's /64: a list that
	// failed answers neither, so the next lease sends the give-up again.
	networkExists, err := h.networkExists(ctx, leased.ClusterId, payload)
	if err != nil {
		return err
	}
	if !networkExists {
		h.deleteAcl(ctx, leased.ClusterId, payload)
	}
	if networkExists {
		report.ExternalId = provider.NetworkName(payload.NetworkId)
		report.Cidr = payload.Cidr
		address, err := RouterAddress(payload.Cidr)
		if err == nil {
			report.Gateway = address.Gateway
		}
		// Incus chose the subnet: report the one it has, not an empty one Titan would record.
		cidrWasChosenByIncus := payload.Cidr == ""
		if cidrWasChosenByIncus {
			cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
			if err == nil {
				picked, err := h.pickedSubnet(ctx, cl, payload)
				if err == nil {
					report.Cidr = picked.Cidr
					report.Gateway = picked.Gateway
				}
			}
		}
	}
	return sendStatus(ctx, h.cp, payload.NetworkId, report)
}

// deleteAcl removes the ACL an earlier run made before its network failed: the control plane deletes a network with no
// provider id locally, so no network.delete comes for it. Best effort: a network that does exist after all keeps its
// ACL, because Incus refuses to delete an ACL in use.
func (h *Create) deleteAcl(ctx context.Context, clusterId string, payload createPayload) {
	cl, err := h.clusters.Cluster(ctx, clusterId)
	if err != nil {
		return
	}
	_ = cl.Provider.DeleteNetworkACL(ctx, payload.ProjectName, provider.NetworkAclName(payload.NetworkId))
}

func (h *Create) networkExists(ctx context.Context, clusterId string, payload createPayload) (bool, error) {
	cl, err := h.clusters.Cluster(ctx, clusterId)
	if err != nil {
		// A cluster that is not configured never had the network; a connection that may come back tells nothing yet.
		if !command.IsRetryable(err) {
			return false, nil
		}
		return false, err
	}
	networks, err := cl.Provider.ListNetworksAllProjects(ctx)
	if err != nil {
		return false, err
	}
	name := provider.NetworkName(payload.NetworkId)
	for _, network := range networks {
		if network.Name == name && network.Project == payload.ProjectName {
			return true, nil
		}
	}
	return false, nil
}

func decodeCreate(leased command.Leased) (createPayload, error) {
	var payload createPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return createPayload{}, err
	}
	isComplete := payload.NetworkId != "" && payload.ProjectName != ""
	if !isComplete {
		return createPayload{}, command.Permanent(errors.New("network.create payload lacks networkId or projectName"))
	}
	return payload, nil
}

// ensureAclSettings sets the ACL keys of an existing network to the request's, keeping the rest of its config.
func ensureAclSettings(ctx context.Context, p provider.Provider, project string, request api.NetworksPost) error {
	existing, etag, err := p.GetNetwork(ctx, project, request.Name)
	if err != nil {
		return err
	}
	put := existing.Writable()
	if put.Config == nil {
		put.Config = map[string]string{}
	}
	changed := false
	for _, key := range []string{keyAcls, keyDefaultIngress, keyDefaultEgress} {
		if put.Config[key] != request.Config[key] {
			put.Config[key] = request.Config[key]
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return p.UpdateNetwork(ctx, project, request.Name, put, etag)
}

// checkPublicIpv6 reads a network with public IPv6 back and compares it with what was asked. A router at another
// address gets nothing from the static OVH route of the /64 while the network looks healthy, so a difference is
// permanent (https://github.com/OpenViCloud/titan-ops/blob/main/docs/infra/incus/design.md#ipv6-ovn-uplink). Addresses are compared parsed: Incus may write them another
// way.
func checkPublicIpv6(ctx context.Context, p provider.Provider, project string, name string, uplink string, publicIpv6 PublicIpv6) error {
	if !publicIpv6.IsOn() {
		return nil
	}
	held, _, err := p.GetNetwork(ctx, project, name)
	if err != nil {
		return err
	}
	mismatch := publicIpv6Mismatch(name, held.Config, uplink, publicIpv6)
	if mismatch == nil {
		return nil
	}
	// Left in place, the reconciler would see it Created and VMs would land on a /64 nothing routes to. Once it is
	// gone the give-up reports no network, and the control plane frees the /64.
	if err := p.DeleteNetwork(ctx, project, name); err != nil {
		return fmt.Errorf("%v; deleting it: %w", mismatch, err)
	}
	return command.Permanent(mismatch)
}

func publicIpv6Mismatch(name string, config map[string]string, uplink string, publicIpv6 PublicIpv6) error {
	if config[keyUplink] != uplink {
		return fmt.Errorf("network %s is on uplink %q, want %q", name, config[keyUplink], uplink)
	}
	wantAddress := publicIpv6.NetworkAddress()
	heldAddress, err := netip.ParsePrefix(config[keyIpv6Address])
	if err != nil || heldAddress != wantAddress {
		return fmt.Errorf("network %s has ipv6.address %q, want %q", name, config[keyIpv6Address], wantAddress)
	}
	heldRouter, err := netip.ParseAddr(config[keyIpv6RouterOnUplink])
	if err != nil || heldRouter != publicIpv6.RouterAddress {
		return fmt.Errorf("network %s has its router at %q on the uplink, want %q", name, config[keyIpv6RouterOnUplink], publicIpv6.RouterAddress)
	}
	return nil
}

// NetworkRequest: tenant IPv4 reaches only the VMs of its own network — no router out, no NAT, and two tenants may use
// the same CIDR (https://github.com/OpenViCloud/titan-ops/blob/main/docs/architecture/vm-network.md#no-ipv4-egress). Without public IPv6 the network has no uplink at all;
// with it, the network goes on the cluster's uplink with its /64, no NAT, and its router at the address the OVH route of
// that /64 points to (https://github.com/OpenViCloud/titan-ops/blob/main/docs/infra/incus/design.md#ipv6-ovn-uplink). The network carries its ACL, with defaultAction for
// traffic no rule allows.
func NetworkRequest(payload createPayload, ipv4Address string, defaultAction string, uplink string, publicIpv6 PublicIpv6) (api.NetworksPost, error) {
	config := map[string]string{
		"ipv4.address":    ipv4Address,
		"ipv4.nat":        "false",
		keyIpv6Address:    "none",
		keyUplink:         "none",
		keyAcls:           provider.NetworkAclName(payload.NetworkId),
		keyDefaultIngress: defaultAction,
		keyDefaultEgress:  defaultAction,
	}
	if publicIpv6.IsOn() {
		if uplink == "" {
			return api.NetworksPost{}, errors.New("no OVN uplink network, which public IPv6 needs")
		}
		config[keyUplink] = uplink
		config[keyIpv6Address] = publicIpv6.NetworkAddress().String()
		config[keyIpv6Nat] = "false"
		config[keyIpv6RouterOnUplink] = publicIpv6.RouterAddress.String()
	}
	return api.NetworksPost{
		Name:       provider.NetworkName(payload.NetworkId),
		Type:       "ovn",
		NetworkPut: api.NetworkPut{Config: config, Description: payload.Name},
	}, nil
}
