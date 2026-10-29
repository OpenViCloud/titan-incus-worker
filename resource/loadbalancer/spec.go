package loadbalancer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
)

// The NIC every Titan VM has on its network (resource/vm/create.go).
const hwaddrKey = "volatile.eth0.hwaddr"

type listener struct {
	Protocol   string `json:"protocol"`
	Port       int    `json:"port"`
	TargetPort int    `json:"targetPort"`
}

type backend struct {
	VmId         string `json:"vmId"`
	ExternalVmId string `json:"externalVmId"`
}

// specPayload is what load-balancer.create and load-balancer.update carry: the whole asked configuration.
type specPayload struct {
	LoadBalancerId    string     `json:"loadBalancerId"`
	IntentId          string     `json:"intentId"`
	ProjectName       string     `json:"projectName"`
	ExternalNetworkId string     `json:"externalNetworkId"`
	PublicIpv6Subnet  string     `json:"publicIpv6Subnet"`
	Vip               string     `json:"vip"`
	Listeners         []listener `json:"listeners"`
	Backends          []backend  `json:"backends"`
}

func (p specPayload) check() error {
	isComplete := p.LoadBalancerId != "" && p.ProjectName != "" && p.ExternalNetworkId != "" && p.PublicIpv6Subnet != "" &&
		p.Vip != "" && len(p.Listeners) > 0 && len(p.Backends) > 0
	if !isComplete {
		return command.Permanent(errors.New("load balancer payload lacks an id, project, network, subnet, VIP, listener or backend"))
	}
	return nil
}

// Eui64 is the SLAAC address a NIC with that MAC takes on the /64 (titan-ops features/load-balancer/design.md#backend-address):
// the guest of the Titan Ubuntu template uses it (measured), and Incus knows the MAC before the guest boots.
func Eui64(subnet string, mac string) (string, error) {
	prefix, err := netip.ParsePrefix(subnet)
	if err != nil || prefix.Bits() != 64 || !prefix.Addr().Is6() {
		return "", fmt.Errorf("%q is not an IPv6 /64", subnet)
	}
	hardware, err := net.ParseMAC(mac)
	if err != nil || len(hardware) != 6 {
		return "", fmt.Errorf("%q is not a 48-bit MAC", mac)
	}
	bytes := prefix.Masked().Addr().As16()
	bytes[8] = hardware[0] ^ 0x02
	bytes[9] = hardware[1]
	bytes[10] = hardware[2]
	bytes[11] = 0xff
	bytes[12] = 0xfe
	bytes[13] = hardware[3]
	bytes[14] = hardware[4]
	bytes[15] = hardware[5]
	return netip.AddrFrom16(bytes).String(), nil
}

// incusSpec builds the Incus configuration: one backend per (listener i, VM), named l<i>-<instance>, so each port
// sends to its own target port (titan-ops features/load-balancer/design.md#lb-resource-model); the health check is on
// (titan-ops features/load-balancer/design.md#health-check). An instance Incus no longer has can never be a backend.
func incusSpec(ctx context.Context, p provider.Provider, payload specPayload) (api.NetworkLoadBalancerPut, error) {
	addresses := map[string]string{}
	for _, b := range payload.Backends {
		instance, err := p.GetInstance(ctx, payload.ProjectName, b.ExternalVmId)
		if provider.IsNotFound(err) {
			return api.NetworkLoadBalancerPut{}, command.Permanent(fmt.Errorf("backend instance %s is gone", b.ExternalVmId))
		}
		if err != nil {
			return api.NetworkLoadBalancerPut{}, err
		}
		mac := instance.Config[hwaddrKey]
		if mac == "" {
			return api.NetworkLoadBalancerPut{}, fmt.Errorf("backend instance %s has no %s yet", b.ExternalVmId, hwaddrKey)
		}
		address, err := Eui64(payload.PublicIpv6Subnet, mac)
		if err != nil {
			return api.NetworkLoadBalancerPut{}, command.Permanent(err)
		}
		addresses[b.ExternalVmId] = address
	}

	put := api.NetworkLoadBalancerPut{
		Description: "titan " + payload.LoadBalancerId,
		Config:      api.ConfigMap{"healthcheck": "true"},
		Backends:    []api.NetworkLoadBalancerBackend{},
		Ports:       []api.NetworkLoadBalancerPort{},
	}
	for i, l := range payload.Listeners {
		names := []string{}
		for _, b := range payload.Backends {
			name := "l" + strconv.Itoa(i) + "-" + b.ExternalVmId
			names = append(names, name)
			put.Backends = append(put.Backends, api.NetworkLoadBalancerBackend{
				Name: name, TargetAddress: addresses[b.ExternalVmId], TargetPort: strconv.Itoa(l.TargetPort),
			})
		}
		put.Ports = append(put.Ports, api.NetworkLoadBalancerPort{
			Protocol: l.Protocol, ListenPort: strconv.Itoa(l.Port), TargetBackend: names,
		})
	}
	return put, nil
}

// apply writes the configuration to Incus: a PUT when the VIP is there (a repeated lease, or an update), a POST when
// it is not.
func apply(ctx context.Context, p provider.Provider, payload specPayload, put api.NetworkLoadBalancerPut) error {
	_, etag, err := p.GetNetworkLoadBalancer(ctx, payload.ProjectName, payload.ExternalNetworkId, payload.Vip)
	if provider.IsNotFound(err) {
		post := api.NetworkLoadBalancersPost{NetworkLoadBalancerPut: put, ListenAddress: payload.Vip}
		return p.CreateNetworkLoadBalancer(ctx, payload.ProjectName, payload.ExternalNetworkId, post)
	}
	if err != nil {
		return err
	}
	return p.UpdateNetworkLoadBalancer(ctx, payload.ProjectName, payload.ExternalNetworkId, payload.Vip, put, etag)
}
