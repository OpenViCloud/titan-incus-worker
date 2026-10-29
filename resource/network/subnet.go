package network

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
)

// Router is the network's own address: the first host of the CIDR, written as Incus wants ipv4.address.
type Router struct {
	Cidr    string // "10.40.0.1/24", or "auto" when Titan chose no CIDR
	Gateway string // "10.40.0.1", or "" with "auto"
}

// RouterAddress: an empty CIDR (the tenant chose none) lets Incus pick a subnet.
func RouterAddress(cidr string) (Router, error) {
	if cidr == "" {
		return Router{Cidr: "auto", Gateway: ""}, nil
	}
	_, subnet, err := net.ParseCIDR(cidr)
	if err != nil {
		return Router{}, fmt.Errorf("network CIDR %q: %w", cidr, err)
	}
	base := subnet.IP.To4()
	if base == nil {
		return Router{}, fmt.Errorf("network CIDR %q is not IPv4", cidr)
	}
	prefix, bits := subnet.Mask.Size()
	hasTwoHosts := bits-prefix >= 2
	if !hasTwoHosts {
		return Router{}, fmt.Errorf("network CIDR %q has no room for a router and a VM", cidr)
	}
	first := net.IPv4(base[0], base[1], base[2], base[3]+1).String()
	return Router{Cidr: fmt.Sprintf("%s/%d", first, prefix), Gateway: first}, nil
}

// Subnet is what Titan records for a network: the network address with its prefix, and the router's address.
type Subnet struct {
	Cidr    string // "10.100.0.0/24"
	Gateway string // "10.100.0.1"
}

// SubnetOf turns Incus' ipv4.address ("10.100.0.1/24") into the Subnet; "auto", "none" or garbage are not one.
func SubnetOf(routerAddress string) (Subnet, bool) {
	router, subnet, err := net.ParseCIDR(routerAddress)
	if err != nil {
		return Subnet{}, false
	}
	isIpv4 := router.To4() != nil
	if !isIpv4 {
		return Subnet{}, false
	}
	return Subnet{Cidr: subnet.String(), Gateway: router.String()}, true
}

// PublicIpv6 is what the control plane gives a network with public IPv6: its /64, and its router's address on the
// uplink, which the static OVH route of that /64 points to (titan-ops infra/incus/design.md#ipv6-ovn-uplink). The zero
// value is a network without public IPv6.
type PublicIpv6 struct {
	Subnet        netip.Prefix // 2001:41d0:d00:d470::/64
	RouterAddress netip.Addr   // 2001:41d0:d00:d400::170
}

func (p PublicIpv6) IsOn() bool {
	return p.Subnet.IsValid()
}

// NetworkAddress is the network's own address in its /64, written as Incus wants ipv6.address:
// "2001:41d0:d00:d470::1/64".
func (p PublicIpv6) NetworkAddress() netip.Prefix {
	return netip.PrefixFrom(p.Subnet.Addr().Next(), p.Subnet.Bits())
}

// PublicIpv6Of reads the two payload fields: both empty is a network without public IPv6, one without the other is
// a payload nothing can be built from.
func PublicIpv6Of(subnet string, routerAddress string) (PublicIpv6, error) {
	if subnet == "" && routerAddress == "" {
		return PublicIpv6{}, nil
	}
	if subnet == "" || routerAddress == "" {
		return PublicIpv6{}, errors.New("public IPv6 needs both publicIpv6Subnet and publicIpv6RouterAddress")
	}
	prefix, err := netip.ParsePrefix(subnet)
	if err != nil {
		return PublicIpv6{}, fmt.Errorf("public IPv6 subnet %q: %w", subnet, err)
	}
	isIpv6 := prefix.Addr().Is6() && !prefix.Addr().Is4In6()
	if !isIpv6 || prefix.Bits() != 64 {
		return PublicIpv6{}, fmt.Errorf("public IPv6 subnet %q is not an IPv6 /64", subnet)
	}
	// A host address with /64 would put the network's address on another host than the one the subnet starts at.
	if prefix != prefix.Masked() {
		return PublicIpv6{}, fmt.Errorf("public IPv6 subnet %q is not the start of its /64", subnet)
	}
	router, err := netip.ParseAddr(routerAddress)
	if err != nil {
		return PublicIpv6{}, fmt.Errorf("public IPv6 router address %q: %w", routerAddress, err)
	}
	routerIsIpv6 := router.Is6() && !router.Is4In6() && router.Zone() == ""
	if !routerIsIpv6 {
		return PublicIpv6{}, fmt.Errorf("public IPv6 router address %q is not an IPv6 address", routerAddress)
	}
	return PublicIpv6{Subnet: prefix, RouterAddress: router}, nil
}
