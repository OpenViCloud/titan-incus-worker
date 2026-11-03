package firewall

import (
	"context"
	"fmt"
	"net/netip"
	"strconv"
	"strings"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
)

// ProviderId is how Titan stores a rule's provider id: "<acl>/<ruleId>".
func ProviderId(aclName string, ruleId string) string {
	return aclName + "/" + ruleId
}

// CreatePayload is what ProviderCommandQueue.EnqueueFirewallRuleCreateAsync sends.
type CreatePayload struct {
	RuleId string `json:"ruleId"`
	// RequestId names the request this command ends (titan-ops architecture/resource-lifecycle.md#callback-routes). Empty
	// on a command of the old control plane that the migration gave no id: it keeps the old reporting until rollout
	// phase 6 (titan-ops architecture/resource-lifecycle.md#rollout).
	RequestId   string `json:"requestId"`
	NetworkId   string `json:"networkId"`
	Direction   string `json:"direction"`
	Protocol    string `json:"protocol"`
	StartPort   *int   `json:"startPort"`
	EndPort     *int   `json:"endPort"`
	Cidr        string `json:"cidr"`
	ProjectName string `json:"projectName"`
}

// AclRule maps a Titan rule onto an ACL entry and the ACL list it goes in ("ingress" or "egress"). A Titan rule
// allows; the network's default action covers everything else.
func AclRule(payload CreatePayload) (string, api.NetworkACLRule, error) {
	rule := api.NetworkACLRule{Action: "allow", Description: payload.RuleId, State: "enabled"}
	cidr, err := netip.ParsePrefix(payload.Cidr)
	if err != nil {
		return "", api.NetworkACLRule{}, fmt.Errorf("firewall rule CIDR %q: %w", payload.Cidr, err)
	}
	if cidr.Addr().Is4In6() {
		return "", api.NetworkACLRule{}, fmt.Errorf("firewall rule CIDR %q is an IPv4 range in IPv6 form", payload.Cidr)
	}

	var direction string
	switch strings.ToLower(payload.Direction) {
	case "ingress":
		direction = "ingress"
		rule.Source = payload.Cidr
	case "egress":
		direction = "egress"
		rule.Destination = payload.Cidr
	default:
		return "", api.NetworkACLRule{}, fmt.Errorf("unknown firewall rule direction %q", payload.Direction)
	}

	switch strings.ToLower(payload.Protocol) {
	case "tcp", "udp":
		rule.Protocol = strings.ToLower(payload.Protocol)
		rule.DestinationPort = portRange(payload.StartPort, payload.EndPort)
	case "icmp":
		// Incus names ICMP per address family; the rule's CIDR says which one is meant.
		if cidr.Addr().Is4() {
			rule.Protocol = "icmp4"
		} else {
			rule.Protocol = "icmp6"
		}
	case "all":
	default:
		return "", api.NetworkACLRule{}, fmt.Errorf("unknown firewall rule protocol %q", payload.Protocol)
	}
	return direction, rule, nil
}

// portRange writes ports as Incus wants destination_port: "22", "5000-5010", or "" for any port.
func portRange(start *int, end *int) string {
	if start == nil {
		return ""
	}
	isOnePort := end == nil || *end == *start
	if isOnePort {
		return strconv.Itoa(*start)
	}
	return strconv.Itoa(*start) + "-" + strconv.Itoa(*end)
}

func hasRule(acl *api.NetworkACL, ruleId string) bool {
	_, found := ruleState(acl, ruleId)
	return found
}

// ruleState is the Incus state of the rule's entry in the ACL; found is false when the ACL has no such entry.
func ruleState(acl *api.NetworkACL, ruleId string) (string, bool) {
	for _, rules := range [][]api.NetworkACLRule{acl.Ingress, acl.Egress} {
		for _, rule := range rules {
			if rule.Description == ruleId {
				return rule.State, true
			}
		}
	}
	return "", false
}

// isInAcl reads the ACL for the rule. An ACL Incus does not have holds no rule; any other failed read is returned,
// since reading it as "not there" would tell the control plane something nobody saw.
func isInAcl(ctx context.Context, p provider.Provider, project string, aclName string, ruleId string) (bool, error) {
	acl, _, err := p.GetNetworkACL(ctx, project, aclName)
	if provider.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return hasRule(acl, ruleId), nil
}
