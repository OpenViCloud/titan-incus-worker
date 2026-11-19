package restore

import (
	"context"
	"log/slog"
	"strings"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/resource/backup"
)

// networkPrefix/aclPrefix reverse provider.NetworkName/provider.NetworkAclName: a network's YAML name is
// "net-"+networkId and its ACL's is "acl-"+networkId, so the networkId is recovered by stripping the prefix.
const (
	networkPrefix = "net-"
	aclPrefix     = "acl-"
)

// keptVolatileKey is the one volatile.* config key a restore keeps: the public IPv6 router address the worker itself
// sets on create (resource/network/create.go keyIpv6RouterOnUplink), which Incus refuses as a bare volatile key only
// when nothing else asks for it — passing it back in the create request is how the worker sets it normally.
const keptVolatileKey = "volatile.network.ipv6.address"

func restoreProject(ctx context.Context, p provider.Provider, project backup.ProjectConfig, logger *slog.Logger, summary *Summary) {
	name := project.Project.Name
	_, err := p.GetProject(ctx, name)
	if err == nil {
		logger.Info("project already there; skipping its create", "project", name)
		summary.Skipped++
		return
	}
	if !provider.IsNotFound(err) {
		logger.Warn("restore project failed", "project", name, "error", err)
		summary.Failed++
		return
	}
	post := api.ProjectsPost{Name: name, ProjectPut: project.Project.ProjectPut}
	if err := p.CreateProject(ctx, post); err != nil {
		logger.Warn("restore project failed", "project", name, "error", err)
		summary.Failed++
		return
	}
	logger.Info("project restored", "project", name)
	summary.Restored++
}

// restoreProfiles updates the project's own default profile (Incus already made one when the project was created)
// to the YAML's, and creates every other profile.
func restoreProfiles(ctx context.Context, p provider.Provider, project backup.ProjectConfig, logger *slog.Logger, summary *Summary) {
	name := project.Project.Name
	for _, profile := range project.Profiles {
		if profile.Name == "default" {
			if err := p.UpdateProfile(ctx, name, "default", profile.ProfilePut); err != nil {
				logger.Warn("restore profile failed", "project", name, "profile", "default", "error", err)
				summary.Failed++
				continue
			}
			logger.Info("profile restored", "project", name, "profile", "default")
			summary.Restored++
			continue
		}
		post := api.ProfilesPost{Name: profile.Name, ProfilePut: profile.ProfilePut}
		if err := p.CreateProfile(ctx, name, post); err != nil {
			logger.Warn("restore profile failed", "project", name, "profile", profile.Name, "error", err)
			summary.Failed++
			continue
		}
		logger.Info("profile restored", "project", name, "profile", profile.Name)
		summary.Restored++
	}
}

// restoreAcls skips the ACL of a network Titan no longer lists (deleted after the backup point), and on every ACL it
// keeps, drops the rules whose description names a firewall rule Titan no longer lists; a rule with no description
// (none Titan ever wrote) is kept.
func restoreAcls(ctx context.Context, p provider.Provider, project backup.ProjectConfig, liveNetworkIds map[string]bool,
	liveRuleIds map[string]bool, logger *slog.Logger, summary *Summary) {
	name := project.Project.Name
	for _, acl := range project.NetworkAcls {
		networkId := strings.TrimPrefix(acl.Name, aclPrefix)
		if networkId == acl.Name || !liveNetworkIds[networkId] {
			logger.Info("network acl skipped: its network is gone from titan", "project", name, "acl", acl.Name)
			summary.Skipped++
			continue
		}
		put := acl.NetworkACLPut
		put.Ingress = filterAclRules(put.Ingress, liveRuleIds)
		put.Egress = filterAclRules(put.Egress, liveRuleIds)
		post := api.NetworkACLsPost{NetworkACLPost: acl.NetworkACLPost, NetworkACLPut: put}
		if err := p.CreateNetworkACL(ctx, name, post); err != nil {
			logger.Warn("restore network acl failed", "project", name, "acl", acl.Name, "error", err)
			summary.Failed++
			continue
		}
		logger.Info("network acl restored", "project", name, "acl", acl.Name)
		summary.Restored++
	}
}

func filterAclRules(rules []api.NetworkACLRule, liveRuleIds map[string]bool) []api.NetworkACLRule {
	kept := []api.NetworkACLRule{}
	for _, rule := range rules {
		if rule.Description == "" || liveRuleIds[rule.Description] {
			kept = append(kept, rule)
		}
	}
	return kept
}

// restoreNetworks skips a network Titan no longer lists, and keeps the config as backed up except for a volatile.*
// key other than keptVolatileKey: Incus writes those itself and may refuse one handed back bare.
func restoreNetworks(ctx context.Context, p provider.Provider, project backup.ProjectConfig, liveNetworkIds map[string]bool,
	logger *slog.Logger, summary *Summary) {
	name := project.Project.Name
	for _, network := range project.Networks {
		networkId := strings.TrimPrefix(network.Name, networkPrefix)
		if networkId == network.Name || !liveNetworkIds[networkId] {
			logger.Info("network skipped: titan no longer lists it", "project", name, "network", network.Name)
			summary.Skipped++
			continue
		}
		post := api.NetworksPost{
			Name: network.Name, Type: network.Type,
			NetworkPut: api.NetworkPut{Config: withoutStaleVolatileKeys(network.Config), Description: network.Description},
		}
		if err := p.CreateNetwork(ctx, name, post); err != nil {
			logger.Warn("restore network failed", "project", name, "network", network.Name, "error", err)
			summary.Failed++
			continue
		}
		logger.Info("network restored", "project", name, "network", network.Name)
		summary.Restored++
	}
}

func withoutStaleVolatileKeys(config map[string]string) map[string]string {
	filtered := map[string]string{}
	for key, value := range config {
		if strings.HasPrefix(key, "volatile.") && key != keptVolatileKey {
			continue
		}
		filtered[key] = value
	}
	return filtered
}

// restoreLoadBalancers keeps only the load balancers whose VIP Titan's load-balancer list still has.
func restoreLoadBalancers(ctx context.Context, p provider.Provider, project backup.ProjectConfig, liveVips map[string]bool,
	logger *slog.Logger, summary *Summary) {
	name := project.Project.Name
	for _, group := range project.LoadBalancers {
		for _, loadBalancer := range group.LoadBalancers {
			key := name + "/" + group.Network + "/" + loadBalancer.ListenAddress
			if !liveVips[key] {
				logger.Info("load balancer skipped: titan no longer lists its vip", "project", name,
					"network", group.Network, "vip", loadBalancer.ListenAddress)
				summary.Skipped++
				continue
			}
			post := api.NetworkLoadBalancersPost{NetworkLoadBalancerPut: loadBalancer.NetworkLoadBalancerPut, ListenAddress: loadBalancer.ListenAddress}
			if err := p.CreateNetworkLoadBalancer(ctx, name, group.Network, post); err != nil {
				logger.Warn("restore load balancer failed", "project", name, "network", group.Network,
					"vip", loadBalancer.ListenAddress, "error", err)
				summary.Failed++
				continue
			}
			logger.Info("load balancer restored", "project", name, "network", group.Network, "vip", loadBalancer.ListenAddress)
			summary.Restored++
		}
	}
}
