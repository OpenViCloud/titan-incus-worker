package restore

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/resource/backup"
)

// The row types below are declared locally rather than imported from each resource/<kind>/reconcile.go: a resource
// package may not import another (dependency_test.go), and the restore's one allowed edge is to resource/backup only.
// Each mirrors the control plane's ReconcilableXQueryResult that the matching reconciler already decodes.

type titanVm struct {
	Id           string  `json:"id"`
	ExternalId   string  `json:"externalId"`
	CurrentState *string `json:"currentState"`
	ProjectName  string  `json:"projectName"`
}

type titanVolume struct {
	Id          string `json:"id"`
	ExternalId  string `json:"externalId"`
	ProjectName string `json:"projectName"`
}

type titanVolumeSnapshot struct {
	Id          string `json:"id"`
	VolumeId    string `json:"volumeId"`
	ExternalId  string `json:"externalId"` // "<volume>/<snapshot>"
	ProjectName string `json:"projectName"`
}

type titanStorage struct {
	Volumes   []titanVolume         `json:"volumes"`
	Snapshots []titanVolumeSnapshot `json:"snapshots"`
}

type titanVmSnapshot struct {
	Id          string `json:"id"`
	VmId        string `json:"vmId"`
	ExternalId  string `json:"externalId"` // "<instance>/<snapshot>"
	ProjectName string `json:"projectName"`
}

type titanNetwork struct {
	Id          string `json:"id"`
	ExternalId  string `json:"externalId"`
	ProjectName string `json:"projectName"`
}

type titanFirewallRule struct {
	Id         string `json:"id"`
	NetworkId  string `json:"networkId"`
	ExternalId string `json:"externalId"` // "<aclName>/<ruleId>"
}

type titanLoadBalancer struct {
	Id                string `json:"id"`
	ProjectName       string `json:"projectName"`
	ExternalNetworkId string `json:"externalNetworkId"`
	Vip               string `json:"vip"`
}

type titanTemplate struct {
	Id                 string `json:"id"`
	ExternalTemplateId string `json:"externalTemplateId"`
}

type titanLists struct {
	Vms             []titanVm
	Volumes         []titanVolume
	VolumeSnapshots []titanVolumeSnapshot
	VmSnapshots     []titanVmSnapshot
	Networks        []titanNetwork
	FirewallRules   []titanFirewallRule
	LoadBalancers   []titanLoadBalancer
	Templates       []titanTemplate
}

// readTitanLists reads every list the restore needs. A failed read aborts the whole run: reading it as empty would
// make the restore skip or delete everything of that kind (titan-ops features/backup/design.md#cluster-restore).
func readTitanLists(ctx context.Context, cp *controlplane.Client, clusterId string) (titanLists, error) {
	query := "?clusterId=" + url.QueryEscape(clusterId)
	var lists titanLists
	if err := cp.GetJSON(ctx, "/internal/reconcile/vms"+query, &lists.Vms); err != nil {
		return titanLists{}, fmt.Errorf("list titan vms: %w", err)
	}
	var storage titanStorage
	if err := cp.GetJSON(ctx, "/internal/reconcile/volumes"+query, &storage); err != nil {
		return titanLists{}, fmt.Errorf("list titan volumes: %w", err)
	}
	lists.Volumes, lists.VolumeSnapshots = storage.Volumes, storage.Snapshots
	if err := cp.GetJSON(ctx, "/internal/reconcile/vm-snapshots"+query, &lists.VmSnapshots); err != nil {
		return titanLists{}, fmt.Errorf("list titan vm snapshots: %w", err)
	}
	if err := cp.GetJSON(ctx, "/internal/reconcile/networks"+query, &lists.Networks); err != nil {
		return titanLists{}, fmt.Errorf("list titan networks: %w", err)
	}
	if err := cp.GetJSON(ctx, "/internal/reconcile/firewall-rules"+query, &lists.FirewallRules); err != nil {
		return titanLists{}, fmt.Errorf("list titan firewall rules: %w", err)
	}
	if err := cp.GetJSON(ctx, "/internal/reconcile/load-balancers"+query, &lists.LoadBalancers); err != nil {
		return titanLists{}, fmt.Errorf("list titan load balancers: %w", err)
	}
	if err := cp.GetJSON(ctx, "/internal/reconcile/templates"+query, &lists.Templates); err != nil {
		return titanLists{}, fmt.Errorf("list titan templates: %w", err)
	}
	return lists, nil
}

// liveNetworkIdsByProject: a network's YAML name is "net-"+Id and its ACL's is "acl-"+Id (provider.NetworkName,
// provider.NetworkAclName), so the network row's own Id is what both compare against.
func liveNetworkIdsByProject(rows []titanNetwork) map[string]map[string]bool {
	live := map[string]map[string]bool{}
	for _, row := range rows {
		if live[row.ProjectName] == nil {
			live[row.ProjectName] = map[string]bool{}
		}
		live[row.ProjectName][row.Id] = true
	}
	return live
}

// liveFirewallRuleIds: a rule's externalId is "<aclName>/<ruleId>" (firewall.ProviderId); the ACL rule's own
// Description carries the bare ruleId (resource/firewall/rule.go), so only the id after the last "/" matters here.
func liveFirewallRuleIds(rows []titanFirewallRule) map[string]bool {
	live := map[string]bool{}
	for _, row := range rows {
		if row.ExternalId == "" {
			continue
		}
		ruleId := row.ExternalId
		if last := strings.LastIndex(row.ExternalId, "/"); last >= 0 {
			ruleId = row.ExternalId[last+1:]
		}
		live[ruleId] = true
	}
	return live
}

// instanceIpv4ByName flattens every project's recorded addresses to instance name -> IPv4: an instance's Incus name
// is unique across the whole cluster, and a VM has one NIC (eth0), so which network it came from does not matter
// here (titan-ops features/backup/design.md#cluster-restore). A config from before this field existed has no
// InstanceAddresses, so the map comes back empty and no VM gets pinned.
func instanceIpv4ByName(projects []backup.ProjectConfig) map[string]string {
	addresses := map[string]string{}
	for _, project := range projects {
		for _, group := range project.InstanceAddresses {
			for instance, address := range group.Addresses {
				addresses[instance] = address
			}
		}
	}
	return addresses
}

// liveLoadBalancerVips keys by project + "/" + network + "/" + VIP, exactly what the YAML's NetworkLoadBalancers
// group (its Network) and each load balancer's ListenAddress give.
func liveLoadBalancerVips(rows []titanLoadBalancer) map[string]bool {
	live := map[string]bool{}
	for _, row := range rows {
		live[row.ProjectName+"/"+row.ExternalNetworkId+"/"+row.Vip] = true
	}
	return live
}

// liveVmSnapshotNamesByInstance keys by project + "/" + instance (provider.ProjectKey), each value the bare
// snapshot names Titan still lists for it.
func liveVmSnapshotNamesByInstance(rows []titanVmSnapshot) map[string]map[string]bool {
	live := map[string]map[string]bool{}
	for _, row := range rows {
		instance, snapshot, err := provider.SplitSnapshotId(row.ExternalId)
		if err != nil {
			continue
		}
		key := provider.ProjectKey(row.ProjectName, instance)
		if live[key] == nil {
			live[key] = map[string]bool{}
		}
		live[key][snapshot] = true
	}
	return live
}

// liveVolumeSnapshotNamesByVolume keys by project + "/" + volume, each value the bare snapshot names Titan still
// lists for it.
func liveVolumeSnapshotNamesByVolume(rows []titanVolumeSnapshot) map[string]map[string]bool {
	live := map[string]map[string]bool{}
	for _, row := range rows {
		volume, snapshot, err := provider.SplitSnapshotId(row.ExternalId)
		if err != nil {
			continue
		}
		key := provider.ProjectKey(row.ProjectName, volume)
		if live[key] == nil {
			live[key] = map[string]bool{}
		}
		live[key][snapshot] = true
	}
	return live
}
