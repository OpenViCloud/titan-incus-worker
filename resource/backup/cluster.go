package backup

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/lxc/incus/v7/shared/api"
	"go.yaml.in/yaml/v4"

	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
)

// tenantProjectPrefix marks Titan's projects (Tenant.ProjectNameFor): the rest (default and its uplink) is the host's,
// which the host's install builds again.
const tenantProjectPrefix = "t-"

// ClusterConfig is the YAML a cluster.backup writes: every tenant project with what a restore builds before it imports
// the disks (titan-ops features/backup/design.md#cluster-backup). Instances and volumes are not in it: each has its own
// archive.
type ClusterConfig struct {
	ClusterId string          `yaml:"clusterId"`
	TakenAt   time.Time       `yaml:"takenAt"`
	Projects  []ProjectConfig `yaml:"projects"`
}

type ProjectConfig struct {
	Project       api.Project            `yaml:"project"`
	Profiles      []api.Profile          `yaml:"profiles"`
	Networks      []api.Network          `yaml:"networks"`
	NetworkAcls   []api.NetworkACL       `yaml:"networkAcls"`
	LoadBalancers []NetworkLoadBalancers `yaml:"loadBalancers"`
	// InstanceAddresses: an older restore reads a config without this field and leaves every NIC alone
	// (titan-ops features/backup/design.md#cluster-restore).
	InstanceAddresses []NetworkInstanceAddresses `yaml:"instanceAddresses"`
}

// NetworkLoadBalancers are the load balancers of one network: Incus lists them per network.
type NetworkLoadBalancers struct {
	Network       string                    `yaml:"network"`
	LoadBalancers []api.NetworkLoadBalancer `yaml:"loadBalancers"`
}

// NetworkInstanceAddresses are the IPv4 addresses this network's instance ports hold at backup time, stopped VMs
// included: dynamic until a restore pins them static. A dynamic one follows import order, so a different restore order
// would otherwise land each VM on a different address (measured on dev, Incus 7.0.2, OVN; titan-ops features/backup/design.md#cluster-restore).
type NetworkInstanceAddresses struct {
	Network string `yaml:"network"`
	// Addresses is keyed by instance name (the lease's hostname), each value its IPv4.
	Addresses map[string]string `yaml:"addresses"`
}

// Store writes one object to the backup bucket; package bucket is the real one.
type Store interface {
	Put(ctx context.Context, key string, body []byte) error
}

// Cluster runs cluster.backup: it reads the config through the Incus API and writes it whole, or nothing. The schedule
// asks it after the cluster's disk backups of the day, so the config is newer than every archive of the day.
type Cluster struct {
	clusters cluster.Clusters
	target   Target
	store    Store
	now      func() time.Time
}

func NewCluster(clusters cluster.Clusters, target Target, store Store) *Cluster {
	return &Cluster{clusters: clusters, target: target, store: store, now: time.Now}
}

func (h *Cluster) Run(ctx context.Context, leased command.Leased, logger *slog.Logger) error {
	if err := h.target.usable(); err != nil {
		return err
	}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return err
	}
	takenAt := h.now()
	config, err := ReadClusterConfig(ctx, cl.Provider, leased.ClusterId, takenAt)
	if err != nil {
		return err
	}
	body, err := yaml.Marshal(config)
	if err != nil {
		return command.Permanent(fmt.Errorf("write the config of cluster %s as YAML: %w", leased.ClusterId, err))
	}
	key := ConfigObjectKey(leased.ClusterId, takenAt)
	if err := h.store.Put(ctx, key, body); err != nil {
		return err
	}
	logger.Info("cluster config uploaded", "object", key, "projects", len(config.Projects), "bytes", len(body))
	return nil
}

// GiveUp has nothing to remove: a run writes one object or none.
func (h *Cluster) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	return nil
}

// ReadClusterConfig reads every tenant project of the cluster. Any read that fails fails the whole: a config missing a
// project would rebuild a cluster without it.
func ReadClusterConfig(ctx context.Context, p provider.Provider, clusterId string, takenAt time.Time) (ClusterConfig, error) {
	projects, err := p.ListProjects(ctx)
	if err != nil {
		return ClusterConfig{}, err
	}
	networks, err := p.ListNetworksAllProjects(ctx)
	if err != nil {
		return ClusterConfig{}, err
	}
	acls, err := p.ListNetworkACLsAllProjects(ctx)
	if err != nil {
		return ClusterConfig{}, err
	}
	config := ClusterConfig{ClusterId: clusterId, TakenAt: takenAt.UTC(), Projects: []ProjectConfig{}}
	for _, project := range projects {
		if !strings.HasPrefix(project.Name, tenantProjectPrefix) {
			continue
		}
		profiles, err := p.ListProfiles(ctx, project.Name)
		if err != nil {
			return ClusterConfig{}, err
		}
		tenant := ProjectConfig{Project: project, Profiles: profiles, Networks: []api.Network{},
			NetworkAcls: []api.NetworkACL{}, LoadBalancers: []NetworkLoadBalancers{}, InstanceAddresses: []NetworkInstanceAddresses{}}
		for _, network := range networks {
			if network.Project != project.Name {
				continue
			}
			tenant.Networks = append(tenant.Networks, network)
			loadBalancers, err := p.ListNetworkLoadBalancers(ctx, project.Name, network.Name)
			if err != nil {
				return ClusterConfig{}, err
			}
			if len(loadBalancers) > 0 {
				tenant.LoadBalancers = append(tenant.LoadBalancers, NetworkLoadBalancers{Network: network.Name, LoadBalancers: loadBalancers})
			}
			addresses, err := instanceIpv4Addresses(ctx, p, project.Name, network.Name)
			if err != nil {
				return ClusterConfig{}, err
			}
			if len(addresses) > 0 {
				tenant.InstanceAddresses = append(tenant.InstanceAddresses, NetworkInstanceAddresses{Network: network.Name, Addresses: addresses})
			}
		}
		for _, acl := range acls {
			if acl.Project == project.Name {
				tenant.NetworkAcls = append(tenant.NetworkAcls, acl)
			}
		}
		config.Projects = append(config.Projects, tenant)
	}
	return config, nil
}

// instanceIpv4Addresses reads one network's DHCP leases and keeps each instance's IPv4, dropping its gateway entry
// and every IPv6 one (a network's public uplink gives it IPv6 too, but the NIC's pinned address on restore is
// IPv4 only, titan-ops features/backup/design.md#cluster-restore).
func instanceIpv4Addresses(ctx context.Context, p provider.Provider, project string, network string) (map[string]string, error) {
	leases, err := p.ListNetworkLeases(ctx, project, network)
	if err != nil {
		return nil, err
	}
	addresses := map[string]string{}
	for _, lease := range leases {
		if lease.Type == "gateway" || strings.Contains(lease.Address, ":") {
			continue
		}
		addresses[lease.Hostname] = lease.Address
	}
	return addresses, nil
}
