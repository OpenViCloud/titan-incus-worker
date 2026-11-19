package backup

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lxc/incus/v7/shared/api"
	"go.yaml.in/yaml/v4"

	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

// fakeStore records each object put, or fails.
type fakeStore struct {
	put map[string][]byte
	err error
}

func (f *fakeStore) Put(ctx context.Context, key string, body []byte) error {
	if f.err != nil {
		return f.err
	}
	if f.put == nil {
		f.put = map[string][]byte{}
	}
	f.put[key] = body
	return nil
}

func newCluster(p *testsupport.Provider, configured Target, store *fakeStore) *Cluster {
	h := NewCluster(clusters(p), configured, store)
	h.now = func() time.Time { return takenAt }
	return h
}

func clusterLeased() command.Leased {
	return leased("cluster.backup", map[string]any{})
}

// A cluster with the uplink in default and one tenant project holding a network with public IPv6, its ACL and a
// load balancer on it.
func tenantCluster() *testsupport.Provider {
	p := testsupport.NewProvider()
	p.Projects["default"] = true
	p.Projects[project] = true
	p.Profiles = map[string][]api.Profile{
		"default": {{Name: "default"}},
		project:   {{Name: "default", ProfilePut: api.ProfilePut{Description: "tenant default"}}},
	}
	p.Networks = []api.Network{
		{Name: "ovnup", Project: "default", Type: "physical"},
		{Name: "net-1", Project: project, Type: "ovn", NetworkPut: api.NetworkPut{Config: map[string]string{
			"ipv4.address": "10.81.0.1/24", "volatile.network.ipv6.address": "2001:db8:0:d401::1"}}},
	}
	p.ACLs[project+"/acl-1"] = &api.NetworkACL{NetworkACLPost: api.NetworkACLPost{Name: "acl-1"}, Project: project,
		NetworkACLPut: api.NetworkACLPut{Ingress: []api.NetworkACLRule{{Action: "allow", Description: "rule-1", State: "enabled"}}}}
	p.LoadBalancers[project+"/net-1/2001:db8:0:d401::1:1"] = &api.NetworkLoadBalancer{ListenAddress: "2001:db8:0:d401::1:1"}
	p.Leases[project+"/net-1"] = []api.NetworkLease{
		{Hostname: "vm-1", Hwaddr: "00:16:3e:00:00:01", Address: "10.81.0.50", Type: "dynamic"},
		{Hostname: "vm-1", Hwaddr: "00:16:3e:00:00:01", Address: "2001:db8:0:d401::50", Type: "dynamic"},
		{Hostname: "net-1", Hwaddr: "00:16:3e:00:00:ff", Address: "10.81.0.1", Type: "gateway"},
	}
	return p
}

// The day's config of the cluster's tenant projects goes to the bucket as YAML, under the cluster's key
// (titan-ops features/backup/design.md#cluster-backup). The uplink and the default project are the host's, not a tenant's.
func TestCluster_WritesTheConfigOfEveryTenantProject_ToTheClustersKey(t *testing.T) {
	store := &fakeStore{}

	err := newCluster(tenantCluster(), target, store).Run(context.Background(), clusterLeased(), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body, written := store.put["incus/mock/cluster/20261026T020709Z.yaml"]
	if !written || len(store.put) != 1 {
		t.Fatalf("put: %v", store.put)
	}
	var config ClusterConfig
	if err := yaml.Unmarshal(body, &config); err != nil {
		t.Fatal(err)
	}
	if config.ClusterId != "mock" || len(config.Projects) != 1 || config.Projects[0].Project.Name != project {
		t.Fatalf("config: %+v", config)
	}
	tenant := config.Projects[0]
	if len(tenant.Profiles) != 1 || tenant.Profiles[0].Description != "tenant default" {
		t.Fatalf("profiles: %+v", tenant.Profiles)
	}
	if len(tenant.Networks) != 1 || tenant.Networks[0].Config["volatile.network.ipv6.address"] != "2001:db8:0:d401::1" {
		t.Fatalf("networks: %+v", tenant.Networks)
	}
	if len(tenant.NetworkAcls) != 1 || tenant.NetworkAcls[0].Ingress[0].Description != "rule-1" {
		t.Fatalf("acls: %+v", tenant.NetworkAcls)
	}
	if len(tenant.LoadBalancers) != 1 || tenant.LoadBalancers[0].Network != "net-1" ||
		tenant.LoadBalancers[0].LoadBalancers[0].ListenAddress != "2001:db8:0:d401::1:1" {
		t.Fatalf("load balancers: %+v", tenant.LoadBalancers)
	}
	if strings.Contains(string(body), "ovnup") {
		t.Fatalf("the uplink is in the config:\n%s", body)
	}
	// The gateway lease and the IPv6 one are dropped; only the instance's IPv4 is kept.
	if len(tenant.InstanceAddresses) != 1 || tenant.InstanceAddresses[0].Network != "net-1" {
		t.Fatalf("instance addresses: %+v", tenant.InstanceAddresses)
	}
	addresses := tenant.InstanceAddresses[0].Addresses
	if len(addresses) != 1 || addresses["vm-1"] != "10.81.0.50" {
		t.Fatalf("instance addresses: %+v", addresses)
	}
}

func TestCluster_ABucketThatRefuses_IsRetryable(t *testing.T) {
	store := &fakeStore{err: errors.New("put object: AccessDenied")}

	err := newCluster(tenantCluster(), target, store).Run(context.Background(), clusterLeased(), testsupport.DiscardLogger())

	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want a retryable error, got %v", err)
	}
}

func TestCluster_NoTargetConfigured_IsPermanent_AndWritesNothing(t *testing.T) {
	store := &fakeStore{}
	missing := target
	missing.Bucket = ""

	err := newCluster(tenantCluster(), missing, store).Run(context.Background(), clusterLeased(), testsupport.DiscardLogger())

	if err == nil || command.IsRetryable(err) || len(store.put) != 0 {
		t.Fatalf("err %v, put %v", err, store.put)
	}
}

// A read of Incus that fails writes nothing: a config with a project missing would rebuild a cluster without it.
func TestCluster_AReadOfIncusThatFails_WritesNothing_AndIsRetryable(t *testing.T) {
	p := tenantCluster()
	p.ListACLsErr = errors.New("connection reset")
	store := &fakeStore{}

	err := newCluster(p, target, store).Run(context.Background(), clusterLeased(), testsupport.DiscardLogger())

	if err == nil || !command.IsRetryable(err) || len(store.put) != 0 {
		t.Fatalf("err %v, put %v", err, store.put)
	}
}

// A failed lease read is the same as any other failed read of the cluster: nothing is written, and the lease is a
// temporary condition (network trouble), not a reason to give up.
func TestCluster_ALeaseReadThatFails_WritesNothing_AndIsRetryable(t *testing.T) {
	p := tenantCluster()
	p.ListLeasesErr = errors.New("connection reset")
	store := &fakeStore{}

	err := newCluster(p, target, store).Run(context.Background(), clusterLeased(), testsupport.DiscardLogger())

	if err == nil || !command.IsRetryable(err) || len(store.put) != 0 {
		t.Fatalf("err %v, put %v", err, store.put)
	}
}
