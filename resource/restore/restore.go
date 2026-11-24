// Package restore runs the worker's one-shot restore: rebuild one Incus cluster on a freshly reinstalled host from
// the backup bucket, then reconcile with the Titan database through the control plane's existing /internal routes
// (titan-ops features/backup/design.md#cluster-restore). It builds from the bucket, never from Titan's database;
// Titan's database only decides which resources still exist, which VMs to start, and which snapshots or firewall
// rules to remove because Titan deleted them after the backup point.
//
// The steps that take one archive into Incus (ImportVolume, ImportVm and the ones beside them) and the pool-room rule
// are also what the commands that import a single VM or volume run, vm.restore reads the archive's index here, and
// the key by which a restore of one says its copy is finished is kept here for both
// (titan-ops features/backup/design.md#restore-drill, titan-ops features/backup/design.md#restore-as-new-resource).
package restore

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"go.yaml.in/yaml/v4"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/resource/backup"
)

// Objects is the restore's narrow port onto the backup bucket: list keys under a prefix, and open one object for
// streaming. Package bucket implements it (same method set; no explicit wiring needed).
type Objects interface {
	List(ctx context.Context, prefix string) ([]string, error)
	Get(ctx context.Context, key string) (io.ReadCloser, error)
}

// Summary is what one restore run did; main maps Failed > 0 to exit 1.
type Summary struct {
	Restored int
	Skipped  int
	Failed   int
}

// Restore holds what a run needs to reach the control plane, the cluster and the bucket.
type Restore struct {
	cp       *controlplane.Client
	clusters cluster.Clusters
	objects  Objects
}

func New(cp *controlplane.Client, clusters cluster.Clusters, objects Objects) *Restore {
	return &Restore{cp: cp, clusters: clusters, objects: objects}
}

// Run restores one cluster once. point is "" for the newest config object, or a backup.TimeLayout moment: the newest
// at or before it. Run never stops at the first failure: it restores what it can and reports every failure in the
// returned error and Summary, so one bad archive never costs the rest of the cluster its restore.
func (r *Restore) Run(ctx context.Context, clusterId string, point string, logger *slog.Logger) (Summary, error) {
	summary := Summary{}
	logger = logger.With("clusterId", clusterId)

	cl, err := r.clusters.Cluster(ctx, clusterId)
	if err != nil {
		return summary, fmt.Errorf("restore cluster %s: %w", clusterId, err)
	}
	p := cl.Provider
	pool := provider.StoragePoolOrDefault(cl.StoragePool)

	keys, err := r.objects.List(ctx, "incus/"+clusterId+"/")
	if err != nil {
		return summary, fmt.Errorf("list the backup bucket of cluster %s: %w", clusterId, err)
	}
	parsed := parseObjectKeys(keys)
	chosenConfig, found := pickConfig(parsed.Configs, point)
	if !found {
		return summary, fmt.Errorf("no cluster config object for cluster %s at or before %q", clusterId, point)
	}
	logger.Info("restore point chosen", "object", chosenConfig.Key, "takenAt", chosenConfig.TakenAt)

	clusterConfig, err := r.readConfig(ctx, chosenConfig.Key)
	if err != nil {
		return summary, fmt.Errorf("read cluster config %s: %w", chosenConfig.Key, err)
	}

	titan, err := readTitanLists(ctx, r.cp, clusterId)
	if err != nil {
		return summary, fmt.Errorf("read titan's lists of cluster %s: %w", clusterId, err)
	}

	for _, project := range clusterConfig.Projects {
		restoreProject(ctx, p, project, logger, &summary)
	}
	for _, project := range clusterConfig.Projects {
		restoreProfiles(ctx, p, project, logger, &summary)
	}
	liveNetworkIds := liveNetworkIdsByProject(titan.Networks)
	liveRuleIds := liveFirewallRuleIds(titan.FirewallRules)
	for _, project := range clusterConfig.Projects {
		restoreAcls(ctx, p, project, liveNetworkIds[project.Project.Name], liveRuleIds, logger, &summary)
	}
	for _, project := range clusterConfig.Projects {
		restoreNetworks(ctx, p, project, liveNetworkIds[project.Project.Name], logger, &summary)
	}

	restoreVolumes(ctx, p, pool, titan.Volumes, titan.VolumeSnapshots, parsed.Archives, chosenConfig.TakenAt, r.objects, logger, &summary)
	addresses := instanceIpv4ByName(clusterConfig.Projects)
	restoreVms(ctx, p, pool, titan.Vms, titan.VmSnapshots, parsed.Archives, chosenConfig.TakenAt, addresses, r.objects, logger, &summary)
	// Load balancers come after the VMs: Incus's OVN backend refuses one whose target has no logical switch port
	// yet, which only exists once the VM is imported (measured on dev, titan-ops features/backup/design.md#cluster-restore).
	liveVips := liveLoadBalancerVips(titan.LoadBalancers)
	for _, project := range clusterConfig.Projects {
		restoreLoadBalancers(ctx, p, project, liveVips, logger, &summary)
	}
	restoreTemplates(ctx, r.cp, p, titan.Templates, clusterId, logger, &summary)

	logger.Info("restore finished", "restored", summary.Restored, "skipped", summary.Skipped, "failed", summary.Failed)
	if summary.Failed > 0 {
		return summary, fmt.Errorf("restore of cluster %s had %d failure(s)", clusterId, summary.Failed)
	}
	return summary, nil
}

func (r *Restore) readConfig(ctx context.Context, key string) (backup.ClusterConfig, error) {
	body, err := r.objects.Get(ctx, key)
	if err != nil {
		return backup.ClusterConfig{}, err
	}
	defer body.Close()
	data, err := io.ReadAll(body)
	if err != nil {
		return backup.ClusterConfig{}, fmt.Errorf("read %s: %w", key, err)
	}
	var config backup.ClusterConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return backup.ClusterConfig{}, fmt.Errorf("decode %s: %w", key, err)
	}
	return config, nil
}
