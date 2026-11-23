// Package drill runs restore.drill: it imports one VM or volume from its newest backup archive into a temporary
// project of the cluster, takes the steps of a real restore, and deletes the project, so a backup is shown to restore
// before a day comes when one is needed (titan-ops features/backup/design.md#restore-drill).
package drill

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"strings"
	"time"

	"github.com/lxc/incus/v7/shared/api"
	"github.com/lxc/incus/v7/shared/units"

	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/resource/backup"
	"github.com/OpenViCloud/titan-incus-worker/resource/restore"
)

// ProjectPrefix starts the name of every project a drill makes. It must never be "t-", the prefix of a tenant's
// project (Tenant.ProjectNameFor): the cluster config backup, the reconcilers and a tenant's own lookups tell the two apart by it.
const ProjectPrefix = "drill-"

// A drill needs room for what it imports with half as much again, and a tenth of the pool beyond that, so a drill
// never takes the pool a tenant's next write needs (measured 2026-11-23: an import grew the pool by 1.03x the root
// disk; titan-ops features/backup/design.md#restore-drill).
const (
	marginDivisor    = 2
	reserveDivisor   = 10
	kindVm           = "vm"
	kindVolume       = "volume"
	cleanupTimeLimit = 15 * time.Minute
)

// payload mirrors ProviderCommandQueue.EnqueueRestoreDrillAsync. The worker reads the VM's devices from Incus to know
// the networks and volumes the archive names; it has no database.
type payload struct {
	Kind        string `json:"kind"`
	ResourceId  string `json:"resourceId"`
	ExternalId  string `json:"externalId"`
	ProjectName string `json:"projectName"`
	TenantId    string `json:"tenantId"`
}

// Drill runs restore.drill. It is on a lease loop of its own: an import is as long as the upload of its disk, and
// vm.backup must not wait behind it (titan-ops architecture/worker-structure.md#worker-roles).
type Drill struct {
	clusters cluster.Clusters
	objects  restore.Objects
	now      func() time.Time
}

func New(clusters cluster.Clusters, objects restore.Objects) *Drill {
	return &Drill{clusters: clusters, objects: objects, now: time.Now}
}

// disk is one archive a drill imports: a volume the VM has attached, or the resource itself.
type disk struct {
	name      string
	key       string
	sizeBytes int64
}

func (h *Drill) Run(ctx context.Context, leased command.Leased, logger *slog.Logger) error {
	in, err := decode(leased)
	if err != nil {
		return err
	}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return err
	}
	p, storagePool := cl.Provider, provider.StoragePoolOrDefault(cl.StoragePool)
	logger = logger.With("kind", in.Kind, "resource", in.ExternalId)

	// Any leftover of an earlier run goes first: a project it died in holds pool space this run is about to measure.
	if err := removeLeftovers(ctx, p, storagePool, logger); err != nil {
		return command.Permanent(err)
	}

	plan, err := h.plan(ctx, p, storagePool, leased.ClusterId, in)
	if err != nil {
		return command.Permanent(err)
	}
	if err := checkPoolSpace(ctx, p, storagePool, plan, logger); err != nil {
		return command.Permanent(err)
	}

	project := ProjectPrefix + h.now().UTC().Format("20060102")
	made := &made{project: project}
	cleaned := false
	defer func() {
		if cleaned {
			return
		}
		// The drill's own context may be what failed it: the cleanup has a lifetime of its own.
		cleanCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeLimit)
		defer cancel()
		if err := made.remove(cleanCtx, p, storagePool); err != nil {
			logger.Warn("removing the drill's project failed; the next drill removes it", "project", project, "error", err)
		}
	}()

	if err := provider.EnsureProject(ctx, p, project); err != nil {
		return command.Permanent(fmt.Errorf("create project %s: %w", project, err))
	}
	made.created = true
	logger.Info("restore drill started", "project", project, "disks", len(plan.volumes)+1)

	for _, network := range plan.networks {
		if err := p.CreateNetwork(ctx, project, networkOf(network)); err != nil {
			return command.Permanent(fmt.Errorf("create network %s in %s: %w", network, project, err))
		}
		made.networks = append(made.networks, network)
	}
	// Volumes first: a VM archive's devices name its attached volumes by their Incus name.
	for _, volume := range plan.volumes {
		made.volumes = append(made.volumes, volume.name)
		if err := restore.ImportVolume(ctx, p, storagePool, project, volume.name, volume.key, h.objects, logger); err != nil {
			return command.Permanent(err)
		}
	}
	if in.Kind == kindVm {
		made.vm = plan.root.name
		if err := restore.ImportVm(ctx, p, storagePool, project, plan.root.name, plan.root.key, h.objects, logger); err != nil {
			return command.Permanent(err)
		}
	} else {
		made.volumes = append(made.volumes, plan.root.name)
		if err := restore.ImportVolume(ctx, p, storagePool, project, plan.root.name, plan.root.key, h.objects, logger); err != nil {
			return command.Permanent(err)
		}
	}

	cleaned = true
	if err := made.remove(ctx, p, storagePool); err != nil {
		cleaned = false
		return command.Permanent(err)
	}
	logger.Info("restore drill verified", "project", project, "object", plan.root.key)
	return nil
}

// GiveUp removes the drill projects a run that died left; the command has no callback.
func (h *Drill) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return nil
	}
	if err := removeLeftovers(ctx, cl.Provider, provider.StoragePoolOrDefault(cl.StoragePool), logger); err != nil {
		logger.Warn("removing the drill's leftovers failed; the next drill removes them", "error", err)
	}
	return nil
}

func decode(leased command.Leased) (payload, error) {
	var in payload
	if err := command.DecodePayload(leased, &in); err != nil {
		return payload{}, err
	}
	isKnown := in.Kind == kindVm || in.Kind == kindVolume
	isComplete := in.ResourceId != "" && in.ExternalId != "" && in.ProjectName != "" && in.TenantId != ""
	if !isKnown || !isComplete {
		return payload{}, command.Permanent(errors.New("restore.drill payload needs kind (vm or volume), resourceId, externalId, projectName and tenantId"))
	}
	return in, nil
}

// drillPlan is everything read before the first write: what to import, in which networks it lives, how much room it
// needs.
type drillPlan struct {
	root     disk
	volumes  []disk
	networks []string
}

// plan reads the tenant's VM or volume (never writes it) and finds the newest archive of each disk the drill imports.
func (h *Drill) plan(ctx context.Context, p provider.Provider, storagePool string, clusterId string, in payload) (drillPlan, error) {
	if in.Kind == kindVolume {
		root, err := h.volumeDisk(ctx, p, storagePool, clusterId, in.TenantId, in.ProjectName, in.ExternalId, in.ResourceId)
		return drillPlan{root: root}, err
	}
	instance, err := p.GetInstance(ctx, in.ProjectName, in.ExternalId)
	if provider.IsNotFound(err) {
		return drillPlan{}, fmt.Errorf("vm %s is not in project %s: nothing to drill", in.ExternalId, in.ProjectName)
	}
	if err != nil {
		return drillPlan{}, fmt.Errorf("read vm %s: %w", in.ExternalId, err)
	}
	key, err := h.newestArchive(ctx, clusterId, in.TenantId, "vms", in.ResourceId)
	if err != nil {
		return drillPlan{}, fmt.Errorf("vm %s: %w", in.ExternalId, err)
	}
	result := drillPlan{root: disk{name: in.ExternalId, key: key}}
	for name, device := range instance.ExpandedDevices {
		switch {
		case device["type"] == "nic" && device["network"] != "":
			result.networks = append(result.networks, device["network"])
		case device["type"] == "disk" && device["source"] != "":
			volumeId, err := provider.VolumeIdFromName(device["source"])
			if err != nil {
				return drillPlan{}, fmt.Errorf("device %s of vm %s names %q: %w", name, in.ExternalId, device["source"], err)
			}
			volume, err := h.volumeDisk(ctx, p, storagePool, clusterId, in.TenantId, in.ProjectName, device["source"], volumeId)
			if err != nil {
				return drillPlan{}, err
			}
			result.volumes = append(result.volumes, volume)
		case device["type"] == "disk" && device["path"] == "/":
			result.root.sizeBytes = parseSize(device["size"])
		}
	}
	return result, nil
}

func (h *Drill) volumeDisk(ctx context.Context, p provider.Provider, storagePool string, clusterId string, tenantId string,
	project string, name string, id string) (disk, error) {
	volume, err := p.GetVolume(ctx, project, storagePool, name)
	if provider.IsNotFound(err) {
		return disk{}, fmt.Errorf("volume %s is not in project %s: nothing to drill", name, project)
	}
	if err != nil {
		return disk{}, fmt.Errorf("read volume %s: %w", name, err)
	}
	key, err := h.newestArchive(ctx, clusterId, tenantId, "volumes", id)
	if err != nil {
		return disk{}, fmt.Errorf("volume %s: %w", name, err)
	}
	return disk{name: name, key: key, sizeBytes: parseSize(volume.Config["size"])}, nil
}

// newestArchive is the newest archive of one resource at or before now, found by listing its own folder of the bucket.
func (h *Drill) newestArchive(ctx context.Context, clusterId string, tenantId string, collection string, id string) (string, error) {
	prefix := path.Dir(backup.ObjectKey(clusterId, tenantId, collection, id, time.Time{})) + "/"
	keys, err := h.objects.List(ctx, prefix)
	if err != nil {
		return "", fmt.Errorf("list the archives under %s: %w", prefix, err)
	}
	var best string
	var bestTime time.Time
	now := h.now()
	for _, key := range keys {
		name, isArchive := strings.CutSuffix(path.Base(key), ".tar.gz")
		takenAt, err := time.Parse(backup.TimeLayout, name)
		if !isArchive || err != nil || takenAt.After(now) {
			continue
		}
		if best == "" || takenAt.After(bestTime) {
			best, bestTime = key, takenAt
		}
	}
	if best == "" {
		return "", fmt.Errorf("has no backup archive under %s", prefix)
	}
	return best, nil
}

// checkPoolSpace refuses the drill when the pool lacks room for it: free >= 1.5 x the bytes imported + a tenth of the
// pool. The message says what was measured.
func checkPoolSpace(ctx context.Context, p provider.Provider, storagePool string, plan drillPlan, logger *slog.Logger) error {
	resources, err := p.GetStoragePoolResources(ctx, storagePool)
	if err != nil {
		return err
	}
	imported := plan.root.sizeBytes
	for _, volume := range plan.volumes {
		imported += volume.sizeBytes
	}
	if imported == 0 {
		logger.Warn("no disk size known for the drill; only the pool reserve is checked")
	}
	space := resources.Space
	free := uint64(0)
	if space.Total > space.Used {
		free = space.Total - space.Used
	}
	need := uint64(imported) + uint64(imported)/marginDivisor + space.Total/reserveDivisor
	if free < need {
		return fmt.Errorf("pool %s has %s free, a drill of %s needs %s (1.5 x the disks + a tenth of the pool): not enough space",
			storagePool, inGiB(free), inGiB(uint64(imported)), inGiB(need))
	}
	return nil
}

func inGiB(bytes uint64) string {
	return fmt.Sprintf("%.1f GiB", float64(bytes)/float64(1<<30))
}

func parseSize(text string) int64 {
	if text == "" {
		return 0
	}
	size, err := units.ParseByteSizeString(text)
	if err != nil {
		return 0
	}
	return size
}

// networkOf is a network of the tenant's name without an uplink: the archive's NIC and its rollback look the network
// up by name in the project, and nothing of the drill's ever carries traffic.
func networkOf(name string) api.NetworksPost {
	return api.NetworksPost{
		Name: name,
		Type: "ovn",
		NetworkPut: api.NetworkPut{Config: map[string]string{
			"network": "none", "ipv4.address": "none", "ipv6.address": "none",
		}},
	}
}

// made is what one run created, removed in reverse: the VM, its volumes, the networks, then the project.
type made struct {
	project  string
	created  bool
	vm       string
	volumes  []string
	networks []string
}

func (m *made) remove(ctx context.Context, p provider.Provider, storagePool string) error {
	if !m.created {
		return nil
	}
	if m.vm != "" {
		if err := p.DeleteInstance(ctx, m.project, m.vm); err != nil && !provider.IsNotFound(err) {
			return fmt.Errorf("delete vm %s: %w", m.vm, err)
		}
	}
	for _, volume := range m.volumes {
		if err := p.DeleteVolume(ctx, m.project, storagePool, volume); err != nil && !provider.IsNotFound(err) {
			return fmt.Errorf("delete volume %s: %w", volume, err)
		}
	}
	for _, network := range m.networks {
		if err := p.DeleteNetwork(ctx, m.project, network); err != nil && !provider.IsNotFound(err) {
			return fmt.Errorf("delete network %s: %w", network, err)
		}
	}
	if err := p.DeleteProject(ctx, m.project); err != nil && !provider.IsNotFound(err) {
		return fmt.Errorf("delete project %s: %w", m.project, err)
	}
	return nil
}

// removeLeftovers deletes every drill project the cluster still has, with what it holds. It lists what is there
// instead of trusting a record: it is for the run that died before it could record anything.
func removeLeftovers(ctx context.Context, p provider.Provider, storagePool string, logger *slog.Logger) error {
	projects, err := p.ListProjects(ctx)
	if err != nil {
		return fmt.Errorf("list projects: %w", err)
	}
	var left []string
	for _, project := range projects {
		if strings.HasPrefix(project.Name, ProjectPrefix) {
			left = append(left, project.Name)
		}
	}
	if len(left) == 0 {
		return nil
	}
	instances, err := p.ListVirtualMachinesAllProjects(ctx)
	if err != nil {
		return fmt.Errorf("list vms: %w", err)
	}
	volumes, err := p.ListVolumesAllProjects(ctx, storagePool)
	if err != nil {
		return fmt.Errorf("list volumes: %w", err)
	}
	networks, err := p.ListNetworksAllProjects(ctx)
	if err != nil {
		return fmt.Errorf("list networks: %w", err)
	}
	for _, project := range left {
		held := made{project: project, created: true}
		for _, instance := range instances {
			if instance.Project == project {
				// A project holds several instances only if a run was interrupted twice: delete each by name.
				if err := p.DeleteInstance(ctx, project, instance.Name); err != nil && !provider.IsNotFound(err) {
					return fmt.Errorf("delete vm %s of leftover project %s: %w", instance.Name, project, err)
				}
			}
		}
		for _, volume := range volumes {
			if volume.Project == project && volume.Type == "custom" {
				held.volumes = append(held.volumes, volume.Name)
			}
		}
		for _, network := range networks {
			if network.Project == project {
				held.networks = append(held.networks, network.Name)
			}
		}
		if err := held.remove(ctx, p, storagePool); err != nil {
			return err
		}
		logger.Info("leftover drill project removed", "project", project)
	}
	return nil
}
