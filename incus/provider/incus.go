package provider

import (
	"context"
	"fmt"
	"io"
	"net/url"

	incus "github.com/lxc/incus/v7/client"
	"github.com/lxc/incus/v7/shared/api"
)

// Incus adapts incus.InstanceServer to Provider. The client's calls take no context, so ctx is honoured between
// operation waits, not inside one HTTP request.
type Incus struct {
	server incus.InstanceServer
}

func NewIncus(server incus.InstanceServer) *Incus {
	return &Incus{server: server}
}

func (p *Incus) GetProject(ctx context.Context, name string) (*api.Project, error) {
	project, _, err := p.server.GetProject(name)
	if err != nil {
		return nil, fmt.Errorf("get project %s: %w", name, err)
	}
	return project, nil
}

func (p *Incus) CreateProject(ctx context.Context, project api.ProjectsPost) error {
	if err := p.server.CreateProject(project); err != nil {
		return fmt.Errorf("create project %s: %w", project.Name, err)
	}
	return nil
}

func (p *Incus) GetInstance(ctx context.Context, project string, name string) (*api.Instance, error) {
	instance, _, err := p.server.UseProject(project).GetInstance(name)
	if err != nil {
		return nil, fmt.Errorf("get instance %s in project %s: %w", name, project, err)
	}
	return instance, nil
}

func (p *Incus) CreateInstance(ctx context.Context, project string, instance api.InstancesPost) error {
	operation, err := p.server.UseProject(project).CreateInstance(instance)
	if err != nil {
		return fmt.Errorf("create instance %s in project %s: %w", instance.Name, project, err)
	}
	if err := WaitOperation(ctx, p.server, operation.Get().ID); err != nil {
		return fmt.Errorf("create instance %s in project %s: %w", instance.Name, project, err)
	}
	return nil
}

func (p *Incus) UpdateInstanceState(ctx context.Context, project string, name string, state api.InstanceStatePut) error {
	operation, err := p.server.UseProject(project).UpdateInstanceState(name, state, "")
	if err != nil {
		return fmt.Errorf("%s instance %s in project %s: %w", state.Action, name, project, err)
	}
	if err := WaitOperation(ctx, p.server, operation.Get().ID); err != nil {
		return fmt.Errorf("%s instance %s in project %s: %w", state.Action, name, project, err)
	}
	return nil
}

func (p *Incus) DeleteInstance(ctx context.Context, project string, name string) error {
	operation, err := p.server.UseProject(project).DeleteInstance(name)
	if err != nil {
		return fmt.Errorf("delete instance %s in project %s: %w", name, project, err)
	}
	if err := WaitOperation(ctx, p.server, operation.Get().ID); err != nil {
		return fmt.Errorf("delete instance %s in project %s: %w", name, project, err)
	}
	return nil
}

func (p *Incus) ListVirtualMachinesAllProjects(ctx context.Context) ([]api.InstanceFull, error) {
	instances, err := p.server.GetInstancesFullAllProjects(api.InstanceTypeVM)
	if err != nil {
		return nil, fmt.Errorf("list virtual machines in all projects: %w", err)
	}
	return instances, nil
}

func (p *Incus) CreateNetwork(ctx context.Context, project string, network api.NetworksPost) error {
	if err := p.server.UseProject(project).CreateNetwork(network); err != nil {
		return fmt.Errorf("create network %s in project %s: %w", network.Name, project, err)
	}
	return nil
}

func (p *Incus) DeleteNetwork(ctx context.Context, project string, name string) error {
	if err := p.server.UseProject(project).DeleteNetwork(name); err != nil {
		return fmt.Errorf("delete network %s in project %s: %w", name, project, err)
	}
	return nil
}

func (p *Incus) ListNetworksAllProjects(ctx context.Context) ([]api.Network, error) {
	networks, err := p.server.GetNetworksAllProjects()
	if err != nil {
		return nil, fmt.Errorf("list networks in all projects: %w", err)
	}
	return networks, nil
}

func (p *Incus) GetNetwork(ctx context.Context, project string, name string) (*api.Network, string, error) {
	network, etag, err := p.server.UseProject(project).GetNetwork(name)
	if err != nil {
		return nil, "", fmt.Errorf("get network %s in project %s: %w", name, project, err)
	}
	return network, etag, nil
}

func (p *Incus) UpdateNetwork(ctx context.Context, project string, name string, network api.NetworkPut, etag string) error {
	if err := p.server.UseProject(project).UpdateNetwork(name, network, etag); err != nil {
		return fmt.Errorf("update network %s in project %s: %w", name, project, err)
	}
	return nil
}

func (p *Incus) GetNetworkACL(ctx context.Context, project string, name string) (*api.NetworkACL, string, error) {
	acl, etag, err := p.server.UseProject(project).GetNetworkACL(name)
	if err != nil {
		return nil, "", fmt.Errorf("get network ACL %s in project %s: %w", name, project, err)
	}
	return acl, etag, nil
}

func (p *Incus) CreateNetworkACL(ctx context.Context, project string, acl api.NetworkACLsPost) error {
	if err := p.server.UseProject(project).CreateNetworkACL(acl); err != nil {
		return fmt.Errorf("create network ACL %s in project %s: %w", acl.Name, project, err)
	}
	return nil
}

func (p *Incus) UpdateNetworkACL(ctx context.Context, project string, name string, acl api.NetworkACLPut, etag string) error {
	if err := p.server.UseProject(project).UpdateNetworkACL(name, acl, etag); err != nil {
		return fmt.Errorf("update network ACL %s in project %s: %w", name, project, err)
	}
	return nil
}

func (p *Incus) DeleteNetworkACL(ctx context.Context, project string, name string) error {
	if err := p.server.UseProject(project).DeleteNetworkACL(name); err != nil {
		return fmt.Errorf("delete network ACL %s in project %s: %w", name, project, err)
	}
	return nil
}

func (p *Incus) GetNetworkLoadBalancer(ctx context.Context, project string, network string, listenAddress string) (*api.NetworkLoadBalancer, string, error) {
	loadBalancer, etag, err := p.server.UseProject(project).GetNetworkLoadBalancer(network, listenAddress)
	if err != nil {
		return nil, "", fmt.Errorf("get load balancer %s on network %s in project %s: %w", listenAddress, network, project, err)
	}
	return loadBalancer, etag, nil
}

func (p *Incus) CreateNetworkLoadBalancer(ctx context.Context, project string, network string, loadBalancer api.NetworkLoadBalancersPost) error {
	if err := p.server.UseProject(project).CreateNetworkLoadBalancer(network, loadBalancer); err != nil {
		return fmt.Errorf("create load balancer %s on network %s in project %s: %w", loadBalancer.ListenAddress, network, project, err)
	}
	return nil
}

func (p *Incus) UpdateNetworkLoadBalancer(ctx context.Context, project string, network string, listenAddress string, loadBalancer api.NetworkLoadBalancerPut, etag string) error {
	if err := p.server.UseProject(project).UpdateNetworkLoadBalancer(network, listenAddress, loadBalancer, etag); err != nil {
		return fmt.Errorf("update load balancer %s on network %s in project %s: %w", listenAddress, network, project, err)
	}
	return nil
}

func (p *Incus) DeleteNetworkLoadBalancer(ctx context.Context, project string, network string, listenAddress string) error {
	if err := p.server.UseProject(project).DeleteNetworkLoadBalancer(network, listenAddress); err != nil {
		return fmt.Errorf("delete load balancer %s on network %s in project %s: %w", listenAddress, network, project, err)
	}
	return nil
}

func (p *Incus) ListNetworkLoadBalancers(ctx context.Context, project string, network string) ([]api.NetworkLoadBalancer, error) {
	loadBalancers, err := p.server.UseProject(project).GetNetworkLoadBalancers(network)
	if err != nil {
		return nil, fmt.Errorf("list load balancers on network %s in project %s: %w", network, project, err)
	}
	return loadBalancers, nil
}

func (p *Incus) ListNetworkACLsAllProjects(ctx context.Context) ([]api.NetworkACL, error) {
	acls, err := p.server.GetNetworkACLsAllProjects()
	if err != nil {
		return nil, fmt.Errorf("list network ACLs in all projects: %w", err)
	}
	return acls, nil
}

func (p *Incus) GetImageAlias(ctx context.Context, name string) (*api.ImageAliasesEntry, error) {
	alias, _, err := p.server.GetImageAlias(name)
	if err != nil {
		return nil, fmt.Errorf("get image alias %s: %w", name, err)
	}
	return alias, nil
}

func (p *Incus) ListImageAliases(ctx context.Context) ([]api.ImageAliasesEntry, error) {
	aliases, err := p.server.GetImageAliases()
	if err != nil {
		return nil, fmt.Errorf("list image aliases: %w", err)
	}
	return aliases, nil
}

func (p *Incus) CreateImage(ctx context.Context, image api.ImagesPost) error {
	operation, err := p.server.CreateImage(image, nil)
	if err != nil {
		return fmt.Errorf("create image: %w", err)
	}
	if err := WaitOperation(ctx, p.server, operation.Get().ID); err != nil {
		return fmt.Errorf("create image: %w", err)
	}
	return nil
}

func (p *Incus) ImportImage(ctx context.Context, image api.ImagesPost, metadata io.Reader, rootfs io.Reader) error {
	args := &incus.ImageCreateArgs{
		MetaFile:   metadata,
		MetaName:   "metadata.tar.gz",
		RootfsFile: rootfs,
		RootfsName: "rootfs.img",
		Type:       string(api.InstanceTypeVM),
	}
	operation, err := p.server.CreateImage(image, args)
	if err != nil {
		return fmt.Errorf("import image: %w", err)
	}
	if err := WaitOperation(ctx, p.server, operation.Get().ID); err != nil {
		return fmt.Errorf("import image: %w", err)
	}
	return nil
}

func (p *Incus) ListImages(ctx context.Context) ([]api.Image, error) {
	images, err := p.server.GetImages()
	if err != nil {
		return nil, fmt.Errorf("list images: %w", err)
	}
	return images, nil
}

func (p *Incus) DeleteImage(ctx context.Context, fingerprint string) error {
	operation, err := p.server.DeleteImage(fingerprint)
	if err != nil {
		return fmt.Errorf("delete image %s: %w", fingerprint, err)
	}
	if err := WaitOperation(ctx, p.server, operation.Get().ID); err != nil {
		return fmt.Errorf("delete image %s: %w", fingerprint, err)
	}
	return nil
}

// A raw GET: the client's own server read stores the answer on the client without a lock, and this client is
// shared with the reconcilers.
func (p *Incus) GetServerArchitectures(ctx context.Context) ([]string, error) {
	response, _, err := p.server.RawQuery("GET", "/1.0", nil, "")
	if err != nil {
		return nil, fmt.Errorf("get server: %w", err)
	}
	var server api.Server
	if err := response.MetadataAsStruct(&server); err != nil {
		return nil, fmt.Errorf("get server: %w", err)
	}
	return server.Environment.Architectures, nil
}

func (p *Incus) PatchInstance(ctx context.Context, project string, name string, instance api.InstancePut) error {
	if err := p.send(ctx, "PATCH", project, "/instances/"+url.PathEscape(name), instance); err != nil {
		return fmt.Errorf("patch instance %s in project %s: %w", name, project, err)
	}
	return nil
}

func (p *Incus) UpdateInstance(ctx context.Context, project string, name string, instance api.InstancePut) error {
	operation, err := p.server.UseProject(project).UpdateInstance(name, instance, "")
	if err != nil {
		return fmt.Errorf("update instance %s in project %s: %w", name, project, err)
	}
	if err := WaitOperation(ctx, p.server, operation.Get().ID); err != nil {
		return fmt.Errorf("update instance %s in project %s: %w", name, project, err)
	}
	return nil
}

func (p *Incus) CreateInstanceSnapshot(ctx context.Context, project string, instance string, snapshot string) error {
	operation, err := p.server.UseProject(project).CreateInstanceSnapshot(instance, api.InstanceSnapshotsPost{Name: snapshot})
	if err != nil {
		return fmt.Errorf("snapshot instance %s as %s in project %s: %w", instance, snapshot, project, err)
	}
	if err := WaitOperation(ctx, p.server, operation.Get().ID); err != nil {
		return fmt.Errorf("snapshot instance %s as %s in project %s: %w", instance, snapshot, project, err)
	}
	return nil
}

func (p *Incus) GetInstanceSnapshot(ctx context.Context, project string, instance string, snapshot string) error {
	if _, _, err := p.server.UseProject(project).GetInstanceSnapshot(instance, snapshot); err != nil {
		return fmt.Errorf("read snapshot %s/%s in project %s: %w", instance, snapshot, project, err)
	}
	return nil
}

func (p *Incus) DeleteInstanceSnapshot(ctx context.Context, project string, instance string, snapshot string) error {
	operation, err := p.server.UseProject(project).DeleteInstanceSnapshot(instance, snapshot)
	if err != nil {
		return fmt.Errorf("delete snapshot %s/%s in project %s: %w", instance, snapshot, project, err)
	}
	if err := WaitOperation(ctx, p.server, operation.Get().ID); err != nil {
		return fmt.Errorf("delete snapshot %s/%s in project %s: %w", instance, snapshot, project, err)
	}
	return nil
}

func (p *Incus) GetVolume(ctx context.Context, project string, pool string, name string) (*api.StorageVolume, error) {
	volume, _, err := p.server.UseProject(project).GetStoragePoolVolume(pool, "custom", name)
	if err != nil {
		return nil, fmt.Errorf("get volume %s in project %s: %w", name, project, err)
	}
	return volume, nil
}

// CreateVolume posts to the path the lab recorded. Not the client's own volume create: it returns before a copy
// (async) has finished.
func (p *Incus) CreateVolume(ctx context.Context, project string, pool string, volume api.StorageVolumesPost) error {
	if err := p.send(ctx, "POST", project, "/storage-pools/"+url.PathEscape(pool)+"/volumes", volume); err != nil {
		return fmt.Errorf("create volume %s in project %s: %w", volume.Name, project, err)
	}
	return nil
}

func (p *Incus) DeleteVolume(ctx context.Context, project string, pool string, name string) error {
	if err := p.server.UseProject(project).DeleteStoragePoolVolume(pool, "custom", name); err != nil {
		return fmt.Errorf("delete volume %s in project %s: %w", name, project, err)
	}
	return nil
}

func (p *Incus) ListVolumesAllProjects(ctx context.Context, pool string) ([]api.StorageVolume, error) {
	volumes, err := p.server.GetStoragePoolVolumesAllProjects(pool)
	if err != nil {
		return nil, fmt.Errorf("list volumes of pool %s in all projects: %w", pool, err)
	}
	return volumes, nil
}

func (p *Incus) CreateVolumeSnapshot(ctx context.Context, project string, pool string, volume string, snapshot string) error {
	post := api.StorageVolumeSnapshotsPost{Name: snapshot}
	operation, err := p.server.UseProject(project).CreateStoragePoolVolumeSnapshot(pool, "custom", volume, post)
	if err != nil {
		return fmt.Errorf("snapshot volume %s as %s in project %s: %w", volume, snapshot, project, err)
	}
	if err := WaitOperation(ctx, p.server, operation.Get().ID); err != nil {
		return fmt.Errorf("snapshot volume %s as %s in project %s: %w", volume, snapshot, project, err)
	}
	return nil
}

func (p *Incus) DeleteVolumeSnapshot(ctx context.Context, project string, pool string, volume string, snapshot string) error {
	operation, err := p.server.UseProject(project).DeleteStoragePoolVolumeSnapshot(pool, "custom", volume, snapshot)
	if err != nil {
		return fmt.Errorf("delete snapshot %s/%s in project %s: %w", volume, snapshot, project, err)
	}
	if err := WaitOperation(ctx, p.server, operation.Get().ID); err != nil {
		return fmt.Errorf("delete snapshot %s/%s in project %s: %w", volume, snapshot, project, err)
	}
	return nil
}

func (p *Incus) ListVolumeSnapshots(ctx context.Context, project string, pool string, volume string) ([]api.StorageVolumeSnapshot, error) {
	snapshots, err := p.server.UseProject(project).GetStoragePoolVolumeSnapshots(pool, "custom", volume)
	if err != nil {
		return nil, fmt.Errorf("list snapshots of volume %s in project %s: %w", volume, project, err)
	}
	return snapshots, nil
}

// send makes one request on a lab-recorded path and, when Incus answers with an operation, waits for it.
func (p *Incus) send(ctx context.Context, method string, project string, path string, body any) error {
	fullPath := "/1.0" + path + "?project=" + url.QueryEscape(project)
	response, _, err := p.server.RawQuery(method, fullPath, body, "")
	if err != nil {
		return err
	}
	isAsync := response.Type == api.AsyncResponse
	if !isAsync {
		return nil
	}
	operation, err := response.MetadataAsOperation()
	if err != nil {
		return err
	}
	return WaitOperation(ctx, p.server, operation.ID)
}

// BackupInstance asks for the instance alone: no snapshots, no attached volumes (each volume has its own backup).
// Incus refuses a backup name when a target is set.
func (p *Incus) BackupInstance(ctx context.Context, project string, name string, target api.BackupTarget) error {
	request := api.InstanceBackupsPost{InstanceOnly: true, RootOnly: true, CompressionAlgorithm: "gzip", Target: &target}
	operation, err := p.server.UseProject(project).CreateInstanceBackup(name, request)
	if err != nil {
		return fmt.Errorf("back up instance %s in project %s: %w", name, project, err)
	}
	if err := WaitOperation(ctx, p.server, operation.Get().ID); err != nil {
		return fmt.Errorf("back up instance %s in project %s: %w", name, project, err)
	}
	return nil
}

func (p *Incus) BackupVolume(ctx context.Context, project string, pool string, name string, target api.BackupTarget) error {
	request := api.StorageVolumeBackupsPost{VolumeOnly: true, CompressionAlgorithm: "gzip", Target: &target}
	operation, err := p.server.UseProject(project).CreateStorageVolumeBackup(pool, name, request)
	if err != nil {
		return fmt.Errorf("back up volume %s in project %s: %w", name, project, err)
	}
	if err := WaitOperation(ctx, p.server, operation.Get().ID); err != nil {
		return fmt.Errorf("back up volume %s in project %s: %w", name, project, err)
	}
	return nil
}
