package provider

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

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

// HTTPClient is the client the adapter reaches Incus with: what limits a request is set on it and on its transport.
func (p *Incus) HTTPClient() (*http.Client, error) {
	return p.server.GetHTTPClient()
}

func (p *Incus) GetProject(ctx context.Context, name string) (*api.Project, error) {
	project, _, err := p.server.GetProject(name)
	if err != nil {
		return nil, fmt.Errorf("get project %s: %w", name, err)
	}
	return project, nil
}

func (p *Incus) ListProjects(ctx context.Context) ([]api.Project, error) {
	projects, err := p.server.GetProjects()
	if err != nil {
		return nil, fmt.Errorf("list projects: %w", err)
	}
	return projects, nil
}

func (p *Incus) ListProfiles(ctx context.Context, project string) ([]api.Profile, error) {
	profiles, err := p.server.UseProject(project).GetProfiles()
	if err != nil {
		return nil, fmt.Errorf("list profiles of project %s: %w", project, err)
	}
	return profiles, nil
}

func (p *Incus) CreateProject(ctx context.Context, project api.ProjectsPost) error {
	if err := p.server.CreateProject(project); err != nil {
		return fmt.Errorf("create project %s: %w", project.Name, err)
	}
	return nil
}

func (p *Incus) DeleteProject(ctx context.Context, name string) error {
	if err := p.server.DeleteProject(name); err != nil {
		return fmt.Errorf("delete project %s: %w", name, err)
	}
	return nil
}

func (p *Incus) GetStoragePoolResources(ctx context.Context, pool string) (*api.ResourcesStoragePool, error) {
	resources, err := p.server.GetStoragePoolResources(pool)
	if err != nil {
		return nil, fmt.Errorf("get the resources of storage pool %s: %w", pool, err)
	}
	return resources, nil
}

func (p *Incus) CreateProfile(ctx context.Context, project string, profile api.ProfilesPost) error {
	if err := p.server.UseProject(project).CreateProfile(profile); err != nil {
		return fmt.Errorf("create profile %s in project %s: %w", profile.Name, project, err)
	}
	return nil
}

// UpdateProfile writes without an ETag: a restore runs alone on a freshly reinstalled host, nobody else writing.
func (p *Incus) UpdateProfile(ctx context.Context, project string, name string, profile api.ProfilePut) error {
	if err := p.server.UseProject(project).UpdateProfile(name, profile, ""); err != nil {
		return fmt.Errorf("update profile %s in project %s: %w", name, project, err)
	}
	return nil
}

func (p *Incus) GetInstance(ctx context.Context, project string, name string) (*api.Instance, error) {
	instance, _, err := p.GetInstanceWithEtag(ctx, project, name)
	return instance, err
}

func (p *Incus) GetInstanceWithEtag(ctx context.Context, project string, name string) (*api.Instance, string, error) {
	instance, etag, err := p.server.UseProject(project).GetInstance(name)
	if err != nil {
		return nil, "", fmt.Errorf("get instance %s in project %s: %w", name, project, err)
	}
	return instance, etag, nil
}

func (p *Incus) GetInstanceFull(ctx context.Context, project string, name string) (*api.InstanceFull, error) {
	instance, _, err := p.server.UseProject(project).GetInstanceFull(name)
	if err != nil {
		return nil, fmt.Errorf("get instance %s with its state in project %s: %w", name, project, err)
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

func (p *Incus) ListNetworkLeases(ctx context.Context, project string, network string) ([]api.NetworkLease, error) {
	leases, err := p.server.UseProject(project).GetNetworkLeases(network)
	if err != nil {
		return nil, fmt.Errorf("list leases of network %s in project %s: %w", network, project, err)
	}
	return leases, nil
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

// GetMetrics reads on the connection without a project: Incus then answers every project's instances.
func (p *Incus) GetMetrics(ctx context.Context) (string, error) {
	metrics, err := p.server.GetMetrics()
	if err != nil {
		return "", fmt.Errorf("get metrics: %w", err)
	}
	return metrics, nil
}

func (p *Incus) PatchInstance(ctx context.Context, project string, name string, instance api.InstancePut) error {
	if err := p.send(ctx, "PATCH", project, "/instances/"+url.PathEscape(name), instance); err != nil {
		return fmt.Errorf("patch instance %s in project %s: %w", name, project, err)
	}
	return nil
}

func (p *Incus) UpdateInstance(ctx context.Context, project string, name string, instance api.InstancePut, etag string) error {
	operation, err := p.server.UseProject(project).UpdateInstance(name, instance, etag)
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

func (p *Incus) GetInstanceSnapshot(ctx context.Context, project string, instance string, snapshot string) (*api.InstanceSnapshot, error) {
	found, _, err := p.server.UseProject(project).GetInstanceSnapshot(instance, snapshot)
	if err != nil {
		return nil, fmt.Errorf("read snapshot %s/%s in project %s: %w", instance, snapshot, project, err)
	}
	return found, nil
}

func (p *Incus) ListInstanceSnapshots(ctx context.Context, project string, instance string) ([]api.InstanceSnapshot, error) {
	snapshots, err := p.server.UseProject(project).GetInstanceSnapshots(instance)
	if err != nil {
		return nil, fmt.Errorf("list snapshots of instance %s in project %s: %w", instance, project, err)
	}
	return snapshots, nil
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

// GetInstanceFile reads a path from the instance's guest filesystem through the Incus agent. A directory answers no
// content and no error (its listing is not read; callers only use this to test existence).
func (p *Incus) GetInstanceFile(ctx context.Context, project string, instance string, path string) ([]byte, error) {
	content, resp, err := p.server.UseProject(project).GetInstanceFile(instance, path)
	if err != nil {
		return nil, fmt.Errorf("get file %s of instance %s in project %s: %w", path, instance, project, err)
	}
	if resp.Type == "directory" {
		return nil, nil
	}
	defer content.Close()
	data, err := io.ReadAll(content)
	if err != nil {
		return nil, fmt.Errorf("read file %s of instance %s in project %s: %w", path, instance, project, err)
	}
	return data, nil
}

// CreateInstanceFile writes a regular file whole: WriteMode "overwrite" replaces whatever was there, never appends.
func (p *Incus) CreateInstanceFile(ctx context.Context, project string, instance string, path string, content []byte, uid int64, gid int64, mode int) error {
	args := incus.InstanceFileArgs{
		Content: bytes.NewReader(content), UID: uid, GID: gid, Mode: mode, Type: "file", WriteMode: "overwrite",
	}
	if err := p.server.UseProject(project).CreateInstanceFile(instance, path, args); err != nil {
		return fmt.Errorf("create file %s of instance %s in project %s: %w", path, instance, project, err)
	}
	return nil
}

// CreateInstanceDirectory makes one directory, owned by root, mode 0755.
func (p *Incus) CreateInstanceDirectory(ctx context.Context, project string, instance string, path string) error {
	args := incus.InstanceFileArgs{UID: 0, GID: 0, Mode: 0755, Type: "directory"}
	if err := p.server.UseProject(project).CreateInstanceFile(instance, path, args); err != nil {
		return fmt.Errorf("create directory %s of instance %s in project %s: %w", path, instance, project, err)
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

// PatchVolumeConfig has Incus merge config into the volume's own. The same PATCH sets the volume's description to the
// one it carries, empty here (cmd/incusd/storage_volumes.go of v7.0.1): Titan gives a volume none.
func (p *Incus) PatchVolumeConfig(ctx context.Context, project string, pool string, name string, config map[string]string) error {
	patch := api.StorageVolumePut{Config: config}
	if err := p.send(ctx, "PATCH", project, "/storage-pools/"+url.PathEscape(pool)+"/volumes/custom/"+url.PathEscape(name), patch); err != nil {
		return fmt.Errorf("patch the config of volume %s in project %s: %w", name, project, err)
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

// BackupInstance asks for the instance with every snapshot it has and its root disk only: no attached volume (each
// volume has its own backup). Incus refuses a backup name when a target is set.
func (p *Incus) BackupInstance(ctx context.Context, project string, name string, target api.BackupTarget) error {
	request := api.InstanceBackupsPost{InstanceOnly: false, RootOnly: true, CompressionAlgorithm: "gzip", Target: &target}
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
	// With every snapshot of the volume.
	request := api.StorageVolumeBackupsPost{VolumeOnly: false, CompressionAlgorithm: "gzip", Target: &target}
	operation, err := p.server.UseProject(project).CreateStorageVolumeBackup(pool, name, request)
	if err != nil {
		return fmt.Errorf("back up volume %s in project %s: %w", name, project, err)
	}
	if err := WaitOperation(ctx, p.server, operation.Get().ID); err != nil {
		return fmt.Errorf("back up volume %s in project %s: %w", name, project, err)
	}
	return nil
}

// ImportInstance streams the archive straight into Incus's own import; backupFile is read as the request body, never
// held whole on the worker.
func (p *Incus) ImportInstance(ctx context.Context, project string, pool string, name string, backupFile io.Reader, overrides InstanceOverrides) error {
	if err := checkOverrides(overrides); err != nil {
		return fmt.Errorf("import instance %s in project %s: %w", name, project, err)
	}
	args := incus.InstanceBackupArgs{BackupFile: backupFile, PoolName: pool, Name: name}
	// A list that is there but empty still makes the client send its header and ask the server for the override
	// extension (client/incus_instances.go of v7.0.1).
	if len(overrides.Devices) > 0 {
		args.Devices = overrides.Devices
	}
	if len(overrides.Config) > 0 {
		args.Config = overrides.Config
	}
	operation, err := p.server.UseProject(project).CreateInstanceFromBackup(args)
	if err != nil {
		return fmt.Errorf("import instance %s in project %s: %w", name, project, err)
	}
	if err := WaitOperation(ctx, p.server, operation.Get().ID); err != nil {
		return fmt.Errorf("import instance %s in project %s: %w", name, project, err)
	}
	return nil
}

// checkOverrides refuses an entry Incus would misread. The client joins the entries with a space into one header; the
// server splits it on spaces, wants "=" in every entry and "," in a device's, and does so only once it holds the whole
// archive (cmd/incusd/instances_post.go of v7.0.1): a bad entry found here costs no upload.
func checkOverrides(overrides InstanceOverrides) error {
	for _, entry := range overrides.Devices {
		device, setting, hasDevice := strings.Cut(entry, ",")
		key, _, hasValue := strings.Cut(setting, "=")
		isUsable := hasDevice && hasValue && device != "" && key != "" && !strings.ContainsAny(entry, overrideBreaks)
		if !isUsable {
			return fmt.Errorf("device override %q is not <device>,<key>=<value> without a space", entry)
		}
	}
	for _, entry := range overrides.Config {
		key, _, hasValue := strings.Cut(entry, "=")
		isUsable := hasValue && key != "" && !strings.ContainsAny(entry, overrideBreaks)
		if !isUsable {
			return fmt.Errorf("config override %q is not <key>=<value> without a space", entry)
		}
	}
	return nil
}

// overrideBreaks are the characters that end an entry in the header, or the header itself.
const overrideBreaks = " \t\r\n"

func (p *Incus) ImportVolume(ctx context.Context, project string, pool string, name string, backupFile io.Reader) error {
	args := incus.StorageVolumeBackupArgs{BackupFile: backupFile, Name: name}
	operation, err := p.server.UseProject(project).CreateStoragePoolVolumeFromBackup(pool, args)
	if err != nil {
		return fmt.Errorf("import volume %s in project %s: %w", name, project, err)
	}
	if err := WaitOperation(ctx, p.server, operation.Get().ID); err != nil {
		return fmt.Errorf("import volume %s in project %s: %w", name, project, err)
	}
	return nil
}

// RestoreVolume asks Incus to put the volume back to snapshot, as the restore rolls an imported volume back to its
// "backup" snapshot. Config is left nil: Incus only touches config during a restore when it is non-nil, and nil
// keeps the volume's own (cmd/incusd/storage_volumes.go of v7.0.1).
func (p *Incus) RestoreVolume(ctx context.Context, project string, pool string, name string, snapshot string) error {
	put := api.StorageVolumePut{Restore: snapshot}
	if err := p.send(ctx, "PUT", project, "/storage-pools/"+url.PathEscape(pool)+"/volumes/custom/"+url.PathEscape(name), put); err != nil {
		return fmt.Errorf("restore volume %s to snapshot %s in project %s: %w", name, snapshot, project, err)
	}
	return nil
}

// ListRunningInstanceOperations and ListRunningVolumeOperations read the project's operations and keep the ones Incus
// has not finished whose resources name the instance, or the custom volume. Read in the v7.0.1 source, not measured on
// a server:
//   - the import of an instance archive is an operation of class "task" described "Restoring backup", with the
//     resources "instances": ["/1.0/instances/<name>"], the name being the one the import was given
//     (cmd/incusd/instances_post.go);
//   - the import of a custom volume archive is one of class "task" described "Restoring custom volume backup", with
//     "storage_volumes": ["/1.0/storage-pools/<pool>/volumes/custom/<name>"] (cmd/incusd/storage_volumes.go);
//   - the list gives each resource with "?project=<project>" after it, for a project other than default, and keeps
//     an operation for five seconds after it ended (internal/server/operations/operations.go).
//
// Every operation that names the object counts, not the import alone: a rollback or a delete nobody waits on any
// more is as much at work on it.
func (p *Incus) ListRunningInstanceOperations(ctx context.Context, project string, name string) ([]api.Operation, error) {
	return p.runningOperationsOn(project, "/1.0/instances/"+name)
}

func (p *Incus) ListRunningVolumeOperations(ctx context.Context, project string, pool string, name string) ([]api.Operation, error) {
	return p.runningOperationsOn(project, "/1.0/storage-pools/"+pool+"/volumes/custom/"+name)
}

func (p *Incus) runningOperationsOn(project string, path string) ([]api.Operation, error) {
	operations, err := p.server.UseProject(project).GetOperations()
	if err != nil {
		return nil, fmt.Errorf("list the operations of project %s: %w", project, err)
	}
	running := []api.Operation{}
	for _, operation := range operations {
		if !operation.StatusCode.IsFinal() && namesResource(operation, path) {
			running = append(running, operation)
		}
	}
	return running, nil
}

// namesResource says whether one of the operation's resources is the object at path, whatever kind Incus lists it
// under.
func namesResource(operation api.Operation, path string) bool {
	for _, resources := range operation.Resources {
		for _, resource := range resources {
			named, err := url.Parse(resource)
			if err == nil && named.Path == path {
				return true
			}
		}
	}
	return false
}
