package testsupport

import (
	"context"
	"io"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
)

// Recorder is the fake Provider that also keeps every write made through it, in call order, as
// "<verb> <project>/<object>": a snapshot is "<parent>/<snapshot>", a load balancer "<network>/<address>", and what
// lives in no project (a project, an image) has an empty one. A test asserts with it the exact writes a command makes
// and that none names an object the command must leave alone; what a write carried is read on the fake. Reads are not
// kept.
type Recorder struct {
	*Provider
	Writes []string
}

func NewRecorder() *Recorder {
	return &Recorder{Provider: NewProvider()}
}

var _ provider.Provider = (*Recorder)(nil)

func (r *Recorder) write(verb string, project string, object string) {
	r.Writes = append(r.Writes, verb+" "+project+"/"+object)
}

func (r *Recorder) CreateProject(ctx context.Context, project api.ProjectsPost) error {
	r.write("create-project", "", project.Name)
	return r.Provider.CreateProject(ctx, project)
}

func (r *Recorder) DeleteProject(ctx context.Context, name string) error {
	r.write("delete-project", "", name)
	return r.Provider.DeleteProject(ctx, name)
}

func (r *Recorder) CreateProfile(ctx context.Context, project string, profile api.ProfilesPost) error {
	r.write("create-profile", project, profile.Name)
	return r.Provider.CreateProfile(ctx, project, profile)
}

func (r *Recorder) UpdateProfile(ctx context.Context, project string, name string, profile api.ProfilePut) error {
	r.write("update-profile", project, name)
	return r.Provider.UpdateProfile(ctx, project, name, profile)
}

func (r *Recorder) CreateInstance(ctx context.Context, project string, instance api.InstancesPost) error {
	r.write("create-vm", project, instance.Name)
	return r.Provider.CreateInstance(ctx, project, instance)
}

// UpdateInstanceState is named by its action: "start-vm", "stop-vm".
func (r *Recorder) UpdateInstanceState(ctx context.Context, project string, name string, state api.InstanceStatePut) error {
	r.write(state.Action+"-vm", project, name)
	return r.Provider.UpdateInstanceState(ctx, project, name, state)
}

func (r *Recorder) DeleteInstance(ctx context.Context, project string, name string) error {
	r.write("delete-vm", project, name)
	return r.Provider.DeleteInstance(ctx, project, name)
}

func (r *Recorder) CreateNetwork(ctx context.Context, project string, network api.NetworksPost) error {
	r.write("create-network", project, network.Name)
	return r.Provider.CreateNetwork(ctx, project, network)
}

func (r *Recorder) DeleteNetwork(ctx context.Context, project string, name string) error {
	r.write("delete-network", project, name)
	return r.Provider.DeleteNetwork(ctx, project, name)
}

func (r *Recorder) UpdateNetwork(ctx context.Context, project string, name string, network api.NetworkPut, etag string) error {
	r.write("update-network", project, name)
	return r.Provider.UpdateNetwork(ctx, project, name, network, etag)
}

func (r *Recorder) CreateNetworkACL(ctx context.Context, project string, acl api.NetworkACLsPost) error {
	r.write("create-acl", project, acl.Name)
	return r.Provider.CreateNetworkACL(ctx, project, acl)
}

func (r *Recorder) UpdateNetworkACL(ctx context.Context, project string, name string, acl api.NetworkACLPut, etag string) error {
	r.write("update-acl", project, name)
	return r.Provider.UpdateNetworkACL(ctx, project, name, acl, etag)
}

func (r *Recorder) DeleteNetworkACL(ctx context.Context, project string, name string) error {
	r.write("delete-acl", project, name)
	return r.Provider.DeleteNetworkACL(ctx, project, name)
}

func (r *Recorder) CreateNetworkLoadBalancer(ctx context.Context, project string, network string, loadBalancer api.NetworkLoadBalancersPost) error {
	r.write("create-load-balancer", project, network+"/"+loadBalancer.ListenAddress)
	return r.Provider.CreateNetworkLoadBalancer(ctx, project, network, loadBalancer)
}

func (r *Recorder) UpdateNetworkLoadBalancer(ctx context.Context, project string, network string, listenAddress string, loadBalancer api.NetworkLoadBalancerPut, etag string) error {
	r.write("update-load-balancer", project, network+"/"+listenAddress)
	return r.Provider.UpdateNetworkLoadBalancer(ctx, project, network, listenAddress, loadBalancer, etag)
}

func (r *Recorder) DeleteNetworkLoadBalancer(ctx context.Context, project string, network string, listenAddress string) error {
	r.write("delete-load-balancer", project, network+"/"+listenAddress)
	return r.Provider.DeleteNetworkLoadBalancer(ctx, project, network, listenAddress)
}

func (r *Recorder) PatchInstance(ctx context.Context, project string, name string, instance api.InstancePut) error {
	r.write("patch-vm", project, name)
	return r.Provider.PatchInstance(ctx, project, name, instance)
}

// UpdateInstance is two writes on Incus, told apart here: a PUT that puts a snapshot back is "rollback-vm" with the
// snapshot, any other "update-vm".
func (r *Recorder) UpdateInstance(ctx context.Context, project string, name string, instance api.InstancePut, etag string) error {
	if instance.Restore != "" {
		r.write("rollback-vm", project, name+"/"+instance.Restore)
	} else {
		r.write("update-vm", project, name)
	}
	return r.Provider.UpdateInstance(ctx, project, name, instance, etag)
}

func (r *Recorder) CreateInstanceSnapshot(ctx context.Context, project string, instance string, snapshot string) error {
	r.write("create-vm-snapshot", project, instance+"/"+snapshot)
	return r.Provider.CreateInstanceSnapshot(ctx, project, instance, snapshot)
}

func (r *Recorder) DeleteInstanceSnapshot(ctx context.Context, project string, instance string, snapshot string) error {
	r.write("delete-vm-snapshot", project, instance+"/"+snapshot)
	return r.Provider.DeleteInstanceSnapshot(ctx, project, instance, snapshot)
}

func (r *Recorder) CreateInstanceFile(ctx context.Context, project string, instance string, path string, content []byte, uid int64, gid int64, mode int) error {
	r.write("write-vm-file", project, instance)
	return r.Provider.CreateInstanceFile(ctx, project, instance, path, content, uid, gid, mode)
}

func (r *Recorder) CreateInstanceDirectory(ctx context.Context, project string, instance string, path string) error {
	r.write("create-vm-directory", project, instance)
	return r.Provider.CreateInstanceDirectory(ctx, project, instance, path)
}

func (r *Recorder) CreateVolume(ctx context.Context, project string, pool string, volume api.StorageVolumesPost) error {
	r.write("create-volume", project, volume.Name)
	return r.Provider.CreateVolume(ctx, project, pool, volume)
}

func (r *Recorder) PatchVolumeConfig(ctx context.Context, project string, pool string, name string, config map[string]string) error {
	r.write("patch-volume", project, name)
	return r.Provider.PatchVolumeConfig(ctx, project, pool, name, config)
}

func (r *Recorder) DeleteVolume(ctx context.Context, project string, pool string, name string) error {
	r.write("delete-volume", project, name)
	return r.Provider.DeleteVolume(ctx, project, pool, name)
}

func (r *Recorder) CreateVolumeSnapshot(ctx context.Context, project string, pool string, volume string, snapshot string) error {
	r.write("create-volume-snapshot", project, volume+"/"+snapshot)
	return r.Provider.CreateVolumeSnapshot(ctx, project, pool, volume, snapshot)
}

func (r *Recorder) DeleteVolumeSnapshot(ctx context.Context, project string, pool string, volume string, snapshot string) error {
	r.write("delete-volume-snapshot", project, volume+"/"+snapshot)
	return r.Provider.DeleteVolumeSnapshot(ctx, project, pool, volume, snapshot)
}

func (r *Recorder) BackupInstance(ctx context.Context, project string, name string, target api.BackupTarget) error {
	r.write("backup-vm", project, name)
	return r.Provider.BackupInstance(ctx, project, name, target)
}

func (r *Recorder) BackupVolume(ctx context.Context, project string, pool string, name string, target api.BackupTarget) error {
	r.write("backup-volume", project, name)
	return r.Provider.BackupVolume(ctx, project, pool, name, target)
}

func (r *Recorder) ImportInstance(ctx context.Context, project string, pool string, name string, backupFile io.Reader, overrides provider.InstanceOverrides) error {
	r.write("import-vm", project, name)
	return r.Provider.ImportInstance(ctx, project, pool, name, backupFile, overrides)
}

func (r *Recorder) ImportVolume(ctx context.Context, project string, pool string, name string, backupFile io.Reader) error {
	r.write("import-volume", project, name)
	return r.Provider.ImportVolume(ctx, project, pool, name, backupFile)
}

func (r *Recorder) RestoreVolume(ctx context.Context, project string, pool string, name string, snapshot string) error {
	r.write("rollback-volume", project, name+"/"+snapshot)
	return r.Provider.RestoreVolume(ctx, project, pool, name, snapshot)
}

// CreateImage and ImportImage name the image by its first alias, the template's.
func (r *Recorder) CreateImage(ctx context.Context, image api.ImagesPost) error {
	r.write("create-image", "", firstAlias(image))
	return r.Provider.CreateImage(ctx, image)
}

func (r *Recorder) ImportImage(ctx context.Context, image api.ImagesPost, metadata io.Reader, rootfs io.Reader) error {
	r.write("import-image", "", firstAlias(image))
	return r.Provider.ImportImage(ctx, image, metadata, rootfs)
}

func (r *Recorder) DeleteImage(ctx context.Context, fingerprint string) error {
	r.write("delete-image", "", fingerprint)
	return r.Provider.DeleteImage(ctx, fingerprint)
}

func firstAlias(image api.ImagesPost) string {
	if len(image.Aliases) == 0 {
		return ""
	}
	return image.Aliases[0].Name
}
