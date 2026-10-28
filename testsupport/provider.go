package testsupport

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
)

// Provider is an in-memory provider.Provider. Errors set on it are returned by the matching call; every call is
// counted in Calls.
type Provider struct {
	mu sync.Mutex

	Calls int
	// Order names each network and network ACL create/delete in call order, e.g. "create-acl p/acl-x".
	Order []string

	Projects        map[string]bool
	CreatedProjects []api.ProjectsPost

	Instances              map[string]*api.Instance // key: project + "/" + name
	CreatedInstances       []api.InstancesPost
	CreateInstanceErr      error
	StateChanges           []api.InstanceStatePut
	UpdateStateErr         error
	DeletedInstances       []string
	DeleteInstanceErr      error
	VirtualMachines        []api.InstanceFull
	ListVirtualMachinesErr error
	Networks               []api.Network
	CreatedNetworks        []api.NetworksPost
	CreateNetworkErr       error
	ListNetworksErr        error
	DeletedNetworks        []string
	DeleteNetworkErr       error
	// GetNetwork reads Networks; its ETag is NetworkEtag. UpdateNetwork records the write and refuses a stale ETag.
	NetworkEtag      string
	UpdatedNetworks  []api.NetworkPut
	UpdateNetworkErr error
	// ACLs is keyed by project + "/" + name; an ACL's ETag is its write count, as text.
	ACLs         map[string]*api.NetworkACL
	aclVersions  map[string]int
	CreatedACLs  []api.NetworkACLsPost
	CreateACLErr error
	UpdatedACLs  []api.NetworkACLPut
	UpdateACLErr error
	DeletedACLs  []string
	DeleteACLErr error
	ListACLsErr  error
	// LoadBalancers is keyed by project + "/" + network + "/" + listen address; an ETag is "lb-etag".
	LoadBalancers         map[string]*api.NetworkLoadBalancer
	CreatedLoadBalancers  []api.NetworkLoadBalancersPost
	CreateLoadBalancerErr error
	UpdatedLoadBalancers  []api.NetworkLoadBalancerPut
	UpdateLoadBalancerErr error
	DeletedLoadBalancers  []string
	DeleteLoadBalancerErr error
	GetLoadBalancerErr    error
	ListLoadBalancersErr  error
	MissingNetworks       map[string]bool // project + "/" + network: ListNetworkLoadBalancers answers 404
	PatchedInstances      []api.InstancePut
	PatchInstanceErr      error
	UpdatedInstances      []api.InstancePut
	UpdateInstanceErr     error
	InstanceSnapshots     map[string]bool // key: project + "/" + instance + "/" + snapshot
	// SnapshotDevices holds the instance's devices at the time of each snapshot, under the same key.
	SnapshotDevices map[string]map[string]map[string]string
	// Copies holds each instance a copy made, with its devices, as it was when made.
	Copies            []api.Instance
	CreateSnapshotErr error
	DeleteSnapshotErr error
	Volumes           map[string]*api.StorageVolume // key: project + "/" + name
	GetVolumeErr      error
	// AfterUpdateInstance runs after each recorded UpdateInstance: a test makes the fake follow what Incus would
	// change, such as a volume's used_by once its disk device is gone.
	AfterUpdateInstance    func()
	CreatedVolumes         []api.StorageVolumesPost
	CreateVolumeErr        error
	DeletedVolumes         []string
	DeleteVolumeErr        error
	AllVolumes             []api.StorageVolume
	ListVolumesErr         error
	VolumeSnapshots        map[string]bool // key: project + "/" + volume + "/" + snapshot
	CreatedVolumeSnapshots []string
	DeletedVolumeSnapshots []string
	ListVolumeSnapshotsErr error
	Aliases                map[string]bool
	CreatedImages          []api.ImagesPost
	CreateImageErr         error
	ImportedImages         []ImportedImage
	ImportImageErr         error
	ListImageAliasesErr    error
	// An alias's image has fingerprint "fp-" + alias; its architecture is ImageArchitectures[fingerprint], or
	// x86_64 when unset.
	ImageArchitectures  map[string]string
	ServerArchitectures []string
	DeletedImages       []string
	DeleteImageErr      error
	// BackupOrder names each step a backup takes, in call order, e.g. "snapshot p/vm/backup".
	BackupOrder     []string
	InstanceBackups []InstanceBackup
	VolumeBackups   []VolumeBackup
	BackupErr       error
}

type InstanceBackup struct {
	Project string
	Name    string
	Target  api.BackupTarget
}

type VolumeBackup struct {
	Project string
	Pool    string
	Name    string
	Target  api.BackupTarget
}

func NewProvider() *Provider {
	return &Provider{
		Projects:           map[string]bool{},
		Instances:          map[string]*api.Instance{},
		InstanceSnapshots:  map[string]bool{},
		SnapshotDevices:    map[string]map[string]map[string]string{},
		Volumes:            map[string]*api.StorageVolume{},
		VolumeSnapshots:    map[string]bool{},
		Aliases:            map[string]bool{},
		ACLs:               map[string]*api.NetworkACL{},
		LoadBalancers:      map[string]*api.NetworkLoadBalancer{},
		MissingNetworks:    map[string]bool{},
		aclVersions:        map[string]int{},
		NetworkEtag:        "network-etag",
		ImageArchitectures: map[string]string{},
		// An x86_64 server.
		ServerArchitectures: []string{"x86_64", "i686"},
	}
}

var _ provider.Provider = (*Provider)(nil)

func NotFound(message string) error {
	return api.StatusErrorf(http.StatusNotFound, "%s", message)
}

func (p *Provider) count() {
	p.mu.Lock()
	p.Calls++
	p.mu.Unlock()
}

func (p *Provider) GetProject(ctx context.Context, name string) (*api.Project, error) {
	p.count()
	if !p.Projects[name] {
		return nil, NotFound("Project not found")
	}
	return &api.Project{Name: name}, nil
}

func (p *Provider) CreateProject(ctx context.Context, project api.ProjectsPost) error {
	p.count()
	p.CreatedProjects = append(p.CreatedProjects, project)
	p.Projects[project.Name] = true
	return nil
}

func (p *Provider) GetInstance(ctx context.Context, project string, name string) (*api.Instance, error) {
	p.count()
	instance, found := p.Instances[project+"/"+name]
	if !found {
		return nil, NotFound("Instance not found")
	}
	return instance, nil
}

func (p *Provider) CreateInstance(ctx context.Context, project string, instance api.InstancesPost) error {
	p.count()
	p.CreatedInstances = append(p.CreatedInstances, instance)
	isCopy := instance.Source.Type == "copy"
	if isCopy && p.CreateInstanceErr == nil {
		p.BackupOrder = append(p.BackupOrder, "copy "+project+"/"+instance.Source.Source+" -> "+instance.Name)
		devices := copiedDevices(p.SnapshotDevices[project+"/"+instance.Source.Source], instance.Devices)
		copied := &api.Instance{Name: instance.Name, StatusCode: api.Stopped, InstancePut: api.InstancePut{Devices: devices}}
		p.Instances[project+"/"+instance.Name] = copied
		p.Copies = append(p.Copies, *copied)
	}
	return p.CreateInstanceErr
}

// copiedDevices is what Incus gives a copy: the source's devices, each replaced by a device of the same name in the
// request (createFromCopy in cmd/incusd/instances_post.go of Incus v7.0.1; measured on dev: incus/provider/testdata/lab-backup-copy-2026-10-28.log).
func copiedDevices(source map[string]map[string]string, requested map[string]map[string]string) map[string]map[string]string {
	devices := map[string]map[string]string{}
	for name, settings := range source {
		devices[name] = settings
	}
	for name, settings := range requested {
		devices[name] = settings
	}
	return devices
}

func (p *Provider) UpdateInstanceState(ctx context.Context, project string, name string, state api.InstanceStatePut) error {
	p.count()
	p.StateChanges = append(p.StateChanges, state)
	return p.UpdateStateErr
}

func (p *Provider) DeleteInstance(ctx context.Context, project string, name string) error {
	p.count()
	p.DeletedInstances = append(p.DeletedInstances, project+"/"+name)
	if p.DeleteInstanceErr == nil {
		p.BackupOrder = append(p.BackupOrder, "delete-instance "+project+"/"+name)
		delete(p.Instances, project+"/"+name)
	}
	return p.DeleteInstanceErr
}

func (p *Provider) ListVirtualMachinesAllProjects(ctx context.Context) ([]api.InstanceFull, error) {
	p.count()
	return p.VirtualMachines, p.ListVirtualMachinesErr
}

func (p *Provider) CreateNetwork(ctx context.Context, project string, network api.NetworksPost) error {
	p.count()
	p.CreatedNetworks = append(p.CreatedNetworks, network)
	p.Order = append(p.Order, "create-network "+project+"/"+network.Name)
	return p.CreateNetworkErr
}

func (p *Provider) DeleteNetwork(ctx context.Context, project string, name string) error {
	p.count()
	p.DeletedNetworks = append(p.DeletedNetworks, project+"/"+name)
	p.Order = append(p.Order, "delete-network "+project+"/"+name)
	return p.DeleteNetworkErr
}

func (p *Provider) ListNetworksAllProjects(ctx context.Context) ([]api.Network, error) {
	if p.ListNetworksErr != nil {
		return nil, p.ListNetworksErr
	}
	p.count()
	return p.Networks, nil
}

func (p *Provider) GetNetwork(ctx context.Context, project string, name string) (*api.Network, string, error) {
	p.count()
	for i := range p.Networks {
		network := p.Networks[i]
		if network.Project == project && network.Name == name {
			return &network, p.NetworkEtag, nil
		}
	}
	return nil, "", NotFound("Network not found")
}

func (p *Provider) UpdateNetwork(ctx context.Context, project string, name string, network api.NetworkPut, etag string) error {
	p.count()
	if p.UpdateNetworkErr != nil {
		return p.UpdateNetworkErr
	}
	if etag != p.NetworkEtag {
		return api.StatusErrorf(http.StatusPreconditionFailed, "ETag doesn't match")
	}
	p.UpdatedNetworks = append(p.UpdatedNetworks, network)
	for i := range p.Networks {
		if p.Networks[i].Project == project && p.Networks[i].Name == name {
			p.Networks[i].NetworkPut = network
		}
	}
	return nil
}

func (p *Provider) aclEtag(key string) string {
	return strconv.Itoa(p.aclVersions[key])
}

// PutACL places an ACL on the fake cluster, as a test's starting state.
func (p *Provider) PutACL(project string, acl api.NetworkACL) {
	acl.Project = project
	key := project + "/" + acl.Name
	p.ACLs[key] = &acl
	p.aclVersions[key]++
}

func (p *Provider) GetNetworkACL(ctx context.Context, project string, name string) (*api.NetworkACL, string, error) {
	p.count()
	key := project + "/" + name
	acl, found := p.ACLs[key]
	if !found {
		return nil, "", NotFound("Network ACL not found")
	}
	copied := *acl
	copied.Ingress = append([]api.NetworkACLRule{}, acl.Ingress...)
	copied.Egress = append([]api.NetworkACLRule{}, acl.Egress...)
	return &copied, p.aclEtag(key), nil
}

func (p *Provider) CreateNetworkACL(ctx context.Context, project string, acl api.NetworkACLsPost) error {
	p.count()
	p.CreatedACLs = append(p.CreatedACLs, acl)
	p.Order = append(p.Order, "create-acl "+project+"/"+acl.Name)
	if p.CreateACLErr != nil {
		return p.CreateACLErr
	}
	key := project + "/" + acl.Name
	if _, exists := p.ACLs[key]; exists {
		// Incus answers a duplicate ACL 400, not 409 (cmd/incusd/network_acls.go of v7.0.1).
		return api.StatusErrorf(http.StatusBadRequest, "The network ACL already exists")
	}
	p.PutACL(project, api.NetworkACL{NetworkACLPost: acl.NetworkACLPost, NetworkACLPut: acl.NetworkACLPut})
	return nil
}

func (p *Provider) UpdateNetworkACL(ctx context.Context, project string, name string, acl api.NetworkACLPut, etag string) error {
	p.count()
	if p.UpdateACLErr != nil {
		return p.UpdateACLErr
	}
	key := project + "/" + name
	found, exists := p.ACLs[key]
	if !exists {
		return NotFound("Network ACL not found")
	}
	if etag != p.aclEtag(key) {
		return api.StatusErrorf(http.StatusPreconditionFailed, "ETag doesn't match")
	}
	p.UpdatedACLs = append(p.UpdatedACLs, acl)
	found.NetworkACLPut = acl
	p.aclVersions[key]++
	return nil
}

func (p *Provider) DeleteNetworkACL(ctx context.Context, project string, name string) error {
	p.count()
	p.DeletedACLs = append(p.DeletedACLs, project+"/"+name)
	p.Order = append(p.Order, "delete-acl "+project+"/"+name)
	if p.DeleteACLErr != nil {
		return p.DeleteACLErr
	}
	key := project + "/" + name
	if _, exists := p.ACLs[key]; !exists {
		return NotFound("Network ACL not found")
	}
	delete(p.ACLs, key)
	return nil
}

func (p *Provider) ListNetworkACLsAllProjects(ctx context.Context) ([]api.NetworkACL, error) {
	p.count()
	if p.ListACLsErr != nil {
		return nil, p.ListACLsErr
	}
	acls := []api.NetworkACL{}
	for _, acl := range p.ACLs {
		acls = append(acls, *acl)
	}
	return acls, nil
}

func (p *Provider) GetImageAlias(ctx context.Context, name string) (*api.ImageAliasesEntry, error) {
	p.count()
	if !p.Aliases[name] {
		return nil, NotFound("Image alias not found")
	}
	return &api.ImageAliasesEntry{Name: name, ImageAliasesEntryPut: api.ImageAliasesEntryPut{Target: "fp-" + name}}, nil
}

func (p *Provider) ListImageAliases(ctx context.Context) ([]api.ImageAliasesEntry, error) {
	p.count()
	entries := []api.ImageAliasesEntry{}
	for name := range p.Aliases {
		entries = append(entries, api.ImageAliasesEntry{Name: name})
	}
	return entries, p.ListImageAliasesErr
}

func (p *Provider) CreateImage(ctx context.Context, image api.ImagesPost) error {
	p.count()
	p.CreatedImages = append(p.CreatedImages, image)
	// The alias exists after a pull that succeeded, and after one that failed because another run's pull made it.
	isMadeByAnotherRun := provider.IsOperationFailureContaining(p.CreateImageErr, "already exists")
	if p.CreateImageErr == nil || isMadeByAnotherRun {
		for _, alias := range image.Aliases {
			p.Aliases[alias.Name] = true
		}
	}
	return p.CreateImageErr
}

// ImportedImage is what an ImportImage call delivered: the request, metadata.yaml from the tarball, and the
// rootfs as a digest so a test compares bytes without keeping them.
type ImportedImage struct {
	Image        api.ImagesPost
	Metadata     string
	RootfsSha256 string
	RootfsBytes  int64
}

func (p *Provider) ImportImage(ctx context.Context, image api.ImagesPost, metadata io.Reader, rootfs io.Reader) error {
	p.count()
	metadataYaml, err := readMetadataYaml(metadata)
	if err != nil {
		return err
	}
	digest := sha256.New()
	size, err := io.Copy(digest, rootfs)
	if err != nil {
		return err
	}
	p.ImportedImages = append(p.ImportedImages, ImportedImage{
		Image: image, Metadata: metadataYaml, RootfsSha256: hex.EncodeToString(digest.Sum(nil)), RootfsBytes: size,
	})
	if p.ImportImageErr != nil {
		return p.ImportImageErr
	}
	for _, alias := range image.Aliases {
		p.Aliases[alias.Name] = true
	}
	return nil
}

func readMetadataYaml(metadata io.Reader) (string, error) {
	unzipped, err := gzip.NewReader(metadata)
	if err != nil {
		return "", fmt.Errorf("metadata tarball is not gzip: %w", err)
	}
	archive := tar.NewReader(unzipped)
	for {
		header, err := archive.Next()
		if err == io.EOF {
			return "", errors.New("metadata tarball has no metadata.yaml")
		}
		if err != nil {
			return "", err
		}
		if header.Name == "metadata.yaml" {
			content, err := io.ReadAll(archive)
			return string(content), err
		}
	}
}

func (p *Provider) ListImages(ctx context.Context) ([]api.Image, error) {
	p.count()
	images := []api.Image{}
	for alias := range p.Aliases {
		fingerprint := "fp-" + alias
		architecture := p.ImageArchitectures[fingerprint]
		if architecture == "" {
			architecture = "x86_64"
		}
		images = append(images, api.Image{Fingerprint: fingerprint, Architecture: architecture})
	}
	return images, nil
}

func (p *Provider) GetServerArchitectures(ctx context.Context) ([]string, error) {
	p.count()
	return p.ServerArchitectures, nil
}

func (p *Provider) DeleteImage(ctx context.Context, fingerprint string) error {
	p.count()
	p.DeletedImages = append(p.DeletedImages, fingerprint)
	if p.DeleteImageErr != nil {
		return p.DeleteImageErr
	}
	for alias := range p.Aliases {
		if "fp-"+alias == fingerprint {
			delete(p.Aliases, alias)
		}
	}
	return nil
}

func (p *Provider) PatchInstance(ctx context.Context, project string, name string, instance api.InstancePut) error {
	p.count()
	p.PatchedInstances = append(p.PatchedInstances, instance)
	return p.PatchInstanceErr
}

func (p *Provider) UpdateInstance(ctx context.Context, project string, name string, instance api.InstancePut) error {
	p.count()
	p.UpdatedInstances = append(p.UpdatedInstances, instance)
	if p.AfterUpdateInstance != nil {
		p.AfterUpdateInstance()
	}
	return p.UpdateInstanceErr
}

func (p *Provider) CreateInstanceSnapshot(ctx context.Context, project string, instance string, snapshot string) error {
	p.count()
	if p.CreateSnapshotErr != nil {
		return p.CreateSnapshotErr
	}
	p.InstanceSnapshots[project+"/"+instance+"/"+snapshot] = true
	devices := map[string]map[string]string{}
	source, isThere := p.Instances[project+"/"+instance]
	if isThere {
		for name, settings := range source.Devices {
			devices[name] = settings
		}
	}
	p.SnapshotDevices[project+"/"+instance+"/"+snapshot] = devices
	p.BackupOrder = append(p.BackupOrder, "snapshot "+project+"/"+instance+"/"+snapshot)
	return nil
}

func (p *Provider) GetInstanceSnapshot(ctx context.Context, project string, instance string, snapshot string) (*api.InstanceSnapshot, error) {
	p.count()
	key := project + "/" + instance + "/" + snapshot
	if !p.InstanceSnapshots[key] {
		return nil, NotFound("Instance_snapshot not found")
	}
	return &api.InstanceSnapshot{Name: snapshot, Devices: p.SnapshotDevices[key]}, nil
}

func (p *Provider) DeleteInstanceSnapshot(ctx context.Context, project string, instance string, snapshot string) error {
	p.count()
	if p.DeleteSnapshotErr != nil {
		return p.DeleteSnapshotErr
	}
	key := project + "/" + instance + "/" + snapshot
	if !p.InstanceSnapshots[key] {
		return NotFound("Instance_snapshot not found")
	}
	delete(p.InstanceSnapshots, key)
	p.BackupOrder = append(p.BackupOrder, "delete-snapshot "+key)
	return nil
}

func (p *Provider) GetVolume(ctx context.Context, project string, pool string, name string) (*api.StorageVolume, error) {
	p.count()
	if p.GetVolumeErr != nil {
		return nil, p.GetVolumeErr
	}
	volume, found := p.Volumes[project+"/"+name]
	if !found {
		return nil, NotFound("Storage pool volume not found")
	}
	return volume, nil
}

func (p *Provider) CreateVolume(ctx context.Context, project string, pool string, volume api.StorageVolumesPost) error {
	p.count()
	p.CreatedVolumes = append(p.CreatedVolumes, volume)
	if p.CreateVolumeErr != nil {
		return p.CreateVolumeErr
	}
	p.Volumes[project+"/"+volume.Name] = &api.StorageVolume{Name: volume.Name, Type: "custom", Project: project}
	if volume.Source.Type == "copy" {
		p.BackupOrder = append(p.BackupOrder, "copy-volume "+project+"/"+volume.Source.Name+" -> "+volume.Name)
	}
	return nil
}

func (p *Provider) DeleteVolume(ctx context.Context, project string, pool string, name string) error {
	p.count()
	p.DeletedVolumes = append(p.DeletedVolumes, project+"/"+name)
	if p.DeleteVolumeErr == nil {
		p.BackupOrder = append(p.BackupOrder, "delete-volume "+project+"/"+name)
		delete(p.Volumes, project+"/"+name)
	}
	return p.DeleteVolumeErr
}

func (p *Provider) ListVolumesAllProjects(ctx context.Context, pool string) ([]api.StorageVolume, error) {
	p.count()
	return p.AllVolumes, p.ListVolumesErr
}

func (p *Provider) CreateVolumeSnapshot(ctx context.Context, project string, pool string, volume string, snapshot string) error {
	p.count()
	p.CreatedVolumeSnapshots = append(p.CreatedVolumeSnapshots, project+"/"+volume+"/"+snapshot)
	if p.CreateSnapshotErr != nil {
		return p.CreateSnapshotErr
	}
	p.VolumeSnapshots[project+"/"+volume+"/"+snapshot] = true
	p.BackupOrder = append(p.BackupOrder, "snapshot-volume "+project+"/"+volume+"/"+snapshot)
	return nil
}

func (p *Provider) DeleteVolumeSnapshot(ctx context.Context, project string, pool string, volume string, snapshot string) error {
	p.count()
	p.DeletedVolumeSnapshots = append(p.DeletedVolumeSnapshots, project+"/"+volume+"/"+snapshot)
	if p.DeleteSnapshotErr == nil {
		p.BackupOrder = append(p.BackupOrder, "delete-volume-snapshot "+project+"/"+volume+"/"+snapshot)
		delete(p.VolumeSnapshots, project+"/"+volume+"/"+snapshot)
	}
	return p.DeleteSnapshotErr
}

// ListVolumeSnapshots names each snapshot "<volume>/<snapshot>", as Incus lists them (lab-storage 03).
func (p *Provider) ListVolumeSnapshots(ctx context.Context, project string, pool string, volume string) ([]api.StorageVolumeSnapshot, error) {
	p.count()
	if p.ListVolumeSnapshotsErr != nil {
		return nil, p.ListVolumeSnapshotsErr
	}
	prefix := project + "/" + volume + "/"
	snapshots := []api.StorageVolumeSnapshot{}
	for key := range p.VolumeSnapshots {
		if strings.HasPrefix(key, prefix) {
			snapshots = append(snapshots, api.StorageVolumeSnapshot{Name: volume + "/" + strings.TrimPrefix(key, prefix)})
		}
	}
	return snapshots, nil
}

// PutLoadBalancer places a load balancer on the fake cluster, as a test's starting state.
func (p *Provider) PutLoadBalancer(project string, network string, loadBalancer api.NetworkLoadBalancer) {
	copied := loadBalancer
	p.LoadBalancers[project+"/"+network+"/"+loadBalancer.ListenAddress] = &copied
}

func (p *Provider) GetNetworkLoadBalancer(ctx context.Context, project string, network string, listenAddress string) (*api.NetworkLoadBalancer, string, error) {
	p.count()
	if p.GetLoadBalancerErr != nil {
		return nil, "", p.GetLoadBalancerErr
	}
	loadBalancer, found := p.LoadBalancers[project+"/"+network+"/"+listenAddress]
	if !found {
		return nil, "", NotFound("Network load balancer not found")
	}
	copied := *loadBalancer
	return &copied, "lb-etag", nil
}

func (p *Provider) CreateNetworkLoadBalancer(ctx context.Context, project string, network string, loadBalancer api.NetworkLoadBalancersPost) error {
	p.count()
	p.CreatedLoadBalancers = append(p.CreatedLoadBalancers, loadBalancer)
	if p.CreateLoadBalancerErr != nil {
		return p.CreateLoadBalancerErr
	}
	p.LoadBalancers[project+"/"+network+"/"+loadBalancer.ListenAddress] = &api.NetworkLoadBalancer{
		NetworkLoadBalancerPut: loadBalancer.NetworkLoadBalancerPut, ListenAddress: loadBalancer.ListenAddress,
	}
	return nil
}

func (p *Provider) UpdateNetworkLoadBalancer(ctx context.Context, project string, network string, listenAddress string, loadBalancer api.NetworkLoadBalancerPut, etag string) error {
	p.count()
	p.UpdatedLoadBalancers = append(p.UpdatedLoadBalancers, loadBalancer)
	if p.UpdateLoadBalancerErr != nil {
		return p.UpdateLoadBalancerErr
	}
	key := project + "/" + network + "/" + listenAddress
	if _, found := p.LoadBalancers[key]; !found {
		return NotFound("Network load balancer not found")
	}
	if etag != "lb-etag" {
		return api.StatusErrorf(http.StatusPreconditionFailed, "ETag doesn't match")
	}
	p.LoadBalancers[key] = &api.NetworkLoadBalancer{NetworkLoadBalancerPut: loadBalancer, ListenAddress: listenAddress}
	return nil
}

func (p *Provider) DeleteNetworkLoadBalancer(ctx context.Context, project string, network string, listenAddress string) error {
	p.count()
	key := project + "/" + network + "/" + listenAddress
	p.DeletedLoadBalancers = append(p.DeletedLoadBalancers, key)
	if p.DeleteLoadBalancerErr != nil {
		return p.DeleteLoadBalancerErr
	}
	if _, found := p.LoadBalancers[key]; !found {
		return NotFound("Network load balancer not found")
	}
	delete(p.LoadBalancers, key)
	return nil
}

func (p *Provider) ListNetworkLoadBalancers(ctx context.Context, project string, network string) ([]api.NetworkLoadBalancer, error) {
	p.count()
	if p.ListLoadBalancersErr != nil {
		return nil, p.ListLoadBalancersErr
	}
	if p.MissingNetworks[project+"/"+network] {
		return nil, NotFound("Network not found")
	}
	prefix := project + "/" + network + "/"
	found := []api.NetworkLoadBalancer{}
	for key, loadBalancer := range p.LoadBalancers {
		if strings.HasPrefix(key, prefix) {
			found = append(found, *loadBalancer)
		}
	}
	return found, nil
}

func (p *Provider) BackupInstance(ctx context.Context, project string, name string, target api.BackupTarget) error {
	p.count()
	p.BackupOrder = append(p.BackupOrder, "backup-instance "+project+"/"+name)
	if p.BackupErr != nil {
		return p.BackupErr
	}
	p.InstanceBackups = append(p.InstanceBackups, InstanceBackup{Project: project, Name: name, Target: target})
	return nil
}

func (p *Provider) BackupVolume(ctx context.Context, project string, pool string, name string, target api.BackupTarget) error {
	p.count()
	p.BackupOrder = append(p.BackupOrder, "backup-volume "+project+"/"+name)
	if p.BackupErr != nil {
		return p.BackupErr
	}
	p.VolumeBackups = append(p.VolumeBackups, VolumeBackup{Project: project, Pool: pool, Name: name, Target: target})
	return nil
}
