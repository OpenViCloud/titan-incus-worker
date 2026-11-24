package testsupport

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/lxc/incus/v7/shared/api"
	"go.yaml.in/yaml/v4"

	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
)

// ArchiveDescription is what a test says a fake backup archive holds. It has two forms as bytes, and ImportInstance and
// ImportVolume read either:
//   - ArchiveBody: one line per snapshot the archive carries ("backup" among them for a well-formed one, absent for an
//     old-format archive), for a test whose code only hands the archive to Incus. A blank line is ignored, so a
//     trailing newline is harmless.
//   - TarGz: a gzipped tar that opens on backup/index.yaml, as Incus writes one, for a test whose code reads the index
//     too. Only this form carries Name, Type, Devices and Config.
type ArchiveDescription struct {
	Snapshots []string
	// Name is the Incus name of the instance or volume the archive was taken of.
	Name string
	// Type is "virtual-machine" when left empty; a volume's archive says "custom".
	Type string
	// Devices and Config are the archived instance's own (config.container of the index).
	Devices map[string]map[string]string
	Config  map[string]string
}

// ArchiveBody is the fake archive body a test gives a fake bucket object: ImportInstance/ImportVolume parse it back.
func ArchiveBody(snapshots ...string) string {
	return strings.Join(snapshots, "\n")
}

const archiveIndexPath = "backup/index.yaml"

// archiveIndex is the part of backup/index.yaml the fake import reads.
type archiveIndex struct {
	Name      string   `yaml:"name"`
	Type      string   `yaml:"type"`
	Snapshots []string `yaml:"snapshots"`
	Config    struct {
		Container *archivedInstance `yaml:"container,omitempty"`
	} `yaml:"config"`
}

type archivedInstance struct {
	Config  map[string]string            `yaml:"config"`
	Devices map[string]map[string]string `yaml:"devices"`
}

// TarGz is the archive as real bytes: a gzipped tar whose only entry is backup/index.yaml, the first entry of an
// archive Incus writes. The fake import reads the same index a handler does, so one description feeds both.
func (d ArchiveDescription) TarGz() string {
	index := archiveIndex{Name: d.Name, Type: d.Type, Snapshots: d.Snapshots}
	if index.Type == "" {
		index.Type = string(api.InstanceTypeVM)
	}
	if d.Devices != nil || d.Config != nil {
		index.Config.Container = &archivedInstance{Config: d.Config, Devices: d.Devices}
	}
	content, err := yaml.Marshal(index)
	if err != nil {
		panic(fmt.Sprintf("encode the index of a fake archive: %v", err))
	}
	var archive bytes.Buffer
	zipped := gzip.NewWriter(&archive)
	packed := tar.NewWriter(zipped)
	header := &tar.Header{Name: archiveIndexPath, Mode: 0o644, Size: int64(len(content))}
	if err := packed.WriteHeader(header); err != nil {
		panic(fmt.Sprintf("write a fake archive: %v", err))
	}
	if _, err := packed.Write(content); err != nil {
		panic(fmt.Sprintf("write a fake archive: %v", err))
	}
	if err := packed.Close(); err != nil {
		panic(fmt.Sprintf("write a fake archive: %v", err))
	}
	if err := zipped.Close(); err != nil {
		panic(fmt.Sprintf("write a fake archive: %v", err))
	}
	return archive.String()
}

var gzipMagic = []byte{0x1f, 0x8b}

// parseArchiveDescription reads either form: gzip bytes are a TarGz archive, anything else a snapshot list.
func parseArchiveDescription(body []byte) (ArchiveDescription, error) {
	if bytes.HasPrefix(body, gzipMagic) {
		return parseArchiveIndex(body)
	}
	description := ArchiveDescription{}
	for _, line := range strings.Split(string(body), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			description.Snapshots = append(description.Snapshots, trimmed)
		}
	}
	return description, nil
}

func parseArchiveIndex(body []byte) (ArchiveDescription, error) {
	unzipped, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return ArchiveDescription{}, fmt.Errorf("fake archive is not gzip: %w", err)
	}
	packed := tar.NewReader(unzipped)
	header, err := packed.Next()
	if err != nil {
		return ArchiveDescription{}, fmt.Errorf("fake archive is not a tar: %w", err)
	}
	if header.Name != archiveIndexPath {
		return ArchiveDescription{}, fmt.Errorf("fake archive opens on %q, not %s", header.Name, archiveIndexPath)
	}
	content, err := io.ReadAll(packed)
	if err != nil {
		return ArchiveDescription{}, err
	}
	var index archiveIndex
	if err := yaml.Unmarshal(content, &index); err != nil {
		return ArchiveDescription{}, fmt.Errorf("decode the index of a fake archive: %w", err)
	}
	description := ArchiveDescription{Name: index.Name, Type: index.Type, Snapshots: index.Snapshots}
	if index.Config.Container != nil {
		description.Devices = index.Config.Container.Devices
		description.Config = index.Config.Container.Config
	}
	return description, nil
}

// Provider is an in-memory provider.Provider. Errors set on it are returned by the matching call; every call is
// counted in Calls.
type Provider struct {
	mu sync.Mutex

	Calls int
	// Order names each network and network ACL create/delete in call order, e.g. "create-acl p/acl-x".
	Order []string

	Projects map[string]bool
	// Profiles is keyed by project.
	Profiles         map[string][]api.Profile
	CreatedProjects  []api.ProjectsPost
	DeletedProjects  []string
	DeleteProjectErr error
	// PoolSpace is what GetStoragePoolResources answers, in bytes, unless PoolResourcesErr is set.
	PoolSpace        api.ResourcesStoragePoolSpace
	PoolResourcesErr error
	// Pools is the storage pool each call that names one was given, in call order. The fake holds its volumes by project
	// and name alone, so a call that names the wrong pool is answered like any other: a test reads here that none did.
	Pools []string
	// RunningOperations holds the operations Incus has not finished, keyed by what each names: project + "/" + the
	// instance's or the volume's name. ListRunningInstanceOperations and ListRunningVolumeOperations answer from it,
	// unless ListOperationsErr is set.
	RunningOperations map[string][]api.Operation
	ListOperationsErr error

	Instances map[string]*api.Instance // key: project + "/" + name
	// GetInstanceErr fails every GetInstance and GetInstanceWithEtag.
	GetInstanceErr    error
	CreatedInstances  []api.InstancesPost
	CreateInstanceErr error
	StateChanges      []api.InstanceStatePut
	UpdateStateErr    error
	// UpdateStateErrs answers the state changes in turn (nil is success); once it runs out, UpdateStateErr answers.
	UpdateStateErrs        []error
	DeletedInstances       []string
	DeleteInstanceErr      error
	VirtualMachines        []api.InstanceFull
	ListVirtualMachinesErr error
	GetInstanceFullErr     error
	// Networks holds the cluster's networks; a CreateNetwork that succeeds adds its network, Created, unless one of
	// that project and name is there already.
	Networks         []api.Network
	CreatedNetworks  []api.NetworksPost
	CreateNetworkErr error
	ListNetworksErr  error
	DeletedNetworks  []string
	DeleteNetworkErr error
	// GetNetwork reads Networks; its ETag is NetworkEtag. UpdateNetwork records the write and refuses a stale ETag.
	NetworkEtag      string
	GetNetworkErr    error
	UpdatedNetworks  []api.NetworkPut
	UpdateNetworkErr error
	// NetworkStatuses, when set, is the status each GetNetwork that finds the network answers in turn, the last one
	// repeating; GetNetworkReads counts every read, errors and not found included.
	NetworkStatuses []string
	GetNetworkReads int
	// Leases is keyed by project + "/" + network: a test sets it to that network's DHCP leases.
	Leases        map[string][]api.NetworkLease
	ListLeasesErr error
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
	GetACLErr    error
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
	// InstanceFiles holds each VM's guest files, keyed project + "/" + instance + "/" + path; a key not there is
	// absent, matching Incus's answer for a path it never wrote. GetInstanceFileErr/CreateInstanceFileErr fail a pull
	// or a push outright; InstanceNotRunning fails every file call the way a stopped VM does ("Instance is not
	// running"). CreatedFiles and CreatedDirectories name every path written, in call order.
	InstanceFiles         map[string][]byte
	GetInstanceFileErr    error
	CreateInstanceFileErr error
	InstanceNotRunning    bool
	CreatedFiles          []string
	CreatedDirectories    []string
	InstanceSnapshots     map[string]bool // key: project + "/" + instance + "/" + snapshot
	// SnapshotDevices holds the instance's devices at the time of each snapshot, under the same key.
	SnapshotDevices          map[string]map[string]map[string]string
	CreateSnapshotErr        error
	GetSnapshotErr           error
	DeleteSnapshotErr        error
	ListInstanceSnapshotsErr error
	Volumes                  map[string]*api.StorageVolume // key: project + "/" + name
	GetVolumeErr             error
	// PatchedVolumeConfigs records each PatchVolumeConfig call, one that failed included.
	PatchedVolumeConfigs []VolumeConfigPatch
	PatchVolumeConfigErr error
	// AfterUpdateInstance runs after each recorded UpdateInstance: a test makes the fake follow what Incus would
	// change, such as a volume's used_by once its disk device is gone.
	AfterUpdateInstance func()
	// AfterGetInstance runs after each GetInstance and GetInstanceWithEtag: a test writes the instance there, as another
	// run would between a read and a write. An instance's ETag is its write count, as text.
	AfterGetInstance       func()
	instanceVersions       map[string]int
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
	GetImageAliasErr       error
	ListImagesErr          error
	// An alias's image has fingerprint "fp-" + alias; its architecture is ImageArchitectures[fingerprint], or
	// x86_64 when unset.
	ImageArchitectures map[string]string
	// UnlistedImages are fingerprints ListImages leaves out although an alias points to them.
	UnlistedImages            map[string]bool
	ServerArchitectures       []string
	GetServerArchitecturesErr error
	DeletedImages             []string
	DeleteImageErr            error
	// BackupOrder names each step a backup takes, in call order, e.g. "snapshot p/vm/backup".
	BackupOrder     []string
	InstanceBackups []InstanceBackup
	VolumeBackups   []VolumeBackup
	BackupErr       error
	// CreatedProfiles/UpdatedProfiles, keyed by project + "/" + name for the update, record a restore rebuilding a
	// project's profiles.
	CreatedProfiles  []api.ProfilesPost
	CreateProfileErr error
	UpdatedProfiles  []ProfileUpdate
	UpdateProfileErr error
	// ImportedInstances/ImportedVolumes record each ImportInstance/ImportVolume call, one that failed included: the
	// archive body, so a test tells which archive was used, and an instance's overrides. ImportInstance/ImportVolume
	// decode the body as testsupport.ArchiveDescription (see parseArchiveDescription) and create the instance/volume
	// with the snapshots it names, as Incus's own import would from a real tar.gz.
	ImportedInstances []InstanceImport
	ImportInstanceErr error
	ImportedVolumes   []VolumeImport
	ImportVolumeErr   error
	// RestoredVolumes names each RestoreVolume call, "project/volume/snapshot".
	RestoredVolumes  []string
	RestoreVolumeErr error
	// Metrics is the OpenMetrics text GetMetrics answers, unless GetMetricsErr is set.
	Metrics       string
	GetMetricsErr error
	// madeValues counts the volatile values an import made, so no two are alike.
	madeValues int
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

type ProfileUpdate struct {
	Project string
	Name    string
	Profile api.ProfilePut
}

type InstanceImport struct {
	Project   string
	Pool      string
	Name      string
	Body      string
	Overrides provider.InstanceOverrides
}

type VolumeConfigPatch struct {
	Project string
	Pool    string
	Name    string
	Config  map[string]string
}

type VolumeImport struct {
	Project string
	Pool    string
	Name    string
	Body    string
}

func NewProvider() *Provider {
	return &Provider{
		Projects:           map[string]bool{},
		Profiles:           map[string][]api.Profile{},
		Instances:          map[string]*api.Instance{},
		InstanceFiles:      map[string][]byte{},
		InstanceSnapshots:  map[string]bool{},
		SnapshotDevices:    map[string]map[string]map[string]string{},
		Leases:             map[string][]api.NetworkLease{},
		Volumes:            map[string]*api.StorageVolume{},
		VolumeSnapshots:    map[string]bool{},
		RunningOperations:  map[string][]api.Operation{},
		Aliases:            map[string]bool{},
		ACLs:               map[string]*api.NetworkACL{},
		LoadBalancers:      map[string]*api.NetworkLoadBalancer{},
		MissingNetworks:    map[string]bool{},
		aclVersions:        map[string]int{},
		instanceVersions:   map[string]int{},
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

// countIn counts a call that names a storage pool, and keeps the pool.
func (p *Provider) countIn(pool string) {
	p.mu.Lock()
	p.Calls++
	p.Pools = append(p.Pools, pool)
	p.mu.Unlock()
}

func (p *Provider) GetProject(ctx context.Context, name string) (*api.Project, error) {
	p.count()
	if !p.Projects[name] {
		return nil, NotFound("Project not found")
	}
	return &api.Project{Name: name}, nil
}

func (p *Provider) ListProjects(ctx context.Context) ([]api.Project, error) {
	p.count()
	names := []string{}
	for name := range p.Projects {
		names = append(names, name)
	}
	sort.Strings(names)
	projects := []api.Project{}
	for _, name := range names {
		projects = append(projects, api.Project{Name: name})
	}
	return projects, nil
}

func (p *Provider) ListProfiles(ctx context.Context, project string) ([]api.Profile, error) {
	p.count()
	return p.Profiles[project], nil
}

func (p *Provider) CreateProfile(ctx context.Context, project string, profile api.ProfilesPost) error {
	p.count()
	p.CreatedProfiles = append(p.CreatedProfiles, profile)
	if p.CreateProfileErr != nil {
		return p.CreateProfileErr
	}
	p.Profiles[project] = append(p.Profiles[project], api.Profile{Name: profile.Name, ProfilePut: profile.ProfilePut})
	return nil
}

func (p *Provider) UpdateProfile(ctx context.Context, project string, name string, profile api.ProfilePut) error {
	p.count()
	p.UpdatedProfiles = append(p.UpdatedProfiles, ProfileUpdate{Project: project, Name: name, Profile: profile})
	if p.UpdateProfileErr != nil {
		return p.UpdateProfileErr
	}
	for i := range p.Profiles[project] {
		if p.Profiles[project][i].Name == name {
			p.Profiles[project][i].ProfilePut = profile
		}
	}
	return nil
}

func (p *Provider) CreateProject(ctx context.Context, project api.ProjectsPost) error {
	p.count()
	p.CreatedProjects = append(p.CreatedProjects, project)
	p.Projects[project.Name] = true
	return nil
}

func (p *Provider) DeleteProject(ctx context.Context, name string) error {
	p.count()
	p.DeletedProjects = append(p.DeletedProjects, name)
	p.Order = append(p.Order, "delete-project "+name)
	if p.DeleteProjectErr != nil {
		return p.DeleteProjectErr
	}
	delete(p.Projects, name)
	return nil
}

func (p *Provider) GetStoragePoolResources(ctx context.Context, pool string) (*api.ResourcesStoragePool, error) {
	p.countIn(pool)
	if p.PoolResourcesErr != nil {
		return nil, p.PoolResourcesErr
	}
	return &api.ResourcesStoragePool{Space: p.PoolSpace}, nil
}

func (p *Provider) GetInstance(ctx context.Context, project string, name string) (*api.Instance, error) {
	instance, _, err := p.GetInstanceWithEtag(ctx, project, name)
	return instance, err
}

// GetInstanceWithEtag answers a copy of the devices and config, as Incus answers a fresh body: a handler editing them
// changes nothing here. Neither map is nil, as in a body Incus sends.
func (p *Provider) GetInstanceWithEtag(ctx context.Context, project string, name string) (*api.Instance, string, error) {
	p.count()
	if p.GetInstanceErr != nil {
		return nil, "", p.GetInstanceErr
	}
	key := project + "/" + name
	instance, found := p.Instances[key]
	if !found {
		return nil, "", NotFound("Instance not found")
	}
	copied := *instance
	copied.Config = map[string]string{}
	maps.Copy(copied.Config, instance.Config)
	copied.Devices = cloneDevices(instance.Devices)
	etag := strconv.Itoa(p.instanceVersions[key])
	if p.AfterGetInstance != nil {
		p.AfterGetInstance()
	}
	return &copied, etag, nil
}

func cloneDevices(devices map[string]map[string]string) map[string]map[string]string {
	cloned := map[string]map[string]string{}
	for device, settings := range devices {
		cloned[device] = maps.Clone(settings)
	}
	return cloned
}

// WriteInstance changes an instance's devices as another writer would, so its ETag changes.
func (p *Provider) WriteInstance(project string, name string, change func(devices map[string]map[string]string)) {
	key := project + "/" + name
	change(p.Instances[key].Devices)
	p.instanceVersions[key]++
}

// GetInstanceFull finds the name among VirtualMachines, where a test puts an instance with its state.
func (p *Provider) GetInstanceFull(ctx context.Context, project string, name string) (*api.InstanceFull, error) {
	p.count()
	if p.GetInstanceFullErr != nil {
		return nil, p.GetInstanceFullErr
	}
	for i := range p.VirtualMachines {
		if p.VirtualMachines[i].Name == name {
			return &p.VirtualMachines[i], nil
		}
	}
	return nil, NotFound("Instance not found")
}

func (p *Provider) CreateInstance(ctx context.Context, project string, instance api.InstancesPost) error {
	p.count()
	p.CreatedInstances = append(p.CreatedInstances, instance)
	return p.CreateInstanceErr
}

func (p *Provider) UpdateInstanceState(ctx context.Context, project string, name string, state api.InstanceStatePut) error {
	p.count()
	p.StateChanges = append(p.StateChanges, state)
	if len(p.UpdateStateErrs) > 0 {
		err := p.UpdateStateErrs[0]
		p.UpdateStateErrs = p.UpdateStateErrs[1:]
		return err
	}
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
	if p.CreateNetworkErr != nil {
		return p.CreateNetworkErr
	}
	for _, held := range p.Networks {
		if held.Project == project && held.Name == network.Name {
			return nil
		}
	}
	p.Networks = append(p.Networks, api.Network{
		Name: network.Name, Project: project, Type: network.Type, Status: "Created", NetworkPut: network.NetworkPut,
	})
	return nil
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
	p.GetNetworkReads++
	if p.GetNetworkErr != nil {
		return nil, "", p.GetNetworkErr
	}
	for i := range p.Networks {
		network := p.Networks[i]
		if network.Project == project && network.Name == name {
			if len(p.NetworkStatuses) > 0 {
				network.Status = p.NetworkStatuses[0]
			}
			if len(p.NetworkStatuses) > 1 {
				p.NetworkStatuses = p.NetworkStatuses[1:]
			}
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

func (p *Provider) ListNetworkLeases(ctx context.Context, project string, network string) ([]api.NetworkLease, error) {
	p.count()
	if p.ListLeasesErr != nil {
		return nil, p.ListLeasesErr
	}
	return p.Leases[project+"/"+network], nil
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
	if p.GetACLErr != nil {
		return nil, "", p.GetACLErr
	}
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
	if p.GetImageAliasErr != nil {
		return nil, p.GetImageAliasErr
	}
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
	if p.ListImagesErr != nil {
		return nil, p.ListImagesErr
	}
	images := []api.Image{}
	for alias := range p.Aliases {
		fingerprint := "fp-" + alias
		if p.UnlistedImages[fingerprint] {
			continue
		}
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
	return p.ServerArchitectures, p.GetServerArchitecturesErr
}

func (p *Provider) GetMetrics(ctx context.Context) (string, error) {
	p.count()
	if p.GetMetricsErr != nil {
		return "", p.GetMetricsErr
	}
	return p.Metrics, nil
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

// PatchInstance merges into an instance the fake holds as Incus's PATCH does: each device named replaces that device
// whole, each config key named is set, the rest stays. An instance the fake does not hold is only recorded.
func (p *Provider) PatchInstance(ctx context.Context, project string, name string, instance api.InstancePut) error {
	p.count()
	key := project + "/" + name
	if p.PatchInstanceErr == nil {
		p.instanceVersions[key]++
		if stored, found := p.Instances[key]; found {
			if stored.Devices == nil {
				stored.Devices = map[string]map[string]string{}
			}
			maps.Copy(stored.Devices, cloneDevices(instance.Devices))
			if stored.Config == nil {
				stored.Config = map[string]string{}
			}
			maps.Copy(stored.Config, instance.Config)
		}
	}
	p.PatchedInstances = append(p.PatchedInstances, instance)
	return p.PatchInstanceErr
}

// UpdateInstance replaces the writable part of an instance the fake holds, so the next read sees the write. A rollback
// (Restore set) is recorded and leaves the devices and config as they are, which is what a disk-only one does on
// Incus; a full rollback's return to the snapshot's devices and config is not played. An instance the fake does not
// hold is only recorded.
func (p *Provider) UpdateInstance(ctx context.Context, project string, name string, instance api.InstancePut, etag string) error {
	p.count()
	key := project + "/" + name
	if etag != "" && etag != strconv.Itoa(p.instanceVersions[key]) {
		return api.StatusErrorf(http.StatusPreconditionFailed, "ETag doesn't match")
	}
	if p.UpdateInstanceErr == nil {
		p.instanceVersions[key]++
		stored, found := p.Instances[key]
		isRollback := instance.Restore != ""
		if found && !isRollback {
			stored.InstancePut = instance
			stored.Devices = cloneDevices(instance.Devices)
			stored.Config = maps.Clone(instance.Config)
		}
	}
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
	if p.GetSnapshotErr != nil {
		return nil, p.GetSnapshotErr
	}
	key := project + "/" + instance + "/" + snapshot
	if !p.InstanceSnapshots[key] {
		return nil, NotFound("Instance_snapshot not found")
	}
	return &api.InstanceSnapshot{Name: snapshot, Devices: p.SnapshotDevices[key]}, nil
}

// ListInstanceSnapshots names each snapshot bare, as Incus does for instance snapshots (unlike a volume's, which Incus
// names "<volume>/<snapshot>"), in the order of their names: a list a test can assert the order of.
func (p *Provider) ListInstanceSnapshots(ctx context.Context, project string, instance string) ([]api.InstanceSnapshot, error) {
	p.count()
	if p.ListInstanceSnapshotsErr != nil {
		return nil, p.ListInstanceSnapshotsErr
	}
	prefix := project + "/" + instance + "/"
	snapshots := []api.InstanceSnapshot{}
	for _, key := range slices.Sorted(maps.Keys(p.InstanceSnapshots)) {
		if strings.HasPrefix(key, prefix) {
			name := strings.TrimPrefix(key, prefix)
			snapshots = append(snapshots, api.InstanceSnapshot{Name: name, Devices: p.SnapshotDevices[key]})
		}
	}
	return snapshots, nil
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

// instanceNotRunning is the text IsInstanceNotRunning matches (titan-ops features/vm/design.md#vm-key-addition: measured
// through the CLI's SFTP wrapper; the fake does not reproduce that wrapper, only the text the classifier reads).
func instanceNotRunning() error {
	return errors.New("Instance is not running")
}

func (p *Provider) GetInstanceFile(ctx context.Context, project string, instance string, path string) ([]byte, error) {
	p.count()
	if p.InstanceNotRunning {
		return nil, instanceNotRunning()
	}
	if p.GetInstanceFileErr != nil {
		return nil, p.GetInstanceFileErr
	}
	content, found := p.InstanceFiles[project+"/"+instance+"/"+path]
	if !found {
		return nil, NotFound("file does not exist")
	}
	return content, nil
}

func (p *Provider) CreateInstanceFile(ctx context.Context, project string, instance string, path string, content []byte, uid int64, gid int64, mode int) error {
	p.count()
	if p.InstanceNotRunning {
		return instanceNotRunning()
	}
	if p.CreateInstanceFileErr != nil {
		return p.CreateInstanceFileErr
	}
	p.InstanceFiles[project+"/"+instance+"/"+path] = content
	p.CreatedFiles = append(p.CreatedFiles, path)
	return nil
}

func (p *Provider) CreateInstanceDirectory(ctx context.Context, project string, instance string, path string) error {
	p.count()
	if p.InstanceNotRunning {
		return instanceNotRunning()
	}
	p.CreatedDirectories = append(p.CreatedDirectories, path)
	return nil
}

func (p *Provider) GetVolume(ctx context.Context, project string, pool string, name string) (*api.StorageVolume, error) {
	p.countIn(pool)
	if p.GetVolumeErr != nil {
		return nil, p.GetVolumeErr
	}
	volume, found := p.Volumes[project+"/"+name]
	if !found {
		return nil, NotFound("Storage pool volume not found")
	}
	return volume, nil
}

// CreateVolume records the call, then answers 409 to a name the project already has, as Incus does (lab-storage 02).
func (p *Provider) CreateVolume(ctx context.Context, project string, pool string, volume api.StorageVolumesPost) error {
	p.countIn(pool)
	p.CreatedVolumes = append(p.CreatedVolumes, volume)
	if p.CreateVolumeErr != nil {
		return p.CreateVolumeErr
	}
	if _, exists := p.Volumes[project+"/"+volume.Name]; exists {
		return api.StatusErrorf(http.StatusConflict, "Volume by that name already exists")
	}
	p.Volumes[project+"/"+volume.Name] = &api.StorageVolume{Name: volume.Name, Type: "custom", Project: project}
	return nil
}

func (p *Provider) PatchVolumeConfig(ctx context.Context, project string, pool string, name string, config map[string]string) error {
	p.countIn(pool)
	p.PatchedVolumeConfigs = append(p.PatchedVolumeConfigs, VolumeConfigPatch{Project: project, Pool: pool, Name: name, Config: maps.Clone(config)})
	if p.PatchVolumeConfigErr != nil {
		return p.PatchVolumeConfigErr
	}
	volume, found := p.Volumes[project+"/"+name]
	if !found {
		return NotFound("Storage pool volume not found")
	}
	if volume.Config == nil {
		volume.Config = map[string]string{}
	}
	maps.Copy(volume.Config, config)
	return nil
}

// DeleteVolume records the call, then refuses a volume an instance of the project has as a disk, as Incus does (400,
// lab-storage 04).
func (p *Provider) DeleteVolume(ctx context.Context, project string, pool string, name string) error {
	p.countIn(pool)
	p.DeletedVolumes = append(p.DeletedVolumes, project+"/"+name)
	if p.DeleteVolumeErr != nil {
		return p.DeleteVolumeErr
	}
	if p.isAttached(project, name) {
		return api.StatusErrorf(http.StatusBadRequest, "The storage volume is still in use")
	}
	p.BackupOrder = append(p.BackupOrder, "delete-volume "+project+"/"+name)
	delete(p.Volumes, project+"/"+name)
	return nil
}

// isAttached says whether an instance of the project has the volume as a disk device of its own.
func (p *Provider) isAttached(project string, volume string) bool {
	for key, instance := range p.Instances {
		if !strings.HasPrefix(key, project+"/") {
			continue
		}
		for _, device := range instance.Devices {
			if device["type"] == "disk" && device["source"] == volume {
				return true
			}
		}
	}
	return false
}

func (p *Provider) ListVolumesAllProjects(ctx context.Context, pool string) ([]api.StorageVolume, error) {
	p.countIn(pool)
	return p.AllVolumes, p.ListVolumesErr
}

func (p *Provider) CreateVolumeSnapshot(ctx context.Context, project string, pool string, volume string, snapshot string) error {
	p.countIn(pool)
	p.CreatedVolumeSnapshots = append(p.CreatedVolumeSnapshots, project+"/"+volume+"/"+snapshot)
	if p.CreateSnapshotErr != nil {
		return p.CreateSnapshotErr
	}
	p.VolumeSnapshots[project+"/"+volume+"/"+snapshot] = true
	p.BackupOrder = append(p.BackupOrder, "snapshot-volume "+project+"/"+volume+"/"+snapshot)
	return nil
}

func (p *Provider) DeleteVolumeSnapshot(ctx context.Context, project string, pool string, volume string, snapshot string) error {
	p.countIn(pool)
	p.DeletedVolumeSnapshots = append(p.DeletedVolumeSnapshots, project+"/"+volume+"/"+snapshot)
	if p.DeleteSnapshotErr == nil {
		p.BackupOrder = append(p.BackupOrder, "delete-volume-snapshot "+project+"/"+volume+"/"+snapshot)
		delete(p.VolumeSnapshots, project+"/"+volume+"/"+snapshot)
	}
	return p.DeleteSnapshotErr
}

// ListVolumeSnapshots names each snapshot "<volume>/<snapshot>", as Incus lists them (lab-storage 03), in the order
// of their names.
func (p *Provider) ListVolumeSnapshots(ctx context.Context, project string, pool string, volume string) ([]api.StorageVolumeSnapshot, error) {
	p.countIn(pool)
	if p.ListVolumeSnapshotsErr != nil {
		return nil, p.ListVolumeSnapshotsErr
	}
	prefix := project + "/" + volume + "/"
	snapshots := []api.StorageVolumeSnapshot{}
	for _, key := range slices.Sorted(maps.Keys(p.VolumeSnapshots)) {
		if strings.HasPrefix(key, prefix) {
			snapshots = append(snapshots, api.StorageVolumeSnapshot{Name: volume + "/" + strings.TrimPrefix(key, prefix)})
		}
	}
	return snapshots, nil
}

// ListRunningInstanceOperations answers what a test put in RunningOperations for the instance.
func (p *Provider) ListRunningInstanceOperations(ctx context.Context, project string, name string) ([]api.Operation, error) {
	p.count()
	if p.ListOperationsErr != nil {
		return nil, p.ListOperationsErr
	}
	return p.RunningOperations[project+"/"+name], nil
}

// ListRunningVolumeOperations answers what a test put in RunningOperations for the volume.
func (p *Provider) ListRunningVolumeOperations(ctx context.Context, project string, pool string, name string) ([]api.Operation, error) {
	p.countIn(pool)
	if p.ListOperationsErr != nil {
		return nil, p.ListOperationsErr
	}
	return p.RunningOperations[project+"/"+name], nil
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
	p.countIn(pool)
	p.BackupOrder = append(p.BackupOrder, "backup-volume "+project+"/"+name)
	if p.BackupErr != nil {
		return p.BackupErr
	}
	p.VolumeBackups = append(p.VolumeBackups, VolumeBackup{Project: project, Pool: pool, Name: name, Target: target})
	return nil
}

// importRefused is how Incus refuses an import it has received whole: its operation ("Restoring backup" for an
// instance, "Restoring custom volume backup" for a volume) fails with this message.
func importRefused(operation string, message string) error {
	return &provider.OperationFailedError{Description: operation, Status: "Failure", Message: message}
}

// ImportInstance reads backupFile whole, in either form of testsupport.ArchiveDescription, and creates the instance it
// describes, stopped, with the snapshots it names and the overrides applied, as Incus's own import would. It refuses
// what Incus 7.0.2 was measured to refuse (titan-ops features/backup/design.md#restore-as-new-resource): a name already
// there, a custom volume device whose volume is attached to another instance, and one whose volume is not in the project.
func (p *Provider) ImportInstance(ctx context.Context, project string, pool string, name string, backupFile io.Reader, overrides provider.InstanceOverrides) error {
	p.countIn(pool)
	body, err := io.ReadAll(backupFile)
	if err != nil {
		return err
	}
	p.ImportedInstances = append(p.ImportedInstances, InstanceImport{Project: project, Pool: pool, Name: name, Body: string(body), Overrides: overrides})
	if p.ImportInstanceErr != nil {
		return p.ImportInstanceErr
	}
	description, err := parseArchiveDescription(body)
	if err != nil {
		return err
	}
	devices, config, err := p.overridden(description, overrides)
	if err != nil {
		return err
	}
	key := project + "/" + name
	if _, exists := p.Instances[key]; exists {
		return importRefused("Restoring backup", "Create instance from backup: Cannot restore volume, already exists on target")
	}
	if err := p.checkVolumeDevices(project, devices); err != nil {
		return err
	}
	p.Instances[key] = &api.Instance{Name: name, StatusCode: api.Stopped, InstancePut: api.InstancePut{Devices: devices, Config: config}}
	for _, snapshot := range description.Snapshots {
		p.InstanceSnapshots[key+"/"+snapshot] = true
		p.SnapshotDevices[key+"/"+snapshot] = map[string]map[string]string{}
	}
	return nil
}

// overridden is the archived instance's devices and config with the overrides applied, by the rules of Incus's import
// (cmd/incusd/instances_post.go and api_internal.go of v7.0.1): "<device>,<key>=<value>" sets one key of one device,
// adding the device when the archive has none of that name; "<key>=<value>" sets one config key. A key left empty is
// not kept, except that Incus fills an empty volatile.uuid, and an empty volatile.<nic>.hwaddr of a NIC the instance
// has, with a value of the instance's own.
func (p *Provider) overridden(description ArchiveDescription, overrides provider.InstanceOverrides) (map[string]map[string]string, map[string]string, error) {
	devices := cloneDevices(description.Devices)
	config := map[string]string{}
	maps.Copy(config, description.Config)
	for _, entry := range overrides.Devices {
		device, setting, hasDevice := strings.Cut(entry, ",")
		settingKey, value, hasValue := strings.Cut(setting, "=")
		if !hasDevice || !hasValue {
			return nil, nil, api.StatusErrorf(http.StatusBadRequest, "Failed to parse device <device>,<key>=<value>: %q", entry)
		}
		if devices[device] == nil {
			devices[device] = map[string]string{}
		}
		devices[device][settingKey] = value
	}
	for _, entry := range overrides.Config {
		configKey, value, hasValue := strings.Cut(entry, "=")
		if !hasValue {
			return nil, nil, api.StatusErrorf(http.StatusBadRequest, "Failed to parse config <key>=<value>: %q", entry)
		}
		config[configKey] = value
	}
	for _, settings := range devices {
		maps.DeleteFunc(settings, func(_ string, value string) bool { return value == "" })
	}
	for _, configKey := range slices.Sorted(maps.Keys(config)) {
		if config[configKey] != "" {
			continue
		}
		device, isHwaddr := strings.CutSuffix(configKey, ".hwaddr")
		device, isVolatile := strings.CutPrefix(device, "volatile.")
		isNicHwaddr := isHwaddr && isVolatile && devices[device]["type"] == "nic"
		switch {
		case configKey == "volatile.uuid":
			p.madeValues++
			config[configKey] = fmt.Sprintf("00000000-0000-4000-8000-%012d", p.madeValues)
		case isNicHwaddr:
			p.madeValues++
			config[configKey] = fmt.Sprintf("10:66:6a:%02x:%02x:%02x", p.madeValues>>16&0xff, p.madeValues>>8&0xff, p.madeValues&0xff)
		default:
			delete(config, configKey)
		}
	}
	return devices, config, nil
}

// checkVolumeDevices validates each custom volume device (a disk with a pool and a source) of an instance about to be
// made in project, as Incus does when it adds the device: the volume is there, and no other instance has it. The
// refusals are worded as the v7.0.1 source words them (cmd/incusd/instances_post.go, cmd/incusd/api_internal.go,
// internal/server/instance/drivers/driver_common.go, internal/server/device/disk.go).
func (p *Provider) checkVolumeDevices(project string, devices map[string]map[string]string) error {
	for _, name := range slices.Sorted(maps.Keys(devices)) {
		device := devices[name]
		isVolume := device["type"] == "disk" && device["pool"] != "" && device["source"] != ""
		if !isVolume {
			continue
		}
		refusal := fmt.Sprintf("Failed importing backup: Failed creating instance record: Failed add validation for device %q: ", name)
		if _, found := p.Volumes[project+"/"+device["source"]]; !found {
			return importRefused("Restoring backup", refusal+"Failed loading custom volume: Storage volume not found")
		}
		if p.isAttached(project, device["source"]) {
			return importRefused("Restoring backup", refusal+"Cannot add un-shared custom storage block volume to more than one instance")
		}
	}
	return nil
}

// ImportVolume reads backupFile whole, in either form of testsupport.ArchiveDescription, and creates the volume with
// the snapshots it names. A name already there is refused as Incus 7.0.2 was measured to
// (titan-ops features/backup/design.md#restore-as-new-resource).
func (p *Provider) ImportVolume(ctx context.Context, project string, pool string, name string, backupFile io.Reader) error {
	p.countIn(pool)
	body, err := io.ReadAll(backupFile)
	if err != nil {
		return err
	}
	p.ImportedVolumes = append(p.ImportedVolumes, VolumeImport{Project: project, Pool: pool, Name: name, Body: string(body)})
	if p.ImportVolumeErr != nil {
		return p.ImportVolumeErr
	}
	description, err := parseArchiveDescription(body)
	if err != nil {
		return err
	}
	if _, exists := p.Volumes[project+"/"+name]; exists {
		return importRefused("Restoring custom volume backup", fmt.Sprintf("Create custom volume from backup: Volume %q already exists in pool %q", name, pool))
	}
	p.Volumes[project+"/"+name] = &api.StorageVolume{Name: name, Type: "custom", Project: project}
	for _, snapshot := range description.Snapshots {
		p.VolumeSnapshots[project+"/"+name+"/"+snapshot] = true
	}
	return nil
}

func (p *Provider) RestoreVolume(ctx context.Context, project string, pool string, name string, snapshot string) error {
	p.countIn(pool)
	p.RestoredVolumes = append(p.RestoredVolumes, project+"/"+name+"/"+snapshot)
	return p.RestoreVolumeErr
}
