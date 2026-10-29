// Package provider is the worker's narrow port onto one Incus cluster. Every call names its project; every call that
// starts an Incus operation waits for it to finish before returning.
package provider

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/lxc/incus/v7/shared/api"
)

// Provider is what the handlers and reconcilers call; the Incus adapter implements it, tests use a fake.
type Provider interface {
	GetProject(ctx context.Context, name string) (*api.Project, error)
	CreateProject(ctx context.Context, project api.ProjectsPost) error

	GetInstance(ctx context.Context, project string, name string) (*api.Instance, error)
	CreateInstance(ctx context.Context, project string, instance api.InstancesPost) error
	UpdateInstanceState(ctx context.Context, project string, name string, state api.InstanceStatePut) error
	DeleteInstance(ctx context.Context, project string, name string) error
	ListVirtualMachinesAllProjects(ctx context.Context) ([]api.InstanceFull, error)

	CreateNetwork(ctx context.Context, project string, network api.NetworksPost) error
	DeleteNetwork(ctx context.Context, project string, name string) error
	ListNetworksAllProjects(ctx context.Context) ([]api.Network, error)
	// GetNetwork returns the network with its ETag; UpdateNetwork replaces its writable part only if the ETag still
	// matches (412 otherwise).
	GetNetwork(ctx context.Context, project string, name string) (*api.Network, string, error)
	UpdateNetwork(ctx context.Context, project string, name string, network api.NetworkPut, etag string) error

	// Network ACLs carry a tenant network's firewall rules (titan-ops features/network/design.md#firewall-on-incus).
	GetNetworkACL(ctx context.Context, project string, name string) (*api.NetworkACL, string, error)
	CreateNetworkACL(ctx context.Context, project string, acl api.NetworkACLsPost) error
	UpdateNetworkACL(ctx context.Context, project string, name string, acl api.NetworkACLPut, etag string) error
	DeleteNetworkACL(ctx context.Context, project string, name string) error
	ListNetworkACLsAllProjects(ctx context.Context) ([]api.NetworkACL, error)

	// Network load balancers live on a tenant network and are named by their listen address, the VIP
	// (titan-ops features/load-balancer/design.md#lb-resource-model). UpdateNetworkLoadBalancer replaces the writable part
	// only if the ETag still matches (412 otherwise).
	GetNetworkLoadBalancer(ctx context.Context, project string, network string, listenAddress string) (*api.NetworkLoadBalancer, string, error)
	CreateNetworkLoadBalancer(ctx context.Context, project string, network string, loadBalancer api.NetworkLoadBalancersPost) error
	UpdateNetworkLoadBalancer(ctx context.Context, project string, network string, listenAddress string, loadBalancer api.NetworkLoadBalancerPut, etag string) error
	DeleteNetworkLoadBalancer(ctx context.Context, project string, network string, listenAddress string) error
	ListNetworkLoadBalancers(ctx context.Context, project string, network string) ([]api.NetworkLoadBalancer, error)

	// PatchInstance merges config and devices (attach, lab-storage 04: sync); UpdateInstance replaces them, or with
	// Restore set puts a snapshot back (detach and revert, lab-storage 04b and 05: an operation).
	PatchInstance(ctx context.Context, project string, name string, instance api.InstancePut) error
	UpdateInstance(ctx context.Context, project string, name string, instance api.InstancePut) error
	CreateInstanceSnapshot(ctx context.Context, project string, instance string, snapshot string) error
	DeleteInstanceSnapshot(ctx context.Context, project string, instance string, snapshot string) error
	// GetInstanceSnapshot answers the snapshot, or a 404 error when it is not there.
	GetInstanceSnapshot(ctx context.Context, project string, instance string, snapshot string) (*api.InstanceSnapshot, error)

	// Custom volumes of a storage pool. CreateVolume makes an empty volume or, with a copy source, a volume from a
	// snapshot (restore, lab-storage 03).
	GetVolume(ctx context.Context, project string, pool string, name string) (*api.StorageVolume, error)
	CreateVolume(ctx context.Context, project string, pool string, volume api.StorageVolumesPost) error
	DeleteVolume(ctx context.Context, project string, pool string, name string) error
	// The all-projects list carries no snapshots (lab-storage 02): ListVolumeSnapshots reads them per volume.
	ListVolumesAllProjects(ctx context.Context, pool string) ([]api.StorageVolume, error)
	CreateVolumeSnapshot(ctx context.Context, project string, pool string, volume string, snapshot string) error
	DeleteVolumeSnapshot(ctx context.Context, project string, pool string, volume string, snapshot string) error
	ListVolumeSnapshots(ctx context.Context, project string, pool string, volume string) ([]api.StorageVolumeSnapshot, error)

	// BackupInstance and BackupVolume have Incus upload an archive of the instance (root disk only) or of the volume
	// to the S3 target itself, and wait for its operation. The operation ends before the upload does, and drops an upload
	// failure (read in the v7.0.1 source): the caller checks the bucket (API extension backup_s3_upload; R2 measured in
	// titan-ops features/backup/design.md#incus-backup).
	BackupInstance(ctx context.Context, project string, name string, target api.BackupTarget) error
	BackupVolume(ctx context.Context, project string, pool string, name string, target api.BackupTarget) error

	// Images and aliases live in the default project: Titan's projects have features.images=false (lab 03c).
	GetImageAlias(ctx context.Context, name string) (*api.ImageAliasesEntry, error)
	ListImageAliases(ctx context.Context) ([]api.ImageAliasesEntry, error)
	CreateImage(ctx context.Context, image api.ImagesPost) error
	// ImportImage uploads a VM image as metadata tarball + qcow2 rootfs, read as streams (never held whole).
	ImportImage(ctx context.Context, image api.ImagesPost, metadata io.Reader, rootfs io.Reader) error
	ListImages(ctx context.Context) ([]api.Image, error)
	DeleteImage(ctx context.Context, fingerprint string) error

	// The architectures this cluster runs (environment.architectures of /1.0), e.g. aarch64 on an arm64 host.
	GetServerArchitectures(ctx context.Context) ([]string, error)
}

// OperationFailedError: the Incus operation finished with Failure (or Cancelled). Message is the operation's err.
type OperationFailedError struct {
	Description string
	Status      string
	Message     string
}

func (e *OperationFailedError) Error() string {
	return "incus operation \"" + e.Description + "\" ended " + e.Status + ": " + e.Message
}

// IsNotFound covers a missing instance, network, alias and a missing project alike: Incus answers 404 for each.
func IsNotFound(err error) bool {
	return api.StatusErrorCheck(err, http.StatusNotFound)
}

func IsConflict(err error) bool {
	return api.StatusErrorCheck(err, http.StatusConflict)
}

// IsAclAlreadyExists: Incus refuses a duplicate network ACL with 400, not 409 (cmd/incusd/network_acls.go of v7.0.1).
func IsAclAlreadyExists(err error) bool {
	return api.StatusErrorCheck(err, http.StatusBadRequest) && strings.Contains(err.Error(), "The network ACL already exists")
}

// IsEtagMismatch: someone changed the object between our read and our write.
func IsEtagMismatch(err error) bool {
	return api.StatusErrorCheck(err, http.StatusPreconditionFailed)
}

// IsInstanceAlreadyExists: Incus refuses a duplicate instance name with a synchronous 500, not 409 (lab 04b).
func IsInstanceAlreadyExists(err error) bool {
	return api.StatusErrorCheck(err, http.StatusInternalServerError) && strings.Contains(err.Error(), "already exists")
}

// IsInstanceSnapshotAlreadyExists: a duplicate instance snapshot fails in its operation, not with a 409 as a volume
// snapshot does (lab-storage 05).
func IsInstanceSnapshotAlreadyExists(err error) bool {
	return IsOperationFailureContaining(err, `This "Instance_snapshot" entry already exists`)
}

// IsOperationFailureContaining: the stop/start "already stopped"/"already running" answers arrive as a failed
// operation, not as an error from the request (lab 05).
func IsOperationFailureContaining(err error, text string) bool {
	var failed *OperationFailedError
	if !errors.As(err, &failed) {
		return false
	}
	return strings.Contains(failed.Message, text)
}
