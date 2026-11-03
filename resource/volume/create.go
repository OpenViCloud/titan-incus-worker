package volume

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
)

// DisplayNameKey carries the name the tenant typed.
const DisplayNameKey = "user.titan-name"

// createPayload mirrors ProviderCommandQueue.EnqueueVolumeCreateAsync.
type createPayload struct {
	VolumeId string `json:"volumeId"`
	// RequestId names the request this command ends (titan-ops architecture/resource-lifecycle.md#callback-routes). Empty
	// on a command of the old control plane that the migration gave no id: it keeps the old reporting until rollout
	// phase 6 (titan-ops architecture/resource-lifecycle.md#rollout).
	RequestId   string `json:"requestId"`
	Name        string `json:"name"`
	SizeGb      int    `json:"sizeGb"`
	ProjectName string `json:"projectName"`
}

// Create runs volume.create: an empty block volume in the tenant's project. Incus has made it once the request
// returns, so the create is done: Done with the provider id and what Incus then shows of the volume.
type Create struct {
	cp       *controlplane.Client
	clusters cluster.Clusters
}

func NewCreate(cp *controlplane.Client, clusters cluster.Clusters) *Create {
	return &Create{cp: cp, clusters: clusters}
}

func (h *Create) Run(ctx context.Context, leased command.Leased, logger *slog.Logger) error {
	var payload createPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	isComplete := payload.VolumeId != "" && payload.ProjectName != "" && payload.SizeGb > 0
	if !isComplete {
		return command.Permanent(fmt.Errorf("volume.create payload lacks volumeId, projectName or sizeGb"))
	}
	name, err := provider.VolumeName(payload.VolumeId)
	if err != nil {
		return command.Permanent(err)
	}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return err
	}
	if err := provider.EnsureProject(ctx, cl.Provider, payload.ProjectName); err != nil {
		return err
	}

	request := api.StorageVolumesPost{
		Name:        name,
		Type:        "custom",
		ContentType: "block",
		StorageVolumePut: api.StorageVolumePut{Config: map[string]string{
			"size":         strconv.Itoa(payload.SizeGb) + "GiB",
			DisplayNameKey: payload.Name,
		}},
	}
	err = cl.Provider.CreateVolume(ctx, payload.ProjectName, provider.StoragePoolOrDefault(cl.StoragePool), request)
	// 409: an earlier run of this command made it (lab-storage 02); the name is ours.
	isOurs := err != nil && provider.IsConflict(err)
	if err != nil && !isOurs {
		return err
	}
	if isOurs {
		logger.Info("already exists; treating it as ours", "object", name)
	}
	if payload.RequestId == "" {
		return SendStatus(ctx, h.cp, payload.VolumeId, StatusReport{Report: ReportCreated, ExternalId: name})
	}
	pool := provider.StoragePoolOrDefault(cl.StoragePool)
	observed := ReadObservation(ctx, cl.Provider, payload.ProjectName, pool, name, logger)
	return EndRequest(ctx, h.cp, payload.VolumeId, payload.RequestId, Done(name, observed))
}

// GiveUp reports the request failed, with the provider id when the volume exists: then the create did happen.
func (h *Create) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	var payload createPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	existing := ExistingVolume(ctx, h.clusters, leased.ClusterId, payload.ProjectName, payload.VolumeId)
	if payload.RequestId != "" {
		return EndRequest(ctx, h.cp, payload.VolumeId, payload.RequestId, Failed(existing, nil, ReasonCreateFailed, cause))
	}
	report := StatusReport{Report: ReportRequestFailed, Reason: ReasonCreateFailed, Detail: cause.Error(), ExternalId: existing}
	return SendStatus(ctx, h.cp, payload.VolumeId, report)
}

// ReadObservation reads the volume once after a command did its work. A read that fails does not undo that work, so the
// report goes without it and the reconciler reports the volume next cycle.
func ReadObservation(ctx context.Context, p provider.Provider, project string, pool string, name string, logger *slog.Logger) *Observation {
	volume, err := p.GetVolume(ctx, project, pool, name)
	if err != nil {
		logger.Warn("volume not read after its command; the reconciler reports it", "object", name, "error", err)
		return nil
	}
	return &Observation{AttachedTo: InstanceNames(volume.UsedBy)}
}

// ExistingVolume returns the volume's Incus name when it is there, "" when it is not or cannot be read.
func ExistingVolume(ctx context.Context, clusters cluster.Clusters, clusterId string, project string, volumeId string) string {
	if project == "" {
		return ""
	}
	name, err := provider.VolumeName(volumeId)
	if err != nil {
		return ""
	}
	cl, err := clusters.Cluster(ctx, clusterId)
	if err != nil {
		return ""
	}
	_, err = cl.Provider.GetVolume(ctx, project, provider.StoragePoolOrDefault(cl.StoragePool), name)
	if err != nil {
		return ""
	}
	return name
}
