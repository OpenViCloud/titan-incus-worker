package backup

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
)

// vmPayload mirrors ProviderCommandQueue.EnqueueVmBackupAsync.
type vmPayload struct {
	VmId         string `json:"vmId"`
	ExternalVmId string `json:"externalVmId"`
	ProjectName  string `json:"projectName"`
	TenantId     string `json:"tenantId"`
}

// Vm runs vm.backup. On an LVM pool Incus reads a running VM's disk as it is being written, so the backup is of a
// stopped copy of a snapshot: one point in time, as after a power cut. Attached volumes are not in it; each has its own
// volume.backup. The command is done when the archive is in the bucket; there is no callback.
type Vm struct {
	clusters cluster.Clusters
	target   Target
	objects  Objects
	arrival  Arrival
	now      func() time.Time
}

func NewVm(clusters cluster.Clusters, target Target, objects Objects) *Vm {
	return &Vm{clusters: clusters, target: target, objects: objects, arrival: DefaultArrival, now: time.Now}
}

func (h *Vm) Run(ctx context.Context, leased command.Leased, logger *slog.Logger) error {
	payload, err := decodeVm(leased)
	if err != nil {
		return err
	}
	if err := h.target.usable(); err != nil {
		return err
	}
	copyName, err := provider.BackupInstanceCopyName(payload.VmId)
	if err != nil {
		return command.Permanent(err)
	}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return err
	}
	p := cl.Provider
	project, instance := payload.ProjectName, payload.ExternalVmId

	_, err = p.GetInstance(ctx, project, instance)
	if provider.IsNotFound(err) {
		return command.Permanent(fmt.Errorf("instance %s is not in project %s: nothing to back up", instance, project))
	}
	if err != nil {
		return err
	}
	if err := removeVmLeftovers(ctx, p, project, instance, copyName); err != nil {
		return err
	}

	if err := p.CreateInstanceSnapshot(ctx, project, instance, snapshotName); err != nil {
		return err
	}
	key := ObjectKey(leased.ClusterId, payload.TenantId, "vms", payload.VmId, h.now())
	runErr := copyAndUpload(ctx, p, project, instance, copyName, h.target.upload(key))
	cleanUp(logger,
		func(ctx context.Context) error { return deleteInstanceIfThere(ctx, p, project, copyName) },
		func(ctx context.Context) error { return p.DeleteInstanceSnapshot(ctx, project, instance, snapshotName) },
	)
	if runErr != nil {
		return runErr
	}
	if err := confirmArrived(ctx, h.objects, h.arrival, key); err != nil {
		return err
	}
	logger.Info("backup uploaded", "object", key)
	return nil
}

func copyAndUpload(ctx context.Context, p provider.Provider, project string, instance string, copyName string,
	upload api.BackupTarget) error {
	snapshot, err := p.GetInstanceSnapshot(ctx, project, instance, snapshotName)
	if err != nil {
		return err
	}
	copyRequest := api.InstancesPost{
		InstancePut: api.InstancePut{Devices: volumesMasked(snapshot.Devices)},
		Name:        copyName,
		Type:        api.InstanceTypeVM,
		Source:      api.InstanceSource{Type: "copy", Source: provider.SnapshotId(instance, snapshotName)},
	}
	if err := p.CreateInstance(ctx, project, copyRequest); err != nil {
		return err
	}
	return p.BackupInstance(ctx, project, copyName, upload)
}

// volumesMasked names each custom volume the snapshot has attached, as a device of type none: a copy takes the
// source's devices except those its request names, so the copy holds no volume (a volume has its own backup). The
// archive then declares the volume as type none under its name; a restore attaches it again
// (https://github.com/OpenViCloud/titan-ops/blob/main/docs/features/backup/design.md#incus-backup).
func volumesMasked(devices map[string]map[string]string) map[string]map[string]string {
	masked := map[string]map[string]string{}
	for name, settings := range devices {
		isVolume := settings["type"] == "disk" && settings["source"] != ""
		if isVolume {
			masked[name] = map[string]string{"type": "none"}
		}
	}
	return masked
}

// GiveUp removes what a run left; the command has no callback.
func (h *Vm) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	payload, err := decodeVm(leased)
	if err != nil {
		return nil
	}
	copyName, err := provider.BackupInstanceCopyName(payload.VmId)
	if err != nil {
		return nil
	}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return nil
	}
	if err := removeVmLeftovers(ctx, cl.Provider, payload.ProjectName, payload.ExternalVmId, copyName); err != nil {
		logger.Warn("removing the backup's leftovers failed; the next run removes them", "error", err)
	}
	return nil
}

func removeVmLeftovers(ctx context.Context, p provider.Provider, project string, instance string, copyName string) error {
	if err := deleteInstanceIfThere(ctx, p, project, copyName); err != nil {
		return err
	}
	_, err := p.GetInstanceSnapshot(ctx, project, instance, snapshotName)
	if provider.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return p.DeleteInstanceSnapshot(ctx, project, instance, snapshotName)
}

func deleteInstanceIfThere(ctx context.Context, p provider.Provider, project string, name string) error {
	_, err := p.GetInstance(ctx, project, name)
	if provider.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return p.DeleteInstance(ctx, project, name)
}

func decodeVm(leased command.Leased) (vmPayload, error) {
	var payload vmPayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return vmPayload{}, err
	}
	isComplete := payload.VmId != "" && payload.ExternalVmId != "" && payload.ProjectName != "" && payload.TenantId != ""
	if !isComplete {
		return vmPayload{}, command.Permanent(fmt.Errorf("vm.backup payload lacks vmId, externalVmId, projectName or tenantId"))
	}
	return payload, nil
}
