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
	zones   cluster.Zones
	target  Target
	objects Objects
	arrival Arrival
	now     func() time.Time
}

func NewVm(zones cluster.Zones, target Target, objects Objects) *Vm {
	return &Vm{zones: zones, target: target, objects: objects, arrival: DefaultArrival, now: time.Now}
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
	zone, err := h.zones.Zone(ctx, leased.ZoneId)
	if err != nil {
		return err
	}
	p := zone.Provider
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
	key := ObjectKey(leased.ZoneId, payload.TenantId, "vms", payload.VmId, h.now())
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
	copyRequest := api.InstancesPost{
		Name:   copyName,
		Type:   api.InstanceTypeVM,
		Source: api.InstanceSource{Type: "copy", Source: provider.SnapshotId(instance, snapshotName)},
	}
	if err := p.CreateInstance(ctx, project, copyRequest); err != nil {
		return err
	}
	return p.BackupInstance(ctx, project, copyName, upload)
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
	zone, err := h.zones.Zone(ctx, leased.ZoneId)
	if err != nil {
		return nil
	}
	if err := removeVmLeftovers(ctx, zone.Provider, payload.ProjectName, payload.ExternalVmId, copyName); err != nil {
		logger.Warn("removing the backup's leftovers failed; the next run removes them", "error", err)
	}
	return nil
}

func removeVmLeftovers(ctx context.Context, p provider.Provider, project string, instance string, copyName string) error {
	if err := deleteInstanceIfThere(ctx, p, project, copyName); err != nil {
		return err
	}
	err := p.GetInstanceSnapshot(ctx, project, instance, snapshotName)
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
