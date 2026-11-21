package backup

import (
	"context"
	"fmt"
	"log/slog"
	"time"

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

// Vm runs vm.backup: Incus uploads an archive of the VM itself with every snapshot it has, taken right after a snapshot
// named backup. On an LVM pool Incus reads the running disk as it is being written; the restore rolls back to that
// snapshot, so what comes back is one point in time, as after a power cut. Attached volumes are not in it; each has its
// own volume.backup. Every other command of the VM waits while this one runs, so no snapshot is taken or deleted halfway
// through the export (titan-ops features/backup/design.md#incus-backup). The command is done when the archive is in the
// bucket; there is no callback.
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
	if err := removeVmLeftovers(ctx, p, project, instance); err != nil {
		return err
	}

	if err := p.CreateInstanceSnapshot(ctx, project, instance, SnapshotName); err != nil {
		return err
	}
	key := ObjectKey(leased.ClusterId, payload.TenantId, "vms", payload.VmId, h.now())
	runErr := p.BackupInstance(ctx, project, instance, h.target.upload(key))
	cleanUp(logger,
		func(ctx context.Context) error { return p.DeleteInstanceSnapshot(ctx, project, instance, SnapshotName) },
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

// GiveUp removes what a run left; the command has no callback.
func (h *Vm) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	payload, err := decodeVm(leased)
	if err != nil {
		return nil
	}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return nil
	}
	if err := removeVmLeftovers(ctx, cl.Provider, payload.ProjectName, payload.ExternalVmId); err != nil {
		logger.Warn("removing the backup's leftovers failed; the next run removes them", "error", err)
	}
	return nil
}

// removeVmLeftovers removes the backup snapshot a run that died left.
func removeVmLeftovers(ctx context.Context, p provider.Provider, project string, instance string) error {
	_, err := p.GetInstanceSnapshot(ctx, project, instance, SnapshotName)
	if provider.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return p.DeleteInstanceSnapshot(ctx, project, instance, SnapshotName)
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
