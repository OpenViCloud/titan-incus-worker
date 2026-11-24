package volume

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/resource/backup"
	"github.com/OpenViCloud/titan-incus-worker/resource/restore"
)

const reasonRestoreFailed = "Restoring the volume from its backup failed due to a system error. Please contact support."

// restorePayload mirrors the volume.restore payload of the control plane's ProviderCommandQueue.
type restorePayload struct {
	// NewVolumeId is the copy's; RequestId is its create request (titan-ops architecture/resource-lifecycle.md#kinds).
	NewVolumeId string `json:"newVolumeId"`
	RequestId   string `json:"requestId"`
	// Name is what the admin called the copy.
	Name           string `json:"name"`
	SourceVolumeId string `json:"sourceVolumeId"`
	// BackupKey is the moment the archive was taken, as its object key spells it (backup.TimeLayout).
	BackupKey   string `json:"backupKey"`
	TenantId    string `json:"tenantId"`
	ProjectName string `json:"projectName"`
	SizeGb      int    `json:"sizeGb"`
}

// Restore runs volume.restore: one archive of a volume imported as a volume of its own, in the tenant's project,
// beside the volume it was taken of, which it never writes (titan-ops features/backup/design.md#restore-as-new-resource).
// It ends the copy's create: Done with its provider id, Failed when it gives up: without the id once nothing of it is
// left on Incus, with it while the copy may still be there. Its last
// write of the copy leaves the command's request on it (restore.FinishedKey); from then on the volume is the
// tenant's, and neither a run nor a give-up that comes after writes it.
type Restore struct {
	cp       *controlplane.Client
	clusters cluster.Clusters
	objects  restore.Objects
}

func NewRestore(cp *controlplane.Client, clusters cluster.Clusters, objects restore.Objects) *Restore {
	return &Restore{cp: cp, clusters: clusters, objects: objects}
}

// restoreOrder is a payload read and checked: the copy's name on Incus and the archive's key in the bucket.
type restoreOrder struct {
	payload restorePayload
	name    string
	key     string
}

func (h *Restore) Run(ctx context.Context, leased command.Leased, logger *slog.Logger) error {
	order, err := decodeRestore(leased)
	if err != nil {
		return err
	}
	cl, err := h.clusters.Cluster(ctx, leased.ClusterId)
	if err != nil {
		return err
	}
	p, pool := cl.Provider, provider.StoragePoolOrDefault(cl.StoragePool)
	project, name := order.payload.ProjectName, order.name

	found, err := p.GetVolume(ctx, project, pool, name)
	isImported := err == nil
	if err != nil && !provider.IsNotFound(err) {
		return fmt.Errorf("look for volume %s: %w", name, err)
	}
	if isImported && restore.IsFinishedBy(found.Config, order.payload.RequestId) {
		// The report of the run that finished it was lost, or the loop's own end of the command was. The volume is the
		// tenant's from that write on: only the report is made again.
		logger.Info("already restored; reporting it again", "object", order.key, "volume", name)
		return EndRequest(ctx, h.cp, order.payload.NewVolumeId, order.payload.RequestId, Done(name, nil))
	}
	// An import an earlier run stopped waiting on may still be at work: what is under the name is then neither imported
	// nor missing (restore.ImportStillRunning).
	running, err := p.ListRunningVolumeOperations(ctx, project, pool, name)
	if err != nil {
		return fmt.Errorf("look for an operation still running on volume %s: %w", name, err)
	}
	if len(running) > 0 {
		return restore.ImportStillRunning(name, running[0])
	}
	if isImported {
		// An earlier run imported it; whether it also rolled it back is what the "backup" snapshot still there says.
		logger.Info("already imported; taking it up from there", "object", order.key, "volume", name)
		if _, err := restore.RollBackVolume(ctx, p, pool, project, name); err != nil {
			return err
		}
	} else {
		if err := checkRoom(ctx, p, pool, order.payload.SizeGb); err != nil {
			return err
		}
		logger.Info("volume restore started", "object", order.key, "source", order.payload.SourceVolumeId, "volume", name)
		err := restore.ImportVolume(ctx, p, pool, project, name, order.key, h.objects, logger)
		if errors.Is(err, restore.ErrOldFormat) {
			return command.Permanent(err)
		}
		if err != nil {
			return err
		}
	}

	// The archive carries the snapshots the tenant took of the original; Titan has no row for them on the copy.
	if err := deleteSnapshots(ctx, p, pool, project, name); err != nil {
		return err
	}
	// The import keeps the original's name. The same write, the last one of the copy, says this command finished it:
	// a run after it must leave the volume to the tenant.
	finished := map[string]string{DisplayNameKey: order.payload.Name, restore.FinishedKey: order.payload.RequestId}
	if err := p.PatchVolumeConfig(ctx, project, pool, name, finished); err != nil {
		return fmt.Errorf("name volume %s: %w", name, err)
	}
	logger.Info("volume restored", "object", order.key, "source", order.payload.SourceVolumeId, "volume", name)
	// No observation: a copy that goes with a VM is reported before that VM is on Incus, and the reconciler says where
	// the volume is attached once it is.
	return EndRequest(ctx, h.cp, order.payload.NewVolumeId, order.payload.RequestId, Done(name, nil))
}

// GiveUp removes what the command imported and ends the create Failed. The provider id goes with it only while the
// volume is, or may be, still on Incus: Titan's delete of the failed volume then reaches it. A copy the command
// finished is not removed, and its create ends Done: the restore did finish, whatever was given up on after it.
func (h *Restore) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	order, refusal := decodeRestore(leased)
	if refusal != nil {
		return h.endRefused(ctx, leased, cause, logger)
	}
	end, err := h.removeImport(ctx, leased.ClusterId, order, cause, logger)
	if err != nil {
		return err
	}
	return EndRequest(ctx, h.cp, order.payload.NewVolumeId, order.payload.RequestId, end)
}

// endRefused ends the create of a payload Run refuses. Run made nothing on Incus for it, and what is under the name it
// gives is not this command's to remove: a copy's id that is the original's names the original. No call is made on
// Incus.
func (h *Restore) endRefused(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	var payload restorePayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return err
	}
	if payload.RequestId == "" {
		logger.Warn("no request id; nothing to end")
		return nil
	}
	if payload.NewVolumeId == "" {
		return command.Permanent(fmt.Errorf("volume.restore payload lacks newVolumeId: nobody to tell"))
	}
	return EndRequest(ctx, h.cp, payload.NewVolumeId, payload.RequestId, Failed("", nil, reasonRestoreFailed, cause))
}

// removeImport deletes the volume under the copy's name, with its snapshots (Incus removes those with the volume:
// DeleteCustomVolume in internal/server/storage/backend.go of v7.0.1), and answers the report that ends the create:
// Failed, with the name while the volume is or may be there and without it once it is known gone. A copy this command
// finished is the tenant's volume: it is left as it is, and the report is the Done of the run that finished it. While
// Incus is still at work on the copy there is no report: the error has the give-up come again
// (restore.ImportStillRunning).
func (h *Restore) removeImport(ctx context.Context, clusterId string, order restoreOrder, cause error, logger *slog.Logger) (RequestEnd, error) {
	project, name := order.payload.ProjectName, order.name
	cl, err := h.clusters.Cluster(ctx, clusterId)
	if err != nil {
		logger.Warn("the cluster of a failed restore is not reachable; its volume may be left", "volume", name, "error", err)
		return Failed(name, nil, reasonRestoreFailed, cause), nil
	}
	p, pool := cl.Provider, provider.StoragePoolOrDefault(cl.StoragePool)
	found, err := p.GetVolume(ctx, project, pool, name)
	isGone := provider.IsNotFound(err)
	if err != nil && !isGone {
		logger.Warn("the volume of a failed restore could not be read; it may be left", "volume", name, "error", err)
		return Failed(name, nil, reasonRestoreFailed, cause), nil
	}
	if !isGone && restore.IsFinishedBy(found.Config, order.payload.RequestId) {
		logger.Info("the restore had finished; its volume is kept", "volume", name)
		return Done(name, nil), nil
	}
	running, err := p.ListRunningVolumeOperations(ctx, project, pool, name)
	if err != nil {
		logger.Warn("the operations on the volume of a failed restore could not be listed; it may be left", "volume", name, "error", err)
		return Failed(name, nil, reasonRestoreFailed, cause), nil
	}
	if len(running) > 0 {
		return RequestEnd{}, restore.ImportStillRunning(name, running[0])
	}
	if isGone {
		return Failed("", nil, reasonRestoreFailed, cause), nil
	}
	err = p.DeleteVolume(ctx, project, pool, name)
	if err != nil && !provider.IsNotFound(err) {
		logger.Warn("removing the volume of a failed restore failed; it is left", "volume", name, "error", err)
		return Failed(name, nil, reasonRestoreFailed, cause), nil
	}
	logger.Info("volume of a failed restore removed", "volume", name)
	return Failed("", nil, reasonRestoreFailed, cause), nil
}

func decodeRestore(leased command.Leased) (restoreOrder, error) {
	var payload restorePayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return restoreOrder{}, err
	}
	isComplete := payload.NewVolumeId != "" && payload.RequestId != "" && payload.SourceVolumeId != "" &&
		payload.BackupKey != "" && payload.TenantId != "" && payload.ProjectName != "" && payload.SizeGb >= 0
	if !isComplete {
		return restoreOrder{}, command.Permanent(errors.New(
			"volume.restore payload lacks newVolumeId, requestId, sourceVolumeId, backupKey, tenantId or projectName, or its sizeGb is below zero"))
	}
	name, err := provider.VolumeName(payload.NewVolumeId)
	if err != nil {
		return restoreOrder{}, command.Permanent(err)
	}
	source, err := provider.VolumeName(payload.SourceVolumeId)
	if err != nil {
		return restoreOrder{}, command.Permanent(err)
	}
	// Whatever is under the copy's name is taken for this command's own import, and written.
	if name == source {
		return restoreOrder{}, command.Permanent(fmt.Errorf("volume.restore payload restores volume %s onto itself", payload.SourceVolumeId))
	}
	// The command carries the moment the archive was taken, never its key: the worker builds the key as the backup did,
	// so a command reaches only an archive of the cluster, tenant and volume it names.
	takenAt, err := time.Parse(backup.TimeLayout, payload.BackupKey)
	if err != nil {
		return restoreOrder{}, command.Permanent(fmt.Errorf("volume.restore payload: backupKey %q is not a moment written as %s",
			payload.BackupKey, backup.TimeLayout))
	}
	key := backup.ObjectKey(leased.ClusterId, payload.TenantId, "volumes", payload.SourceVolumeId, takenAt)
	return restoreOrder{payload: payload, name: name, key: key}, nil
}

// checkRoom is permanent only for a pool that was read and is too full; a pool that could not be read is asked again
// by the next lease.
func checkRoom(ctx context.Context, p provider.Provider, pool string, sizeGb int) error {
	err := restore.CheckPoolRoom(ctx, p, pool, int64(sizeGb)<<30)
	if errors.Is(err, restore.ErrNoRoom) {
		return command.Permanent(err)
	}
	return err
}

// deleteSnapshots removes every snapshot the volume has. One already gone was removed by another run of the command.
func deleteSnapshots(ctx context.Context, p provider.Provider, pool string, project string, volume string) error {
	snapshots, err := p.ListVolumeSnapshots(ctx, project, pool, volume)
	if err != nil {
		return fmt.Errorf("list the snapshots of volume %s: %w", volume, err)
	}
	for _, found := range snapshots {
		// Incus lists a volume's snapshot as "<volume>/<snapshot>" (lab-storage 03).
		snapshot := strings.TrimPrefix(found.Name, volume+"/")
		err := p.DeleteVolumeSnapshot(ctx, project, pool, volume, snapshot)
		if err != nil && !provider.IsNotFound(err) {
			return fmt.Errorf("delete the %q snapshot of volume %s: %w", snapshot, volume, err)
		}
	}
	return nil
}
