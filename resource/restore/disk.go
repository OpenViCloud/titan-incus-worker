package restore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"time"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/resource/backup"
)

// restoreVolumes imports every volume Titan still lists, from the newest archive at or before atOrBefore, rolls it
// back to the backup snapshot and removes it, then deletes any further snapshot Titan no longer lists. Volumes run
// before VMs: a VM archive's devices name its attached volumes by their Incus name, so the volume must already exist
// (titan-ops features/backup/design.md#cluster-restore).
func restoreVolumes(ctx context.Context, p provider.Provider, pool string, rows []titanVolume, snapshotRows []titanVolumeSnapshot,
	archives map[string][]archiveKey, atOrBefore time.Time, objects Objects, logger *slog.Logger, summary *Summary) {
	liveSnapshots := liveVolumeSnapshotNamesByVolume(snapshotRows)
	for _, row := range rows {
		if row.ExternalId == "" {
			continue
		}
		chosen, found := pickArchive(archives["volumes/"+row.Id], atOrBefore)
		if !found {
			logger.Warn("volume has no archive at or before the restore point; skipped", "volumeId", row.Id, "project", row.ProjectName)
			summary.Skipped++
			continue
		}
		if err := ImportVolume(ctx, p, pool, row.ProjectName, row.ExternalId, chosen.Key, objects, logger); err != nil {
			logger.Warn("restore volume failed", "volumeId", row.Id, "project", row.ProjectName, "volume", row.ExternalId,
				"object", chosen.Key, "error", err)
			summary.Failed++
			continue
		}
		live := liveSnapshots[provider.ProjectKey(row.ProjectName, row.ExternalId)]
		pruneVolumeSnapshots(ctx, p, pool, row.ProjectName, row.ExternalId, live, logger)
		logger.Info("volume restored", "volumeId", row.Id, "project", row.ProjectName, "volume", row.ExternalId, "object", chosen.Key)
		summary.Restored++
	}
}

// ErrOldFormat is in the error of an import whose archive carries no backup snapshot: an archive from before backups
// took one cannot be rolled back to the moment it was taken, and no retry changes that.
var ErrOldFormat = errors.New("an old-format archive, not usable")

func oldFormat(key string) error {
	return fmt.Errorf("archive %s has no %q snapshot: %w", key, backup.SnapshotName, ErrOldFormat)
}

// ImportVolume streams the archive at key into Incus, checks it carries the backup snapshot (an archive without one is
// an old-format archive this restore cannot use), then rolls the volume back to it and removes it. A restore drill runs
// the same steps in its temporary project (titan-ops features/backup/design.md#restore-drill), and volume.restore under
// a new name, so each error names its step.
func ImportVolume(ctx context.Context, p provider.Provider, pool string, project string, name string, key string,
	objects Objects, logger *slog.Logger) error {
	body, err := objects.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("read archive %s: %w", key, err)
	}
	defer body.Close()
	if err := p.ImportVolume(ctx, project, pool, name, body); err != nil {
		return fmt.Errorf("import volume %s: %w", name, err)
	}
	rolledBack, err := RollBackVolume(ctx, p, pool, project, name)
	if err != nil {
		return err
	}
	if !rolledBack {
		if deleteErr := p.DeleteVolume(ctx, project, pool, name); deleteErr != nil {
			logger.Warn("removing an old-format archive's import failed; the next restore finds it there",
				"project", project, "volume", name, "error", deleteErr)
		}
		return oldFormat(key)
	}
	return nil
}

// RollBackVolume rolls an imported volume back to the backup snapshot and removes that snapshot. It answers false,
// having written nothing, when the volume has no such snapshot: an old-format archive when the import was this run's,
// a rollback an earlier run finished when it was not.
func RollBackVolume(ctx context.Context, p provider.Provider, pool string, project string, name string) (bool, error) {
	has, err := hasVolumeSnapshot(ctx, p, pool, project, name, backup.SnapshotName)
	if err != nil {
		return false, fmt.Errorf("look for the %q snapshot of volume %s: %w", backup.SnapshotName, name, err)
	}
	if !has {
		return false, nil
	}
	if err := p.RestoreVolume(ctx, project, pool, name, backup.SnapshotName); err != nil {
		return false, fmt.Errorf("roll volume %s back to %q: %w", name, backup.SnapshotName, err)
	}
	if err := p.DeleteVolumeSnapshot(ctx, project, pool, name, backup.SnapshotName); err != nil {
		return false, fmt.Errorf("delete the %q snapshot of volume %s: %w", backup.SnapshotName, name, err)
	}
	return true, nil
}

func hasVolumeSnapshot(ctx context.Context, p provider.Provider, pool string, project string, volume string, snapshot string) (bool, error) {
	snapshots, err := p.ListVolumeSnapshots(ctx, project, pool, volume)
	if err != nil {
		return false, err
	}
	for _, found := range snapshots {
		_, bare, err := provider.SplitSnapshotId(found.Name)
		if err != nil {
			bare = found.Name
		}
		if bare == snapshot {
			return true, nil
		}
	}
	return false, nil
}

// pruneVolumeSnapshots removes every snapshot Titan no longer lists; the backup snapshot is already gone by the time
// this runs. A failed read or delete is logged and left for a later run: it is not this archive's import that failed.
func pruneVolumeSnapshots(ctx context.Context, p provider.Provider, pool string, project string, volume string, live map[string]bool, logger *slog.Logger) {
	snapshots, err := p.ListVolumeSnapshots(ctx, project, pool, volume)
	if err != nil {
		logger.Warn("could not list volume snapshots to prune", "project", project, "volume", volume, "error", err)
		return
	}
	for _, found := range snapshots {
		_, bare, err := provider.SplitSnapshotId(found.Name)
		if err != nil {
			bare = found.Name
		}
		if bare == backup.SnapshotName || live[bare] {
			continue
		}
		if err := p.DeleteVolumeSnapshot(ctx, project, pool, volume, bare); err != nil {
			logger.Warn("deleting a volume snapshot titan no longer lists failed", "project", project, "volume", volume,
				"snapshot", bare, "error", err)
			continue
		}
		logger.Info("volume snapshot deleted: titan no longer lists it", "project", project, "volume", volume, "snapshot", bare)
	}
}

// restoreVms imports every VM Titan still lists, rolls it back to the backup snapshot, removes extra snapshots, pins
// its private IPv4 when one was recorded, and starts it only when Titan last recorded it Running or Ready.
func restoreVms(ctx context.Context, p provider.Provider, pool string, rows []titanVm, snapshotRows []titanVmSnapshot,
	archives map[string][]archiveKey, atOrBefore time.Time, addresses map[string]string, objects Objects, logger *slog.Logger, summary *Summary) {
	liveSnapshots := liveVmSnapshotNamesByInstance(snapshotRows)
	for _, row := range rows {
		if row.ExternalId == "" {
			continue
		}
		chosen, found := pickArchive(archives["vms/"+row.Id], atOrBefore)
		if !found {
			logger.Warn("vm has no archive at or before the restore point; skipped", "vmId", row.Id, "project", row.ProjectName)
			summary.Skipped++
			continue
		}
		if err := ImportVm(ctx, p, pool, row.ProjectName, row.ExternalId, chosen.Key, objects, logger); err != nil {
			logger.Warn("restore vm failed", "vmId", row.Id, "project", row.ProjectName, "vm", row.ExternalId,
				"object", chosen.Key, "error", err)
			summary.Failed++
			continue
		}
		if address, recorded := addresses[row.ExternalId]; recorded {
			pinIpv4(ctx, p, row.ProjectName, row.ExternalId, address, logger)
		}
		live := liveSnapshots[provider.ProjectKey(row.ProjectName, row.ExternalId)]
		pruneVmSnapshots(ctx, p, row.ProjectName, row.ExternalId, live, logger)
		if shouldStart(row.CurrentState) {
			state := api.InstanceStatePut{Action: "start"}
			if err := p.UpdateInstanceState(ctx, row.ProjectName, row.ExternalId, state); err != nil {
				logger.Warn("starting the restored vm failed; it stays stopped", "vmId", row.Id, "vm", row.ExternalId, "error", err)
			} else {
				logger.Info("vm started", "vmId", row.Id, "project", row.ProjectName, "vm", row.ExternalId)
			}
		}
		logger.Info("vm restored", "vmId", row.Id, "project", row.ProjectName, "vm", row.ExternalId, "object", chosen.Key)
		summary.Restored++
	}
}

func shouldStart(currentState *string) bool {
	if currentState == nil {
		return false
	}
	return *currentState == "Running" || *currentState == "Ready"
}

// ImportVm is ImportVolume's steps for a VM: import, the backup snapshot there, a full rollback to it, the snapshot
// removed. Full, not disk-only: the backup snapshot is the point in time Incus promises, config included, unlike
// vm-snapshot.revert's disk-only semantics for a tenant's own request (resource/vmsnapshot/revert.go).
func ImportVm(ctx context.Context, p provider.Provider, pool string, project string, name string, key string,
	objects Objects, logger *slog.Logger) error {
	return importVm(ctx, p, pool, project, name, key, provider.InstanceOverrides{}, false, objects, logger)
}

// ImportVmWithOverrides is ImportVm for a copy that lives beside the VM its archive was taken of: Incus applies the
// overrides as it imports, and the rollback is disk-only, because a full one puts the archived devices and volatile
// keys back and the copy would again name the original's volumes, address and port (measured on Incus 7.0.2,
// titan-ops features/backup/design.md#restore-as-new-resource).
func ImportVmWithOverrides(ctx context.Context, p provider.Provider, pool string, project string, name string, key string,
	overrides provider.InstanceOverrides, objects Objects, logger *slog.Logger) error {
	return importVm(ctx, p, pool, project, name, key, overrides, true, objects, logger)
}

func importVm(ctx context.Context, p provider.Provider, pool string, project string, name string, key string,
	overrides provider.InstanceOverrides, diskOnly bool, objects Objects, logger *slog.Logger) error {
	body, err := objects.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("read archive %s: %w", key, err)
	}
	defer body.Close()
	if err := p.ImportInstance(ctx, project, pool, name, body, overrides); err != nil {
		return fmt.Errorf("import vm %s: %w", name, err)
	}
	rolledBack, err := rollBackVm(ctx, p, project, name, diskOnly)
	if err != nil {
		return err
	}
	if !rolledBack {
		if deleteErr := p.DeleteInstance(ctx, project, name); deleteErr != nil {
			logger.Warn("removing an old-format archive's import failed; the next restore finds it there",
				"project", project, "vm", name, "error", deleteErr)
		}
		return oldFormat(key)
	}
	return nil
}

// RollBackVmDiskOnly is RollBackVolume for a VM imported with overrides: the disk goes back to the backup snapshot,
// the devices and config stay the import's.
func RollBackVmDiskOnly(ctx context.Context, p provider.Provider, project string, name string) (bool, error) {
	return rollBackVm(ctx, p, project, name, true)
}

func rollBackVm(ctx context.Context, p provider.Provider, project string, name string, diskOnly bool) (bool, error) {
	_, err := p.GetInstanceSnapshot(ctx, project, name, backup.SnapshotName)
	if provider.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("look for the %q snapshot of vm %s: %w", backup.SnapshotName, name, err)
	}
	rollback := api.InstancePut{Restore: backup.SnapshotName, DiskOnly: diskOnly}
	if err := p.UpdateInstance(ctx, project, name, rollback, ""); err != nil {
		return false, fmt.Errorf("roll vm %s back to %q: %w", name, backup.SnapshotName, err)
	}
	if err := p.DeleteInstanceSnapshot(ctx, project, name, backup.SnapshotName); err != nil {
		return false, fmt.Errorf("delete the %q snapshot of vm %s: %w", backup.SnapshotName, name, err)
	}
	return true, nil
}

// pinIpv4 sets the VM's NIC (eth0) to the IPv4 OVN's DHCP gave it when the backup was taken. OVN otherwise hands out
// a fresh dynamic address the moment an instance is created or imported, so importing in a different order would
// leave every VM with a different address than before; setting it statically is accepted even while another
// instance's port still holds it dynamically, OVN moves that one instead (measured on dev, Incus 7.0.2, OVN,
// titan-ops features/backup/design.md#cluster-restore). Incus's PATCH replaces a whole device, so the NIC is read
// first and sent back with the address added. A failed pin is logged and left: the VM still restores, with whatever
// address OVN gives it.
func pinIpv4(ctx context.Context, p provider.Provider, project string, instance string, address string, logger *slog.Logger) {
	current, err := p.GetInstance(ctx, project, instance)
	if err != nil {
		logger.Warn("pinning the vm's private ipv4 failed: reading the vm", "project", project, "vm", instance, "address", address, "error", err)
		return
	}
	nic, found := current.Devices["eth0"]
	if !found {
		logger.Warn("pinning the vm's private ipv4 skipped: the vm has no eth0 of its own", "project", project, "vm", instance, "address", address)
		return
	}
	pinned := maps.Clone(nic)
	pinned["ipv4.address"] = address
	patch := api.InstancePut{Devices: map[string]map[string]string{"eth0": pinned}}
	if err := p.PatchInstance(ctx, project, instance, patch); err != nil {
		logger.Warn("pinning the vm's private ipv4 failed", "project", project, "vm", instance, "address", address, "error", err)
		return
	}
	logger.Info("vm's private ipv4 pinned", "project", project, "vm", instance, "address", address)
}

func pruneVmSnapshots(ctx context.Context, p provider.Provider, project string, instance string, live map[string]bool, logger *slog.Logger) {
	snapshots, err := p.ListInstanceSnapshots(ctx, project, instance)
	if err != nil {
		logger.Warn("could not list vm snapshots to prune", "project", project, "vm", instance, "error", err)
		return
	}
	for _, found := range snapshots {
		if found.Name == backup.SnapshotName || live[found.Name] {
			continue
		}
		if err := p.DeleteInstanceSnapshot(ctx, project, instance, found.Name); err != nil {
			logger.Warn("deleting a vm snapshot titan no longer lists failed", "project", project, "vm", instance,
				"snapshot", found.Name, "error", err)
			continue
		}
		logger.Info("vm snapshot deleted: titan no longer lists it", "project", project, "vm", instance, "snapshot", found.Name)
	}
}
