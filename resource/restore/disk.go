package restore

import (
	"context"
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

// ImportVolume streams the archive at key into Incus, checks it carries the backup snapshot (an archive without one is
// an old-format archive this restore cannot use), then rolls the volume back to it and removes it. A restore drill runs
// the same steps in its temporary project (titan-ops features/backup/design.md#restore-drill), so each error names its
// step.
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
	has, err := hasVolumeSnapshot(ctx, p, pool, project, name, backup.SnapshotName)
	if err != nil {
		return fmt.Errorf("look for the %q snapshot of volume %s: %w", backup.SnapshotName, name, err)
	}
	if !has {
		if deleteErr := p.DeleteVolume(ctx, project, pool, name); deleteErr != nil {
			logger.Warn("removing an old-format archive's import failed; the next restore finds it there",
				"project", project, "volume", name, "error", deleteErr)
		}
		return fmt.Errorf("archive %s has no %q snapshot: an old-format archive, not usable", key, backup.SnapshotName)
	}
	if err := p.RestoreVolume(ctx, project, pool, name, backup.SnapshotName); err != nil {
		return fmt.Errorf("roll volume %s back to %q: %w", name, backup.SnapshotName, err)
	}
	if err := p.DeleteVolumeSnapshot(ctx, project, pool, name, backup.SnapshotName); err != nil {
		return fmt.Errorf("delete the %q snapshot of volume %s: %w", backup.SnapshotName, name, err)
	}
	return nil
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
// removed.
func ImportVm(ctx context.Context, p provider.Provider, pool string, project string, name string, key string,
	objects Objects, logger *slog.Logger) error {
	body, err := objects.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("read archive %s: %w", key, err)
	}
	defer body.Close()
	if err := p.ImportInstance(ctx, project, pool, name, body); err != nil {
		return fmt.Errorf("import vm %s: %w", name, err)
	}
	_, err = p.GetInstanceSnapshot(ctx, project, name, backup.SnapshotName)
	if provider.IsNotFound(err) {
		if deleteErr := p.DeleteInstance(ctx, project, name); deleteErr != nil {
			logger.Warn("removing an old-format archive's import failed; the next restore finds it there",
				"project", project, "vm", name, "error", deleteErr)
		}
		return fmt.Errorf("archive %s has no %q snapshot: an old-format archive, not usable", key, backup.SnapshotName)
	}
	if err != nil {
		return fmt.Errorf("look for the %q snapshot of vm %s: %w", backup.SnapshotName, name, err)
	}
	// Full restore, not disk-only: the backup snapshot is the point in time Incus promises, config included, unlike
	// vm-snapshot.revert's disk-only semantics for a tenant's own request (resource/vmsnapshot/revert.go).
	if err := p.UpdateInstance(ctx, project, name, api.InstancePut{Restore: backup.SnapshotName}, ""); err != nil {
		return fmt.Errorf("roll vm %s back to %q: %w", name, backup.SnapshotName, err)
	}
	if err := p.DeleteInstanceSnapshot(ctx, project, name, backup.SnapshotName); err != nil {
		return fmt.Errorf("delete the %q snapshot of vm %s: %w", backup.SnapshotName, name, err)
	}
	return nil
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
