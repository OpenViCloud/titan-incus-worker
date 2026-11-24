package vm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"time"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/resource/backup"
	"github.com/OpenViCloud/titan-incus-worker/resource/restore"
)

const reasonRestoreFailed = "Restoring the VM from its backup failed due to a system error. Please contact support."

// restorePayload mirrors the vm.restore payload of the control plane's ProviderCommandQueue.
type restorePayload struct {
	// VmId is the copy's; RequestId is its create request (titan-ops architecture/resource-lifecycle.md#kinds).
	VmId      string `json:"vmId"`
	RequestId string `json:"requestId"`
	// Name is what the admin called the copy.
	Name       string `json:"name"`
	SourceVmId string `json:"sourceVmId"`
	// BackupKey is the moment the archive was taken, as its object key spells it (backup.TimeLayout).
	BackupKey   string `json:"backupKey"`
	TenantId    string `json:"tenantId"`
	ProjectName string `json:"projectName"`
	DiskSizeGb  int    `json:"diskSizeGb"`
	// Volumes are the volumes restored with the VM: each has a volume.restore of its own, which makes the copy.
	Volumes []restoredVolume `json:"volumes"`
}

type restoredVolume struct {
	SourceVolumeId string `json:"sourceVolumeId"`
	NewVolumeId    string `json:"newVolumeId"`
	// RequestId is the request the copy's own volume.restore ends: that command leaves it on the copy once it has
	// finished it (restore.FinishedKey).
	RequestId string `json:"requestId"`
}

// Restore runs vm.restore: one archive of a VM imported as a VM of its own, in the tenant's project, beside the VM
// it was taken of, which may be running and which no step writes
// (titan-ops features/backup/design.md#restore-as-new-resource). The copy is left stopped. It ends the copy's create:
// Done with its provider id, Failed when it gives up: without the id once nothing of it is left on Incus, with it while
// the copy may still be there, so that Titan's delete still reaches it. Its last write of the copy
// leaves the command's request on it (restore.FinishedKey); from then on the copy is the tenant's, and neither a run
// nor a give-up that comes after writes it.
type Restore struct {
	cp       *controlplane.Client
	clusters cluster.Clusters
	objects  restore.Objects
}

func NewRestore(cp *controlplane.Client, clusters cluster.Clusters, objects restore.Objects) *Restore {
	return &Restore{cp: cp, clusters: clusters, objects: objects}
}

// restoreOrder is a payload read and checked: the copy's name on Incus, the archive's key in the bucket, and the
// volumes that go with the VM.
type restoreOrder struct {
	payload restorePayload
	name    string
	key     string
	// copies are the volumes' copies, in the payload's order; copyOf finds the Incus name of one by the Incus name of
	// the volume it copies, which is what an archived disk device names as its source.
	copies []volumeCopy
	copyOf map[string]string
}

// volumeCopy is the copy of a volume restored with the VM: its Incus name, and the request its own volume.restore ends.
type volumeCopy struct {
	name      string
	requestId string
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

	found, err := p.GetInstance(ctx, project, name)
	isImported := err == nil
	if err != nil && !provider.IsNotFound(err) {
		return fmt.Errorf("look for vm %s: %w", name, err)
	}
	if isImported && restore.IsFinishedBy(found.Config, order.payload.RequestId) {
		// The report of the run that finished it was lost, or the loop's own end of the command was. The VM is the
		// tenant's from that write on; a placeholder is still this command's, and no device of the copy names one.
		logger.Info("already restored; reporting it again", "object", order.key, "vm", name)
		if err := removePlaceholders(ctx, p, pool, project, name); err != nil {
			return err
		}
		return sendRequestEnd(ctx, h.cp, order.payload.VmId, order.payload.RequestId, doneEnd(ctx, p, project, name, logger))
	}
	// An import an earlier run stopped waiting on may still be at work: what is under the name is then neither imported
	// nor missing (restore.ImportStillRunning).
	running, err := p.ListRunningInstanceOperations(ctx, project, name)
	if err != nil {
		return fmt.Errorf("look for an operation still running on vm %s: %w", name, err)
	}
	if len(running) > 0 {
		return restore.ImportStillRunning(name, running[0])
	}
	if isImported {
		// An earlier run imported it; whether it also rolled it back is what the "backup" snapshot still there says.
		logger.Info("already imported; taking it up from there", "object", order.key, "vm", name)
		if _, err := restore.RollBackVmDiskOnly(ctx, p, project, name); err != nil {
			return err
		}
	} else if err := h.importCopy(ctx, p, pool, order, logger); err != nil {
		return err
	}

	// The archive carries the snapshots the tenant took of the original; Titan has no row for them on the copy.
	if err := deleteSnapshots(ctx, p, project, name); err != nil {
		return err
	}
	if err := finishCopy(ctx, p, pool, project, name, order); err != nil {
		return err
	}
	if err := removePlaceholders(ctx, p, pool, project, name); err != nil {
		return err
	}
	logger.Info("vm restored", "object", order.key, "source", order.payload.SourceVmId, "vm", name)
	return sendRequestEnd(ctx, h.cp, order.payload.VmId, order.payload.RequestId, doneEnd(ctx, p, project, name, logger))
}

// doneEnd is the report of a copy that is finished, with what Incus shows of it: stopped as the restore leaves it, or
// whatever the tenant has made of it by a later run.
func doneEnd(ctx context.Context, p provider.Provider, project string, name string, logger *slog.Logger) requestEnd {
	return requestEnd{Outcome: controlplane.OutcomeDone, ExternalId: name, Observed: readObserved(ctx, p, project, name, logger)}
}

// importCopy imports the archive under the copy's name and rolls its disk back to the moment the backup was taken.
// Every check that needs no import comes before its first write.
func (h *Restore) importCopy(ctx context.Context, p provider.Provider, pool string, order restoreOrder, logger *slog.Logger) error {
	project, name := order.payload.ProjectName, order.name
	for _, copied := range order.copies {
		if err := checkVolumeRestored(ctx, p, pool, project, copied); err != nil {
			return err
		}
	}
	if err := checkRoom(ctx, p, pool, order.payload.DiskSizeGb); err != nil {
		return err
	}
	index, err := restore.ReadIndex(ctx, h.objects, order.key)
	if err != nil {
		return err
	}
	plan := planImport(index.Devices, name, order.copyOf)

	for _, placeholder := range plan.placeholders {
		if err := createPlaceholder(ctx, p, pool, project, placeholder); err != nil {
			return err
		}
	}
	logger.Info("vm restore started", "object", order.key, "source", order.payload.SourceVmId, "vm", name)
	err = restore.ImportVmWithOverrides(ctx, p, pool, project, name, order.key, plan.overrides, h.objects, logger)
	if errors.Is(err, restore.ErrOldFormat) {
		// The import is removed, or the give-up removes it; the placeholders made for it go too.
		if removeErr := removePlaceholders(ctx, p, pool, project, name); removeErr != nil {
			logger.Warn("removing the placeholders of an old-format archive's import failed; the give-up removes them", "vm", name, "error", removeErr)
		}
		return command.Permanent(err)
	}
	return err
}

// GiveUp removes what the command made and ends the create Failed. The provider id goes with it only while the copy
// is, or may be, still on Incus: Titan's delete of the failed VM then reaches it. The volumes' copies stay: each ended
// a request of its own. A copy the command finished is not removed, and its create ends Done: the restore did finish,
// whatever was given up on after it.
func (h *Restore) GiveUp(ctx context.Context, leased command.Leased, cause error, logger *slog.Logger) error {
	order, refusal := decodeRestore(leased)
	if refusal != nil {
		return h.endRefused(ctx, leased, cause, logger)
	}
	end, err := h.removeImport(ctx, leased.ClusterId, order, cause, logger)
	if err != nil {
		return err
	}
	return sendRequestEnd(ctx, h.cp, order.payload.VmId, order.payload.RequestId, end)
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
	if payload.VmId == "" {
		return command.Permanent(fmt.Errorf("vm.restore payload lacks vmId: nobody to tell"))
	}
	return sendRequestEnd(ctx, h.cp, payload.VmId, payload.RequestId, failedEnd("", reasonRestoreFailed, cause))
}

// removeImport deletes the copy and then its placeholders, and answers the report that ends the create: Failed, with
// the copy's name while the copy is or may be on Incus and without it once it is known gone. The worker never starts
// the copy, so it is deleted as it stands (a running one Incus refuses to delete); one that will not delete keeps its
// placeholders, which Incus refuses to delete under it. Nothing removes a placeholder left here once the command is
// over, so the line that says some are left names each. A copy this command finished is the tenant's VM, started or not: it is left as it is, only its
// placeholders go, and the report is the Done of the run that finished it. While Incus is still at work on the copy
// there is no report and no write: the error has the give-up come again (restore.ImportStillRunning).
func (h *Restore) removeImport(ctx context.Context, clusterId string, order restoreOrder, cause error, logger *slog.Logger) (requestEnd, error) {
	project, name := order.payload.ProjectName, order.name
	cl, err := h.clusters.Cluster(ctx, clusterId)
	if err != nil {
		logger.Warn("the cluster of a failed restore is not reachable; its vm may be left with its placeholders", "vm", name,
			"placeholders", "not read; named "+placeholderName("<n>", name), "error", err)
		return failedEnd(name, reasonRestoreFailed, cause), nil
	}
	p, pool := cl.Provider, provider.StoragePoolOrDefault(cl.StoragePool)
	found, err := p.GetInstance(ctx, project, name)
	isGone := provider.IsNotFound(err)
	if err != nil && !isGone {
		logger.Warn("the vm of a failed restore could not be read; it may be left with its placeholders", "vm", name,
			"placeholders", placeholdersLeft(ctx, p, pool, project, name), "error", err)
		return failedEnd(name, reasonRestoreFailed, cause), nil
	}
	isFinished := !isGone && restore.IsFinishedBy(found.Config, order.payload.RequestId)
	if !isFinished {
		running, err := p.ListRunningInstanceOperations(ctx, project, name)
		if err != nil {
			logger.Warn("the operations on the vm of a failed restore could not be listed; it may be left with its placeholders",
				"vm", name, "placeholders", placeholdersLeft(ctx, p, pool, project, name), "error", err)
			return failedEnd(name, reasonRestoreFailed, cause), nil
		}
		if len(running) > 0 {
			return requestEnd{}, restore.ImportStillRunning(name, running[0])
		}
	}
	if !isGone && !isFinished {
		err := p.DeleteInstance(ctx, project, name)
		if err != nil && !provider.IsNotFound(err) {
			logger.Warn("removing the vm of a failed restore failed; it is left with its placeholders", "vm", name,
				"placeholders", placeholdersLeft(ctx, p, pool, project, name), "error", err)
			return failedEnd(name, reasonRestoreFailed, cause), nil
		}
		logger.Info("vm of a failed restore removed", "vm", name)
	}
	if err := removePlaceholders(ctx, p, pool, project, name); err != nil {
		logger.Warn("removing the placeholders of a restore failed; they are left", "vm", name,
			"placeholders", placeholdersLeft(ctx, p, pool, project, name), "error", err)
	}
	if isFinished {
		logger.Info("the restore had finished; its vm is kept", "vm", name)
		return doneEnd(ctx, p, project, name, logger), nil
	}
	return failedEnd("", reasonRestoreFailed, cause), nil
}

func decodeRestore(leased command.Leased) (restoreOrder, error) {
	var payload restorePayload
	if err := command.DecodePayload(leased, &payload); err != nil {
		return restoreOrder{}, err
	}
	isComplete := payload.VmId != "" && payload.RequestId != "" && payload.SourceVmId != "" && payload.BackupKey != "" &&
		payload.TenantId != "" && payload.ProjectName != "" && payload.DiskSizeGb >= 0
	if !isComplete {
		return restoreOrder{}, command.Permanent(errors.New(
			"vm.restore payload lacks vmId, requestId, sourceVmId, backupKey, tenantId or projectName, or its diskSizeGb is below zero"))
	}
	name, err := provider.InstanceName(payload.VmId)
	if err != nil {
		return restoreOrder{}, command.Permanent(err)
	}
	source, err := provider.InstanceName(payload.SourceVmId)
	if err != nil {
		return restoreOrder{}, command.Permanent(err)
	}
	// Whatever is under the copy's name is taken for this command's own import, and written.
	if name == source {
		return restoreOrder{}, command.Permanent(fmt.Errorf("vm.restore payload restores vm %s onto itself", payload.SourceVmId))
	}
	order := restoreOrder{payload: payload, name: name, copyOf: map[string]string{}}
	for _, volume := range payload.Volumes {
		if volume.SourceVolumeId == "" || volume.NewVolumeId == "" || volume.RequestId == "" {
			return restoreOrder{}, command.Permanent(errors.New("vm.restore payload has a volume without sourceVolumeId, newVolumeId or requestId"))
		}
		original, err := provider.VolumeName(volume.SourceVolumeId)
		if err != nil {
			return restoreOrder{}, command.Permanent(err)
		}
		copied, err := provider.VolumeName(volume.NewVolumeId)
		if err != nil {
			return restoreOrder{}, command.Permanent(err)
		}
		isCopyListed := slices.ContainsFunc(order.copies, func(listed volumeCopy) bool { return listed.name == copied })
		if _, isListed := order.copyOf[original]; isListed || isCopyListed {
			return restoreOrder{}, command.Permanent(fmt.Errorf("vm.restore payload names volume %s or its copy %s twice",
				volume.SourceVolumeId, volume.NewVolumeId))
		}
		order.copyOf[original] = copied
		order.copies = append(order.copies, volumeCopy{name: copied, requestId: volume.RequestId})
	}
	// A copy under the name of a volume the payload copies would be that original: the VM's copy must never hold it.
	for _, copied := range order.copies {
		if _, isAnOriginal := order.copyOf[copied.name]; isAnOriginal {
			return restoreOrder{}, command.Permanent(fmt.Errorf("vm.restore payload restores volume %s onto a volume it copies", copied.name))
		}
	}
	// The command carries the moment the archive was taken, never its key: the worker builds the key as the backup did,
	// so a command reaches only an archive of the cluster, tenant and VM it names.
	takenAt, err := time.Parse(backup.TimeLayout, payload.BackupKey)
	if err != nil {
		return restoreOrder{}, command.Permanent(fmt.Errorf("vm.restore payload: backupKey %q is not a moment written as %s",
			payload.BackupKey, backup.TimeLayout))
	}
	order.key = backup.ObjectKey(leased.ClusterId, payload.TenantId, "vms", payload.SourceVmId, takenAt)
	return order, nil
}

// checkVolumeRestored refuses, for another lease to try, a volume's copy its own volume.restore has not finished. One
// that is not on Incus fails the import on its device after the whole upload; one that is there without that
// command's request on it may be imported and not rolled back yet, and the VM would be reported with a disk that
// holds what the original held after its backup.
func checkVolumeRestored(ctx context.Context, p provider.Provider, pool string, project string, copied volumeCopy) error {
	found, err := p.GetVolume(ctx, project, pool, copied.name)
	isFinished := err == nil && restore.IsFinishedBy(found.Config, copied.requestId)
	if provider.IsNotFound(err) || (err == nil && !isFinished) {
		return fmt.Errorf("volume %s is not restored yet", copied.name)
	}
	if err != nil {
		return fmt.Errorf("look for volume %s: %w", copied.name, err)
	}
	return nil
}

// importPlan is what the archived devices ask of an import beside the VM they were archived of.
type importPlan struct {
	overrides    provider.InstanceOverrides
	placeholders []string
}

// planImport reads the archived instance's own devices. Each names what the original holds, and the copy must hold
// none of it (measured on Incus 7.0.2, titan-ops features/backup/design.md#restore-as-new-resource):
//   - a volume's disk goes to the volume's copy, or, for a volume that is not restored, to a placeholder: Incus fails
//     the import on a volume attached elsewhere or not there, and a volume that is there and free would become the
//     copy's;
//   - a NIC gets a MAC of its own, and loses an address pinned on it;
//   - the instance gets a uuid of its own, after which OVN names its port.
//
// The devices are read in the order of their names and the config entries sorted, so the same archive asks the same
// import and the same placeholder names every run.
func planImport(devices map[string]map[string]string, instance string, copyOf map[string]string) importPlan {
	plan := importPlan{overrides: provider.InstanceOverrides{Config: []string{"volatile.uuid="}}}
	for _, device := range slices.Sorted(maps.Keys(devices)) {
		settings := devices[device]
		switch {
		case isVolumeDisk(settings):
			source, isRestored := copyOf[settings["source"]]
			if !isRestored {
				source = placeholderName(strconv.Itoa(len(plan.placeholders)), instance)
				plan.placeholders = append(plan.placeholders, source)
			}
			plan.overrides.Devices = append(plan.overrides.Devices, device+",source="+source)
		case settings["type"] == "nic":
			plan.overrides.Config = append(plan.overrides.Config, "volatile."+device+".hwaddr=")
			if settings["ipv4.address"] != "" {
				plan.overrides.Devices = append(plan.overrides.Devices, device+",ipv4.address=")
			}
		}
	}
	slices.Sort(plan.overrides.Config)
	return plan
}

// isVolumeDisk says whether a device is a custom volume's: a disk with a source that is not the root disk.
func isVolumeDisk(settings map[string]string) bool {
	return settings["type"] == "disk" && settings["source"] != "" && settings["path"] != "/"
}

const (
	placeholderPrefix = "ph"
	placeholderSize   = "1GiB"
)

// placeholderName is the volume standing in for the archived volume of that number that is not restored. It is made
// from the copy's name, so a run finds the placeholders of an earlier one, and it is no Titan volume's name ("vol…").
func placeholderName(number string, instance string) string {
	return placeholderPrefix + number + "-" + instance
}

// createPlaceholder makes an empty block volume; on a thin pool it takes no space. One already there is an earlier
// run's.
func createPlaceholder(ctx context.Context, p provider.Provider, pool string, project string, name string) error {
	request := api.StorageVolumesPost{
		Name:             name,
		Type:             "custom",
		ContentType:      "block",
		StorageVolumePut: api.StorageVolumePut{Config: map[string]string{"size": placeholderSize}},
	}
	err := p.CreateVolume(ctx, project, pool, request)
	if err != nil && !provider.IsConflict(err) {
		return fmt.Errorf("create placeholder %s: %w", name, err)
	}
	return nil
}

// placeholdersOf lists the placeholders of the copy that are on Incus, found by their names: a run may be the one after
// the run that made them. They are made in the order of their numbers and deleted in the reverse, so the ones left by a
// run that died are always the first few, and counting up to the first one missing finds them all.
func placeholdersOf(ctx context.Context, p provider.Provider, pool string, project string, instance string) ([]string, error) {
	found := []string{}
	for {
		name := placeholderName(strconv.Itoa(len(found)), instance)
		_, err := p.GetVolume(ctx, project, pool, name)
		if provider.IsNotFound(err) {
			return found, nil
		}
		if err != nil {
			return nil, fmt.Errorf("look for placeholder %s: %w", name, err)
		}
		found = append(found, name)
	}
}

// removePlaceholders deletes the placeholders of the copy, the last one made first.
func removePlaceholders(ctx context.Context, p provider.Provider, pool string, project string, instance string) error {
	placeholders, err := placeholdersOf(ctx, p, pool, project, instance)
	if err != nil {
		return err
	}
	for _, name := range slices.Backward(placeholders) {
		err := p.DeleteVolume(ctx, project, pool, name)
		if err != nil && !provider.IsNotFound(err) {
			return fmt.Errorf("delete placeholder %s: %w", name, err)
		}
	}
	return nil
}

// placeholdersLeft is what a give-up's warning says of the placeholders it leaves on Incus: their names, or, when they
// cannot be looked for, why and how they are named.
func placeholdersLeft(ctx context.Context, p provider.Provider, pool string, project string, instance string) any {
	left, err := placeholdersOf(ctx, p, pool, project, instance)
	if err != nil {
		return fmt.Sprintf("not read (%v); named %s", err, placeholderName("<n>", instance))
	}
	return left
}

// checkRoom is permanent only for a pool that was read and is too full; a pool that could not be read is asked again
// by the next lease.
func checkRoom(ctx context.Context, p provider.Provider, pool string, diskSizeGb int) error {
	err := restore.CheckPoolRoom(ctx, p, pool, int64(diskSizeGb)<<30)
	if errors.Is(err, restore.ErrNoRoom) {
		return command.Permanent(err)
	}
	return err
}

// deleteSnapshots removes every snapshot the instance has. One already gone was removed by another run of the command.
func deleteSnapshots(ctx context.Context, p provider.Provider, project string, instance string) error {
	snapshots, err := p.ListInstanceSnapshots(ctx, project, instance)
	if err != nil {
		return fmt.Errorf("list the snapshots of vm %s: %w", instance, err)
	}
	for _, snapshot := range snapshots {
		err := p.DeleteInstanceSnapshot(ctx, project, instance, snapshot.Name)
		if err != nil && !provider.IsNotFound(err) {
			return fmt.Errorf("delete the %q snapshot of vm %s: %w", snapshot.Name, instance, err)
		}
	}
	return nil
}

// finishCopy leaves the copy with the disk devices Titan records for it, the name the admin gave it and the request of
// this command, which says the copy is finished (restore.FinishedKey): each volume's copy under a device named after
// it, as volume.attach names one, and no other volume. The import named the devices after the originals, and pointed
// the ones not restored at placeholders. One PUT does it all, so a copy that carries the request has its devices too.
// The PUT replaces the device list, so it carries the read's ETag: Incus refuses it (412) when the instance was written
// since, and the next lease reads again. A copy that already carries the request was finished by another run of the
// command since this one looked for it: it is the tenant's, and is not written.
func finishCopy(ctx context.Context, p provider.Provider, pool string, project string, name string, order restoreOrder) error {
	instance, etag, err := p.GetInstanceWithEtag(ctx, project, name)
	if err != nil {
		return fmt.Errorf("read vm %s: %w", name, err)
	}
	if restore.IsFinishedBy(instance.Config, order.payload.RequestId) {
		return nil
	}
	put := instance.Writable()
	if put.Devices == nil {
		put.Devices = map[string]map[string]string{}
	}
	if put.Config == nil {
		put.Config = map[string]string{}
	}
	wanted := map[string]map[string]string{}
	for _, copied := range order.copies {
		wanted[copied.name] = map[string]string{"type": "disk", "pool": pool, "source": copied.name}
	}
	for device, settings := range put.Devices {
		_, isWanted := wanted[device]
		if isVolumeDisk(settings) && !isWanted {
			delete(put.Devices, device)
		}
	}
	maps.Copy(put.Devices, wanted)
	put.Config[displayNameKey] = order.payload.Name
	put.Config[restore.FinishedKey] = order.payload.RequestId
	if err := p.UpdateInstance(ctx, project, name, put, etag); err != nil {
		return fmt.Errorf("set the devices of vm %s: %w", name, err)
	}
	return nil
}
