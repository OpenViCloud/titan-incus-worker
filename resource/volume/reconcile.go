package volume

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"slices"
	"strings"
	"sync"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
)

// ReconcilableVolume mirrors ReconcilableVolumeQueryResult: what Titan last recorded of a volume. The attached VM is
// named by its Incus instance, so the worker compares names it reads in used_by.
type ReconcilableVolume struct {
	Id                          string `json:"id"`
	ExternalId                  string `json:"externalId"`
	CurrentExistence            string `json:"currentExistence"`
	CurrentAttachedExternalVmId string `json:"currentAttachedExternalVmId"`
	CurrentAttachedElsewhere    bool   `json:"currentAttachedElsewhere"`
}

// ReconcilableVolumeSnapshot mirrors ReconcilableVolumeSnapshotQueryResult; VolumeId is the parent volume.
type ReconcilableVolumeSnapshot struct {
	Id               string `json:"id"`
	VolumeId         string `json:"volumeId"`
	ExternalId       string `json:"externalId"`
	CurrentExistence string `json:"currentExistence"`
}

type reconcilableStorage struct {
	Volumes   []ReconcilableVolume         `json:"volumes"`
	Snapshots []ReconcilableVolumeSnapshot `json:"snapshots"`
}

// The CurrentExistence words of the control plane (Titan.Domain.Values.CurrentExistence) the reconciler compares to.
const (
	existenceExists   = "Exists"
	existenceNotFound = "NotFound"
)

// observedVolume is what one read of the pool says about a volume Titan knows.
type observedVolume struct {
	project   string
	instances []string
}

// Reconciler observes the clusters' volumes and volume snapshots. It remembers, per volume used by an instance outside
// Titan, the instances it last reported: the control plane records only that something outside Titan uses the volume,
// not which instances, so without this the volume would be sent every cycle
// (titan-ops architecture/resource-lifecycle.md#reconciler). A restart forgets, and each such volume is sent once more.
type Reconciler struct {
	mu            sync.Mutex
	sentElsewhere map[string]string
}

func NewReconciler() *Reconciler {
	return &Reconciler{sentElsewhere: map[string]string{}}
}

// Reconcile observes one cluster's volumes and volume snapshots and reports what differs on their observation routes;
// it never acts on Incus, and it never reports Deleted: a volume or snapshot gone is NotFound, only Titan's own delete
// command ends one (titan-ops architecture/desired-and-current.md#existence-axis). The all-projects volume list has no
// snapshots (lab-storage 02), so the snapshots of each volume are one more read. A failed read skips what it would have
// said: reading it as "nothing there" would report everything gone.
func (r *Reconciler) Reconcile(ctx context.Context, cp *controlplane.Client, p provider.Provider, clusterId string, storagePool string, logger *slog.Logger) error {
	pool := provider.StoragePoolOrDefault(storagePool)
	var rows reconcilableStorage
	if err := cp.GetJSON(ctx, "/internal/reconcile/volumes?clusterId="+url.QueryEscape(clusterId), &rows); err != nil {
		return fmt.Errorf("list reconcilable storage of cluster %s: %w", clusterId, err)
	}
	if len(rows.Volumes) == 0 && len(rows.Snapshots) == 0 {
		return nil
	}
	volumes, err := p.ListVolumesAllProjects(ctx, pool)
	if err != nil {
		return fmt.Errorf("observe volumes of cluster %s: %w", clusterId, err)
	}
	observed := ObserveVolumes(volumes)

	for _, row := range rows.Volumes {
		report, shouldReport := DecideVolumeReport(row, observed)
		if !shouldReport || r.sentAlready(row, report) {
			continue
		}
		err := sendObservation(ctx, cp, row.Id, *report.Exists, report.AttachedTo)
		if err != nil {
			logger.Warn("volume report failed; next cycle sends it again", "volumeId", row.Id, "error", err)
			continue
		}
		r.remember(row, report)
		logger.Info("reconcile report sent", "volumeId", row.Id, "recordedExistence", row.CurrentExistence,
			"exists", *report.Exists, "attachedTo", report.AttachedTo)
	}

	snapshotsByVolume := map[string]map[string]bool{}
	for _, row := range rows.Snapshots {
		parent, _, err := provider.SplitSnapshotId(row.ExternalId)
		if err != nil {
			continue
		}
		_, alreadyRead := snapshotsByVolume[parent]
		volume, volumePresent := observed[parent]
		if alreadyRead || !volumePresent {
			continue
		}
		snapshots, err := p.ListVolumeSnapshots(ctx, volume.project, pool, parent)
		if err != nil {
			logger.Warn("reading a volume's snapshots failed; skipping them this cycle", "volume", parent, "error", err)
			continue
		}
		snapshotsByVolume[parent] = SnapshotNames(parent, snapshots)
	}
	for _, row := range rows.Snapshots {
		report, shouldReport := DecideSnapshotReport(row, observed, snapshotsByVolume)
		if !shouldReport {
			continue
		}
		err := sendSnapshotObservation(ctx, cp, row.VolumeId, row.Id, *report.Exists)
		if err != nil {
			logger.Warn("volume snapshot report failed; next cycle sends it again", "snapshotId", row.Id, "error", err)
			continue
		}
		logger.Info("reconcile report sent", "snapshotId", row.Id, "volumeId", row.VolumeId,
			"recordedExistence", row.CurrentExistence, "exists", *report.Exists)
	}
	return nil
}

// sentAlready: a volume recorded as used outside Titan whose instances are the ones last reported has nothing new to say.
func (r *Reconciler) sentAlready(row ReconcilableVolume, report StatusReport) bool {
	if !row.CurrentAttachedElsewhere || !*report.Exists {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	sent, known := r.sentElsewhere[row.Id]
	return known && sent == instanceSet(report.AttachedTo)
}

// remember keeps the instances just reported for every volume reported in use, since the control plane may record any
// of them as used outside Titan, and forgets a volume reported free or gone.
func (r *Reconciler) remember(row ReconcilableVolume, report StatusReport) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !*report.Exists || len(report.AttachedTo) == 0 {
		delete(r.sentElsewhere, row.Id)
		return
	}
	r.sentElsewhere[row.Id] = instanceSet(report.AttachedTo)
}

func instanceSet(names []string) string {
	sorted := slices.Clone(names)
	slices.Sort(sorted)
	return strings.Join(sorted, "\n")
}

// ObserveVolumes keys the pool's custom volumes by name, with the instances using each. Instance and image volumes
// share the list (lab-storage 02) and are not Titan's volumes.
func ObserveVolumes(volumes []api.StorageVolume) map[string]observedVolume {
	observed := map[string]observedVolume{}
	for _, volume := range volumes {
		if volume.Type != "custom" {
			continue
		}
		observed[volume.Name] = observedVolume{project: volume.Project, instances: InstanceNames(volume.UsedBy)}
	}
	return observed
}

// SnapshotNames reads a snapshot list, which names each "<volume>/<snapshot>" (lab-storage 03).
func SnapshotNames(volume string, snapshots []api.StorageVolumeSnapshot) map[string]bool {
	names := map[string]bool{}
	for _, snapshot := range snapshots {
		_, name, err := provider.SplitSnapshotId(snapshot.Name)
		if err != nil {
			name = snapshot.Name
		}
		names[name] = true
	}
	return names
}

// DecideVolumeReport: a volume not in the pool is NotFound unless that is already recorded; otherwise only a change of
// existence or attachment is sent.
func DecideVolumeReport(row ReconcilableVolume, observed map[string]observedVolume) (StatusReport, bool) {
	volume, present := observed[row.ExternalId]
	if !present {
		alreadyRecordedGone := row.CurrentExistence == existenceNotFound
		return Observed(false, nil), !alreadyRecordedGone
	}
	changed := row.CurrentExistence != existenceExists || attachmentChanged(row, volume.instances)
	return Observed(true, volume.instances), changed
}

// attachmentChanged compares the instances using the volume with the Titan VM last recorded. The worker cannot tell
// an instance outside Titan from another Titan VM, so a volume recorded as used elsewhere is always a candidate; the
// Reconciler sends it only when its instances changed since the last report.
func attachmentChanged(row ReconcilableVolume, instances []string) bool {
	if row.CurrentAttachedElsewhere {
		return true
	}
	if row.CurrentAttachedExternalVmId == "" {
		return len(instances) > 0
	}
	usedByTheRecordedVmOnly := len(instances) == 1 && instances[0] == row.CurrentAttachedExternalVmId
	return !usedByTheRecordedVmOnly
}

// DecideSnapshotReport: a snapshot whose volume is gone is gone too (Incus deletes them together, lab-storage 07).
// Nothing is said about one whose volume's snapshots could not be read this cycle.
func DecideSnapshotReport(row ReconcilableVolumeSnapshot, observed map[string]observedVolume, snapshotsByVolume map[string]map[string]bool) (StatusReport, bool) {
	parent, name, err := provider.SplitSnapshotId(row.ExternalId)
	if err != nil {
		return StatusReport{}, false
	}
	alreadyRecordedGone := row.CurrentExistence == existenceNotFound
	_, volumePresent := observed[parent]
	if !volumePresent {
		return Observed(false, nil), !alreadyRecordedGone
	}
	snapshots, read := snapshotsByVolume[parent]
	if !read {
		return StatusReport{}, false
	}
	if !snapshots[name] {
		return Observed(false, nil), !alreadyRecordedGone
	}
	return Observed(true, nil), row.CurrentExistence != existenceExists
}
