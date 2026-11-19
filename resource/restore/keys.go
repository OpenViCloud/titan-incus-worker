package restore

import (
	"strings"
	"time"

	"github.com/OpenViCloud/titan-incus-worker/resource/backup"
)

// configKey is one cluster.backup object: incus/<clusterId>/cluster/<ts>.yaml.
type configKey struct {
	TakenAt time.Time
	Key     string
}

// archiveKey is one vm.backup or volume.backup object: incus/<clusterId>/<tenantId>/<vms|volumes>/<id>/<ts>.tar.gz.
// The tenant segment is not kept: the id alone, not the tenant folder it sits under, is what Titan's rows name
// (titan-ops features/backup/design.md#cluster-restore).
type archiveKey struct {
	TakenAt time.Time
	Key     string
}

type parsedKeys struct {
	Configs []configKey
	// Archives is keyed by collection ("vms" or "volumes") + "/" + id, every archive of that id.
	Archives map[string][]archiveKey
}

// parseObjectKeys sorts List's keys into cluster config objects and resource archives, ignoring anything whose shape
// it does not recognize (a key the backup side changed under this worker's feet is not this restore's to fail on).
func parseObjectKeys(keys []string) parsedKeys {
	result := parsedKeys{Archives: map[string][]archiveKey{}}
	for _, key := range keys {
		parts := strings.Split(key, "/")
		if len(parts) == 4 && parts[0] == "incus" && parts[2] == "cluster" {
			takenAt, ok := parseTakenAt(parts[3], ".yaml")
			if ok {
				result.Configs = append(result.Configs, configKey{TakenAt: takenAt, Key: key})
			}
			continue
		}
		if len(parts) != 6 || parts[0] != "incus" {
			continue
		}
		collection, id := parts[3], parts[4]
		if collection != "vms" && collection != "volumes" {
			continue
		}
		takenAt, ok := parseTakenAt(parts[5], ".tar.gz")
		if !ok {
			continue
		}
		mapKey := collection + "/" + id
		result.Archives[mapKey] = append(result.Archives[mapKey], archiveKey{TakenAt: takenAt, Key: key})
	}
	return result
}

func parseTakenAt(filename string, suffix string) (time.Time, bool) {
	trimmed := strings.TrimSuffix(filename, suffix)
	if trimmed == filename {
		return time.Time{}, false
	}
	takenAt, err := time.Parse(backup.TimeLayout, trimmed)
	if err != nil {
		return time.Time{}, false
	}
	return takenAt, true
}

// pickConfig is the newest config at or before point ("" for no limit): the restore point.
func pickConfig(configs []configKey, point string) (configKey, bool) {
	limit, hasLimit := time.Time{}, false
	if point != "" {
		parsed, err := time.Parse(backup.TimeLayout, point)
		if err == nil {
			limit, hasLimit = parsed, true
		}
	}
	var best configKey
	found := false
	for _, candidate := range configs {
		if hasLimit && candidate.TakenAt.After(limit) {
			continue
		}
		if !found || candidate.TakenAt.After(best.TakenAt) {
			best, found = candidate, true
		}
	}
	return best, found
}

// pickArchive is the newest archive at or before atOrBefore (the restore point's own time): the one disk state the
// cluster config and the resource's archive agree on.
func pickArchive(archives []archiveKey, atOrBefore time.Time) (archiveKey, bool) {
	var best archiveKey
	found := false
	for _, candidate := range archives {
		if candidate.TakenAt.After(atOrBefore) {
			continue
		}
		if !found || candidate.TakenAt.After(best.TakenAt) {
			best, found = candidate, true
		}
	}
	return best, found
}
