package provider

import (
	"encoding/base32"
	"encoding/hex"
	"fmt"
	"strings"
)

// Titan names every Incus object it creates from the Titan id: unique in the project, so a repeated create finds its
// own object (https://github.com/OpenViCloud/titan-ops/blob/main/docs/architecture/intent-and-observation.md#provider-create-idempotency). The id is written in base32,
// not as a UUID: LVM refuses an LV name past 117 characters and Incus doubles every "-" in it (lab-storage 05c), which
// leaves a "<prefix>-<uuid>" snapshot of a "vm-<uuid>" no room (https://github.com/OpenViCloud/titan-ops/blob/main/docs/features/vm/design.md#incus-names).
const (
	instancePrefix = "vm"
	volumePrefix   = "vol"
	snapshotPrefix = "s"
	// A backup uploads a stopped copy of a snapshot, never the disk a running VM is writing
	// (https://github.com/OpenViCloud/titan-ops/blob/main/docs/features/backup/design.md#incus-backup).
	backupInstanceCopyPrefix = "bk"
	backupVolumeCopyPrefix   = "bkv"
)

var lowerBase32 = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

func InstanceName(vmId string) (string, error) {
	return objectName(instancePrefix, vmId)
}

func VolumeName(volumeId string) (string, error) {
	return objectName(volumePrefix, volumeId)
}

func BackupInstanceCopyName(vmId string) (string, error) {
	return objectName(backupInstanceCopyPrefix, vmId)
}

func BackupVolumeCopyName(volumeId string) (string, error) {
	return objectName(backupVolumeCopyPrefix, volumeId)
}

// SnapshotName names a snapshot of an instance or of a volume; it is unique under its parent.
func SnapshotName(snapshotId string) (string, error) {
	return objectName(snapshotPrefix, snapshotId)
}

// objectName is prefix + the 16 bytes of the id in lowercase base32 (26 characters). A letter first and only a-z, 2-7
// after it: a valid instance name (a hostname) and a valid volume and snapshot name.
func objectName(prefix string, id string) (string, error) {
	digits := strings.ReplaceAll(id, "-", "")
	bytes, err := hex.DecodeString(digits)
	if err != nil || len(bytes) != 16 {
		return "", fmt.Errorf("%q is not a Titan id (a UUID)", id)
	}
	return prefix + lowerBase32.EncodeToString(bytes), nil
}

// NetworkName is the name Titan gives a network on Incus, unique in the project.
func NetworkName(networkId string) string {
	return "net-" + networkId
}

// NetworkAclName is the network ACL that holds a network's firewall rules, in the network's project
// (https://github.com/OpenViCloud/titan-ops/blob/main/docs/features/network/design.md#firewall-on-incus).
func NetworkAclName(networkId string) string {
	return "acl-" + networkId
}

// TemplateAlias is the template's image alias on a cluster: Titan's name, so a repeated sync finds it.
func TemplateAlias(templateId string) string {
	return "tpl-" + templateId
}

// SnapshotId is how Titan stores a snapshot's provider id: "<parent>/<snapshot>", the name Incus gives it in lists
// (lab-storage 03) and in a copy source.
func SnapshotId(parent string, snapshot string) string {
	return parent + "/" + snapshot
}

// SplitSnapshotId reverses SnapshotId.
func SplitSnapshotId(externalId string) (string, string, error) {
	parent, snapshot, found := strings.Cut(externalId, "/")
	if !found || parent == "" || snapshot == "" {
		return "", "", fmt.Errorf("%q is not a snapshot provider id (<parent>/<snapshot>)", externalId)
	}
	return parent, snapshot, nil
}

// DefaultStoragePool is the pool a zone uses when its cluster names none (the lab's pool).
const DefaultStoragePool = "default"

func StoragePoolOrDefault(pool string) string {
	if pool == "" {
		return DefaultStoragePool
	}
	return pool
}
