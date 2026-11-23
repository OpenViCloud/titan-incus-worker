package provider

import (
	"encoding/base32"
	"encoding/hex"
	"fmt"
	"strings"
)

// Titan names every Incus object it creates from the Titan id: unique in the project, so a repeated create finds its
// own object (titan-ops architecture/intent-and-observation.md#provider-create-idempotency). The id is written in base32,
// not as a UUID: LVM refuses an LV name past 117 characters and Incus doubles every "-" in it (lab-storage 05c), which
// leaves a "<prefix>-<uuid>" snapshot of a "vm-<uuid>" no room (titan-ops features/vm/design.md#incus-names).
const (
	instancePrefix = "vm"
	volumePrefix   = "vol"
	snapshotPrefix = "s"
)

var lowerBase32 = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

func InstanceName(vmId string) (string, error) {
	return objectName(instancePrefix, vmId)
}

// instanceNameLength is "vm" and the 26 characters of objectName.
const instanceNameLength = len(instancePrefix) + 26

// IsInstanceName says whether an Incus instance has the shape InstanceName gives: "vm" and 26 characters of a-z, 2-7.
// The shape, not a decode: an instance named by hand in Titan's shape counts too.
func IsInstanceName(name string) bool {
	if len(name) != instanceNameLength || !strings.HasPrefix(name, instancePrefix) {
		return false
	}
	for _, character := range name[len(instancePrefix):] {
		isBase32 := (character >= 'a' && character <= 'z') || (character >= '2' && character <= '7')
		if !isBase32 {
			return false
		}
	}
	return true
}

func VolumeName(volumeId string) (string, error) {
	return objectName(volumePrefix, volumeId)
}

// VolumeIdFromName is the Titan id (a UUID) a volume name was made from; a drill finds a VM's attached volumes by the
// device's source name and needs their ids to find their archives.
func VolumeIdFromName(name string) (string, error) {
	encoded, hasPrefix := strings.CutPrefix(name, volumePrefix)
	if !hasPrefix {
		return "", fmt.Errorf("%q is not a Titan volume name", name)
	}
	bytes, err := lowerBase32.DecodeString(encoded)
	if err != nil || len(bytes) != 16 {
		return "", fmt.Errorf("%q is not a Titan volume name", name)
	}
	hexDigits := hex.EncodeToString(bytes)
	return hexDigits[0:8] + "-" + hexDigits[8:12] + "-" + hexDigits[12:16] + "-" + hexDigits[16:20] + "-" + hexDigits[20:], nil
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

// ProjectKey keys an Incus object listed across every project by its project and name: names are unique within a
// project only, and an operator may make one of the same name elsewhere (titan-ops features/backup/design.md#incus-backup).
// An empty project is the key of a row from a control plane that sent no project: the object found by its name alone.
func ProjectKey(project string, name string) string {
	return project + "/" + name
}

// NetworkAclName is the network ACL that holds a network's firewall rules, in the network's project
// (titan-ops features/network/design.md#firewall-on-incus).
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

// DefaultStoragePool is the pool a cluster uses when it names none (the lab's pool).
const DefaultStoragePool = "default"

func StoragePoolOrDefault(pool string) string {
	if pool == "" {
		return DefaultStoragePool
	}
	return pool
}
