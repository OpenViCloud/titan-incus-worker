package restore

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"

	"go.yaml.in/yaml/v4"
)

// indexPath is the first tar entry of an archive Incus writes, of a VM and of a volume alike (measured on Incus
// 7.0.2, titan-ops features/backup/design.md#restore-as-new-resource).
const indexPath = "backup/index.yaml"

// maxIndexBytes is the size Incus itself stops reading an index at (internal/server/backup/backup_info.go of v7.0.1).
const maxIndexBytes = 1 << 20

// Index is what an archive says of itself before its disk: enough to prepare an import under another name.
type Index struct {
	// Name is the Incus name of the instance or volume the archive was taken of.
	Name string
	// Type is "virtual-machine" for a VM's archive, "custom" for a volume's.
	Type string
	// Snapshots are the snapshots the archive carries.
	Snapshots []string
	// Devices and Config are the archived instance's own, without what its profiles add: an import override addresses
	// a device of the instance itself. A volume's archive has neither.
	Devices map[string]map[string]string
	Config  map[string]string
}

// indexFile is the part of backup/index.yaml a restore reads.
type indexFile struct {
	Name      string   `yaml:"name"`
	Type      string   `yaml:"type"`
	Snapshots []string `yaml:"snapshots"`
	Config    struct {
		Container struct {
			Config  map[string]string            `yaml:"config"`
			Devices map[string]map[string]string `yaml:"devices"`
		} `yaml:"container"`
	} `yaml:"config"`
}

// ReadIndex reads the index of the archive at key and closes the stream there: the disk behind the index is as large
// as the archive, and the import streams all of it once more.
func ReadIndex(ctx context.Context, objects Objects, key string) (Index, error) {
	body, err := objects.Get(ctx, key)
	if err != nil {
		return Index{}, fmt.Errorf("read archive %s: %w", key, err)
	}
	defer body.Close()
	unzipped, err := gzip.NewReader(body)
	if err != nil {
		return Index{}, fmt.Errorf("archive %s is not a gzip stream: %w", key, err)
	}
	entries := tar.NewReader(unzipped)
	first, err := entries.Next()
	if err != nil {
		return Index{}, fmt.Errorf("read the first entry of archive %s: %w", key, err)
	}
	if first.Name != indexPath {
		return Index{}, fmt.Errorf("archive %s opens on %q, not on %s", key, first.Name, indexPath)
	}
	if first.Size > maxIndexBytes {
		return Index{}, fmt.Errorf("the index of archive %s is %d bytes, over the %d a restore reads", key, first.Size, maxIndexBytes)
	}
	content, err := io.ReadAll(entries)
	if err != nil {
		return Index{}, fmt.Errorf("read the index of archive %s: %w", key, err)
	}
	var file indexFile
	if err := yaml.Unmarshal(content, &file); err != nil {
		return Index{}, fmt.Errorf("decode the index of archive %s: %w", key, err)
	}
	return Index{
		Name:      file.Name,
		Type:      file.Type,
		Snapshots: file.Snapshots,
		Devices:   file.Config.Container.Devices,
		Config:    file.Config.Container.Config,
	}, nil
}
