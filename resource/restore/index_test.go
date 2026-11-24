package restore

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

const indexKey = "incus/c1/tenants/t1/vms/vm1/20261122T020000Z.tar.gz"

// vmIndex is the shape of backup/index.yaml measured on Incus 7.0.2 for a VM with one attached volume and a pinned
// address, cut to the fields a restore reads plus some it must skip. expanded_devices carries one device more than
// devices, as it does for an instance whose profile adds one.
const vmIndex = `name: vmoriginal
backend: lvm
pool: default
snapshots:
    - backup
    - sold
optimized: false
optimized_header: false
type: virtual-machine
config:
    container:
        architecture: x86_64
        config:
            limits.cpu: "2"
            user.titan-name: web-1
            volatile.eth0.hwaddr: 10:66:6a:9c:6a:01
            volatile.uuid: 6d3f6f6a-52b5-4bd5-9d3c-0d2f6a1f0a11
        devices:
            eth0:
                ipv4.address: 10.189.217.2
                network: net-a0331ba7
                type: nic
            root:
                path: /
                pool: default
                size: 5GiB
                type: disk
            voloriginal:
                pool: default
                source: voloriginal
                type: disk
        expanded_devices:
            eth0:
                ipv4.address: 10.189.217.2
                network: net-a0331ba7
                type: nic
            fromprofile:
                path: /mnt
                source: /srv
                type: disk
            root:
                path: /
                pool: default
                size: 5GiB
                type: disk
            voloriginal:
                pool: default
                source: voloriginal
                type: disk
        name: vmoriginal
        status: Running
        type: virtual-machine
    pool:
        name: default
        driver: lvm
`

type tarEntry struct {
	name    string
	content []byte
}

func tarGz(t *testing.T, entries ...tarEntry) []byte {
	t.Helper()
	var archive bytes.Buffer
	zipped := gzip.NewWriter(&archive)
	packed := tar.NewWriter(zipped)
	for _, entry := range entries {
		if err := packed.WriteHeader(&tar.Header{Name: entry.name, Mode: 0o644, Size: int64(len(entry.content))}); err != nil {
			t.Fatal(err)
		}
		if _, err := packed.Write(entry.content); err != nil {
			t.Fatal(err)
		}
	}
	if err := packed.Close(); err != nil {
		t.Fatal(err)
	}
	if err := zipped.Close(); err != nil {
		t.Fatal(err)
	}
	return archive.Bytes()
}

// trackedObject is one object of a bucket that tells how much of it was read and whether it was closed.
type trackedObject struct {
	content []byte
	getErr  error

	opened int
	read   int
	closed int
}

func (o *trackedObject) List(ctx context.Context, prefix string) ([]string, error) {
	return nil, nil
}

func (o *trackedObject) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if o.getErr != nil {
		return nil, o.getErr
	}
	o.opened++
	return &trackedStream{object: o, reader: bytes.NewReader(o.content)}, nil
}

type trackedStream struct {
	object *trackedObject
	reader *bytes.Reader
}

func (s *trackedStream) Read(buffer []byte) (int, error) {
	count, err := s.reader.Read(buffer)
	s.object.read += count
	return count, err
}

func (s *trackedStream) Close() error {
	s.object.closed++
	return nil
}

func TestReadIndex_ReadsTheNameTypeSnapshots_AndTheInstancesOwnDevicesAndConfig(t *testing.T) {
	object := &trackedObject{content: tarGz(t,
		tarEntry{"backup/index.yaml", []byte(vmIndex)},
		tarEntry{"backup/virtual-machine.img", []byte("the disk")},
	)}

	index, err := ReadIndex(context.Background(), object, indexKey)

	if err != nil {
		t.Fatal(err)
	}
	if index.Name != "vmoriginal" || index.Type != "virtual-machine" || !slices.Equal(index.Snapshots, []string{"backup", "sold"}) {
		t.Errorf("index: %+v", index)
	}
	wantDevices := map[string]map[string]string{
		"eth0":        {"type": "nic", "network": "net-a0331ba7", "ipv4.address": "10.189.217.2"},
		"root":        {"type": "disk", "path": "/", "pool": "default", "size": "5GiB"},
		"voloriginal": {"type": "disk", "pool": "default", "source": "voloriginal"},
	}
	if len(index.Devices) != len(wantDevices) {
		t.Errorf("devices are not the instance's own three: %v", index.Devices)
	}
	for name, want := range wantDevices {
		if !maps.Equal(index.Devices[name], want) {
			t.Errorf("device %s: got %v, want %v", name, index.Devices[name], want)
		}
	}
	wantConfig := map[string]string{
		"limits.cpu": "2", "user.titan-name": "web-1",
		"volatile.eth0.hwaddr": "10:66:6a:9c:6a:01", "volatile.uuid": "6d3f6f6a-52b5-4bd5-9d3c-0d2f6a1f0a11",
	}
	if !maps.Equal(index.Config, wantConfig) {
		t.Errorf("config: %v", index.Config)
	}
}

// A volume's archive has no instance in it: its index still says what it is and which snapshots it carries.
func TestReadIndex_AVolumesArchive_HasNoDevices(t *testing.T) {
	volumeIndex := "name: voloriginal\ntype: custom\nsnapshots:\n    - backup\nconfig:\n    volume:\n        config:\n            size: 1GiB\n        name: voloriginal\n"
	object := &trackedObject{content: tarGz(t, tarEntry{"backup/index.yaml", []byte(volumeIndex)})}

	index, err := ReadIndex(context.Background(), object, indexKey)

	if err != nil {
		t.Fatal(err)
	}
	if index.Name != "voloriginal" || index.Type != "custom" || !slices.Equal(index.Snapshots, []string{"backup"}) || len(index.Devices) != 0 || len(index.Config) != 0 {
		t.Errorf("index: %+v", index)
	}
}

// The archive a test hands the fake import is one this reader reads: a handler's test serves both from one object.
func TestReadIndex_ReadsTheArchiveTheFakeImportReads(t *testing.T) {
	description := testsupport.ArchiveDescription{
		Name: "vmoriginal", Snapshots: []string{"backup"},
		Devices: map[string]map[string]string{"eth0": {"type": "nic", "network": "net-1"}},
		Config:  map[string]string{"volatile.uuid": "6d3f6f6a-52b5-4bd5-9d3c-0d2f6a1f0a11"},
	}
	object := &trackedObject{content: []byte(description.TarGz())}

	index, err := ReadIndex(context.Background(), object, indexKey)

	if err != nil {
		t.Fatal(err)
	}
	isAsDescribed := index.Name == "vmoriginal" && index.Type == "virtual-machine" && slices.Equal(index.Snapshots, []string{"backup"}) &&
		maps.Equal(index.Devices["eth0"], description.Devices["eth0"]) && maps.Equal(index.Config, description.Config)
	if !isAsDescribed {
		t.Errorf("index: %+v", index)
	}
}

// Only the first entry is read: the index is a few KB of an archive as large as a disk, and the import that follows
// streams the whole of it once more.
func TestReadIndex_ClosesTheStream_WithoutReadingTheDiskBehindTheIndex(t *testing.T) {
	disk := make([]byte, 4<<20)
	if _, err := rand.Read(disk); err != nil {
		t.Fatal(err)
	}
	object := &trackedObject{content: tarGz(t,
		tarEntry{"backup/index.yaml", []byte(vmIndex)},
		tarEntry{"backup/virtual-machine.img", disk},
	)}

	_, err := ReadIndex(context.Background(), object, indexKey)

	if err != nil {
		t.Fatal(err)
	}
	if object.opened != 1 || object.closed != 1 {
		t.Errorf("opened %d time(s), closed %d", object.opened, object.closed)
	}
	if object.read > 256<<10 {
		t.Errorf("read %d of %d bytes for an index of %d", object.read, len(object.content), len(vmIndex))
	}
}

func TestReadIndex_AnArchiveItCannotUse_IsAnErrorNamingTheArchive_AndTheStreamIsClosed(t *testing.T) {
	notYaml := "name: [unclosed"
	for name, tc := range map[string]struct {
		content []byte
		says    string
	}{
		"the first entry is not the index": {
			tarGz(t, tarEntry{"backup/virtual-machine.img", []byte("the disk")}, tarEntry{"backup/index.yaml", []byte(vmIndex)}),
			`"backup/virtual-machine.img"`,
		},
		"the index is nested deeper": {tarGz(t, tarEntry{"other/backup/index.yaml", []byte(vmIndex)}), `"other/backup/index.yaml"`},
		"not gzip":                   {[]byte(testsupport.ArchiveBody("backup")), "gzip"},
		"an empty object":            {nil, "gzip"},
		"a tar with no entry":        {tarGz(t), "entry"},
		"an index that is not yaml":  {tarGz(t, tarEntry{"backup/index.yaml", []byte(notYaml)}), "decode"},
	} {
		object := &trackedObject{content: tc.content}

		index, err := ReadIndex(context.Background(), object, indexKey)

		if err == nil || !strings.Contains(err.Error(), tc.says) || !strings.Contains(err.Error(), indexKey) {
			t.Errorf("%s: err = %v, want it to say %s and name the archive", name, err, tc.says)
		}
		if index.Name != "" || len(index.Devices) != 0 {
			t.Errorf("%s: an index came back with the error: %+v", name, index)
		}
		if object.closed != 1 {
			t.Errorf("%s: the stream was closed %d time(s)", name, object.closed)
		}
	}
}

// Incus itself refuses an index over 1 MiB (internal/server/backup/backup_info.go of v7.0.1); the worker holds no more
// of an archive in memory than that.
func TestReadIndex_AnIndexOfExactlyTheLimit_IsRead_AndOneByteMore_IsRefused(t *testing.T) {
	limit := 1 << 20
	padded := func(size int) []byte {
		padding := "\n# " + strings.Repeat("x", size-len(vmIndex)-3)
		return []byte(vmIndex + padding)
	}
	for name, tc := range map[string]struct {
		size int
		ok   bool
	}{"exactly the limit": {limit, true}, "one byte more": {limit + 1, false}} {
		object := &trackedObject{content: tarGz(t, tarEntry{"backup/index.yaml", padded(tc.size)})}

		index, err := ReadIndex(context.Background(), object, indexKey)

		if tc.ok != (err == nil) {
			t.Errorf("%s: err = %v, want success=%v", name, err, tc.ok)
		}
		if tc.ok && index.Name != "vmoriginal" {
			t.Errorf("%s: index: %+v", name, index)
		}
		if !tc.ok && (err == nil || !strings.Contains(err.Error(), "1048577") || !strings.Contains(err.Error(), indexKey)) {
			t.Errorf("%s: err = %v, want it to say the size and name the archive", name, err)
		}
		if object.closed != 1 {
			t.Errorf("%s: the stream was closed %d time(s)", name, object.closed)
		}
	}
}

func TestReadIndex_AnObjectThatCannotBeOpened_IsThatError(t *testing.T) {
	unreachable := errors.New("bucket unreachable")
	object := &trackedObject{getErr: unreachable}

	_, err := ReadIndex(context.Background(), object, indexKey)

	if !errors.Is(err, unreachable) || !strings.Contains(err.Error(), indexKey) {
		t.Fatalf("err = %v, want the bucket's error with the archive named", err)
	}
}
