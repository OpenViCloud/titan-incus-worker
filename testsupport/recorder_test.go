package testsupport

import (
	"context"
	"io"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
)

// isRead says whether a method of the port only reads: the port names every read Get… or List….
func isRead(method string) bool {
	return strings.HasPrefix(method, "Get") || strings.HasPrefix(method, "List")
}

// callWithEmptyArguments calls one method of the Recorder with a context, an empty reader and zero values. The fake
// may panic on a zero value; the Recorder has kept the write by then, which is all this asks of it.
func callWithEmptyArguments(recorder *Recorder, name string) {
	defer func() { _ = recover() }()
	method := reflect.ValueOf(recorder).MethodByName(name)
	contextType := reflect.TypeFor[context.Context]()
	readerType := reflect.TypeFor[io.Reader]()
	arguments := []reflect.Value{}
	for i := range method.Type().NumIn() {
		parameter := method.Type().In(i)
		switch parameter {
		case contextType:
			arguments = append(arguments, reflect.ValueOf(context.Background()))
		case readerType:
			arguments = append(arguments, reflect.ValueOf(strings.NewReader("")))
		default:
			arguments = append(arguments, reflect.Zero(parameter))
		}
	}
	method.Call(arguments)
}

// A test that says "no write names the original" is only as good as the list of writes: a write of the port the
// Recorder does not keep would pass it unseen.
func TestRecorder_KeepsEveryWriteOfThePort_AndNoRead(t *testing.T) {
	port := reflect.TypeFor[provider.Provider]()
	if port.NumMethod() < 50 {
		t.Fatalf("read only %d methods of the port; the scan is not seeing it", port.NumMethod())
	}
	for i := range port.NumMethod() {
		name := port.Method(i).Name
		recorder := NewRecorder()

		callWithEmptyArguments(recorder, name)

		wantWrites := 1
		if isRead(name) {
			wantWrites = 0
		}
		if len(recorder.Writes) != wantWrites {
			t.Errorf("%s: kept %q, want %d write(s)", name, recorder.Writes, wantWrites)
		}
	}
}

func TestRecorder_NamesEachWriteByItsVerbItsProjectAndItsObject_InCallOrder(t *testing.T) {
	recorder := NewRecorder()
	ctx := context.Background()
	recorder.Volumes["p1/vol1"] = &api.StorageVolume{Name: "vol1"}

	_ = recorder.CreateVolume(ctx, "p1", "pool1", api.StorageVolumesPost{Name: "vol2"})
	_ = recorder.ImportVolume(ctx, "p1", "pool1", "vol3", strings.NewReader(ArchiveBody("backup")))
	_ = recorder.RestoreVolume(ctx, "p1", "pool1", "vol3", "backup")
	_ = recorder.DeleteVolumeSnapshot(ctx, "p1", "pool1", "vol3", "backup")
	_ = recorder.PatchVolumeConfig(ctx, "p1", "pool1", "vol1", map[string]string{"user.titan-name": "data"})
	_ = recorder.ImportInstance(ctx, "p1", "pool1", "vm1", strings.NewReader(ArchiveBody("backup")), provider.InstanceOverrides{})
	_ = recorder.UpdateInstance(ctx, "p1", "vm1", api.InstancePut{Restore: "backup", DiskOnly: true}, "")
	_ = recorder.DeleteInstanceSnapshot(ctx, "p1", "vm1", "backup")
	_ = recorder.UpdateInstance(ctx, "p1", "vm1", api.InstancePut{}, "")
	_ = recorder.PatchInstance(ctx, "p1", "vm1", api.InstancePut{})
	_ = recorder.UpdateInstanceState(ctx, "p1", "vm1", api.InstanceStatePut{Action: "start"})
	_ = recorder.DeleteInstance(ctx, "p1", "vm1")
	_ = recorder.DeleteVolume(ctx, "p1", "pool1", "vol2")
	_ = recorder.DeleteProject(ctx, "p1")

	want := []string{
		"create-volume p1/vol2",
		"import-volume p1/vol3",
		"rollback-volume p1/vol3/backup",
		"delete-volume-snapshot p1/vol3/backup",
		"patch-volume p1/vol1",
		"import-vm p1/vm1",
		"rollback-vm p1/vm1/backup",
		"delete-vm-snapshot p1/vm1/backup",
		"update-vm p1/vm1",
		"patch-vm p1/vm1",
		"start-vm p1/vm1",
		"delete-vm p1/vm1",
		"delete-volume p1/vol2",
		"delete-project /p1",
	}
	if !slices.Equal(recorder.Writes, want) {
		t.Errorf("writes\n got %q\nwant %q", recorder.Writes, want)
	}
}

// The write reaches the fake: a test reads what it changed there.
func TestRecorder_PassesEachWriteOnToTheFake(t *testing.T) {
	recorder := NewRecorder()

	err := recorder.CreateVolume(context.Background(), "p1", "pool1", api.StorageVolumesPost{Name: "vol1"})

	if err != nil {
		t.Fatal(err)
	}
	if _, made := recorder.Volumes["p1/vol1"]; !made || len(recorder.CreatedVolumes) != 1 {
		t.Errorf("the fake did not get the write: volumes %v, created %+v", recorder.Volumes, recorder.CreatedVolumes)
	}
}
