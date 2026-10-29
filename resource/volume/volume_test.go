package volume

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

const (
	volumeId = "11111111-2222-3333-4444-555555555555"
	// Oracle: python3 base32 of volumeId, as in provider/names_test.go.
	volumeName = "volceircejceiztgrcekvkvkvkvku"
	project    = "t-0123456789abcdef0123456789abcdef"
	clusterId  = "mock"
	instance   = "vmaaaa"
)

func clusters(p *testsupport.Provider, pool string) cluster.Fixed {
	return cluster.Fixed{clusterId: {Provider: p, StoragePool: pool}}
}

func leased(kind string, payload map[string]any) command.Leased {
	encoded, _ := json.Marshal(payload)
	return command.Leased{Kind: kind, ClusterId: clusterId, Payload: encoded, Attempt: 1, MaxAttempts: 5}
}

func createLeased() command.Leased {
	return leased("volume.create", map[string]any{
		"volumeId": volumeId, "name": "data-1", "sizeGb": 20, "projectName": project,
	})
}

func attachLeased(kind string) command.Leased {
	return leased(kind, map[string]any{
		"volumeId": volumeId, "externalVolumeId": volumeName, "externalVmId": instance, "projectName": project,
	})
}

func onlyBody(t *testing.T, cp *testsupport.ControlPlane, prefix string) map[string]any {
	t.Helper()
	requests := cp.RequestsTo(prefix)
	if len(requests) != 1 {
		t.Fatalf("want one %s, got %d: %+v", prefix, len(requests), cp.Requests())
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(requests[0].Body), &body); err != nil {
		t.Fatal(err)
	}
	return body
}

const statusPath = "PATCH /internal/volumes/" + volumeId + "/status"

const usedByInstance = "/1.0/instances/" + instance + "?project=" + project

// attachedTo reads a report's instance list; an absent list is no instance.
func attachedTo(body map[string]any) []string {
	names := []string{}
	list, _ := body["attachedTo"].([]any)
	for _, name := range list {
		names = append(names, name.(string))
	}
	return names
}

func TestCreate_MakesTheBlockVolumeInTheTenantsProject_AndReportsCreatedWithItsName(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	err := NewCreate(cp.Client(), clusters(p, "")).Run(context.Background(), createLeased(), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if !p.Projects[project] {
		t.Fatal("the tenant's project was not ensured")
	}
	if len(p.CreatedVolumes) != 1 {
		t.Fatalf("want one volume, got %+v", p.CreatedVolumes)
	}
	created := p.CreatedVolumes[0]
	isAsAsked := created.Name == volumeName && created.Type == "custom" && created.ContentType == "block" &&
		created.Config["size"] == "20GiB" && created.Config[DisplayNameKey] == "data-1"
	if !isAsAsked {
		t.Fatalf("volume: %+v", created)
	}
	body := onlyBody(t, cp, statusPath)
	if body["report"] != "Created" || body["externalId"] != volumeName {
		t.Fatalf("report: %v", body)
	}
}

// A second lease of the same create (the first one's lease ran out) finds the volume made: one volume, reported.
func TestCreate_VolumeAlreadyThere_ReportsCreated(t *testing.T) {
	logger, logs := testsupport.NewLogCapture(t)
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.CreateVolumeErr = api.StatusErrorf(http.StatusConflict, "Volume by that name already exists")

	err := NewCreate(cp.Client(), clusters(p, "")).Run(context.Background(), createLeased(), logger)

	if err != nil {
		t.Fatalf("a duplicate must not fail the create: %v", err)
	}
	body := onlyBody(t, cp, statusPath)
	if body["report"] != "Created" || body["externalId"] != volumeName {
		t.Fatalf("want Created with the volume's name, got %v", body)
	}
	if got := logs.Objects("already exists; treating it as ours"); len(got) != 1 {
		t.Fatalf("want 1 line(s) %q, got %v", "already exists; treating it as ours", got)
	}
}

func TestCreate_IncusError_FailsTheRunAndReportsNothing(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.CreateVolumeErr = api.StatusErrorf(http.StatusInternalServerError, "injected create failure")

	err := NewCreate(cp.Client(), clusters(p, "")).Run(context.Background(), createLeased(), testsupport.DiscardLogger())

	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want a retryable error, got %v", err)
	}
	if len(cp.RequestsTo("PATCH")) != 0 {
		t.Fatal("reported on a failed create")
	}
}

func TestCreate_PayloadWithoutSize_IsPermanent(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	payload := leased("volume.create", map[string]any{"volumeId": volumeId, "name": "d", "projectName": project})

	err := NewCreate(cp.Client(), clusters(testsupport.NewProvider(), "")).Run(context.Background(), payload, testsupport.DiscardLogger())

	if err == nil || command.IsRetryable(err) {
		t.Fatalf("want a permanent error, got %v", err)
	}
}

// The tenant reads the fixed reason; the raw error goes to the operator's detail.
func TestCreate_GiveUp_ReportsRequestFailed_CarryingTheNameOnlyWhenTheVolumeExists(t *testing.T) {
	for _, exists := range []bool{true, false} {
		cp := testsupport.NewControlPlane(t)
		p := testsupport.NewProvider()
		if exists {
			p.Volumes[project+"/"+volumeName] = &api.StorageVolume{Name: volumeName}
		}

		err := NewCreate(cp.Client(), clusters(p, "")).GiveUp(context.Background(), createLeased(), errors.New("raw: lvcreate failed"), testsupport.DiscardLogger())

		if err != nil {
			t.Fatal(err)
		}
		body := onlyBody(t, cp, statusPath)
		wantId := ""
		if exists {
			wantId = volumeName
		}
		gotId, _ := body["externalId"].(string)
		isTheGiveUp := body["report"] == "RequestFailed" && body["reason"] == ReasonCreateFailed &&
			body["detail"] == "raw: lvcreate failed" && gotId == wantId
		if !isTheGiveUp {
			t.Fatalf("exists=%v: report %v", exists, body)
		}
	}
}

func usedVolume(users ...string) *api.StorageVolume {
	return &api.StorageVolume{Name: volumeName, Type: "custom", Project: project, UsedBy: users}
}

// The attach reports what Incus then shows: the instances in the volume's used_by, for the control plane to match to
// its VMs (titan-ops features/volume/design.md#attachment-from-used-by).
func TestAttach_PatchesADiskNamedAfterTheVolume_InTheClustersPool_AndReportsTheInstancesUsingIt(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Volumes[project+"/"+volumeName] = usedVolume(usedByInstance)

	err := NewAttach(cp.Client(), clusters(p, "pool1")).Run(context.Background(), attachLeased("volume.attach"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if len(p.PatchedInstances) != 1 {
		t.Fatalf("want one PATCH, got %+v", p.PatchedInstances)
	}
	device := p.PatchedInstances[0].Devices[volumeName]
	if device["type"] != "disk" || device["source"] != volumeName || device["pool"] != "pool1" || device["path"] != "" {
		t.Fatalf("device: %v", device)
	}
	body := onlyBody(t, cp, statusPath)
	names := attachedTo(body)
	if body["report"] != "Observed" || body["exists"] != true || len(names) != 1 || names[0] != instance {
		t.Fatalf("report: %v", body)
	}
}

func TestAttach_VmGone_IsPermanent_AndGiveUpReportsRequestFailed_WithWhatIncusShows(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.PatchInstanceErr = testsupport.NotFound("Instance not found")
	p.Volumes[project+"/"+volumeName] = usedVolume()
	handler := NewAttach(cp.Client(), clusters(p, ""))

	err := handler.Run(context.Background(), attachLeased("volume.attach"), testsupport.DiscardLogger())
	if err == nil || command.IsRetryable(err) {
		t.Fatalf("want a permanent error, got %v", err)
	}
	if err := handler.GiveUp(context.Background(), attachLeased("volume.attach"), err, testsupport.DiscardLogger()); err != nil {
		t.Fatal(err)
	}

	body := onlyBody(t, cp, statusPath)
	isTheGiveUp := body["report"] == "RequestFailed" && body["reason"] == reasonAttachFailed && body["exists"] == true &&
		len(attachedTo(body)) == 0 && body["detail"] != nil
	if !isTheGiveUp {
		t.Fatalf("report: %v", body)
	}
}

func TestDetach_RemovesOnlyThisVolumesDisk_FromTheInstanceUsingIt_AndReportsWhatIncusThenShows(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Volumes[project+"/"+volumeName] = usedVolume(usedByInstance)
	p.Instances[project+"/"+instance] = &api.Instance{Name: instance, InstancePut: api.InstancePut{Devices: map[string]map[string]string{
		"root":     {"type": "disk", "pool": "default", "path": "/"},
		volumeName: {"type": "disk", "pool": "default", "source": volumeName},
		"other":    {"type": "disk", "pool": "default", "source": "volother"},
	}}}
	// Incus drops the instance from used_by once its device is gone; the fake volume does not follow by itself.
	p.AfterUpdateInstance = func() { p.Volumes[project+"/"+volumeName] = usedVolume() }

	err := NewDetach(cp.Client(), clusters(p, "")).Run(context.Background(), attachLeased("volume.detach"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if len(p.UpdatedInstances) != 1 {
		t.Fatalf("want one PUT, got %+v", p.UpdatedInstances)
	}
	devices := p.UpdatedInstances[0].Devices
	_, stillThere := devices[volumeName]
	_, rootKept := devices["root"]
	_, otherKept := devices["other"]
	if stillThere || !rootKept || !otherKept {
		t.Fatalf("devices after detach: %v", devices)
	}
	body := onlyBody(t, cp, statusPath)
	if body["report"] != "Observed" || body["exists"] != true || len(attachedTo(body)) != 0 {
		t.Fatalf("report: %v", body)
	}
}

// A repeat after the device is gone (lab-storage 04b) changes nothing and reports the same.
func TestDetach_VolumeUsedByNoInstance_ReportsItUnattachedWithoutAPut(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Volumes[project+"/"+volumeName] = usedVolume()

	err := NewDetach(cp.Client(), clusters(p, "")).Run(context.Background(), attachLeased("volume.detach"), testsupport.DiscardLogger())

	if err != nil || len(p.UpdatedInstances) != 0 {
		t.Fatalf("err %v, PUTs %+v", err, p.UpdatedInstances)
	}
	body := onlyBody(t, cp, statusPath)
	if body["report"] != "Observed" || body["exists"] != true || len(attachedTo(body)) != 0 {
		t.Fatalf("report: %v", body)
	}
}

// An attach or detach that happened while its report failed five times: the give-up carries what Incus shows, which
// the control plane applies before it withdraws anything.
func TestGiveUps_CarryWhatTheVolumesUsedByShows(t *testing.T) {
	cases := []struct {
		kind       string
		usedBy     []string
		wantReason string
		wantNames  int
	}{
		{"volume.attach", []string{usedByInstance}, reasonAttachFailed, 1},
		{"volume.attach", nil, reasonAttachFailed, 0},
		{"volume.detach", nil, reasonDetachFailed, 0},
		{"volume.detach", []string{usedByInstance}, reasonDetachFailed, 1},
	}
	for _, c := range cases {
		cp := testsupport.NewControlPlane(t)
		p := testsupport.NewProvider()
		p.Volumes[project+"/"+volumeName] = usedVolume(c.usedBy...)
		var handler command.Handler = NewAttach(cp.Client(), clusters(p, ""))
		if c.kind == "volume.detach" {
			handler = NewDetach(cp.Client(), clusters(p, ""))
		}

		err := handler.GiveUp(context.Background(), attachLeased(c.kind), errors.New("callback refused"), testsupport.DiscardLogger())

		if err != nil {
			t.Fatal(err)
		}
		body := onlyBody(t, cp, statusPath)
		isTheGiveUp := body["report"] == "RequestFailed" && body["reason"] == c.wantReason &&
			body["detail"] == "callback refused" && body["exists"] == true && len(attachedTo(body)) == c.wantNames
		if !isTheGiveUp {
			t.Errorf("%s with used_by %v: report %v", c.kind, c.usedBy, body)
		}
	}
}

// used_by cannot be read: the give-up says nothing about the attachment, only that the request failed.
func TestGiveUp_UnreadableVolume_ReportsTheFailureAlone(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.GetVolumeErr = errors.New("connection refused")

	err := NewDetach(cp.Client(), clusters(p, "")).GiveUp(context.Background(), attachLeased("volume.detach"), errors.New("x"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body := onlyBody(t, cp, statusPath)
	_, carriesExists := body["exists"]
	if body["report"] != "RequestFailed" || carriesExists {
		t.Fatalf("report: %v", body)
	}
}

// The volume is gone: the give-up says so, and the control plane records NotFound.
func TestGiveUp_VolumeGone_ReportsItNotFound(t *testing.T) {
	cp := testsupport.NewControlPlane(t)

	err := NewDetach(cp.Client(), clusters(testsupport.NewProvider(), "")).GiveUp(context.Background(), attachLeased("volume.detach"), errors.New("x"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body := onlyBody(t, cp, statusPath)
	if body["report"] != "RequestFailed" || body["exists"] != false {
		t.Fatalf("report: %v", body)
	}
}

func TestInstancesUsing_ReadsInstancesAndTheirProject_SkipsEverythingElse(t *testing.T) {
	refs := InstancesUsing([]string{
		"/1.0/instances/vma?project=t-1",
		"/1.0/instances/vmb",
		"/1.0/instances/vma/snapshots/s1?project=t-1",
		"/1.0/profiles/default",
	})

	want := []InstanceRef{{Project: "t-1", Name: "vma"}, {Project: "default", Name: "vmb"}}
	if len(refs) != len(want) || refs[0] != want[0] || refs[1] != want[1] {
		t.Fatalf("got %+v", refs)
	}
}

func deleteLeased() command.Leased {
	return leased("volume.delete", map[string]any{
		"volumeId": volumeId, "externalVolumeId": volumeName, "projectName": project,
	})
}

func TestDelete_VolumeAlreadyGone_ReportsDeleted(t *testing.T) {
	logger, logs := testsupport.NewLogCapture(t)
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.DeleteVolumeErr = testsupport.NotFound("Storage pool volume not found")

	err := NewDelete(cp.Client(), clusters(p, "")).Run(context.Background(), deleteLeased(), logger)

	if err != nil || onlyBody(t, cp, statusPath)["report"] != "Deleted" {
		t.Fatalf("err %v", err)
	}
	if got := logs.Objects("already gone; nothing to delete"); len(got) != 1 {
		t.Fatalf("want 1 line(s) %q, got %v", "already gone; nothing to delete", got)
	}
}

func TestDelete_StillInUse_IsRetried_AndGiveUpReportsRequestFailed(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.DeleteVolumeErr = api.StatusErrorf(http.StatusBadRequest, "The storage volume is still in use")
	handler := NewDelete(cp.Client(), clusters(p, ""))

	err := handler.Run(context.Background(), deleteLeased(), testsupport.DiscardLogger())
	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want a retryable error, got %v", err)
	}
	if len(cp.RequestsTo("PATCH")) != 0 {
		t.Fatal("reported a delete that did not happen")
	}
	if err := handler.GiveUp(context.Background(), deleteLeased(), errors.New("still in use"), testsupport.DiscardLogger()); err != nil {
		t.Fatal(err)
	}

	body := onlyBody(t, cp, statusPath)
	if body["report"] != "RequestFailed" || body["reason"] != reasonDeleteFailed || body["detail"] != "still in use" {
		t.Fatalf("report: %v", body)
	}
}

func TestDecideVolumeReport(t *testing.T) {
	observed := ObserveVolumes([]api.StorageVolume{
		{Name: "vola", Type: "custom", Project: project},
		{Name: "volb", Type: "custom", Project: project, UsedBy: []string{"/1.0/instances/vma?project=" + project}},
		{Name: "volc", Type: "custom", Project: project, UsedBy: []string{
			"/1.0/instances/vma?project=" + project, "/1.0/instances/foreign?project=" + project}},
		{Name: "vma", Type: "virtual-machine", Project: project},
	})
	cases := []struct {
		name      string
		row       ReconcilableVolume
		wantSend  bool
		wantExist bool
		wantNames []string
	}{
		{"seen first", ReconcilableVolume{ExternalId: "vola", CurrentExistence: "NotCreated"}, true, true, nil},
		{"unchanged", ReconcilableVolume{ExternalId: "vola", CurrentExistence: "Exists"}, false, true, nil},
		{"attached outside Titan's knowledge", ReconcilableVolume{ExternalId: "volb", CurrentExistence: "Exists"}, true, true, []string{"vma"}},
		{"attachment unchanged", ReconcilableVolume{ExternalId: "volb", CurrentExistence: "Exists", CurrentAttachedExternalVmId: "vma"}, false, true, []string{"vma"}},
		{"detached", ReconcilableVolume{ExternalId: "vola", CurrentExistence: "Exists", CurrentAttachedExternalVmId: "vma"}, true, true, nil},
		{"moved to another instance", ReconcilableVolume{ExternalId: "volb", CurrentExistence: "Exists", CurrentAttachedExternalVmId: "vmz"}, true, true, []string{"vma"}},
		// The worker cannot tell an instance outside Titan from another Titan VM: a row recorded elsewhere is always sent.
		{"elsewhere, always sent", ReconcilableVolume{ExternalId: "volc", CurrentExistence: "Exists", CurrentAttachedExternalVmId: "vma", CurrentAttachedElsewhere: true}, true, true, []string{"vma", "foreign"}},
		{"gone", ReconcilableVolume{ExternalId: "volgone", CurrentExistence: "Exists"}, true, false, nil},
		{"gone, already recorded", ReconcilableVolume{ExternalId: "volgone", CurrentExistence: "NotFound"}, false, false, nil},
		{"found again", ReconcilableVolume{ExternalId: "vola", CurrentExistence: "NotFound"}, true, true, nil},
		// An instance's own volume is not a Titan volume of that name.
		{"instance volume", ReconcilableVolume{ExternalId: "vma", CurrentExistence: "Exists"}, true, false, nil},
	}
	for _, c := range cases {
		report, send := DecideVolumeReport(c.row, observed)
		if send != c.wantSend {
			t.Errorf("%s: send %v, want %v", c.name, send, c.wantSend)
			continue
		}
		if !send {
			continue
		}
		isAsWanted := report.Report == "Observed" && report.Exists != nil && *report.Exists == c.wantExist &&
			strings.Join(report.AttachedTo, ",") == strings.Join(c.wantNames, ",")
		if !isAsWanted {
			t.Errorf("%s: report %+v", c.name, report)
		}
	}
}

func TestDecideSnapshotReport(t *testing.T) {
	observed := ObserveVolumes([]api.StorageVolume{{Name: "vola", Type: "custom", Project: project}, {Name: "volunread", Type: "custom", Project: project}})
	read := map[string]map[string]bool{"vola": {"sone": true}}
	cases := []struct {
		name      string
		row       ReconcilableVolumeSnapshot
		wantSend  bool
		wantExist bool
	}{
		{"seen first", ReconcilableVolumeSnapshot{ExternalId: "vola/sone", CurrentExistence: "NotCreated"}, true, true},
		{"unchanged", ReconcilableVolumeSnapshot{ExternalId: "vola/sone", CurrentExistence: "Exists"}, false, true},
		{"gone from its volume", ReconcilableVolumeSnapshot{ExternalId: "vola/stwo", CurrentExistence: "Exists"}, true, false},
		{"gone, already recorded", ReconcilableVolumeSnapshot{ExternalId: "vola/stwo", CurrentExistence: "NotFound"}, false, false},
		{"its volume gone", ReconcilableVolumeSnapshot{ExternalId: "volgone/sone", CurrentExistence: "Exists"}, true, false},
		{"its volume gone, already recorded", ReconcilableVolumeSnapshot{ExternalId: "volgone/sone", CurrentExistence: "NotFound"}, false, false},
		{"snapshots not read this cycle", ReconcilableVolumeSnapshot{ExternalId: "volunread/sone", CurrentExistence: "Exists"}, false, false},
		{"not a snapshot id", ReconcilableVolumeSnapshot{ExternalId: "not-a-snapshot-id", CurrentExistence: "Exists"}, false, false},
	}
	for _, c := range cases {
		report, send := DecideSnapshotReport(c.row, observed, read)
		if send != c.wantSend {
			t.Errorf("%s: send %v, want %v", c.name, send, c.wantSend)
			continue
		}
		if send && (report.Report != "Observed" || report.Exists == nil || *report.Exists != c.wantExist) {
			t.Errorf("%s: report %+v", c.name, report)
		}
	}
}

// Reconciler never reports Deleted: a volume gone is NotFound, whatever was asked
// (titan-ops architecture/desired-and-current.md#existence-axis).
func TestReconcile_ReadsTheSnapshotsOfAVolumeOnce_AndReportsEachChange_NeverDeleted(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond("GET", "/internal/reconcile/volumes?clusterId=mock", 200, `{
		"volumes":[
			{"id":"v1","externalId":"vola","currentExistence":"NotCreated","currentAttachedExternalVmId":null,"currentAttachedElsewhere":false},
			{"id":"v2","externalId":"volgone","currentExistence":"Exists","currentAttachedExternalVmId":null,"currentAttachedElsewhere":false}],
		"snapshots":[
			{"id":"s1","volumeId":"v1","externalId":"vola/sone","currentExistence":"NotCreated"},
			{"id":"s2","volumeId":"v1","externalId":"vola/stwo","currentExistence":"Exists"}]}`)
	p := testsupport.NewProvider()
	p.AllVolumes = []api.StorageVolume{{Name: "vola", Type: "custom", Project: project, UsedBy: []string{usedByInstance}}}
	p.VolumeSnapshots[project+"/vola/sone"] = true
	logger, logs := testsupport.NewLogCapture(t)

	err := Reconcile(context.Background(), cp.Client(), p, clusterId, "", logger)

	if err != nil {
		t.Fatal(err)
	}
	// One pool read and one snapshot read for vola, though two of its snapshots are listed.
	if p.Calls != 2 {
		t.Fatalf("want 2 provider reads, got %d", p.Calls)
	}
	reports := map[string]map[string]any{}
	for _, request := range cp.RequestsTo("PATCH") {
		var body map[string]any
		_ = json.Unmarshal([]byte(request.Body), &body)
		reports[request.Path] = body
	}
	want := map[string]bool{
		"/internal/volumes/v1/status":              true,
		"/internal/volumes/v2/status":              false,
		"/internal/volumes/v1/snapshots/s1/status": true,
		"/internal/volumes/v1/snapshots/s2/status": false,
	}
	if len(reports) != len(want) {
		t.Fatalf("reports %v, want %v", reports, want)
	}
	for path, exists := range want {
		body := reports[path]
		if body["report"] != "Observed" || body["exists"] != exists {
			t.Errorf("%s: %v, want Observed exists=%v", path, body, exists)
		}
	}
	if names := attachedTo(reports["/internal/volumes/v1/status"]); len(names) != 1 || names[0] != instance {
		t.Errorf("v1 attachedTo %v", names)
	}
	volumeLines := map[any]map[string]any{}
	snapshotLines := map[any]map[string]any{}
	for _, sent := range logs.Records("reconcile report sent") {
		_, isSnapshot := sent["snapshotId"]
		if isSnapshot {
			snapshotLines[sent["snapshotId"]] = sent
			continue
		}
		volumeLines[sent["volumeId"]] = sent
	}
	isV1Logged := volumeLines["v1"]["recordedExistence"] == "NotCreated" && volumeLines["v1"]["exists"] == true
	isV2Logged := volumeLines["v2"]["recordedExistence"] == "Exists" && volumeLines["v2"]["exists"] == false
	if len(volumeLines) != 2 || !isV1Logged || !isV2Logged {
		t.Errorf("volume report lines: %v", volumeLines)
	}
	isS1Logged := snapshotLines["s1"]["recordedExistence"] == "NotCreated" && snapshotLines["s1"]["exists"] == true
	isS2Logged := snapshotLines["s2"]["recordedExistence"] == "Exists" && snapshotLines["s2"]["exists"] == false
	if len(snapshotLines) != 2 || !isS1Logged || !isS2Logged {
		t.Errorf("snapshot report lines: %v", snapshotLines)
	}
}

func TestReconcile_FailedPoolRead_ReportsNothing(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond("GET", "/internal/reconcile/volumes?clusterId=mock", 200,
		`{"volumes":[{"id":"v1","externalId":"vola","currentExistence":"Exists"}],"snapshots":[]}`)
	p := testsupport.NewProvider()
	p.ListVolumesErr = errors.New("connection refused")

	err := Reconcile(context.Background(), cp.Client(), p, clusterId, "", slog.Default())

	if err == nil || len(cp.RequestsTo("PATCH")) != 0 {
		t.Fatalf("err %v, reports %+v", err, cp.RequestsTo("PATCH"))
	}
}
