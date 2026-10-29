package vm

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

// Oracle for instanceName: python3 base32 of the UUID, as in provider/names_test.go.
const instanceName = "vmceircejceiztgrcekvkvkvkvku"

const (
	vmId      = "11111111-2222-3333-4444-555555555555"
	project   = "t-0123456789abcdef0123456789abcdef"
	clusterId = "mock"
	intentId  = "99999999-8888-7777-6666-555555555555"
)

func clusters(p *testsupport.Provider) cluster.Fixed {
	return cluster.Fixed{clusterId: {Provider: p, StoragePool: "pool1", OvnUplinkNetwork: "ovnup"}}
}

func leased(kind string, payload map[string]any) command.Leased {
	encoded, _ := json.Marshal(payload)
	return command.Leased{Kind: kind, ClusterId: clusterId, Payload: encoded, Attempt: 1, MaxAttempts: 5}
}

func createLeased(overrides map[string]any) command.Leased {
	payload := map[string]any{
		"vmId": vmId, "name": "web-1", "vmOfferingId": "m", "cpuUnits": 1.5, "vcpuCount": 2, "memoryMb": 2048,
		"templateId": "tpl-abc", "externalNetworkId": "net-1", "projectName": project,
		"diskSizeGb": 20, "userData": "",
	}
	for key, value := range overrides {
		payload[key] = value
	}
	return leased("vm.create", payload)
}

func onlyBody(t *testing.T, cp *testsupport.ControlPlane, prefix string) map[string]any {
	t.Helper()
	requests := cp.RequestsTo(prefix)
	if len(requests) != 1 {
		t.Fatalf("want one %s, got %d", prefix, len(requests))
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(requests[0].Body), &body); err != nil {
		t.Fatal(err)
	}
	return body
}

func TestCreate_BuildsTheInstance_AndReportsCreatedWithTheInstanceName(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), createLeased(nil), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if len(p.CreatedProjects) != 1 || p.CreatedProjects[0].Name != project || p.CreatedProjects[0].Config["features.images"] != "false" {
		t.Fatalf("project not ensured as the lab records it: %+v", p.CreatedProjects)
	}
	created := p.CreatedInstances[0]
	if created.Name != instanceName || created.Type != api.InstanceTypeVM || !created.Start || created.Source.Alias != "tpl-abc" {
		t.Fatalf("instance: %+v", created)
	}
	if created.Config["limits.cpu"] != "2" || created.Config["limits.memory"] != "2048MiB" || created.Config["user.titan-name"] != "web-1" {
		t.Fatalf("config: %v", created.Config)
	}
	root := created.Devices["root"]
	if root["pool"] != "pool1" || root["size"] != "20GiB" || root["path"] != "/" {
		t.Fatalf("root: %v", root)
	}
	if created.Devices["eth0"]["network"] != "net-1" {
		t.Fatalf("eth0: %v", created.Devices["eth0"])
	}
	body := onlyBody(t, cp, "PATCH /internal/vms/"+vmId+"/status")
	if body["report"] != "Created" || body["externalId"] != instanceName || body["status"] != nil {
		t.Fatalf("report: %v", body)
	}
}

func TestCreate_BareCloudConfigHeader_IsDropped(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), createLeased(map[string]any{"userData": "#cloud-config\n"}), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if _, has := p.CreatedInstances[0].Config["cloud-init.user-data"]; has {
		t.Fatal("an empty cloud-config fails the create on Incus (lab 04); it must not be sent")
	}
}

func TestCreate_CloudConfigWithContent_IsSent(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	userData := "#cloud-config\npackage_update: false\n"

	NewCreate(cp.Client(), clusters(p)).Run(context.Background(), createLeased(map[string]any{"userData": userData}), testsupport.DiscardLogger())

	if p.CreatedInstances[0].Config["cloud-init.user-data"] != userData {
		t.Fatal("user data with content was dropped")
	}
}

// Titan's own script (keys picked at create) travels apart from the customer's user-data, so both run
// (titan-ops features/vm/design.md#ssh-key). A script starts with "#!", which HasUserDataContent reads as a comment.
func TestCreate_VendorData_IsSentAsCloudInitVendorData(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	vendorData := "#!/bin/sh\necho keys\n"
	userData := "#cloud-config\npackage_update: false\n"

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(),
		createLeased(map[string]any{"vendorData": vendorData, "userData": userData}), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	config := p.CreatedInstances[0].Config
	if config["cloud-init.vendor-data"] != vendorData {
		t.Fatalf("vendor data = %q, want %q", config["cloud-init.vendor-data"], vendorData)
	}
	if config["cloud-init.user-data"] != userData {
		t.Fatal("the customer's user data must stay next to the vendor data")
	}
}

// An older control plane sends no field at all; a newer one sends "" when no key was picked.
func TestCreate_NoVendorData_SetsNoKey(t *testing.T) {
	for _, payload := range []map[string]any{{}, {"vendorData": ""}} {
		cp := testsupport.NewControlPlane(t)
		p := testsupport.NewProvider()

		err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), createLeased(payload), testsupport.DiscardLogger())

		if err != nil {
			t.Fatal(err)
		}
		if _, has := p.CreatedInstances[0].Config["cloud-init.vendor-data"]; has {
			t.Fatalf("payload %v: no vendor data was asked for, none may be set", payload)
		}
	}
}

func TestCreate_InstanceAlreadyExists_IsOurs_AndReportsCreated(t *testing.T) {
	logger, logs := testsupport.NewLogCapture(t)
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Projects[project] = true
	p.CreateInstanceErr = api.StatusErrorf(http.StatusInternalServerError, `Instance "%s" already exists`, instanceName)
	p.Instances[project+"/"+instanceName] = &api.Instance{Name: instanceName, StatusCode: api.Running}

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), createLeased(nil), logger)

	if err != nil {
		t.Fatal(err)
	}
	if len(p.StateChanges) != 0 {
		t.Fatalf("touched a running instance: %+v", p.StateChanges)
	}
	if len(p.CreatedProjects) != 0 {
		t.Fatal("created a project that exists")
	}
	if onlyBody(t, cp, "PATCH /internal/vms/")["report"] != "Created" {
		t.Fatal("want Created")
	}
	if got := logs.Objects("already exists; treating it as ours"); len(got) != 1 {
		t.Fatalf("want 1 line(s) %q, got %v", "already exists; treating it as ours", got)
	}
}

// An earlier run created the instance and died before it started: the rerun starts it, or the VM waits
// for Running for ever.
func TestCreate_InstanceAlreadyExistsButStopped_IsStarted_ThenReportsCreated(t *testing.T) {
	logger, logs := testsupport.NewLogCapture(t)
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Projects[project] = true
	p.CreateInstanceErr = api.StatusErrorf(http.StatusInternalServerError, `Instance "%s" already exists`, instanceName)
	p.Instances[project+"/"+instanceName] = &api.Instance{Name: instanceName, StatusCode: api.Stopped}

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), createLeased(nil), logger)

	if err != nil {
		t.Fatal(err)
	}
	if len(p.StateChanges) != 1 || p.StateChanges[0].Action != "start" {
		t.Fatalf("state changes: %+v", p.StateChanges)
	}
	if onlyBody(t, cp, "PATCH /internal/vms/")["report"] != "Created" {
		t.Fatal("want Created")
	}
	if got := logs.Objects("already exists; treating it as ours"); len(got) != 1 {
		t.Fatalf("want 1 line(s) %q, got %v", "already exists; treating it as ours", got)
	}
	if got := logs.Objects("found our instance stopped; starting it"); len(got) != 1 {
		t.Fatalf("want 1 line(s) %q, got %v", "found our instance stopped; starting it", got)
	}
}

func TestCreate_OperationFailure_IsRetryable_AndReportsNothing(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.CreateInstanceErr = &provider.OperationFailedError{Status: "Failure", Message: "injected create failure"}

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), createLeased(nil), testsupport.DiscardLogger())

	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want a retryable error, got %v", err)
	}
	if len(cp.Requests()) != 0 {
		t.Fatal("reported a create that failed")
	}
}

func TestCreate_GiveUp_CarriesTheIdOfAnInstanceThatExists_AndASanitizedReason(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Instances[project+"/"+instanceName] = &api.Instance{Name: instanceName}

	err := NewCreate(cp.Client(), clusters(p)).GiveUp(context.Background(), createLeased(nil), errors.New("raw: pod worker-7 op 42"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	body := onlyBody(t, cp, "PATCH /internal/vms/"+vmId+"/status")
	if body["externalId"] != instanceName || body["reason"] != reasonProvisionFailed || body["detail"] != "raw: pod worker-7 op 42" {
		t.Fatalf("give-up: %v", body)
	}
}

func actionLeased(kind string) command.Leased {
	return leased(kind, map[string]any{
		"vmId": vmId, "intentId": intentId, "externalVmId": instanceName, "projectName": project,
	})
}

func TestStop_AlreadyStopped_IsSuccess(t *testing.T) {
	logger, logs := testsupport.NewLogCapture(t)
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.UpdateStateErr = &provider.OperationFailedError{Status: "Failure", Message: "The instance is already stopped"}

	err := NewStop(cp.Client(), clusters(p)).Run(context.Background(), actionLeased("vm.stop"), logger)

	if err != nil {
		t.Fatalf("want success, got %v", err)
	}
	if p.StateChanges[0].Action != "stop" || !p.StateChanges[0].Force {
		t.Fatalf("want a forced stop, got %+v", p.StateChanges[0])
	}
	if got := logs.Objects("already in the asked state"); len(got) != 1 {
		t.Fatalf("want 1 line(s) %q, got %v", "already in the asked state", got)
	}
}

func TestStart_AlreadyRunning_IsSuccess(t *testing.T) {
	logger, logs := testsupport.NewLogCapture(t)
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Instances[project+"/"+instanceName] = &api.Instance{Name: instanceName, StatusCode: api.Running}
	p.UpdateStateErr = &provider.OperationFailedError{Status: "Failure", Message: "The instance is already running"}

	err := NewStart(cp.Client(), clusters(p)).Run(context.Background(), actionLeased("vm.start"), logger)

	if err != nil {
		t.Fatalf("want success, got %v", err)
	}
	if got := logs.Objects("already in the asked state"); len(got) != 1 {
		t.Fatalf("want 1 line(s) %q, got %v", "already in the asked state", got)
	}
}

// Incus answers action "start" on a Frozen instance with "already running" and leaves it Frozen; "unfreeze" is
// what resumes it (titan-ops features/vm/design.md#requests-follow-incus).
func TestStart_FrozenInstance_IsUnfrozen(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Instances[project+"/"+instanceName] = &api.Instance{Name: instanceName, StatusCode: api.Frozen}

	err := NewStart(cp.Client(), clusters(p)).Run(context.Background(), actionLeased("vm.start"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if len(p.StateChanges) != 1 || p.StateChanges[0].Action != "unfreeze" {
		t.Fatalf("want one unfreeze, got %+v", p.StateChanges)
	}
}

func TestStart_StoppedInstance_IsStarted(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Instances[project+"/"+instanceName] = &api.Instance{Name: instanceName, StatusCode: api.Stopped}

	err := NewStart(cp.Client(), clusters(p)).Run(context.Background(), actionLeased("vm.start"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if len(p.StateChanges) != 1 || p.StateChanges[0].Action != "start" {
		t.Fatalf("want one start, got %+v", p.StateChanges)
	}
}

func TestStart_InstanceGone_IsPermanent(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	err := NewStart(cp.Client(), clusters(p)).Run(context.Background(), actionLeased("vm.start"), testsupport.DiscardLogger())

	if err == nil || command.IsRetryable(err) {
		t.Fatalf("want a permanent error, got %v", err)
	}
	if len(p.StateChanges) != 0 {
		t.Fatalf("changed the state of an instance that is gone: %+v", p.StateChanges)
	}
}

func TestStop_InjectedFailure_IsRetryable(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.UpdateStateErr = &provider.OperationFailedError{Status: "Failure", Message: "injected stop failure"}

	err := NewStop(cp.Client(), clusters(p)).Run(context.Background(), actionLeased("vm.stop"), testsupport.DiscardLogger())

	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want retryable, got %v", err)
	}
}

func TestStop_GiveUp_WithdrawsTheIntent(t *testing.T) {
	cp := testsupport.NewControlPlane(t)

	err := NewStop(cp.Client(), clusters(testsupport.NewProvider())).GiveUp(context.Background(), actionLeased("vm.stop"), errors.New("x"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	requests := cp.RequestsTo("PATCH /internal/vms/" + vmId + "/intents/" + intentId + "/failed")
	if len(requests) != 1 || requests[0].Body != "" {
		t.Fatalf("want one bodyless intent-failed callback, got %+v", cp.Requests())
	}
}

func deleteLeased() command.Leased {
	return leased("vm.delete", map[string]any{"vmId": vmId, "externalVmId": instanceName, "projectName": project})
}

func TestDelete_InstanceGone_ReportsDeleted(t *testing.T) {
	logger, logs := testsupport.NewLogCapture(t)
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	err := NewDelete(cp.Client(), clusters(p)).Run(context.Background(), deleteLeased(), logger)

	if err != nil {
		t.Fatal(err)
	}
	if len(p.DeletedInstances) != 0 {
		t.Fatal("deleted an instance that is gone")
	}
	body := onlyBody(t, cp, "PATCH /internal/vms/"+vmId+"/status")
	if body["report"] != "Deleted" || body["status"] != nil {
		t.Fatalf("report: %v", body)
	}
	if got := logs.Objects("already gone; nothing to delete"); len(got) != 1 {
		t.Fatalf("want 1 line(s) %q, got %v", "already gone; nothing to delete", got)
	}
}

func TestDelete_RunningInstance_IsStoppedThenDeleted(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Instances[project+"/"+instanceName] = &api.Instance{Name: instanceName, StatusCode: api.Running}

	err := NewDelete(cp.Client(), clusters(p)).Run(context.Background(), deleteLeased(), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if len(p.StateChanges) != 1 || p.StateChanges[0].Action != "stop" || len(p.DeletedInstances) != 1 {
		t.Fatalf("want stop then delete, got %+v / %v", p.StateChanges, p.DeletedInstances)
	}
	if onlyBody(t, cp, "PATCH /internal/vms/")["report"] != "Deleted" {
		t.Fatal("want Deleted")
	}
}

// Incus deletes a frozen instance after a forced stop (titan-ops features/vm/design.md#requests-follow-incus): only Stopped skips the stop.
func TestDelete_FrozenInstance_IsStoppedThenDeleted(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Instances[project+"/"+instanceName] = &api.Instance{Name: instanceName, StatusCode: api.Frozen}

	err := NewDelete(cp.Client(), clusters(p)).Run(context.Background(), deleteLeased(), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	stoppedByForce := len(p.StateChanges) == 1 && p.StateChanges[0].Action == "stop" && p.StateChanges[0].Force
	if !stoppedByForce || len(p.DeletedInstances) != 1 {
		t.Fatalf("want a forced stop then delete, got %+v / %v", p.StateChanges, p.DeletedInstances)
	}
}

func TestDelete_DeleteAnswers404_ReportsDeleted(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Instances[project+"/"+instanceName] = &api.Instance{Name: instanceName, StatusCode: api.Stopped}
	p.DeleteInstanceErr = testsupport.NotFound("Instance not found")

	err := NewDelete(cp.Client(), clusters(p)).Run(context.Background(), deleteLeased(), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if len(p.StateChanges) != 0 {
		t.Fatal("stopped a stopped instance")
	}
	if onlyBody(t, cp, "PATCH /internal/vms/")["report"] != "Deleted" {
		t.Fatal("want Deleted")
	}
}

// Incus refusing the delete is worth another lease: the command is retried, and nothing is reported meanwhile.
func TestDelete_DeleteRefused_IsRetryable_AndReportsNothing(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.Instances[project+"/"+instanceName] = &api.Instance{Name: instanceName, StatusCode: api.Stopped}
	p.DeleteInstanceErr = api.StatusErrorf(http.StatusInternalServerError, "injected delete failure")

	err := NewDelete(cp.Client(), clusters(p)).Run(context.Background(), deleteLeased(), testsupport.DiscardLogger())

	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want retryable, got %v", err)
	}
	if len(cp.Requests()) != 0 {
		t.Fatalf("reported while the delete has not happened: %+v", cp.Requests())
	}
}

// A VM keeps its delete waiting when the command gives up (titan-ops architecture/desired-and-current.md#abandoned-request):
// the give-up tells the control plane nothing.
func TestDelete_GiveUp_SendsNothing(t *testing.T) {
	cp := testsupport.NewControlPlane(t)

	err := NewDelete(cp.Client(), clusters(testsupport.NewProvider())).GiveUp(context.Background(), deleteLeased(), errors.New("x"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if len(cp.Requests()) != 0 {
		t.Fatalf("want no callback, got %+v", cp.Requests())
	}
}

func TestStart_GiveUp_WithdrawsTheIntent(t *testing.T) {
	cp := testsupport.NewControlPlane(t)

	err := NewStart(cp.Client(), clusters(testsupport.NewProvider())).GiveUp(context.Background(), actionLeased("vm.start"), errors.New("x"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	requests := cp.RequestsTo("PATCH /internal/vms/" + vmId + "/intents/" + intentId + "/failed")
	if len(requests) != 1 || requests[0].Body != "" {
		t.Fatalf("want one bodyless intent-failed callback, got %+v", cp.Requests())
	}
}

func TestCreate_ClusterNotConfigured_IsPermanent(t *testing.T) {
	cp := testsupport.NewControlPlane(t)

	err := NewCreate(cp.Client(), cluster.Fixed{}).Run(context.Background(), createLeased(nil), testsupport.DiscardLogger())

	if err == nil || command.IsRetryable(err) || !errors.Is(err, cluster.ErrNoCluster) {
		t.Fatalf("want permanent ErrNoCluster, got %v", err)
	}
}

// Every VM sits on a tenant network; an instance created without its NIC would run unreachable while the control plane
// reports it Running.
func TestCreate_WithoutANetwork_IsPermanent_AndCreatesNothing(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	err := NewCreate(cp.Client(), cluster.Fixed{clusterId: {Provider: p}}).Run(
		context.Background(), createLeased(map[string]any{"externalNetworkId": ""}), testsupport.DiscardLogger())

	if err == nil || command.IsRetryable(err) {
		t.Fatalf("want a permanent error, got %v", err)
	}
	if len(p.Instances) != 0 {
		t.Fatal("created an instance without a network")
	}
}
