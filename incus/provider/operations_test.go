package provider

import (
	"context"
	"crypto/tls"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/lxc/incus/v7/shared/api"
)

// The operations below are written as the v7.0.1 source renders them (internal/server/operations/operations.go,
// cmd/incusd/instances_post.go, cmd/incusd/storage_volumes.go); none is a recording of a server.
const (
	instanceImportRunning = `{"id":"op-vm-import","class":"task","description":"Restoring backup","status":"Running","status_code":103,
		"resources":{"instances":["/1.0/instances/vmcopy?project=t-x"]},"may_cancel":false,"err":"","location":"none"}`
	volumeImportRunning = `{"id":"op-volume-import","class":"task","description":"Restoring custom volume backup","status":"Running","status_code":103,
		"resources":{"storage_volumes":["/1.0/storage-pools/pool1/volumes/custom/volcopy?project=t-x"]},"may_cancel":false,"err":"","location":"none"}`
)

// operationsListing is the body of GET /1.0/operations?recursion=1: the operations under the lower-case name of
// their status (cmd/incusd/operations.go of v7.0.1).
func operationsListing(byStatus map[string][]string) string {
	groups := []string{}
	for _, status := range slices.Sorted(maps.Keys(byStatus)) {
		groups = append(groups, `"`+status+`":[`+strings.Join(byStatus[status], ",")+`]`)
	}
	return `{"type":"sync","status":"Success","status_code":200,"metadata":{` + strings.Join(groups, ",") + `}}`
}

// newOperationsTestServer answers the list of operations with listing, under the status given, and records each
// request for it.
func newOperationsTestServer(t *testing.T, status int, listing string) (*Incus, func() []recordedRequest) {
	t.Helper()
	var mu sync.Mutex
	var requests []recordedRequest
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/1.0/operations" {
			io.WriteString(w, `{"type":"sync","status":"Success","status_code":200,"metadata":{"api_extensions":[],"api_status":"stable","api_version":"1.0","auth":"trusted","public":false}}`)
			return
		}
		mu.Lock()
		requests = append(requests, recordedRequest{method: r.Method, path: r.URL.Path, query: r.URL.RawQuery})
		mu.Unlock()
		w.WriteHeader(status)
		io.WriteString(w, listing)
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAnyClientCert}
	server.StartTLS()
	t.Cleanup(server.Close)
	return connectTestServer(t, server), func() []recordedRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]recordedRequest{}, requests...)
	}
}

func idsOf(operations []api.Operation) []string {
	ids := []string{}
	for _, operation := range operations {
		ids = append(ids, operation.ID)
	}
	slices.Sort(ids)
	return ids
}

// An import the worker stopped waiting on goes on in Incus: its operation names the instance it makes, in the project
// it makes it in (titan-ops features/backup/design.md#restore-as-new-resource).
func TestListRunningInstanceOperations_AnswersTheImportIncusHasNotFinished_ReadAmongTheProjectsOperations(t *testing.T) {
	incus, requests := newOperationsTestServer(t, http.StatusOK, operationsListing(map[string][]string{
		"running": {instanceImportRunning, volumeImportRunning},
	}))

	running, err := incus.ListRunningInstanceOperations(context.Background(), "t-x", "vmcopy")

	if err != nil {
		t.Fatal(err)
	}
	if len(running) != 1 || running[0].ID != "op-vm-import" || running[0].Description != "Restoring backup" {
		t.Errorf("running: %+v, want the import alone", running)
	}
	seen := requests()
	if len(seen) != 1 || seen[0].method != http.MethodGet {
		t.Fatalf("requests: %+v, want one read of the operations", seen)
	}
	if query := seen[0].query; !strings.Contains(query, "project=t-x") || !strings.Contains(query, "recursion=1") {
		t.Errorf("query %q, want the operations themselves, of project t-x", query)
	}
}

func TestListRunningVolumeOperations_AnswersTheImportIncusHasNotFinished_OfTheVolumeInItsPool(t *testing.T) {
	incus, requests := newOperationsTestServer(t, http.StatusOK, operationsListing(map[string][]string{
		"running": {instanceImportRunning, volumeImportRunning},
	}))

	running, err := incus.ListRunningVolumeOperations(context.Background(), "t-x", "pool1", "volcopy")

	if err != nil {
		t.Fatal(err)
	}
	if len(running) != 1 || running[0].ID != "op-volume-import" || running[0].Description != "Restoring custom volume backup" {
		t.Errorf("running: %+v, want the import alone", running)
	}
	if seen := requests(); len(seen) != 1 || !strings.Contains(seen[0].query, "project=t-x") {
		t.Errorf("requests: %+v, want one read of the operations of project t-x", seen)
	}
	// A volume is its pool's: one of the same name in another pool is another volume.
	inAnotherPool, err := incus.ListRunningVolumeOperations(context.Background(), "t-x", "pool2", "volcopy")
	if err != nil || len(inAnotherPool) != 0 {
		t.Errorf("asked for the volume of pool2: %+v, %v; want none", inAnotherPool, err)
	}
}

// operationOn is an operation in the state given whose resources, under kind, are the paths given.
func operationOn(id string, statusCode api.StatusCode, kind string, paths ...string) string {
	quoted := []string{}
	for _, path := range paths {
		quoted = append(quoted, `"`+path+`"`)
	}
	return `{"id":"` + id + `","class":"task","description":"Restoring backup","status":"` + statusCode.String() + `","status_code":` +
		strconv.Itoa(int(statusCode)) + `,"resources":{"` + kind + `":[` + strings.Join(quoted, ",") + `]}}`
}

// Not finished is everything before an end: an operation Incus has made and not started yet, and one it is
// cancelling, are still at work on what they name. The default project's resources carry no "?project=".
func TestListRunningOperations_CountAnOperationThatHasNotEnded_WhateverItsStateOrTheKindItNamesTheObjectUnder(t *testing.T) {
	listing := operationsListing(map[string][]string{
		"pending":    {operationOn("op-pending", api.Pending, "instances", "/1.0/instances/vmcopy?project=t-x")},
		"cancelling": {operationOn("op-cancelling", api.Cancelling, "instances", "/1.0/instances/vmother?project=t-x", "/1.0/instances/vmcopy?project=t-x")},
		"running": {
			operationOn("op-default-project", api.Running, "instances", "/1.0/instances/vmcopy"),
			operationOn("op-another-kind", api.Running, "containers", "/1.0/instances/vmcopy?project=t-x"),
			operationOn("op-volume", api.Running, "storage_volumes", "/1.0/storage-pools/pool1/volumes/custom/volcopy"),
		},
	})
	incus, _ := newOperationsTestServer(t, http.StatusOK, listing)

	onInstance, err := incus.ListRunningInstanceOperations(context.Background(), "t-x", "vmcopy")
	if err != nil {
		t.Fatal(err)
	}
	onVolume, err := incus.ListRunningVolumeOperations(context.Background(), "t-x", "pool1", "volcopy")
	if err != nil {
		t.Fatal(err)
	}

	wantOnInstance := []string{"op-another-kind", "op-cancelling", "op-default-project", "op-pending"}
	if got := idsOf(onInstance); !slices.Equal(got, wantOnInstance) {
		t.Errorf("on the instance: %q, want %q", got, wantOnInstance)
	}
	if got := idsOf(onVolume); !slices.Equal(got, []string{"op-volume"}) {
		t.Errorf("on the volume: %q, want op-volume alone", got)
	}
}

// Incus keeps an operation for five seconds after it ends; one that names something else, or only a snapshot of the
// object, is not at work on the object.
func TestListRunningOperations_AnswerNone_WhenNoOperationThatHasNotEndedNamesTheObject(t *testing.T) {
	for name, byStatus := range map[string]map[string][]string{
		"no operation at all":      {},
		"the import succeeded":     {"success": {operationOn("op", api.Success, "instances", "/1.0/instances/vmcopy?project=t-x"), operationOn("op2", api.Success, "storage_volumes", "/1.0/storage-pools/pool1/volumes/custom/volcopy?project=t-x")}},
		"the import failed":        {"failure": {operationOn("op", api.Failure, "instances", "/1.0/instances/vmcopy?project=t-x"), operationOn("op2", api.Failure, "storage_volumes", "/1.0/storage-pools/pool1/volumes/custom/volcopy?project=t-x")}},
		"the import was cancelled": {"cancelled": {operationOn("op", api.Cancelled, "instances", "/1.0/instances/vmcopy?project=t-x")}},
		"a name that starts alike": {"running": {
			operationOn("op", api.Running, "instances", "/1.0/instances/vmcopy2?project=t-x"),
			operationOn("op2", api.Running, "storage_volumes", "/1.0/storage-pools/pool1/volumes/custom/volcopy2?project=t-x"),
		}},
		"a snapshot of it alone": {"running": {
			operationOn("op", api.Running, "instances_snapshots", "/1.0/instances/vmcopy/snapshots/backup?project=t-x"),
			operationOn("op2", api.Running, "storage_volume_snapshots", "/1.0/storage-pools/pool1/volumes/custom/volcopy/snapshots/backup?project=t-x"),
		}},
		"the volume's name in another pool":  {"running": {operationOn("op", api.Running, "storage_volumes", "/1.0/storage-pools/other/volumes/custom/volcopy?project=t-x")}},
		"a volume under the instance's name": {"running": {operationOn("op", api.Running, "storage_volumes", "/1.0/storage-pools/pool1/volumes/custom/vmcopy?project=t-x")}},
		"an operation that names nothing":    {"running": {`{"id":"op","class":"task","description":"Restoring backup","status":"Running","status_code":103}`}},
		"a resource that is no path":         {"running": {operationOn("op", api.Running, "instances", "%zz")}},
	} {
		incus, _ := newOperationsTestServer(t, http.StatusOK, operationsListing(byStatus))

		onInstance, err := incus.ListRunningInstanceOperations(context.Background(), "t-x", "vmcopy")
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		onVolume, err := incus.ListRunningVolumeOperations(context.Background(), "t-x", "pool1", "volcopy")
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}

		if len(onInstance) != 0 || len(onVolume) != 0 {
			t.Errorf("%s: on the instance %q, on the volume %q; want none", name, idsOf(onInstance), idsOf(onVolume))
		}
	}
}

// Operations that cannot be read are not "none": the caller must not take the object for free of any.
func TestListRunningOperations_AListIncusRefuses_IsAnError(t *testing.T) {
	refusal := `{"type":"error","status":"","status_code":0,"operation":"","error_code":403,"error":"not authorized","metadata":null}`
	incus, _ := newOperationsTestServer(t, http.StatusForbidden, refusal)

	onInstance, instanceErr := incus.ListRunningInstanceOperations(context.Background(), "t-x", "vmcopy")
	onVolume, volumeErr := incus.ListRunningVolumeOperations(context.Background(), "t-x", "pool1", "volcopy")

	for kind, err := range map[string]error{"instance": instanceErr, "volume": volumeErr} {
		if err == nil || !strings.Contains(err.Error(), "list the operations of project t-x") || !strings.Contains(err.Error(), "not authorized") {
			t.Errorf("%s: err = %v, want the refusal, naming the project", kind, err)
		}
	}
	if onInstance != nil || onVolume != nil {
		t.Errorf("answered %+v and %+v beside an error", onInstance, onVolume)
	}
}
