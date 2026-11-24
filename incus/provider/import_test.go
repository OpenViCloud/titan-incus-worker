package provider

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// These tests read the request the adapter builds off a server that answers like Incus; none replays a lab recording.

type recordedRequest struct {
	method string
	path   string
	query  string
	header http.Header
	body   string
}

// newImportTestServer advertises the API extensions the client asks for before it imports an archive under a pool, a
// name and overrides (client/incus_instances.go of v7.0.1), answers an import with an operation that then succeeds, and
// any other write with a bare success. It records every request but the client's own reads of /1.0 and of the operation.
func newImportTestServer(t *testing.T) (*Incus, func() []recordedRequest) {
	t.Helper()
	var mu sync.Mutex
	var requests []recordedRequest
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/1.0" {
			io.WriteString(w, `{"type":"sync","status":"Success","status_code":200,"metadata":{"api_extensions":["container_backup","container_backup_override_pool","backup_override_name","backup_override_config"],"api_status":"stable","api_version":"1.0","auth":"trusted","public":false}}`)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/1.0/operations/op1") {
			io.WriteString(w, `{"type":"sync","status":"Success","status_code":200,"metadata":{"id":"op1","status":"Success","status_code":200}}`)
			return
		}
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		requests = append(requests, recordedRequest{method: r.Method, path: r.URL.Path, query: r.URL.RawQuery, header: r.Header.Clone(), body: string(body)})
		mu.Unlock()
		isImport := r.Method == http.MethodPost && r.URL.Path == "/1.0/instances"
		if isImport {
			w.WriteHeader(http.StatusAccepted)
			io.WriteString(w, `{"type":"async","status":"Operation created","status_code":100,"operation":"/1.0/operations/op1","metadata":{"id":"op1","status":"Running","status_code":103}}`)
			return
		}
		io.WriteString(w, `{"type":"sync","status":"Success","status_code":200,"metadata":null}`)
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAnyClientCert}
	server.StartTLS()
	t.Cleanup(server.Close)
	sum := sha256.Sum256(server.Certificate().Raw)
	certificatePem, keyPem := clientIdentity(t)
	connected, err := Connect(Credentials{Endpoint: server.URL, ServerFingerprint: hex.EncodeToString(sum[:]),
		ClientCertificatePem: certificatePem, ClientKeyPem: keyPem})
	if err != nil {
		t.Fatal(err)
	}
	return NewIncus(connected), func() []recordedRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]recordedRequest{}, requests...)
	}
}

// Incus reads the overrides from two headers, entries joined by one space (cmd/incusd/instances_post.go of v7.0.1), and
// applies them to the archived instance before it creates it (titan-ops features/backup/design.md#restore-as-new-resource).
func TestImportInstance_WithOverrides_SendsThemInTheDevicesAndConfigHeaders(t *testing.T) {
	incus, requests := newImportTestServer(t)
	overrides := InstanceOverrides{
		Devices: []string{"voloriginal,source=volcopy", "eth0,ipv4.address="},
		Config:  []string{"volatile.uuid=", "volatile.eth0.hwaddr="},
	}

	err := incus.ImportInstance(context.Background(), "t-x", "pool1", "vmcopy", strings.NewReader("the archive"), overrides)

	if err != nil {
		t.Fatal(err)
	}
	seen := requests()
	if len(seen) != 1 {
		t.Fatalf("want one import request, got %+v", seen)
	}
	sent := seen[0]
	if sent.method != http.MethodPost || sent.path != "/1.0/instances" || sent.query != "project=t-x" {
		t.Fatalf("request: %s %s?%s", sent.method, sent.path, sent.query)
	}
	if got := sent.header.Values("X-Incus-devices"); len(got) != 1 || got[0] != "voloriginal,source=volcopy eth0,ipv4.address=" {
		t.Errorf("X-Incus-devices: %q", got)
	}
	if got := sent.header.Values("X-Incus-config"); len(got) != 1 || got[0] != "volatile.uuid= volatile.eth0.hwaddr=" {
		t.Errorf("X-Incus-config: %q", got)
	}
	if sent.header.Get("X-Incus-name") != "vmcopy" || sent.header.Get("X-Incus-pool") != "pool1" {
		t.Errorf("name and pool headers: %v", sent.header)
	}
	if sent.body != "the archive" {
		t.Errorf("body: %q", sent.body)
	}
}

// An override header that is there but empty is still an override to the client, which then asks the server for an
// extension the import does not need: no override means no header at all.
func TestImportInstance_WithoutOverrides_SendsNeitherHeader(t *testing.T) {
	for name, overrides := range map[string]InstanceOverrides{
		"the zero value": {},
		"empty lists":    {Devices: []string{}, Config: []string{}},
	} {
		incus, requests := newImportTestServer(t)

		err := incus.ImportInstance(context.Background(), "t-x", "pool1", "vmaaaa", strings.NewReader("the archive"), overrides)

		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		seen := requests()
		if len(seen) != 1 {
			t.Fatalf("%s: want one import request, got %+v", name, seen)
		}
		devices, config := seen[0].header.Values("X-Incus-devices"), seen[0].header.Values("X-Incus-config")
		if len(devices) != 0 || len(config) != 0 {
			t.Errorf("%s: override headers sent: devices %q, config %q", name, devices, config)
		}
		if seen[0].header.Get("X-Incus-name") != "vmaaaa" || seen[0].header.Get("X-Incus-pool") != "pool1" || seen[0].body != "the archive" {
			t.Errorf("%s: request: %v, body %q", name, seen[0].header, seen[0].body)
		}
	}
}

// unreadReader fails the test when the archive is read: a refused import must not have started the upload.
type unreadReader struct {
	t *testing.T
}

func (r unreadReader) Read(buffer []byte) (int, error) {
	r.t.Error("the archive was read")
	return 0, io.EOF
}

// Incus parses the override headers only once it holds the whole archive, and splits them on spaces: an entry it would
// misread is refused here, before a byte is uploaded.
func TestImportInstance_AnOverrideIncusWouldMisread_IsRefusedBeforeTheUpload(t *testing.T) {
	for name, overrides := range map[string]InstanceOverrides{
		"a device value with a space": {Devices: []string{"vol,source=a b"}},
		"a device without its key":    {Devices: []string{"vol,source"}},
		"a device without its name":   {Devices: []string{"source=vol"}},
		"a device with an empty name": {Devices: []string{",source=vol"}},
		"a device with an empty key":  {Devices: []string{"vol,=x"}},
		"a config value with a space": {Config: []string{"user.titan-name=web 1"}},
		"a config without a value":    {Config: []string{"volatile.uuid"}},
		"a config with an empty key":  {Config: []string{"=x"}},
		"a config value with a break": {Config: []string{"user.note=a\nb"}},
	} {
		incus, requests := newImportTestServer(t)

		err := incus.ImportInstance(context.Background(), "t-x", "pool1", "vmcopy", unreadReader{t}, overrides)

		if err == nil || !strings.Contains(err.Error(), "override") {
			t.Errorf("%s: want the override refused, got %v", name, err)
		}
		if seen := requests(); len(seen) != 0 {
			t.Errorf("%s: a request was sent: %+v", name, seen)
		}
	}
}

// A PATCH merges the keys it carries into the volume's config and keeps the others (cmd/incusd/storage_volumes.go of
// v7.0.1): a restored volume gets its own name while its size stays the archive's.
func TestPatchVolumeConfig_PatchesTheVolumeWithTheKeysGiven_InItsProject(t *testing.T) {
	incus, requests := newImportTestServer(t)

	err := incus.PatchVolumeConfig(context.Background(), "t-x", "pool 1", "volcopy", map[string]string{"user.titan-name": "data-2"})

	if err != nil {
		t.Fatal(err)
	}
	seen := requests()
	if len(seen) != 1 {
		t.Fatalf("want one request, got %+v", seen)
	}
	sent := seen[0]
	if sent.method != http.MethodPatch || sent.path != "/1.0/storage-pools/pool 1/volumes/custom/volcopy" || sent.query != "project=t-x" {
		t.Fatalf("request: %s %s?%s", sent.method, sent.path, sent.query)
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(sent.body), &body); err != nil {
		t.Fatalf("body %q: %v", sent.body, err)
	}
	config, _ := body["config"].(map[string]any)
	if len(config) != 1 || config["user.titan-name"] != "data-2" {
		t.Errorf("config sent: %v", body)
	}
	if _, hasRestore := body["restore"]; hasRestore {
		t.Errorf("a config patch asked for a restore: %v", body)
	}
}

func TestPatchVolumeConfig_AVolumeThatIsGone_ComesBackAsNotFound(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPatch {
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `{"type":"error","status":"","status_code":0,"operation":"","error_code":404,"error":"Storage volume not found","metadata":null}`)
			return
		}
		io.WriteString(w, `{"type":"sync","status":"Success","status_code":200,"metadata":{"api_extensions":[],"api_status":"stable","api_version":"1.0","auth":"trusted","public":false}}`)
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAnyClientCert}
	server.StartTLS()
	t.Cleanup(server.Close)
	sum := sha256.Sum256(server.Certificate().Raw)
	certificatePem, keyPem := clientIdentity(t)
	connected, err := Connect(Credentials{Endpoint: server.URL, ServerFingerprint: hex.EncodeToString(sum[:]),
		ClientCertificatePem: certificatePem, ClientKeyPem: keyPem})
	if err != nil {
		t.Fatal(err)
	}

	err = NewIncus(connected).PatchVolumeConfig(context.Background(), "t-x", "default", "volgone", map[string]string{"user.titan-name": "x"})

	if !IsNotFound(err) {
		t.Fatalf("want the 404, got %v", err)
	}
}
