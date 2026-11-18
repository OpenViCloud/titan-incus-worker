package provider

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// These tests check only the request this adapter builds (and how it reads a synthetic answer shaped like Incus's
// own, from the fixed headers the client library already documents, api.ParseFileHeaders among them). They are not a
// replay of a dev-lab recording: GetInstanceFile and CreateInstanceFile have none yet (see the report for the
// recordings still needed — an absent path, a push to a stopped VM, a push whose agent is not yet up).

type recordedFileRequest struct {
	method string
	path   string
	query  string
	header http.Header
	body   string
}

// newFileTestServer answers a fixed response to every request and records each one; path is "" or "directory" for a
// GetInstanceFile, matching api.ParseFileHeaders' X-Incus-type header.
func newFileTestServer(t *testing.T, fileType string, content string, status int) (*httptest.Server, *[]recordedFileRequest) {
	t.Helper()
	var mu sync.Mutex
	var requests []recordedFileRequest
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		requests = append(requests, recordedFileRequest{method: r.Method, path: r.URL.Path, query: r.URL.RawQuery, header: r.Header.Clone(), body: string(body)})
		mu.Unlock()
		if r.URL.Path == "/1.0" {
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"type":"sync","status":"Success","status_code":200,"metadata":{"api_extensions":["directory_manipulation"],"api_status":"stable","api_version":"1.0","auth":"trusted","public":false}}`)
			return
		}
		// A write (CreateInstanceFile/CreateInstanceDirectory): Incus answers a bare sync success, body ignored by the
		// client past the status check.
		if r.Method == http.MethodPost {
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"type":"sync","status":"Success","status_code":200,"metadata":null}`)
			return
		}
		if status != http.StatusOK {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			io.WriteString(w, `{"type":"error","error_code":`+strconv.Itoa(status)+`,"error":"file does not exist"}`)
			return
		}
		w.Header().Set("X-Incus-uid", "0")
		w.Header().Set("X-Incus-gid", "0")
		w.Header().Set("X-Incus-mode", "0644")
		w.Header().Set("X-Incus-type", fileType)
		if fileType == "directory" {
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"type":"sync","status":"Success","status_code":200,"metadata":["a","b"]}`)
			return
		}
		io.WriteString(w, content)
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAnyClientCert}
	server.StartTLS()
	t.Cleanup(server.Close)
	return server, &requests
}

func connectTestServer(t *testing.T, server *httptest.Server) *Incus {
	t.Helper()
	sum := sha256.Sum256(server.Certificate().Raw)
	certificatePem, keyPem := clientIdentity(t)
	connected, err := Connect(Credentials{
		Endpoint: server.URL, ServerFingerprint: hex.EncodeToString(sum[:]),
		ClientCertificatePem: certificatePem, ClientKeyPem: keyPem,
	})
	if err != nil {
		t.Fatal(err)
	}
	return NewIncus(connected)
}

func TestGetInstanceFile_ReadsTheContentAndProject(t *testing.T) {
	server, requests := newFileTestServer(t, "file", "ssh-ed25519 AAAAone\n", http.StatusOK)
	incusClient := connectTestServer(t, server)

	content, err := incusClient.GetInstanceFile(context.Background(), "t-x", "vmaaaa", "/etc/ssh/openvicloud/ubuntu")

	if err != nil || string(content) != "ssh-ed25519 AAAAone\n" {
		t.Fatalf("content %q, err %v", content, err)
	}
	found := fileRequest(*requests, "/1.0/instances/vmaaaa/files")
	if found == nil || found.query != "path=%2Fetc%2Fssh%2Fopenvicloud%2Fubuntu&project=t-x" {
		t.Fatalf("request: %+v", found)
	}
}

func TestGetInstanceFile_Directory_HasNoContentAndNoError(t *testing.T) {
	server, _ := newFileTestServer(t, "directory", "", http.StatusOK)
	incusClient := connectTestServer(t, server)

	content, err := incusClient.GetInstanceFile(context.Background(), "t-x", "vmaaaa", "/etc/ssh/openvicloud")

	if err != nil || content != nil {
		t.Fatalf("content %q, err %v", content, err)
	}
}

func TestGetInstanceFile_Absent_IsInstanceFileNotFound(t *testing.T) {
	server, _ := newFileTestServer(t, "file", "", http.StatusNotFound)
	incusClient := connectTestServer(t, server)

	_, err := incusClient.GetInstanceFile(context.Background(), "t-x", "vmaaaa", "/etc/ssh/openvicloud/ubuntu")

	if !IsInstanceFileNotFound(err) {
		t.Fatalf("want IsInstanceFileNotFound, got %v", err)
	}
}

func TestCreateInstanceFile_SendsOwnerModeAndOverwrite_NeverAppend(t *testing.T) {
	server, requests := newFileTestServer(t, "file", "", http.StatusOK)
	incusClient := connectTestServer(t, server)

	err := incusClient.CreateInstanceFile(context.Background(), "t-x", "vmaaaa", "/etc/ssh/openvicloud/ubuntu",
		[]byte("ssh-ed25519 AAAAone\n"), 0, 0, 0o644)

	if err != nil {
		t.Fatal(err)
	}
	found := fileRequest(*requests, "/1.0/instances/vmaaaa/files")
	if found == nil {
		t.Fatalf("no file request recorded")
	}
	if found.method != http.MethodPost || found.body != "ssh-ed25519 AAAAone\n" {
		t.Fatalf("request: %+v", found)
	}
	if found.header.Get("X-Incus-uid") != "0" || found.header.Get("X-Incus-gid") != "0" ||
		found.header.Get("X-Incus-mode") != "0644" || found.header.Get("X-Incus-type") != "file" ||
		found.header.Get("X-Incus-write") != "overwrite" {
		t.Fatalf("headers: %v", found.header)
	}
}

func TestCreateInstanceDirectory_SendsTypeDirectory_OwnedByRoot(t *testing.T) {
	server, requests := newFileTestServer(t, "file", "", http.StatusOK)
	incusClient := connectTestServer(t, server)

	err := incusClient.CreateInstanceDirectory(context.Background(), "t-x", "vmaaaa", "/etc/ssh/openvicloud")

	if err != nil {
		t.Fatal(err)
	}
	found := fileRequest(*requests, "/1.0/instances/vmaaaa/files")
	if found == nil {
		t.Fatalf("no file request recorded")
	}
	if found.header.Get("X-Incus-uid") != "0" || found.header.Get("X-Incus-gid") != "0" ||
		found.header.Get("X-Incus-type") != "directory" {
		t.Fatalf("headers: %v", found.header)
	}
}

func fileRequest(requests []recordedFileRequest, path string) *recordedFileRequest {
	for i := range requests {
		if strings.HasPrefix(requests[i].path, path) {
			return &requests[i]
		}
	}
	return nil
}
