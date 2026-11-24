package provider

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	incus "github.com/lxc/incus/v7/client"
)

// importServerCredentials starts a TLS 1.3 server that answers the client's own reads like an Incus that takes
// imports (GET /1.0 with the extensions an import under a pool and a name asks for, the wait on operation op1 as
// done), hands the import itself to answerImport, and returns how to reach it.
func importServerCredentials(t *testing.T, answerImport http.HandlerFunc) Credentials {
	t.Helper()
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/1.0":
			io.WriteString(w, `{"type":"sync","status":"Success","status_code":200,"metadata":{"api_extensions":["container_backup","container_backup_override_pool","backup_override_name"],"api_status":"stable","api_version":"1.0","auth":"trusted","public":false}}`)
		case strings.HasPrefix(r.URL.Path, "/1.0/operations/op1"):
			io.WriteString(w, `{"type":"sync","status":"Success","status_code":200,"metadata":{"id":"op1","status":"Success","status_code":200}}`)
		default:
			answerImport(w, r)
		}
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAnyClientCert}
	server.StartTLS()
	t.Cleanup(server.Close)
	sum := sha256.Sum256(server.Certificate().Raw)
	certificatePem, keyPem := clientIdentity(t)
	return Credentials{Endpoint: server.URL, ServerFingerprint: hex.EncodeToString(sum[:]),
		ClientCertificatePem: certificatePem, ClientKeyPem: keyPem}
}

const importAccepted = `{"type":"async","status":"Operation created","status_code":100,"operation":"/1.0/operations/op1","metadata":{"id":"op1","status":"Running","status_code":103}}`

// slowArchive is a request body that takes its time: each read gives one piece after a pause.
type slowArchive struct {
	pieces int
	pause  time.Duration
}

func (a *slowArchive) Read(buffer []byte) (int, error) {
	if a.pieces == 0 {
		return 0, io.EOF
	}
	time.Sleep(a.pause)
	a.pieces--
	return copy(buffer, "a piece of a disk;"), nil
}

// An import is one request whose body is a whole disk archive: hours for a large one. The import loop's client puts no
// limit on the whole request, and limits only a write that moves nothing, so an upload that keeps moving is never cut
// (titan-ops features/backup/design.md#restore-as-new-resource).
func TestConnectForImports_AnUploadLongerThanTheLimit_IsNotCut(t *testing.T) {
	const limit = 500 * time.Millisecond
	received := make(chan int, 1)
	credentials := importServerCredentials(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- len(body)
		w.WriteHeader(http.StatusAccepted)
		io.WriteString(w, importAccepted)
	})
	server, err := connectForImports(credentials, limit)
	if err != nil {
		t.Fatal(err)
	}
	archive := &slowArchive{pieces: 6, pause: 200 * time.Millisecond}
	started := time.Now()

	err = NewIncus(server).ImportInstance(context.Background(), "t-x", "pool1", "vmcopy", archive, InstanceOverrides{})

	if err != nil {
		t.Fatalf("an upload that outlasts the limit was cut: %v", err)
	}
	if took := time.Since(started); took <= limit {
		t.Fatalf("the upload took %s, no longer than the limit of %s: the test proves nothing", took, limit)
	}
	if got, want := <-received, 6*len("a piece of a disk;"); got != want {
		t.Errorf("the server received %d bytes of the archive, want %d", got, want)
	}
}

// The Incus client's calls take no context: nothing but its HTTP client ends a request whose answer never comes, and
// such a request would hold the import loop, and with the lease it keeps extending every drill and restore, until then.
func TestConnectForImports_ARequestTheServerReadsAndNeverAnswers_FailsOnceTheLimitIsOver(t *testing.T) {
	const limit = 300 * time.Millisecond
	// Long enough for a slow machine to see the limit pass, short enough to end a test whose limit never comes.
	const patience = 20 * time.Second
	release := make(chan struct{})
	credentials := importServerCredentials(t, func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		<-release
	})
	// Cleanups run last in, first out: the handler is let go before the server waits for it to return.
	t.Cleanup(func() { close(release) })
	server, err := connectForImports(credentials, limit)
	if err != nil {
		t.Fatal(err)
	}
	failed := make(chan error, 1)
	started := time.Now()

	go func() {
		failed <- NewIncus(server).ImportInstance(context.Background(), "t-x", "pool1", "vmcopy", strings.NewReader("the archive"), InstanceOverrides{})
	}()

	select {
	case err := <-failed:
		took := time.Since(started)
		if err == nil || !strings.Contains(err.Error(), "timeout awaiting response headers") {
			t.Fatalf("want the wait for the answer to run out, got %v", err)
		}
		if took < limit {
			t.Errorf("the request failed after %s, before the limit of %s", took, limit)
		}
	case <-time.After(patience):
		t.Fatalf("still waiting for an answer %s after a limit of %s", patience, limit)
	}
}

// endlessArchive is a request body with no end: more than any buffer between the worker and a server that does not
// read holds.
type endlessArchive struct {
	read atomic.Int64
}

func (a *endlessArchive) Read(buffer []byte) (int, error) {
	for i := range buffer {
		buffer[i] = 'x'
	}
	a.read.Add(int64(len(buffer)))
	return len(buffer), nil
}

// The wait for an answer starts once the whole request is sent: an upload the server stops reading never gets there,
// and the Incus client's calls take no context that could end it. Each write of the upload must move within the limit.
func TestConnectForImports_AnUploadTheServerStopsReading_FailsOnceTheLimitIsOver(t *testing.T) {
	const limit = 300 * time.Millisecond
	const patience = 20 * time.Second
	release := make(chan struct{})
	credentials := importServerCredentials(t, func(w http.ResponseWriter, r *http.Request) {
		<-release
	})
	t.Cleanup(func() { close(release) })
	server, err := connectForImports(credentials, limit)
	if err != nil {
		t.Fatal(err)
	}
	archive := &endlessArchive{}
	failed := make(chan error, 1)
	started := time.Now()

	go func() {
		failed <- NewIncus(server).ImportInstance(context.Background(), "t-x", "pool1", "vmcopy", archive, InstanceOverrides{})
	}()

	select {
	case err := <-failed:
		took := time.Since(started)
		if err == nil || !strings.Contains(err.Error(), "i/o timeout") {
			t.Fatalf("want a write that ran out of time, got %v", err)
		}
		if took < limit {
			t.Errorf("the upload failed after %s, before the limit of %s", took, limit)
		}
		if archive.read.Load() == 0 {
			t.Error("the upload never started: the test proves nothing about a write that stalls")
		}
	case <-time.After(patience):
		t.Fatalf("still uploading %s after a limit of %s, %d bytes read from the archive", patience, limit, archive.read.Load())
	}
}

// The Incus client clears the transport's wait for an answer, for good, the first time it waits on an operation
// (GetOperationWait in client/incus_operations.go of v7.0.1), and an import is followed by exactly that wait: the
// limit must still hold for the request after it.
func TestConnectForImports_ARequestNeverAnswered_AfterTheClientWaitedOnAnOperation_StillFailsOnceTheLimitIsOver(t *testing.T) {
	const limit = 300 * time.Millisecond
	const patience = 20 * time.Second
	release := make(chan struct{})
	var imports atomic.Int32
	credentials := importServerCredentials(t, func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		if imports.Add(1) == 1 {
			w.WriteHeader(http.StatusAccepted)
			io.WriteString(w, importAccepted)
			return
		}
		<-release
	})
	t.Cleanup(func() { close(release) })
	server, err := connectForImports(credentials, limit)
	if err != nil {
		t.Fatal(err)
	}
	importer := NewIncus(server)
	if err := importer.ImportInstance(context.Background(), "t-x", "pool1", "vmfirst", strings.NewReader("the archive"), InstanceOverrides{}); err != nil {
		t.Fatalf("the first import, whose operation the client waits on: %v", err)
	}
	failed := make(chan error, 1)
	started := time.Now()

	go func() {
		failed <- importer.ImportInstance(context.Background(), "t-x", "pool1", "vmsecond", strings.NewReader("the archive"), InstanceOverrides{})
	}()

	select {
	case err := <-failed:
		took := time.Since(started)
		if err == nil || !strings.Contains(err.Error(), "timeout awaiting response headers") {
			t.Fatalf("want the wait for the answer to run out, got %v", err)
		}
		if took < limit {
			t.Errorf("the request failed after %s, before the limit of %s", took, limit)
		}
	case <-time.After(patience):
		t.Fatalf("still waiting for an answer %s after a limit of %s", patience, limit)
	}
}

func transportOf(t *testing.T, server incus.InstanceServer) (*http.Client, *http.Transport) {
	t.Helper()
	client, err := server.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	wrapped, isOurs := client.Transport.(incus.HTTPTransporter)
	if !isOurs {
		t.Fatalf("the client's transport is a %T", client.Transport)
	}
	return client, wrapped.Transport()
}

// Three clients, one per kind of work: every request cut at two minutes; an import by the worker's import loop, with no
// limit on the whole upload, a write that moves nothing cut at ten minutes, and an answer that must start within ten
// minutes; and the cluster restore's, whose Job has a deadline of its own and which adds no limit to the Incus client's.
func TestConnect_EachClient_LimitsARequestAsItsWorkNeeds(t *testing.T) {
	pinned := newPinTestServer(t)
	certificatePem, keyPem := clientIdentity(t)
	credentials := Credentials{Endpoint: pinned.server.URL, ServerFingerprint: pinned.fingerprint,
		ClientCertificatePem: certificatePem, ClientKeyPem: keyPem}

	everyOtherLoop, err := Connect(credentials)
	if err != nil {
		t.Fatal(err)
	}
	importLoop, err := ConnectForImports(credentials)
	if err != nil {
		t.Fatal(err)
	}
	clusterRestore, err := ConnectWithoutRequestTimeout(credentials)
	if err != nil {
		t.Fatal(err)
	}

	if client, _ := transportOf(t, everyOtherLoop); client.Timeout != 2*time.Minute {
		t.Errorf("Connect: a whole request may take %s, want two minutes", client.Timeout)
	}
	client, transport := transportOf(t, importLoop)
	if client.Timeout != 0 || transport.ResponseHeaderTimeout != 10*time.Minute {
		t.Errorf("ConnectForImports: a whole request may take %s (0 is no limit) and its answer %s to start; want no limit and ten minutes",
			client.Timeout, transport.ResponseHeaderTimeout)
	}
	client, transport = transportOf(t, clusterRestore)
	if client.Timeout != 0 || transport.ResponseHeaderTimeout <= 10*time.Minute {
		t.Errorf("ConnectWithoutRequestTimeout: a whole request may take %s (0 is no limit) and its answer %s to start; want no limit and the Incus client's own wait",
			client.Timeout, transport.ResponseHeaderTimeout)
	}
}

// The import loop's client pins the server as every client does: the limit is set on the transport the pin is on.
func TestConnectForImports_OtherFingerprint_IsRefused(t *testing.T) {
	pinned := newPinTestServer(t)
	certificatePem, keyPem := clientIdentity(t)

	_, err := ConnectForImports(Credentials{
		Endpoint: pinned.server.URL, ServerFingerprint: strings.Repeat("ab", 32),
		ClientCertificatePem: certificatePem, ClientKeyPem: keyPem,
	})

	if err == nil || !strings.Contains(err.Error(), "fingerprint") {
		t.Fatalf("want a fingerprint refusal, got %v", err)
	}
}
