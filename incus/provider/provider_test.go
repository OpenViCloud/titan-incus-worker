package provider

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lxc/incus/v7/shared/api"
)

type scriptedWaiter struct {
	answers []*api.Operation
	err     error
	calls   int
}

func (w *scriptedWaiter) GetOperationWait(uuid string, timeout int) (*api.Operation, string, error) {
	w.calls++
	if w.err != nil {
		return nil, "", w.err
	}
	answer := w.answers[0]
	if len(w.answers) > 1 {
		w.answers = w.answers[1:]
	}
	return answer, "", nil
}

func operation(code api.StatusCode, message string) *api.Operation {
	return &api.Operation{ID: "op", Description: "Creating instance", Status: code.String(), StatusCode: code, Err: message}
}

func TestWaitOperation_WaitsPastRunning_UntilSuccess(t *testing.T) {
	waiter := &scriptedWaiter{answers: []*api.Operation{operation(api.Running, ""), operation(api.Running, ""), operation(api.Success, "")}}

	err := WaitOperation(context.Background(), waiter, "op")

	if err != nil || waiter.calls != 3 {
		t.Fatalf("want success after 3 waits, got %v after %d", err, waiter.calls)
	}
}

func TestWaitOperation_Failure_CarriesTheOperationError(t *testing.T) {
	waiter := &scriptedWaiter{answers: []*api.Operation{operation(api.Failure, "The instance is already stopped")}}

	err := WaitOperation(context.Background(), waiter, "op")

	if !IsOperationFailureContaining(err, "already stopped") {
		t.Fatalf("want the operation's error, got %v", err)
	}
}

// Incus drops a finished operation 5 seconds after it ends, then answers a wait for it with the 404 of an operation it
// does not know (testdata lab-compute-09): that 404 says nothing about the object the operation acted on
// (titan-ops infra/incus/design.md#operation-forgotten).
func TestWaitOperation_ForgottenOperation_IsNotTheObjectGone(t *testing.T) {
	waiter := &scriptedWaiter{err: api.StatusErrorf(http.StatusNotFound, "Operation not found")}

	err := WaitOperation(context.Background(), waiter, "op")

	if err == nil || IsNotFound(err) {
		t.Fatalf("want an error that is not a 404, got %v", err)
	}
	if !strings.Contains(err.Error(), "Operation not found") {
		t.Fatalf("lost Incus's answer: %v", err)
	}
}

func TestWaitOperation_CancelledContext_Stops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	waiter := &scriptedWaiter{answers: []*api.Operation{operation(api.Running, "")}}

	err := WaitOperation(ctx, waiter, "op")

	if !errors.Is(err, context.Canceled) || waiter.calls != 0 {
		t.Fatalf("want cancelled before waiting, got %v after %d", err, waiter.calls)
	}
}

func TestIsInstanceAlreadyExists_MatchesTheLabsSynchronous500(t *testing.T) {
	err := api.StatusErrorf(http.StatusInternalServerError, `Instance "vm-1" already exists`)

	if !IsInstanceAlreadyExists(err) {
		t.Fatal("missed the lab's duplicate-instance answer")
	}
	if IsInstanceAlreadyExists(api.StatusErrorf(http.StatusInternalServerError, "disk full")) {
		t.Fatal("matched an unrelated 500")
	}
}

// pinTestServer is a TLS 1.3 server that asks for a client certificate and answers GET /1.0 like Incus.
type pinTestServer struct {
	server      *httptest.Server
	fingerprint string

	mu                 sync.Mutex
	clientCertificates int
}

func newPinTestServer(t *testing.T) *pinTestServer {
	t.Helper()
	pinned := &pinTestServer{}
	pinned.server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pinned.mu.Lock()
		pinned.clientCertificates += len(r.TLS.PeerCertificates)
		pinned.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"type":"sync","status":"Success","status_code":200,"metadata":{"api_extensions":[],"api_status":"stable","api_version":"1.0","auth":"trusted","public":false}}`)
	}))
	pinned.server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAnyClientCert}
	pinned.server.StartTLS()
	t.Cleanup(pinned.server.Close)
	sum := sha256.Sum256(pinned.server.Certificate().Raw)
	pinned.fingerprint = hex.EncodeToString(sum[:])
	return pinned
}

func clientIdentity(t *testing.T) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "titan"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDer, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certificatePem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPem := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDer})
	return string(certificatePem), string(keyPem)
}

func TestConnect_PinnedFingerprint_Connects_AndPresentsTheClientCertificate(t *testing.T) {
	server := newPinTestServer(t)
	certificatePem, keyPem := clientIdentity(t)

	_, err := Connect(Credentials{
		Endpoint: server.server.URL, ServerFingerprint: strings.ToUpper(server.fingerprint),
		ClientCertificatePem: certificatePem, ClientKeyPem: keyPem,
	})

	if err != nil {
		t.Fatalf("a server with the pinned fingerprint was refused: %v", err)
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.clientCertificates == 0 {
		t.Fatal("the client certificate was not presented")
	}
}

func TestConnect_OtherFingerprint_IsRefused(t *testing.T) {
	server := newPinTestServer(t)
	certificatePem, keyPem := clientIdentity(t)
	other := strings.Repeat("ab", 32)

	_, err := Connect(Credentials{
		Endpoint: server.server.URL, ServerFingerprint: other,
		ClientCertificatePem: certificatePem, ClientKeyPem: keyPem,
	})

	if err == nil || !strings.Contains(err.Error(), "fingerprint") {
		t.Fatalf("want a fingerprint refusal, got %v", err)
	}
}

type fakeProjects struct {
	Provider
	exists    bool
	createErr error
	created   []api.ProjectsPost
}

func (f *fakeProjects) GetProject(ctx context.Context, name string) (*api.Project, error) {
	if f.exists {
		return &api.Project{Name: name}, nil
	}
	return nil, api.StatusErrorf(http.StatusNotFound, "Project not found")
}

func (f *fakeProjects) CreateProject(ctx context.Context, project api.ProjectsPost) error {
	f.created = append(f.created, project)
	return f.createErr
}

func TestEnsureProject(t *testing.T) {
	existing := &fakeProjects{exists: true}
	missing := &fakeProjects{}
	raced := &fakeProjects{createErr: api.StatusErrorf(http.StatusConflict, "already exists")}

	if err := EnsureProject(context.Background(), existing, "t-1"); err != nil || len(existing.created) != 0 {
		t.Fatalf("existing: %v %v", err, existing.created)
	}
	if err := EnsureProject(context.Background(), missing, "t-1"); err != nil || len(missing.created) != 1 {
		t.Fatalf("missing: %v %v", err, missing.created)
	}
	if missing.created[0].Config["features.networks"] != "true" || missing.created[0].Config["features.images"] != "false" {
		t.Fatalf("config: %v", missing.created[0].Config)
	}
	if err := EnsureProject(context.Background(), raced, "t-1"); err != nil {
		t.Fatalf("a 409 is another run that got there first: %v", err)
	}
}
