package cluster

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
	"testing"
	"time"

	incus "github.com/lxc/incus/v7/client"

	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

const connectionPath = "/internal/clusters/mock/connection"

func TestCluster_NoClusterRow_IsErrNoCluster_AndPermanent(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, connectionPath, http.StatusNotFound, `{"code":"CLUSTER_NOT_FOUND"}`)
	resolver := NewResolver(cp.Client(), func(provider.Credentials) (provider.Provider, error) {
		t.Fatal("connected without a cluster")
		return nil, nil
	}, testsupport.DiscardLogger())

	_, err := resolver.Cluster(context.Background(), "mock")

	var answer interface{ Permanent() bool }
	saysPermanent := errors.As(err, &answer) && answer.Permanent()
	if !errors.Is(err, ErrNoCluster) || !saysPermanent {
		t.Fatalf("want permanent ErrNoCluster, got %v", err)
	}
}

func TestCluster_ConnectsWithTheCredentials_ThenServesFromTheCache(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	cp.Respond(http.MethodGet, connectionPath, http.StatusOK,
		`{"endpoint":"https://incus:8443","serverFingerprint":"ab","clientCertificatePem":"CERT","clientKeyPem":"KEY","ovnUplinkNetwork":"ovnup","storagePool":null}`)
	connects := []provider.Credentials{}
	fake := testsupport.NewProvider()
	logger, logs := testsupport.NewLogCapture(t)
	resolver := NewResolver(cp.Client(), func(credentials provider.Credentials) (provider.Provider, error) {
		connects = append(connects, credentials)
		return fake, nil
	}, logger)
	now := time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)
	resolver.now = func() time.Time { return now }

	first, err := resolver.Cluster(context.Background(), "mock")
	if err != nil {
		t.Fatal(err)
	}
	resolver.Cluster(context.Background(), "mock")
	now = now.Add(cacheFor + time.Second)
	resolver.Cluster(context.Background(), "mock")

	want := provider.Credentials{Endpoint: "https://incus:8443", ServerFingerprint: "ab", ClientCertificatePem: "CERT", ClientKeyPem: "KEY"}
	if len(connects) != 2 || connects[0] != want {
		t.Fatalf("want one connect per cache period with the credentials, got %+v", connects)
	}
	if first.OvnUplinkNetwork != "ovnup" || first.StoragePool != "" || first.Provider != fake {
		t.Fatalf("cluster: %+v", first)
	}
	connected := logs.Records("connected to the cluster")
	if len(connected) != 2 || connected[0]["clusterId"] != "mock" || connected[0]["endpoint"] != "https://incus:8443" {
		t.Fatalf("want one line per connect with the cluster and endpoint, got %v", connected)
	}
	for field, value := range connected[0] {
		if value == "KEY" || value == "CERT" {
			t.Fatalf("the connect line carries the client credentials in %s", field)
		}
	}
}

// incusCredentials starts a TLS 1.3 server that asks for a client certificate and answers GET /1.0 like Incus, and
// returns how to reach it: its fingerprint pinned, a client identity made for the test.
func incusCredentials(t *testing.T) provider.Credentials {
	t.Helper()
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"type":"sync","status":"Success","status_code":200,"metadata":{"api_extensions":[],"api_status":"stable","api_version":"1.0","auth":"trusted","public":false}}`)
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAnyClientCert}
	server.StartTLS()
	t.Cleanup(server.Close)
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
	certificateDer, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDer, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint := sha256.Sum256(server.Certificate().Raw)
	return provider.Credentials{
		Endpoint:             server.URL,
		ServerFingerprint:    hex.EncodeToString(fingerprint[:]),
		ClientCertificatePem: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificateDer})),
		ClientKeyPem:         string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDer})),
	}
}

// limitsOf connects and answers what limits a request of the client made: the time the whole of it may take, and the
// time its answer may take to start (0 is no limit).
func limitsOf(t *testing.T, name string, connect ConnectFunc, credentials provider.Credentials) (time.Duration, time.Duration) {
	t.Helper()
	connected, err := connect(credentials)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	adapter, isIncus := connected.(*provider.Incus)
	if !isIncus {
		t.Fatalf("%s: connected a %T, want the Incus adapter", name, connected)
	}
	client, err := adapter.HTTPClient()
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	transporter, isTheClients := client.Transport.(incus.HTTPTransporter)
	if !isTheClients {
		t.Fatalf("%s: the client's transport is a %T", name, client.Transport)
	}
	return client.Timeout, transporter.Transport().ResponseHeaderTimeout
}

// Three ways to connect, one per kind of work, told apart by what limits a request: every loop but one cuts a whole
// request at two minutes; the import loop puts no limit on a whole upload, cuts a write that moves nothing for ten
// minutes, and gives an answer ten minutes to start; the cluster restore, whose Job has a deadline of its own, adds no
// limit to the Incus client's. A resolver built with the wrong one cuts an import at two minutes, or leaves the import
// loop waiting without end on a request Incus never answers.
func TestConnectFuncs_EachBuildTheClientItsWorkNeeds(t *testing.T) {
	credentials := incusCredentials(t)

	everyOtherLoop, _ := limitsOf(t, "ConnectIncus", ConnectIncus, credentials)
	importLoop, importAnswer := limitsOf(t, "ConnectIncusForImports", ConnectIncusForImports, credentials)
	clusterRestore, restoreAnswer := limitsOf(t, "ConnectIncusWithoutRequestTimeout", ConnectIncusWithoutRequestTimeout, credentials)

	if everyOtherLoop != 2*time.Minute {
		t.Errorf("ConnectIncus: a whole request may take %s, want two minutes", everyOtherLoop)
	}
	if importLoop != 0 || importAnswer != 10*time.Minute {
		t.Errorf("ConnectIncusForImports: a whole request may take %s (0 is no limit) and its answer %s to start; want no limit and ten minutes",
			importLoop, importAnswer)
	}
	if clusterRestore != 0 || restoreAnswer <= 10*time.Minute {
		t.Errorf("ConnectIncusWithoutRequestTimeout: a whole request may take %s (0 is no limit) and its answer %s to start; want no limit and the Incus client's own wait",
			clusterRestore, restoreAnswer)
	}
}
