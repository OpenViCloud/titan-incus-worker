package cluster

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

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
