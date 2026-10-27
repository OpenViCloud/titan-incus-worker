// Package cluster connects to an Incus cluster by its id, reading the connection from the control plane
// (https://github.com/OpenViCloud/titan-ops/blob/main/docs/features/cluster/design.md#worker-cluster-connection), caching the connection for a few minutes.
package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
)

// ErrNoCluster: the control plane configures no cluster of that id (404 CLUSTER_NOT_FOUND). A command for it cannot
// succeed by retrying.
var ErrNoCluster error = &noClusterError{}

type noClusterError struct{}

func (e *noClusterError) Error() string {
	return "cluster not configured"
}

// Permanent tells the command loop not to retry, without this package importing it.
func (e *noClusterError) Permanent() bool {
	return true
}

const cacheFor = 5 * time.Minute

// Cluster is one connected cluster with its provider settings ("" = not set).
type Cluster struct {
	Provider         provider.Provider
	OvnUplinkNetwork string
	StoragePool      string
}

// connectionResponse mirrors the control plane's ClusterConnectionResponse. Never logged: it carries the private key.
type connectionResponse struct {
	Endpoint             string  `json:"endpoint"`
	ServerFingerprint    string  `json:"serverFingerprint"`
	ClientCertificatePem string  `json:"clientCertificatePem"`
	ClientKeyPem         string  `json:"clientKeyPem"`
	OvnUplinkNetwork     *string `json:"ovnUplinkNetwork"`
	StoragePool          *string `json:"storagePool"`
}

// Clusters resolves a cluster id to a connected cluster; Resolver in production, Fixed in tests.
type Clusters interface {
	Cluster(ctx context.Context, clusterId string) (*Cluster, error)
}

// Fixed serves preset clusters; any other id is not configured.
type Fixed map[string]*Cluster

func (f Fixed) Cluster(ctx context.Context, clusterId string) (*Cluster, error) {
	cl, found := f[clusterId]
	if !found {
		return nil, fmt.Errorf("cluster %s: %w", clusterId, ErrNoCluster)
	}
	return cl, nil
}

type ConnectFunc func(provider.Credentials) (provider.Provider, error)

// ConnectIncus is the production ConnectFunc.
func ConnectIncus(credentials provider.Credentials) (provider.Provider, error) {
	server, err := provider.Connect(credentials)
	if err != nil {
		return nil, err
	}
	return provider.NewIncus(server), nil
}

type cached struct {
	cl        *Cluster
	expiresAt time.Time
}

type Resolver struct {
	cp      *controlplane.Client
	connect ConnectFunc
	logger  *slog.Logger
	now     func() time.Time

	mu       sync.Mutex
	clusters map[string]cached
}

func NewResolver(cp *controlplane.Client, connect ConnectFunc, logger *slog.Logger) *Resolver {
	return &Resolver{cp: cp, connect: connect, logger: logger, now: time.Now, clusters: map[string]cached{}}
}

// Cluster returns the connected cluster, connecting on a cache miss. errors.Is(err, ErrNoCluster) when the control
// plane configures none of that id.
func (r *Resolver) Cluster(ctx context.Context, clusterId string) (*Cluster, error) {
	r.mu.Lock()
	entry, found := r.clusters[clusterId]
	r.mu.Unlock()
	isFresh := found && r.now().Before(entry.expiresAt)
	if isFresh {
		return entry.cl, nil
	}

	cl, err := r.load(ctx, clusterId)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.clusters[clusterId] = cached{cl: cl, expiresAt: r.now().Add(cacheFor)}
	r.mu.Unlock()
	return cl, nil
}

func (r *Resolver) load(ctx context.Context, clusterId string) (*Cluster, error) {
	var connection connectionResponse
	err := r.cp.GetJSON(ctx, "/internal/clusters/"+url.PathEscape(clusterId)+"/connection", &connection)
	if controlplane.StatusCodeOf(err) == http.StatusNotFound {
		return nil, fmt.Errorf("cluster %s: %w (%w)", clusterId, ErrNoCluster, err)
	}
	if err != nil {
		return nil, fmt.Errorf("read the connection of cluster %s: %w", clusterId, err)
	}

	connected, err := r.connect(provider.Credentials{
		Endpoint:             connection.Endpoint,
		ServerFingerprint:    connection.ServerFingerprint,
		ClientCertificatePem: connection.ClientCertificatePem,
		ClientKeyPem:         connection.ClientKeyPem,
	})
	if err != nil {
		return nil, fmt.Errorf("cluster %s: %w", clusterId, err)
	}
	// The connection is kept for cacheFor: a cluster re-trusted or moved meanwhile is reached only after it runs out.
	r.logger.Info("connected to the cluster", "clusterId", clusterId, "endpoint", connection.Endpoint,
		"cachedFor", cacheFor.String())
	return &Cluster{
		Provider:         connected,
		OvnUplinkNetwork: valueOrEmpty(connection.OvnUplinkNetwork),
		StoragePool:      valueOrEmpty(connection.StoragePool),
	}, nil
}

func valueOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
