package provider

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	incus "github.com/lxc/incus/v7/client"
)

// Credentials: how to reach one cluster. Never logged: the key is Titan's identity.
type Credentials struct {
	Endpoint             string
	ServerFingerprint    string
	ClientCertificatePem string
	ClientKeyPem         string
}

// Connect opens a client to the cluster, accepting the server by its pinned SHA-256 fingerprint alone — chain and host
// name are not checked, because an Incus server presents a self-signed certificate
// (titan-ops features/cluster/design.md#cluster-incus).
func Connect(credentials Credentials) (incus.InstanceServer, error) {
	server, err := incus.ConnectIncus(credentials.Endpoint, ConnectionArgs(credentials))
	if err != nil {
		return nil, fmt.Errorf("connect to incus at %s: %w", credentials.Endpoint, err)
	}
	return server, nil
}

// ConnectWithoutRequestTimeout is Connect for the cluster restore, whose import sends a whole disk archive in one
// request body: the two-minute request timeout of Connect would cut it
// (titan-ops features/backup/design.md#cluster-restore). The restore Job's own deadline bounds the work instead. The
// Incus client starts with an hour for an answer to start (client/util.go of v7.0.1) and clears it at its first wait on
// an operation (client/incus_operations.go), so after that this client limits no request at all.
func ConnectWithoutRequestTimeout(credentials Credentials) (incus.InstanceServer, error) {
	args := ConnectionArgs(credentials)
	args.HTTPClient = &http.Client{}
	server, err := incus.ConnectIncus(credentials.Endpoint, args)
	if err != nil {
		return nil, fmt.Errorf("connect to incus at %s: %w", credentials.Endpoint, err)
	}
	return server, nil
}

// importProgressLimit is how long a request of the import loop may go without moving: the time Incus may take to
// start its answer once a whole request is sent, and the time one write of an upload may take. For an import the answer
// comes once Incus has read the index of the archive it received, the first entry (cmd/incusd/instances_post.go and
// internal/server/backup/backup_info.go of v7.0.1); the import itself is an operation, which the worker waits on in
// requests of waitSeconds each.
const importProgressLimit = 10 * time.Minute

// ConnectForImports is Connect for the worker's import loop, whose commands import a disk archive
// (titan-ops features/backup/design.md#restore-drill, titan-ops features/backup/design.md#restore-as-new-resource). An
// import sends a whole disk in one request body, hours for a large one, so no limit is set on the whole request. A
// request that stops moving must still end: the Incus client's calls take no context, and the command would hold the
// loop, and through the lease it keeps extending every drill and restore, for as long as the request hangs. So the
// wait for the answer to start is limited, and so is each write of the upload. Go counts the first from the moment the
// request, body included, is written, and the second is set anew for every write: neither cuts an upload that moves,
// however slowly.
func ConnectForImports(credentials Credentials) (incus.InstanceServer, error) {
	return connectForImports(credentials, importProgressLimit)
}

func connectForImports(credentials Credentials, progressLimit time.Duration) (incus.InstanceServer, error) {
	args := ConnectionArgs(credentials)
	args.HTTPClient = &http.Client{}
	pin := args.TransportWrapper
	// The client builds its own transport and hands it to the wrapper: the only place to set a limit on it.
	args.TransportWrapper = func(transport *http.Transport) incus.HTTPTransporter {
		// The client dials and does the TLS handshake itself, pin included, in DialTLSContext (client/util.go of v7.0.1):
		// the connection it returns is the one every byte of a request is written to.
		dial := transport.DialTLSContext
		transport.DialTLSContext = func(ctx context.Context, network string, address string) (net.Conn, error) {
			connection, err := dial(ctx, network, address)
			if err != nil {
				return nil, err
			}
			return &writeLimitedConn{Conn: connection, limit: progressLimit}, nil
		}
		return &answerLimitedTransport{HTTPTransporter: pin(transport), limit: progressLimit}
	}
	server, err := incus.ConnectIncus(credentials.Endpoint, args)
	if err != nil {
		return nil, fmt.Errorf("connect to incus at %s: %w", credentials.Endpoint, err)
	}
	return server, nil
}

// answerLimitedTransport sets the wait for an answer on the transport before every request, not once: the Incus client
// clears it there, for good, each time it waits on an operation (GetOperationWait in client/incus_operations.go of
// v7.0.1), and an import is followed by that wait. A wait on an operation is answered within waitSeconds, well inside
// the limit. The import loop runs one command at a time, so no two requests set and read the field at once.
type answerLimitedTransport struct {
	incus.HTTPTransporter
	limit time.Duration
}

func (t *answerLimitedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	t.Transport().ResponseHeaderTimeout = t.limit
	return t.HTTPTransporter.RoundTrip(request)
}

// writeLimitedConn fails a write that has not gone through within limit: an upload the server has stopped reading.
// The transport's own wait for an answer starts only once the whole request is written, so it never ends such a
// request. The deadline is set for each write, so an upload that moves is not cut, however long it takes in all.
type writeLimitedConn struct {
	net.Conn
	limit time.Duration
}

func (c *writeLimitedConn) Write(data []byte) (int, error) {
	if err := c.Conn.SetWriteDeadline(time.Now().Add(c.limit)); err != nil {
		return 0, err
	}
	return c.Conn.Write(data)
}

// ConnectionArgs pins the server in TransportWrapper: the client builds its own transport and discards the one on
// HTTPClient, so a pin set anywhere else would be lost while InsecureSkipVerify lets every server through.
func ConnectionArgs(credentials Credentials) *incus.ConnectionArgs {
	return &incus.ConnectionArgs{
		TLSClientCert:      credentials.ClientCertificatePem,
		TLSClientKey:       credentials.ClientKeyPem,
		InsecureSkipVerify: true,
		HTTPClient:         &http.Client{Timeout: 2 * time.Minute},
		TransportWrapper: func(transport *http.Transport) incus.HTTPTransporter {
			transport.TLSClientConfig.MinVersion = tls.VersionTLS13
			// VerifyConnection runs on every handshake, resumed ones included, even with InsecureSkipVerify.
			transport.TLSClientConfig.VerifyConnection = PinnedFingerprintVerifier(credentials.ServerFingerprint)
			return &pinnedTransport{transport: transport}
		},
		// The worker waits on /operations/{id}/wait; the events websocket is never needed.
		SkipGetEvents: true,
	}
}

// PinnedFingerprintVerifier accepts only a server whose leaf certificate hashes to fingerprint (hex, any case).
func PinnedFingerprintVerifier(fingerprint string) func(tls.ConnectionState) error {
	pinned := strings.ToLower(strings.ReplaceAll(fingerprint, ":", ""))
	return func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 {
			return errors.New("incus server presented no certificate")
		}
		sum := sha256.Sum256(state.PeerCertificates[0].Raw)
		presented := hex.EncodeToString(sum[:])
		if presented != pinned {
			return fmt.Errorf("incus server certificate fingerprint %s is not the pinned %s", presented, pinned)
		}
		return nil
	}
}

type pinnedTransport struct {
	transport *http.Transport
}

func (t *pinnedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return t.transport.RoundTrip(request)
}

func (t *pinnedTransport) Transport() *http.Transport {
	return t.transport
}
