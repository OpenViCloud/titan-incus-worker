package provider

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
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
