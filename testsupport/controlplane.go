// Package testsupport holds the fakes the worker's tests share: an httptest control plane, an in-memory Provider, a
// Recorder of the writes made on a Provider, and a Bucket.
// Only tests import it.
package testsupport

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
)

const Token = "test-internal-token"

type Request struct {
	Method string
	Path   string // with the query
	Body   string
	Token  string
}

type response struct {
	status int
	body   string
}

// ControlPlane answers 204 to every PUT/PATCH and "[]" to every GET unless told otherwise, and records each request.
type ControlPlane struct {
	server *httptest.Server

	mu        sync.Mutex
	requests  []Request
	responses map[string][]response
}

func NewControlPlane(t *testing.T) *ControlPlane {
	t.Helper()
	cp := &ControlPlane{responses: map[string][]response{}}
	cp.server = httptest.NewServer(http.HandlerFunc(cp.serve))
	t.Cleanup(cp.server.Close)
	return cp
}

func (cp *ControlPlane) Client() *controlplane.Client {
	return controlplane.NewWithHTTPClient(cp.server.URL, Token, cp.server.Client())
}

// Respond queues an answer for "METHOD path"; queued answers are used in order, the last one repeats.
func (cp *ControlPlane) Respond(method string, path string, status int, body string) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	key := method + " " + path
	cp.responses[key] = append(cp.responses[key], response{status: status, body: body})
}

func (cp *ControlPlane) Requests() []Request {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	copied := make([]Request, len(cp.requests))
	copy(copied, cp.requests)
	return copied
}

// RequestsTo returns the requests whose "METHOD path" starts with prefix.
func (cp *ControlPlane) RequestsTo(prefix string) []Request {
	matching := []Request{}
	for _, request := range cp.Requests() {
		if strings.HasPrefix(request.Method+" "+request.Path, prefix) {
			matching = append(matching, request)
		}
	}
	return matching
}

func (cp *ControlPlane) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	path := r.URL.Path
	if r.URL.RawQuery != "" {
		path += "?" + r.URL.RawQuery
	}

	cp.mu.Lock()
	cp.requests = append(cp.requests, Request{Method: r.Method, Path: path, Body: string(body), Token: r.Header.Get("X-Internal-Token")})
	key := r.Method + " " + path
	queued := cp.responses[key]
	answer := defaultResponse(r.Method)
	if len(queued) > 0 {
		answer = queued[0]
		if len(queued) > 1 {
			cp.responses[key] = queued[1:]
		}
	}
	cp.mu.Unlock()

	if answer.body != "" {
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(answer.status)
	io.WriteString(w, answer.body)
}

func defaultResponse(method string) response {
	if method == http.MethodGet {
		return response{status: http.StatusOK, body: "[]"}
	}
	return response{status: http.StatusNoContent}
}
