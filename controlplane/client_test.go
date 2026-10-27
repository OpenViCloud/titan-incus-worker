package controlplane

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestClient_SendsTheInternalToken(t *testing.T) {
	var token string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token = r.Header.Get("X-Internal-Token")
		w.Write([]byte("[]"))
	}))
	defer server.Close()
	var ignored []any

	NewWithHTTPClient(server.URL, "secret", server.Client()).GetJSON(context.Background(), "/internal/clusters", &ignored)

	if token != "secret" {
		t.Fatalf("token %q, want the internal token", token)
	}
}

func TestClient_Non2xx_IsAnHTTPErrorWithABoundedBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		w.Write([]byte(strings.Repeat("x", 10000)))
	}))
	defer server.Close()

	err := NewWithHTTPClient(server.URL, "t", server.Client()).Patch(context.Background(), "/internal/vms/1/status", map[string]string{})

	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != 422 || len(httpErr.Body) != maxErrorBody {
		t.Fatalf("got %v", err)
	}
}

func TestClient_NoAnswer_IsATransportError(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	url := server.URL
	server.Close()

	err := New(url, "t").Put(context.Background(), "/internal/x", nil)

	var transportErr *TransportError
	if !errors.As(err, &transportErr) {
		t.Fatalf("want a transport error, got %v", err)
	}
}

func TestHTTPError_Permanent(t *testing.T) {
	cases := map[int]bool{400: true, 401: true, 404: true, 422: true, 409: false, 429: false, 500: false, 503: false}
	for status, want := range cases {
		got := (&HTTPError{StatusCode: status}).Permanent()
		if got != want {
			t.Errorf("%d: want Permanent() = %v", status, want)
		}
	}
}

func TestTransportError_DoesNotSayPermanent(t *testing.T) {
	var transportErr error = &TransportError{Err: errors.New("refused")}
	_, saysPermanent := transportErr.(interface{ Permanent() bool })
	if saysPermanent {
		t.Error("a transport error must stay retryable, so it must not answer Permanent")
	}
}
