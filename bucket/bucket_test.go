package bucket

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

type fakeStore struct {
	mu       sync.Mutex
	objects  map[string]bool
	status   int // answers every request with this status when set
	requests []string
}

func (f *fakeStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, r.Method+" "+r.URL.Path+" auth="+strings.SplitN(r.Header.Get("Authorization"), " ", 2)[0])
	if f.status != 0 {
		w.WriteHeader(f.status)
		return
	}
	if f.objects[r.URL.Path] {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

func open(t *testing.T, store *fakeStore) *Bucket {
	t.Helper()
	server := httptest.NewServer(store)
	t.Cleanup(server.Close)
	return New(server.URL, "titan-backup-dev", "key-id", "secret-key")
}

func TestHas_AnObjectThere_IsTrue_ReadPathStyle_AndSigned(t *testing.T) {
	store := &fakeStore{objects: map[string]bool{"/titan-backup-dev/incus/a/b.tar.gz": true}}

	has, err := open(t, store).Has(context.Background(), "incus/a/b.tar.gz")

	if err != nil || !has {
		t.Fatalf("has %v, err %v", has, err)
	}
	if len(store.requests) != 1 || store.requests[0] != "HEAD /titan-backup-dev/incus/a/b.tar.gz auth=AWS4-HMAC-SHA256" {
		t.Fatalf("requests: %v", store.requests)
	}
}

func TestHas_NoObject_IsFalse_NotAnError(t *testing.T) {
	store := &fakeStore{objects: map[string]bool{}}

	has, err := open(t, store).Has(context.Background(), "incus/a/b.tar.gz")

	if err != nil || has {
		t.Fatalf("has %v, err %v", has, err)
	}
}

// Access denied is not "no object": the run fails now with the bucket's own answer, instead of polling for two minutes
// for an object it can never see.
func TestHas_Refused_IsAnError_WithoutTheKey(t *testing.T) {
	store := &fakeStore{status: http.StatusForbidden}

	has, err := open(t, store).Has(context.Background(), "incus/a/b.tar.gz")

	if err == nil || has {
		t.Fatalf("has %v, err %v", has, err)
	}
	if strings.Contains(err.Error(), "secret-key") {
		t.Fatalf("the error holds the secret: %v", err)
	}
}
