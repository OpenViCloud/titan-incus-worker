package bucket

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
)

type fakeStore struct {
	mu       sync.Mutex
	objects  map[string]bool
	status   int // answers every request with this status when set
	requests []string
	bodies   map[string]string
	encoding []string
	// keys is what List serves, in order; pageSize splits it into pages of that many keys (0 = one page).
	keys     []string
	pageSize int
}

func (f *fakeStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, r.Method+" "+r.URL.Path+" auth="+strings.SplitN(r.Header.Get("Authorization"), " ", 2)[0])
	if r.Method == http.MethodPut {
		body, _ := io.ReadAll(r.Body)
		if f.bodies == nil {
			f.bodies = map[string]string{}
		}
		f.bodies[r.URL.Path] = string(body)
		f.encoding = append(f.encoding, r.Header.Get("Content-Encoding"))
	}
	if f.status != 0 {
		w.WriteHeader(f.status)
		return
	}
	if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
		f.serveList(w, r)
		return
	}
	if r.Method == http.MethodGet && f.bodies[r.URL.Path] != "" {
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, f.bodies[r.URL.Path])
		return
	}
	if f.objects[r.URL.Path] || r.Method == http.MethodPut {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

// serveList answers ListObjectsV2 (list-type=2) the shape the SDK's XML deserializer expects, paged by pageSize and
// continuation-token, which carries the next start index as plain text (nothing a real S3 token's shape; this fake
// has no other reader of it).
func (f *fakeStore) serveList(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	matching := []string{}
	for _, key := range f.keys {
		if strings.HasPrefix(key, prefix) {
			matching = append(matching, key)
		}
	}
	start := 0
	if token := r.URL.Query().Get("continuation-token"); token != "" {
		start, _ = strconv.Atoi(token)
	}
	pageSize := f.pageSize
	if pageSize <= 0 {
		pageSize = len(matching)
	}
	end := min(start+pageSize, len(matching))
	page := matching[start:end]
	isTruncated := end < len(matching)

	var body strings.Builder
	body.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	for _, key := range page {
		body.WriteString("<Contents><Key>" + key + "</Key></Contents>")
	}
	if isTruncated {
		body.WriteString("<IsTruncated>true</IsTruncated><NextContinuationToken>" + strconv.Itoa(end) + "</NextContinuationToken>")
	} else {
		body.WriteString("<IsTruncated>false</IsTruncated>")
	}
	body.WriteString("</ListBucketResult>")
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, body.String())
}

func open(t *testing.T, store *fakeStore) *Bucket {
	t.Helper()
	server := httptest.NewServer(store)
	t.Cleanup(server.Close)
	return New(server.URL, "titan-backup-dev", "key-id", "secret-key")
}

// The fake's host is an IP, so the SDK writes the path style whatever the option says: this does not prove path-style.
func TestHas_AnObjectThere_IsTrue_AndTheRequestIsSigned(t *testing.T) {
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
func TestHas_Refused_IsAnError_WithoutTheSecret(t *testing.T) {
	store := &fakeStore{status: http.StatusForbidden}

	has, err := open(t, store).Has(context.Background(), "incus/a/b.tar.gz")

	if err == nil || has {
		t.Fatalf("has %v, err %v", has, err)
	}
	if strings.Contains(err.Error(), "secret-key") {
		t.Fatalf("the error holds the secret: %v", err)
	}
}

// A cluster's config is one small object the worker writes itself (titan-ops features/backup/design.md#cluster-backup):
// one signed PUT whose body is the object, not the aws-chunked encoding R2 refuses.
func TestPut_WritesTheBody_InOneSignedPlainRequest(t *testing.T) {
	store := &fakeStore{objects: map[string]bool{}}

	err := open(t, store).Put(context.Background(), "incus/a/cluster/t.yaml", []byte("clusterId: a\n"))

	if err != nil {
		t.Fatal(err)
	}
	if len(store.requests) != 1 || store.requests[0] != "PUT /titan-backup-dev/incus/a/cluster/t.yaml auth=AWS4-HMAC-SHA256" {
		t.Fatalf("requests: %v", store.requests)
	}
	if store.bodies["/titan-backup-dev/incus/a/cluster/t.yaml"] != "clusterId: a\n" || strings.Contains(strings.Join(store.encoding, ","), "aws-chunked") {
		t.Fatalf("bodies %q, content-encoding %q", store.bodies, store.encoding)
	}
}

func TestPut_ARefusal_IsAnError(t *testing.T) {
	store := &fakeStore{status: http.StatusForbidden}

	err := open(t, store).Put(context.Background(), "incus/a/cluster/t.yaml", []byte("x"))

	if err == nil {
		t.Fatal("want the refusal as an error")
	}
}

// A restore reads a cluster's whole object tree this way (titan-ops features/backup/design.md#cluster-restore): List
// must read every page, not just the first.
func TestList_ReadsEveryPage(t *testing.T) {
	store := &fakeStore{
		keys: []string{
			"incus/a/cluster/1.yaml", "incus/a/t1/vms/v1/1.tar.gz", "incus/a/t1/vms/v1/2.tar.gz",
		},
		pageSize: 1,
	}

	keys, err := open(t, store).List(context.Background(), "incus/a/")

	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(keys, ",") != strings.Join(store.keys, ",") {
		t.Fatalf("keys %v, want %v", keys, store.keys)
	}
	pages := 0
	for _, request := range store.requests {
		if strings.HasPrefix(request, "GET ") {
			pages++
		}
	}
	if pages != 3 {
		t.Fatalf("want 3 pages read one key at a time, got %d requests", pages)
	}
}

func TestList_OnlyTheMatchingPrefix(t *testing.T) {
	store := &fakeStore{keys: []string{"incus/a/cluster/1.yaml", "incus/b/cluster/1.yaml"}}

	keys, err := open(t, store).List(context.Background(), "incus/a/")

	if err != nil || len(keys) != 1 || keys[0] != "incus/a/cluster/1.yaml" {
		t.Fatalf("keys %v, err %v", keys, err)
	}
}

func TestList_ARefusal_IsAnError(t *testing.T) {
	store := &fakeStore{status: http.StatusForbidden}

	_, err := open(t, store).List(context.Background(), "incus/a/")

	if err == nil {
		t.Fatal("want the refusal as an error")
	}
}

// Get streams the object; a restore feeds it straight into Incus's import without holding it whole.
func TestGet_ReadsTheWholeBody(t *testing.T) {
	store := &fakeStore{objects: map[string]bool{"/titan-backup-dev/incus/a/t1/vms/v1/1.tar.gz": true},
		bodies: map[string]string{"/titan-backup-dev/incus/a/t1/vms/v1/1.tar.gz": "backup\n"}}

	body, err := open(t, store).Get(context.Background(), "incus/a/t1/vms/v1/1.tar.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	content, err := io.ReadAll(body)

	if err != nil || string(content) != "backup\n" {
		t.Fatalf("content %q, err %v", content, err)
	}
}

func TestGet_NoObject_IsAnError(t *testing.T) {
	store := &fakeStore{objects: map[string]bool{}}

	_, err := open(t, store).Get(context.Background(), "incus/a/missing.tar.gz")

	if err == nil {
		t.Fatal("want a 404 as an error")
	}
}
