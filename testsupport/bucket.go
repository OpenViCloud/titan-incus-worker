package testsupport

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
)

// Bucket is an in-memory backup bucket, key to body: what a restore lists and opens. An archive's body is one of the
// two forms of ArchiveDescription.
type Bucket struct {
	Objects map[string]string
	// Opened names each object a Get opened, in call order; Open counts the ones not closed yet. An archive is as
	// large as a disk, so a command that leaves one open holds its connection for the life of the worker.
	Opened []string
	Open   int
	// GetErr fails every Get.
	GetErr error
}

func NewBucket() *Bucket {
	return &Bucket{Objects: map[string]string{}}
}

func (b *Bucket) List(ctx context.Context, prefix string) ([]string, error) {
	keys := []string{}
	for key := range b.Objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (b *Bucket) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if b.GetErr != nil {
		return nil, b.GetErr
	}
	body, found := b.Objects[key]
	if !found {
		return nil, fmt.Errorf("fake bucket: no object %s", key)
	}
	b.Opened = append(b.Opened, key)
	b.Open++
	return &openObject{Reader: strings.NewReader(body), bucket: b}, nil
}

type openObject struct {
	io.Reader
	bucket *Bucket
	closed bool
}

func (o *openObject) Close() error {
	if !o.closed {
		o.closed = true
		o.bucket.Open--
	}
	return nil
}
