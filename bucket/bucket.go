// Package bucket reads and writes the backup bucket: whether an object is there (Has), its whole object tree (List) and
// one object streamed open (Get) for a restore, and the cluster config object the worker writes itself (Put). Incus
// writes the disk backups; the worker only checks that they arrived (titan-ops features/backup/design.md#incus-backup).
// The worker writes a cluster's config itself (titan-ops features/backup/design.md#cluster-backup) and reads the whole
// tree back on a restore (titan-ops features/backup/design.md#cluster-restore).
package bucket

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Bucket struct {
	client *s3.Client
	name   string
}

// New talks path-style to the endpoint, as Incus does for the upload; the endpoint is a bare https://host, as Incus
// keeps only scheme and host of it. R2's region is "auto".
func New(endpoint string, name string, accessKeyId string, secretAccessKey string) *Bucket {
	client := s3.New(s3.Options{
		Region:                     "auto",
		BaseEndpoint:               aws.String(endpoint),
		UsePathStyle:               true,
		Credentials:                credentials.NewStaticCredentialsProvider(accessKeyId, secretAccessKey, ""),
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
	return &Bucket{client: client, name: name}
}

// Has answers whether the object is in the bucket. Only a 404 is "no": a refusal or an unreachable endpoint is an error.
func (b *Bucket) Has(ctx context.Context, key string) (bool, error) {
	_, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b.name), Key: aws.String(key)})
	if err == nil {
		return true, nil
	}
	var response *awshttp.ResponseError
	if errors.As(err, &response) && response.HTTPStatusCode() == http.StatusNotFound {
		return false, nil
	}
	return false, fmt.Errorf("read object %s of bucket %s: %w", key, b.name, err)
}

// List returns every object key under prefix, across as many pages as the bucket holds
// (titan-ops features/backup/design.md#cluster-restore: a restore reads a cluster's whole object tree this way, since
// it builds every id it restores from the keys themselves, never from Titan's database).
func (b *Bucket) List(ctx context.Context, prefix string) ([]string, error) {
	keys := []string{}
	paginator := s3.NewListObjectsV2Paginator(b.client, &s3.ListObjectsV2Input{Bucket: aws.String(b.name), Prefix: aws.String(prefix)})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list objects under %s of bucket %s: %w", prefix, b.name, err)
		}
		for _, object := range page.Contents {
			keys = append(keys, aws.ToString(object.Key))
		}
	}
	return keys, nil
}

// Get opens the object for streaming; the caller closes it. A restore reads an archive this way and feeds it straight
// into Incus's import, never holding it whole on the worker.
func (b *Bucket) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b.name), Key: aws.String(key)})
	if err != nil {
		return nil, fmt.Errorf("read object %s of bucket %s: %w", key, b.name, err)
	}
	return out.Body, nil
}

// Put writes one object in one request. The body is a byte slice, so the SDK signs its hash in the header and sends it
// plain, not in the aws-chunked encoding R2 refuses.
func (b *Bucket) Put(ctx context.Context, key string, body []byte) error {
	_, err := b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(b.name),
		Key:           aws.String(key),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
	})
	if err != nil {
		return fmt.Errorf("write object %s of bucket %s: %w", key, b.name, err)
	}
	return nil
}
