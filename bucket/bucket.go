// Package bucket reads the backup bucket: whether an object is there. Incus writes the backups; the worker only checks
// that they arrived (https://github.com/OpenViCloud/titan-ops/blob/main/docs/features/backup/design.md#incus-backup).
package bucket

import (
	"context"
	"errors"
	"fmt"
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
