// Package backup sends a tenant VM's root disk and a custom volume to the environment's backup bucket
// (titan-ops features/backup/design.md#incus-backup). Incus uploads the
// archive itself; the worker only asks for it, so no byte of it passes through the worker.
package backup

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/loop/command"
)

// Target is the S3 bucket Incus uploads to and the key pair Incus gets with each request.
type Target struct {
	Url             string
	Bucket          string
	AccessKeyId     string
	SecretAccessKey string
}

// Missing names each empty setting, by its environment variable; an empty list is a usable target.
func (t Target) Missing() []string {
	missing := []string{}
	settings := []struct {
		name  string
		value string
	}{
		{"BACKUP_S3_URL", t.Url},
		{"BACKUP_S3_BUCKET", t.Bucket},
		{"BACKUP_S3_ACCESS_KEY_ID", t.AccessKeyId},
		{"BACKUP_S3_SECRET_ACCESS_KEY", t.SecretAccessKey},
	}
	for _, setting := range settings {
		if strings.TrimSpace(setting.value) == "" {
			missing = append(missing, setting.name)
		}
	}
	return missing
}

// usable fails permanently while a setting is empty: a lease more changes nothing until the worker is redeployed.
func (t Target) usable() error {
	missing := t.Missing()
	if len(missing) == 0 {
		return nil
	}
	return command.Permanent(fmt.Errorf("no backup target: %s not set", strings.Join(missing, ", ")))
}

func (t Target) upload(key string) api.BackupTarget {
	return api.BackupTarget{
		Protocol:   "s3",
		URL:        t.Url,
		BucketName: t.Bucket,
		Path:       key,
		AccessKey:  t.AccessKeyId,
		SecretKey:  t.SecretAccessKey,
	}
}

// ObjectKey is where one backup lands. The key alone says what it is, so the bucket is read without the database:
// incus/<cluster>/<tenant>/<vms|volumes>/<id>/<UTC time>.tar.gz. S3 shows an upload only once it is complete, so an
// object under the key is a whole backup.
func ObjectKey(clusterId string, tenantId string, collection string, resourceId string, takenAt time.Time) string {
	return "incus/" + clusterId + "/" + tenantId + "/" + collection + "/" + resourceId + "/" +
		takenAt.UTC().Format("20060102T150405Z") + ".tar.gz"
}

// Objects answers whether an object is in the backup bucket; package bucket is the real one.
type Objects interface {
	Has(ctx context.Context, key string) (bool, error)
}

// Arrival is how long a run waits for the object after Incus ends its operation, and how often it asks.
type Arrival struct {
	Every  time.Duration
	Within time.Duration
}

// DefaultArrival: Incus ends the operation once the archive is written into the upload, before the last part and
// the multipart completion have gone (cmd/incusd/instance_backup.go of v7.0.1), and a failure there is dropped. Two
// minutes is assumed, not measured: a tail longer than that fails the run, and the next lease backs up again.
var DefaultArrival = Arrival{Every: 5 * time.Second, Within: 2 * time.Minute}

// confirmArrived waits for the object. One that never shows is a retryable failure: the next lease backs up again.
func confirmArrived(ctx context.Context, objects Objects, arrival Arrival, key string) error {
	deadline := time.Now().Add(arrival.Within)
	for {
		has, err := objects.Has(ctx, key)
		if err != nil {
			return err
		}
		if has {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("incus reported the backup done, but %s is not in the bucket after %s", key, arrival.Within)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(arrival.Every):
		}
	}
}

// snapshotName is the snapshot a backup copies from; a run removes it when done.
const snapshotName = "backup"

// cleanUp removes what a run made, with its own deadline, so a run that timed out still cleans up. A step that fails
// leaves its object to the next run, which removes leftovers first.
func cleanUp(logger *slog.Logger, steps ...func(ctx context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, step := range steps {
		if err := step(ctx); err != nil {
			logger.Warn("backup clean-up step failed; the next run removes it", "error", err)
		}
	}
}
