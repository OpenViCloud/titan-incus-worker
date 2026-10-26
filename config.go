package main

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/OpenViCloud/titan-incus-worker/resource/backup"
)

type config struct {
	cpBaseUrl         string
	cpInternalToken   string
	pollInterval      time.Duration
	reconcileInterval time.Duration
	imageServer       string
	healthAddr        string
	backupTarget      backup.Target
}

// loadConfig reads the environment; lookup is os.Getenv outside tests.
func loadConfig(lookup func(string) string) (config, error) {
	loaded := config{
		cpBaseUrl:       lookup("CP_BASE_URL"),
		cpInternalToken: lookup("CP_INTERNAL_TOKEN"),
		imageServer:     valueOr(lookup("IMAGE_SERVER"), "https://images.linuxcontainers.org"),
		healthAddr:      valueOr(lookup("HEALTH_ADDR"), ":8080"),
		// The key pair comes from an optional Secret: without it the worker starts, and backups fail saying so.
		backupTarget: backup.Target{
			Url:             lookup("BACKUP_S3_URL"),
			Bucket:          lookup("BACKUP_S3_BUCKET"),
			AccessKeyId:     lookup("BACKUP_S3_ACCESS_KEY_ID"),
			SecretAccessKey: lookup("BACKUP_S3_SECRET_ACCESS_KEY"),
		},
	}
	if loaded.cpBaseUrl == "" || loaded.cpInternalToken == "" {
		return config{}, errors.New("CP_BASE_URL and CP_INTERNAL_TOKEN are required")
	}

	var err error
	loaded.pollInterval, err = durationOr(lookup, "POLL_INTERVAL", 2*time.Second)
	if err != nil {
		return config{}, err
	}
	loaded.reconcileInterval, err = durationOr(lookup, "RECONCILE_INTERVAL", 2*time.Minute)
	if err != nil {
		return config{}, err
	}
	return loaded, nil
}

func valueOr(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func durationOr(lookup func(string) string, key string, fallback time.Duration) (time.Duration, error) {
	raw := lookup(key)
	if raw == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("%s must be positive, got %s", key, raw)
	}
	return parsed, nil
}

func hostnameOrUnknown() string {
	name, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return name
}
