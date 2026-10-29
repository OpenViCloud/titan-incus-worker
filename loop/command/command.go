// Package command leases provider commands from the control plane, runs them one at a time, and reports the outcome
// (titan-ops architecture/provider-and-worker.md#provider-command-lease).
package command

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"
)

// Leased mirrors the control plane's LeasedProviderCommandResult.
type Leased struct {
	Id          string          `json:"id"`
	Key         string          `json:"key"`
	Kind        string          `json:"kind"`
	ClusterId   string          `json:"clusterId"`
	Payload     json.RawMessage `json:"payload"`
	Attempt     int             `json:"attempt"`
	MaxAttempts int             `json:"maxAttempts"`
	LeaseUntil  string          `json:"leaseUntil"` // unread; kept as text so a format change cannot break leasing
}

// Handler runs one kind. Run must be safe to repeat from the start: a lease that ran out lets a second run overlap.
type Handler interface {
	// Run does the provider work and sends the success callback.
	Run(ctx context.Context, leased Leased, logger *slog.Logger) error
	// GiveUp sends the resource's give-up callback; nil when the kind has none. cause is why it gave up.
	GiveUp(ctx context.Context, leased Leased, cause error, logger *slog.Logger) error
}

// Route binds a kind to its handler and the longest one run may take; past it the run fails and retries.
type Route struct {
	Handler Handler
	Timeout time.Duration
}

// DecodePayload reads the payload; a payload that does not decode can never run, so the error is permanent.
func DecodePayload(leased Leased, target any) error {
	if err := json.Unmarshal(leased.Payload, target); err != nil {
		return Permanent(errors.Join(errors.New(leased.Kind+" payload does not decode"), err))
	}
	return nil
}
