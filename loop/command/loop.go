package command

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/OpenViCloud/titan-incus-worker/controlplane"
)

const (
	// The control plane leases for 60 seconds; extending every 20 leaves two misses before it runs out.
	defaultExtendEvery = 20 * time.Second
	// The failure route refuses a longer error.
	maxReportedError = 4096
	giveUpTimeout    = 2 * time.Minute
)

var errLeaseLost = errors.New("the lease was taken by another worker")

// errLastLeaseRanOut is the cause handed to GiveUp when the command arrives past its last attempt.
var errLastLeaseRanOut = errors.New("the last lease ran out without a report")

type Loop struct {
	cp           *controlplane.Client
	routes       map[string]Route
	pollInterval time.Duration
	extendEvery  time.Duration
	newLeaseId   func() string
	logger       *slog.Logger
	// unansweredLeaseId is a lease whose PUT or GET failed in transit: the control plane may have given it a command,
	// so the next poll asks again under the same id instead of leaving that command held by an id nobody reads.
	unansweredLeaseId string
}

func NewLoop(cp *controlplane.Client, routes map[string]Route, pollInterval time.Duration, logger *slog.Logger) *Loop {
	return &Loop{
		cp:           cp,
		routes:       routes,
		pollInterval: pollInterval,
		extendEvery:  defaultExtendEvery,
		newLeaseId:   uuid.NewString,
		logger:       logger,
	}
}

// SetExtendEvery is for tests that cannot wait 20 seconds.
func (l *Loop) SetExtendEvery(every time.Duration) {
	l.extendEvery = every
}

// Run polls until ctx ends. After a command it polls again at once: there may be more due.
func (l *Loop) Run(ctx context.Context) {
	for {
		ranACommand, err := l.PollOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			l.logger.Warn("polling for provider commands failed", "error", err)
		}
		if ranACommand {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(l.pollInterval):
		}
	}
}

// PollOnce takes at most one command under a lease and runs it. Returns whether there was one.
func (l *Loop) PollOnce(ctx context.Context) (bool, error) {
	leaseId := l.unansweredLeaseId
	if leaseId == "" {
		leaseId = l.newLeaseId()
	}
	l.unansweredLeaseId = leaseId
	leasePath := "/internal/provider-commands/leases/" + leaseId
	// PUT is idempotent per lease id (titan-ops architecture/provider-and-worker.md#provider-command-lease).
	if err := l.cp.Put(ctx, leasePath, map[string][]string{"kinds": l.kinds()}); err != nil {
		return false, fmt.Errorf("lease provider command: %w", err)
	}

	var leased Leased
	err := l.cp.GetJSON(ctx, leasePath, &leased)
	if controlplane.StatusCodeOf(err) == http.StatusNotFound {
		l.unansweredLeaseId = ""
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read leased provider command: %w", err)
	}
	l.unansweredLeaseId = ""
	l.execute(ctx, leaseId, leased)
	return true, nil
}

func (l *Loop) kinds() []string {
	kinds := []string{}
	for kind := range l.routes {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	return kinds
}

func (l *Loop) execute(ctx context.Context, leaseId string, leased Leased) {
	// commandId and leaseId find the row in core.provider_commands; every line of this run, the handler's too, has them.
	logger := l.logger.With("commandId", leased.Id, "leaseId", leaseId, "kind", leased.Kind, "key", leased.Key,
		"clusterId", leased.ClusterId, "attempt", leased.Attempt, "maxAttempts", leased.MaxAttempts)
	logger.Info("provider command started")
	route, known := l.routes[leased.Kind]
	if !known {
		// Only kinds this worker named are leased, so this is a control plane bug; failing keeps it visible.
		l.reportFailure(ctx, logger, leaseId, fmt.Errorf("no handler for kind %q", leased.Kind), false)
		return
	}

	workCtx, cancelWork := context.WithCancelCause(ctx)
	defer cancelWork(nil)
	stopExtending := l.keepLeaseAlive(ctx, leaseId, cancelWork, logger)
	defer stopExtending()

	isPastTheLastAttempt := leased.Attempt > leased.MaxAttempts
	if isPastTheLastAttempt {
		logger.Warn("command arrived past its last attempt; sending only the give-up callback")
		l.giveUpAndFail(ctx, workCtx, logger, leaseId, leased, route, errLastLeaseRanOut, stopExtending)
		return
	}

	runCtx, cancelRun := context.WithTimeout(workCtx, route.Timeout)
	startedAt := time.Now()
	runErr := route.Handler.Run(runCtx, leased, logger)
	cancelRun()
	logger = logger.With("durationMs", time.Since(startedAt).Milliseconds())

	if l.mustStopReporting(ctx, workCtx, logger) {
		return
	}
	if runErr == nil {
		stopExtending()
		l.reportCompletion(ctx, logger, leaseId)
		return
	}

	isLastAttempt := leased.Attempt >= leased.MaxAttempts
	isRetryable := IsRetryable(runErr)
	if !isRetryable || isLastAttempt {
		logger.Warn("provider command gave up", "error", runErr, "retryable", isRetryable)
		l.giveUpAndFail(ctx, workCtx, logger, leaseId, leased, route, runErr, stopExtending)
		return
	}
	logger.Warn("provider command failed; it will be retried", "error", runErr)
	stopExtending()
	l.reportFailure(ctx, logger, leaseId, runErr, true)
}

// giveUpAndFail sends the give-up callback before the failure (titan-ops architecture/provider-and-worker.md#retry-on-workflow).
// A give-up callback that may pass later is not followed by the failure: the lease runs out, and the next lease arrives
// past its last attempt and sends the callback again. Failing now would leave the resource waiting for ever.
func (l *Loop) giveUpAndFail(ctx context.Context, workCtx context.Context, logger *slog.Logger, leaseId string,
	leased Leased, route Route, cause error, stopExtending func()) {
	giveUpCtx, cancel := context.WithTimeout(workCtx, giveUpTimeout)
	giveUpErr := route.Handler.GiveUp(giveUpCtx, leased, cause, logger)
	cancel()

	if l.mustStopReporting(ctx, workCtx, logger) {
		return
	}
	if giveUpErr != nil {
		if IsRetryable(giveUpErr) {
			logger.Warn("give-up callback failed; leaving the lease to run out so the next lease sends it again", "error", giveUpErr)
			return
		}
		logger.Error("give-up callback refused by the control plane", "error", giveUpErr)
	}
	stopExtending()
	l.reportFailure(ctx, logger, leaseId, cause, false)
}

// mustStopReporting: a lost lease belongs to another worker now, and a shutdown leaves the lease to run out.
func (l *Loop) mustStopReporting(ctx context.Context, workCtx context.Context, logger *slog.Logger) bool {
	if ctx.Err() != nil {
		logger.Info("shutting down mid-command; the lease will run out")
		return true
	}
	if errors.Is(context.Cause(workCtx), errLeaseLost) {
		logger.Warn("lease lost mid-command; not reporting")
		return true
	}
	return false
}

// keepLeaseAlive extends the lease every extendEvery until stopped; a 404/409 cancels the work. The returned stop is
// idempotent and waits for the extender to exit.
func (l *Loop) keepLeaseAlive(ctx context.Context, leaseId string, cancelWork context.CancelCauseFunc,
	logger *slog.Logger) func() {
	extendCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(l.extendEvery)
		defer ticker.Stop()
		for {
			select {
			case <-extendCtx.Done():
				return
			case <-ticker.C:
			}
			err := l.cp.Patch(extendCtx, "/internal/provider-commands/leases/"+leaseId, nil)
			if err == nil {
				continue
			}
			statusCode := controlplane.StatusCodeOf(err)
			isLost := statusCode == http.StatusNotFound || statusCode == http.StatusConflict
			if isLost {
				cancelWork(errLeaseLost)
				return
			}
			if extendCtx.Err() == nil {
				logger.Warn("extending the lease failed; will try again", "error", err)
			}
		}
	}()
	return func() {
		stop()
		<-done
	}
}

func (l *Loop) reportCompletion(ctx context.Context, logger *slog.Logger, leaseId string) {
	err := l.cp.Put(ctx, "/internal/provider-commands/leases/"+leaseId+"/completion", nil)
	if err != nil {
		logger.Warn("reporting completion failed; the lease will run out and the command run again", "error", err)
		return
	}
	logger.Info("provider command done")
}

func (l *Loop) reportFailure(ctx context.Context, logger *slog.Logger, leaseId string, cause error, retryable bool) {
	body := map[string]any{"error": truncate(cause.Error(), maxReportedError), "retryable": retryable}
	err := l.cp.Put(ctx, "/internal/provider-commands/leases/"+leaseId+"/failure", body)
	if err != nil {
		logger.Warn("reporting failure failed; the lease will run out", "error", err)
	}
}

// truncate keeps at most limit bytes without splitting a UTF-8 character.
func truncate(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut]
}
