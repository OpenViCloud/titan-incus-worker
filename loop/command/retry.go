package command

import "errors"

// permanent is how an error from any package says no retry can fix it, so that package need not import this one
// (controlplane.HTTPError for a refusal, cluster.ErrNoCluster).
type permanent interface {
	Permanent() bool
}

type permanentError struct {
	err error
}

func (e *permanentError) Error() string {
	return e.err.Error()
}

func (e *permanentError) Unwrap() error {
	return e.err
}

func (e *permanentError) Permanent() bool {
	return true
}

// Permanent marks err as one no retry can fix: the command fails at once.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsRetryable: every error is worth another lease unless some error in its chain says it is permanent
// (titan-ops architecture/provider-and-worker.md#retry-on-workflow). Provider errors count as transient: an Incus
// operation that failed may pass next time.
func IsRetryable(err error) bool {
	var answer permanent
	if !errors.As(err, &answer) {
		return true
	}
	return !answer.Permanent()
}
