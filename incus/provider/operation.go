package provider

import (
	"context"
	"fmt"

	"github.com/lxc/incus/v7/shared/api"
)

// waitSeconds bounds one server-side wait so a cancelled context is noticed between waits.
const waitSeconds = 30

// OperationWaiter is the one call WaitOperation needs; incus.InstanceServer has it.
type OperationWaiter interface {
	GetOperationWait(uuid string, timeout int) (*api.Operation, string, error)
}

// OperationForgottenError: Incus drops a finished operation 5 s after it ends, then answers a wait for it with 404
// (testdata lab-compute-09). That 404 is not the object's, and the outcome is unknown, so this error does not unwrap
// to it and is retried (titan-ops infra/incus/design.md#operation-forgotten).
type OperationForgottenError struct {
	OperationId string
	Answer      string
}

func (e *OperationForgottenError) Error() string {
	return "waiting for incus operation " + e.OperationId + ": " + e.Answer + " (outcome unknown)"
}

// WaitOperation polls /operations/{id}/wait until the operation is final.
func WaitOperation(ctx context.Context, waiter OperationWaiter, operationId string) error {
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("waiting for incus operation %s: %w", operationId, err)
		}
		operation, _, err := waiter.GetOperationWait(operationId, waitSeconds)
		if IsNotFound(err) {
			return &OperationForgottenError{OperationId: operationId, Answer: err.Error()}
		}
		if err != nil {
			return fmt.Errorf("waiting for incus operation %s: %w", operationId, err)
		}
		if !operation.StatusCode.IsFinal() {
			continue
		}
		if operation.StatusCode == api.Success {
			return nil
		}
		return &OperationFailedError{Description: operation.Description, Status: operation.Status, Message: operation.Err}
	}
}
