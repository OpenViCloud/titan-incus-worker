package vm

import (
	"context"
	"errors"
	"testing"

	"github.com/lxc/incus/v7/shared/api"

	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

// A stop shuts the guest down and waits the VM's stop timeout before it powers the VM off
// (titan-ops features/vm/design.md#vm-stop-timeout).

func stopLeased(stopTimeoutSeconds any) command.Leased {
	payload := map[string]any{"vmId": vmId, "requestId": requestId, "externalVmId": instanceName, "projectName": project}
	if stopTimeoutSeconds != nil {
		payload["stopTimeoutSeconds"] = stopTimeoutSeconds
	}
	return leased("vm.stop", payload)
}

func TestStop_ShutsTheGuestDown_WaitingTheVmsStopTimeout(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	err := NewStop(cp.Client(), clusters(p)).Run(context.Background(), stopLeased(600), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	want := []api.InstanceStatePut{{Action: "stop", Timeout: 600}}
	if len(p.StateChanges) != 1 || p.StateChanges[0] != want[0] {
		t.Fatalf("want %+v, got %+v", want, p.StateChanges)
	}
	if body := onlyBody(t, cp, requestRoute); body["outcome"] != "Done" {
		t.Fatalf("report: %v", body)
	}
}

func TestStop_PayloadWithoutAStopTimeout_Waits120Seconds(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	err := NewStop(cp.Client(), clusters(p)).Run(context.Background(), stopLeased(nil), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	want := api.InstanceStatePut{Action: "stop", Timeout: 120}
	if len(p.StateChanges) != 1 || p.StateChanges[0] != want {
		t.Fatalf("want %+v, got %+v", want, p.StateChanges)
	}
}

// What Incus answers when the guest is still up after the timeout, and when the instance is in Error and cannot shut
// down cleanly; a Frozen one Incus resumes and shuts down (lxc/incus v7.0.1 internal/server/instance/drivers/driver_qemu.go
// Shutdown; the timeout's message as testdata lab-compute-10 recorded it).
func TestStop_TheGuestDoesNotShutDown_IsPoweredOff_ThenEndsDone(t *testing.T) {
	for _, message := range []string{
		`Failed shutting down instance, status is "Running": context deadline exceeded`,
		"The instance cannot be cleanly shutdown as in Error status",
	} {
		t.Run(message, func(t *testing.T) {
			logger, logs := testsupport.NewLogCapture(t)
			cp := testsupport.NewControlPlane(t)
			p := testsupport.NewProvider()
			p.UpdateStateErrs = []error{&provider.OperationFailedError{Status: "Failure", Message: message}, nil}

			err := NewStop(cp.Client(), clusters(p)).Run(context.Background(), stopLeased(30), logger)

			if err != nil {
				t.Fatal(err)
			}
			want := []api.InstanceStatePut{{Action: "stop", Timeout: 30}, {Action: "stop", Force: true}}
			if len(p.StateChanges) != 2 || p.StateChanges[0] != want[0] || p.StateChanges[1] != want[1] {
				t.Fatalf("want %+v, got %+v", want, p.StateChanges)
			}
			if got := logs.Objects("the guest did not shut down; powering off"); len(got) != 1 {
				t.Fatalf("want 1 line(s) %q, got %v", "the guest did not shut down; powering off", got)
			}
			if body := onlyBody(t, cp, requestRoute); body["outcome"] != "Done" {
				t.Fatalf("report: %v", body)
			}
		})
	}
}

func TestStop_AlreadyStoppedWhileShuttingDown_IsSuccess_WithoutAPowerOff(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.UpdateStateErr = &provider.OperationFailedError{Status: "Failure", Message: "The instance is already stopped"}

	err := NewStop(cp.Client(), clusters(p)).Run(context.Background(), stopLeased(120), testsupport.DiscardLogger())

	if err != nil {
		t.Fatalf("want success, got %v", err)
	}
	if len(p.StateChanges) != 1 || p.StateChanges[0].Force {
		t.Fatalf("want one shutdown and no power-off, got %+v", p.StateChanges)
	}
}

// Only Incus saying the shutdown did not happen leads to a power-off; a call that did not reach Incus is retried
// whole by the next lease.
func TestStop_ACallThatDidNotReachIncus_IsRetryable_WithoutAPowerOff(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.UpdateStateErr = errors.New("connection reset by peer")

	err := NewStop(cp.Client(), clusters(p)).Run(context.Background(), stopLeased(120), testsupport.DiscardLogger())

	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want retryable, got %v", err)
	}
	if len(p.StateChanges) != 1 || p.StateChanges[0].Force {
		t.Fatalf("want one shutdown and no power-off, got %+v", p.StateChanges)
	}
}

func TestStop_ThePowerOffFails_IsRetryable(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.UpdateStateErrs = []error{
		&provider.OperationFailedError{Status: "Failure", Message: `Failed shutting down instance, status is "Running": context deadline exceeded`},
		&provider.OperationFailedError{Status: "Failure", Message: "injected power-off failure"},
	}

	err := NewStop(cp.Client(), clusters(p)).Run(context.Background(), stopLeased(120), testsupport.DiscardLogger())

	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want retryable, got %v", err)
	}
	if len(p.StateChanges) != 2 || p.StateChanges[0].Force || !p.StateChanges[1].Force {
		t.Fatalf("want a shutdown then a power-off, got %+v", p.StateChanges)
	}
}
