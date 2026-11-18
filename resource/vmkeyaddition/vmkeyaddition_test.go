package vmkeyaddition

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/testsupport"
)

const (
	keyAdditionId = "11111111-2222-3333-4444-555555555555"
	requestId     = "77777777-6666-5555-4444-333333333333"
	vmId          = "99999999-8888-7777-6666-555555555555"
	instance      = "vminstance"
	project       = "t-0123456789abcdef0123456789abcdef"
	clusterId     = "mock"
)

func clusters(p *testsupport.Provider) cluster.Fixed {
	return cluster.Fixed{clusterId: {Provider: p}}
}

func leased(kind string, payload map[string]any) command.Leased {
	encoded, _ := json.Marshal(payload)
	return command.Leased{Kind: kind, ClusterId: clusterId, Payload: encoded, Attempt: 1, MaxAttempts: 5}
}

func createLeased(keyLines ...string) command.Leased {
	if len(keyLines) == 0 {
		keyLines = []string{"ssh-ed25519 AAAAone"}
	}
	return leased("vm-key-addition.create", map[string]any{
		"keyAdditionId": keyAdditionId, "requestId": requestId, "vmId": vmId, "externalVmId": instance,
		"projectName": project, "keyLines": keyLines,
	})
}

func key(path string) string {
	return project + "/" + instance + "/" + path
}

func TestCreate_NoKeyFileNoConfig_WritesBoth_CreatingTheirDirectories_AndNeedsARestart(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), createLeased("ssh-ed25519 AAAAone"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if string(p.InstanceFiles[key(keyFilePath)]) != "ssh-ed25519 AAAAone\n" {
		t.Fatalf("key file: %q", p.InstanceFiles[key(keyFilePath)])
	}
	if string(p.InstanceFiles[key(sshdConfigPath)]) != sshdConfigLine {
		t.Fatalf("sshd config: %q", p.InstanceFiles[key(sshdConfigPath)])
	}
	wantDirs := map[string]bool{"/etc/ssh/openvicloud": true, "/etc/ssh/sshd_config.d": true}
	if len(p.CreatedDirectories) != 2 || !wantDirs[p.CreatedDirectories[0]] || !wantDirs[p.CreatedDirectories[1]] {
		t.Fatalf("directories created: %v", p.CreatedDirectories)
	}
	body := requestEnd(t, cp)
	if body["outcome"] != "Done" || body["restartNeeded"] != true || len(body) != 2 {
		t.Fatalf("report: %v", body)
	}
}

func TestCreate_ConfigAlreadyThere_LeavesItAlone_AndNeedsNoRestart(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.InstanceFiles[key(sshdConfigPath)] = []byte(sshdConfigLine)

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), createLeased("ssh-ed25519 AAAAone"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if len(p.CreatedDirectories) != 1 || p.CreatedDirectories[0] != "/etc/ssh/openvicloud" {
		t.Fatalf("directories created: %v", p.CreatedDirectories)
	}
	if string(p.InstanceFiles[key(sshdConfigPath)]) != sshdConfigLine {
		t.Fatalf("sshd config rewritten: %q", p.InstanceFiles[key(sshdConfigPath)])
	}
	body := requestEnd(t, cp)
	if body["outcome"] != "Done" || body["restartNeeded"] != false || len(body) != 2 {
		t.Fatalf("report: %v", body)
	}
}

// A key already in the file goes in once; the existing line and its order stay.
func TestCreate_KeyFileHasOneLineAlready_AppendsOnlyTheMissingOne_KeepingOrder(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.InstanceFiles[key(keyFilePath)] = []byte("ssh-ed25519 AAAAold\n")
	p.InstanceFiles[key(sshdConfigPath)] = []byte(sshdConfigLine)

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(),
		createLeased("ssh-ed25519 AAAAold", "ssh-ed25519 AAAAnew"), testsupport.DiscardLogger())

	if err != nil {
		t.Fatal(err)
	}
	if string(p.InstanceFiles[key(keyFilePath)]) != "ssh-ed25519 AAAAold\nssh-ed25519 AAAAnew\n" {
		t.Fatalf("key file: %q", p.InstanceFiles[key(keyFilePath)])
	}
}

// Repeating the same create (a retried lease) changes nothing more: the append is safe to run again from the start.
func TestCreate_RunTwice_IsIdempotent(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	h := NewCreate(cp.Client(), clusters(p))

	if err := h.Run(context.Background(), createLeased("ssh-ed25519 AAAAone"), testsupport.DiscardLogger()); err != nil {
		t.Fatal(err)
	}
	if err := h.Run(context.Background(), createLeased("ssh-ed25519 AAAAone"), testsupport.DiscardLogger()); err != nil {
		t.Fatal(err)
	}

	if string(p.InstanceFiles[key(keyFilePath)]) != "ssh-ed25519 AAAAone\n" {
		t.Fatalf("key file: %q", p.InstanceFiles[key(keyFilePath)])
	}
}

// "Instance is not running" is the tenant's to fix (restart it, or add the key once it is up): not worth a retry.
func TestCreate_VmNotRunning_IsPermanent_AndReportsNothing(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.InstanceNotRunning = true

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), createLeased(), testsupport.DiscardLogger())

	if err == nil || command.IsRetryable(err) {
		t.Fatalf("want a permanent error, got %v", err)
	}
	if len(cp.Requests()) != 0 {
		t.Fatalf("reported %+v", cp.Requests())
	}
}

// Every other Incus error (the agent on a Running VM not yet up among them, not measured on the dev lab) is
// retryable by the loop's default.
func TestCreate_OtherProviderError_IsRetryable_AndReportsNothing(t *testing.T) {
	cp := testsupport.NewControlPlane(t)
	p := testsupport.NewProvider()
	p.GetInstanceFileErr = errors.New("agent not reachable")

	err := NewCreate(cp.Client(), clusters(p)).Run(context.Background(), createLeased(), testsupport.DiscardLogger())

	if err == nil || !command.IsRetryable(err) {
		t.Fatalf("want retryable, got %v", err)
	}
	if len(cp.Requests()) != 0 {
		t.Fatalf("reported %+v", cp.Requests())
	}
}
