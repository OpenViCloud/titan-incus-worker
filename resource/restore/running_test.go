package restore

import (
	"testing"

	"github.com/lxc/incus/v7/shared/api"
)

// An admin reading the command's error finds the operation on Incus by its id, and reads what it is doing.
func TestImportStillRunning_NamesTheCopyAndTheOperation_AsIncusDescribesIt(t *testing.T) {
	err := ImportStillRunning("vmcopy", api.Operation{ID: "0b1e", Description: "Restoring backup", StatusCode: api.Running})

	want := "import of vmcopy is still running: incus operation 0b1e (Restoring backup)"
	if err == nil || err.Error() != want {
		t.Fatalf("err = %v, want %q", err, want)
	}
}
