package restore

import (
	"fmt"

	"github.com/lxc/incus/v7/shared/api"
)

// ImportStillRunning is the refusal, for another lease to try, of a copy an Incus operation is still at work on. An
// import goes on in Incus after the run that asked for it stopped waiting on it (a wait that failed, a lease lost, a
// worker restarted), and what it has made so far is not what it will leave: Incus makes a volume and its snapshots
// before it unpacks the archive into them, and an instance only once its disk is unpacked
// (CreateCustomVolumeFromBackup in internal/server/storage/backend.go and createFromBackup in
// cmd/incusd/instances_post.go of v7.0.1; read there, not measured on a server). A copy found meanwhile is not
// imported yet, and one not found is not gone: nothing is written, removed or reported until the operation has ended.
// The operation is told as Incus describes it: it is the import, or a later step of a run nobody waits on any more.
func ImportStillRunning(name string, operation api.Operation) error {
	return fmt.Errorf("import of %s is still running: incus operation %s (%s)", name, operation.ID, operation.Description)
}
