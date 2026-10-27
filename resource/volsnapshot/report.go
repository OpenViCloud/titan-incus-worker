// Package volsnapshot runs the volume snapshot provider commands: create, delete, and restore into a new volume. The
// cluster's storage reconciler (package volume) observes them.
package volsnapshot

// The give-up reasons reach the tenant as the snapshot's requestFailureReason, so they are fixed texts; the raw error
// goes to the admin-only detail.
const (
	reasonCreateFailed = "Snapshot creation failed due to a system error. Please try again or contact support."
	reasonDeleteFailed = "Snapshot deletion failed due to a system error. Please try again or contact support."
)
