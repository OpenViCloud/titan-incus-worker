package restore

// FinishedKey is the config key a restore of one VM or volume sets on the copy with its last write of it, to the
// request the command ends. From then on the copy is the tenant's: it may be started, get volumes and snapshots, and
// the daily backup takes its own "backup" snapshot of it. A command leased again after that must not roll the copy
// back, drop its snapshots, rewrite its devices or remove it, and its own request on the copy is how it knows
// (titan-ops features/backup/design.md#restore-as-new-resource).
const FinishedKey = "user.titan-restore"

// IsFinishedBy says whether the copy with this config was finished by the restore that ends requestId. Another
// request there came with the archive: a resource that was itself restored carries its own restore's. No request
// finishes nothing: a config without the key would match it.
func IsFinishedBy(config map[string]string, requestId string) bool {
	return requestId != "" && config[FinishedKey] == requestId
}
