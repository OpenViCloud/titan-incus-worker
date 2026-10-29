namespace Titan.Domain.Aggregates.Platform.ProviderCommand;

/// The worker routes on these; a kind no worker claims stays Pending (titan-ops architecture/provider-and-worker.md#provider-command-table).
public static class ProviderCommandKinds
{
    public const string VmCreate = "vm.create";
    public const string VmDelete = "vm.delete";
    public const string VmStop = "vm.stop";
    public const string VmStart = "vm.start";
    public const string VmSnapshotCreate = "vm-snapshot.create";
    public const string VmSnapshotRevert = "vm-snapshot.revert";
    public const string VmSnapshotDelete = "vm-snapshot.delete";
    public const string NetworkCreate = "network.create";
    public const string NetworkDelete = "network.delete";
    public const string FirewallRuleCreate = "firewall-rule.create";
    public const string FirewallRuleDelete = "firewall-rule.delete";
    public const string VolumeCreate = "volume.create";
    public const string VolumeAttach = "volume.attach";
    public const string VolumeDetach = "volume.detach";
    public const string VolumeDelete = "volume.delete";
    public const string VolumeSnapshotCreate = "volume-snapshot.create";
    public const string VolumeSnapshotDelete = "volume-snapshot.delete";
    public const string VolumeSnapshotRestore = "volume-snapshot.restore";
    public const string LoadBalancerCreate = "load-balancer.create";
    public const string LoadBalancerUpdate = "load-balancer.update";
    public const string LoadBalancerDelete = "load-balancer.delete";
    public const string TemplateSync = "template.sync";
    public const string VmBackup = "vm.backup";
    public const string VolumeBackup = "volume.backup";
}
