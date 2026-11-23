// Command incus-worker is Titan's only actor on Incus: it leases provider commands from the control plane, runs them
// against the cluster, reports back, and runs the reconcilers
// (titan-ops architecture/provider-and-worker.md#go-worker).
package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/OpenViCloud/titan-incus-worker/bucket"
	"github.com/OpenViCloud/titan-incus-worker/controlplane"
	"github.com/OpenViCloud/titan-incus-worker/incus/cluster"
	"github.com/OpenViCloud/titan-incus-worker/incus/provider"
	"github.com/OpenViCloud/titan-incus-worker/loop/command"
	"github.com/OpenViCloud/titan-incus-worker/loop/reconcile"
	"github.com/OpenViCloud/titan-incus-worker/resource/backup"
	"github.com/OpenViCloud/titan-incus-worker/resource/drill"
	"github.com/OpenViCloud/titan-incus-worker/resource/firewall"
	"github.com/OpenViCloud/titan-incus-worker/resource/loadbalancer"
	"github.com/OpenViCloud/titan-incus-worker/resource/network"
	"github.com/OpenViCloud/titan-incus-worker/resource/restore"
	"github.com/OpenViCloud/titan-incus-worker/resource/template"
	"github.com/OpenViCloud/titan-incus-worker/resource/vm"
	"github.com/OpenViCloud/titan-incus-worker/resource/vmkeyaddition"
	"github.com/OpenViCloud/titan-incus-worker/resource/vmsnapshot"
	"github.com/OpenViCloud/titan-incus-worker/resource/volsnapshot"
	"github.com/OpenViCloud/titan-incus-worker/resource/volume"
)

// The longest one run of a kind may take. An operation that never ends must not hold its command slot for ever; past
// the limit the run fails and is retried.
const (
	vmCommandTimeout = 15 * time.Minute
	// A stop waits up to the VM's stop timeout, which the control plane keeps at 1800 s or less, then powers off
	// (titan-ops features/vm/design.md#vm-stop-timeout).
	vmStopCommandTimeout  = 40 * time.Minute
	networkCommandTimeout = 5 * time.Minute
	storageCommandTimeout = 15 * time.Minute
	templateSyncTimeout   = 60 * time.Minute
	// Incus uploads a whole disk; on the dev Mac a 5 GiB root took 17 minutes at the link's upload speed.
	backupTimeout = 6 * time.Hour
	// A drill imports a whole disk (the 5 GiB root of the dev lab took 7 minutes, so a 500 GiB disk takes hours) and
	// deletes its project, all on the cluster's own host.
	drillTimeout = 6 * time.Hour
)

// backupKinds run on a loop of their own: one backup holds its loop for as long as the upload takes, and a tenant's
// create or delete must not wait behind it. A cluster's config is asked after the day's disks and waits behind them.
var backupKinds = []string{"vm.backup", "volume.backup", "cluster.backup"}

// drillKinds run on a loop of their own too: one drill holds its loop for as long as it imports a disk, and neither a
// tenant's command nor the day's backups may wait behind it (titan-ops features/backup/design.md#restore-drill).
var drillKinds = []string{"restore.drill"}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("worker", hostnameOrUnknown())
	loaded, err := loadConfig(os.Getenv)
	if err != nil {
		logger.Error("incus-worker cannot start", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	cp := controlplane.New(loaded.cpBaseUrl, loaded.cpInternalToken)
	clusters := cluster.NewResolver(cp, cluster.ConnectIncus, logger)

	if loaded.role == roleRestore {
		// An archive import is one request as long as the upload of a whole disk.
		restoreClusters := cluster.NewResolver(cp, cluster.ConnectIncusWithoutRequestTimeout, logger)
		code := runRestore(ctx, cp, restoreClusters, loaded, logger)
		stop()
		os.Exit(code)
	}

	tenantRoutes, backupRoutes, drillRoutes := splitRoutes(routes(cp, clusters, loaded.imageServer, loaded.backupTarget))
	running := loops{
		commands:   command.NewPool(cp, tenantRoutes, loaded.pollInterval, loaded.commandSlots, logger),
		backups:    command.NewLoop(cp, backupRoutes, loaded.pollInterval, logger.With("loop", "backup")),
		drills:     command.NewLoop(cp, drillRoutes, loaded.pollInterval, logger.With("loop", "drill")),
		reconciler: reconcile.NewRunner(cp, clusters, reconcilers(), loaded.reconcileInterval, logger),
		metrics:    reconcile.NewRunner(cp, clusters, metricsReconcilers(), loaded.metricsInterval, logger.With("loop", "metrics")),
	}.of(loaded.role)
	health := healthServer(loaded.healthAddr)

	var stopped sync.WaitGroup
	for _, loop := range running {
		stopped.Go(func() { loop.Run(ctx) })
	}
	go func() {
		err := health.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("health server stopped", "error", err)
			stop()
		}
	}()
	logger.Info("incus-worker started", "controlPlane", loaded.cpBaseUrl, "role", loaded.role,
		"commandSlots", loaded.commandSlots, "pollInterval", loaded.pollInterval,
		"reconcileInterval", loaded.reconcileInterval, "metricsInterval", loaded.metricsInterval)
	if missing := loaded.backupTarget.Missing(); len(missing) > 0 {
		logger.Warn("no backup target: every vm.backup and volume.backup fails until it is set", "missing", missing)
	}

	<-ctx.Done()
	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = health.Shutdown(shutdownCtx)
	stopped.Wait()
}

// runRestore runs WORKER_ROLE=restore once and returns the process exit code: 0 on success, 1 on any failure
// (titan-ops features/backup/design.md#cluster-restore). It needs the backup bucket, unlike every other role, so it
// checks the target itself instead of only warning as the backup loop does.
func runRestore(ctx context.Context, cp *controlplane.Client, clusters cluster.Clusters, loaded config, logger *slog.Logger) int {
	logger = logger.With("role", roleRestore)
	if missing := loaded.backupTarget.Missing(); len(missing) > 0 {
		logger.Error("no backup target: a restore needs the bucket", "missing", missing)
		return 1
	}
	objects := bucket.New(loaded.backupTarget.Url, loaded.backupTarget.Bucket, loaded.backupTarget.AccessKeyId, loaded.backupTarget.SecretAccessKey)
	summary, err := restore.New(cp, clusters, objects).Run(ctx, loaded.restoreClusterId, loaded.restorePoint, logger)
	if err != nil {
		logger.Error("restore failed", "clusterId", loaded.restoreClusterId, "restored", summary.Restored,
			"skipped", summary.Skipped, "failed", summary.Failed, "error", err)
		return 1
	}
	return 0
}

func routes(cp *controlplane.Client, clusters cluster.Clusters, imageServer string, target backup.Target) map[string]command.Route {
	objects := bucket.New(target.Url, target.Bucket, target.AccessKeyId, target.SecretAccessKey)
	return map[string]command.Route{
		"vm.create":            {Handler: vm.NewCreate(cp, clusters), Timeout: vmCommandTimeout},
		"vm.delete":            {Handler: vm.NewDelete(cp, clusters), Timeout: vmCommandTimeout},
		"vm.stop":              {Handler: vm.NewStop(cp, clusters), Timeout: vmStopCommandTimeout},
		"vm.start":             {Handler: vm.NewStart(cp, clusters), Timeout: vmCommandTimeout},
		"network.create":       {Handler: network.NewCreate(cp, clusters), Timeout: networkCommandTimeout},
		"network.delete":       {Handler: network.NewDelete(cp, clusters), Timeout: networkCommandTimeout},
		"firewall-rule.create": {Handler: firewall.NewCreate(cp, clusters), Timeout: networkCommandTimeout},
		"firewall-rule.delete": {Handler: firewall.NewDelete(cp, clusters), Timeout: networkCommandTimeout},
		"template.sync":        {Handler: template.NewSync(cp, clusters, imageServer), Timeout: templateSyncTimeout},

		"load-balancer.create": {Handler: loadbalancer.NewCreate(cp, clusters), Timeout: networkCommandTimeout},
		"load-balancer.update": {Handler: loadbalancer.NewUpdate(cp, clusters), Timeout: networkCommandTimeout},
		"load-balancer.delete": {Handler: loadbalancer.NewDelete(cp, clusters), Timeout: networkCommandTimeout},

		"volume.create":           {Handler: volume.NewCreate(cp, clusters), Timeout: storageCommandTimeout},
		"volume.attach":           {Handler: volume.NewAttach(cp, clusters), Timeout: storageCommandTimeout},
		"volume.detach":           {Handler: volume.NewDetach(cp, clusters), Timeout: storageCommandTimeout},
		"volume.delete":           {Handler: volume.NewDelete(cp, clusters), Timeout: storageCommandTimeout},
		"volume-snapshot.create":  {Handler: volsnapshot.NewCreate(cp, clusters), Timeout: storageCommandTimeout},
		"volume-snapshot.delete":  {Handler: volsnapshot.NewDelete(cp, clusters), Timeout: storageCommandTimeout},
		"volume-snapshot.restore": {Handler: volsnapshot.NewRestore(cp, clusters), Timeout: storageCommandTimeout},
		"vm-snapshot.create":      {Handler: vmsnapshot.NewCreate(cp, clusters), Timeout: vmCommandTimeout},
		"vm-snapshot.revert":      {Handler: vmsnapshot.NewRevert(cp, clusters), Timeout: vmCommandTimeout},
		"vm-snapshot.delete":      {Handler: vmsnapshot.NewDelete(cp, clusters), Timeout: vmCommandTimeout},
		"vm-key-addition.create":  {Handler: vmkeyaddition.NewCreate(cp, clusters), Timeout: vmCommandTimeout},

		"vm.backup":     {Handler: backup.NewVm(clusters, target, objects), Timeout: backupTimeout},
		"volume.backup": {Handler: backup.NewVolume(clusters, target, objects), Timeout: backupTimeout},
		// A few reads of the Incus API and one small object.
		"cluster.backup": {Handler: backup.NewCluster(clusters, target, objects), Timeout: networkCommandTimeout},

		"restore.drill": {Handler: drill.New(clusters, objects), Timeout: drillTimeout},
	}
}

type runner interface {
	Run(ctx context.Context)
}

// loops are every loop a worker has; a role runs some of them.
type loops struct {
	commands, backups, drills, reconciler, metrics runner
}

// of lists the loops of role: a commands process leases (tenant commands, backups, drills), an inventory process reads
// the clusters, all does both.
// Leases may come from any number of processes, while two processes of role inventory or all would read and report
// each cluster twice per cycle (titan-ops architecture/worker-structure.md#worker-roles).
func (l loops) of(r role) []runner {
	switch r {
	case roleCommands:
		return []runner{l.commands, l.backups, l.drills}
	case roleInventory:
		return []runner{l.reconciler, l.metrics}
	default:
		return []runner{l.commands, l.backups, l.drills, l.reconciler, l.metrics}
	}
}

// splitRoutes takes the backup kinds and the drill kinds out of all routes, each for its own loop.
func splitRoutes(all map[string]command.Route) (map[string]command.Route, map[string]command.Route, map[string]command.Route) {
	tenant := map[string]command.Route{}
	backups := map[string]command.Route{}
	drills := map[string]command.Route{}
	for kind, route := range all {
		tenant[kind] = route
	}
	for _, kind := range backupKinds {
		backups[kind] = tenant[kind]
		delete(tenant, kind)
	}
	for _, kind := range drillKinds {
		drills[kind] = tenant[kind]
		delete(tenant, kind)
	}
	return tenant, backups, drills
}

// reconcilers lists every resource the reconcile loop observes, in the order of one cluster's cycle.
func reconcilers() []reconcile.Kind {
	return []reconcile.Kind{
		{Name: "vms", Reconcile: reconcilerOfTheProvider(vm.Reconcile)},
		{Name: "networks", Reconcile: reconcilerOfTheProvider(network.Reconcile)},
		{Name: "firewall-rules", Reconcile: reconcilerOfTheProvider(firewall.Reconcile)},
		{Name: "templates", Reconcile: reconcilerOfTheProvider(template.Reconcile)},
		{Name: "volumes", Reconcile: reconcileVolumes(volume.NewReconciler())},
		{Name: "vm-snapshots", Reconcile: reconcilerOfTheProvider(vmsnapshot.Reconcile)},
		{Name: "load-balancers", Reconcile: reconcilerOfTheProvider(loadbalancer.Reconcile)},
	}
}

// metricsReconcilers run on a runner of their own: usage is read every minute, while the state reconcilers' two-minute
// cycle would leave the graphs too coarse (titan-ops features/vm/design.md#vm-metrics).
func metricsReconcilers() []reconcile.Kind {
	return []reconcile.Kind{
		{Name: "vm-metrics", Reconcile: reconcilerOfTheProvider(vm.NewMetrics().Reconcile)},
	}
}

type providerReconciler func(ctx context.Context, cp *controlplane.Client, p provider.Provider, clusterId string, logger *slog.Logger) error

// reconcilerOfTheProvider adapts a reconciler that needs nothing of the cluster but its provider.
func reconcilerOfTheProvider(reconcileKind providerReconciler) func(context.Context, *controlplane.Client, *cluster.Cluster, string, *slog.Logger) error {
	return func(ctx context.Context, cp *controlplane.Client, cl *cluster.Cluster, clusterId string, logger *slog.Logger) error {
		return reconcileKind(ctx, cp, cl.Provider, clusterId, logger)
	}
}

// reconcileVolumes also needs the cluster's storage pool; one Reconciler for every cluster keeps what it reported.
func reconcileVolumes(volumes *volume.Reconciler) func(context.Context, *controlplane.Client, *cluster.Cluster, string, *slog.Logger) error {
	return func(ctx context.Context, cp *controlplane.Client, cl *cluster.Cluster, clusterId string, logger *slog.Logger) error {
		return volumes.Reconcile(ctx, cp, cl.Provider, clusterId, cl.StoragePool, logger)
	}
}

func healthServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok")
	})
	return &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
}
