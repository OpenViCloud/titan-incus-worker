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
	"github.com/OpenViCloud/titan-incus-worker/resource/firewall"
	"github.com/OpenViCloud/titan-incus-worker/resource/loadbalancer"
	"github.com/OpenViCloud/titan-incus-worker/resource/network"
	"github.com/OpenViCloud/titan-incus-worker/resource/template"
	"github.com/OpenViCloud/titan-incus-worker/resource/vm"
	"github.com/OpenViCloud/titan-incus-worker/resource/vmsnapshot"
	"github.com/OpenViCloud/titan-incus-worker/resource/volsnapshot"
	"github.com/OpenViCloud/titan-incus-worker/resource/volume"
)

// The longest one run of a kind may take. An operation that never ends must not hold the worker,
// which runs one command at a time; past the limit the run fails and is retried. A pull that runs long blocks every
// other command meanwhile.
const (
	vmCommandTimeout      = 15 * time.Minute
	networkCommandTimeout = 5 * time.Minute
	storageCommandTimeout = 15 * time.Minute
	templateSyncTimeout   = 60 * time.Minute
	// Incus uploads a whole disk; on the dev Mac a 5 GiB root took 17 minutes at the link's upload speed.
	backupTimeout = 6 * time.Hour
)

// backupKinds run on a loop of their own: one backup holds its loop for as long as the upload takes, and a tenant's
// create or delete must not wait behind it.
var backupKinds = []string{"vm.backup", "volume.backup"}

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
	tenantRoutes, backupRoutes := splitBackupRoutes(routes(cp, clusters, loaded.imageServer, loaded.backupTarget))
	loop := command.NewLoop(cp, tenantRoutes, loaded.pollInterval, logger)
	backupLoop := command.NewLoop(cp, backupRoutes, loaded.pollInterval, logger.With("loop", "backup"))
	reconciler := reconcile.NewRunner(cp, clusters, reconcilers(), loaded.reconcileInterval, logger)
	health := healthServer(loaded.healthAddr)

	var running sync.WaitGroup
	running.Add(3)
	go func() {
		defer running.Done()
		loop.Run(ctx)
	}()
	go func() {
		defer running.Done()
		backupLoop.Run(ctx)
	}()
	go func() {
		defer running.Done()
		reconciler.Run(ctx)
	}()
	go func() {
		err := health.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("health server stopped", "error", err)
			stop()
		}
	}()
	logger.Info("incus-worker started", "controlPlane", loaded.cpBaseUrl, "pollInterval", loaded.pollInterval,
		"reconcileInterval", loaded.reconcileInterval)
	if missing := loaded.backupTarget.Missing(); len(missing) > 0 {
		logger.Warn("no backup target: every vm.backup and volume.backup fails until it is set", "missing", missing)
	}

	<-ctx.Done()
	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = health.Shutdown(shutdownCtx)
	running.Wait()
}

func routes(cp *controlplane.Client, clusters cluster.Clusters, imageServer string, target backup.Target) map[string]command.Route {
	objects := bucket.New(target.Url, target.Bucket, target.AccessKeyId, target.SecretAccessKey)
	return map[string]command.Route{
		"vm.create":            {Handler: vm.NewCreate(cp, clusters), Timeout: vmCommandTimeout},
		"vm.delete":            {Handler: vm.NewDelete(cp, clusters), Timeout: vmCommandTimeout},
		"vm.stop":              {Handler: vm.NewStop(cp, clusters), Timeout: vmCommandTimeout},
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

		"vm.backup":     {Handler: backup.NewVm(clusters, target, objects), Timeout: backupTimeout},
		"volume.backup": {Handler: backup.NewVolume(clusters, target, objects), Timeout: backupTimeout},
	}
}

// splitBackupRoutes takes the backup kinds out of all routes, for their own loop.
func splitBackupRoutes(all map[string]command.Route) (map[string]command.Route, map[string]command.Route) {
	tenant := map[string]command.Route{}
	backups := map[string]command.Route{}
	for kind, route := range all {
		tenant[kind] = route
	}
	for _, kind := range backupKinds {
		backups[kind] = tenant[kind]
		delete(tenant, kind)
	}
	return tenant, backups
}

// reconcilers lists every resource the reconcile loop observes, in the order of one cluster's cycle.
func reconcilers() []reconcile.Kind {
	return []reconcile.Kind{
		{Name: "vms", Reconcile: reconcilerOfTheProvider(vm.Reconcile)},
		{Name: "networks", Reconcile: reconcilerOfTheProvider(network.Reconcile)},
		{Name: "firewall-rules", Reconcile: reconcilerOfTheProvider(firewall.Reconcile)},
		{Name: "templates", Reconcile: reconcilerOfTheProvider(template.Reconcile)},
		{Name: "volumes", Reconcile: reconcileVolumes},
		{Name: "vm-snapshots", Reconcile: reconcilerOfTheProvider(vmsnapshot.Reconcile)},
		{Name: "load-balancers", Reconcile: reconcilerOfTheProvider(loadbalancer.Reconcile)},
	}
}

type providerReconciler func(ctx context.Context, cp *controlplane.Client, p provider.Provider, clusterId string, logger *slog.Logger) error

// reconcilerOfTheProvider adapts a reconciler that needs nothing of the cluster but its provider.
func reconcilerOfTheProvider(reconcileKind providerReconciler) func(context.Context, *controlplane.Client, *cluster.Cluster, string, *slog.Logger) error {
	return func(ctx context.Context, cp *controlplane.Client, cl *cluster.Cluster, clusterId string, logger *slog.Logger) error {
		return reconcileKind(ctx, cp, cl.Provider, clusterId, logger)
	}
}

// reconcileVolumes also needs the cluster's storage pool.
func reconcileVolumes(ctx context.Context, cp *controlplane.Client, cl *cluster.Cluster, clusterId string, logger *slog.Logger) error {
	return volume.Reconcile(ctx, cp, cl.Provider, clusterId, cl.StoragePool, logger)
}

func healthServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok")
	})
	return &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
}
