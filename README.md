# titan-incus-worker — OpenViCloud's worker on Incus

`titan-incus-worker` is the part of [OpenViCloud](https://github.com/OpenViCloud) that changes things on
[Incus](https://linuxcontainers.org/incus/) (the control plane itself only reads, and relays SSH sessions). The control
plane writes provider commands (create a VM, delete a network, sync a template…) to a table; the worker leases them
over the control plane's `/internal` API, carries them out on the command's Incus cluster, and reports the outcome. Every
`RECONCILE_INTERVAL` (two minutes by default) it also reads each cluster's VMs, networks,
firewall rules, volumes, snapshots, templates and load balancers from Incus and reports what it sees. It never pulls
Incus back to what was asked for.

It needs the OpenViCloud control plane: it holds no state of its own, and learns the clusters, each one's address and
the credentials from the control plane.

## Run

```sh
CP_BASE_URL=https://control-plane.example \
CP_INTERNAL_TOKEN=… \
go run .
```

| Variable | Default | Meaning |
|---|---|---|
| `CP_BASE_URL` | (required) | Control plane base URL |
| `CP_INTERNAL_TOKEN` | (required) | Sent as `X-Internal-Token` on every `/internal` call |
| `WORKER_ROLE` | `all` | Which loops this process runs: `commands` (customer commands and backups), `inventory` (reading every cluster: state and VM usage), or `all` of them. Command processes can run side by side; run one inventory process, or each cluster is read and reported once per process |
| `COMMAND_SLOTS` | `4` | How many customer commands one process runs at once; backups run one at a time on a loop of their own. Which commands may run side by side is the control plane's lease rule |
| `POLL_INTERVAL` | `2s` | How often an idle worker asks for a command |
| `RECONCILE_INTERVAL` | `2m` | How often each cluster is read from Incus |
| `METRICS_INTERVAL` | `60s` | How often each cluster's VM usage (CPU, memory, root filesystem, disk I/O) is read from Incus and sent to the control plane |
| `IMAGE_SERVER` | `https://images.linuxcontainers.org` | Where templates without their own image URL come from |
| `HEALTH_ADDR` | `:8080` | `/healthz` listener |
| `BACKUP_S3_URL`, `BACKUP_S3_BUCKET` | (none) | S3 endpoint and bucket Incus uploads `vm.backup` and `volume.backup` archives to |
| `BACKUP_S3_ACCESS_KEY_ID`, `BACKUP_S3_SECRET_ACCESS_KEY` | (none) | Key pair for that bucket, handed to Incus with each backup; with any of the four empty the worker still starts, and each backup fails naming what is missing |

## Layout

Packages are grouped by role, and imports go one way (`dependency_test.go` fails otherwise):

| Package | Role |
|---|---|
| `main.go` | The one place that knows every package: routes each command kind and starts the loops of its `WORKER_ROLE`; backups run on a command loop of their own, so a long upload never holds a tenant's command |
| `loop/command`, `loop/reconcile` | Lease, retry and log each command · the reconcile cycle per cluster and kind; know no resource |
| `resource/<kind>/` | One resource each: a file per verb, its report and, for most, its reconciler · `resource/backup/` is the one exception: it backs up two resources (`vm.go`, `volume.go`), has no callback, so no report, and no reconciler |
| `incus/provider`, `incus/cluster` | The Incus port and every name Titan gives things on Incus · cluster id → connection |
| `controlplane/` | Calls the control plane |
| `bucket/` | Reads the backup bucket: whether a backup's object arrived |
| `testsupport/` | Test helpers shared by the packages above |

`incus/provider/testdata/` holds what Incus answered in the lab (7.0.1; `lab-metrics-2026-11-10.txt` from a 7.0.2 server); the tests replay it.

## Development

```sh
go vet ./...
go test ./...
go build -o incus-worker .
```

`contract/ProviderCommandKinds.cs` is a verbatim copy of the control plane's list of command kinds, and
`contract/SOURCE` names the [OpenViCloud/titan-control-plane](https://github.com/OpenViCloud/titan-control-plane) commit it was
copied from. A test fails when a kind in that copy has no route here; a route with no kind yet is allowed, since a new
kind ships here before the control plane enqueues it. The control plane's CI runs the same test with its own kinds
against this repo's `main`: a control plane change adding a kind this repo does not route yet fails its check, and no
control plane image is published from it.
To take a newer list:

```sh
scripts/sync-kinds.sh <control-plane commit, tag or branch>
```

CI fails when the copy differs from the file at `contract/SOURCE`. Each push to `main` whose tests pass publishes
the image `ghcr.io/openvicloud/titan-incus-worker:<commit sha>` (linux/arm64 and linux/amd64). The design rules of
the worker live in titan-ops (private): `titan-ops architecture/worker-structure.md` and
`titan-ops architecture/provider-and-worker.md`.

## License

[MIT](LICENSE)
