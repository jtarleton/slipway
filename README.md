# Slipway

A deployment control plane for containerized Drupal on k3s.

The deployable unit is **code + database + files**, not just an image. Slipway
shows those three artifacts across dev, stage and prod as a grid, and every
operation is one artifact moving between two environments.

**Code promotes up. Data copies down.**

GitHub Actions builds and pushes an image, then stops. Slipway owns everything
stateful: it patches Deployments directly and runs each long operation as a
Kubernetes Job it creates and watches, so work survives a control-plane restart.

## Status

Phase 2, plus the copy-down lane brought forward because standing up `stage`
required it.

| Working | |
|---|---|
| `slipway grid` | what is running in every environment, resolved to a digest |
| `slipway pin -env NAME` | rewrite a tag-pinned Deployment to the digest it already runs |
| `slipway deploy -env NAME -image REF` | patch and wait for the rollout |
| `slipway copy-down -from prod -to stage` | files, database, sanitize — in order |
| `slipway resume` | re-attach to work left in flight |
| `slipway serve` | web UI over the same operations, plus a background reconcile loop |

Not built yet: the update-hook sequence's own tests against a live cluster,
snapshots and rollback.

## The web UI

`slipway serve -addr :8080` runs two things in one process: an HTTP server that
renders the grid and drives operations, and a loop that reconciles in-flight
jobs every few seconds — so a sequence started from the CLI, or stranded by a
crash, finishes without a terminal held open.

Every operation is the same `internal/ops` call the CLI makes; the server only
adds a single-flight guard (one operation at a time — copy-down scales a
Deployment to zero and a deploy landing mid-copy is the exact race to avoid) and
fans the operation's progress out to every open tab over Server-Sent Events. The
page is one embedded HTML file with no build step and no dependencies.

## Layout

    internal/jobs      state machine and deterministic Job naming
    internal/store     SQLite control-plane state
    internal/k8s       cluster read and write
    internal/engine    the reconcile loop
    internal/drupal    what each operation actually runs
    internal/grid      the "what is running where" read model, shared CLI/UI
    internal/ops       the operations, with nothing about how they are invoked
    internal/web       HTTP server, SSE, and the embedded page
    cmd/slipway        CLI

## Design notes

**The reconciler is a pure function.** `jobs.Decide(state, observation)` takes
the stored state and what the cluster reports, and returns a decision. It does
no I/O, which is why it can be tested exhaustively — `TestDecideIsTotal` walks
every state against every shape of observation.

**Job names are deterministic.** Resubmitting after a crash must collide with
the previous attempt rather than starting a second dump against the same
database. The name is written to SQLite *before* the Job is submitted, so a
crash in that gap leaves a recoverable record.

**Sequences stall where they fail.** `Runnable()` only offers a job whose
predecessors have all succeeded, which is what stops a sanitize running against
a database that never finished loading.

## Assumptions

- InnoDB only, asserted at environment registration. This is what makes
  `mysqldump --single-transaction` correct with no locking and no scale-down.
- Composer-managed Drupal 11 with a `docroot/` layout.
- `settings.php`, `files` and `private` live on a shared `app` volume.
- One application, many environments. Single-tenant by design.

## Known issues in the environments this manages

- Backup CronJobs hardcode `jt-drupal` in their S3 destination and share one
  healthchecks.io URL, so any second environment overwrites production's backup
  and pings the switch that says production succeeded. Stage's are suspended as
  a stopgap; the destinations need to be per-environment.
- `jamestarleton-k8s-manifests` no longer syncs anything for the Drupal
  namespaces — ArgoCD was removed from them so Slipway could own the spec.
  Backups of the removed Applications are in `deploy/argocd-removed/`.

## Development

    go test ./...
