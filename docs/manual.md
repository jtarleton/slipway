# Slipway user manual

Slipway is a deployment control plane for containerized Drupal on k3s. The
deployable unit is **code + database + files**, and every operation is one of
those three artifacts moving between two environments.

- **Code promotes up** — dev → stage → prod, always by image digest.
- **Data copies down** — prod → stage → dev, sanitized on arrival.

Every long-running operation executes as a Kubernetes Job that outlives the
control plane, so a crash mid-deploy is recoverable rather than fatal
(`slipway resume`, or just leave `slipway serve` running).

The same operations are available two ways: the `slipway` CLI, and the web UI
behind `slipway serve`. They call the same code — the CLI prints progress to the
terminal, the web UI streams it to the browser.

---

## Environments

Three environments are registered automatically on first run:

| Name  | Namespace         | Host                    | Role             |
|-------|-------------------|-------------------------|------------------|
| dev   | `jt-drupal-dev`   | dev.jamestarleton.com   | integration      |
| stage | `jt-drupal-stage` | stage.jamestarleton.com | pre-production   |
| prod  | `jt-drupal`       | jamestarleton.com       | **production**   |

`prod` is protected: it can never be the *target* of a copy-down, and
`sql:sanitize` refuses to run against it.

---

## Command line

```
slipway <command> [flags]
```

Running `slipway` with no arguments prints the usage summary.

### Global flags

These apply to every command:

| Flag          | Default              | Meaning                                            |
|---------------|----------------------|----------------------------------------------------|
| `-db`         | `slipway.db`         | path to the SQLite control-plane database          |
| `-kubeconfig` | standard rules       | kubeconfig path (honours `KUBECONFIG`, in-cluster) |
| `-workload`   | `drupal`             | name of the Drupal Deployment                      |
| `-container`  | `drupal`             | container within that Deployment                   |

### `slipway grid`

Show what every environment is running, resolved to an image digest.

```
slipway grid
```

```
ENVIRONMENT   NAMESPACE         CODE                          REPLICAS   STATE
dev           jt-drupal-dev     sha256:4d367eec328a           1/1        healthy
stage         jt-drupal-stage   sha256:4d367eec328a           1/1        healthy
prod          jt-drupal         sha256:4d367eec328a           1/1        healthy
```

The `CODE` column reads:

- `sha256:…` — the Deployment spec names a digest; it is truthful.
- `sha256:… (tag NAME)` — pinned by a tag; the digest shown is what the pods
  actually resolved. The tag can move without the pods moving, so run
  `slipway pin` to make the spec honest.
- `REF (untracked)` — still on a tag, nothing resolved yet.
- `—` — never deployed to.

Times out after 30 seconds — a grid read should be fast or fail.

### `slipway pin -env NAME`

Rewrite a tag-pinned Deployment to the exact digest its pods already run.

```
slipway pin -env prod
```

This changes **no running code** — it is the same image — but it makes
"what is deployed" stop depending on a tag that can be repointed. It is a no-op
if the Deployment is already digest-pinned, and an error if there is no running
pod to resolve a digest from.

### `slipway deploy -env NAME -image REF`

Snapshot the database, deploy an image, wait for the rollout, then run the
post-deploy update sequence.

```
slipway deploy -env stage -image ghcr.io/jtarleton/d11app@sha256:4d367eec328a…
```

| Flag                  | Meaning                                                        |
|-----------------------|---------------------------------------------------------------|
| `-image REF`          | image to deploy (required)                                     |
| `-no-snapshot`        | skip the pre-deploy database snapshot                          |
| `-skip-update`        | patch and roll out only; do not run update hooks              |
| `-skip-config-import` | run the update sequence but omit `drush config:import`        |

A database snapshot is taken **before** anything is patched, and the image being
replaced is recorded — this is what gives `slipway rollback` somewhere to go
back to. A failed snapshot aborts the deploy; deploying with no way back defeats
the purpose. `-no-snapshot` skips it deliberately (e.g. a code-only change you
are certain of, or an environment whose database you do not care about).

The update sequence runs behind maintenance mode:

```
drush state:set system.maintenance_mode 1
drush cache:rebuild
drush updatedb -y
drush config:import -y          # unless -skip-config-import
drush cache:rebuild
drush state:set system.maintenance_mode 0
```

If `updatedb` or `config:import` fails, the script stops **before** clearing
maintenance mode. A half-updated database serving traffic is worse than a
maintenance page, so the site stays down until someone looks at it. That is
deliberate — the error message says so.

Pass a digest-pinned reference (`repo@sha256:…`). A bare tag is accepted with a
warning, but what runs will not be traceable to a release.

Times out after 6 hours (the snapshot can be slow for a large database; a hung
rollout is caught by the rollout's own progress deadline, not this one).

### `slipway snapshot -env NAME`

Dump an environment's database to object storage and record it.

```
slipway snapshot -env prod
```

Runs as one Job with two containers: a mariadb container writes a gzipped
`mysqldump` into a job-local scratch volume, then an aws-cli container uploads it
to `s3://…/slipway/<env>/db/<timestamp>.sql.gz`. The application keeps serving
throughout — `--single-transaction` makes the dump consistent without locking.

The control plane records where the dump went (the `snapshots` table); the file
itself lives in object storage.

### `slipway snapshots -env NAME`

List an environment's recorded snapshots, newest first.

```
slipway snapshots -env prod
```

```
ID   TAKEN                 SANITIZED   OBJECT
14   2026-09-07 14:32:01   false       s3://…/slipway/prod/db/1788793921.sql.gz
9    2026-09-06 09:15:44   false       s3://…/slipway/prod/db/1788700544.sql.gz
```

The `ID` is what `slipway restore` takes.

### `slipway restore -env NAME -snapshot ID`

Load a specific snapshot back into its environment.

```
slipway restore -env stage -snapshot 14
```

| Flag           | Meaning                                                    |
|----------------|----------------------------------------------------------|
| `-snapshot ID` | the snapshot to restore, from `slipway snapshots` (required) |

Unlike `rollback` this touches only the database — the running image is left
alone. The app is scaled to zero for the load and back up afterwards. Use it to
undo a bad copy-down, recover from corruption, or go back further than the last
deploy.

A snapshot can only be restored into the environment it was taken from.

### `slipway rollback -env NAME`

Return an environment to its previous deployment.

```
slipway rollback -env stage              # re-deploy the previous image
slipway rollback -env stage -with-data   # …and restore the snapshot from before that deploy
```

| Flag         | Meaning                                                              |
|--------------|--------------------------------------------------------------------|
| `-with-data` | also restore the database snapshot taken before that deploy         |

By default rollback only re-patches the image — fast, and the common case is
"the new build is broken, get the old one back". The database is left alone; if
the deploy ran update hooks, it is still on the newer schema, and the command
says so.

`-with-data` additionally restores the pre-deploy snapshot: the app is scaled to
zero, the snapshot is streamed back in (aws-cli downloads it, mariadb loads it),
and the app is scaled back up on the old image. Use it when the deploy changed
data, not just code — code and database return to the pre-deploy state together.

Rollback walks the deploy log: a second rollback undoes the deployment before
the one already rolled back. It only knows about deploys made through Slipway;
if nothing is recorded, it is an error.

Times out after 6 hours.

### `slipway copy-down -from ENV -to ENV`

Copy the database, the files, or both, from one environment down to another.

```
slipway copy-down -from prod -to stage              # files + database + sanitize
slipway copy-down -from prod -to dev -clean         # first seed of a fresh environment
slipway copy-down -from prod -to stage -skip-files  # database only
slipway copy-down -from prod -to stage -skip-db     # files only
```

| Flag          | Meaning                                                             |
|---------------|--------------------------------------------------------------------|
| `-from ENV`   | source environment (required)                                       |
| `-to ENV`     | target environment (required; never production)                     |
| `-skip-files` | copy the database only                                              |
| `-skip-db`    | copy the files only                                                 |
| `-clean`      | empty the target file tree before pulling — needed on a first seed  |

The full sequence, strictly ordered:

1. push the source's app volume to object storage
2. pull it into the target
3. stream the database across
4. `drush sql:sanitize` on the target

Nothing runs until its predecessor has **succeeded**, so a failed database load
can never be followed by a sanitize that makes the result look deliberate. The
target Deployment is scaled to zero for the duration and back to one afterwards.

`sql:sanitize` covers Drupal core's user and session tables. A site with custom
tables holding personal data needs its own `hook_drush_sql_sync` — this
operation does not know about them.

Use `-clean` only the first time an environment is seeded. A `subPath` mount
leaves a directory where `settings.php` should be; `-clean` wipes it first.
Afterwards the file sync is incremental and `-clean` is wasteful.

Times out after 6 hours.

### `slipway resume`

Re-attach to any work left in flight and drive it to completion.

```
slipway resume
```

This is the crash-recovery path. If the control plane died — or you closed the
terminal — mid `deploy` or `copy-down`, the Kubernetes Jobs kept running and the
SQLite records survived. `resume` picks up where it stopped. It prints
`nothing in flight` if there is nothing to do.

If a sequence failed partway, `resume` reports it as stalled and stops. Clear it
with `slipway cancel`.

Times out after 6 hours.

### `slipway cancel -group NAME`

Release a stalled sequence by cancelling the steps wedged behind a failed one.

```
slipway cancel -group copy-down-prod-stage-1788728604
```

When a copy-down step fails, everything after it stops — it will never run,
because its predecessor never succeeded. `resume` keeps reporting the group as
failed. `cancel` marks those waiting steps `cancelled` so the sequence is
closed out. The failed step itself is left exactly as it is; nothing is retried
or cleaned up automatically.

Only works on a genuinely stalled group; anything else is an error.

Finding the group name: the web UI's "Stalled sequences" panel shows it
directly. From the CLI there is no job listing yet — read it from the database:

```
sqlite3 slipway.db \
  "SELECT DISTINCT group_id FROM jobs WHERE state NOT IN ('succeeded','failed','cancelled','orphaned')"
```

### `slipway history`

Show the operation log — every deploy, snapshot, rollback, copy-down, restore,
pin and cancel, whether it ran from the CLI or the web UI, and how it turned out.

```
slipway history
```

```
WHEN                  ACTOR   ACTION      TARGET        OUTCOME
2026-09-07 14:41:12   web     rollback    stage         ok
2026-09-07 14:32:01   cli     deploy      stage         ok
2026-09-07 12:05:33   web     copy-down   prod→stage    failed: sequence stalled with 1 step(s) unfinished
```

This is the "what happened to prod last week" view. It is recorded from
`internal/ops`, so the CLI and the web UI both feed the same log. Job-level
detail (individual Kubernetes Jobs and their states) is separate — see the
"Job steps" table in the web UI.

### `slipway serve -addr ADDR`

Run the web UI and a background reconcile loop.

```
slipway serve -addr :8080
```

| Flag         | Default  | Meaning                          |
|--------------|----------|----------------------------------|
| `-addr ADDR` | `:8080`  | address to listen on             |

Runs until it receives `SIGINT` or `SIGTERM`. While it runs, a loop reconciles
any in-flight jobs every few seconds — so a sequence started from the CLI, or
left behind by a crash, finishes without anyone keeping a terminal open. See the
next section.

---

## Web UI

Open `http://<host>:8080/` after `slipway serve`.

The page connects to a live event stream; the **live** / **reconnecting…**
indicator in the top-right shows whether that connection is up. Everything on
the page updates in place as operations run — no refresh needed.

### The workflow matrix

The three artifacts run down the side, the environments across the top:

```
            dev              stage                  prod
CODE        sha256:4d367e…   sha256:4d367e…         sha256:4d367e…
            pinned           pinned                 pinned
DATABASE    not copied       copied 2026-09-06      source of record
FILES       not copied       copied 2026-09-06      source of record
```

- **Code** cells show the running digest and whether the Deployment spec is
  digest-pinned (`pinned`) or still on a tag (`tag — not traceable`).
- **Database** / **Files** cells show when data was last copied into that
  environment, read from job history. `prod` is the *source of record* — data
  is copied *from* it, never *into* it.

### Deploying — drag a Code cell

Drag a **Code** cell onto a **later** environment's column (dev → stage,
stage → prod, or dev → prod). A confirm dialog opens showing the source, the
target, and the exact digest that will be deployed, with optional checkboxes for
**skip update hooks** and **skip config:import**. Confirm to run it — the
equivalent of `slipway deploy -env <target> -image <source's digest>`.

A Code cell is only draggable if its digest is known (pinned, or resolvable from
a running pod). A cell on a bare tag with nothing resolved cannot be promoted.

### Copying data down — drag a Database or Files cell

Drag the **Database** or **Files** cell from **prod** onto an **earlier**
environment (stage or dev). A confirm dialog opens:

- **Database** → runs a database-only copy-down (`-skip-files`); the dialog notes
  that the copy is sanitized on arrival.
- **Files** → runs a files-only copy-down (`-skip-db`), with a **clean the target
  tree first** checkbox for a first seed.

To copy *both* at once, or to copy from `stage` rather than `prod`, use
**Manual operations** (below).

### Per-environment actions

Under each **Code** cell:

- **snapshot** — dump that environment's database to object storage
  (`slipway snapshot`). Always available.
- **roll back** — appears once a deploy has been made through Slipway. Opens a
  dialog showing the image it would return to, with an "also restore the
  database snapshot" checkbox (`slipway rollback [-with-data]`).
- **pin** — appears when the cell is on a bare tag; rewrites the spec to the
  running digest (`slipway pin`).

The **Database** cell shows how many snapshots that environment has.

### Resume in-flight

The **Resume in-flight** button (top-right) runs `slipway resume`: it drives any
queued work to completion. Useful after restarting `slipway serve` while a
sequence was mid-flight — though the background loop will also pick it up on its
own within a few seconds.

### Manual operations

The **Manual operations** disclosure holds what the drag gestures don't cover:

- **Deploy a specific image** — deploy an arbitrary image reference to any
  environment (e.g. a hotfix build, or a rollback to an older digest).
- **Copy down (all options)** — choose source and target freely and combine
  `database only` / `files only` / `clean target tree`.

### Live log, stalled sequences, recent jobs

Below the matrix:

- **Live log** — the running operation's output, streamed line by line. Shows
  `idle — no operation running` when nothing is active.
- **Stalled sequences** — appears only when a copy-down is wedged behind a
  failed step. Shows which step blocked it and how many steps are waiting, with
  a **Cancel sequence** button (`slipway cancel`).
- **Snapshots** — appears once any snapshot exists. Lists every recorded
  snapshot (env, when, object key) with a **restore** button
  (`slipway restore`), which opens a confirm dialog before overwriting that
  environment's database.
- **Operation history** — appears once anything has run. Every operation (deploy,
  snapshot, rollback, …), who ran it (`cli` / `web`), against which environment,
  and the outcome. The same log `slipway history` prints.
- **Job steps** — every step of every recent operation, with its Kubernetes-Job
  state (`succeeded`, `failed`, `running`, `pending`, `cancelled`, or `stalled`
  for a step stuck behind a failure). Lower-level than the history table.

### One operation at a time

The server runs a single foreground operation at a time. A copy-down scales a
Deployment to zero, and a deploy landing in the middle of that is exactly the
race the control plane exists to prevent — so a second operation started while
one is running is refused, not queued. The buttons and drag targets disable
while an operation is in progress.

---

## HTTP API

The web UI is a thin client over these endpoints; they are also usable directly.

| Method | Path              | Body (form-encoded)                                       |
|--------|-------------------|----------------------------------------------------------|
| GET    | `/`               | the HTML page                                             |
| GET    | `/events`         | Server-Sent Events: `grid`, `jobs`, `stalled`, `snapshots`, `history`, `state`, `log` |
| GET    | `/api/grid`       | current grid as JSON                                      |
| GET    | `/api/jobs`       | recent jobs as JSON                                       |
| GET    | `/api/state`      | `{running, operation, log}`                               |
| POST   | `/api/pin`        | `env`                                                     |
| POST   | `/api/deploy`     | `env`, `image`, `no_snapshot`, `skip_update`, `skip_config_import` |
| POST   | `/api/snapshot`   | `env`                                                    |
| POST   | `/api/restore`    | `env`, `snapshot` (id)                                   |
| POST   | `/api/rollback`   | `env`, `with_data`                                       |
| POST   | `/api/copy-down`  | `from`, `to`, `skip_files`, `skip_db`, `clean`            |
| POST   | `/api/resume`     | —                                                        |
| POST   | `/api/cancel`     | `group`                                                   |

`pin`, `deploy`, `copy-down` and `resume` answer `202 Accepted` when the
operation starts and `409 Conflict` when another is already running; progress
then arrives on `/events`, not in the response. `cancel` is a synchronous
bookkeeping write — `200 OK`, or `409` if the group is not stalled.

---

## Recovering from a crash

Because every operation runs as a Kubernetes Job with its state in SQLite, a
crash is recoverable:

1. The Kubernetes Jobs keep running regardless of the control plane.
2. On restart, `slipway serve`'s reconcile loop — or a manual `slipway resume` —
   re-attaches to them and drives them to completion.
3. If a step failed, the sequence stalls there. Decide whether to fix and rerun
   the underlying problem, or `slipway cancel -group NAME` to close it out.

A deploy interrupted during its update sequence may leave the site in
maintenance mode. That is deliberate — clear it only once you have confirmed the
database is consistent (`drush state:set system.maintenance_mode 0`), or
`slipway rollback -env NAME -with-data` to return both code and database to the
pre-deploy snapshot.

## Assumptions

- InnoDB tables only — this is what makes `mysqldump --single-transaction`
  correct with no locking.
- Composer-managed Drupal 11 with a `docroot/` layout.
- `settings.php`, `files`, and `private` live on a shared `app` volume.
- One application, many environments. Single-tenant by design.
