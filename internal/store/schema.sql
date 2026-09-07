-- Slipway control-plane schema.
--
-- The separation that matters: a release is an immutable built artifact, a
-- deployment is one release landing in one environment. Promotion is therefore
-- cheap — it creates a deployment referencing a release that already exists,
-- and never rebuilds.

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS environments (
    id            INTEGER PRIMARY KEY,
    name          TEXT    NOT NULL UNIQUE,   -- dev | stage | prod
    rank          INTEGER NOT NULL,          -- promotion order, ascending
    namespace     TEXT    NOT NULL UNIQUE,
    ingress_host  TEXT    NOT NULL,
    is_production INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS releases (
    id           INTEGER PRIMARY KEY,
    git_sha      TEXT NOT NULL,
    git_ref      TEXT NOT NULL,              -- refs/heads/main, refs/tags/v2.1
    image_digest TEXT NOT NULL UNIQUE,       -- sha256:… never a mutable tag
    built_at     TEXT NOT NULL,
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS snapshots (
    id          INTEGER PRIMARY KEY,
    env_id      INTEGER NOT NULL REFERENCES environments(id),
    object_key  TEXT    NOT NULL UNIQUE,
    bytes       INTEGER,
    sanitized   INTEGER NOT NULL DEFAULT 0,
    expires_at  TEXT,                        -- NULL means keep forever
    created_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS deployments (
    id            INTEGER PRIMARY KEY,
    env_id        INTEGER NOT NULL REFERENCES environments(id),
    release_id    INTEGER NOT NULL REFERENCES releases(id),
    supersedes_id INTEGER REFERENCES deployments(id),
    snapshot_id   INTEGER REFERENCES snapshots(id),  -- taken pre-deploy
    state         TEXT    NOT NULL,
    actor         TEXT    NOT NULL,
    started_at    TEXT,
    finished_at   TEXT,
    created_at    TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- The engine's table. k8s_job_name is written before the Job is submitted, so
-- a crash in that gap leaves a recoverable record rather than an orphan; it is
-- the key the reconciler uses to find its way back.
CREATE TABLE IF NOT EXISTS jobs (
    id            INTEGER PRIMARY KEY,
    deployment_id INTEGER REFERENCES deployments(id),
    env_id        INTEGER NOT NULL REFERENCES environments(id),
    -- group_id ties jobs into one ordered sequence; seq is the position within
    -- it. A sequence runs strictly in order and stalls where it fails, which is
    -- what stops a sanitize running against a database that never loaded.
    group_id      TEXT    NOT NULL,
    seq           INTEGER NOT NULL,
    kind          TEXT    NOT NULL,
    k8s_job_name  TEXT    NOT NULL UNIQUE,
    state         TEXT    NOT NULL,
    reason        TEXT    NOT NULL DEFAULT '',
    payload       BLOB,
    created_at    TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at    TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS audit (
    id     INTEGER PRIMARY KEY,
    actor  TEXT NOT NULL,
    action TEXT NOT NULL,
    target TEXT NOT NULL,
    detail BLOB,
    at     TEXT NOT NULL DEFAULT (datetime('now'))
);

-- deploy_log records each code deployment so a rollback knows what was running
-- before and which pre-deploy snapshot to restore. It is the pragmatic
-- stand-in for the releases/deployments tables above, which wait on a GitHub
-- Actions integration that records git provenance.
CREATE TABLE IF NOT EXISTS deploy_log (
    id          INTEGER PRIMARY KEY,
    env_id      INTEGER NOT NULL REFERENCES environments(id),
    from_image  TEXT    NOT NULL DEFAULT '',   -- what was running before this deploy
    to_image    TEXT    NOT NULL,              -- what this deploy put in place
    snapshot_id INTEGER REFERENCES snapshots(id),  -- the pre-deploy database snapshot, if one was taken
    rolled_back INTEGER NOT NULL DEFAULT 0,     -- set once this deploy has been rolled back
    actor       TEXT    NOT NULL DEFAULT '',
    at          TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_deploy_log_env ON deploy_log(env_id, at DESC);

-- schedules are recurring operations slipway runs itself while `serve` is up:
-- a nightly copy-down into dev, a periodic snapshot, drush cron. One operation
-- per row, its arguments in typed columns rather than a blob so `slipway
-- schedules` is readable.
CREATE TABLE IF NOT EXISTS schedules (
    id          INTEGER PRIMARY KEY,
    name        TEXT    NOT NULL UNIQUE,
    spec        TEXT    NOT NULL,              -- five-field cron
    op          TEXT    NOT NULL,              -- snapshot | copy-down | console
    env         TEXT    NOT NULL DEFAULT '',   -- snapshot, console
    from_env    TEXT    NOT NULL DEFAULT '',   -- copy-down
    to_env      TEXT    NOT NULL DEFAULT '',   -- copy-down
    skip_files  INTEGER NOT NULL DEFAULT 0,    -- copy-down
    skip_db     INTEGER NOT NULL DEFAULT 0,    -- copy-down
    cmd         TEXT    NOT NULL DEFAULT '',   -- console
    shell       INTEGER NOT NULL DEFAULT 0,    -- console
    enabled     INTEGER NOT NULL DEFAULT 1,
    last_run    TEXT,                          -- NULL until it has fired once
    last_status TEXT    NOT NULL DEFAULT '',
    created_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- The reconciler's hot path: every job not yet in a terminal state.
CREATE INDEX IF NOT EXISTS idx_jobs_unfinished
    ON jobs(state) WHERE state NOT IN ('succeeded', 'failed', 'cancelled', 'orphaned');

CREATE INDEX IF NOT EXISTS idx_deployments_env ON deployments(env_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_snapshots_env   ON snapshots(env_id, created_at DESC);
