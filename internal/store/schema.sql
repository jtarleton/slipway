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

-- The reconciler's hot path: every job not yet in a terminal state.
CREATE INDEX IF NOT EXISTS idx_jobs_unfinished
    ON jobs(state) WHERE state NOT IN ('succeeded', 'failed', 'cancelled', 'orphaned');

CREATE INDEX IF NOT EXISTS idx_deployments_env ON deployments(env_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_snapshots_env   ON snapshots(env_id, created_at DESC);
