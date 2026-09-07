package store

import (
	"database/sql"
	"fmt"
)

// Snapshot is a database dump sitting in object storage.
type Snapshot struct {
	ID        int64
	EnvID     int64
	ObjectKey string
	Sanitized bool
	CreatedAt string
}

// RecordSnapshot writes a snapshot row. The Job that produced it has already
// uploaded the dump; this is just the control plane remembering where it went.
func (d *DB) RecordSnapshot(envID int64, objectKey string, sanitized bool) (int64, error) {
	res, err := d.Exec(`
		INSERT INTO snapshots (env_id, object_key, sanitized)
		VALUES (?, ?, ?)`,
		envID, objectKey, sanitized)
	if err != nil {
		return 0, fmt.Errorf("record snapshot %s: %w", objectKey, err)
	}
	return res.LastInsertId()
}

// SnapshotsFor lists an environment's snapshots, newest first.
func (d *DB) SnapshotsFor(envID int64) ([]Snapshot, error) {
	rows, err := d.Query(`
		SELECT id, env_id, object_key, sanitized, created_at
		FROM snapshots WHERE env_id = ? ORDER BY created_at DESC, id DESC`, envID)
	if err != nil {
		return nil, fmt.Errorf("list snapshots for env %d: %w", envID, err)
	}
	defer rows.Close()

	var out []Snapshot
	for rows.Next() {
		var s Snapshot
		var sanitized int
		if err := rows.Scan(&s.ID, &s.EnvID, &s.ObjectKey, &sanitized, &s.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan snapshot: %w", err)
		}
		s.Sanitized = sanitized != 0
		out = append(out, s)
	}
	return out, rows.Err()
}

// Snapshot resolves one snapshot by id.
func (d *DB) Snapshot(id int64) (Snapshot, error) {
	var s Snapshot
	var sanitized int
	err := d.QueryRow(`
		SELECT id, env_id, object_key, sanitized, created_at
		FROM snapshots WHERE id = ?`, id).
		Scan(&s.ID, &s.EnvID, &s.ObjectKey, &sanitized, &s.CreatedAt)
	if err != nil {
		return Snapshot{}, fmt.Errorf("snapshot %d: %w", id, err)
	}
	s.Sanitized = sanitized != 0
	return s, nil
}

// Deploy is one row of the deploy log — enough to undo a deployment.
type Deploy struct {
	ID         int64
	EnvID      int64
	FromImage  string
	ToImage    string
	SnapshotID *int64
	RolledBack bool
	Actor      string
	At         string
}

// LogDeploy records a deployment. from is what was running before, to is what
// was just put in place, snapshotID is the pre-deploy database snapshot if one
// was taken (nil otherwise).
func (d *DB) LogDeploy(envID int64, from, to string, snapshotID *int64, actor string) (int64, error) {
	if to == "" {
		return 0, fmt.Errorf("log deploy: no target image")
	}
	res, err := d.Exec(`
		INSERT INTO deploy_log (env_id, from_image, to_image, snapshot_id, actor)
		VALUES (?, ?, ?, ?, ?)`,
		envID, from, to, snapshotID, actor)
	if err != nil {
		return 0, fmt.Errorf("log deploy to env %d: %w", envID, err)
	}
	return res.LastInsertId()
}

// LastDeploy returns an environment's most recent deployment that has not
// already been rolled back — the one a rollback would undo. sql.ErrNoRows means
// there is nothing to roll back to.
func (d *DB) LastDeploy(envID int64) (Deploy, error) {
	var dep Deploy
	var snap sql.NullInt64
	var rolled int
	err := d.QueryRow(`
		SELECT id, env_id, from_image, to_image, snapshot_id, rolled_back, actor, at
		FROM deploy_log
		WHERE env_id = ? AND rolled_back = 0
		ORDER BY at DESC, id DESC
		LIMIT 1`, envID).
		Scan(&dep.ID, &dep.EnvID, &dep.FromImage, &dep.ToImage, &snap, &rolled, &dep.Actor, &dep.At)
	if err != nil {
		return Deploy{}, err
	}
	if snap.Valid {
		dep.SnapshotID = &snap.Int64
	}
	dep.RolledBack = rolled != 0
	return dep, nil
}

// MarkRolledBack flags a deploy-log row as undone, so the next rollback targets
// the deployment before it rather than this one again.
func (d *DB) MarkRolledBack(id int64) error {
	_, err := d.Exec(`UPDATE deploy_log SET rolled_back = 1 WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("mark deploy %d rolled back: %w", id, err)
	}
	return nil
}
