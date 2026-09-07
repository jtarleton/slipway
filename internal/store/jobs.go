package store

import (
	"fmt"

	"github.com/jtarleton/slipway/internal/jobs"
)

// Job is a persisted job record — the durable half of the pair the engine keeps
// in agreement with the cluster.
type Job struct {
	ID           int64
	DeploymentID *int64
	EnvID        int64
	GroupID      string
	Seq          int
	Kind         jobs.Kind
	K8sJobName   string
	State        jobs.State
	Reason       string

	// Payload carries the operation's arguments — which environment to copy
	// from, which object key to read. Never credentials: those are mounted from
	// Kubernetes Secrets at run time so the store stays safe to back up.
	Payload []byte
}

// CreateJob writes a job in the Pending state with its Kubernetes name already
// assigned.
//
// The ordering is the point: the name is durable before anything is submitted,
// so a crash before submission leaves a row the reconciler can find and retry,
// and a crash after submission leaves a row whose name still matches the Job
// now running in the cluster.
func (d *DB) CreateJob(j Job) (int64, error) {
	if !j.Kind.Valid() {
		return 0, fmt.Errorf("create job: unknown kind %q", j.Kind)
	}
	if j.K8sJobName == "" {
		return 0, fmt.Errorf("create job: k8s job name must be assigned before insert")
	}

	res, err := d.Exec(`
		INSERT INTO jobs (deployment_id, env_id, group_id, seq, kind, k8s_job_name, state, payload)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		j.DeploymentID, j.EnvID, j.GroupID, j.Seq, string(j.Kind), j.K8sJobName, string(jobs.Pending), j.Payload)
	if err != nil {
		return 0, fmt.Errorf("create job %s: %w", j.K8sJobName, err)
	}
	return res.LastInsertId()
}

// Unfinished returns every job the reconciler still owes an answer for, oldest
// first. This is what runs on boot.
func (d *DB) Unfinished() ([]Job, error) {
	rows, err := d.Query(`
		SELECT id, deployment_id, env_id, group_id, seq, kind, k8s_job_name, state, reason, payload
		FROM jobs
		WHERE state NOT IN ('succeeded', 'failed', 'cancelled', 'orphaned')
		ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list unfinished jobs: %w", err)
	}
	defer rows.Close()

	var out []Job
	for rows.Next() {
		var j Job
		var kind, state string
		if err := rows.Scan(&j.ID, &j.DeploymentID, &j.EnvID, &j.GroupID, &j.Seq, &kind, &j.K8sJobName, &state, &j.Reason, &j.Payload); err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		j.Kind, j.State = jobs.Kind(kind), jobs.State(state)
		out = append(out, j)
	}
	return out, rows.Err()
}

// JobRow is a job with the timestamps the UI needs to show progress over time.
// The engine works in Job values; this is strictly a read model for display.
type JobRow struct {
	Job
	CreatedAt string
	UpdatedAt string
}

// Recent returns the most recently touched jobs, newest first, terminal ones
// included. This is what the web UI's activity table renders.
func (d *DB) Recent(limit int) ([]JobRow, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := d.Query(`
		SELECT id, deployment_id, env_id, group_id, seq, kind, k8s_job_name, state, reason, payload,
		       created_at, updated_at
		FROM jobs
		ORDER BY updated_at DESC, id DESC
		LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("list recent jobs: %w", err)
	}
	defer rows.Close()

	var out []JobRow
	for rows.Next() {
		var r JobRow
		var kind, state string
		if err := rows.Scan(&r.ID, &r.DeploymentID, &r.EnvID, &r.GroupID, &r.Seq, &kind,
			&r.K8sJobName, &state, &r.Reason, &r.Payload, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		r.Kind, r.State = jobs.Kind(kind), jobs.State(state)
		out = append(out, r)
	}
	return out, rows.Err()
}

// Runnable returns the jobs the engine may act on right now: within each
// group, the earliest unfinished job whose predecessors have all succeeded.
//
// A group whose earlier job failed returns nothing and stalls there. That is
// the point — it is what stops a sanitize from running against a database that
// never finished loading, which would otherwise look like a clean stage
// environment holding production data.
func (d *DB) Runnable() ([]Job, error) {
	rows, err := d.Query(`
		SELECT j.id, j.deployment_id, j.env_id, j.group_id, j.seq, j.kind,
		       j.k8s_job_name, j.state, j.reason, j.payload
		FROM jobs j
		WHERE j.state NOT IN ('succeeded', 'failed', 'cancelled', 'orphaned')
		  AND NOT EXISTS (
		      SELECT 1 FROM jobs p
		      WHERE p.group_id = j.group_id
		        AND p.seq < j.seq
		        AND p.state != 'succeeded'
		  )
		ORDER BY j.group_id, j.seq`)
	if err != nil {
		return nil, fmt.Errorf("list runnable jobs: %w", err)
	}
	defer rows.Close()

	var out []Job
	for rows.Next() {
		var j Job
		var kind, state string
		if err := rows.Scan(&j.ID, &j.DeploymentID, &j.EnvID, &j.GroupID, &j.Seq, &kind,
			&j.K8sJobName, &state, &j.Reason, &j.Payload); err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		j.Kind, j.State = jobs.Kind(kind), jobs.State(state)
		out = append(out, j)
	}
	return out, rows.Err()
}

// Advance moves a job to next, refusing any transition the state machine does
// not allow.
//
// The guard is in the WHERE clause as well as in Go: two reconcile passes
// racing on the same job must not both apply, and the database is the only
// place that can settle it.
func (d *DB) Advance(id int64, from, to jobs.State, reason string) error {
	if !jobs.CanTransition(from, to) {
		return fmt.Errorf("advance job %d: illegal transition %s -> %s", id, from, to)
	}

	res, err := d.Exec(`
		UPDATE jobs SET state = ?, reason = ?, updated_at = datetime('now')
		WHERE id = ? AND state = ?`,
		string(to), reason, id, string(from))
	if err != nil {
		return fmt.Errorf("advance job %d: %w", id, err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("advance job %d: %w", id, err)
	}
	if n == 0 {
		return fmt.Errorf("advance job %d: no longer in state %s", id, from)
	}
	return nil
}
