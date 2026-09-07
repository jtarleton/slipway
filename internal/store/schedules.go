package store

import (
	"database/sql"
	"fmt"
)

// Schedule is one recurring operation. Exactly the fields for its Op are set;
// the rest are zero.
type Schedule struct {
	ID   int64
	Name string
	Spec string // five-field cron
	Op   string // snapshot | copy-down | console

	Env       string // snapshot, console
	From      string // copy-down
	To        string // copy-down
	SkipFiles bool   // copy-down
	SkipDB    bool   // copy-down
	Cmd       string // console
	Shell     bool   // console

	Enabled    bool
	LastRun    string // "" until it has fired
	LastStatus string
	CreatedAt  string
}

// UpsertSchedule creates a schedule or replaces it in place, keyed on name.
// Editing a schedule clears its run history so the evaluator treats the new
// spec fresh.
func (d *DB) UpsertSchedule(s Schedule) (int64, error) {
	if s.Name == "" || s.Spec == "" || s.Op == "" {
		return 0, fmt.Errorf("schedule needs a name, a spec and an op")
	}
	res, err := d.Exec(`
		INSERT INTO schedules (name, spec, op, env, from_env, to_env, skip_files, skip_db, cmd, shell, enabled)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
		ON CONFLICT(name) DO UPDATE SET
			spec = excluded.spec, op = excluded.op, env = excluded.env,
			from_env = excluded.from_env, to_env = excluded.to_env,
			skip_files = excluded.skip_files, skip_db = excluded.skip_db,
			cmd = excluded.cmd, shell = excluded.shell,
			last_run = NULL, last_status = ''`,
		s.Name, s.Spec, s.Op, s.Env, s.From, s.To,
		b2i(s.SkipFiles), b2i(s.SkipDB), s.Cmd, b2i(s.Shell))
	if err != nil {
		return 0, fmt.Errorf("upsert schedule %s: %w", s.Name, err)
	}
	if id, err := res.LastInsertId(); err == nil && id > 0 {
		return id, nil
	}
	var id int64
	if err := d.QueryRow(`SELECT id FROM schedules WHERE name = ?`, s.Name).Scan(&id); err != nil {
		return 0, fmt.Errorf("resolve schedule %s: %w", s.Name, err)
	}
	return id, nil
}

// Schedules lists every schedule, name order.
func (d *DB) Schedules() ([]Schedule, error) {
	rows, err := d.Query(`
		SELECT id, name, spec, op, env, from_env, to_env, skip_files, skip_db, cmd, shell,
		       enabled, last_run, last_status, created_at
		FROM schedules ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list schedules: %w", err)
	}
	defer rows.Close()

	var out []Schedule
	for rows.Next() {
		var s Schedule
		var skipFiles, skipDB, shell, enabled int
		var lastRun sql.NullString
		if err := rows.Scan(&s.ID, &s.Name, &s.Spec, &s.Op, &s.Env, &s.From, &s.To,
			&skipFiles, &skipDB, &s.Cmd, &shell, &enabled, &lastRun, &s.LastStatus, &s.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan schedule: %w", err)
		}
		s.SkipFiles, s.SkipDB, s.Shell, s.Enabled = skipFiles != 0, skipDB != 0, shell != 0, enabled != 0
		s.LastRun = lastRun.String
		out = append(out, s)
	}
	return out, rows.Err()
}

// ScheduleByName resolves one schedule; sql.ErrNoRows if there is no such name.
func (d *DB) ScheduleByName(name string) (Schedule, error) {
	all, err := d.Schedules()
	if err != nil {
		return Schedule{}, err
	}
	for _, s := range all {
		if s.Name == name {
			return s, nil
		}
	}
	return Schedule{}, sql.ErrNoRows
}

// SetScheduleEnabled turns a schedule on or off.
func (d *DB) SetScheduleEnabled(name string, enabled bool) error {
	res, err := d.Exec(`UPDATE schedules SET enabled = ? WHERE name = ?`, b2i(enabled), name)
	if err != nil {
		return fmt.Errorf("set schedule %s enabled: %w", name, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("no schedule named %q", name)
	}
	return nil
}

// DeleteSchedule removes a schedule.
func (d *DB) DeleteSchedule(name string) error {
	res, err := d.Exec(`DELETE FROM schedules WHERE name = ?`, name)
	if err != nil {
		return fmt.Errorf("delete schedule %s: %w", name, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("no schedule named %q", name)
	}
	return nil
}

// RecordScheduleRun stamps a schedule with when it last fired and how it went.
func (d *DB) RecordScheduleRun(name, at, status string) error {
	_, err := d.Exec(`UPDATE schedules SET last_run = ?, last_status = ? WHERE name = ?`, at, status, name)
	if err != nil {
		return fmt.Errorf("record schedule run %s: %w", name, err)
	}
	return nil
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}
