package store

import (
	"encoding/json"
	"fmt"
)

// AuditEntry is one line of the operation history: who ran what against which
// environment, and how it turned out.
type AuditEntry struct {
	ID     int64
	Actor  string
	Action string // deploy, snapshot, rollback, copy-down, …
	Target string // usually an environment name
	Detail json.RawMessage
	At     string
}

// Record appends an audit entry. detail is an opaque JSON blob the caller
// shapes — arguments, outcome, an error string.
func (d *DB) Record(actor, action, target string, detail []byte) error {
	if action == "" {
		return fmt.Errorf("audit record: no action")
	}
	if actor == "" {
		actor = "unknown"
	}
	if len(detail) == 0 {
		detail = []byte("{}")
	}
	_, err := d.Exec(`
		INSERT INTO audit (actor, action, target, detail)
		VALUES (?, ?, ?, ?)`,
		actor, action, target, detail)
	if err != nil {
		return fmt.Errorf("audit record %s: %w", action, err)
	}
	return nil
}

// History returns the most recent audit entries, newest first.
func (d *DB) History(limit int) ([]AuditEntry, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := d.Query(`
		SELECT id, actor, action, target, detail, at
		FROM audit ORDER BY at DESC, id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("read audit log: %w", err)
	}
	defer rows.Close()

	var out []AuditEntry
	for rows.Next() {
		var e AuditEntry
		var detail []byte
		if err := rows.Scan(&e.ID, &e.Actor, &e.Action, &e.Target, &detail, &e.At); err != nil {
			return nil, fmt.Errorf("scan audit entry: %w", err)
		}
		if len(detail) == 0 {
			detail = []byte("{}")
		}
		e.Detail = detail
		out = append(out, e)
	}
	return out, rows.Err()
}
