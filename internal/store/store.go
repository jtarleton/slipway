// Package store is Slipway's control-plane state: a single SQLite file holding
// environments, releases, deployments, snapshots and jobs.
//
// SQLite means one writer, which means one control-plane replica. That is a
// deliberate trade. Availability here comes from making restarts boring — the
// reconciler picks up whatever was in flight — not from running two copies.
package store

import (
	"database/sql"
	_ "embed"
	"fmt"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

// DB wraps the control-plane database.
type DB struct {
	*sql.DB
}

// Open opens (or creates) the store at path and applies the schema.
//
// The connection pool is capped at one. SQLite in WAL mode tolerates concurrent
// readers, but Go's pool will happily open a second writer and earn a
// SQLITE_BUSY under load; serializing here costs nothing at Slipway's scale and
// removes a class of intermittent failure.
func Open(path string) (*DB, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	db.SetMaxOpenConns(1)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping %s: %w", path, err)
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return &DB{db}, nil
}

// Environment is one deployment target: a namespace, a hostname, and a position
// in the promotion order.
type Environment struct {
	ID           int64
	Name         string
	Rank         int
	Namespace    string
	IngressHost  string
	IsProduction bool
}

// UpsertEnvironment registers an environment or updates it in place, keyed on
// name. Registration is where InnoDB gets asserted — see the assumptions in the
// README — so this is deliberately explicit rather than inferred from a scan of
// the cluster.
func (d *DB) UpsertEnvironment(e Environment) (int64, error) {
	res, err := d.Exec(`
		INSERT INTO environments (name, rank, namespace, ingress_host, is_production)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			rank          = excluded.rank,
			namespace     = excluded.namespace,
			ingress_host  = excluded.ingress_host,
			is_production = excluded.is_production`,
		e.Name, e.Rank, e.Namespace, e.IngressHost, e.IsProduction)
	if err != nil {
		return 0, fmt.Errorf("upsert environment %s: %w", e.Name, err)
	}
	if id, err := res.LastInsertId(); err == nil && id > 0 {
		return id, nil
	}
	var id int64
	if err := d.QueryRow(`SELECT id FROM environments WHERE name = ?`, e.Name).Scan(&id); err != nil {
		return 0, fmt.Errorf("resolve environment %s: %w", e.Name, err)
	}
	return id, nil
}

// Environments returns every environment in promotion order — the left-to-right
// order of the grid.
func (d *DB) Environments() ([]Environment, error) {
	rows, err := d.Query(`
		SELECT id, name, rank, namespace, ingress_host, is_production
		FROM environments ORDER BY rank`)
	if err != nil {
		return nil, fmt.Errorf("list environments: %w", err)
	}
	defer rows.Close()

	var out []Environment
	for rows.Next() {
		var e Environment
		if err := rows.Scan(&e.ID, &e.Name, &e.Rank, &e.Namespace, &e.IngressHost, &e.IsProduction); err != nil {
			return nil, fmt.Errorf("scan environment: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
