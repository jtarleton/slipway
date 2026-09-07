package store

import (
	"database/sql"
	"fmt"
)

// Release is an immutable built artifact: an image, and the git commit it came
// from. CI records one after every build; a deployment references one that
// already exists, which is what makes promotion cheap.
type Release struct {
	ID          int64
	GitSHA      string
	GitRef      string // refs/heads/main, refs/tags/v2.1.0
	ImageRef    string // repo@sha256:… — what a deploy actually patches
	ImageDigest string // sha256:… — the bare digest, unique per build
	BuiltAt     string
	CreatedAt   string
}

// CreateRelease records a build. It is idempotent on the image digest: CI that
// retries a notification, or two refs pointing at one build, collapse to one
// row rather than erroring.
func (d *DB) CreateRelease(r Release) (int64, error) {
	if r.ImageDigest == "" || r.ImageRef == "" {
		return 0, fmt.Errorf("create release: image ref and digest are required")
	}
	if r.BuiltAt == "" {
		return 0, fmt.Errorf("create release: built_at is required")
	}

	res, err := d.Exec(`
		INSERT INTO releases (git_sha, git_ref, image_digest, image_ref, built_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(image_digest) DO UPDATE SET
			git_sha  = excluded.git_sha,
			git_ref  = excluded.git_ref,
			image_ref = excluded.image_ref,
			built_at = excluded.built_at`,
		r.GitSHA, r.GitRef, r.ImageDigest, r.ImageRef, r.BuiltAt)
	if err != nil {
		return 0, fmt.Errorf("create release %s: %w", r.ImageDigest, err)
	}
	if id, err := res.LastInsertId(); err == nil && id > 0 {
		return id, nil
	}
	var id int64
	if err := d.QueryRow(`SELECT id FROM releases WHERE image_digest = ?`, r.ImageDigest).Scan(&id); err != nil {
		return 0, fmt.Errorf("resolve release %s: %w", r.ImageDigest, err)
	}
	return id, nil
}

// Releases lists recorded releases, newest build first.
func (d *DB) Releases(limit int) ([]Release, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := d.Query(`
		SELECT id, git_sha, git_ref, image_ref, image_digest, built_at, created_at
		FROM releases ORDER BY built_at DESC, id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("list releases: %w", err)
	}
	defer rows.Close()
	return scanReleases(rows)
}

// ReleaseByDigest finds the release for an image digest, if one was recorded.
// sql.ErrNoRows means the digest running somewhere did not come through CI.
func (d *DB) ReleaseByDigest(digest string) (Release, error) {
	rows, err := d.Query(`
		SELECT id, git_sha, git_ref, image_ref, image_digest, built_at, created_at
		FROM releases WHERE image_digest = ?`, digest)
	if err != nil {
		return Release{}, fmt.Errorf("release by digest: %w", err)
	}
	defer rows.Close()
	list, err := scanReleases(rows)
	if err != nil {
		return Release{}, err
	}
	if len(list) == 0 {
		return Release{}, sql.ErrNoRows
	}
	return list[0], nil
}

// ReleaseByRef finds the most recent release built from a git ref. The ref may
// be given in full (refs/tags/v2.1.0) or short (v2.1.0, main).
func (d *DB) ReleaseByRef(ref string) (Release, error) {
	rows, err := d.Query(`
		SELECT id, git_sha, git_ref, image_ref, image_digest, built_at, created_at
		FROM releases
		WHERE git_ref = ? OR git_ref = 'refs/tags/' || ? OR git_ref = 'refs/heads/' || ?
		ORDER BY built_at DESC, id DESC LIMIT 1`, ref, ref, ref)
	if err != nil {
		return Release{}, fmt.Errorf("release by ref: %w", err)
	}
	defer rows.Close()
	list, err := scanReleases(rows)
	if err != nil {
		return Release{}, err
	}
	if len(list) == 0 {
		return Release{}, sql.ErrNoRows
	}
	return list[0], nil
}

func scanReleases(rows *sql.Rows) ([]Release, error) {
	var out []Release
	for rows.Next() {
		var r Release
		if err := rows.Scan(&r.ID, &r.GitSHA, &r.GitRef, &r.ImageRef, &r.ImageDigest, &r.BuiltAt, &r.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan release: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
