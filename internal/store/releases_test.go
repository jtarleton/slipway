package store

import (
	"database/sql"
	"errors"
	"strings"
	"testing"
)

func rel(digest string) Release {
	return Release{
		GitSHA: "abc1234deadbeef", GitRef: "refs/tags/v2.1.0",
		ImageRef: "ghcr.io/x/d@" + digest, ImageDigest: digest, BuiltAt: "2026-09-07T10:00:00Z",
	}
}

func TestCreateReleaseIsIdempotentOnDigest(t *testing.T) {
	db := open(t)
	digest := "sha256:" + strings.Repeat("a", 64)

	first, err := db.CreateRelease(rel(digest))
	if err != nil {
		t.Fatalf("CreateRelease: %v", err)
	}
	// A retry from CI with the same digest but a moved tag updates in place.
	r2 := rel(digest)
	r2.GitRef = "refs/heads/main"
	second, err := db.CreateRelease(r2)
	if err != nil {
		t.Fatalf("CreateRelease retry: %v", err)
	}
	if first != second {
		t.Fatalf("retry created a second row: %d then %d", first, second)
	}

	list, err := db.Releases(10)
	if err != nil || len(list) != 1 {
		t.Fatalf("Releases = %d (err %v), want 1", len(list), err)
	}
	if list[0].GitRef != "refs/heads/main" {
		t.Errorf("GitRef = %q, want the updated ref", list[0].GitRef)
	}
}

func TestCreateReleaseRequiresADigestAndBuildTime(t *testing.T) {
	db := open(t)
	if _, err := db.CreateRelease(Release{ImageRef: "x", BuiltAt: "t"}); err == nil {
		t.Error("CreateRelease accepted a release with no digest")
	}
	if _, err := db.CreateRelease(Release{ImageRef: "x", ImageDigest: "sha256:x"}); err == nil {
		t.Error("CreateRelease accepted a release with no build time")
	}
}

func TestReleaseByRefMatchesShortAndFullRefs(t *testing.T) {
	db := open(t)
	digest := "sha256:" + strings.Repeat("b", 64)
	if _, err := db.CreateRelease(rel(digest)); err != nil {
		t.Fatalf("CreateRelease: %v", err)
	}

	for _, ref := range []string{"v2.1.0", "refs/tags/v2.1.0"} {
		got, err := db.ReleaseByRef(ref)
		if err != nil {
			t.Fatalf("ReleaseByRef(%q): %v", ref, err)
		}
		if got.ImageDigest != digest {
			t.Errorf("ReleaseByRef(%q) = %q, want %q", ref, got.ImageDigest, digest)
		}
	}
	if _, err := db.ReleaseByRef("nope"); !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("ReleaseByRef(nope) err = %v, want sql.ErrNoRows", err)
	}
}

func TestReleaseByDigest(t *testing.T) {
	db := open(t)
	digest := "sha256:" + strings.Repeat("c", 64)
	if _, err := db.CreateRelease(rel(digest)); err != nil {
		t.Fatalf("CreateRelease: %v", err)
	}
	got, err := db.ReleaseByDigest(digest)
	if err != nil || got.GitRef != "refs/tags/v2.1.0" {
		t.Fatalf("ReleaseByDigest = %+v, %v", got, err)
	}
	if _, err := db.ReleaseByDigest("sha256:unknown"); !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("ReleaseByDigest(unknown) err = %v, want sql.ErrNoRows", err)
	}
}

// The migration adds deploy_log.release_id; a second Open must not choke on it
// already existing.
func TestMigrationsAreIdempotent(t *testing.T) {
	path := t.TempDir() + "/m.db"
	for i := 0; i < 3; i++ {
		db, err := Open(path)
		if err != nil {
			t.Fatalf("Open pass %d: %v", i, err)
		}
		db.Close()
	}

	db, _ := Open(path)
	defer db.Close()
	envID, _ := db.UpsertEnvironment(Environment{Name: "e", Rank: 1, Namespace: "n", IngressHost: "h"})
	relID, _ := db.CreateRelease(rel("sha256:" + strings.Repeat("d", 64)))
	if _, err := db.LogDeploy(envID, "", "img@sha256:x", nil, &relID, "t"); err != nil {
		t.Fatalf("LogDeploy with a release_id: %v", err)
	}
	dep, err := db.LastDeploy(envID)
	if err != nil || dep.ReleaseID == nil || *dep.ReleaseID != relID {
		t.Fatalf("LastDeploy = %+v (err %v), want release_id %d", dep, err, relID)
	}
}
