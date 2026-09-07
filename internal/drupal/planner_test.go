package drupal

import (
	"strings"
	"testing"

	"github.com/jtarleton/slipway/internal/jobs"
	"github.com/jtarleton/slipway/internal/store"
)

var (
	prod  = store.Environment{ID: 1, Name: "prod", Namespace: "jt-drupal", IsProduction: true}
	stage = store.Environment{ID: 2, Name: "stage", Namespace: "jt-drupal-stage"}
)

func planner() *Planner {
	return NewPlanner([]store.Environment{prod, stage}, DefaultImages())
}

func jobWith(kind jobs.Kind, envID int64, p Params) store.Job {
	payload, err := p.Encode()
	if err != nil {
		panic(err)
	}
	return store.Job{ID: 1, EnvID: envID, Kind: kind, Payload: payload}
}

func TestCopyDatabaseTargetsTheRightHosts(t *testing.T) {
	spec, err := planner().SpecFor(jobWith(jobs.KindRestore, stage.ID, Params{
		SourceNamespace: prod.Namespace,
	}))
	if err != nil {
		t.Fatalf("SpecFor: %v", err)
	}

	if got := spec.Env["SRC_HOST"]; got != "db.jt-drupal.svc.cluster.local" {
		t.Errorf("SRC_HOST = %q, want prod's database", got)
	}
	if got := spec.Env["DST_HOST"]; got != "db" {
		t.Errorf("DST_HOST = %q, want the local service", got)
	}
	if spec.EnvFromSecret != SecretDB {
		t.Errorf("EnvFromSecret = %q, want %q", spec.EnvFromSecret, SecretDB)
	}
}

// Without pipefail a failed mysqldump exits zero through the pipe, and the
// target is left half-loaded looking like a success.
func TestCopyDatabaseFailsOnADroppedPipe(t *testing.T) {
	spec, err := planner().SpecFor(jobWith(jobs.KindRestore, stage.ID, Params{
		SourceNamespace: prod.Namespace,
	}))
	if err != nil {
		t.Fatalf("SpecFor: %v", err)
	}
	if !contains(spec.Command, "pipefail") {
		t.Errorf("copy command does not set pipefail: %v", spec.Command)
	}
}

// Re-running a half-finished restore leaves the target worse than a loud
// failure would.
func TestCopyDatabaseNeverRetries(t *testing.T) {
	spec, _ := planner().SpecFor(jobWith(jobs.KindRestore, stage.ID, Params{
		SourceNamespace: prod.Namespace,
	}))
	if spec.BackoffLimit != 0 {
		t.Errorf("BackoffLimit = %d, want 0 for a restore", spec.BackoffLimit)
	}
}

func TestCopyDatabaseRejectsAMissingOrSelfSource(t *testing.T) {
	p := planner()

	if _, err := p.SpecFor(jobWith(jobs.KindRestore, stage.ID, Params{})); err == nil {
		t.Error("planned a copy with no source namespace")
	}
	if _, err := p.SpecFor(jobWith(jobs.KindRestore, stage.ID, Params{
		SourceNamespace: stage.Namespace,
	})); err == nil {
		t.Error("planned a copy from an environment into itself")
	}
}

// The guard that matters most: sanitize destroys data, so pointing it at
// production must be impossible rather than merely discouraged.
func TestSanitizeRefusesProduction(t *testing.T) {
	_, err := planner().SpecFor(jobWith(jobs.KindSanitize, prod.ID, Params{
		DrupalImage: "ghcr.io/jtarleton/d11app@sha256:abc",
	}))
	if err == nil {
		t.Fatal("planner produced a sanitize job targeting production")
	}
	if !strings.Contains(err.Error(), "production") {
		t.Errorf("error = %q, want it to name production as the reason", err)
	}
}

func TestSanitizeRunsDrushFromTheSitesOwnImage(t *testing.T) {
	image := "ghcr.io/jtarleton/d11app@sha256:4d367e"
	spec, err := planner().SpecFor(jobWith(jobs.KindSanitize, stage.ID, Params{DrupalImage: image}))
	if err != nil {
		t.Fatalf("SpecFor: %v", err)
	}
	if spec.Image != image {
		t.Errorf("Image = %q, want drush to run from the code it belongs with", spec.Image)
	}
	if !contains(spec.Command, "sql:sanitize") {
		t.Errorf("command does not sanitize: %v", spec.Command)
	}

	// settings.php must be mounted or drush cannot find the database.
	var settings bool
	for _, m := range spec.Mounts {
		if m.SubPath == PathSettings {
			settings = true
		}
	}
	if !settings {
		t.Errorf("settings.php is not mounted; drush will not bootstrap: %+v", spec.Mounts)
	}
}

func TestSanitizeNeedsAnImage(t *testing.T) {
	if _, err := planner().SpecFor(jobWith(jobs.KindSanitize, stage.ID, Params{})); err == nil {
		t.Error("planned a sanitize with no image to run drush from")
	}
}

func TestSyncFilesDirectionChangesTheCommand(t *testing.T) {
	key := "s3://amazon-jtarleton-s3/s3fs-private/slipway/prod/app"

	push, err := planner().SpecFor(jobWith(jobs.KindSyncFiles, prod.ID, Params{ObjectKey: key, Push: true}))
	if err != nil {
		t.Fatalf("push SpecFor: %v", err)
	}
	pull, err := planner().SpecFor(jobWith(jobs.KindSyncFiles, stage.ID, Params{ObjectKey: key}))
	if err != nil {
		t.Fatalf("pull SpecFor: %v", err)
	}

	if !contains(push.Command, `sync /data "$OBJECT_KEY"`) {
		t.Errorf("push does not sync the volume upward: %v", push.Command)
	}
	if !contains(pull.Command, `sync "$OBJECT_KEY" /data`) {
		t.Errorf("pull does not sync the volume downward: %v", pull.Command)
	}
	// Clearing is opt-in: needed the first time an environment is seeded,
	// wasteful on every refresh afterwards since sync is incremental.
	if contains(pull.Command, "rm -rf /data/*") {
		t.Errorf("a plain pull cleared the target tree: %v", pull.Command)
	}
	if push.Env["OBJECT_KEY"] != key || pull.Env["OBJECT_KEY"] != key {
		t.Error("both directions must address the same object")
	}

	// The whole volume moves, not the subPaths — settings.php, files and
	// private travel together or the target will not boot.
	var whole bool
	for _, m := range pull.Mounts {
		if m.PVC == VolumeApp && m.Path == "/data" && m.SubPath == "" {
			whole = true
		}
	}
	if !whole {
		t.Errorf("files lane does not mount the whole app volume: %+v", pull.Mounts)
	}
}

func TestSyncFilesNeedsAnObjectKey(t *testing.T) {
	if _, err := planner().SpecFor(jobWith(jobs.KindSyncFiles, stage.ID, Params{Push: true})); err == nil {
		t.Error("planned a files sync with no object key")
	}
}

func TestSnapshotDumpsWithMariadbAndUploadsWithAWS(t *testing.T) {
	spec, err := planner().SpecFor(jobWith(jobs.KindSnapshot, prod.ID, Params{
		SnapshotKey: "s3://bucket/slipway/prod/db/123.sql.gz",
	}))
	if err != nil {
		t.Fatalf("SpecFor: %v", err)
	}

	if spec.Init == nil {
		t.Fatal("snapshot has no init container to run the dump")
	}
	if spec.Init.Image != DefaultImages().MariaDB {
		t.Errorf("dump runs in %q, want the mariadb image", spec.Init.Image)
	}
	if spec.Init.EnvFromSecret != SecretDB {
		t.Errorf("dump has no database secret: %q", spec.Init.EnvFromSecret)
	}
	if !contains(spec.Init.Command, "gzip") || !contains(spec.Init.Command, "/scratch/dump.sql.gz") {
		t.Errorf("dump does not gzip into scratch: %v", spec.Init.Command)
	}
	if spec.Image != DefaultImages().AWSCLI {
		t.Errorf("upload runs in %q, want the aws-cli image", spec.Image)
	}
	if spec.Env["OBJECT_KEY"] != "s3://bucket/slipway/prod/db/123.sql.gz" {
		t.Errorf("OBJECT_KEY = %q", spec.Env["OBJECT_KEY"])
	}
	if spec.Scratch != "/scratch" {
		t.Errorf("Scratch = %q", spec.Scratch)
	}
	if !contains(spec.Init.Command, "pipefail") {
		t.Error("the dump does not set pipefail — a failed mysqldump would upload a truncated file")
	}
}

func TestSnapshotNeedsAKey(t *testing.T) {
	if _, err := planner().SpecFor(jobWith(jobs.KindSnapshot, prod.ID, Params{})); err == nil {
		t.Error("planned a snapshot with no object key")
	}
}

func TestRestoreFromSnapshotDownloadsThenLoads(t *testing.T) {
	spec, err := planner().SpecFor(jobWith(jobs.KindRestore, stage.ID, Params{
		SnapshotKey: "s3://bucket/slipway/stage/db/9.sql.gz",
	}))
	if err != nil {
		t.Fatalf("SpecFor: %v", err)
	}
	if spec.Init == nil || spec.Init.Image != DefaultImages().AWSCLI {
		t.Fatalf("download does not run in the aws-cli image: %+v", spec.Init)
	}
	if len(spec.Init.Mounts) == 0 || spec.Init.Mounts[0].Secret != SecretAWS {
		t.Errorf("download init has no aws credentials: %+v", spec.Init.Mounts)
	}
	if spec.Image != DefaultImages().MariaDB {
		t.Errorf("load runs in %q, want the mariadb image", spec.Image)
	}
	if !contains(spec.Command, "gunzip") {
		t.Errorf("load does not decompress the dump: %v", spec.Command)
	}
	if spec.BackoffLimit != 0 {
		t.Errorf("a restore from snapshot retried automatically (BackoffLimit=%d)", spec.BackoffLimit)
	}
}

func TestUnknownEnvironmentAndKindAreRefused(t *testing.T) {
	p := planner()

	if _, err := p.SpecFor(store.Job{EnvID: 99, Kind: jobs.KindRestore}); err == nil {
		t.Error("planned a job for an unregistered environment")
	}
	if _, err := p.SpecFor(store.Job{EnvID: stage.ID, Kind: jobs.KindPatch}); err == nil {
		t.Error("planned a job for a kind the planner does not implement")
	}
}

func TestNamespaceFor(t *testing.T) {
	p := planner()
	if got, err := p.NamespaceFor(prod.ID); err != nil || got != "jt-drupal" {
		t.Errorf("NamespaceFor(prod) = %q, %v", got, err)
	}
	if _, err := p.NamespaceFor(99); err == nil {
		t.Error("resolved a namespace for an unknown environment")
	}
}

func contains(command []string, needle string) bool {
	return strings.Contains(strings.Join(command, " "), needle)
}

func TestUpdateRunsTheSequenceInOrder(t *testing.T) {
	spec, err := planner().SpecFor(jobWith(jobs.KindUpdatedb, stage.ID, Params{
		DrupalImage: "ghcr.io/jtarleton/d11app@sha256:4d367e",
	}))
	if err != nil {
		t.Fatalf("SpecFor: %v", err)
	}

	script := strings.Join(spec.Command, " ")
	order := []string{
		"state:set system.maintenance_mode 1",
		"updatedb -y",
		"config:import -y",
		"cache:rebuild",
		"state:set system.maintenance_mode 0",
	}

	// Scan forward rather than indexing from the start: cache:rebuild appears
	// twice, so a fresh Index would always find the first occurrence and the
	// assertion would be meaningless.
	pos := 0
	for _, step := range order {
		at := strings.Index(script[pos:], step)
		if at < 0 {
			t.Fatalf("update sequence is missing %q after position %d", step, pos)
		}
		pos += at + len(step)
	}
}

// The site must stay in maintenance mode when an update hook fails. `set -e`
// is what delivers that: the script exits before it can clear the flag.
func TestUpdateHoldsMaintenanceModeOnFailure(t *testing.T) {
	spec, _ := planner().SpecFor(jobWith(jobs.KindUpdatedb, stage.ID, Params{
		DrupalImage: "ghcr.io/jtarleton/d11app@sha256:4d367e",
	}))

	if !contains(spec.Command, "-eo") || !contains(spec.Command, "pipefail") {
		t.Fatalf("update script does not abort on error: %v", spec.Command)
	}

	script := strings.Join(spec.Command, " ")
	clear := strings.Index(script, "maintenance_mode 0")
	update := strings.Index(script, "updatedb")
	if clear < update {
		t.Error("maintenance mode is cleared before updatedb runs; a failed update would serve a half-updated database")
	}
	// A trap or a deferred cleanup would clear the flag even on failure, which
	// is exactly what must not happen.
	if strings.Contains(script, "trap ") {
		t.Error("update script installs a trap, which would clear maintenance mode on failure")
	}
}

// Update hooks are not idempotent; re-running one that half-applied turns a
// failed deploy into a corrupted one.
func TestUpdateNeverRetries(t *testing.T) {
	spec, _ := planner().SpecFor(jobWith(jobs.KindUpdatedb, stage.ID, Params{
		DrupalImage: "ghcr.io/jtarleton/d11app@sha256:4d367e",
	}))
	if spec.BackoffLimit != 0 {
		t.Errorf("BackoffLimit = %d, want 0 for update hooks", spec.BackoffLimit)
	}
}

func TestUpdateCanSkipConfigImport(t *testing.T) {
	spec, err := planner().SpecFor(jobWith(jobs.KindUpdatedb, stage.ID, Params{
		DrupalImage:      "ghcr.io/jtarleton/d11app@sha256:4d367e",
		SkipConfigImport: true,
	}))
	if err != nil {
		t.Fatalf("SpecFor: %v", err)
	}
	if contains(spec.Command, "config:import") {
		t.Error("config:import ran despite SkipConfigImport")
	}
	if !contains(spec.Command, "updatedb") {
		t.Error("skipping config:import should not skip updatedb")
	}
}

func TestSyncFilesCleanIsOptIn(t *testing.T) {
	key := "s3://amazon-jtarleton-s3/s3fs-private/slipway/prod/app"

	clean, err := planner().SpecFor(jobWith(jobs.KindSyncFiles, stage.ID, Params{ObjectKey: key, Clean: true}))
	if err != nil {
		t.Fatalf("SpecFor: %v", err)
	}
	if !contains(clean.Command, "rm -rf /data/*") {
		t.Errorf("a clean pull does not empty the target tree: %v", clean.Command)
	}
	if !contains(clean.Command, "aws s3 sync") {
		t.Errorf("a clean pull does not sync: %v", clean.Command)
	}
}

// MariaDB 11 dropped the mysqldump and mysql compatibility symlinks. Hardcoding
// either name breaks on one major version or the other, so the binaries are
// resolved at run time.
func TestCopyDatabaseResolvesTheClientBinaries(t *testing.T) {
	spec, err := planner().SpecFor(jobWith(jobs.KindRestore, stage.ID, Params{
		SourceNamespace: prod.Namespace,
	}))
	if err != nil {
		t.Fatalf("SpecFor: %v", err)
	}

	for _, name := range []string{"mariadb-dump", "mysqldump", "command -v mariadb", "command -v mysql"} {
		if !contains(spec.Command, name) {
			t.Errorf("copy script does not look for %q: %v", name, spec.Command)
		}
	}

	// --no-tablespaces is a MySQL 8 flag; MariaDB's dumper rejects it outright.
	if contains(spec.Command, "--no-tablespaces") {
		t.Error("copy script passes --no-tablespaces, which MariaDB rejects as an unknown option")
	}
}
