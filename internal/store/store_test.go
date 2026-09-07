package store

import (
	"path/filepath"
	"testing"

	"github.com/jtarleton/slipway/internal/jobs"
)

func open(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "slipway.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestOpenIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "slipway.db")

	for i := 0; i < 3; i++ {
		db, err := Open(path)
		if err != nil {
			t.Fatalf("Open (pass %d): %v", i, err)
		}
		db.Close()
	}
}

func TestEnvironmentsComeBackInPromotionOrder(t *testing.T) {
	db := open(t)

	// Inserted out of order on purpose — the grid reads left to right by rank,
	// not by insertion.
	for _, e := range []Environment{
		{Name: "prod", Rank: 30, Namespace: "jt-drupal", IngressHost: "jamestarleton.com", IsProduction: true},
		{Name: "dev", Rank: 10, Namespace: "jt-drupal-dev", IngressHost: "dev.jamestarleton.com"},
		{Name: "stage", Rank: 20, Namespace: "jt-drupal-stage", IngressHost: "stage.jamestarleton.com"},
	} {
		if _, err := db.UpsertEnvironment(e); err != nil {
			t.Fatalf("UpsertEnvironment(%s): %v", e.Name, err)
		}
	}

	got, err := db.Environments()
	if err != nil {
		t.Fatalf("Environments: %v", err)
	}

	want := []string{"dev", "stage", "prod"}
	if len(got) != len(want) {
		t.Fatalf("got %d environments, want %d", len(got), len(want))
	}
	for i, name := range want {
		if got[i].Name != name {
			t.Errorf("position %d = %s, want %s", i, got[i].Name, name)
		}
	}
	if !got[2].IsProduction {
		t.Error("prod did not round-trip as production")
	}
}

func TestUpsertEnvironmentUpdatesInPlace(t *testing.T) {
	db := open(t)

	first, err := db.UpsertEnvironment(Environment{
		Name: "dev", Rank: 10, Namespace: "jt-drupal-dev", IngressHost: "old.example.com",
	})
	if err != nil {
		t.Fatalf("first upsert: %v", err)
	}

	second, err := db.UpsertEnvironment(Environment{
		Name: "dev", Rank: 10, Namespace: "jt-drupal-dev", IngressHost: "dev.jamestarleton.com",
	})
	if err != nil {
		t.Fatalf("second upsert: %v", err)
	}
	if first != second {
		t.Errorf("upsert created a second row: id %d then %d", first, second)
	}

	envs, err := db.Environments()
	if err != nil {
		t.Fatalf("Environments: %v", err)
	}
	if len(envs) != 1 {
		t.Fatalf("got %d environments, want 1", len(envs))
	}
	if envs[0].IngressHost != "dev.jamestarleton.com" {
		t.Errorf("ingress host = %q, want the updated value", envs[0].IngressHost)
	}
}

func seedEnv(t *testing.T, db *DB) int64 {
	t.Helper()
	id, err := db.UpsertEnvironment(Environment{
		Name: "prod", Rank: 30, Namespace: "jt-drupal", IngressHost: "jamestarleton.com", IsProduction: true,
	})
	if err != nil {
		t.Fatalf("seed environment: %v", err)
	}
	return id
}

func TestCreateJobRequiresANameAndAKnownKind(t *testing.T) {
	db := open(t)
	envID := seedEnv(t, db)

	if _, err := db.CreateJob(Job{EnvID: envID, Kind: jobs.KindSnapshot}); err == nil {
		t.Error("CreateJob accepted an empty Kubernetes name; the name must be durable before submission")
	}
	if _, err := db.CreateJob(Job{EnvID: envID, Kind: "deploy-everything", K8sJobName: "x"}); err == nil {
		t.Error("CreateJob accepted an unknown kind")
	}
}

func TestCreateJobRejectsDuplicateNames(t *testing.T) {
	db := open(t)
	envID := seedEnv(t, db)

	name := jobs.Name("prod", 1, 1, jobs.KindSnapshot)
	j := Job{EnvID: envID, Seq: 1, Kind: jobs.KindSnapshot, K8sJobName: name}

	if _, err := db.CreateJob(j); err != nil {
		t.Fatalf("first CreateJob: %v", err)
	}
	if _, err := db.CreateJob(j); err == nil {
		t.Error("CreateJob allowed a duplicate name; two rows pointing at one Kubernetes Job is the bug the unique index exists to prevent")
	}
}

func TestUnfinishedExcludesTerminalJobs(t *testing.T) {
	db := open(t)
	envID := seedEnv(t, db)

	var ids []int64
	for i, kind := range []jobs.Kind{jobs.KindSnapshot, jobs.KindPatch, jobs.KindUpdatedb} {
		id, err := db.CreateJob(Job{
			EnvID:      envID,
			Seq:        i,
			Kind:       kind,
			K8sJobName: jobs.Name("prod", 1, i, kind),
		})
		if err != nil {
			t.Fatalf("CreateJob: %v", err)
		}
		ids = append(ids, id)
	}

	if got, err := db.Unfinished(); err != nil || len(got) != 3 {
		t.Fatalf("Unfinished = %d jobs (err %v), want 3", len(got), err)
	}

	if err := db.Advance(ids[0], jobs.Pending, jobs.Succeeded, "done"); err != nil {
		t.Fatalf("Advance: %v", err)
	}

	got, err := db.Unfinished()
	if err != nil {
		t.Fatalf("Unfinished: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("Unfinished = %d jobs, want 2 after one finished", len(got))
	}
	for _, j := range got {
		if j.State.Terminal() {
			t.Errorf("Unfinished returned terminal job %d in state %s", j.ID, j.State)
		}
	}
}

func TestAdvanceRefusesIllegalTransitions(t *testing.T) {
	db := open(t)
	envID := seedEnv(t, db)

	id, err := db.CreateJob(Job{
		EnvID: envID, Seq: 1, Kind: jobs.KindRestore,
		K8sJobName: jobs.Name("prod", 2, 1, jobs.KindRestore),
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	if err := db.Advance(id, jobs.Succeeded, jobs.Running, "resurrection"); err == nil {
		t.Error("Advance allowed a transition out of a terminal state")
	}
}

// TestAdvanceIsGuardedByCurrentState covers the race the WHERE clause exists
// for: two reconcile passes deciding about the same job, where only the first
// may win.
func TestAdvanceIsGuardedByCurrentState(t *testing.T) {
	db := open(t)
	envID := seedEnv(t, db)

	id, err := db.CreateJob(Job{
		EnvID: envID, Seq: 1, Kind: jobs.KindSyncFiles,
		K8sJobName: jobs.Name("prod", 3, 1, jobs.KindSyncFiles),
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	if err := db.Advance(id, jobs.Pending, jobs.Running, "first pass wins"); err != nil {
		t.Fatalf("first Advance: %v", err)
	}
	if err := db.Advance(id, jobs.Pending, jobs.Submitted, "second pass is stale"); err == nil {
		t.Error("Advance applied an update from a stale view of the job's state")
	}
}

func TestRunnableRespectsSequenceOrder(t *testing.T) {
	db := open(t)
	envID := seedEnv(t, db)

	kinds := []jobs.Kind{jobs.KindSyncFiles, jobs.KindRestore, jobs.KindSanitize}
	var ids []int64
	for i, k := range kinds {
		id, err := db.CreateJob(Job{
			EnvID: envID, GroupID: "copy-down-1", Seq: i, Kind: k,
			K8sJobName: jobs.Name("stage", 1, i, k),
		})
		if err != nil {
			t.Fatalf("CreateJob: %v", err)
		}
		ids = append(ids, id)
	}

	// Only the head of the sequence is runnable.
	got, err := db.Runnable()
	if err != nil {
		t.Fatalf("Runnable: %v", err)
	}
	if len(got) != 1 || got[0].ID != ids[0] {
		t.Fatalf("Runnable returned %d jobs, want only the first in the sequence", len(got))
	}

	if err := db.Advance(ids[0], jobs.Pending, jobs.Succeeded, "done"); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	got, _ = db.Runnable()
	if len(got) != 1 || got[0].ID != ids[1] {
		t.Fatalf("after the first succeeded, Runnable should offer the second: %+v", got)
	}
}

// A failed step must stall everything after it. Letting a sanitize run against
// a database that never loaded would leave stage looking clean while holding
// production data.
func TestRunnableStallsAfterAFailure(t *testing.T) {
	db := open(t)
	envID := seedEnv(t, db)

	var ids []int64
	for i, k := range []jobs.Kind{jobs.KindRestore, jobs.KindSanitize} {
		id, err := db.CreateJob(Job{
			EnvID: envID, GroupID: "copy-down-2", Seq: i, Kind: k,
			K8sJobName: jobs.Name("stage", 2, i, k),
		})
		if err != nil {
			t.Fatalf("CreateJob: %v", err)
		}
		ids = append(ids, id)
	}

	if err := db.Advance(ids[0], jobs.Pending, jobs.Failed, "dump died"); err != nil {
		t.Fatalf("Advance: %v", err)
	}

	got, err := db.Runnable()
	if err != nil {
		t.Fatalf("Runnable: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Runnable offered %d jobs after a failed predecessor: %+v", len(got), got)
	}
	// The job is still unfinished — stalled, not lost.
	unfinished, _ := db.Unfinished()
	if len(unfinished) != 1 || unfinished[0].ID != ids[1] {
		t.Errorf("the stalled job should remain visible as unfinished: %+v", unfinished)
	}
}

// Independent sequences must not block one another.
func TestRunnableRunsGroupsInParallel(t *testing.T) {
	db := open(t)
	envID := seedEnv(t, db)

	for _, group := range []string{"a", "b"} {
		for i, k := range []jobs.Kind{jobs.KindRestore, jobs.KindSanitize} {
			if _, err := db.CreateJob(Job{
				EnvID: envID, GroupID: group, Seq: i, Kind: k,
				K8sJobName: jobs.Name("stage-"+group, 3, i, k),
			}); err != nil {
				t.Fatalf("CreateJob: %v", err)
			}
		}
	}

	got, err := db.Runnable()
	if err != nil {
		t.Fatalf("Runnable: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("Runnable returned %d jobs, want the head of each of the 2 groups", len(got))
	}
}
