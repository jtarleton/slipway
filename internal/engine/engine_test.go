package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/jtarleton/slipway/internal/jobs"
	"github.com/jtarleton/slipway/internal/k8s"
	"github.com/jtarleton/slipway/internal/store"
)

// fakeCluster records what the engine asked it to do.
type fakeCluster struct {
	obs        map[string]jobs.Observation
	submitted  []string
	submitErr  error
	observeErr error
}

func (f *fakeCluster) ObserveJob(_ context.Context, _, name string) (jobs.Observation, error) {
	if f.observeErr != nil {
		return jobs.Observation{}, f.observeErr
	}
	return f.obs[name], nil
}

func (f *fakeCluster) SubmitJob(_ context.Context, _, name string, _ k8s.JobSpec) error {
	if f.submitErr != nil {
		return f.submitErr
	}
	f.submitted = append(f.submitted, name)
	return nil
}

// fakeStore is a minimal in-memory stand-in that enforces the same transition
// rules the real store does.
type fakeStore struct {
	jobs      []store.Job
	advances  []advance
	advanceOn error
}

type advance struct {
	id   int64
	from jobs.State
	to   jobs.State
}

func (f *fakeStore) Runnable() ([]store.Job, error) { return f.jobs, nil }

func (f *fakeStore) Advance(id int64, from, to jobs.State, _ string) error {
	if f.advanceOn != nil {
		return f.advanceOn
	}
	if !jobs.CanTransition(from, to) {
		return errors.New("illegal transition")
	}
	f.advances = append(f.advances, advance{id, from, to})
	for i := range f.jobs {
		if f.jobs[i].ID == id {
			f.jobs[i].State = to
		}
	}
	return nil
}

type fakePlanner struct{ err error }

func (f fakePlanner) SpecFor(store.Job) (k8s.JobSpec, error) {
	if f.err != nil {
		return k8s.JobSpec{}, f.err
	}
	return k8s.JobSpec{Image: "drupal@sha256:abc", Command: []string{"drush", "cr"}}, nil
}

func (f fakePlanner) NamespaceFor(int64) (string, error) { return "jt-drupal", nil }

func job(id int64, name string, state jobs.State) store.Job {
	return store.Job{ID: id, EnvID: 1, Seq: 1, Kind: jobs.KindSnapshot, K8sJobName: name, State: state}
}

func TestPendingJobIsSubmittedThenAdvanced(t *testing.T) {
	cluster := &fakeCluster{obs: map[string]jobs.Observation{}}
	st := &fakeStore{jobs: []store.Job{job(1, "slipway-snapshot-prod-1-1", jobs.Pending)}}

	stats, err := New(st, cluster, fakePlanner{}, nil).ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}

	if len(cluster.submitted) != 1 {
		t.Fatalf("submitted %d jobs, want 1", len(cluster.submitted))
	}
	if stats.Submitted != 1 || stats.Advanced != 1 || stats.Errors != 0 {
		t.Errorf("stats = %+v, want 1 submitted, 1 advanced, 0 errors", stats)
	}
	if got := st.advances[0]; got.from != jobs.Pending || got.to != jobs.Submitted {
		t.Errorf("advanced %s -> %s, want pending -> submitted", got.from, got.to)
	}
}

// The invariant the whole crash story rests on: if submission fails, the job
// must stay Pending so the next pass tries again. Recording Submitted here
// would strand the operation forever.
func TestFailedSubmissionLeavesTheJobPending(t *testing.T) {
	cluster := &fakeCluster{obs: map[string]jobs.Observation{}, submitErr: errors.New("apiserver down")}
	st := &fakeStore{jobs: []store.Job{job(1, "slipway-snapshot-prod-1-1", jobs.Pending)}}

	stats, err := New(st, cluster, fakePlanner{}, nil).ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("ReconcileOnce returned an error for a per-job failure: %v", err)
	}
	if stats.Errors != 1 {
		t.Errorf("Errors = %d, want 1", stats.Errors)
	}
	if len(st.advances) != 0 {
		t.Errorf("job advanced despite a failed submission: %+v", st.advances)
	}
	if st.jobs[0].State != jobs.Pending {
		t.Errorf("state = %s, want pending so the next pass retries", st.jobs[0].State)
	}
}

// After a crash between submitting and recording, the Job is already in the
// cluster. The engine must adopt it rather than submit a second dump against
// the same database.
func TestCrashGapAdoptsTheExistingJob(t *testing.T) {
	name := "slipway-snapshot-prod-1-1"
	cluster := &fakeCluster{obs: map[string]jobs.Observation{
		name: {Found: true, Active: 1},
	}}
	st := &fakeStore{jobs: []store.Job{job(1, name, jobs.Pending)}}

	if _, err := New(st, cluster, fakePlanner{}, nil).ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}

	if len(cluster.submitted) != 0 {
		t.Errorf("engine resubmitted a job that was already running: %v", cluster.submitted)
	}
	if got := st.advances[0]; got.to != jobs.Running {
		t.Errorf("advanced to %s, want running", got.to)
	}
}

func TestVanishedJobIsOrphaned(t *testing.T) {
	cluster := &fakeCluster{obs: map[string]jobs.Observation{}}
	st := &fakeStore{jobs: []store.Job{job(1, "slipway-snapshot-prod-1-1", jobs.Running)}}

	if _, err := New(st, cluster, fakePlanner{}, nil).ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}
	if got := st.advances[0]; got.to != jobs.Orphaned {
		t.Errorf("advanced to %s, want orphaned", got.to)
	}
}

func TestStableJobIsLeftAlone(t *testing.T) {
	name := "slipway-snapshot-prod-1-1"
	cluster := &fakeCluster{obs: map[string]jobs.Observation{name: {Found: true, Active: 1}}}
	st := &fakeStore{jobs: []store.Job{job(1, name, jobs.Running)}}

	stats, err := New(st, cluster, fakePlanner{}, nil).ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}
	if len(st.advances) != 0 || stats.Advanced != 0 {
		t.Errorf("engine wrote to the store for a job that had not changed: %+v", st.advances)
	}
}

// One unreachable environment must not stall the others.
func TestOneFailureDoesNotAbortThePass(t *testing.T) {
	name := "slipway-snapshot-prod-1-2"
	cluster := &fakeCluster{obs: map[string]jobs.Observation{
		name: {Found: true, Succeeded: 1},
	}}
	st := &fakeStore{jobs: []store.Job{
		job(1, "slipway-snapshot-prod-1-1", jobs.Pending),
		job(2, name, jobs.Running),
	}}
	// The first job fails to plan; the second must still be reconciled.
	e := New(st, cluster, fakePlanner{err: errors.New("no spec")}, nil)

	stats, err := e.ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}
	if stats.Observed != 2 {
		t.Errorf("Observed = %d, want 2", stats.Observed)
	}
	if stats.Errors != 1 {
		t.Errorf("Errors = %d, want 1", stats.Errors)
	}
	if len(st.advances) != 1 || st.advances[0].to != jobs.Succeeded {
		t.Errorf("the healthy job was not reconciled: %+v", st.advances)
	}
}

func TestCancelledContextStopsThePass(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cluster := &fakeCluster{obs: map[string]jobs.Observation{}}
	st := &fakeStore{jobs: []store.Job{job(1, "slipway-snapshot-prod-1-1", jobs.Pending)}}

	if _, err := New(st, cluster, fakePlanner{}, nil).ReconcileOnce(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if len(cluster.submitted) != 0 {
		t.Error("engine submitted work after its context was cancelled")
	}
}
