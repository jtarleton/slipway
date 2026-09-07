// Package engine drives jobs to completion.
//
// The engine owns no judgment of its own. It reads the jobs the store still
// owes an answer for, asks the cluster what it sees, and hands both to
// jobs.Decide. Everything here is plumbing around that one pure function, which
// is what keeps the interesting behaviour testable without a cluster.
package engine

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jtarleton/slipway/internal/jobs"
	"github.com/jtarleton/slipway/internal/k8s"
	"github.com/jtarleton/slipway/internal/store"
)

// Cluster is the engine's view of Kubernetes.
type Cluster interface {
	ObserveJob(ctx context.Context, namespace, name string) (jobs.Observation, error)

	// SubmitJob must be idempotent on name: a collision with an existing Job is
	// this engine's own earlier attempt and counts as success.
	SubmitJob(ctx context.Context, namespace, name string, spec k8s.JobSpec) error
}

// Store is the engine's view of the control-plane database.
type Store interface {
	// Runnable returns only jobs whose predecessors have succeeded, so the
	// engine never has to reason about ordering itself.
	Runnable() ([]store.Job, error)
	Advance(id int64, from, to jobs.State, reason string) error
}

// Planner turns a job record into something runnable. Keeping this behind an
// interface is what stops Drupal specifics — drush commands, dump flags —
// leaking into the engine.
type Planner interface {
	SpecFor(job store.Job) (k8s.JobSpec, error)
	NamespaceFor(envID int64) (string, error)
}

// Engine reconciles stored jobs against the cluster.
type Engine struct {
	store   Store
	cluster Cluster
	planner Planner
	log     *slog.Logger
}

// New builds an Engine. A nil logger discards output.
func New(s Store, c Cluster, p Planner, log *slog.Logger) *Engine {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Engine{store: s, cluster: c, planner: p, log: log}
}

// Stats summarizes one reconcile pass.
type Stats struct {
	Observed  int
	Submitted int
	Advanced  int
	Errors    int
}

// ReconcileOnce makes one pass over every unfinished job.
//
// A failure on one job never aborts the pass. One environment being
// unreachable must not stall deploys to the others, so errors are counted and
// logged, and the job is left exactly as it was for the next pass to retry.
func (e *Engine) ReconcileOnce(ctx context.Context) (Stats, error) {
	pending, err := e.store.Runnable()
	if err != nil {
		return Stats{}, fmt.Errorf("list runnable jobs: %w", err)
	}

	var stats Stats
	for _, job := range pending {
		if ctx.Err() != nil {
			return stats, ctx.Err()
		}
		stats.Observed++

		if err := e.reconcile(ctx, job, &stats); err != nil {
			stats.Errors++
			e.log.Error("reconcile failed",
				"job", job.K8sJobName, "state", job.State, "err", err)
		}
	}
	return stats, nil
}

func (e *Engine) reconcile(ctx context.Context, job store.Job, stats *Stats) error {
	namespace, err := e.planner.NamespaceFor(job.EnvID)
	if err != nil {
		return fmt.Errorf("resolve namespace: %w", err)
	}

	obs, err := e.cluster.ObserveJob(ctx, namespace, job.K8sJobName)
	if err != nil {
		return fmt.Errorf("observe: %w", err)
	}

	decision := jobs.Decide(job.State, obs)

	switch decision.Action {
	case jobs.Wait:
		return nil

	case jobs.Submit:
		spec, err := e.planner.SpecFor(job)
		if err != nil {
			return fmt.Errorf("plan: %w", err)
		}
		// Order matters. The job stays Pending until the submission is
		// confirmed, so a failure here — or a crash immediately after — leaves
		// a record the next pass retries against the same deterministic name.
		if err := e.cluster.SubmitJob(ctx, namespace, job.K8sJobName, spec); err != nil {
			return fmt.Errorf("submit: %w", err)
		}
		stats.Submitted++

	case jobs.Adopt, jobs.Finalize:
		// nothing to do in the cluster; the store just has to catch up

	default:
		return fmt.Errorf("unknown action %q", decision.Action)
	}

	if err := e.store.Advance(job.ID, job.State, decision.Next, decision.Reason); err != nil {
		return fmt.Errorf("advance: %w", err)
	}
	stats.Advanced++

	e.log.Info("job advanced",
		"job", job.K8sJobName,
		"from", job.State, "to", decision.Next,
		"action", decision.Action, "reason", decision.Reason)
	return nil
}
