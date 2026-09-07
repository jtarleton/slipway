// Package jobs implements Slipway's durable job engine.
//
// Every stateful operation Slipway performs — snapshotting a database, running
// update hooks, syncing files — executes as a Kubernetes Job that outlives the
// control plane. The engine's only real responsibility is keeping a SQLite row
// and a Kubernetes Job in agreement across restarts, crashes, and partial
// failures.
//
// Three rules make that possible:
//
//   - A job's Kubernetes name is derived deterministically from its identity,
//     so resubmitting after a crash collides with the previous attempt instead
//     of duplicating work.
//   - The name is persisted before the Job is submitted, so a crash in the gap
//     between those two steps leaves a recoverable record rather than an
//     orphaned Job nobody is watching.
//   - Reconciliation is a pure function of (stored state, cluster observation),
//     which is what lets it be tested exhaustively without a cluster.
package jobs

// State is the stored lifecycle state of a job, as recorded in SQLite.
type State string

const (
	// Pending means the row exists and its Kubernetes name is reserved, but the
	// Job has not been submitted — or the control plane died before it learned
	// whether the submission landed.
	Pending State = "pending"

	// Submitted means the Job exists in the cluster but no pod is active yet.
	Submitted State = "submitted"

	// Running means at least one pod is active.
	Running State = "running"

	// Succeeded, Failed, and Cancelled are terminal.
	Succeeded State = "succeeded"
	Failed    State = "failed"
	Cancelled State = "cancelled"

	// Orphaned means the Job vanished from the cluster before reaching a
	// terminal state — TTL controller, namespace deletion, a human with
	// kubectl. Recorded distinctly from Failed because the cause is external
	// and the operation may in fact have completed.
	Orphaned State = "orphaned"
)

// transitions lists the states each state may legally move to.
//
// Pending reaches every terminal state directly: if the control plane crashes
// between submitting a Job and recording that it did so, the next observation
// may find that Job already finished.
var transitions = map[State][]State{
	Pending:   {Submitted, Running, Succeeded, Failed, Cancelled, Orphaned},
	Submitted: {Running, Succeeded, Failed, Cancelled, Orphaned},
	Running:   {Succeeded, Failed, Cancelled, Orphaned},
	Succeeded: {},
	Failed:    {},
	Cancelled: {},
	Orphaned:  {},
}

// States returns every valid state, in lifecycle order.
func States() []State {
	return []State{Pending, Submitted, Running, Succeeded, Failed, Cancelled, Orphaned}
}

// Valid reports whether s is a state the engine recognizes.
func (s State) Valid() bool {
	_, ok := transitions[s]
	return ok
}

// Terminal reports whether s admits no further transitions.
func (s State) Terminal() bool {
	next, ok := transitions[s]
	return ok && len(next) == 0
}

// CanTransition reports whether from -> to is a legal move. An unknown state on
// either side is never legal, and a state never transitions to itself.
func CanTransition(from, to State) bool {
	for _, s := range transitions[from] {
		if s == to {
			return true
		}
	}
	return false
}

// Observation is what the cluster reports about a job's Kubernetes counterpart
// at a single point in time. A zero Observation means "looked, found nothing".
type Observation struct {
	// Found reports whether a Job with the expected name exists.
	Found bool

	// Active, Succeeded, and Failed mirror batchv1.JobStatus pod counts.
	Active    int32
	Succeeded int32
	Failed    int32

	// DeadlineExceeded reports a DeadlineExceeded condition on the Job, which
	// is distinct from ordinary pod failure and worth a different message.
	DeadlineExceeded bool
}

// Action is what the reconciler should do about a job.
type Action string

const (
	// Wait means the stored state already matches the cluster.
	Wait Action = "wait"

	// Submit means create the Kubernetes Job. Safe to retry: the name is
	// deterministic, so a duplicate submission is rejected by the API server
	// rather than producing a second Job.
	Submit Action = "submit"

	// Adopt means a Job exists whose progress the stored state has not caught
	// up with — typically after a control-plane restart.
	Adopt Action = "adopt"

	// Finalize means the job has reached a terminal state and should be
	// recorded and stopped watching.
	Finalize Action = "finalize"
)

// Decision is the reconciler's verdict for one job.
//
// Next is the state to record *after* Action succeeds, never before. For
// Submit in particular, a failed submission must leave the job in Pending so
// the next pass retries it.
type Decision struct {
	Action Action
	Next   State
	Reason string
}

// Decide resolves a job's stored state against a fresh observation of the
// cluster. It is pure: same inputs, same decision, no I/O.
//
// The ordering of the checks encodes the engine's failure semantics. Success is
// tested before failure because a Job with a retry budget can accumulate failed
// pods on its way to succeeding, and that outcome is a success. Deadline is
// tested before ordinary failure only to produce a more useful reason.
func Decide(cur State, obs Observation) Decision {
	if !cur.Valid() {
		return Decision{Wait, cur, "unknown state; refusing to act"}
	}
	if cur.Terminal() {
		return Decision{Wait, cur, "already terminal"}
	}

	if !obs.Found {
		if cur == Pending {
			return Decision{Submit, Submitted, "no Job in cluster; safe to submit"}
		}
		return Decision{Finalize, Orphaned, "Job disappeared before reaching a terminal state"}
	}

	switch {
	case obs.Succeeded > 0:
		return Decision{Finalize, Succeeded, "Job reported a succeeded pod"}

	case obs.DeadlineExceeded:
		return Decision{Finalize, Failed, "Job exceeded its active deadline"}

	case obs.Failed > 0 && obs.Active == 0:
		return Decision{Finalize, Failed, "Job exhausted its retries"}

	case obs.Active > 0:
		if cur == Running {
			return Decision{Wait, Running, "pod still active"}
		}
		return Decision{Adopt, Running, "pod is active; catching up to the cluster"}

	default:
		// Found, but nothing running, succeeded, or terminally failed: the Job
		// exists and has not started a pod yet.
		if cur == Pending {
			return Decision{Adopt, Submitted, "Job already exists from a previous attempt"}
		}
		return Decision{Wait, cur, "Job exists; no pod yet"}
	}
}
