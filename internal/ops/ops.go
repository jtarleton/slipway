// Package ops is the set of operations Slipway performs, with nothing about how
// they are invoked.
//
// Each operation is the same work whether a person typed `slipway deploy` or
// clicked a button in the web UI — the README's promise that "these subcommands
// are the same operations the UI will eventually drive". The only thing the
// caller supplies beyond arguments is a Reporter: where the running commentary
// goes. The CLI points it at stdout; the server fans it out to browsers.
package ops

import (
	"context"
	"fmt"
	"time"

	"github.com/jtarleton/slipway/internal/drupal"
	"github.com/jtarleton/slipway/internal/engine"
	"github.com/jtarleton/slipway/internal/grid"
	"github.com/jtarleton/slipway/internal/jobs"
	"github.com/jtarleton/slipway/internal/k8s"
	"github.com/jtarleton/slipway/internal/store"
)

// Reporter receives one progress line at a time. A nil Reporter discards them.
type Reporter func(line string)

// Runner performs operations against one control-plane database and one cluster.
type Runner struct {
	DB        *store.DB
	Client    *k8s.Client
	Workload  string // the Drupal Deployment's name
	Container string // the container within it
	Images    drupal.Images
	Report    Reporter
}

func (r *Runner) say(format string, args ...any) {
	if r.Report != nil {
		r.Report(fmt.Sprintf(format, args...))
	}
}

// Grid reads what every environment is running.
func (r *Runner) Grid(ctx context.Context) ([]grid.Cell, error) {
	envs, err := r.DB.Environments()
	if err != nil {
		return nil, err
	}
	return grid.Read(ctx, r.Client, envs, r.Workload)
}

// Pin rewrites a tag-pinned Deployment to the digest its pods already resolved.
//
// This changes no running code — it is the same image — but it makes the spec
// truthful, so "what is deployed" stops depending on a tag that can move.
func (r *Runner) Pin(ctx context.Context, name string) error {
	env, err := r.env(name)
	if err != nil {
		return err
	}

	wl, err := r.Client.Workload(ctx, env.Namespace, r.Workload)
	if err != nil {
		return err
	}
	switch {
	case !wl.Found:
		return fmt.Errorf("%s: no %s deployment in %s", name, r.Workload, env.Namespace)
	case wl.Digest != "":
		r.say("%s is already pinned to %s", name, wl.Digest)
		return nil
	case wl.RunningDigest == "":
		return fmt.Errorf("%s: no running pod to resolve a digest from", name)
	}

	pinned := k8s.Repository(wl.Image) + "@" + wl.RunningDigest
	r.say("%s: %s -> %s", name, wl.Image, pinned)

	if err := r.Client.PatchImage(ctx, env.Namespace, r.Workload, r.Container, pinned); err != nil {
		return err
	}
	return r.watch(ctx, env.Namespace)
}

// Deploy patches an environment to an image, waits for the rollout, then runs
// the post-deploy update sequence unless it is skipped.
func (r *Runner) Deploy(ctx context.Context, name, image string, skipUpdate, skipConfig bool) error {
	if image == "" {
		return fmt.Errorf("deploy needs an image")
	}
	env, err := r.env(name)
	if err != nil {
		return err
	}

	if k8s.Digest(image) == "" {
		r.say("warning: %s is not digest-pinned; what runs here will not be traceable to a release", image)
	}

	r.say("%s: deploying %s", name, image)
	if err := r.Client.PatchImage(ctx, env.Namespace, r.Workload, r.Container, image); err != nil {
		return err
	}
	if err := r.watch(ctx, env.Namespace); err != nil {
		return err
	}
	if skipUpdate {
		r.say("  update hooks skipped")
		return nil
	}
	return r.runUpdate(ctx, env, image, skipConfig)
}

// runUpdate queues the post-deploy sequence and drives it.
//
// The rollout completes first: update hooks must run against the new code, and
// the code only exists once the pods carrying it are serving.
func (r *Runner) runUpdate(ctx context.Context, env store.Environment, image string, skipConfig bool) error {
	payload, err := drupal.Params{DrupalImage: image, SkipConfigImport: skipConfig}.Encode()
	if err != nil {
		return err
	}

	now := time.Now().Unix()
	if _, err := r.DB.CreateJob(store.Job{
		EnvID:      env.ID,
		GroupID:    fmt.Sprintf("update-%s-%d", env.Name, now),
		Seq:        0,
		Kind:       jobs.KindUpdatedb,
		K8sJobName: jobs.Name(env.Name, now, 0, jobs.KindUpdatedb),
		Payload:    payload,
	}); err != nil {
		return err
	}

	r.say("  running update hooks behind maintenance mode")
	if err := r.RunSequence(ctx); err != nil {
		return fmt.Errorf("%w\n  NOTE: %s may still be in maintenance mode — that is deliberate, "+
			"a half-updated database must not serve traffic", err, env.Name)
	}
	return nil
}

// CopyDown moves a database and the app volume from one environment to another.
//
// The sequence is fixed and ordered: push the source's files to object storage,
// pull them into the target, stream the database across, then sanitize. Nothing
// runs until its predecessor has succeeded, so a failed load can never be
// followed by a sanitize that makes the result look deliberate.
func (r *Runner) CopyDown(ctx context.Context, from, to string, skipFiles, clean bool) error {
	src, err := r.env(from)
	if err != nil {
		return err
	}
	dst, err := r.env(to)
	if err != nil {
		return err
	}
	if dst.IsProduction {
		return fmt.Errorf("refusing to copy into %s: it is production", dst.Name)
	}

	// drush has to run from the code it belongs with, so the target's own image
	// is the one to sanitize with.
	wl, err := r.Client.Workload(ctx, dst.Namespace, r.Workload)
	if err != nil {
		return err
	}
	image := wl.Image
	if image == "" {
		return fmt.Errorf("%s has no %s deployment to take an image from", dst.Name, r.Workload)
	}

	group := fmt.Sprintf("copy-down-%s-%s-%d", src.Name, dst.Name, time.Now().Unix())
	key := fmt.Sprintf("s3://amazon-jtarleton-s3/s3fs-private/slipway/%s/app", src.Name)

	type step struct {
		env    store.Environment
		kind   jobs.Kind
		params drupal.Params
	}

	var steps []step
	if !skipFiles {
		steps = append(steps,
			step{src, jobs.KindSyncFiles, drupal.Params{ObjectKey: key, Push: true}},
			step{dst, jobs.KindSyncFiles, drupal.Params{ObjectKey: key, Clean: clean}},
		)
	}
	steps = append(steps,
		step{dst, jobs.KindRestore, drupal.Params{SourceNamespace: src.Namespace}},
		step{dst, jobs.KindSanitize, drupal.Params{DrupalImage: image}},
	)

	for i, step := range steps {
		payload, err := step.params.Encode()
		if err != nil {
			return err
		}
		if _, err := r.DB.CreateJob(store.Job{
			EnvID:      step.env.ID,
			GroupID:    group,
			Seq:        i,
			Kind:       step.kind,
			K8sJobName: jobs.Name(step.env.Name, time.Now().Unix(), i, step.kind),
			Payload:    payload,
		}); err != nil {
			return err
		}
	}
	r.say("%s -> %s: %d steps queued as %s", src.Name, dst.Name, len(steps), group)

	// The target's pods must be gone, not merely failing: kubelet recreates
	// subPath directories on every pod start, which would restore the directory
	// the files pull is about to remove.
	r.say("  scaling %s/%s to 0", dst.Namespace, r.Workload)
	if err := r.Client.Scale(ctx, dst.Namespace, r.Workload, 0); err != nil {
		return err
	}
	if err := r.waitForPodsGone(ctx, dst.Namespace); err != nil {
		return err
	}

	if err := r.RunSequence(ctx); err != nil {
		return err
	}

	r.say("  scaling %s/%s back to 1", dst.Namespace, r.Workload)
	if err := r.Client.Scale(ctx, dst.Namespace, r.Workload, 1); err != nil {
		return err
	}
	return r.watch(ctx, dst.Namespace)
}

// Cancel releases a stalled sequence by marking its unfinished steps cancelled.
//
// This is a bookkeeping operation — it touches only the control-plane store,
// not the cluster. The failed step that stalled the group stays as it is; what
// changes is that the steps waiting behind it stop being offered to the engine
// and stop making `resume` report a failure.
func (r *Runner) Cancel(ctx context.Context, group string) error {
	stalls, err := r.DB.Stalls()
	if err != nil {
		return err
	}
	var stall *store.Stall
	for i := range stalls {
		if stalls[i].GroupID == group {
			stall = &stalls[i]
			break
		}
	}
	if stall == nil {
		return fmt.Errorf("%s is not a stalled sequence", group)
	}

	n, err := r.DB.CancelGroup(group)
	if err != nil {
		return err
	}
	r.say("%s: cancelled %d step(s) stalled behind %s (seq %d): %s",
		group, n, stall.BlockedByKind, stall.BlockedBySeq, stall.BlockedByReason)
	return nil
}

// Resume drives whatever is already queued. Running it after the control plane
// dies mid-operation is the whole point of keeping work in Kubernetes Jobs.
func (r *Runner) Resume(ctx context.Context) error {
	remaining, err := r.DB.Unfinished()
	if err != nil {
		return err
	}
	if len(remaining) == 0 {
		r.say("nothing in flight")
		return nil
	}
	r.say("resuming %d unfinished step(s)", len(remaining))
	return r.RunSequence(ctx)
}

// ReconcileOnce makes a single engine pass over the runnable jobs. The web
// server's background loop calls this on a ticker; RunSequence is the same pass
// wrapped in a loop that also narrates progress.
func (r *Runner) ReconcileOnce(ctx context.Context) (engine.Stats, error) {
	envs, err := r.DB.Environments()
	if err != nil {
		return engine.Stats{}, err
	}
	eng := engine.New(r.DB, r.Client, drupal.NewPlanner(envs, r.Images), nil)
	return eng.ReconcileOnce(ctx)
}

// RunSequence drives the engine until nothing is left to run.
//
// A pass that advances nothing while jobs remain means the sequence has stalled
// on a failure; reporting that beats spinning until the context expires.
func (r *Runner) RunSequence(ctx context.Context) error {
	envs, err := r.DB.Environments()
	if err != nil {
		return err
	}
	eng := engine.New(r.DB, r.Client, drupal.NewPlanner(envs, r.Images), nil)

	// Only report transitions; a reconcile loop that reprints the same line
	// every few seconds buries the one line that mattered.
	lastLine := map[int64]string{}

	for {
		stats, err := eng.ReconcileOnce(ctx)
		if err != nil {
			return err
		}

		remaining, err := r.DB.Unfinished()
		if err != nil {
			return err
		}
		if len(remaining) == 0 {
			r.say("  all steps succeeded")
			return nil
		}

		runnable, err := r.DB.Runnable()
		if err != nil {
			return err
		}
		if len(runnable) == 0 {
			return fmt.Errorf("sequence stalled with %d step(s) unfinished; a step failed", len(remaining))
		}
		for _, j := range runnable {
			line := fmt.Sprintf("  [%d] %s %s: %s", j.Seq, j.Kind, j.State, j.Reason)
			if lastLine[j.ID] != line {
				r.say("%s", line)
				lastLine[j.ID] = line
			}
		}
		if stats.Errors > 0 {
			return fmt.Errorf("%d step(s) errored", stats.Errors)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
}

func (r *Runner) waitForPodsGone(ctx context.Context, namespace string) error {
	for {
		gone, err := r.Client.PodsGone(ctx, namespace, r.Workload)
		if err != nil {
			return err
		}
		if gone {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func (r *Runner) watch(ctx context.Context, namespace string) error {
	err := r.Client.WaitForRollout(ctx, namespace, r.Workload, time.Second, func(s k8s.RolloutStatus) {
		r.say("  %s", s.Message)
	})
	if err != nil {
		return err
	}
	r.say("  rollout complete")
	return nil
}

func (r *Runner) env(name string) (store.Environment, error) {
	if name == "" {
		return store.Environment{}, fmt.Errorf("this operation needs an environment")
	}
	envs, err := r.DB.Environments()
	if err != nil {
		return store.Environment{}, err
	}
	for _, e := range envs {
		if e.Name == name {
			return e, nil
		}
	}
	return store.Environment{}, fmt.Errorf("unknown environment %q", name)
}
