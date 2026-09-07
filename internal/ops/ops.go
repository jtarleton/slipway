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
	"encoding/json"
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
	Actor     string // recorded in the audit log; "cli" or "web"
}

func (r *Runner) say(format string, args ...any) {
	if r.Report != nil {
		r.Report(fmt.Sprintf(format, args...))
	}
}

// audit records the outcome of one operation. Called deferred with a pointer to
// the operation's named error return, so it sees whether the operation
// succeeded. An audit-write failure is reported but never fails the operation —
// the work happened either way.
func (r *Runner) audit(action, target string, detail map[string]any, errp *error) {
	if detail == nil {
		detail = map[string]any{}
	}
	if *errp != nil {
		detail["error"] = (*errp).Error()
		detail["ok"] = false
	} else {
		detail["ok"] = true
	}
	blob, _ := json.Marshal(detail)
	if err := r.DB.Record(r.actor(), action, target, blob); err != nil {
		r.say("warning: audit record failed: %v", err)
	}
}

func (r *Runner) actor() string {
	if r.Actor == "" {
		return "unknown"
	}
	return r.Actor
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
func (r *Runner) Pin(ctx context.Context, name string) (err error) {
	defer r.audit("pin", name, nil, &err)

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

// Deploy snapshots the database, patches the environment to an image, waits for
// the rollout, then runs the post-deploy update sequence unless it is skipped.
//
// The snapshot comes first and a failed snapshot aborts the deploy: the whole
// point of taking it is to have something to roll back to, so deploying without
// one defeats the exercise. Pass noSnapshot to skip it deliberately.
func (r *Runner) Deploy(ctx context.Context, name, image, release string, skipUpdate, skipConfig, noSnapshot bool) (err error) {
	detail := map[string]any{"no_snapshot": noSnapshot, "skip_update": skipUpdate}
	defer r.audit("deploy", name, detail, &err)

	if (image == "") == (release == "") {
		return fmt.Errorf("deploy needs exactly one of an image or a release")
	}
	env, err := r.env(name)
	if err != nil {
		return err
	}

	var releaseID *int64
	if release != "" {
		rel, err := r.DB.ReleaseByRef(release)
		if err != nil {
			return fmt.Errorf("no release %q recorded (see 'slipway releases'): %w", release, err)
		}
		image = rel.ImageRef
		releaseID = &rel.ID
		detail["release"] = rel.GitRef
		detail["release_sha"] = rel.GitSHA
		r.say("%s resolves to %s (%s)", release, short(image), rel.GitSHA)
	}
	detail["image"] = image

	if k8s.Digest(image) == "" {
		r.say("warning: %s is not digest-pinned; what runs here will not be traceable to a release", image)
	}

	// What is running now, captured before the patch so a rollback knows where
	// to go back to.
	wl, err := r.Client.Workload(ctx, env.Namespace, r.Workload)
	if err != nil {
		return err
	}
	fromImage := wl.Image

	var snapshotID *int64
	if noSnapshot {
		r.say("  pre-deploy snapshot skipped")
	} else {
		id, err := r.snapshotDatabase(ctx, env)
		if err != nil {
			return fmt.Errorf("pre-deploy snapshot failed, not deploying: %w", err)
		}
		snapshotID = id
	}

	r.say("%s: deploying %s", name, image)
	if err := r.Client.PatchImage(ctx, env.Namespace, r.Workload, r.Container, image); err != nil {
		return err
	}
	if err := r.watch(ctx, env.Namespace); err != nil {
		return err
	}

	if _, err := r.DB.LogDeploy(env.ID, fromImage, image, snapshotID, releaseID, r.actor()); err != nil {
		r.say("warning: deploy succeeded but was not recorded for rollback: %v", err)
	}

	if skipUpdate {
		r.say("  update hooks skipped")
		return nil
	}
	return r.runUpdate(ctx, env, image, skipConfig)
}

// RecordRelease registers a built image against the commit it came from. CI
// calls this after every build; deploys then reference a release by its git ref
// instead of pasting a digest, and the grid can name what is running.
func (r *Runner) RecordRelease(imageRef, gitSHA, gitRef, builtAt string) (err error) {
	defer r.audit("release", gitRef, map[string]any{"image": imageRef, "sha": gitSHA}, &err)

	digest := k8s.Digest(imageRef)
	if digest == "" {
		return fmt.Errorf("release image %q must be digest-pinned (repo@sha256:…)", imageRef)
	}
	if builtAt == "" {
		builtAt = time.Now().UTC().Format(time.RFC3339)
	}
	id, err := r.DB.CreateRelease(store.Release{
		GitSHA: gitSHA, GitRef: gitRef, ImageRef: imageRef, ImageDigest: digest, BuiltAt: builtAt,
	})
	if err != nil {
		return err
	}
	r.say("recorded release #%d: %s from %s", id, gitRef, shortSHA(gitSHA))
	return nil
}

// Releases lists recorded releases, newest build first.
func (r *Runner) Releases() ([]store.Release, error) { return r.DB.Releases(50) }

func shortSHA(sha string) string {
	if len(sha) > 7 {
		return sha[:7]
	}
	if sha == "" {
		return "(no sha)"
	}
	return sha
}

// Adopt makes slipway the unambiguous owner of an environment's namespace by
// stripping any leftover ArgoCD tracking metadata. Safe to run repeatedly; a
// namespace that was never under ArgoCD is left untouched.
func (r *Runner) Adopt(ctx context.Context, name string) (err error) {
	defer r.audit("adopt", name, nil, &err)

	env, err := r.env(name)
	if err != nil {
		return err
	}

	r.say("%s: detaching ArgoCD from %s", name, env.Namespace)
	n, err := r.Client.DetachArgo(ctx, env.Namespace, func(kind, obj string) {
		r.say("  cleared %s/%s", kind, obj)
	})
	if err != nil {
		return err
	}
	if n == 0 {
		r.say("%s: %s carried no ArgoCD metadata — nothing to do", name, env.Namespace)
	} else {
		r.say("%s: cleared ArgoCD tracking from %d resource(s); slipway now owns %s", name, n, env.Namespace)
	}
	return nil
}

// Snapshot dumps an environment's database to object storage and records it.
func (r *Runner) Snapshot(ctx context.Context, name string) (err error) {
	defer r.audit("snapshot", name, nil, &err)

	env, err := r.env(name)
	if err != nil {
		return err
	}
	id, err := r.snapshotDatabase(ctx, env)
	if err != nil {
		return err
	}
	r.say("%s: snapshot #%d recorded", name, *id)
	return nil
}

// snapshotDatabase queues and drives a KindSnapshot job, then records where the
// dump landed. It returns the snapshot's row id.
func (r *Runner) snapshotDatabase(ctx context.Context, env store.Environment) (*int64, error) {
	now := time.Now().Unix()
	key := fmt.Sprintf("s3://amazon-jtarleton-s3/s3fs-private/slipway/%s/db/%d.sql.gz", env.Name, now)

	payload, err := drupal.Params{SnapshotKey: key}.Encode()
	if err != nil {
		return nil, err
	}
	if _, err := r.DB.CreateJob(store.Job{
		EnvID:      env.ID,
		GroupID:    fmt.Sprintf("snapshot-%s-%d", env.Name, now),
		Seq:        0,
		Kind:       jobs.KindSnapshot,
		K8sJobName: jobs.Name(env.Name, now, 0, jobs.KindSnapshot),
		Payload:    payload,
	}); err != nil {
		return nil, err
	}

	r.say("  snapshotting %s database → %s", env.Name, key)
	if err := r.RunSequence(ctx); err != nil {
		return nil, err
	}

	id, err := r.DB.RecordSnapshot(env.ID, key, false)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

// Rollback returns an environment to its previous deployment.
//
// By default it only re-patches the image — fast, and the common case is "the
// new build is broken, get the old one back". withData additionally restores
// the database snapshot taken before that deploy, scaling the app down for the
// load; use it when the deploy changed data, not just code.
func (r *Runner) Rollback(ctx context.Context, name string, withData bool) (err error) {
	defer r.audit("rollback", name, map[string]any{"with_data": withData}, &err)

	env, err := r.env(name)
	if err != nil {
		return err
	}

	dep, err := r.DB.LastDeploy(env.ID)
	if err != nil {
		return fmt.Errorf("%s: no recorded deployment to roll back (deploy through slipway first): %w", name, err)
	}
	if dep.FromImage == "" {
		return fmt.Errorf("%s: the last deploy recorded no prior image — nothing to roll back to", name)
	}
	if withData && dep.SnapshotID == nil {
		return fmt.Errorf("%s: -with-data asked for, but that deploy took no snapshot", name)
	}

	r.say("%s: rolling back %s → %s", name, short(dep.ToImage), short(dep.FromImage))

	if withData {
		snap, err := r.DB.Snapshot(*dep.SnapshotID)
		if err != nil {
			return err
		}
		// Patch the image and load the snapshot while the app is down, so it
		// comes back up on the old code against the old data in one step.
		err = r.withScaledDown(ctx, env, func() error {
			if err := r.Client.PatchImage(ctx, env.Namespace, r.Workload, r.Container, dep.FromImage); err != nil {
				return err
			}
			return r.restoreFromSnapshot(ctx, env, snap.ObjectKey)
		})
		if err != nil {
			return err
		}
	} else {
		if err := r.Client.PatchImage(ctx, env.Namespace, r.Workload, r.Container, dep.FromImage); err != nil {
			return err
		}
		if err := r.watch(ctx, env.Namespace); err != nil {
			return err
		}
	}

	if err := r.DB.MarkRolledBack(dep.ID); err != nil {
		r.say("warning: rollback done but not marked — the next rollback may repeat it: %v", err)
	}
	if !withData {
		r.say("  NOTE: code is back on %s but the database was not touched. If the deploy "+
			"ran update hooks, roll back again with -with-data or restore a snapshot by hand.", short(dep.FromImage))
	}
	return nil
}

// Restore loads a specific recorded snapshot back into its environment.
//
// Unlike rollback this touches only the database — the running image is left
// alone. Use it to undo a bad copy-down, recover from corruption, or return to
// any point further back than the last deploy.
func (r *Runner) Restore(ctx context.Context, name string, snapshotID int64) (err error) {
	defer r.audit("restore", name, map[string]any{"snapshot": snapshotID}, &err)

	env, err := r.env(name)
	if err != nil {
		return err
	}
	snap, err := r.DB.Snapshot(snapshotID)
	if err != nil {
		return fmt.Errorf("%s: no snapshot #%d: %w", name, snapshotID, err)
	}
	if snap.EnvID != env.ID {
		return fmt.Errorf("snapshot #%d was taken from a different environment; restore it there", snapshotID)
	}

	r.say("%s: restoring database from snapshot #%d (%s)", name, snapshotID, snap.CreatedAt)
	return r.withScaledDown(ctx, env, func() error {
		return r.restoreFromSnapshot(ctx, env, snap.ObjectKey)
	})
}

// Snapshots lists an environment's recorded snapshots, newest first.
func (r *Runner) Snapshots(name string) ([]store.Snapshot, error) {
	env, err := r.env(name)
	if err != nil {
		return nil, err
	}
	return r.DB.SnapshotsFor(env.ID)
}

// withScaledDown runs fn with the Drupal Deployment scaled to zero, then scales
// it back and waits for the rollout. A database load must not happen under a
// live application, and the pods must be gone rather than merely restarting.
// fn's error is returned even if scaling back up succeeds — but the scale-up is
// always attempted, so a failure does not strand the environment at zero.
func (r *Runner) withScaledDown(ctx context.Context, env store.Environment, fn func() error) error {
	r.say("  scaling %s/%s to 0", env.Namespace, r.Workload)
	if err := r.Client.Scale(ctx, env.Namespace, r.Workload, 0); err != nil {
		return err
	}
	if err := r.waitForPodsGone(ctx, env.Namespace); err != nil {
		return err
	}

	fnErr := fn()

	r.say("  scaling %s/%s back to 1", env.Namespace, r.Workload)
	if err := r.Client.Scale(ctx, env.Namespace, r.Workload, 1); err != nil {
		if fnErr != nil {
			return fnErr
		}
		return err
	}
	if fnErr != nil {
		return fnErr
	}
	return r.watch(ctx, env.Namespace)
}

// restoreFromSnapshot queues and drives a KindRestore job that loads a database
// snapshot from object storage.
func (r *Runner) restoreFromSnapshot(ctx context.Context, env store.Environment, key string) error {
	now := time.Now().Unix()
	payload, err := drupal.Params{SnapshotKey: key}.Encode()
	if err != nil {
		return err
	}
	if _, err := r.DB.CreateJob(store.Job{
		EnvID:      env.ID,
		GroupID:    fmt.Sprintf("restore-%s-%d", env.Name, now),
		Seq:        0,
		Kind:       jobs.KindRestore,
		K8sJobName: jobs.Name(env.Name, now, 0, jobs.KindRestore),
		Payload:    payload,
	}); err != nil {
		return err
	}
	r.say("  restoring %s database from %s", env.Name, key)
	return r.RunSequence(ctx)
}

func short(image string) string {
	if d := k8s.Digest(image); d != "" {
		return k8s.Repository(image) + "@" + d[:19]
	}
	return image
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

// CopyDown moves the database, the app volume, or both from one environment down
// to another.
//
// The full sequence is fixed and ordered: push the source's files to object
// storage, pull them into the target, stream the database across, then sanitize.
// Nothing runs until its predecessor has succeeded, so a failed load can never
// be followed by a sanitize that makes the result look deliberate. skipFiles
// drops the two file steps; skipDB drops the restore and the sanitize — the
// Acquia workflow's separate "drag Database" and "drag Files" gestures land
// here as one of those two subsets.
func (r *Runner) CopyDown(ctx context.Context, from, to string, skipFiles, skipDB, clean bool) (err error) {
	defer r.audit("copy-down", from+"→"+to, map[string]any{
		"skip_files": skipFiles, "skip_db": skipDB, "clean": clean,
	}, &err)

	if skipFiles && skipDB {
		return fmt.Errorf("copy-down with nothing to copy: both files and database skipped")
	}
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
	if image == "" && !skipDB {
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
	if !skipDB {
		steps = append(steps,
			step{dst, jobs.KindRestore, drupal.Params{SourceNamespace: src.Namespace}},
			step{dst, jobs.KindSanitize, drupal.Params{DrupalImage: image}},
		)
	}

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
func (r *Runner) Cancel(ctx context.Context, group string) (err error) {
	defer r.audit("cancel", group, nil, &err)

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
