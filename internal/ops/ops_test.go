package ops

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jtarleton/slipway/internal/drupal"
	"github.com/jtarleton/slipway/internal/k8s"
	"github.com/jtarleton/slipway/internal/store"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func deployment(ns string, replicas int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "drupal", Namespace: ns},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "drupal", Image: "reg/d:main"}}},
			},
		},
	}
}

func testRunner(t *testing.T, deps ...*appsv1.Deployment) *Runner {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	for _, e := range []store.Environment{
		{Name: "dev", Rank: 10, Namespace: "jt-drupal-dev", IngressHost: "d"},
		{Name: "prod", Rank: 30, Namespace: "jt-drupal", IngressHost: "p", IsProduction: true},
	} {
		if _, err := db.UpsertEnvironment(e); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	objs := make([]runtime.Object, len(deps))
	for i, d := range deps {
		objs[i] = d
	}

	return &Runner{
		DB: db, Client: k8s.NewWithInterface(fake.NewClientset(objs...)),
		Workload: "drupal", Container: "drupal", Images: drupal.DefaultImages(),
		Report: func(string) {},
	}
}

func TestRestoreRejectsASnapshotFromAnotherEnvironment(t *testing.T) {
	r := testRunner(t, deployment("jt-drupal-dev", 1), deployment("jt-drupal", 1))
	envs, _ := r.DB.Environments()

	id, err := r.DB.RecordSnapshot(envs[1].ID, "s3://b/prod/db/1.sql.gz", false) // prod
	if err != nil {
		t.Fatalf("RecordSnapshot: %v", err)
	}

	err = r.Restore(context.Background(), "dev", id)
	if err == nil || !strings.Contains(err.Error(), "different environment") {
		t.Fatalf("Restore into the wrong env = %v, want a rejection", err)
	}
}

func TestRestoreRejectsAnUnknownSnapshot(t *testing.T) {
	r := testRunner(t, deployment("jt-drupal-dev", 1))
	if err := r.Restore(context.Background(), "dev", 999); err == nil {
		t.Fatal("Restore accepted a snapshot id that does not exist")
	}
}

func TestRollbackNeedsARecordedDeploy(t *testing.T) {
	r := testRunner(t, deployment("jt-drupal-dev", 1))
	if err := r.Rollback(context.Background(), "dev", false); err == nil {
		t.Fatal("Rollback with no deploy log succeeded")
	}
}

func TestValidateSchedule(t *testing.T) {
	r := testRunner(t, deployment("jt-drupal-dev", 1), deployment("jt-drupal", 1))

	bad := []store.Schedule{
		{Name: "a", Spec: "nope", Op: "snapshot", Env: "dev"},                     // bad cron
		{Name: "b", Spec: "0 3 * * *", Op: "frobnicate", Env: "dev"},              // unknown op
		{Name: "c", Spec: "0 3 * * *", Op: "snapshot", Env: "staging-typo"},       // unknown env
		{Name: "d", Spec: "0 3 * * *", Op: "console", Env: "dev"},                 // console with no command
		{Name: "e", Spec: "0 3 * * *", Op: "copy-down", From: "prod", To: "prod"}, // into prod
		{Name: "f", Spec: "0 3 * * *", Op: "copy-down", From: "prod", To: "dev", SkipFiles: true, SkipDB: true},
	}
	for _, s := range bad {
		if err := r.validateSchedule(s); err == nil {
			t.Errorf("validateSchedule(%s) accepted an invalid schedule", s.Name)
		}
	}

	ok := []store.Schedule{
		{Name: "g", Spec: "0 3 * * *", Op: "snapshot", Env: "prod"},
		{Name: "h", Spec: "*/15 * * * *", Op: "console", Env: "dev", Cmd: "cron"},
		{Name: "i", Spec: "0 4 * * 0", Op: "copy-down", From: "prod", To: "dev"},
	}
	for _, s := range ok {
		if err := r.validateSchedule(s); err != nil {
			t.Errorf("validateSchedule(%s) rejected a valid schedule: %v", s.Name, err)
		}
	}
}

func TestRunScheduleDispatchesAndUnknownOpFails(t *testing.T) {
	r := testRunner(t, deployment("jt-drupal-dev", 1))
	if err := r.RunSchedule(context.Background(), store.Schedule{Name: "x", Op: "frob"}); err == nil {
		t.Error("RunSchedule ran an unknown operation")
	}
	// A console schedule dispatches to Console, which reaches the exec layer.
	err := r.RunSchedule(context.Background(), store.Schedule{Name: "c", Op: "console", Env: "dev", Cmd: "status"})
	if err == nil || !strings.Contains(err.Error(), "cluster config") {
		t.Errorf("console schedule = %v, want it to dispatch to Console", err)
	}
}

// Both the automatic evaluator and "run now" go through RunSchedule, which must
// stamp last_run / last_status either way.
func TestRunScheduleRecordsTheOutcome(t *testing.T) {
	r := testRunner(t, deployment("jt-drupal-dev", 1))
	if _, err := r.DB.UpsertSchedule(store.Schedule{
		Name: "hourly-cron", Spec: "0 * * * *", Op: "console", Env: "dev", Cmd: "status",
	}); err != nil {
		t.Fatalf("UpsertSchedule: %v", err)
	}

	_ = r.RunScheduleByName(context.Background(), "hourly-cron") // fails at exec, that's fine

	s, err := r.DB.ScheduleByName("hourly-cron")
	if err != nil {
		t.Fatal(err)
	}
	if s.LastRun == "" {
		t.Error("RunScheduleByName did not stamp last_run")
	}
	if !strings.HasPrefix(s.LastStatus, "failed:") {
		t.Errorf("last_status = %q, want the failure recorded", s.LastStatus)
	}
}

func TestWithActorRestores(t *testing.T) {
	r := testRunner(t)
	r.Actor = "cli"
	_ = r.WithActor("cron", func() error {
		if r.Actor != "cron" {
			t.Errorf("actor = %q inside WithActor, want cron", r.Actor)
		}
		return nil
	})
	if r.Actor != "cli" {
		t.Errorf("actor = %q after WithActor, want it restored to cli", r.Actor)
	}
}

func TestConsoleCommand(t *testing.T) {
	if got := consoleCommand("status", false); len(got) != 2 || got[0] != "/app/vendor/bin/drush" || got[1] != "status" {
		t.Errorf("drush command = %q", got)
	}
	if got := consoleCommand("cache:rebuild -y", false); len(got) != 3 || got[2] != "-y" {
		t.Errorf("drush command with args = %q", got)
	}
	if got := consoleCommand("ls -la /app | head", true); len(got) != 3 || got[0] != "sh" || got[1] != "-c" || got[2] != "ls -la /app | head" {
		t.Errorf("shell command = %q", got)
	}
}

func TestLineWriter(t *testing.T) {
	var lines []string
	w := &lineWriter{emit: func(s string) { lines = append(lines, s) }}
	w.Write([]byte("one\ntw"))
	w.Write([]byte("o\r\nthree"))
	w.flush()
	want := []string{"one", "two", "three"}
	if len(lines) != 3 || lines[0] != want[0] || lines[1] != want[1] || lines[2] != want[2] {
		t.Errorf("lines = %q, want %q", lines, want)
	}
}

func TestConsoleValidatesAndIsAudited(t *testing.T) {
	r := testRunner(t, deployment("jt-drupal-dev", 1))

	if err := r.Console(context.Background(), "dev", "   ", false); err == nil {
		t.Error("Console accepted an empty command")
	}
	if err := r.Console(context.Background(), "nope", "status", false); err == nil {
		t.Error("Console accepted an unknown environment")
	}
	// A valid call reaches the exec layer, which has no cluster config in tests.
	err := r.Console(context.Background(), "dev", "status", false)
	if err == nil || !strings.Contains(err.Error(), "cluster config") {
		t.Fatalf("Console with a real command = %v, want it to reach exec", err)
	}

	hist, _ := r.DB.History(10)
	if len(hist) != 3 {
		t.Fatalf("got %d audit rows, want one per Console call", len(hist))
	}
	for _, h := range hist {
		if h.Action != "console" {
			t.Errorf("audit action = %q", h.Action)
		}
	}
}

func TestAdoptRejectsAnUnknownEnvAndIsAudited(t *testing.T) {
	r := testRunner(t, deployment("jt-drupal-dev", 1))
	if err := r.Adopt(context.Background(), "staging-typo"); err == nil {
		t.Fatal("Adopt accepted an environment that is not registered")
	}
	hist, _ := r.DB.History(5)
	if len(hist) != 1 || hist[0].Action != "adopt" {
		t.Fatalf("adopt failure not audited: %+v", hist)
	}
}

func TestRecordReleaseRequiresADigestPinnedImage(t *testing.T) {
	r := testRunner(t)
	if err := r.RecordRelease("ghcr.io/x/d:latest", "sha", "refs/tags/v1", ""); err == nil {
		t.Fatal("RecordRelease accepted a tag-only image")
	}
	digest := "sha256:" + strings.Repeat("a", 64)
	if err := r.RecordRelease("ghcr.io/x/d@"+digest, "abcdef0", "refs/tags/v1", ""); err != nil {
		t.Fatalf("RecordRelease: %v", err)
	}
	rels, _ := r.Releases()
	if len(rels) != 1 || rels[0].ImageDigest != digest {
		t.Fatalf("Releases = %+v", rels)
	}
}

func TestDeployRejectsBothOrNeitherImageAndRelease(t *testing.T) {
	r := testRunner(t, deployment("jt-drupal-dev", 1))
	if err := r.Deploy(context.Background(), "dev", "", "", false, false, false); err == nil {
		t.Error("Deploy accepted neither image nor release")
	}
	if err := r.Deploy(context.Background(), "dev", "x@sha256:y", "v1", false, false, false); err == nil {
		t.Error("Deploy accepted both an image and a release")
	}
}

func TestDeployByUnknownReleaseFails(t *testing.T) {
	r := testRunner(t, deployment("jt-drupal-dev", 1))
	err := r.Deploy(context.Background(), "dev", "", "v9.9.9", false, false, false)
	if err == nil || !strings.Contains(err.Error(), "no release") {
		t.Fatalf("Deploy by unknown release = %v", err)
	}
}

// Every operation leaves an audit entry, whether it succeeded or failed.
func TestOperationsAreAudited(t *testing.T) {
	r := testRunner(t, deployment("jt-drupal-dev", 1))
	r.Actor = "cli"

	// A rollback that fails (nothing to roll back) still records.
	_ = r.Rollback(context.Background(), "dev", false)
	// A restore that fails validation still records.
	_ = r.Restore(context.Background(), "dev", 404)

	hist, err := r.DB.History(10)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(hist) != 2 {
		t.Fatalf("got %d audit entries, want 2: %+v", len(hist), hist)
	}
	for _, e := range hist {
		if e.Actor != "cli" {
			t.Errorf("entry %s recorded actor %q", e.Action, e.Actor)
		}
		var d struct {
			OK    bool   `json:"ok"`
			Error string `json:"error"`
		}
		if err := json.Unmarshal(e.Detail, &d); err != nil {
			t.Fatalf("detail is not json: %v", err)
		}
		if d.OK || d.Error == "" {
			t.Errorf("failed op %s recorded as ok=%v error=%q", e.Action, d.OK, d.Error)
		}
	}
}

// withScaledDown must always attempt to scale back up, even when the work it
// brackets fails — otherwise a failed restore strands the environment at zero.
func TestWithScaledDownAlwaysScalesBackUp(t *testing.T) {
	r := testRunner(t, deployment("jt-drupal-dev", 1))
	env, err := r.env("dev")
	if err != nil {
		t.Fatal(err)
	}

	sentinel := errors.New("the bracketed work failed")
	got := r.withScaledDown(context.Background(), env, func() error { return sentinel })
	if !errors.Is(got, sentinel) {
		t.Fatalf("withScaledDown returned %v, want the inner error", got)
	}

	wl, err := r.Client.Workload(context.Background(), env.Namespace, "drupal")
	if err != nil {
		t.Fatal(err)
	}
	if wl.Desired != 1 {
		t.Errorf("Deployment left at %d replicas after a failed restore, want it back at 1", wl.Desired)
	}
}
