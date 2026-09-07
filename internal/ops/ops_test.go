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
