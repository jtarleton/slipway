package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jtarleton/slipway/internal/drupal"
	"github.com/jtarleton/slipway/internal/k8s"
	"github.com/jtarleton/slipway/internal/ops"
	"github.com/jtarleton/slipway/internal/store"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func deployment(ns, image string, desired, ready int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "drupal", Namespace: ns},
		Spec: appsv1.DeploymentSpec{
			Replicas: &desired,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "drupal", Image: image}}},
			},
		},
		Status: appsv1.DeploymentStatus{ReadyReplicas: ready},
	}
}

func testServer(t *testing.T) *server {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "slipway.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	for _, e := range []store.Environment{
		{Name: "dev", Rank: 10, Namespace: "jt-drupal-dev", IngressHost: "dev.example.com"},
		{Name: "prod", Rank: 30, Namespace: "jt-drupal", IngressHost: "example.com", IsProduction: true},
	} {
		if _, err := db.UpsertEnvironment(e); err != nil {
			t.Fatalf("seed %s: %v", e.Name, err)
		}
	}

	digest := "sha256:" + strings.Repeat("a", 64)
	cs := fake.NewClientset(
		deployment("jt-drupal-dev", "reg/d:main", 1, 1),
		deployment("jt-drupal", "reg/d@"+digest, 2, 2),
	)
	runner := &ops.Runner{
		DB: db, Client: k8s.NewWithInterface(cs),
		Workload: "drupal", Container: "drupal", Images: drupal.DefaultImages(),
	}
	s := &server{runner: runner, hub: newHub()}
	runner.Report = s.emit
	return s
}

func mux(s *server) http.Handler {
	m := http.NewServeMux()
	s.routes(m)
	return m
}

func TestGridEndpoint(t *testing.T) {
	s := testServer(t)
	req := httptest.NewRequest("GET", "/api/grid", nil)
	rec := httptest.NewRecorder()
	mux(s).ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	var rows []gridRow
	if err := json.Unmarshal(rec.Body.Bytes(), &rows); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(rows) != 2 || rows[0].Env != "dev" || rows[1].Env != "prod" {
		t.Fatalf("rows = %+v", rows)
	}
	if !rows[1].Pinned || rows[1].State != "healthy" {
		t.Errorf("prod row = %+v, want pinned & healthy", rows[1])
	}
	if rows[0].Pinned {
		t.Errorf("dev row = %+v, want not pinned", rows[0])
	}
}

func TestIndexServed(t *testing.T) {
	s := testServer(t)
	rec := httptest.NewRecorder()
	mux(s).ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "slipway") {
		t.Fatalf("index not served: %d", rec.Code)
	}
}

func TestSingleFlightRejectsASecondOperation(t *testing.T) {
	s := testServer(t)

	release := make(chan struct{})
	if err := s.start("first", time.Minute, func(context.Context) error {
		<-release
		return nil
	}); err != nil {
		t.Fatalf("first start: %v", err)
	}
	t.Cleanup(func() { close(release) })

	// Give the goroutine a moment to mark itself current.
	waitFor(t, func() bool { return s.stateView().Running })

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/resume", nil)
	mux(s).ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("second operation: status %d, want 409", rec.Code)
	}
}

func TestOperationProgressReachesTheState(t *testing.T) {
	s := testServer(t)
	done := make(chan struct{})
	if err := s.start("noisy", time.Minute, func(context.Context) error {
		s.runner.Report("halfway")
		close(done)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	<-done
	waitFor(t, func() bool {
		for _, line := range s.stateView().Log {
			if line == "halfway" {
				return true
			}
		}
		return false
	})
}

func TestEventStreamSendsAnOpeningSnapshot(t *testing.T) {
	s := testServer(t)
	srv := httptest.NewServer(mux(s))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type = %q", ct)
	}

	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	got := string(buf[:n])
	for _, want := range []string{"event: grid", "event: jobs", "event: state"} {
		if !strings.Contains(got, want) {
			t.Errorf("opening snapshot missing %q; got:\n%s", want, got)
		}
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within 2s")
}
