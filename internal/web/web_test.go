package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jtarleton/slipway/internal/drupal"
	"github.com/jtarleton/slipway/internal/jobs"
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

	// A digest-pinned env exposes exactly that ref for promotion; a tag-only env
	// with no resolvable running digest offers nothing.
	if rows[1].Promote != "reg/d@sha256:"+strings.Repeat("a", 64) {
		t.Errorf("prod promote ref = %q, want the pinned image", rows[1].Promote)
	}
	if rows[0].Promote != "" {
		t.Errorf("dev promote ref = %q, want empty (not traceable)", rows[0].Promote)
	}
	if rows[0].Rank == 0 || rows[1].Rank <= rows[0].Rank {
		t.Errorf("ranks not ordered: dev=%d prod=%d", rows[0].Rank, rows[1].Rank)
	}
}

func TestGridReportsWhenDataWasLastCopiedIn(t *testing.T) {
	s := testServer(t)
	envs, _ := s.runner.DB.Environments()
	dev := envs[0]

	// A succeeded restore and a succeeded files pull into dev.
	filesPull, _ := drupal.Params{ObjectKey: "s3://x", Push: false}.Encode()
	for _, seed := range []struct {
		kind    jobs.Kind
		payload []byte
	}{
		{jobs.KindRestore, nil},
		{jobs.KindSyncFiles, filesPull},
	} {
		id, err := s.runner.DB.CreateJob(store.Job{
			EnvID: dev.ID, GroupID: "g", Seq: 0, Kind: seed.kind,
			K8sJobName: jobs.Name("dev", 42, int(seed.kind[0]), seed.kind), Payload: seed.payload,
		})
		if err != nil {
			t.Fatalf("seed %s: %v", seed.kind, err)
		}
		if err := s.runner.DB.Advance(id, jobs.Pending, jobs.Succeeded, "ok"); err != nil {
			t.Fatal(err)
		}
	}

	rows, err := s.gridRows(context.Background())
	if err != nil {
		t.Fatalf("gridRows: %v", err)
	}
	if rows[0].DBSynced == "" || rows[0].FilesSynced == "" {
		t.Errorf("dev row = %+v, want both sync times populated", rows[0])
	}
	if rows[1].DBSynced != "" {
		t.Errorf("prod row = %+v, want no sync time", rows[1])
	}
}

func TestGridExposesSnapshotsAndRollbackTarget(t *testing.T) {
	s := testServer(t)
	envs, _ := s.runner.DB.Environments()
	prod := envs[1] // testServer seeds dev then prod

	snapID, _ := s.runner.DB.RecordSnapshot(prod.ID, "s3://b/prod/db/1.sql.gz", false)
	if _, err := s.runner.DB.LogDeploy(prod.ID, "reg/d@sha256:old", "reg/d@sha256:new", &snapID, nil, "t"); err != nil {
		t.Fatalf("LogDeploy: %v", err)
	}

	rows, err := s.gridRows(context.Background())
	if err != nil {
		t.Fatalf("gridRows: %v", err)
	}
	if rows[1].Snapshots != 1 {
		t.Errorf("prod row Snapshots = %d, want 1", rows[1].Snapshots)
	}
	if rows[1].RollbackTo != "reg/d@sha256:old" {
		t.Errorf("prod row RollbackTo = %q, want the prior image", rows[1].RollbackTo)
	}
	if rows[0].RollbackTo != "" || rows[0].Snapshots != 0 {
		t.Errorf("dev row should have no snapshots or rollback target: %+v", rows[0])
	}
}

func TestSnapshotsAreStreamedAndRestorable(t *testing.T) {
	s := testServer(t)
	envs, _ := s.runner.DB.Environments()
	dev := envs[0]
	if _, err := s.runner.DB.RecordSnapshot(dev.ID, "s3://b/dev/db/7.sql.gz", false); err != nil {
		t.Fatalf("RecordSnapshot: %v", err)
	}

	rows, err := s.snapshotRows()
	if err != nil || len(rows) != 1 || rows[0].Env != "dev" || rows[0].Key != "s3://b/dev/db/7.sql.gz" {
		t.Fatalf("snapshotRows = %+v (err %v)", rows, err)
	}

	// It reaches a new SSE client in the opening burst.
	srv := httptest.NewServer(mux(s))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/events", nil)
	resp, _ := http.DefaultClient.Do(req)
	buf := make([]byte, 8192)
	n, _ := resp.Body.Read(buf)
	resp.Body.Close()
	if !strings.Contains(string(buf[:n]), "event: snapshots") {
		t.Errorf("opening snapshot missing the snapshots event:\n%s", buf[:n])
	}

	// A restore into the wrong environment is refused before any work starts.
	rec := httptest.NewRecorder()
	mux(s).ServeHTTP(rec, formPost("/api/restore", "env=prod&snapshot="+strconv.FormatInt(rows[0].ID, 10)))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("restore status %d: %s", rec.Code, rec.Body)
	}
	waitFor(t, func() bool {
		for _, l := range s.stateView().Log {
			if strings.Contains(l, "different environment") {
				return true
			}
		}
		return false
	})
}

func TestRestoreNeedsEnvAndSnapshot(t *testing.T) {
	s := testServer(t)
	rec := httptest.NewRecorder()
	mux(s).ServeHTTP(rec, formPost("/api/restore", "env=dev"))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("restore with no snapshot id: status %d, want 400", rec.Code)
	}
}

func TestGridNamesTheRunningRelease(t *testing.T) {
	s := testServer(t)
	digest := "sha256:" + strings.Repeat("a", 64) // matches prod's image in testServer
	if _, err := s.runner.DB.CreateRelease(store.Release{
		GitSHA: "cafe1234feed", GitRef: "refs/tags/v3.0.0",
		ImageRef: "reg/d@" + digest, ImageDigest: digest, BuiltAt: "2026-09-07T10:00:00Z",
	}); err != nil {
		t.Fatalf("CreateRelease: %v", err)
	}

	rows, err := s.gridRows(context.Background())
	if err != nil {
		t.Fatalf("gridRows: %v", err)
	}
	if rows[1].Release != "v3.0.0" || rows[1].GitSHA != "cafe123" {
		t.Errorf("prod row = %+v, want it named for the release", rows[1])
	}
	if rows[0].Release != "" {
		t.Errorf("dev row = %+v, want no release (its digest is unknown)", rows[0])
	}
}

func TestRecordReleaseEndpointIsTokenGated(t *testing.T) {
	s := testServer(t)
	digest := "sha256:" + strings.Repeat("f", 64)
	body := "image=reg/d@" + digest + "&ref=refs/tags/v1&sha=abcdef0"

	// No token configured → the endpoint is not there.
	rec := httptest.NewRecorder()
	mux(s).ServeHTTP(rec, formPost("/api/releases", body))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("no-token status %d, want 404", rec.Code)
	}

	s.releaseToken = "s3cret"

	// Wrong token → 401.
	rec = httptest.NewRecorder()
	req := formPost("/api/releases", body)
	req.Header.Set("Authorization", "Bearer wrong")
	mux(s).ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("bad-token status %d, want 401", rec.Code)
	}

	// Right token → recorded.
	rec = httptest.NewRecorder()
	req = formPost("/api/releases", body)
	req.Header.Set("Authorization", "Bearer s3cret")
	mux(s).ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("good-token status %d: %s", rec.Code, rec.Body)
	}
	rels, _ := s.runner.Releases()
	if len(rels) != 1 || rels[0].ImageDigest != digest {
		t.Errorf("release not recorded: %+v", rels)
	}
}

func TestHistoryIsStreamedWithOutcomes(t *testing.T) {
	s := testServer(t)
	if err := s.runner.DB.Record("web", "deploy", "stage", []byte(`{"ok":false,"error":"rollout stalled\nmore"}`)); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if err := s.runner.DB.Record("cli", "snapshot", "prod", []byte(`{"ok":true}`)); err != nil {
		t.Fatalf("Record: %v", err)
	}

	rows, err := s.historyRows()
	if err != nil {
		t.Fatalf("historyRows: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	// Newest first; the failure keeps only its first line.
	if rows[0].Action != "snapshot" || !rows[0].OK || rows[0].Outcome != "ok" {
		t.Errorf("row 0 = %+v", rows[0])
	}
	if rows[1].OK || rows[1].Outcome != "rollout stalled" {
		t.Errorf("row 1 = %+v, want a one-line failure reason", rows[1])
	}

	srv := httptest.NewServer(mux(s))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/events", nil)
	resp, _ := http.DefaultClient.Do(req)
	buf := make([]byte, 8192)
	n, _ := resp.Body.Read(buf)
	resp.Body.Close()
	if !strings.Contains(string(buf[:n]), "event: history") {
		t.Errorf("opening burst missing the history event:\n%s", buf[:n])
	}
}

func TestRollbackWithNothingToUndoReportsIt(t *testing.T) {
	s := testServer(t)
	rec := httptest.NewRecorder()
	mux(s).ServeHTTP(rec, formPost("/api/rollback", "env=dev"))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	waitFor(t, func() bool {
		for _, l := range s.stateView().Log {
			if strings.Contains(l, "no recorded deployment to roll back") {
				return true
			}
		}
		return false
	})
}

func TestCopyDownRejectsCopyingNothing(t *testing.T) {
	s := testServer(t)
	rec := httptest.NewRecorder()
	mux(s).ServeHTTP(rec, formPost("/api/copy-down", "from=prod&to=dev&skip_db=true&skip_files=true"))
	// The guard fires inside the operation goroutine, so the request is
	// accepted and the failure lands in the log.
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	waitFor(t, func() bool {
		for _, l := range s.stateView().Log {
			if strings.Contains(l, "nothing to copy") {
				return true
			}
		}
		return false
	})
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

// stallGroup seeds a three-step sequence whose middle step failed, leaving the
// last step wedged — the shape the "Stalled sequences" panel exists to show.
func stallGroup(t *testing.T, db *store.DB, group string) {
	t.Helper()
	envs, _ := db.Environments()
	envID := envs[0].ID
	kinds := []jobs.Kind{jobs.KindSyncFiles, jobs.KindRestore, jobs.KindSanitize}
	var ids []int64
	for i, k := range kinds {
		id, err := db.CreateJob(store.Job{
			EnvID: envID, GroupID: group, Seq: i, Kind: k,
			K8sJobName: jobs.Name(group, 1, i, k),
		})
		if err != nil {
			t.Fatalf("seed job: %v", err)
		}
		ids = append(ids, id)
	}
	if err := db.Advance(ids[0], jobs.Pending, jobs.Succeeded, "ok"); err != nil {
		t.Fatal(err)
	}
	if err := db.Advance(ids[1], jobs.Pending, jobs.Failed, "dump died"); err != nil {
		t.Fatal(err)
	}
}

func TestStalledSequenceIsStreamedAndCancellable(t *testing.T) {
	s := testServer(t)
	stallGroup(t, s.runner.DB, "copy-down-prod-dev-1")

	srv := httptest.NewServer(mux(s))
	defer srv.Close()

	// The opening SSE snapshot carries the stall.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	buf := make([]byte, 8192)
	n, _ := resp.Body.Read(buf)
	resp.Body.Close()
	if got := string(buf[:n]); !strings.Contains(got, "event: stalled") || !strings.Contains(got, "copy-down-prod-dev-1") {
		t.Fatalf("opening snapshot missing the stall:\n%s", got)
	}

	// An unknown group is rejected.
	rec := httptest.NewRecorder()
	mux(s).ServeHTTP(rec, formPost("/api/cancel", "group=nope"))
	if rec.Code != http.StatusConflict {
		t.Fatalf("cancel of unknown group: status %d, want 409", rec.Code)
	}

	// Cancelling the real group clears it.
	rec = httptest.NewRecorder()
	mux(s).ServeHTTP(rec, formPost("/api/cancel", "group=copy-down-prod-dev-1"))
	if rec.Code != 200 {
		t.Fatalf("cancel: status %d: %s", rec.Code, rec.Body)
	}
	if stalls, _ := s.stallRows(); len(stalls) != 0 {
		t.Errorf("stall still present after cancel: %+v", stalls)
	}
}

func formPost(path, body string) *http.Request {
	req := httptest.NewRequest("POST", path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return req
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
