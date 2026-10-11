package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func apiRequest(method, path, body string) *http.Request {
	var req *http.Request
	if body != "" {
		req = formPost(path, body)
		req.Method = method
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	req.Header.Set("Authorization", "Bearer atok")
	req.Header.Set("Accept", "application/json")
	return req
}

func TestAPITokenAuthenticatesAPIPathsOnly(t *testing.T) {
	s := testServer(t)
	s.basicAuth = "jt:s3cret"
	s.releaseToken = "rtok"
	s.apiToken = "atok"
	h := s.withAuth(mux(s))

	for _, path := range []string{"/api/grid", "/api/version", "/api/releases", "/api/history", "/api/snapshots", "/api/stalled", "/api/schedules"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, apiRequest("GET", path, ""))
		if rec.Code != http.StatusOK {
			t.Errorf("GET %s with API token = %d, want 200: %s", path, rec.Code, rec.Body)
		}
		if got := strings.TrimSpace(rec.Body.String()); got == "null" {
			t.Errorf("GET %s answered null, want a JSON list or object", path)
		}
	}

	// Not the HTML page.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, apiRequest("GET", "/", ""))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("GET / with API token = %d, want 401", rec.Code)
	}

	// No session cookie is handed out for a token request.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, apiRequest("GET", "/api/grid", ""))
	if len(rec.Result().Cookies()) != 0 {
		t.Fatal("API token request was issued a session cookie")
	}

	// A wrong token is not let through.
	rec = httptest.NewRecorder()
	req := apiRequest("GET", "/api/grid", "")
	req.Header.Set("Authorization", "Bearer nope")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("bad API token = %d, want 401", rec.Code)
	}

	// The API token cannot record releases, and the release token cannot run
	// operations.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, apiRequest("POST", "/api/releases", "image=x@sha256:"+strings.Repeat("a", 64)+"&ref=v1"))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("POST /api/releases with API token = %d, want 401", rec.Code)
	}
	rec = httptest.NewRecorder()
	req = apiRequest("GET", "/api/grid", "")
	req.Header.Set("Authorization", "Bearer rtok")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("GET /api/grid with release token = %d, want 401", rec.Code)
	}
}

func TestAPITokenWithoutBasicAuthStillRecordsTheActor(t *testing.T) {
	s := testServer(t)
	s.apiToken = "atok"
	h := s.withAuth(mux(s))

	rec := httptest.NewRecorder()
	req := apiRequest("POST", "/api/rollback", "env=prod")
	req.Header.Set("X-Slipway-Actor", "drupal:jt")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("rollback = %d: %s", rec.Code, rec.Body)
	}
	waitFor(t, func() bool { return !s.stateView().Running })
	assertLastActor(t, s, "rollback", "drupal:jt")
}

func TestActorHeaderIsValidatedAndOnlyHonouredWithTheToken(t *testing.T) {
	s := testServer(t)
	s.basicAuth = "jt:s3cret"
	s.apiToken = "atok"
	s.baseActor = "web"
	s.runner.Actor = "web"
	h := s.withAuth(mux(s))

	rec := httptest.NewRecorder()
	req := apiRequest("GET", "/api/grid", "")
	req.Header.Set("X-Slipway-Actor", "drupal:jt; DROP")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), `"error"`) {
		t.Fatalf("malformed actor = %d %s, want a 400 JSON error", rec.Code, rec.Body)
	}

	// Through the token, the actor lands in the audit log.
	rec = httptest.NewRecorder()
	req = apiRequest("POST", "/api/rollback", "env=prod")
	req.Header.Set("X-Slipway-Actor", "drupal:jt")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("rollback = %d: %s", rec.Code, rec.Body)
	}
	var started map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &started); err != nil || started["operation"] != "rollback prod" {
		t.Fatalf("202 body = %q, want {\"operation\":\"rollback prod\"}", rec.Body)
	}
	waitFor(t, func() bool { return !s.stateView().Running })
	assertLastActor(t, s, "rollback", "drupal:jt")
	if s.runner.Actor != "web" {
		t.Fatalf("runner actor left as %q after the operation, want web restored", s.runner.Actor)
	}

	// Through Basic auth, the header is ignored.
	rec = httptest.NewRecorder()
	req = formPost("/api/rollback", "env=prod")
	req.SetBasicAuth("jt", "s3cret")
	req.Header.Set("X-Slipway-Actor", "drupal:mallory")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("basic-auth rollback = %d: %s", rec.Code, rec.Body)
	}
	if strings.TrimSpace(rec.Body.String()) != "started" {
		t.Fatalf("plain-text client got %q, want the plain started body", rec.Body)
	}
	waitFor(t, func() bool { return !s.stateView().Running })
	assertLastActor(t, s, "rollback", "web")
}

func TestCancelThroughTheAPIRecordsTheActor(t *testing.T) {
	s := testServer(t)
	s.apiToken = "atok"
	stallGroup(t, s.runner.DB, "copy-down-prod-dev-1")
	h := s.withAuth(mux(s))

	rec := httptest.NewRecorder()
	req := apiRequest("POST", "/api/cancel", "group=copy-down-prod-dev-1")
	req.Header.Set("X-Slipway-Actor", "drush:root")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"operation"`) {
		t.Fatalf("cancel = %d %s", rec.Code, rec.Body)
	}
	assertLastActor(t, s, "cancel", "drush:root")
}

func TestJSONErrorsOnlyForJSONClients(t *testing.T) {
	s := testServer(t)

	rec := httptest.NewRecorder()
	req := formPost("/api/deploy", "env=prod")
	req.Header.Set("Accept", "application/json")
	mux(s).ServeHTTP(rec, req)
	var body map[string]string
	if rec.Code != http.StatusBadRequest || json.Unmarshal(rec.Body.Bytes(), &body) != nil || body["error"] == "" {
		t.Fatalf("JSON client got %d %q, want a 400 {\"error\": ...}", rec.Code, rec.Body)
	}

	rec = httptest.NewRecorder()
	mux(s).ServeHTTP(rec, formPost("/api/deploy", "env=prod"))
	if rec.Code != http.StatusBadRequest || strings.HasPrefix(rec.Body.String(), "{") {
		t.Fatalf("text client got %d %q, want a plain-text 400", rec.Code, rec.Body)
	}
}

func TestHistoryLimitAndSnapshotFilter(t *testing.T) {
	s := testServer(t)
	for i := 0; i < 3; i++ {
		if err := s.runner.DB.Record("cli", "snapshot", "prod", []byte(`{"ok":true}`)); err != nil {
			t.Fatal(err)
		}
	}

	rec := httptest.NewRecorder()
	mux(s).ServeHTTP(rec, httptest.NewRequest("GET", "/api/history?limit=2", nil))
	var hist []historyRow
	if err := json.Unmarshal(rec.Body.Bytes(), &hist); err != nil || len(hist) != 2 {
		t.Fatalf("history?limit=2 = %s, want 2 rows", rec.Body)
	}

	rec = httptest.NewRecorder()
	mux(s).ServeHTTP(rec, httptest.NewRequest("GET", "/api/history?limit=zero", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad limit = %d, want 400", rec.Code)
	}

	rec = httptest.NewRecorder()
	mux(s).ServeHTTP(rec, httptest.NewRequest("GET", "/api/snapshots?env=dev", nil))
	if got := strings.TrimSpace(rec.Body.String()); got != "[]" {
		t.Fatalf("snapshots for an env with none = %s, want []", got)
	}
}

func TestVersionEndpoint(t *testing.T) {
	s := testServer(t)
	s.version = "abc1234"
	rec := httptest.NewRecorder()
	mux(s).ServeHTTP(rec, httptest.NewRequest("GET", "/api/version", nil))
	var v struct {
		API     int    `json:"api"`
		Slipway string `json:"slipway"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil || v.API != APIVersion || v.Slipway != "abc1234" {
		t.Fatalf("version = %s", rec.Body)
	}
}

func assertLastActor(t *testing.T, s *server, action, want string) {
	t.Helper()
	entries, err := s.runner.DB.History(1)
	if err != nil || len(entries) == 0 {
		t.Fatalf("no history: %v", err)
	}
	if e := entries[0]; e.Action != action || e.Actor != want {
		t.Fatalf("last audit entry = %s by %q, want %s by %q", e.Action, e.Actor, action, want)
	}
}
