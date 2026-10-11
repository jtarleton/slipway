package web

// API v1: what programmatic clients (the drupal/slipway module, scripts) need
// on top of the endpoints the bundled UI already uses — an API token, an
// audit actor per request, plain-JSON reads of what is otherwise SSE-only, and
// JSON bodies on writes and errors. See "HTTP API" in docs/manual.md.

import (
	"context"
	"crypto/subtle"
	"net/http"
	"regexp"
	"strconv"
	"strings"
)

// APIVersion is reported by GET /api/version. Bump it only for changes a
// client has to know about; additive endpoints and fields do not count.
const APIVersion = 1

type ctxKey int

const actorKey ctxKey = iota

// actorPattern is what X-Slipway-Actor must look like: a lowercase source, a
// colon, and a bounded account name — "drupal:jt", "drush:root".
var actorPattern = regexp.MustCompile(`^[a-z]+:[A-Za-z0-9._@-]{1,64}$`)

// isAPIPath reports whether the API token may authenticate a request for path:
// the JSON API and the event stream, never the HTML page — a token pasted into
// a browser must not drive the UI.
func isAPIPath(path string) bool {
	return strings.HasPrefix(path, "/api/") || path == "/events"
}

// withAPIToken admits a request that carries the API token as a bearer, and
// attaches its audit actor. It returns false when the request did not use the
// token, leaving it to the other credentials. An X-Slipway-Actor that does not
// match actorPattern is refused outright rather than silently recorded as
// "api" — a client that sends one expects to be named in the audit log.
func (s *server) withAPIToken(next http.Handler, w http.ResponseWriter, r *http.Request) bool {
	if s.apiToken == "" || !isAPIPath(r.URL.Path) || !constantEq(bearer(r), s.apiToken) {
		return false
	}
	actor := "api"
	if h := r.Header.Get("X-Slipway-Actor"); h != "" {
		if !actorPattern.MatchString(h) {
			httpError(w, r, "X-Slipway-Actor must match "+actorPattern.String(), http.StatusBadRequest)
			return true
		}
		actor = h
	}
	next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), actorKey, actor)))
	return true
}

// actorFrom is the audit actor an API-token request named, or "" for any other
// request — a Basic-auth browser session cannot claim an identity, it is
// recorded as the Runner's default ("web").
func actorFrom(r *http.Request) string {
	a, _ := r.Context().Value(actorKey).(string)
	return a
}

// wantsJSON reports whether the client asked for JSON answers. The bundled UI
// shows response bodies as text, so it keeps getting plain text.
func wantsJSON(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Accept"), "application/json")
}

// httpError answers with msg as {"error": msg} to JSON clients and as plain
// text to everyone else.
func httpError(w http.ResponseWriter, r *http.Request, msg string, code int) {
	if !wantsJSON(r) {
		http.Error(w, msg, code)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	w.Write([]byte(mustJSON(map[string]string{"error": msg}) + "\n"))
}

func (s *server) handleVersion(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{"api": APIVersion, "slipway": s.version})
}

func (s *server) handleReleases(w http.ResponseWriter, r *http.Request) {
	rows, err := s.releaseRows()
	if err != nil {
		httpError(w, r, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, rows)
}

// handleSnapshots lists snapshots, optionally for one environment (?env=).
func (s *server) handleSnapshots(w http.ResponseWriter, r *http.Request) {
	rows, err := s.snapshotRows()
	if err != nil {
		httpError(w, r, err.Error(), http.StatusInternalServerError)
		return
	}
	if env := r.URL.Query().Get("env"); env != "" {
		kept := make([]snapshotRow, 0, len(rows))
		for _, row := range rows {
			if row.Env == env {
				kept = append(kept, row)
			}
		}
		rows = kept
	}
	writeJSON(w, rows)
}

// handleHistory lists the audit log, newest first; ?limit= (default 50, max 500).
func (s *server) handleHistory(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			httpError(w, r, "limit must be a positive integer", http.StatusBadRequest)
			return
		}
		limit = min(n, 500)
	}
	rows, err := s.historyRowsN(limit)
	if err != nil {
		httpError(w, r, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, rows)
}

func (s *server) handleStalled(w http.ResponseWriter, r *http.Request) {
	rows, err := s.stallRows()
	if err != nil {
		httpError(w, r, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, rows)
}

func (s *server) handleSchedules(w http.ResponseWriter, r *http.Request) {
	rows, err := s.scheduleRows()
	if err != nil {
		httpError(w, r, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, rows)
}

// constantEq compares two secrets without leaking where they differ.
func constantEq(got, want string) bool {
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}
