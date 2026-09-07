package web

import (
	"context"
	_ "embed"
	"encoding/json"
	"net/http"
	"time"
)

//go:embed index.html
var indexHTML []byte

// deployDeadline and copyDownDeadline mirror the CLI's per-command timeouts in
// cmd/slipway: a rollout should not wait forever, and a database move should not
// fail for want of patience.
const (
	deployDeadline   = 15 * time.Minute
	copyDownDeadline = 6 * time.Hour
)

func (s *server) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(indexHTML)
}

// handleEvents is the Server-Sent Events stream. A browser opens one and
// receives grid, jobs, state and log events for the life of the connection.
func (s *server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch, unsubscribe := s.hub.subscribe()
	defer unsubscribe()

	// Send the current picture immediately so a tab opened mid-operation is not
	// blank until the next tick.
	writeEvent(w, "grid", jsonOrEmpty(s.gridRows(r.Context())))
	writeEvent(w, "jobs", jsonOrEmpty(s.jobRows()))
	writeEvent(w, "state", mustJSON(s.stateView()))
	flusher.Flush()

	keepalive := time.NewTicker(20 * time.Second)
	defer keepalive.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-keepalive.C:
			w.Write([]byte(": keepalive\n\n"))
			flusher.Flush()
		case e, ok := <-ch:
			if !ok {
				return
			}
			writeEvent(w, e.name, e.data)
			flusher.Flush()
		}
	}
}

func (s *server) handleGrid(w http.ResponseWriter, r *http.Request) {
	rows, err := s.gridRows(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, rows)
}

func (s *server) handleJobs(w http.ResponseWriter, r *http.Request) {
	rows, err := s.jobRows()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, rows)
}

func (s *server) handleState(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.stateView())
}

func (s *server) handlePin(w http.ResponseWriter, r *http.Request) {
	env := r.FormValue("env")
	if env == "" {
		http.Error(w, "pin needs env", http.StatusBadRequest)
		return
	}
	s.launch(w, "pin "+env, deployDeadline, func(ctx context.Context) error {
		return s.runner.Pin(ctx, env)
	})
}

func (s *server) handleDeploy(w http.ResponseWriter, r *http.Request) {
	env := r.FormValue("env")
	image := r.FormValue("image")
	if env == "" || image == "" {
		http.Error(w, "deploy needs env and image", http.StatusBadRequest)
		return
	}
	skipUpdate := r.FormValue("skip_update") == "true"
	skipConfig := r.FormValue("skip_config_import") == "true"
	s.launch(w, "deploy "+env, deployDeadline, func(ctx context.Context) error {
		return s.runner.Deploy(ctx, env, image, skipUpdate, skipConfig)
	})
}

func (s *server) handleCopyDown(w http.ResponseWriter, r *http.Request) {
	from := r.FormValue("from")
	to := r.FormValue("to")
	if from == "" || to == "" {
		http.Error(w, "copy-down needs from and to", http.StatusBadRequest)
		return
	}
	skipFiles := r.FormValue("skip_files") == "true"
	clean := r.FormValue("clean") == "true"
	s.launch(w, "copy-down "+from+"→"+to, copyDownDeadline, func(ctx context.Context) error {
		return s.runner.CopyDown(ctx, from, to, skipFiles, clean)
	})
}

func (s *server) handleResume(w http.ResponseWriter, r *http.Request) {
	s.launch(w, "resume", copyDownDeadline, func(ctx context.Context) error {
		return s.runner.Resume(ctx)
	})
}

// launch starts a foreground operation and answers the request immediately —
// 202 if it began, 409 if another operation already holds the floor. Progress
// arrives over the event stream, not in this response.
func (s *server) launch(w http.ResponseWriter, name string, deadline time.Duration, fn func(context.Context) error) {
	if err := s.start(name, deadline, fn); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("started\n"))
}

// --- small helpers ---------------------------------------------------------

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func writeEvent(w http.ResponseWriter, name, data string) {
	w.Write([]byte("event: " + name + "\ndata: " + data + "\n\n"))
}

func jsonOrEmpty[T any](v T, err error) string {
	if err != nil {
		return "[]"
	}
	return mustJSON(v)
}
