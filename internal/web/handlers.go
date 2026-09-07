package web

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

//go:embed index.html
var indexHTML []byte

// deployDeadline and copyDownDeadline mirror the CLI's per-command timeouts in
// cmd/slipway. Deploy now takes a database snapshot first, so it gets the long
// deadline too; a genuinely hung rollout is caught by the rollout's own
// progress deadline, not by this.
const (
	deployDeadline   = 6 * time.Hour
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
	writeEvent(w, "stalled", jsonOrEmpty(s.stallRows()))
	writeEvent(w, "snapshots", jsonOrEmpty(s.snapshotRows()))
	writeEvent(w, "history", jsonOrEmpty(s.historyRows()))
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
	noSnapshot := r.FormValue("no_snapshot") == "true"
	s.launch(w, "deploy "+env, deployDeadline, func(ctx context.Context) error {
		return s.runner.Deploy(ctx, env, image, skipUpdate, skipConfig, noSnapshot)
	})
}

func (s *server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	env := r.FormValue("env")
	if env == "" {
		http.Error(w, "snapshot needs env", http.StatusBadRequest)
		return
	}
	s.launch(w, "snapshot "+env, copyDownDeadline, func(ctx context.Context) error {
		return s.runner.Snapshot(ctx, env)
	})
}

func (s *server) handleRestore(w http.ResponseWriter, r *http.Request) {
	env := r.FormValue("env")
	id, _ := strconv.ParseInt(r.FormValue("snapshot"), 10, 64)
	if env == "" || id == 0 {
		http.Error(w, "restore needs env and snapshot", http.StatusBadRequest)
		return
	}
	s.launch(w, fmt.Sprintf("restore %s #%d", env, id), copyDownDeadline, func(ctx context.Context) error {
		return s.runner.Restore(ctx, env, id)
	})
}

func (s *server) handleRollback(w http.ResponseWriter, r *http.Request) {
	env := r.FormValue("env")
	if env == "" {
		http.Error(w, "rollback needs env", http.StatusBadRequest)
		return
	}
	withData := r.FormValue("with_data") == "true"
	s.launch(w, "rollback "+env, copyDownDeadline, func(ctx context.Context) error {
		return s.runner.Rollback(ctx, env, withData)
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
	skipDB := r.FormValue("skip_db") == "true"
	clean := r.FormValue("clean") == "true"

	what := "copy-down"
	switch {
	case skipDB:
		what = "copy files"
	case skipFiles:
		what = "copy database"
	}
	s.launch(w, what+" "+from+"→"+to, copyDownDeadline, func(ctx context.Context) error {
		return s.runner.CopyDown(ctx, from, to, skipFiles, skipDB, clean)
	})
}

func (s *server) handleResume(w http.ResponseWriter, r *http.Request) {
	s.launch(w, "resume", copyDownDeadline, func(ctx context.Context) error {
		return s.runner.Resume(ctx)
	})
}

// handleCancel releases a stalled sequence. Unlike the others this is an
// instant control-plane write, not a long operation, so it runs inline and
// answers with the outcome rather than going through the single-flight guard —
// a stalled group has no runnable steps, so nothing is driving it to collide
// with.
func (s *server) handleCancel(w http.ResponseWriter, r *http.Request) {
	group := r.FormValue("group")
	if group == "" {
		http.Error(w, "cancel needs group", http.StatusBadRequest)
		return
	}
	if err := s.runner.Cancel(r.Context(), group); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	s.pushJobs()
	w.Write([]byte("cancelled\n"))
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
