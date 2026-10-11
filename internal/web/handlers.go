package web

import (
	"context"
	"crypto/subtle"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jtarleton/slipway/internal/store"
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
	consoleDeadline  = 15 * time.Minute
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
		httpError(w, r, "streaming unsupported", http.StatusInternalServerError)
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
	writeEvent(w, "releases", jsonOrEmpty(s.releaseRows()))
	writeEvent(w, "schedules", jsonOrEmpty(s.scheduleRows()))
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
		httpError(w, r, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, rows)
}

func (s *server) handleJobs(w http.ResponseWriter, r *http.Request) {
	rows, err := s.jobRows()
	if err != nil {
		httpError(w, r, err.Error(), http.StatusInternalServerError)
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
		httpError(w, r, "pin needs env", http.StatusBadRequest)
		return
	}
	s.launch(w, r, "pin "+env, deployDeadline, func(ctx context.Context) error {
		return s.runner.Pin(ctx, env)
	})
}

func (s *server) handleAdopt(w http.ResponseWriter, r *http.Request) {
	env := r.FormValue("env")
	if env == "" {
		httpError(w, r, "adopt needs env", http.StatusBadRequest)
		return
	}
	s.launch(w, r, "adopt "+env, deployDeadline, func(ctx context.Context) error {
		return s.runner.Adopt(ctx, env)
	})
}

func (s *server) handleScheduleUpsert(w http.ResponseWriter, r *http.Request) {
	sc := store.Schedule{
		Name: r.FormValue("name"), Spec: r.FormValue("spec"), Op: r.FormValue("op"),
		Env: r.FormValue("env"), From: r.FormValue("from"), To: r.FormValue("to"),
		SkipFiles: r.FormValue("skip_files") == "true", SkipDB: r.FormValue("skip_db") == "true",
		Cmd: r.FormValue("cmd"), Shell: r.FormValue("shell") == "true",
	}
	if err := s.runner.AddSchedule(sc); err != nil {
		httpError(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	s.pushSchedules()
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("saved\n"))
}

func (s *server) handleScheduleToggle(w http.ResponseWriter, r *http.Request) {
	name := r.FormValue("name")
	if name == "" {
		httpError(w, r, "toggle needs name", http.StatusBadRequest)
		return
	}
	if err := s.runner.SetScheduleEnabled(name, r.FormValue("enabled") == "true"); err != nil {
		httpError(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	s.pushSchedules()
	w.Write([]byte("ok\n"))
}

func (s *server) handleScheduleRemove(w http.ResponseWriter, r *http.Request) {
	name := r.FormValue("name")
	if name == "" {
		httpError(w, r, "remove needs name", http.StatusBadRequest)
		return
	}
	if err := s.runner.DeleteSchedule(name); err != nil {
		httpError(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	s.pushSchedules()
	w.Write([]byte("removed\n"))
}

func (s *server) handleScheduleRun(w http.ResponseWriter, r *http.Request) {
	name := r.FormValue("name")
	if name == "" {
		httpError(w, r, "run needs name", http.StatusBadRequest)
		return
	}
	s.launch(w, r, "cron: "+name+" (manual)", copyDownDeadline, func(ctx context.Context) error {
		return s.runner.WithActor("cron", func() error {
			return s.runner.RunScheduleByName(ctx, name)
		})
	})
}

func (s *server) handleConsole(w http.ResponseWriter, r *http.Request) {
	env := r.FormValue("env")
	cmd := r.FormValue("cmd")
	if env == "" || cmd == "" {
		httpError(w, r, "console needs env and cmd", http.StatusBadRequest)
		return
	}
	shell := r.FormValue("shell") == "true"
	s.launch(w, r, "console "+env, consoleDeadline, func(ctx context.Context) error {
		return s.runner.Console(ctx, env, cmd, shell)
	})
}

func (s *server) handleDeploy(w http.ResponseWriter, r *http.Request) {
	env := r.FormValue("env")
	image := r.FormValue("image")
	release := r.FormValue("release")
	if env == "" || (image == "") == (release == "") {
		httpError(w, r, "deploy needs env and exactly one of image or release", http.StatusBadRequest)
		return
	}
	skipUpdate := r.FormValue("skip_update") == "true"
	skipConfig := r.FormValue("skip_config_import") == "true"
	noSnapshot := r.FormValue("no_snapshot") == "true"
	label := "deploy " + env
	if release != "" {
		label += " " + release
	}
	s.launch(w, r, label, deployDeadline, func(ctx context.Context) error {
		return s.runner.Deploy(ctx, env, image, release, skipUpdate, skipConfig, noSnapshot)
	})
}

func (s *server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	env := r.FormValue("env")
	if env == "" {
		httpError(w, r, "snapshot needs env", http.StatusBadRequest)
		return
	}
	s.launch(w, r, "snapshot "+env, copyDownDeadline, func(ctx context.Context) error {
		return s.runner.Snapshot(ctx, env)
	})
}

// handleRecordRelease is the CI hook: after a build, CI POSTs the image and its
// git provenance here. It is gated by a bearer token and disabled entirely when
// no token is configured — this is the one endpoint reachable from outside.
func (s *server) handleRecordRelease(w http.ResponseWriter, r *http.Request) {
	if s.releaseToken == "" {
		httpError(w, r, "release recording is not enabled (start serve with -release-token)", http.StatusNotFound)
		return
	}
	if subtle.ConstantTimeCompare([]byte(bearer(r)), []byte(s.releaseToken)) != 1 {
		httpError(w, r, "unauthorized", http.StatusUnauthorized)
		return
	}

	image := r.FormValue("image")
	sha := r.FormValue("sha")
	ref := r.FormValue("ref")
	builtAt := r.FormValue("built_at")
	if image == "" || ref == "" {
		httpError(w, r, "release needs image and ref", http.StatusBadRequest)
		return
	}
	if err := s.runner.RecordRelease(image, sha, ref, builtAt); err != nil {
		httpError(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	s.pushJobs()
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("recorded\n"))
}

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if after, ok := strings.CutPrefix(h, "Bearer "); ok {
		return after
	}
	return ""
}

func (s *server) handleRestore(w http.ResponseWriter, r *http.Request) {
	env := r.FormValue("env")
	id, _ := strconv.ParseInt(r.FormValue("snapshot"), 10, 64)
	if env == "" || id == 0 {
		httpError(w, r, "restore needs env and snapshot", http.StatusBadRequest)
		return
	}
	s.launch(w, r, fmt.Sprintf("restore %s #%d", env, id), copyDownDeadline, func(ctx context.Context) error {
		return s.runner.Restore(ctx, env, id)
	})
}

func (s *server) handleRollback(w http.ResponseWriter, r *http.Request) {
	env := r.FormValue("env")
	if env == "" {
		httpError(w, r, "rollback needs env", http.StatusBadRequest)
		return
	}
	withData := r.FormValue("with_data") == "true"
	s.launch(w, r, "rollback "+env, copyDownDeadline, func(ctx context.Context) error {
		return s.runner.Rollback(ctx, env, withData)
	})
}

func (s *server) handleCopyDown(w http.ResponseWriter, r *http.Request) {
	from := r.FormValue("from")
	to := r.FormValue("to")
	if from == "" || to == "" {
		httpError(w, r, "copy-down needs from and to", http.StatusBadRequest)
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
	s.launch(w, r, what+" "+from+"→"+to, copyDownDeadline, func(ctx context.Context) error {
		return s.runner.CopyDown(ctx, from, to, skipFiles, skipDB, clean)
	})
}

func (s *server) handleResume(w http.ResponseWriter, r *http.Request) {
	s.launch(w, r, "resume", copyDownDeadline, func(ctx context.Context) error {
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
		httpError(w, r, "cancel needs group", http.StatusBadRequest)
		return
	}
	// Cancel runs outside the single-flight guard, possibly beside a running
	// operation, so the actor is passed explicitly instead of through the
	// Runner's shared Actor field.
	actor := actorFrom(r)
	if actor == "" {
		actor = s.baseActor
	}
	if err := s.runner.CancelAs(r.Context(), group, actor); err != nil {
		httpError(w, r, err.Error(), http.StatusConflict)
		return
	}
	s.pushJobs()
	if wantsJSON(r) {
		writeJSON(w, map[string]string{"operation": "cancel " + group})
		return
	}
	w.Write([]byte("cancelled\n"))
}

// launch starts a foreground operation and answers the request immediately —
// 202 if it began, 409 if another operation already holds the floor. Progress
// arrives over the event stream, not in this response. An API client's actor
// (X-Slipway-Actor) is applied inside the operation's goroutine, where the
// single-flight guard makes Runner.WithActor safe; a schedule's own "cron"
// actor, set inside fn, still wins.
func (s *server) launch(w http.ResponseWriter, r *http.Request, name string, deadline time.Duration, fn func(context.Context) error) {
	run := fn
	if actor := actorFrom(r); actor != "" {
		run = func(ctx context.Context) error {
			return s.runner.WithActor(actor, func() error { return fn(ctx) })
		}
	}
	if err := s.start(name, deadline, run); err != nil {
		httpError(w, r, err.Error(), http.StatusConflict)
		return
	}
	if wantsJSON(r) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(mustJSON(map[string]string{"operation": name}) + "\n"))
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
