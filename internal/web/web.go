// Package web is the browser-facing half of the control plane.
//
// `slipway serve` runs two things at once: an HTTP server that renders the grid
// and drives operations, and a background loop that reconciles in-flight jobs
// so a sequence started from the CLI — or left behind by a crash — finishes
// without anyone keeping a terminal open. Both talk to the same internal/ops
// Runner; the only thing the web layer adds is fanning that Runner's progress
// out to every open tab over Server-Sent Events.
package web

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jtarleton/slipway/internal/cron"
	"github.com/jtarleton/slipway/internal/drupal"
	"github.com/jtarleton/slipway/internal/grid"
	"github.com/jtarleton/slipway/internal/jobs"
	"github.com/jtarleton/slipway/internal/k8s"
	"github.com/jtarleton/slipway/internal/ops"
	"github.com/jtarleton/slipway/internal/store"
)

// reconcileInterval is how often the background loop makes an engine pass while
// jobs are in flight. It matches RunSequence's own poll interval.
const reconcileInterval = 3 * time.Second

// gridInterval is how often the grid is re-read from the cluster and pushed to
// connected browsers. The grid is three API calls; a homelab cluster does not
// need it faster, and every operation forces a refresh when it ends anyway.
const gridInterval = 15 * time.Second

// cronInterval is how often schedules are evaluated. Twice a minute is enough
// at minute granularity, with a per-minute guard against double-firing.
const cronInterval = 30 * time.Second

// Config is what `slipway serve` needs beyond the Runner.
type Config struct {
	Addr string

	// BasicAuth, "user:password", gates the whole UI and API with HTTP Basic
	// auth when set. Leave empty only when something else fronts slipway with
	// authentication — it can deploy, roll back, and exec into containers.
	BasicAuth string

	// ReleaseToken, if non-empty, enables POST /api/releases for CI to record
	// builds — bearer token, and exempt from BasicAuth so CI needs only the one
	// credential. Empty disables the endpoint.
	ReleaseToken string
}

// Serve runs the web UI and the reconcile loop until the process is signalled.
func Serve(ctx context.Context, cfg Config, runner *ops.Runner) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	s := &server{runner: runner, hub: newHub(), releaseToken: cfg.ReleaseToken, basicAuth: cfg.BasicAuth}
	// Route the Runner's progress to every browser. Single-flight (see start)
	// means only one operation writes here at a time.
	runner.Report = s.emit

	mux := http.NewServeMux()
	s.routes(mux)

	if s.basicAuth == "" {
		log.Printf("slipway: WARNING — no -auth set; the UI and API are unauthenticated")
	}

	httpSrv := &http.Server{
		Addr:         announcedAddr(cfg.Addr),
		Handler:      s.withAuth(mux),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 0, // SSE responses are open-ended
	}

	go s.background(ctx)

	errc := make(chan error, 1)
	go func() {
		log.Printf("slipway: listening on %s", cfg.Addr)
		errc <- httpSrv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		if st := s.stateView(); st.Running {
			// The operation's goroutine runs on its own context and will be
			// cut off when the process exits. That is survivable by design —
			// the job records are in SQLite and the Kubernetes Jobs keep
			// running — but `slipway resume` will be needed on restart.
			log.Printf("slipway: shutting down while %q is still running; resume it after restart", st.Operation)
		} else {
			log.Printf("slipway: shutting down")
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return httpSrv.Shutdown(shutdownCtx)
	}
}

// withAuth gates every request with HTTP Basic auth when basicAuth is set.
// POST /api/releases is exempt — it carries its own bearer token so CI needs
// only one credential.
func (s *server) withAuth(next http.Handler) http.Handler {
	if s.basicAuth == "" {
		return next
	}
	user, pass, _ := strings.Cut(s.basicAuth, ":")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// /healthz is for the kubelet probe; POST /api/releases carries its own
		// bearer token. Everything else needs the basic-auth credentials.
		if r.URL.Path == "/healthz" ||
			(r.Method == http.MethodPost && r.URL.Path == "/api/releases") {
			next.ServeHTTP(w, r)
			return
		}
		u, p, ok := r.BasicAuth()
		if !ok ||
			subtle.ConstantTimeCompare([]byte(u), []byte(user)) != 1 ||
			subtle.ConstantTimeCompare([]byte(p), []byte(pass)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="slipway"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func announcedAddr(addr string) string {
	if addr == "" {
		return ":8080"
	}
	return addr
}

type server struct {
	runner       *ops.Runner
	hub          *hub
	releaseToken string // bearer token for POST /api/releases; "" disables it
	basicAuth    string // "user:password" gating the whole UI; "" = open

	mu      sync.Mutex
	current string   // running operation's name, "" when idle
	logs    []string // progress lines of the current or most recent operation

	sentMu sync.Mutex
	sent   map[string]string // last payload broadcast per snapshot event name
}

// pushSnapshot broadcasts data as event name, unless it is byte-for-byte what
// was last sent — an idle tab should not receive a jobs event every 3 seconds
// just because the reconcile loop ticked.
func (s *server) pushSnapshot(name, data string) {
	s.sentMu.Lock()
	if s.sent == nil {
		s.sent = map[string]string{}
	}
	unchanged := s.sent[name] == data
	s.sent[name] = data
	s.sentMu.Unlock()

	if !unchanged {
		s.hub.broadcast(name, data)
	}
}

func (s *server) routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /{$}", s.handleIndex)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /events", s.handleEvents)
	mux.HandleFunc("GET /api/grid", s.handleGrid)
	mux.HandleFunc("GET /api/jobs", s.handleJobs)
	mux.HandleFunc("GET /api/state", s.handleState)
	mux.HandleFunc("POST /api/pin", s.handlePin)
	mux.HandleFunc("POST /api/adopt", s.handleAdopt)
	mux.HandleFunc("POST /api/console", s.handleConsole)
	mux.HandleFunc("POST /api/deploy", s.handleDeploy)
	mux.HandleFunc("POST /api/snapshot", s.handleSnapshot)
	mux.HandleFunc("POST /api/restore", s.handleRestore)
	mux.HandleFunc("POST /api/rollback", s.handleRollback)
	mux.HandleFunc("POST /api/copy-down", s.handleCopyDown)
	mux.HandleFunc("POST /api/resume", s.handleResume)
	mux.HandleFunc("POST /api/cancel", s.handleCancel)
	mux.HandleFunc("POST /api/releases", s.handleRecordRelease)
	mux.HandleFunc("POST /api/schedules", s.handleScheduleUpsert)
	mux.HandleFunc("POST /api/schedules/toggle", s.handleScheduleToggle)
	mux.HandleFunc("POST /api/schedules/remove", s.handleScheduleRemove)
	mux.HandleFunc("POST /api/schedules/run", s.handleScheduleRun)
}

// --- operation lifecycle -------------------------------------------------------

// start runs fn as the single foreground operation.
//
// Only one runs at a time: copy-down scales a Deployment to zero, and a deploy
// landing in the middle of that is exactly the race the control plane exists to
// prevent. A second request while one is running is refused, not queued.
func (s *server) start(name string, deadline time.Duration, fn func(context.Context) error) error {
	s.mu.Lock()
	if s.current != "" {
		busy := s.current
		s.mu.Unlock()
		return fmt.Errorf("%s is already running", busy)
	}
	s.current = name
	s.logs = nil
	s.mu.Unlock()

	s.broadcastState()
	s.reportf("%s: started", name)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), deadline)
		defer cancel()

		err := fn(ctx)

		s.mu.Lock()
		s.current = ""
		s.mu.Unlock()

		if err != nil {
			s.reportf("%s: FAILED — %v", name, err)
		} else {
			s.reportf("%s: done", name)
		}
		s.broadcastState()
		s.pushGrid(context.Background())
		s.pushJobs()
	}()
	return nil
}

func (s *server) reportf(format string, args ...any) { s.emit(fmt.Sprintf(format, args...)) }

// emit appends one progress line and pushes it to every browser.
func (s *server) emit(line string) {
	s.mu.Lock()
	s.logs = append(s.logs, line)
	if len(s.logs) > 500 {
		s.logs = s.logs[len(s.logs)-500:]
	}
	s.mu.Unlock()

	s.hub.broadcast("log", mustJSON(map[string]string{
		"line": line,
		"at":   time.Now().Format("15:04:05"),
	}))
}

// --- background loop ---------------------------------------------------------

func (s *server) background(ctx context.Context) {
	reconcile := time.NewTicker(reconcileInterval)
	defer reconcile.Stop()
	gridTick := time.NewTicker(gridInterval)
	defer gridTick.Stop()
	cronTick := time.NewTicker(cronInterval)
	defer cronTick.Stop()

	s.pushGrid(ctx)
	s.pushJobs()

	for {
		select {
		case <-ctx.Done():
			return

		case <-gridTick.C:
			s.pushGrid(ctx)

		case <-cronTick.C:
			s.runDueSchedules()

		case <-reconcile.C:
			// A foreground operation drives its own reconciliation and treats a
			// racing engine pass as a failure, so stand down while one holds
			// the floor.
			s.mu.Lock()
			busy := s.current != ""
			s.mu.Unlock()

			if !busy {
				unfinished, err := s.runner.DB.Unfinished()
				if err == nil && len(unfinished) > 0 {
					if _, err := s.runner.ReconcileOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
						log.Printf("slipway: background reconcile: %v", err)
					}
				}
			}
			s.pushJobs()
		}
	}
}

// runDueSchedules fires at most one schedule whose cron spec matches the
// current minute and that has not already run this minute. Missed windows (the
// server was down) are not caught up — cron here is best-effort, not a queue.
func (s *server) runDueSchedules() {
	s.mu.Lock()
	busy := s.current != ""
	s.mu.Unlock()
	if busy {
		return
	}

	schedules, err := s.runner.Schedules()
	if err != nil {
		log.Printf("slipway: read schedules: %v", err)
		return
	}
	now := time.Now()
	for _, sc := range schedules {
		if !sc.Enabled || !scheduleDue(sc, now) {
			continue
		}
		name := sc.Name
		def := sc
		// RunSchedule stamps the final last_run / last_status itself.
		err := s.start("cron: "+name, cronScheduleDeadline(def.Op), func(ctx context.Context) error {
			defer s.pushSchedules()
			return s.runner.WithActor("cron", func() error { return s.runner.RunSchedule(ctx, def) })
		})
		if err == nil {
			// Stamp last_run now so a second cron tick this minute does not
			// re-fire while the first run is still going.
			_ = s.runner.DB.RecordScheduleRun(name, now.UTC().Format(time.RFC3339), "running")
			s.pushSchedules()
		}
		return // one at a time
	}
}

func scheduleDue(s store.Schedule, now time.Time) bool {
	m, err := cron.Match(s.Spec, now)
	if err != nil || !m {
		return false
	}
	if s.LastRun == "" {
		return true
	}
	last, err := time.Parse(time.RFC3339, s.LastRun)
	if err != nil {
		return true
	}
	return !last.Truncate(time.Minute).Equal(now.Truncate(time.Minute))
}

func cronScheduleDeadline(op string) time.Duration {
	switch op {
	case "copy-down":
		return copyDownDeadline
	default:
		return consoleDeadline
	}
}

func (s *server) pushGrid(ctx context.Context) {
	rows, err := s.gridRows(ctx)
	if err != nil {
		log.Printf("slipway: read grid: %v", err)
		return
	}
	s.pushSnapshot("grid", mustJSON(rows))
}

func (s *server) pushJobs() {
	rows, err := s.jobRows()
	if err != nil {
		log.Printf("slipway: read jobs: %v", err)
		return
	}
	s.pushSnapshot("jobs", mustJSON(rows))

	stalls, err := s.stallRows()
	if err != nil {
		log.Printf("slipway: read stalls: %v", err)
		return
	}
	s.pushSnapshot("stalled", mustJSON(stalls))

	snaps, err := s.snapshotRows()
	if err != nil {
		log.Printf("slipway: read snapshots: %v", err)
		return
	}
	s.pushSnapshot("snapshots", mustJSON(snaps))

	hist, err := s.historyRows()
	if err != nil {
		log.Printf("slipway: read history: %v", err)
		return
	}
	s.pushSnapshot("history", mustJSON(hist))

	rels, err := s.releaseRows()
	if err != nil {
		log.Printf("slipway: read releases: %v", err)
		return
	}
	s.pushSnapshot("releases", mustJSON(rels))

	s.pushSchedules()
}

func (s *server) pushSchedules() {
	rows, err := s.scheduleRows()
	if err != nil {
		log.Printf("slipway: read schedules: %v", err)
		return
	}
	s.pushSnapshot("schedules", mustJSON(rows))
}

func (s *server) broadcastState() {
	s.hub.broadcast("state", mustJSON(s.stateView()))
}

// --- view models -----------------------------------------------------------

// gridRow is one environment column of the Acquia-style matrix: its identity,
// its code cell, and when its database and files were last copied in.
type gridRow struct {
	Env       string `json:"env"`
	Namespace string `json:"namespace"`
	Host      string `json:"host"`
	Rank      int    `json:"rank"` // promotion order; the matrix uses it to know "up" from "down"
	Prod      bool   `json:"prod"`

	Code     string `json:"code"`    // short digest for display
	Pinned   bool   `json:"pinned"`  // spec names a digest, not a tag
	Promote  string `json:"promote"` // the exact ref a promotion out of this env would deploy ("" if not traceable)
	Release  string `json:"release"` // git ref of the running image, if it came through CI
	GitSHA   string `json:"git_sha"` // short sha of that release
	Replicas string `json:"replicas"`
	State    string `json:"state"`

	DBSynced    string `json:"db_synced"`    // when a database was last copied into this env
	FilesSynced string `json:"files_synced"` // when files were last copied into this env

	Snapshots  int    `json:"snapshots"`   // how many database snapshots are recorded for this env
	RollbackTo string `json:"rollback_to"` // image the last deploy would roll back to ("" if nothing to undo)
}

func (s *server) gridRows(ctx context.Context) ([]gridRow, error) {
	cells, err := s.runner.Grid(ctx)
	if err != nil {
		return nil, err
	}

	dbSync, filesSync, err := s.lastCopies()
	if err != nil {
		return nil, err
	}

	rows := make([]gridRow, 0, len(cells))
	for _, c := range cells {
		row := gridRow{
			Env:         c.Env.Name,
			Namespace:   c.Env.Namespace,
			Host:        c.Env.IngressHost,
			Rank:        c.Env.Rank,
			Prod:        c.Env.IsProduction,
			Code:        grid.Code(c.Workload),
			Pinned:      grid.Pinned(c.Workload),
			Promote:     promoteRef(c.Workload),
			Replicas:    fmt.Sprintf("%d/%d", c.Workload.Ready, c.Workload.Desired),
			State:       grid.State(c.Workload),
			DBSynced:    dbSync[c.Env.ID],
			FilesSynced: filesSync[c.Env.ID],
		}
		if snaps, err := s.runner.DB.SnapshotsFor(c.Env.ID); err == nil {
			row.Snapshots = len(snaps)
		}
		if dep, err := s.runner.DB.LastDeploy(c.Env.ID); err == nil && dep.FromImage != "" {
			row.RollbackTo = dep.FromImage
		}
		if digest := runningDigest(c.Workload); digest != "" {
			if rel, err := s.runner.DB.ReleaseByDigest(digest); err == nil {
				row.Release = shortRef(rel.GitRef)
				row.GitSHA = shortSHA(rel.GitSHA)
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// runningDigest is the digest actually live: the pinned one, or the one the
// pods resolved a tag to.
func runningDigest(w k8s.Workload) string {
	if w.Digest != "" {
		return w.Digest
	}
	return w.RunningDigest
}

func shortRef(ref string) string {
	ref = strings.TrimPrefix(ref, "refs/tags/")
	ref = strings.TrimPrefix(ref, "refs/heads/")
	return ref
}

func shortSHA(sha string) string {
	if len(sha) > 7 {
		return sha[:7]
	}
	return sha
}

// promoteRef is the image a promotion out of this environment would deploy: the
// digest it is pinned to, or the digest its pods resolved a tag to. Empty when
// neither is known — a promotion from here would not be traceable to a release,
// so the matrix does not offer one.
func promoteRef(w k8s.Workload) string {
	switch {
	case !w.Found:
		return ""
	case w.Digest != "":
		return w.Image
	case w.RunningDigest != "":
		return k8s.Repository(w.Image) + "@" + w.RunningDigest
	default:
		return ""
	}
}

// lastCopies reads job history for the most recent successful database restore
// and files pull into each environment — the "Database last copied" and "Files
// last copied" lines the Acquia workflow shows under each environment.
func (s *server) lastCopies() (db, files map[int64]string, err error) {
	recent, err := s.runner.DB.Recent(200)
	if err != nil {
		return nil, nil, err
	}
	db, files = map[int64]string{}, map[int64]string{}
	// Recent is newest-first, so the first hit per environment is the latest.
	for _, j := range recent {
		if j.State != jobs.Succeeded {
			continue
		}
		switch j.Kind {
		case jobs.KindRestore:
			if _, seen := db[j.EnvID]; !seen {
				db[j.EnvID] = j.UpdatedAt
			}
		case jobs.KindSyncFiles:
			var p drupal.Params
			_ = json.Unmarshal(j.Payload, &p)
			if p.Push { // a push is the source uploading; a pull is the copy landing
				continue
			}
			if _, seen := files[j.EnvID]; !seen {
				files[j.EnvID] = j.UpdatedAt
			}
		}
	}
	return db, files, nil
}

type jobRow struct {
	Group   string `json:"group"`
	Seq     int    `json:"seq"`
	Env     string `json:"env"`
	Kind    string `json:"kind"`
	State   string `json:"state"`
	Reason  string `json:"reason"`
	Updated string `json:"updated"`
}

func (s *server) jobRows() ([]jobRow, error) {
	recent, err := s.runner.DB.Recent(60)
	if err != nil {
		return nil, err
	}
	envs, err := s.runner.DB.Environments()
	if err != nil {
		return nil, err
	}
	name := map[int64]string{}
	for _, e := range envs {
		name[e.ID] = e.Name
	}

	rows := make([]jobRow, 0, len(recent))
	for _, j := range recent {
		rows = append(rows, jobRow{
			Group:   j.GroupID,
			Seq:     j.Seq,
			Env:     name[j.EnvID],
			Kind:    string(j.Kind),
			State:   string(j.State),
			Reason:  j.Reason,
			Updated: j.UpdatedAt,
		})
	}
	return rows, nil
}

type stallRow struct {
	Group   string `json:"group"`
	Env     string `json:"env"`
	Seq     int    `json:"seq"`
	Kind    string `json:"kind"`
	State   string `json:"state"`
	Reason  string `json:"reason"`
	Waiting int    `json:"waiting"`
}

func (s *server) stallRows() ([]stallRow, error) {
	stalls, err := s.runner.DB.Stalls()
	if err != nil {
		return nil, err
	}
	envs, err := s.runner.DB.Environments()
	if err != nil {
		return nil, err
	}
	name := map[int64]string{}
	for _, e := range envs {
		name[e.ID] = e.Name
	}

	rows := make([]stallRow, 0, len(stalls))
	for _, st := range stalls {
		rows = append(rows, stallRow{
			Group:   st.GroupID,
			Env:     name[st.EnvID],
			Seq:     st.BlockedBySeq,
			Kind:    string(st.BlockedByKind),
			State:   string(st.BlockedByState),
			Reason:  st.BlockedByReason,
			Waiting: st.Waiting,
		})
	}
	return rows, nil
}

type snapshotRow struct {
	ID        int64  `json:"id"`
	Env       string `json:"env"`
	Taken     string `json:"taken"`
	Key       string `json:"key"`
	Sanitized bool   `json:"sanitized"`
}

func (s *server) snapshotRows() ([]snapshotRow, error) {
	envs, err := s.runner.DB.Environments()
	if err != nil {
		return nil, err
	}
	var rows []snapshotRow
	for _, e := range envs {
		snaps, err := s.runner.DB.SnapshotsFor(e.ID)
		if err != nil {
			return nil, err
		}
		for _, sn := range snaps {
			rows = append(rows, snapshotRow{
				ID: sn.ID, Env: e.Name, Taken: sn.CreatedAt, Key: sn.ObjectKey, Sanitized: sn.Sanitized,
			})
		}
	}
	return rows, nil
}

type historyRow struct {
	When    string `json:"when"`
	Actor   string `json:"actor"`
	Action  string `json:"action"`
	Target  string `json:"target"`
	OK      bool   `json:"ok"`
	Outcome string `json:"outcome"` // "ok" or a short failure reason
}

func (s *server) historyRows() ([]historyRow, error) {
	entries, err := s.runner.DB.History(80)
	if err != nil {
		return nil, err
	}
	rows := make([]historyRow, 0, len(entries))
	for _, e := range entries {
		var d struct {
			OK    bool   `json:"ok"`
			Error string `json:"error"`
		}
		_ = json.Unmarshal(e.Detail, &d)
		outcome := "ok"
		if !d.OK {
			outcome = "failed"
			if d.Error != "" {
				outcome = firstLine(d.Error)
			}
		}
		rows = append(rows, historyRow{
			When: e.At, Actor: e.Actor, Action: e.Action, Target: e.Target,
			OK: d.OK, Outcome: outcome,
		})
	}
	return rows, nil
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

type releaseRow struct {
	Built  string `json:"built"`
	Ref    string `json:"ref"`
	GitSHA string `json:"git_sha"`
	Image  string `json:"image"`
}

type scheduleRow struct {
	Name       string `json:"name"`
	Spec       string `json:"spec"`
	What       string `json:"what"` // human summary of the operation
	Enabled    bool   `json:"enabled"`
	Next       string `json:"next"` // next fire time, "" if disabled or unparseable
	LastRun    string `json:"last_run"`
	LastStatus string `json:"last_status"`
}

func (s *server) scheduleRows() ([]scheduleRow, error) {
	scheds, err := s.runner.Schedules()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	rows := make([]scheduleRow, 0, len(scheds))
	for _, sc := range scheds {
		r := scheduleRow{
			Name: sc.Name, Spec: sc.Spec, What: scheduleWhat(sc),
			Enabled: sc.Enabled, LastRun: sc.LastRun, LastStatus: sc.LastStatus,
		}
		if sc.Enabled {
			base := now
			if t, err := time.Parse(time.RFC3339, sc.LastRun); err == nil {
				base = t
			}
			if next, err := cron.Next(sc.Spec, base); err == nil {
				r.Next = next.Format("2006-01-02 15:04 MST")
			}
		}
		rows = append(rows, r)
	}
	return rows, nil
}

func scheduleWhat(s store.Schedule) string {
	switch s.Op {
	case "snapshot":
		return "snapshot " + s.Env
	case "copy-down":
		lane := "files + database"
		if s.SkipFiles {
			lane = "database"
		} else if s.SkipDB {
			lane = "files"
		}
		return fmt.Sprintf("copy-down %s→%s (%s)", s.From, s.To, lane)
	case "console":
		if s.Shell {
			return fmt.Sprintf("console %s: sh -c %q", s.Env, s.Cmd)
		}
		return fmt.Sprintf("console %s: drush %s", s.Env, s.Cmd)
	default:
		return s.Op
	}
}

func (s *server) releaseRows() ([]releaseRow, error) {
	rels, err := s.runner.Releases()
	if err != nil {
		return nil, err
	}
	rows := make([]releaseRow, 0, len(rels))
	for _, r := range rels {
		rows = append(rows, releaseRow{
			Built: r.BuiltAt, Ref: shortRef(r.GitRef), GitSHA: shortSHA(r.GitSHA), Image: r.ImageRef,
		})
	}
	return rows, nil
}

type stateView struct {
	Running   bool     `json:"running"`
	Operation string   `json:"operation"`
	Log       []string `json:"log"`
}

func (s *server) stateView() stateView {
	s.mu.Lock()
	defer s.mu.Unlock()
	logs := make([]string, len(s.logs))
	copy(logs, s.logs)
	return stateView{Running: s.current != "", Operation: s.current, Log: logs}
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return `{"error":"marshal failed"}`
	}
	return string(b)
}
