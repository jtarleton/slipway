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
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jtarleton/slipway/internal/grid"
	"github.com/jtarleton/slipway/internal/ops"
)

// reconcileInterval is how often the background loop makes an engine pass while
// jobs are in flight. It matches RunSequence's own poll interval.
const reconcileInterval = 3 * time.Second

// gridInterval is how often the grid is re-read from the cluster and pushed to
// connected browsers. The grid is three API calls; a homelab cluster does not
// need it faster, and every operation forces a refresh when it ends anyway.
const gridInterval = 15 * time.Second

// Serve runs the web UI and the reconcile loop until the process is signalled.
func Serve(ctx context.Context, addr string, runner *ops.Runner) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	s := &server{runner: runner, hub: newHub()}
	// Route the Runner's progress to every browser. Single-flight (see start)
	// means only one operation writes here at a time.
	runner.Report = s.emit

	mux := http.NewServeMux()
	s.routes(mux)

	httpSrv := &http.Server{
		Addr:         announcedAddr(addr),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 0, // SSE responses are open-ended
	}

	go s.background(ctx)

	errc := make(chan error, 1)
	go func() {
		log.Printf("slipway: listening on %s", addr)
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

func announcedAddr(addr string) string {
	if addr == "" {
		return ":8080"
	}
	return addr
}

type server struct {
	runner *ops.Runner
	hub    *hub

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
	mux.HandleFunc("GET /events", s.handleEvents)
	mux.HandleFunc("GET /api/grid", s.handleGrid)
	mux.HandleFunc("GET /api/jobs", s.handleJobs)
	mux.HandleFunc("GET /api/state", s.handleState)
	mux.HandleFunc("POST /api/pin", s.handlePin)
	mux.HandleFunc("POST /api/deploy", s.handleDeploy)
	mux.HandleFunc("POST /api/copy-down", s.handleCopyDown)
	mux.HandleFunc("POST /api/resume", s.handleResume)
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

	s.pushGrid(ctx)
	s.pushJobs()

	for {
		select {
		case <-ctx.Done():
			return

		case <-gridTick.C:
			s.pushGrid(ctx)

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
}

func (s *server) broadcastState() {
	s.hub.broadcast("state", mustJSON(s.stateView()))
}

// --- view models -----------------------------------------------------------

type gridRow struct {
	Env       string `json:"env"`
	Namespace string `json:"namespace"`
	Host      string `json:"host"`
	Prod      bool   `json:"prod"`
	Code      string `json:"code"`
	Pinned    bool   `json:"pinned"`
	Replicas  string `json:"replicas"`
	State     string `json:"state"`
}

func (s *server) gridRows(ctx context.Context) ([]gridRow, error) {
	cells, err := s.runner.Grid(ctx)
	if err != nil {
		return nil, err
	}
	rows := make([]gridRow, 0, len(cells))
	for _, c := range cells {
		rows = append(rows, gridRow{
			Env:       c.Env.Name,
			Namespace: c.Env.Namespace,
			Host:      c.Env.IngressHost,
			Prod:      c.Env.IsProduction,
			Code:      grid.Code(c.Workload),
			Pinned:    grid.Pinned(c.Workload),
			Replicas:  fmt.Sprintf("%d/%d", c.Workload.Ready, c.Workload.Desired),
			State:     grid.State(c.Workload),
		})
	}
	return rows, nil
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
