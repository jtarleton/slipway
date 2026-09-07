// Command slipway is the control plane.
//
// Phase 2: the grid reads the cluster, and the code lane can write to it. The
// subcommands here and the web UI behind `slipway serve` drive the same
// operations in internal/ops — the CLI just points their progress at stdout.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/jtarleton/slipway/internal/drupal"
	"github.com/jtarleton/slipway/internal/grid"
	"github.com/jtarleton/slipway/internal/k8s"
	"github.com/jtarleton/slipway/internal/ops"
	"github.com/jtarleton/slipway/internal/store"
	"github.com/jtarleton/slipway/internal/web"
)

const usage = `slipway — deployment control plane for Drupal on k3s

  slipway grid                     show what is running in every environment
  slipway pin   -env NAME          rewrite a tag-pinned Deployment to the digest it is already running
  slipway deploy -env NAME -image REF   snapshot the database, deploy, wait for rollout, run update hooks
        -no-snapshot  skip the pre-deploy snapshot   -skip-update  skip update hooks
  slipway snapshot  -env NAME       dump the database to object storage and record it
  slipway snapshots -env NAME       list the recorded snapshots for an environment
  slipway restore  -env NAME -snapshot ID   load a specific snapshot back into its environment
  slipway rollback -env NAME        re-deploy the previous image   -with-data  also restore its pre-deploy snapshot
  slipway copy-down -from prod -to stage   copy database and files down, sanitizing on arrival
        -skip-files  database only   -skip-db  files only   -clean  empty the target tree first (first seed)
  slipway resume                   re-attach to work left in flight
  slipway cancel -group NAME        release a stalled sequence, cancelling its unfinished steps
  slipway history                  show the operation log — what ran, against what, how it turned out
  slipway serve -addr :8080        run the web UI and the reconcile loop

Flags:
  -db          control-plane database (default slipway.db)
  -kubeconfig  kubeconfig path (default: standard loading rules)
  -workload    name of the Drupal Deployment (default drupal)
  -container   container within that Deployment (default drupal)
  -addr        address for 'serve' to listen on (default :8080)
`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "slipway: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		fmt.Print(usage)
		return nil
	}

	command, rest := args[0], args[1:]
	fs := flag.NewFlagSet(command, flag.ExitOnError)
	var (
		dbPath     = fs.String("db", "slipway.db", "control-plane database")
		kubeconfig = fs.String("kubeconfig", "", "kubeconfig path")
		workload   = fs.String("workload", "drupal", "Drupal Deployment name")
		container  = fs.String("container", "drupal", "container name")
		env        = fs.String("env", "", "environment name")
		skipUpdate = fs.Bool("skip-update", false, "deploy code without running update hooks")
		skipConfig = fs.Bool("skip-config-import", false, "omit config:import from the update sequence")
		noSnapshot = fs.Bool("no-snapshot", false, "deploy without taking a pre-deploy database snapshot")
		withData   = fs.Bool("with-data", false, "rollback: also restore the deploy's pre-deploy snapshot")
		from       = fs.String("from", "", "source environment for copy-down")
		to         = fs.String("to", "", "target environment for copy-down")
		skipFiles  = fs.Bool("skip-files", false, "copy the database only, leaving files alone")
		skipDB     = fs.Bool("skip-db", false, "copy files only, leaving the database alone")
		clean      = fs.Bool("clean", false, "empty the target file tree before pulling (needed on first seed)")
		image      = fs.String("image", "", "image reference to deploy")
		group      = fs.String("group", "", "job group for 'cancel'")
		snapshotID = fs.Int64("snapshot", 0, "snapshot id for 'restore'")
		addr       = fs.String("addr", ":8080", "address for 'serve' to listen on")
	)
	if err := fs.Parse(rest); err != nil {
		return err
	}

	db, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := seed(db); err != nil {
		return err
	}

	client, err := k8s.New(*kubeconfig)
	if err != nil {
		return err
	}

	runner := &ops.Runner{
		DB:        db,
		Client:    client,
		Workload:  *workload,
		Container: *container,
		Images:    drupal.DefaultImages(),
		Report:    func(line string) { fmt.Println(line) },
		Actor:     "cli",
	}

	if command == "serve" {
		runner.Actor = "web"
		return web.Serve(context.Background(), *addr, runner)
	}

	// Reading the grid should fail fast; moving a database should not fail at
	// all for want of patience. A single shared deadline can only be wrong for
	// one of them.
	deadline := 30 * time.Second
	switch command {
	case "pin":
		deadline = 15 * time.Minute
	case "deploy", "snapshot", "restore", "rollback", "copy-down", "resume":
		deadline = 6 * time.Hour
	}
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()

	switch command {
	case "grid":
		return printGrid(ctx, runner)
	case "history":
		return printHistory(db)
	case "pin":
		return runner.Pin(ctx, *env)
	case "deploy":
		return runner.Deploy(ctx, *env, *image, *skipUpdate, *skipConfig, *noSnapshot)
	case "snapshot":
		return runner.Snapshot(ctx, *env)
	case "snapshots":
		return printSnapshots(runner, *env)
	case "restore":
		if *snapshotID == 0 {
			return fmt.Errorf("restore needs -snapshot ID (see 'slipway snapshots -env %s')", *env)
		}
		return runner.Restore(ctx, *env, *snapshotID)
	case "rollback":
		return runner.Rollback(ctx, *env, *withData)
	case "copy-down":
		return runner.CopyDown(ctx, *from, *to, *skipFiles, *skipDB, *clean)
	case "resume":
		return runner.Resume(ctx)
	case "cancel":
		return runner.Cancel(ctx, *group)
	default:
		fmt.Print(usage)
		return fmt.Errorf("unknown command %q", command)
	}
}

func printHistory(db *store.DB) error {
	entries, err := db.History(50)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		fmt.Println("no operations recorded yet")
		return nil
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "WHEN\tACTOR\tACTION\tTARGET\tOUTCOME")
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
				outcome = "failed: " + firstLine(d.Error)
			}
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", e.At, e.Actor, e.Action, e.Target, outcome)
	}
	return w.Flush()
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

func printSnapshots(runner *ops.Runner, env string) error {
	if env == "" {
		return fmt.Errorf("snapshots needs -env")
	}
	snaps, err := runner.Snapshots(env)
	if err != nil {
		return err
	}
	if len(snaps) == 0 {
		fmt.Printf("%s has no recorded snapshots\n", env)
		return nil
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "ID\tTAKEN\tSANITIZED\tOBJECT")
	for _, s := range snaps {
		fmt.Fprintf(w, "%d\t%s\t%t\t%s\n", s.ID, s.CreatedAt, s.Sanitized, s.ObjectKey)
	}
	return w.Flush()
}

func printGrid(ctx context.Context, runner *ops.Runner) error {
	cells, err := runner.Grid(ctx)
	if err != nil {
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "ENVIRONMENT\tNAMESPACE\tCODE\tREPLICAS\tSTATE")
	for _, c := range cells {
		fmt.Fprintf(w, "%s\t%s\t%s\t%d/%d\t%s\n",
			c.Env.Name, c.Env.Namespace, grid.Code(c.Workload),
			c.Workload.Ready, c.Workload.Desired, grid.State(c.Workload))
	}
	return w.Flush()
}

func seed(db *store.DB) error {
	envs, err := db.Environments()
	if err != nil {
		return err
	}
	if len(envs) > 0 {
		return nil
	}
	for _, e := range []store.Environment{
		{Name: "dev", Rank: 10, Namespace: "jt-drupal-dev", IngressHost: "dev.jamestarleton.com"},
		{Name: "stage", Rank: 20, Namespace: "jt-drupal-stage", IngressHost: "stage.jamestarleton.com"},
		{Name: "prod", Rank: 30, Namespace: "jt-drupal", IngressHost: "jamestarleton.com", IsProduction: true},
	} {
		if _, err := db.UpsertEnvironment(e); err != nil {
			return err
		}
	}
	return nil
}
