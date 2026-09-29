// Command pipeline is the local CLI entry point. It loads a declarative
// pipeline spec, validates it, executes with bounded parallelism (optionally
// resuming from the previous run and bounded by a run-wide time budget) and
// emits a structured report. It needs no network, credentials or paid
// resources.
//
// Subcommands:
//
//	pipeline run   -spec <path> [flags]   (default when no subcommand given)
//	pipeline history -state <dir> [-pipeline <name>] [-diff <a> <b>]
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lisama569/build-pipeline-engine-go/engine"
	"github.com/lisama569/build-pipeline-engine-go/pipeline"
	"github.com/lisama569/build-pipeline-engine-go/pkg/history"
)

func main() {
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "history" {
		os.Exit(runHistory(args[1:]))
	}
	// Default (and explicit "run") subcommand.
	if len(args) > 0 && args[0] == "run" {
		args = args[1:]
	}
	os.Exit(runPipeline(args))
}

func runPipeline(args []string) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	var (
		specPath    = fs.String("spec", "", "path to pipeline spec JSON")
		parallel    = fs.Int("parallelism", 4, "max parallel tasks (1..64)")
		attempts    = fs.Int("attempts", 1, "max attempts per task")
		taskTimeout = fs.Duration("task-timeout", 10*time.Minute, "per-attempt timeout")
		grace       = fs.Duration("shutdown-grace", 10*time.Second, "cancel convergence bound")
		policy      = fs.String("queue", "fifo", "ready queue policy: fifo|priority")
		cacheDir    = fs.String("cache", ".pipeline-cache", "cache directory (empty disables)")
		failFast    = fs.Bool("fail-fast", false, "cancel remaining work on first failure")
		runBudget   = fs.Duration("run-budget", 0, "total run wall-clock budget (0 = unlimited), e.g. 10m")
		stateDir    = fs.String("state", ".pipeline-state", "resume/history state directory (empty disables)")
		resumeRun   = fs.Bool("resume", false, "reuse still-valid verdicts of the previous run")
		noResume    = fs.Bool("no-resume", false, "force a clean run even when state exists")
		historyN    = fs.Int("history", 10, "retained run records per pipeline (0 disables, max 50)")
		jsonOut     = fs.Bool("json", false, "emit machine-readable JSON report")
	)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *specPath == "" {
		fmt.Fprintln(os.Stderr, "missing -spec")
		fs.Usage()
		return 2
	}
	if *noResume {
		*resumeRun = false
	}

	p, err := engine.LoadSpec(*specPath)
	if err != nil {
		return fatal("load", err)
	}

	eng, err := engine.NewEngine(pipeline.Config{
		MaxParallelism:     *parallel,
		MaxAttempts:        *attempts,
		DefaultTaskTimeout: *taskTimeout,
		ShutdownGrace:      *grace,
		QueuePolicy:        pipeline.QueuePolicy(*policy),
		CacheDir:           *cacheDir,
		FailFast:           *failFast,
		RetryBackoffBase:   50 * time.Millisecond,
		RetryBackoffCap:    2 * time.Second,
		RunBudget:          *runBudget,
		StateDir:           *stateDir,
		Resume:             *resumeRun,
		HistoryRuns:        *historyN,
	})
	if err != nil {
		return fatal("config", err)
	}
	defer eng.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rep, err := eng.Run(ctx, p)
	if err != nil {
		return fatal("validate", err) // graph/config errors are distinguishable here
	}

	if *jsonOut {
		raw, _ := json.MarshalIndent(rep, "", "  ")
		fmt.Println(string(raw))
	} else {
		fmt.Print(rep.Text())
	}
	if rep.Status != "succeeded" {
		return 1
	}
	return 0
}

func runHistory(args []string) int {
	fs := flag.NewFlagSet("history", flag.ContinueOnError)
	var (
		stateDir  = fs.String("state", ".pipeline-state", "state directory written by runs")
		pipeName  = fs.String("pipeline", "", "pipeline name (default: list pipelines)")
		jsonOut   = fs.Bool("json", false, "emit machine-readable JSON")
		diffOlder = fs.String("diff", "", "compare two runs: older run id (use with -diff-newer)")
		diffNewer = fs.String("diff-newer", "", "newer run id; empty means the latest retained run")
	)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *stateDir == "" {
		fmt.Fprintln(os.Stderr, "history needs a -state directory")
		return 2
	}
	store, err := history.Open(*stateDir)
	if err != nil {
		return fatal("history", err)
	}
	if !store.Enabled() {
		fmt.Fprintln(os.Stderr, "history disabled (no state directory)")
		return 2
	}

	if *diffOlder != "" {
		return showDiff(store, *pipeName, *diffOlder, *diffNewer, *jsonOut)
	}
	return showList(store, *pipeName, *jsonOut)
}

func showList(store *history.Store, pipeName string, jsonOut bool) int {
	if pipeName == "" {
		// No per-pipeline name available without a run; state layout uses
		// hashed directories, so require a pipeline for listing.
		fmt.Fprintln(os.Stderr, "use -pipeline <name> to list a pipeline's runs")
		return 2
	}
	recs, err := store.List(pipeName)
	if err != nil {
		return fatal("history", err)
	}
	if jsonOut {
		raw, _ := json.MarshalIndent(recs, "", "  ")
		fmt.Println(string(raw))
		return 0
	}
	if len(recs) == 0 {
		fmt.Printf("no retained runs for pipeline %q\n", pipeName)
		return 0
	}
	fmt.Printf("pipeline %q (%d retained runs, newest first):\n", pipeName, len(recs))
	for _, r := range recs {
		parent := ""
		if r.ParentRunID != "" {
			parent = " parent=" + r.ParentRunID
		}
		fmt.Printf("  %s  %-10s stop=%-17s reused=%d reran=%d cached=%d%s\n",
			r.RunID, r.RunStatus, r.StopReason,
			r.ReusedCount, r.ReranCount, r.CachedCount, parent)
	}
	return 0
}

func showDiff(store *history.Store, pipeName, olderID, newerID string, jsonOut bool) int {
	if pipeName == "" {
		fmt.Fprintln(os.Stderr, "diff needs -pipeline <name>")
		return 2
	}
	older, err := store.Load(pipeName, olderID)
	if err != nil {
		return fatal("history", err)
	}
	if older == nil {
		fmt.Fprintf(os.Stderr, "no run %q for pipeline %q\n", olderID, pipeName)
		return 1
	}
	var newer *history.Record
	if newerID == "" {
		newer, err = store.Latest(pipeName)
		if err != nil {
			return fatal("history", err)
		}
	} else {
		newer, err = store.Load(pipeName, newerID)
		if err != nil {
			return fatal("history", err)
		}
	}
	if newer == nil {
		fmt.Fprintln(os.Stderr, "newer run not found")
		return 1
	}
	d := history.Compare(older, newer)
	if jsonOut {
		raw, _ := json.MarshalIndent(d, "", "  ")
		fmt.Println(string(raw))
		return 0
	}
	fmt.Printf("diff %s -> %s (pipeline %q)\n", d.OlderRunID, d.NewerRunID, pipeName)
	fmt.Printf("  reused (%d): %v\n", len(d.Reused), d.Reused)
	fmt.Printf("  new (%d): %v\n", len(d.New), d.New)
	fmt.Printf("  removed (%d): %v\n", len(d.Removed), d.Removed)
	fmt.Printf("  reran (%d):\n", len(d.Reran))
	for _, r := range d.Reran {
		fmt.Printf("    - %s: %s\n", r.ID, r.Reason)
	}
	fmt.Printf("  status changed (%d):\n", len(d.StatusChanged))
	for _, c := range d.StatusChanged {
		fmt.Printf("    - %s: %s -> %s\n", c.ID, c.Before, c.After)
	}
	return 0
}

func fatal(stage string, err error) int {
	fmt.Fprintf(os.Stderr, "%s error: %v\n", stage, err)
	return 1
}
