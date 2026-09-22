// Command pipeline is the local CLI entry point. It loads a declarative
// pipeline spec, validates it, executes with bounded parallelism and emits a
// structured report. It needs no network, credentials or paid resources.
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
)

func main() {
	var (
		specPath    = flag.String("spec", "", "path to pipeline spec JSON")
		parallel    = flag.Int("parallelism", 4, "max parallel tasks (1..64)")
		attempts    = flag.Int("attempts", 1, "max attempts per task")
		taskTimeout = flag.Duration("task-timeout", 10*time.Minute, "per-attempt timeout")
		grace       = flag.Duration("shutdown-grace", 10*time.Second, "cancel convergence bound")
		policy      = flag.String("queue", "fifo", "ready queue policy: fifo|priority")
		cacheDir    = flag.String("cache", ".pipeline-cache", "cache directory (empty disables)")
		failFast    = flag.Bool("fail-fast", false, "cancel remaining work on first failure")
		jsonOut     = flag.Bool("json", false, "emit machine-readable JSON report")
	)
	flag.Parse()
	if *specPath == "" {
		fmt.Fprintln(os.Stderr, "missing -spec")
		flag.Usage()
		os.Exit(2)
	}

	p, err := engine.LoadSpec(*specPath)
	if err != nil {
		fatal("load", err)
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
	})
	if err != nil {
		fatal("config", err)
	}
	defer eng.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rep, err := eng.Run(ctx, p)
	if err != nil {
		fatal("validate", err) // graph/config errors are distinguishable here
	}

	if *jsonOut {
		raw, _ := json.MarshalIndent(rep, "", "  ")
		fmt.Println(string(raw))
	} else {
		fmt.Print(rep.Text())
	}
	if rep.Status != "succeeded" {
		os.Exit(1)
	}
}

func fatal(stage string, err error) {
	fmt.Fprintf(os.Stderr, "%s error: %v\n", stage, err)
	os.Exit(1)
}
