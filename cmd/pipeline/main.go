// Command pipeline is the local CLI for running JSON-described build
// pipelines. It executes repository-native build/verification commands with
// bounded parallelism and emits a deterministic JSON report. It uses no
// external services, credentials or paid resources.
//
// Usage:
//
//	pipeline -spec pipeline.json [-report report.json] [-jobs 4] \
//	         [-cachedir .pipeline-cache] [-timeout 5m] [-retries 2]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/lisama569/build-pipeline-engine-go/pipeline"
)

func main() {
	if code := run(); code != 0 {
		os.Exit(code)
	}
}

func run() int {
	specPath := flag.String("spec", "pipeline.json", "path to the JSON pipeline spec")
	reportPath := flag.String("report", "", "write the JSON report to this path (stdout if empty)")
	jobs := flag.Int("jobs", 0, "parallelism limit (default 4; hard max 256)")
	cacheDir := flag.String("cachedir", "", "cache directory (empty = in-memory only)")
	timeout := flag.Duration("timeout", 0, "default per-task timeout (max 30m)")
	maxTimeout := flag.Duration("max-timeout", 0, "maximum accepted task timeout")
	retries := flag.Int("retries", 0, "default retries per task (max 10)")
	backoff := flag.Duration("backoff", 0, "retry base backoff (exponential, capped)")
	grace := flag.Duration("shutdown-grace", 0, "bounded wait for canceled processes")
	root := flag.String("root", ".", "repository root for relative inputs and commands")
	verbose := flag.Bool("v", false, "log scheduling decisions to stderr")
	flag.Parse()

	cfg := pipeline.DefaultConfig()
	cfg.RepoRoot = *root
	cfg.CacheDir = *cacheDir
	if *jobs != 0 {
		cfg.Parallelism = *jobs
	}
	if *timeout != 0 {
		cfg.DefaultTimeout = *timeout
	}
	if *maxTimeout != 0 {
		cfg.MaxTimeout = *maxTimeout
	}
	if *retries != 0 {
		cfg.DefaultRetries = *retries
	}
	if *backoff != 0 {
		cfg.RetryBackoff = *backoff
	}
	if *grace != 0 {
		cfg.ShutdownGrace = *grace
	}
	if *verbose {
		cfg.Logf = func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, "[scheduler] "+format+"\n", args...)
		}
	}

	spec, err := pipeline.LoadSpec(*specPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "spec error:", err)
		return 2
	}

	engine, err := pipeline.NewEngine(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "config error:", err)
		return 2
	}
	defer engine.Close()

	p, err := engine.ToPipeline(spec)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pipeline error:", err)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	report, err := engine.Run(ctx, p)
	if err != nil {
		fmt.Fprintln(os.Stderr, "graph error:", err)
		return 2
	}
	raw, err := report.JSON()
	if err != nil {
		fmt.Fprintln(os.Stderr, "report error:", err)
		return 2
	}
	if *reportPath != "" {
		if err := os.WriteFile(*reportPath, raw, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "report write error:", err)
			return 2
		}
	} else {
		fmt.Println(string(raw))
	}

	if report.Status == pipeline.StatusSucceeded {
		return 0
	}
	return 1
}
