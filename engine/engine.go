// Package engine wires graph validation, bounded scheduling, local
// execution, cache and retries into the public pipeline runner.
package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/lisama569/build-pipeline-engine-go/pipeline"
	"github.com/lisama569/build-pipeline-engine-go/pkg/cache"
	"github.com/lisama569/build-pipeline-engine-go/pkg/exec"
	"github.com/lisama569/build-pipeline-engine-go/pkg/graph"
	"github.com/lisama569/build-pipeline-engine-go/pkg/report"
	"github.com/lisama569/build-pipeline-engine-go/pkg/retry"
	"github.com/lisama569/build-pipeline-engine-go/pkg/scheduler"
)

// Hard configuration bounds. Values beyond these are clamped or rejected so
// no configuration can request unbounded parallelism, time or retries.
const (
	HardMaxParallelism = 64
	HardMaxTimeout     = 30 * time.Minute
	HardShutdownGrace  = 60 * time.Second
)

// Engine runs pipelines. A single Engine can safely run several pipelines
// concurrently; its cache and in-flight deduper are shared, so a task with
// the same cache key is executed at most once and every observer receives
// the identical conclusion.
type Engine struct {
	cfg     pipeline.Config
	runner  *exec.Runner
	store   *cache.Store
	deduper *cache.InFlight
	pol     retry.Policy
	envHash string
}

// NewEngine validates cfg (clamping to documented hard bounds), opens the
// cache and prepares the shared runner/deduper.
func NewEngine(cfg pipeline.Config) (*Engine, error) {
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}
	store, err := cache.Open(cfg.CacheDir)
	if err != nil {
		return nil, err
	}
	pol, err := retry.NewPolicy(cfg.MaxAttempts, cfg.RetryBackoffBase, cfg.RetryBackoffCap)
	if err != nil {
		return nil, err
	}
	return &Engine{
		cfg:     cfg,
		runner:  exec.NewRunner(cfg.RootDir),
		store:   store,
		deduper: cache.NewInFlight(),
		pol:     pol,
		envHash: environmentFingerprint(),
	}, nil
}

func validateConfig(cfg *pipeline.Config) error {
	if cfg.MaxParallelism < 1 {
		return pipeline.NewError(pipeline.ErrConfigInvalid,
			"max parallelism must be >= 1, got %d", cfg.MaxParallelism)
	}
	if cfg.MaxParallelism > HardMaxParallelism {
		cfg.MaxParallelism = HardMaxParallelism // clamp, do not error
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 1
	}
	if cfg.MaxAttempts < 0 {
		return pipeline.NewError(pipeline.ErrConfigInvalid, "max attempts must be >= 1")
	}
	if cfg.DefaultTaskTimeout < 0 {
		return pipeline.NewError(pipeline.ErrConfigInvalid, "default task timeout must be >= 0")
	}
	if cfg.DefaultTaskTimeout > HardMaxTimeout {
		cfg.DefaultTaskTimeout = HardMaxTimeout
	}
	if cfg.ShutdownGrace < 0 {
		return pipeline.NewError(pipeline.ErrConfigInvalid, "shutdown grace must be >= 0")
	}
	if cfg.ShutdownGrace == 0 {
		cfg.ShutdownGrace = 10 * time.Second
	}
	if cfg.ShutdownGrace > HardShutdownGrace {
		cfg.ShutdownGrace = HardShutdownGrace
	}
	switch cfg.QueuePolicy {
	case "", pipeline.QueueFIFO:
		cfg.QueuePolicy = pipeline.QueueFIFO
	case pipeline.QueuePriority:
	default:
		return pipeline.NewError(pipeline.ErrConfigInvalid,
			"unknown queue policy %q", cfg.QueuePolicy)
	}
	if cfg.RetryBackoffBase < 0 || cfg.RetryBackoffCap < 0 {
		return pipeline.NewError(pipeline.ErrConfigInvalid, "backoff must be >= 0")
	}
	return nil
}

// environmentFingerprint hashes the full sorted process environment so a
// change in the ambient environment changes every cache key.
func environmentFingerprint() string {
	env := os.Environ()
	sorted := append([]string(nil), env...)
	sort.Strings(sorted)
	h := sha256.New()
	h.Write([]byte(strings.Join(sorted, "\n")))
	return hex.EncodeToString(h.Sum(nil))
}

// Run validates and executes p to completion.
func (e *Engine) Run(ctx context.Context, p *pipeline.Pipeline) (*report.Report, error) {
	if p == nil {
		return nil, pipeline.NewError(pipeline.ErrConfigInvalid, "nil pipeline")
	}
	g, err := graph.New(p.Tasks)
	if err != nil {
		return nil, err
	}
	return e.RunGraph(ctx, p.Name, g)
}

// RunGraph runs a pre-validated graph.
func (e *Engine) RunGraph(ctx context.Context, name string, g *graph.Graph) (*report.Report, error) {
	sched := scheduler.New(g, e.cfg, e.runner, e.store, e.deduper, e.pol, e.envHash)
	results := sched.Run(ctx)
	return report.Build(name, results, sched.Events()), nil
}

// Store exposes the cache store (used by tests for poisoning entries).
func (e *Engine) Store() *cache.Store { return e.store }

// Close releases engine resources.
func (e *Engine) Close() error { return nil }
