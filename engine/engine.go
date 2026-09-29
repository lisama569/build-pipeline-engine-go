// Package engine wires graph validation, bounded scheduling, local
// execution, cache and retries into the public pipeline runner. It also
// owns the run-level features layered over a single scheduler run:
//
//   - resume (Config.Resume + Config.StateDir): load the previous run's
//     checksummed checkpoint, pre-seed still-valid successful verdicts and
//     re-execute only failed/skipped/canceled, changed/new, cache-disabled
//     and dependency-restructured tasks plus their closure;
//   - run budget (Config.RunBudget): a wall-clock deadline reported
//     distinctly as stop_reason=budget_exhausted;
//   - run history (Config.HistoryRuns): bounded, per-pipeline comparable run
//     records safe for concurrent writers.
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
	"github.com/lisama569/build-pipeline-engine-go/pkg/history"
	"github.com/lisama569/build-pipeline-engine-go/pkg/report"
	"github.com/lisama569/build-pipeline-engine-go/pkg/resume"
	"github.com/lisama569/build-pipeline-engine-go/pkg/retry"
	"github.com/lisama569/build-pipeline-engine-go/pkg/scheduler"
)

// Hard configuration bounds. Values beyond these are clamped or rejected so
// no configuration can request unbounded parallelism, time or retries.
const (
	HardMaxParallelism = 64
	HardMaxTimeout     = 30 * time.Minute
	HardShutdownGrace  = 60 * time.Second
	// HardMaxRunBudget caps the run-wide wall-clock budget (24h).
	HardMaxRunBudget = 24 * time.Hour
	// HardHistoryRuns is the upper bound for retained run records/pipeline.
	HardHistoryRuns = history.HardHistoryCap
)

// Engine runs pipelines. A single Engine can safely run several pipelines
// concurrently; its cache and in-flight deduper are shared, so a task with
// the same cache key is executed at most once and every observer receives
// the identical conclusion. Resume checkpoints and run history are isolated
// per pipeline name and written atomically, so concurrent runs cannot
// overwrite each other.
type Engine struct {
	cfg          pipeline.Config
	runner       *exec.Runner
	store        *cache.Store
	deduper      *cache.InFlight
	pol          retry.Policy
	envHash      string
	checkpoints  *resume.Store
	historyStore *history.Store
}

// NewEngine validates cfg (clamping to documented hard bounds), opens the
// cache and state stores and prepares the shared runner/deduper.
func NewEngine(cfg pipeline.Config) (*Engine, error) {
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}
	store, err := cache.Open(cfg.CacheDir)
	if err != nil {
		return nil, err
	}
	checkpoints, err := resume.OpenStore(cfg.StateDir)
	if err != nil {
		return nil, err
	}
	historyStore, err := history.Open(cfg.StateDir)
	if err != nil {
		return nil, err
	}
	pol, err := retry.NewPolicy(cfg.MaxAttempts, cfg.RetryBackoffBase, cfg.RetryBackoffCap)
	if err != nil {
		return nil, err
	}
	return &Engine{
		cfg:          cfg,
		runner:       exec.NewRunner(cfg.RootDir),
		store:        store,
		deduper:      cache.NewInFlight(),
		pol:          pol,
		envHash:      environmentFingerprint(),
		checkpoints:  checkpoints,
		historyStore: historyStore,
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
	if cfg.RunBudget < 0 {
		return pipeline.NewError(pipeline.ErrConfigInvalid, "run budget must be >= 0")
	}
	if cfg.RunBudget > HardMaxRunBudget {
		cfg.RunBudget = HardMaxRunBudget // clamp, do not error
	}
	if cfg.HistoryRuns < 0 {
		return pipeline.NewError(pipeline.ErrConfigInvalid, "history runs must be >= 0")
	}
	if cfg.HistoryRuns > HardHistoryRuns {
		cfg.HistoryRuns = HardHistoryRuns // clamp, do not error
	}
	if cfg.Resume && cfg.StateDir == "" {
		return pipeline.NewError(pipeline.ErrConfigInvalid,
			"resume requires a state directory (StateDir)")
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
	return e.runGraph(ctx, p.Name, p.Tasks, g)
}

// RunGraph runs a pre-validated graph.
func (e *Engine) RunGraph(ctx context.Context, name string, g *graph.Graph) (*report.Report, error) {
	tasks := make([]*pipeline.Task, 0, len(g.Nodes()))
	for _, n := range g.Nodes() {
		tasks = append(tasks, n.Task)
	}
	return e.runGraph(ctx, name, tasks, g)
}

// runGraph executes one run, optionally resuming from the previous checkpoint,
// then persists the new checkpoint and history record. Persistence failures
// never discard the run's conclusion: the report is still returned.
func (e *Engine) runGraph(ctx context.Context, name string, tasks []*pipeline.Task,
	g *graph.Graph) (*report.Report, error) {
	runID := resume.NewRunID()
	startedAt := time.Now()

	// 1) load the previous checkpoint (if resuming) and compute the plan.
	var plan *resume.Plan
	var parentRunID string
	var resumeMeta report.ResumeMeta
	var err error
	if e.cfg.Resume && e.checkpoints.Enabled() {
		// A corrupt checkpoint is rejected (and quarantined by the store) and
		// the run proceeds clean: cp stays nil, plan stays nil.
		cp, _ := e.checkpoints.Load(name)
		if cp != nil {
			plan, err = resume.Build(tasks, cp, e.envHash)
			if err != nil {
				return nil, err
			}
			parentRunID = cp.RunID
			resumeMeta = report.ResumeMeta{
				Resumed:     true,
				ParentRunID: cp.RunID,
				ReusedTasks: plan.ReusableTasks(),
				ReusedCount: plan.ReusedCount(),
				ReranCount:  plan.ReranCount(),
			}
		}
	}

	// 2) execute.
	sched := scheduler.NewWithPlan(g, e.cfg, e.runner, e.store, e.deduper, e.pol,
		e.envHash, plan)
	results := sched.Run(ctx)
	stopReason := sched.StopReason()

	// 3) report.
	rep := report.BuildWithMeta(name, results, sched.Events(), report.Meta{
		RunID:      runID,
		StopReason: stopReason,
		Resume:     resumeMeta,
		RunBudget:  e.cfg.RunBudget,
	})

	// 4) persist checkpoint + history (best effort; state issues never erase
	// the conclusion that was actually reached).
	cachedCount := 0
	for _, r := range results {
		if r.Cached {
			cachedCount++
		}
	}
	if e.checkpoints.Enabled() {
		cp := resume.BuildCheckpoint(name, runID, tasks, results, e.envHash, time.Now())
		if err := e.checkpoints.Save(cp); err != nil {
			rep.NonDeterminism = append(rep.NonDeterminism,
				"checkpoint write failed: "+err.Error())
		}
	}
	if e.historyStore.Enabled() && e.cfg.HistoryRuns > 0 {
		rec := history.FromResults(name, runID, parentRunID, startedAt,
			stopReason, rep.Status, rep.Conclusion, tasks, results,
			resumeMeta.ReusedCount, cachedCount)
		if err := e.historyStore.Save(rec, e.cfg.HistoryRuns); err != nil {
			rep.NonDeterminism = append(rep.NonDeterminism,
				"history write failed: "+err.Error())
		}
	}
	return rep, nil
}

// History exposes the retained run-record store (nil when StateDir is unset).
func (e *Engine) History() *history.Store { return e.historyStore }

// Checkpoints exposes the resume checkpoint store (nil when StateDir unset).
func (e *Engine) Checkpoints() *resume.Store { return e.checkpoints }

// Store exposes the cache store (used by tests for poisoning entries).
func (e *Engine) Store() *cache.Store { return e.store }

// Close releases engine resources.
func (e *Engine) Close() error { return nil }
