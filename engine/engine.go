// Package engine wires graph validation, bounded scheduling, local
// execution, cache, retries, resume, run budgets and retained run history
// into the public pipeline runner.
package engine

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/lisama569/build-pipeline-engine-go/pipeline"
	"github.com/lisama569/build-pipeline-engine-go/pkg/cache"
	"github.com/lisama569/build-pipeline-engine-go/pkg/exec"
	"github.com/lisama569/build-pipeline-engine-go/pkg/graph"
	"github.com/lisama569/build-pipeline-engine-go/pkg/history"
	"github.com/lisama569/build-pipeline-engine-go/pkg/report"
	"github.com/lisama569/build-pipeline-engine-go/pkg/retry"
	"github.com/lisama569/build-pipeline-engine-go/pkg/scheduler"
	"github.com/lisama569/build-pipeline-engine-go/pkg/state"
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
// the identical conclusion. Per-pipeline state/history locks additionally
// serialize read-modify-write of resume snapshots and history records.
type Engine struct {
	cfg     pipeline.Config
	runner  *exec.Runner
	store   *cache.Store
	deduper *cache.InFlight
	pol     retry.Policy
	envHash string
	state   *state.Store
	history *history.Store
}

// NewEngine validates cfg (clamping to documented hard bounds), opens the
// cache, state and history stores and prepares the shared runner/deduper.
func NewEngine(cfg pipeline.Config) (*Engine, error) {
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}
	store, err := cache.Open(cfg.CacheDir)
	if err != nil {
		return nil, err
	}
	st, err := state.Open(cfg.StateDir)
	if err != nil {
		return nil, err
	}
	hist, err := history.Open(cfg.HistoryDir, cfg.HistoryRuns)
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
		state:   st,
		history: hist,
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
	if cfg.RunBudget < 0 {
		return pipeline.NewError(pipeline.ErrConfigInvalid, "run budget must be >= 0")
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
// change in the ambient environment changes every cache/basis key.
func environmentFingerprint() string {
	env := os.Environ()
	sorted := append([]string(nil), env...)
	sort.Strings(sorted)
	h := sha256.New()
	h.Write([]byte(strings.Join(sorted, "\n")))
	return hex.EncodeToString(h.Sum(nil))
}

// newRunID returns a short, collision-resistant run identifier. It is
// deliberately excluded from the conclusion hash (see report
// non_determinism).
func newRunID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failing is extraordinary; fall back to time so an id
		// still exists, but this path is not expected on supported platforms.
		return "r" + strconvI(time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

func strconvI(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// Run validates and executes p to completion. When state/history are
// configured, it resumes from the last successful run and retains a bounded
// record; otherwise it behaves as a single cache-backed run.
func (e *Engine) Run(ctx context.Context, p *pipeline.Pipeline) (*report.Report, error) {
	if p == nil {
		return nil, pipeline.NewError(pipeline.ErrConfigInvalid, "nil pipeline")
	}
	g, err := graph.New(p.Tasks)
	if err != nil {
		return nil, err
	}
	return e.runGraph(ctx, p.Name, g)
}

// pipelineLocks acquires the per-pipeline state and/or history locks for the
// whole run. When both stores share a root directory one lock suffices
// (flock is per-file and would otherwise dead-lock the same process).
type pipelineLocks struct {
	locks []releaser
}

type releaser interface{ Release() error }

func (l *pipelineLocks) release() {
	for i := len(l.locks) - 1; i >= 0; i-- {
		_ = l.locks[i].Release()
	}
}

func (e *Engine) acquireLocks(name string) (*pipelineLocks, error) {
	var lk pipelineLocks
	stateDir := e.state.Dir()
	histDir := e.history.Dir()
	same := stateDir != "" && stateDir == histDir
	if e.state.Enabled() {
		l, err := e.state.Lock(name)
		if err != nil {
			return nil, err
		}
		if l != nil {
			lk.locks = append(lk.locks, l)
		}
	}
	if e.history.Enabled() && !same {
		l, err := e.history.Lock(name)
		if err != nil {
			lk.release()
			return nil, err
		}
		if l != nil {
			lk.locks = append(lk.locks, l)
		}
	}
	return &lk, nil
}

func (e *Engine) runGraph(ctx context.Context, name string, g *graph.Graph) (*report.Report, error) {
	lk, err := e.acquireLocks(name)
	if err != nil {
		return nil, err
	}
	defer lk.release()

	runID := newRunID()
	started := time.Now()

	// 1) load the last snapshot (if any) and plan the resume.
	var snap *state.Snapshot
	var plan *state.Plan
	if e.state.Enabled() {
		snap, err = e.state.Load(name)
		if err != nil {
			return nil, err
		}
		if snap != nil {
			plan, _ = state.PlanResume(g, snap, e.envHash)
		}
	}

	// 2) schedule.
	sched := scheduler.New(g, scheduler.Options{
		Config:    e.cfg,
		Runner:    e.runner,
		Store:     e.store,
		Deduper:   e.deduper,
		Policy:    e.pol,
		BaseEnv:   e.envHash,
		Plan:      plan,
		StartedAt: started,
	})
	rr := sched.Run(ctx)

	// 3) persist this run's snapshot (even on failure/timeout/budget: the
	// successful subset is reusable next time; non-successes are not).
	var resumedFrom string
	if snap != nil {
		resumedFrom = snap.RunID
	}
	if e.state.Enabled() {
		newSnap := buildSnapshot(name, runID, started, g, rr, e.envHash)
		if werr := e.state.Save(name, newSnap); werr != nil {
			// A state write failure must not hide the actual run conclusion.
			_ = werr
		}
	}

	rep := report.BuildWithMeta(name, rr.Results, sched.Events(), report.Meta{
		RunID:         runID,
		StartedAt:     started,
		EndReason:     rr.EndReason,
		ResumeUsed:    plan != nil,
		ResumedFromID: resumedFrom,
	})

	// 4) append history (with the computed conclusion) and compare against
	// the previous retained record.
	if e.history.Enabled() {
		rec := buildRecord(runID, name, started, g, rr, rep)
		prev, _ := e.history.Append(rec)
		rep.Diff = history.Compare(prev, rec)
	}
	return rep, nil
}

// RunGraph runs a pre-validated graph without resume/history wiring (kept for
// API compatibility and for tests that drive a single cache-backed run).
func (e *Engine) RunGraph(ctx context.Context, name string, g *graph.Graph) (*report.Report, error) {
	started := time.Now()
	sched := scheduler.New(g, scheduler.Options{
		Config:    e.cfg,
		Runner:    e.runner,
		Store:     e.store,
		Deduper:   e.deduper,
		Policy:    e.pol,
		BaseEnv:   e.envHash,
		StartedAt: started,
	})
	rr := sched.Run(ctx)
	return report.BuildWithMeta(name, rr.Results, sched.Events(), report.Meta{
		StartedAt: started, EndReason: rr.EndReason,
	}), nil
}

// buildSnapshot turns run results into a reusable snapshot. The Reused flag
// is normalized away on persistence: a stored result is a plain conclusion.
func buildSnapshot(name, runID string, started time.Time,
	g *graph.Graph, rr scheduler.RunResult, baseEnv string) *state.Snapshot {
	snap := &state.Snapshot{
		Version: 1, RunID: runID, Pipeline: name, StartedAt: started,
		Tasks: make(map[string]state.TaskSnapshot, len(rr.Results)),
	}
	for _, n := range g.Nodes() {
		r, ok := rr.Results[n.Task.ID]
		if !ok {
			continue
		}
		r.Reused = false
		r.ReuseInvalidation = ""
		snap.Tasks[n.Task.ID] = state.TaskSnapshot{
			Result:      r,
			BasisHash:   rr.Bases[n.Task.ID],
			Fingerprint: state.FingerprintKey(n.Task, baseEnv),
		}
	}
	return snap
}

// buildRecord assembles a history record from the run; status/conclusion
// mirror the built report.
func buildRecord(runID, name string, started time.Time,
	g *graph.Graph, rr scheduler.RunResult, rep *report.Report) *history.Record {
	tasks := make([]history.TaskRecord, 0, len(g.Nodes()))
	for _, n := range g.Nodes() {
		r, ok := rr.Results[n.Task.ID]
		if !ok {
			continue
		}
		tasks = append(tasks, history.TaskRecord{
			ID:                n.Task.ID,
			Status:            string(r.Status),
			Reused:            r.Reused,
			Cached:            r.Cached,
			Reason:            r.Reason,
			OutputDigest:      r.Output.Digest,
			ReuseInvalidation: r.ReuseInvalidation,
		})
	}
	return &history.Record{
		ID: runID, Pipeline: name, StartedAt: started,
		Conclusion: rep.Conclusion, Status: rep.Status, Tasks: tasks,
	}
}

// Store exposes the cache store (used by tests for poisoning entries).
func (e *Engine) Store() *cache.Store { return e.store }

// StateDir exposes the configured state root.
func (e *Engine) StateDir() string {
	if e.cfg.StateDir == "" {
		return ""
	}
	return filepath.Clean(e.cfg.StateDir)
}

// HistoryDir exposes the configured history root.
func (e *Engine) HistoryDir() string {
	if e.cfg.HistoryDir == "" {
		return ""
	}
	return filepath.Clean(e.cfg.HistoryDir)
}

// Close releases engine resources.
func (e *Engine) Close() error { return nil }
