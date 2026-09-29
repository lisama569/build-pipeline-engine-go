// Package scheduler implements bounded-parallel, deterministic execution of a
// validated task graph.
//
// Scheduling rules:
//   - At most Config.MaxParallelism tasks run at once. Surplus ready tasks
//     wait in a bounded in-memory queue (never one goroutine per task).
//   - Queue order is deterministic: FIFO by declaration index, or
//     "priority" = more transitive downstream dependents first, then index.
//   - When a task reaches a non-success state, every node it (transitively)
//     unblocks is marked SKIPPED with reason "upstream:<dep>", never run and
//     never reported as success.
//   - Cancellation propagates via per-attempt contexts with each task's
//     timeout; after the run ctx ends, shutdown is bounded by
//     Config.ShutdownGrace, so no worker or process is left dangling.
//   - Cache hits produce no side effects: the task body is never invoked.
//     Identical work across pipelines is executed once via the shared
//     InFlight deduper, and all observers get the same conclusion.
//   - Resume: a resume plan may mark tasks "reused" (previous success with an
//     unchanged per-task input basis). Reused tasks are pre-concluded before
//     the worker pool starts; they never execute, independently of whether
//     the task has caching enabled.
//   - Run budget: Config.RunBudget, once used up, stops all new dispatches,
//     cancels in-flight work (bounded convergence), and concludes the run
//     with the distinguishable status budget_exhausted - never confused with
//     a per-task timeout (timed_out) or a manual cancel (canceled).
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lisama569/build-pipeline-engine-go/pipeline"
	"github.com/lisama569/build-pipeline-engine-go/pkg/cache"
	"github.com/lisama569/build-pipeline-engine-go/pkg/exec"
	"github.com/lisama569/build-pipeline-engine-go/pkg/graph"
	"github.com/lisama569/build-pipeline-engine-go/pkg/retry"
	"github.com/lisama569/build-pipeline-engine-go/pkg/state"
)

// Event kinds emitted to the deterministic trace.
const (
	EvValidated       = "validated"
	EvReady           = "ready"
	EvQueue           = "queued"
	EvDispatch        = "dispatch"
	EvCacheCheck      = "cache_check"
	EvCacheHit        = "cache_hit"
	EvCacheMiss       = "cache_miss"
	EvAttempt         = "attempt"
	EvRetryWait       = "retry_wait"
	EvFinish          = "finish"
	EvSkip            = "skip"
	EvCancel          = "cancel"
	EvTimeout         = "timeout"
	EvReuse           = "reused"
	EvBudgetExhausted = "budget_exhausted"
)

// Event is one deterministic scheduling decision.
type Event struct {
	Seq    int    `json:"seq"`
	TaskID string `json:"task"`
	Kind   string `json:"kind"`
	Detail string `json:"detail"`
}

// abortCause classifies why the run stopped launching new work.
type abortCause int

const (
	abortNone     abortCause = iota
	abortCanceled            // parent context canceled / fail-fast
	abortBudget              // run-wide wall-clock budget used up
)

// Options wires a single scheduler run.
type Options struct {
	Config    pipeline.Config
	Runner    *exec.Runner
	Store     *cache.Store
	Deduper   *cache.InFlight
	Policy    retry.Policy
	BaseEnv   string
	Plan      *state.Plan // optional resume plan; nil => clean run
	StartedAt time.Time   // run start (budget anchor); zero means now
}

// RunResult is the scheduler's final word.
type RunResult struct {
	Results   map[string]pipeline.TaskResult
	Bases     map[string]string // task id -> current basis hash
	EndReason string            // pipeline.EndReason*
}

// Scheduler drives one run of a validated graph.
type Scheduler struct {
	g       *graph.Graph
	cfg     pipeline.Config
	runner  *exec.Runner
	store   *cache.Store
	deduper *cache.InFlight
	pol     retry.Policy
	baseEnv string
	plan    *state.Plan
	start   time.Time

	mu      sync.Mutex
	events  []Event
	seq     int
	results map[string]pipeline.TaskResult
	abort   abortCause
}

// New creates a scheduler for one run.
func New(g *graph.Graph, opts Options) *Scheduler {
	start := opts.StartedAt
	if start.IsZero() {
		start = time.Now()
	}
	return &Scheduler{
		g: g, cfg: opts.Config, runner: opts.Runner, store: opts.Store,
		deduper: opts.Deduper, pol: opts.Policy, baseEnv: opts.BaseEnv,
		plan: opts.Plan, start: start,
		results: make(map[string]pipeline.TaskResult, len(g.Nodes())),
	}
}

// Events returns the ordered trace of scheduling decisions.
func (s *Scheduler) Events() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Event, len(s.events))
	copy(out, s.events)
	return out
}

func (s *Scheduler) emit(task, kind, format string, args ...any) {
	s.mu.Lock()
	s.seq++
	s.events = append(s.events, Event{
		Seq: s.seq, TaskID: task, Kind: kind, Detail: fmt.Sprintf(format, args...),
	})
	s.mu.Unlock()
}

func (s *Scheduler) record(id string, r pipeline.TaskResult) {
	s.mu.Lock()
	s.results[id] = r
	s.mu.Unlock()
}

func (s *Scheduler) setAbort(c abortCause) {
	s.mu.Lock()
	if s.abort == abortNone {
		s.abort = c
	}
	s.mu.Unlock()
}

func (s *Scheduler) abortNow() abortCause {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.abort
}

// completion is a worker's final word on a task.
type completion struct {
	node   *graph.Node
	result pipeline.TaskResult
}

// Run executes the graph until success, failure propagation, cancellation or
// run-budget exhaustion.
func (s *Scheduler) Run(ctx context.Context) RunResult {
	par := s.cfg.MaxParallelism
	if par < 1 {
		par = 1
	}
	s.emit(s.g.Nodes()[0].Task.ID, EvValidated,
		"graph accepted: nodes=%d parallelism=%d policy=%s",
		len(s.g.Nodes()), par, s.cfg.QueuePolicy)

	track := graph.NewReadyState(s.g)
	results := make(map[string]pipeline.TaskResult, len(s.g.Nodes()))
	bases := make(map[string]string, len(s.g.Nodes()))

	// runCtx is canceled on fail-fast, manual cancellation or budget use;
	// every attempt is derived from it, so the stop signal reaches work.
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	// 0) Resume: pre-conclude every task the plan marks reusable, in
	// topological order. They never reach a worker, cost no attempt, and are
	// indistinguishable (for conclusion purposes) from a fresh success.
	reused := 0
	if s.plan != nil {
		for _, n := range s.g.Topo() {
			e, ok := s.plan.Entries[n.Task.ID]
			if !ok || !e.Reuse {
				continue
			}
			r := e.Snapshot.Result
			r.TaskID = n.Task.ID
			r.Reused = true
			r.ReuseInvalidation = ""
			r.BasisHash = e.BasisHash
			results[n.Task.ID] = r
			s.record(n.Task.ID, r)
			bases[n.Task.ID] = e.BasisHash
			track.Mark(n.Task.ID, r.Status)
			reused++
			s.emit(n.Task.ID, EvReuse,
				"carried over from previous run; basis=%s body not executed", e.BasisHash[:12])
		}
		if reused > 0 {
			s.emit("-", EvValidated, "resume: %d/%d tasks reused, %d to execute",
				reused, len(s.g.Nodes()), len(s.g.Nodes())-reused)
		}
	}

	// Bounded worker pool: exactly par long-lived goroutines. The work
	// channel is buffered with capacity par: a dispatch (which only happens
	// while inflight < par) therefore never blocks the scheduling loop,
	// while at most par tasks can ever be active. No per-task goroutines.
	work := make(chan *graph.Node, par)
	done := make(chan completion, par)
	var wg sync.WaitGroup
	for i := 0; i < par; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range work {
				done <- s.execute(runCtx, n)
			}
		}()
	}

	var queued []*graph.Node
	queuedSet := make(map[string]bool, len(s.g.Nodes()))
	inflight := 0
	finished := reused
	total := len(s.g.Nodes())
	launchStopped := reused == total
	cause := abortNone

	enqueueReady := func() {
		for _, n := range track.Ready(s.g) {
			if queuedSet[n.Task.ID] {
				continue
			}
			queuedSet[n.Task.ID] = true
			s.emit(n.Task.ID, EvReady, "all deps succeeded; eligible")
			s.emit(n.Task.ID, EvQueue, "%s", queueReason(n, len(queued), s.cfg.QueuePolicy))
			queued = append(queued, n)
		}
	}
	enqueueReady()

	var budgetCh <-chan time.Time
	if s.cfg.RunBudget > 0 {
		budgetCh = time.After(s.cfg.RunBudget)
	}
	var graceCh <-chan time.Time

	finalizeUnlaunched := func(detail string, st pipeline.TaskStatus, reason string) {
		for _, n := range s.g.Nodes() {
			if _, ok := results[n.Task.ID]; ok {
				continue
			}
			if cur, seen := track.Status(n.Task.ID); seen && pipeline.IsTerminal(cur) {
				continue
			}
			s.emit(n.Task.ID, EvCancel, "%s", detail)
			res := pipeline.TaskResult{
				TaskID: n.Task.ID, Status: st, Reason: reason, Detail: detail,
				StartedAt: time.Now(), EndedAt: time.Now(),
			}
			results[n.Task.ID] = res
			s.record(n.Task.ID, res)
			track.Mark(n.Task.ID, st)
			finished++
		}
		queued = nil
	}

	endReason := pipeline.EndReasonCompleted
	for finished < total {
		// Propagate failure: every transitively blocked node -> skipped.
		for _, n := range track.BlockedByFailed(s.g) {
			dep := track.FailedDep(n.Task.ID)
			s.emit(n.Task.ID, EvSkip, "upstream failure: dependency %q non-success", dep)
			res := pipeline.TaskResult{
				TaskID:    n.Task.ID,
				Status:    pipeline.StatusSkipped,
				Reason:    "upstream:" + dep,
				Detail:    "skipped because dependency " + dep + " did not succeed",
				EndedAt:   time.Now(),
				StartedAt: time.Now(),
			}
			bases[n.Task.ID] = s.computeBasis(n, results, bases)
			res.BasisHash = bases[n.Task.ID]
			results[n.Task.ID] = res
			s.record(n.Task.ID, res)
			track.Mark(n.Task.ID, pipeline.StatusSkipped)
			delete(queuedSet, n.Task.ID)
			finished++
		}

		// Stop signal has fully drained: conclude immediately.
		if launchStopped && inflight == 0 {
			switch cause {
			case abortBudget:
				finalizeUnlaunched("never launched after run budget exhausted",
					pipeline.StatusBudgetExhausted, string(pipeline.ErrRunBudget))
				endReason = pipeline.EndReasonBudgetExhausted
			default:
				finalizeUnlaunched("never launched after run was canceled",
					pipeline.StatusCanceled, string(pipeline.ErrTaskCanceled))
				endReason = pipeline.EndReasonCanceled
			}
			break
		}

		var dispatchCh chan *graph.Node
		var next *graph.Node
		if !launchStopped && inflight < par && len(queued) > 0 {
			next = pickNext(queued, s.cfg.QueuePolicy)
			dispatchCh = work
		}
		// Skip propagation above (or a completion) may have concluded the
		// whole graph; exit before selecting on channels that will never
		// fire again.
		if finished >= total {
			break
		}

		select {
		case dispatchCh <- next:
			queued = removeChosen(queued, next)
			// Keep queuedSet marked until the task reaches a terminal state:
			// Ready() still considers an in-flight task eligible, so deleting
			// here would re-queue and double-execute it.
			inflight++
			s.emit(next.Task.ID, EvDispatch, "slot available; inflight=%d/%d", inflight, par)
			enqueueReady()

		case c := <-done:
			inflight--
			finished++
			res := c.result

			// Budget stopped the run while this task was in flight: an
			// observed canceled/deadline end is attributed to the budget, so
			// it is never misreported as a manual cancel or task timeout.
			if cause == abortBudget &&
				(res.Status == pipeline.StatusCanceled || res.Status == pipeline.StatusTimedOut) {
				s.emit(c.node.Task.ID, EvBudgetExhausted,
					"in flight when the run budget was used up; bounded by grace=%s", s.cfg.ShutdownGrace)
				res.Status = pipeline.StatusBudgetExhausted
				res.Reason = string(pipeline.ErrRunBudget)
				res.Detail = "run budget exhausted while task was in flight"
			}

			bases[c.node.Task.ID] = s.computeBasis(c.node, results, bases)
			res.BasisHash = bases[c.node.Task.ID]
			if s.plan != nil {
				if e, ok := s.plan.Entries[c.node.Task.ID]; ok && !e.Reuse {
					res.ReuseInvalidation = e.Reason
				}
			}
			results[c.node.Task.ID] = res
			s.record(c.node.Task.ID, res)
			track.Mark(c.node.Task.ID, res.Status)
			if s.cfg.FailFast && !pipeline.IsSuccess(res.Status) {
				launchStopped = true
				cause = abortCanceled
				s.setAbort(abortCanceled)
				s.emit(c.node.Task.ID, EvCancel, "fail-fast: no new tasks; canceling running work")
				cancelRun()
			}
			enqueueReady()

		case <-budgetCh:
			launchStopped = true
			cause = abortBudget
			s.setAbort(abortBudget)
			endReason = pipeline.EndReasonBudgetExhausted
			s.emit("-", EvBudgetExhausted,
				"run budget %s used up; no new tasks; canceling in-flight work", s.cfg.RunBudget)
			cancelRun()
			if s.cfg.ShutdownGrace > 0 {
				graceCh = time.After(s.cfg.ShutdownGrace)
			}

		case <-runCtx.Done():
			if !launchStopped {
				launchStopped = true
				cause = abortCanceled
				s.setAbort(abortCanceled)
				endReason = pipeline.EndReasonCanceled
				s.emit("-", EvCancel, "run context ended; drain bounded by grace=%s", s.cfg.ShutdownGrace)
				if s.cfg.ShutdownGrace > 0 {
					graceCh = time.After(s.cfg.ShutdownGrace)
				}
			}

		case <-graceCh:
			s.emit("-", EvCancel, "shutdown grace exceeded; force-converging")
			if cause == abortBudget {
				finalizeUnlaunched("run budget exhausted; shutdown grace exceeded",
					pipeline.StatusBudgetExhausted, string(pipeline.ErrRunBudget))
				endReason = pipeline.EndReasonBudgetExhausted
			} else {
				finalizeUnlaunched("canceled: shutdown grace exceeded",
					pipeline.StatusCanceled, string(pipeline.ErrTaskCanceled))
				endReason = pipeline.EndReasonCanceled
			}
			close(work)
			wg.Wait()
			return RunResult{Results: results, Bases: bases, EndReason: endReason}
		}
	}

	close(work)
	wg.Wait()
	return RunResult{Results: results, Bases: bases, EndReason: endReason}
}

// computeBasis folds the recorded conclusions of the direct dependencies
// (their bases and output digests) into the task's current basis.
func (s *Scheduler) computeBasis(n *graph.Node,
	results map[string]pipeline.TaskResult, bases map[string]string) string {
	deps := make([]state.DepInput, 0, len(n.Deps))
	for _, d := range n.Deps {
		deps = append(deps, state.DepInput{
			ID:           d.Task.ID,
			BasisHash:    bases[d.Task.ID],
			OutputDigest: results[d.Task.ID].Output.Digest,
		})
	}
	return state.BasisHash(n.Task, deps, s.baseEnv)
}

// execute runs one task through the cross-pipeline deduper.
func (s *Scheduler) execute(runCtx context.Context, n *graph.Node) completion {
	task := n.Task
	fp := cache.FingerprintFromTask(task, s.baseEnv)
	key := s.store.Key(fp)

	mk := func(status pipeline.TaskStatus, attempts int, reason, detail string,
		out pipeline.ExecOutput, cached bool, start, end time.Time) pipeline.TaskResult {
		return pipeline.TaskResult{
			TaskID: task.ID, Status: status, Attempts: attempts,
			Reason: reason, Detail: detail, Output: out, Cached: cached,
			StartedAt: start, EndedAt: end,
		}
	}

	val, err := s.deduper.Do(runCtx, key, func(leaderCtx context.Context) (any, error) {
		return s.executeOnce(leaderCtx, n, key, mk)
	})
	if err != nil {
		st, code := pipeline.StatusCanceled, pipeline.ErrTaskCanceled
		if errors.Is(err, context.DeadlineExceeded) {
			st, code = pipeline.StatusTimedOut, pipeline.ErrTaskTimeout
		}
		return completion{node: n, result: mk(st, 0, string(code), err.Error(),
			pipeline.ExecOutput{}, false, time.Now(), time.Now())}
	}
	return completion{node: n, result: val.(pipeline.TaskResult)}
}

// executeOnce performs cache lookup and the bounded attempt loop. It runs at
// most once per cache key per run (guarded by InFlight).
func (s *Scheduler) executeOnce(ctx context.Context, n *graph.Node, key string,
	mk func(pipeline.TaskStatus, int, string, string, pipeline.ExecOutput, bool, time.Time, time.Time) pipeline.TaskResult,
) (pipeline.TaskResult, error) {
	task := n.Task
	start := time.Now()
	end := func() time.Time { return time.Now() }

	// 1) cache lookup; a hit never invokes the task body (no side effects).
	if s.store.Enabled() && !task.DisableCache {
		s.emit(task.ID, EvCacheCheck, "lookup key=%s", key[:12])
		if e, err := s.store.Get(ctx, key, ""); err == nil {
			s.emit(task.ID, EvCacheHit, "restored; task body not executed")
			return mk(pipeline.StatusCacheHit, 0, "cache_hit",
				"restored from cache "+key[:12], e.Output, true, start, end()), nil
		} else if pe := pipeline.AsError(err); pe != nil {
			s.emit(task.ID, EvCacheMiss, "cache rejected (%s): %s; recomputing", pe.Code, pe.Detail)
		} else {
			s.emit(task.ID, EvCacheMiss, "no entry; computing")
		}
	}

	timeout := task.Timeout
	if timeout <= 0 {
		timeout = s.cfg.DefaultTaskTimeout
	}
	maxAttempts := task.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = s.pol.MaxAttempts
	}
	if maxAttempts > s.pol.HardAttemptCap {
		maxAttempts = s.pol.HardAttemptCap
	}
	pol := retry.Policy{
		MaxAttempts:    maxAttempts,
		Base:           s.pol.Base,
		Cap:            s.pol.Cap,
		HardAttemptCap: s.pol.HardAttemptCap,
	}

	var lastOut pipeline.ExecOutput
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		s.emit(task.ID, EvAttempt, "attempt %d/%d timeout=%s idempotent=%t",
			attempt, maxAttempts, timeout, task.Idempotent)

		attemptCtx := ctx
		var cancel context.CancelFunc
		if timeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, timeout)
		}
		s.mu.Lock()
		deps := make(map[string]pipeline.TaskResult, len(n.Deps))
		for _, d := range n.Deps {
			deps[d.Task.ID] = s.results[d.Task.ID]
		}
		s.mu.Unlock()

		out, runErr := s.runner.Run(attemptCtx, task, pipeline.ExecInput{
			TaskID: task.ID, Attempt: attempt, DepResults: deps,
		})
		if cancel != nil {
			cancel()
		}
		lastOut = out

		if runErr == nil {
			if s.store.Enabled() && !task.DisableCache {
				if werr := s.store.Put(ctx, cache.Entry{Key: key, Digest: out.Digest, Output: out}); werr != nil {
					s.emit(task.ID, EvFinish, "cache write failed (non-fatal): %v", werr)
				}
			}
			s.emit(task.ID, EvFinish, "status=succeeded attempts=%d", attempt)
			return mk(pipeline.StatusSucceeded, attempt, "ok", "succeeded", out, false, start, end()), nil
		}

		// Timeout / cancellation are terminal, distinct, never retried.
		if pe := pipeline.AsError(runErr); pe != nil {
			if pe.Code == pipeline.ErrTaskTimeout {
				s.emit(task.ID, EvTimeout, "attempt %d exceeded %s; timed_out, not retried", attempt, timeout)
				return mk(pipeline.StatusTimedOut, attempt, string(pipeline.ErrTaskTimeout),
					pe.Detail, lastOut, false, start, end()), nil
			}
			if pe.Code == pipeline.ErrTaskCanceled {
				s.emit(task.ID, EvCancel, "attempt %d canceled", attempt)
				return mk(pipeline.StatusCanceled, attempt, string(pipeline.ErrTaskCanceled),
					pe.Detail, lastOut, false, start, end()), nil
			}
		}

		dec := retry.Classify(runErr, attempt, pol, task.Idempotent)
		if !dec.Retry {
			reason := string(pipeline.ErrTaskFailed)
			detail := runErr.Error()
			if dec.Reason == pipeline.ErrRetryUnsafe {
				reason = string(pipeline.ErrRetryUnsafe)
				detail = "transient failure on non-idempotent task; not retried: " + detail
				s.emit(task.ID, EvFinish, "retry refused: non-idempotent side effect")
			} else if attempt >= maxAttempts {
				s.emit(task.ID, EvFinish, "attempts exhausted at %d", attempt)
			}
			return mk(pipeline.StatusFailed, attempt, reason, detail, lastOut, false, start, end()), nil
		}

		s.emit(task.ID, EvRetryWait, "backoff %s before attempt %d", dec.Wait, attempt+1)
		if werr := retry.Sleep(ctx, dec.Wait); werr != nil {
			cst, code := pipeline.StatusCanceled, pipeline.ErrTaskCanceled
			if errors.Is(werr, context.DeadlineExceeded) {
				cst, code = pipeline.StatusTimedOut, pipeline.ErrTaskTimeout
			}
			return mk(cst, attempt, string(code), "stopped during backoff: "+werr.Error(),
				lastOut, false, start, end()), nil
		}
	}
	return mk(pipeline.StatusFailed, maxAttempts, string(pipeline.ErrTaskFailed),
		"retry budget exhausted", lastOut, false, start, end()), nil
}

// pickNext selects and removes the next ready node deterministically.
func pickNext(queued []*graph.Node, policy pipeline.QueuePolicy) *graph.Node {
	idx := 0
	if policy == pipeline.QueuePriority {
		for i := 1; i < len(queued); i++ {
			if queued[i].RemainingDependents > queued[idx].RemainingDependents ||
				(queued[i].RemainingDependents == queued[idx].RemainingDependents &&
					queued[i].Index < queued[idx].Index) {
				idx = i
			}
		}
	}
	return queued[idx]
}

func removeChosen(queued []*graph.Node, chosen *graph.Node) []*graph.Node {
	for i, n := range queued {
		if n == chosen {
			return append(queued[:i], queued[i+1:]...)
		}
	}
	return queued
}

func queueReason(n *graph.Node, depth int, pol pipeline.QueuePolicy) string {
	if pol == pipeline.QueuePriority {
		return fmt.Sprintf("queued depth=%d downstream=%d", depth, n.RemainingDependents)
	}
	return fmt.Sprintf("queued depth=%d (fifo index=%d)", depth, n.Index)
}
