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
//   - A run-wide Config.RunBudget is a third, distinct stop source: once it
//     elapses no task is dispatched and running work is canceled, reported
//     as stop_reason=budget_exhausted (never confused with a task timeout or
//     a manual cancel).
//   - On a resuming run, reusable prior verdicts (provided via a resume.Plan)
//     are pre-seeded as terminal successes before the worker pool starts;
//     they are never dispatched and produce no side effects.
//   - Cache hits produce no side effects: the task body is never invoked.
//     Identical work across pipelines is executed once via the shared
//     InFlight deduper, and all observers get the same conclusion.
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
	"github.com/lisama569/build-pipeline-engine-go/pkg/resume"
	"github.com/lisama569/build-pipeline-engine-go/pkg/retry"
)

// Event kinds emitted to the deterministic trace.
const (
	EvValidated   = "validated"
	EvReady       = "ready"
	EvQueue       = "queued"
	EvDispatch    = "dispatch"
	EvCacheCheck  = "cache_check"
	EvCacheHit    = "cache_hit"
	EvCacheMiss   = "cache_miss"
	EvAttempt     = "attempt"
	EvRetryWait   = "retry_wait"
	EvFinish      = "finish"
	EvSkip        = "skip"
	EvCancel      = "cancel"
	EvTimeout     = "timeout"
	EvResumeReuse = "resume_reuse"
	EvResumeRerun = "resume_rerun"
	EvBudget      = "budget"
	EvStop        = "stop"
)

// Event is one deterministic scheduling decision.
type Event struct {
	Seq    int    `json:"seq"`
	TaskID string `json:"task"`
	Kind   string `json:"kind"`
	Detail string `json:"detail"`
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
	plan    *resume.Plan

	mu         sync.Mutex
	events     []Event
	seq        int
	results    map[string]pipeline.TaskResult
	stopReason pipeline.StopReason
}

// New creates a scheduler for one run.
func New(g *graph.Graph, cfg pipeline.Config, runner *exec.Runner,
	store *cache.Store, deduper *cache.InFlight, pol retry.Policy, baseEnv string) *Scheduler {
	return NewWithPlan(g, cfg, runner, store, deduper, pol, baseEnv, nil)
}

// NewWithPlan creates a scheduler for a resuming run: reusable verdicts from
// plan are pre-seeded as terminal successes and never dispatched.
func NewWithPlan(g *graph.Graph, cfg pipeline.Config, runner *exec.Runner,
	store *cache.Store, deduper *cache.InFlight, pol retry.Policy, baseEnv string,
	plan *resume.Plan) *Scheduler {
	return &Scheduler{
		g: g, cfg: cfg, runner: runner, store: store,
		deduper: deduper, pol: pol, baseEnv: baseEnv, plan: plan,
		results: make(map[string]pipeline.TaskResult, len(g.Nodes())),
	}
}

// StopReason reports why the run ended (valid after Run returns).
func (s *Scheduler) StopReason() pipeline.StopReason {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopReason == "" {
		return pipeline.StopCompleted
	}
	return s.stopReason
}

func (s *Scheduler) setStopReason(r pipeline.StopReason) {
	s.mu.Lock()
	if s.stopReason == "" || s.stopReason == pipeline.StopCompleted {
		s.stopReason = r
	}
	s.mu.Unlock()
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

// completion is a worker's final word on a task.
type completion struct {
	node   *graph.Node
	result pipeline.TaskResult
}

// Run executes the graph until success, failure propagation or cancellation.
func (s *Scheduler) Run(ctx context.Context) map[string]pipeline.TaskResult {
	par := s.cfg.MaxParallelism
	if par < 1 {
		par = 1
	}
	s.emit(s.g.Nodes()[0].Task.ID, EvValidated,
		"graph accepted: nodes=%d parallelism=%d policy=%s",
		len(s.g.Nodes()), par, s.cfg.QueuePolicy)

	track := graph.NewReadyState(s.g)
	results := make(map[string]pipeline.TaskResult, len(s.g.Nodes()))

	// Resume: pre-seed reusable verdicts as terminal successes. They are
	// marked on the readiness tracker, so the normal readiness logic treats
	// them exactly like freshly completed tasks; they are never dispatched
	// and their bodies produce no side effects.
	if s.plan != nil {
		for _, n := range s.g.Nodes() {
			d, ok := s.plan.Decision(n.Task.ID)
			if !ok || !d.Reuse {
				continue
			}
			r := d.Snapshot.ToResult()
			now := time.Now()
			r.StartedAt, r.EndedAt = now, now
			results[n.Task.ID] = r
			s.record(n.Task.ID, r)
			track.Mark(n.Task.ID, r.Status)
			s.emit(n.Task.ID, EvResumeReuse,
				"verdict reused from previous run; status=%s digest=%s; body not executed",
				r.Status, r.Output.Digest)
		}
		if s.plan.ReusedCount() > 0 {
			s.emit("-", EvValidated,
				"resume plan: %d reused, %d to execute",
				s.plan.ReusedCount(), s.plan.ReranCount())
		}
	}

	// runCtx is canceled on budget exhaustion, fail-fast or parent
	// cancellation; every attempt is derived from it, so cancellation reaches
	// running tasks.
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	// The run-wide wall-clock budget, if configured. When it fires no new
	// task is dispatched and running work is canceled; this source is kept
	// distinct from the parent context (manual interrupt) and fail-fast.
	var budgetCh <-chan time.Time
	if s.cfg.RunBudget > 0 {
		budgetCh = time.After(s.cfg.RunBudget)
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
	finished := len(results) // pre-seeded reused verdicts already count
	total := len(s.g.Nodes())
	launchStopped := false

	enqueueReady := func() {
		for _, n := range track.Ready(s.g) {
			if queuedSet[n.Task.ID] {
				continue
			}
			queuedSet[n.Task.ID] = true
			s.emit(n.Task.ID, EvReady, "all deps succeeded; eligible")
			if d, ok := s.planDecision(n.Task.ID); ok && !d.Reuse {
				s.emit(n.Task.ID, EvResumeRerun, "re-executing: %s", d.RerunReason)
			}
			s.emit(n.Task.ID, EvQueue, "%s", queueReason(n, len(queued), s.cfg.QueuePolicy))
			queued = append(queued, n)
		}
	}
	enqueueReady()

	var graceCh <-chan time.Time

	// finalizeCanceled concludes every never-launched, still-pending node.
	// reasonCode/detail distinguish budget exhaustion, manual cancel,
	// fail-fast and grace backstop so the report names the actual cause.
	finalizeCanceled := func(reasonCode, detail string) {
		for _, n := range s.g.Nodes() {
			if _, ok := results[n.Task.ID]; ok {
				continue
			}
			if st, seen := track.Status(n.Task.ID); seen && pipeline.IsTerminal(st) {
				continue
			}
			s.emit(n.Task.ID, EvCancel, "%s", detail)
			res := pipeline.TaskResult{
				TaskID: n.Task.ID, Status: pipeline.StatusCanceled,
				Reason:    reasonCode,
				Detail:    detail,
				StartedAt: time.Now(), EndedAt: time.Now(),
			}
			results[n.Task.ID] = res
			s.record(n.Task.ID, res)
			track.Mark(n.Task.ID, pipeline.StatusCanceled)
			finished++
		}
		queued = nil
	}

	// stop records the distinguished cause once launching has stopped.
	beginShutdown := func(why pipeline.StopReason, evKind, format string, args ...any) {
		if launchStopped {
			return
		}
		launchStopped = true
		s.setStopReason(why)
		args = append(args, s.cfg.ShutdownGrace)
		s.emit("-", evKind, format+" ; drain bounded by grace=%s", args...)
		if s.cfg.ShutdownGrace > 0 {
			graceCh = time.After(s.cfg.ShutdownGrace)
		}
	}

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
			results[n.Task.ID] = res
			s.record(n.Task.ID, res)
			track.Mark(n.Task.ID, pipeline.StatusSkipped)
			delete(queuedSet, n.Task.ID)
			finished++
		}

		// Cancellation/fail-fast/budget has fully drained: conclude immediately.
		if launchStopped && inflight == 0 {
			finalizeCanceled(s.cancelReasonCode(), "never launched after "+s.stopDetail())
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
		case <-budgetCh:
			// Exhausting the budget both stops dispatch AND cancels running
			// work (a distinct cause from the parent context and fail-fast).
			cancelRun()
			beginShutdown(pipeline.StopBudgetExhausted, EvBudget,
				"run budget of %s exhausted; no new tasks dispatched, canceling running work",
				s.cfg.RunBudget)

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
			results[c.node.Task.ID] = c.result
			s.record(c.node.Task.ID, c.result)
			track.Mark(c.node.Task.ID, c.result.Status)
			if s.cfg.FailFast && !launchStopped && !pipeline.IsSuccess(c.result.Status) {
				cancelRun()
				beginShutdown(pipeline.StopFailFast, EvCancel,
					"fail-fast after task %q non-success; no new tasks, canceling running work",
					c.node.Task.ID)
			}
			enqueueReady()

		case <-runCtx.Done():
			// Could be parent cancellation (manual interrupt) or fail-fast
			// already having called cancelRun; budget fires its own channel.
			if !launchStopped {
				if ctx.Err() != nil {
					beginShutdown(pipeline.StopCanceled, EvCancel,
						"run context canceled by caller; canceling running work")
				} else {
					beginShutdown(pipeline.StopCanceled, EvCancel,
						"run context ended; canceling running work")
				}
			}

		case <-graceCh:
			// Defensive backstop: a task ignored cancellation past the grace
			// window. Conclude the run NOW (bounded convergence) rather than
			// waiting for the stuck body, but never leak its worker: close the
			// work channel so idle workers exit, and drain worker completions
			// in the background until every worker has returned. The stuck
			// attempt is already canceled (and commands are SIGKILLed by the
			// exec layer), so its worker finishes within the task's own bound.
			s.emit("-", EvCancel, "shutdown grace exceeded; force-converging")
			finalizeCanceled(s.cancelReasonCode(), "canceled: shutdown grace exceeded")
			close(work)
			go func() {
				defer close(done)
				wg.Wait()
			}()
			go func() {
				for range done {
				}
			}()
			s.emit("-", EvStop, "run ended: stop_reason=%s (grace exceeded)", s.StopReason())
			return results
		}
	}

	close(work)
	wg.Wait()
	close(done)
	s.emit("-", EvStop, "run ended: stop_reason=%s", s.StopReason())
	return results
}

// planDecision returns the resume decision for a task, if any.
func (s *Scheduler) planDecision(id string) (resume.Decision, bool) {
	if s.plan == nil {
		return resume.Decision{}, false
	}
	return s.plan.Decision(id)
}

// cancelReasonCode maps the active stop reason onto the reason code recorded
// on unlaunched/terminated tasks.
func (s *Scheduler) cancelReasonCode() string {
	switch s.StopReason() {
	case pipeline.StopBudgetExhausted:
		return string(pipeline.ErrRunBudget)
	case pipeline.StopFailFast:
		return string(pipeline.ErrTaskCanceled)
	default:
		return string(pipeline.ErrTaskCanceled)
	}
}

func (s *Scheduler) stopDetail() string {
	switch s.StopReason() {
	case pipeline.StopBudgetExhausted:
		return "run budget exhausted"
	case pipeline.StopFailFast:
		return "fail-fast"
	default:
		return "run was canceled"
	}
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
		res := mk(st, 0, string(code), err.Error(),
			pipeline.ExecOutput{}, false, time.Now(), time.Now())
		s.annotateRerun(task.ID, &res)
		return completion{node: n, result: res}
	}
	res := val.(pipeline.TaskResult)
	s.annotateRerun(task.ID, &res)
	return completion{node: n, result: res}
}

// annotateRerun records why a known task re-executed in a resuming run. New
// tasks keep an empty reason; when resume is inactive there is no decision.
func (s *Scheduler) annotateRerun(id string, r *pipeline.TaskResult) {
	if s.plan == nil {
		return
	}
	d, ok := s.plan.Decision(id)
	if !ok || d.Reuse {
		return
	}
	if d.RerunReason == resume.RerunNewTask {
		return
	}
	r.RerunReason = d.RerunReason
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

func queueReason(n *graph.Node, depth int, policy pipeline.QueuePolicy) string {
	if policy == pipeline.QueuePriority {
		return fmt.Sprintf("queued depth=%d downstream=%d", depth, n.RemainingDependents)
	}
	return fmt.Sprintf("queued depth=%d (fifo index=%d)", depth, n.Index)
}
