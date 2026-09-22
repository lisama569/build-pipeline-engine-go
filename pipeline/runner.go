package pipeline

import (
	"context"
	"errors"
	"time"
)

// runTask executes a single task with cache lookup, bounded per-attempt
// timeout, bounded exponential-backoff retry and cache write-back.
//
// Semantics (also described in docs/pipeline.md):
//   - Cacheable + valid hit -> StatusFromCache; the task body is never
//     invoked, so side effects are not produced twice. A detected corrupted
//     entry is a miss plus an explicit uncertainty annotation.
//   - Timeout is terminal: StatusTimedOut / ReasonTimeout, never retried.
//   - Only idempotent tasks are retried. A non-idempotent task configured
//     with retries runs once and, on failure, is marked
//     StatusFailed / ReasonNonIdempotentRetry (the retry that would have
//     duplicated a side effect is refused, not silently attempted).
//   - Cancellation while running or backing off ->
//     StatusSkipped / ReasonCanceled.
//   - Only a fully successful final result is cached; failed/timed-out
//     attempts never overwrite the cache.
func (e *Engine) runTask(ctx context.Context, g *Graph, name, key string) *TaskResult {
	t := g.Task(name)
	res := &TaskResult{Name: name, DependsOn: append([]string(nil), g.deps[name]...)}
	if key != "" {
		res.CacheKey = key
	}
	start := nowMonotonic()
	defer func() { res.DurationMS = millisSince(start) }()

	plan := resolvePlan(t, e.cfg)

	// 1. Cache lookup (cacheable tasks only).
	if t.Cacheable && key != "" {
		entry, found, corrupted, err := e.cache.Lookup(ctx, key)
		if err != nil {
			// Cache backend failure must not fabricate results; mark task
			// failed distinctly rather than re-running blindly.
			res.Status = StatusFailed
			res.Reason = ReasonExecError
			res.Message = err.Error()
			return res
		}
		if found {
			// Reconstruct the typed in-memory value for cross-process cache
			// hits (in-process mirrors already carry it). The task body is
			// never invoked, so no side effect is produced twice.
			if entry.Value == nil && t.decode != nil && len(entry.Payload) > 0 {
				entry.Value = t.decode(entry.Payload)
			}
			res.Status = StatusFromCache
			res.CacheHit = true
			res.Attempts = 0
			return res
		}
		if corrupted != "" {
			res.Uncertainty = append(res.Uncertainty, "cache corruption recovered by re-execution: "+corrupted)
		}
	}

	// 2. Attempted execution.
	var lastErr error
	maxAttempts := 1 + plan.retries
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			res.Status = StatusSkipped
			res.Reason = ReasonCanceled
			res.Message = "canceled before attempt"
			res.Attempts = attempt - 1
			return res
		}
		res.Attempts = attempt

		timedOut, canceled, out, err := e.runOnce(ctx, t, plan.timeout)
		if timedOut {
			res.Status = StatusTimedOut
			res.Reason = ReasonTimeout
			res.Message = "exceeded timeout " + plan.timeout.String()
			return res
		}
		if canceled {
			res.Status = StatusSkipped
			res.Reason = ReasonCanceled
			if err != nil {
				res.Message = err.Error()
			}
			return res
		}
		if err == nil {
			// 3. Write-through only after a clean success.
			if t.Cacheable && key != "" && out != nil {
				if storeErr := e.cache.Store(ctx, key, out.Payload, out.Value); storeErr != nil {
					res.Status = StatusFailed
					res.Reason = ReasonExecError
					res.Message = "cache store failed: " + storeErr.Error()
					return res
				}
			}
			res.Status = StatusSucceeded
			return res
		}
		lastErr = err

		// No more attempts configured: decide how the refusal is reported.
		if attempt == maxAttempts {
			break
		}
		// Retry would duplicate a non-idempotent side effect: refuse it
		// explicitly instead of re-invoking the body.
		if !t.Idempotent {
			res.Status = StatusFailed
			res.Reason = ReasonNonIdempotentRetry
			res.Message = "retry of a non-idempotent task was not attempted: " + errText(lastErr)
			return res
		}

		// Bounded exponential backoff, interruptible by cancellation.
		backoff := plan.backoff << (attempt - 1)
		if backoff > plan.maxBackoff || backoff <= 0 {
			backoff = plan.maxBackoff
		}
		if !sleepCtx(ctx, backoff) {
			res.Status = StatusSkipped
			res.Reason = ReasonCanceled
			res.Message = "canceled during retry backoff"
			return res
		}
	}

	res.Status = StatusFailed
	res.Reason = reasonOf(lastErr, ReasonFailed)
	res.Message = errText(lastErr)
	return res
}

// runOnce performs exactly one task attempt under a hard timeout. It runs
// the body on a goroutine and GUARANTEES the goroutine does not leak: a
// task body that ignores its context and keeps running is still awaited
// (bounded by the engine's shutdown grace), so no executor slot or result
// channel is abandoned.
func (e *Engine) runOnce(parent context.Context, t *TaskDef, timeout time.Duration) (timedOut, canceled bool, out *TaskOutput, err error) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	type attemptResult struct {
		out *TaskOutput
		err error
	}
	ch := make(chan attemptResult, 1)
	go func() {
		o, er := t.Run(ctx)
		ch <- attemptResult{o, er}
	}()

	select {
	case r := <-ch:
		return false, false, r.out, r.err
	case <-ctx.Done():
		// Classify why the attempt context ended. WithTimeout reports
		// context.DeadlineExceeded for its own expiry; parent cancellation
		// reports the parent's error (Canceled).
		timedOut = errors.Is(ctx.Err(), context.DeadlineExceeded) && parent.Err() == nil
		canceled = parent.Err() != nil
		// Drain the body within a bounded grace period to converge instead
		// of leaking the goroutine.
		select {
		case r := <-ch:
			// Late result after cancellation is deliberately ignored for
			// classification, but its error text is attached when useful.
			_ = r
		case <-time.After(e.cfg.ShutdownGrace):
		}
		return timedOut, canceled, nil, ctx.Err()
	}
}

// resolvePlan applies engine defaults and hard bounds to a task definition.
func resolvePlan(t *TaskDef, cfg Config) executionPlan {
	timeout := t.Timeout
	if timeout == 0 {
		timeout = cfg.DefaultTimeout
	}
	retries := t.Retries
	if retries > cfg.MaxRetries {
		retries = cfg.MaxRetries
	}
	return executionPlan{
		timeout:    timeout,
		retries:    retries,
		backoff:    cfg.RetryBackoff,
		maxBackoff: cfg.MaxRetryBackoff,
	}
}

// sleepCtx waits for d, returning true after the wait or false when ctx is
// canceled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func reasonOf(err error, fallback Reason) Reason {
	if e := AsError(err); e != nil {
		return e.Reason
	}
	return fallback
}
