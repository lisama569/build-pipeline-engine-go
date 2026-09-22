// Package retry provides bounded exponential backoff and error
// classification for task attempts.
//
// Rules:
//   - Attempt counts are hard-capped (DefaultHardAttemptCap); configuration
//     above the cap is clamped, not silently accepted.
//   - Timeouts and context cancellation are NEVER retried; they surface as
//     distinct statuses (timed_out / canceled).
//   - A transient error on a non-idempotent task is not retried; instead the
//     decision carries ErrRetryUnsafe so the report can distinguish it.
//   - Backoff between attempts is bounded exponential:
//     wait = min(Cap, Base * 2^(attempt-1)), interruptible by ctx.
package retry

import (
	"context"
	"errors"
	"time"

	"github.com/lisama569/build-pipeline-engine-go/pipeline"
)

// Policy is a bounded retry policy.
type Policy struct {
	// MaxAttempts includes the first attempt; clamped to [1, HardAttemptCap].
	MaxAttempts int
	// Base and Cap bound exponential backoff.
	Base time.Duration
	Cap  time.Duration
	// HardAttemptCap is the engine-wide upper bound for this policy.
	HardAttemptCap int
}

// Engine-wide hard bounds so configuration cannot request unbounded work.
const (
	DefaultHardAttemptCap = 10
	DefaultHardTimeout    = 30 * time.Minute
	DefaultBackoffBase    = 50 * time.Millisecond
	DefaultBackoffCap     = 2 * time.Second
)

// NewPolicy validates and clamps retry configuration into safe bounds.
func NewPolicy(maxAttempts int, base, cap time.Duration) (Policy, error) {
	if maxAttempts < 1 {
		return Policy{}, pipeline.NewError(pipeline.ErrConfigInvalid,
			"max attempts must be >= 1, got %d", maxAttempts)
	}
	if maxAttempts > DefaultHardAttemptCap {
		maxAttempts = DefaultHardAttemptCap
	}
	if base < 0 || cap < 0 {
		return Policy{}, pipeline.NewError(pipeline.ErrConfigInvalid,
			"backoff durations must be non-negative")
	}
	if base == 0 {
		base = DefaultBackoffBase
	}
	if cap == 0 {
		cap = DefaultBackoffCap
	}
	if cap < base {
		cap = base
	}
	return Policy{
		MaxAttempts:    maxAttempts,
		Base:           base,
		Cap:            cap,
		HardAttemptCap: DefaultHardAttemptCap,
	}, nil
}

// Backoff returns the wait before the (attempt+1)-th run. attempt is the
// 1-based number of the attempt that just finished.
func (p Policy) Backoff(attempt int) time.Duration {
	if attempt < 1 {
		return 0
	}
	d := p.Base
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= p.Cap {
			return p.Cap
		}
	}
	if d > p.Cap {
		return p.Cap
	}
	return d
}

// Decision says whether another attempt is allowed.
type Decision struct {
	Retry  bool
	Reason pipeline.ErrorCode
	Wait   time.Duration
}

// noRetry builds a terminal decision.
func noRetry(reason pipeline.ErrorCode) Decision {
	return Decision{Retry: false, Reason: reason}
}

// Classify decides whether err after attempt (1-based) permits another try.
//
//	err            | idempotent | result
//	timeout        | *          | no retry, ErrTaskTimeout
//	cancellation   | *          | no retry, ErrTaskCanceled
//	transient      | true       | retry with backoff (if attempts remain)
//	transient      | false      | no retry, ErrRetryUnsafe (error classified)
//	permanent      | *          | no retry, ErrTaskFailed
func Classify(err error, attempt int, p Policy, idempotent bool) Decision {
	if err == nil {
		return noRetry("")
	}

	var pe *pipeline.Error
	if errors.As(err, &pe) {
		switch pe.Code {
		case pipeline.ErrTaskTimeout:
			// Timeouts are terminal for this task: explicitly marked, never
			// fed back into the retry loop.
			return noRetry(pipeline.ErrTaskTimeout)
		case pipeline.ErrTaskCanceled:
			return noRetry(pipeline.ErrTaskCanceled)
		}
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return noRetry(pipeline.ErrTaskTimeout)
	}
	if errors.Is(err, context.Canceled) {
		return noRetry(pipeline.ErrTaskCanceled)
	}

	if !isTransient(err) {
		return noRetry(pipeline.ErrTaskFailed)
	}

	// Transient failure: retries are only safe for idempotent work.
	if !idempotent {
		return noRetry(pipeline.ErrRetryUnsafe)
	}
	if attempt >= p.MaxAttempts {
		return noRetry(pipeline.ErrTaskFailed)
	}
	return Decision{Retry: true, Reason: "retry_transient", Wait: p.Backoff(attempt)}
}

// Transient marks an error as retryable for idempotent tasks.
type Transient struct{ Err error }

func (t *Transient) Error() string { return "transient: " + t.Err.Error() }
func (t *Transient) Unwrap() error { return t.Err }

// MarkTransient wraps err as transient.
func MarkTransient(err error) error {
	if err == nil {
		return nil
	}
	return &Transient{Err: err}
}

func isTransient(err error) bool {
	var t *Transient
	return errors.As(err, &t)
}

// Sleep waits for d but returns as soon as ctx is done.
func Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
