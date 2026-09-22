package retry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lisama569/build-pipeline-engine-go/pipeline"
)

func TestPolicy_ClampsToHardCap(t *testing.T) {
	p, err := NewPolicy(1000, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if p.MaxAttempts != DefaultHardAttemptCap {
		t.Fatalf("expected clamp to %d, got %d", DefaultHardAttemptCap, p.MaxAttempts)
	}
	if p.Base != DefaultBackoffBase || p.Cap != DefaultBackoffCap {
		t.Fatalf("default backoff wrong: %v/%v", p.Base, p.Cap)
	}
	t.Logf("attempts clamped to hard cap %d; backoff base=%s cap=%s",
		p.MaxAttempts, p.Base, p.Cap)
}

func TestPolicy_RejectsZero(t *testing.T) {
	if _, err := NewPolicy(0, 0, 0); pipeline.AsError(err) == nil ||
		pipeline.AsError(err).Code != pipeline.ErrConfigInvalid {
		t.Fatalf("expected config error, got %v", err)
	}
}

func TestBackoff_BoundedExponential(t *testing.T) {
	p, _ := NewPolicy(5, 10*time.Millisecond, 40*time.Millisecond)
	cases := []struct {
		attempt int
		want    time.Duration
	}{{1, 10}, {2, 20}, {3, 40}, {4, 40}, {10, 40}}
	for _, c := range cases {
		if got := p.Backoff(c.attempt); got != c.want*time.Millisecond {
			t.Fatalf("attempt %d: want %s got %s", c.attempt, c.want*time.Millisecond, got)
		}
	}
	t.Logf("backoff bounded: 10ms,20ms,40ms(cap),40ms(cap) ...")
}

func TestClassify_TimeoutNeverRetried(t *testing.T) {
	p, _ := NewPolicy(5, time.Millisecond, time.Second)
	timeout := pipeline.NewError(pipeline.ErrTaskTimeout, "boom")
	d := Classify(timeout, 1, p, true)
	if d.Retry || d.Reason != pipeline.ErrTaskTimeout {
		t.Fatalf("timeout must be terminal, got %+v", d)
	}
	t.Logf("timeout classified as %s and never retried (idempotent=true)", d.Reason)
}

func TestClassify_CanceledNeverRetried(t *testing.T) {
	p, _ := NewPolicy(5, time.Millisecond, time.Second)
	d := Classify(context.Canceled, 1, p, true)
	if d.Retry || d.Reason != pipeline.ErrTaskCanceled {
		t.Fatalf("cancel must be terminal, got %+v", d)
	}
	t.Logf("cancellation classified as %s", d.Reason)
}

func TestClassify_TransientIdempotentRetries(t *testing.T) {
	p, _ := NewPolicy(3, time.Millisecond, time.Second)
	err := MarkTransient(errors.New("flake"))
	d := Classify(err, 1, p, true)
	if !d.Retry || d.Wait <= 0 {
		t.Fatalf("expected retry with backoff, got %+v", d)
	}
	t.Logf("transient+idempotent -> retry after %s", d.Wait)

	// last attempt must not retry
	d2 := Classify(err, 3, p, true)
	if d2.Retry {
		t.Fatalf("must not retry after max attempts")
	}
}

func TestClassify_NonIdempotentNotRetried(t *testing.T) {
	p, _ := NewPolicy(3, time.Millisecond, time.Second)
	err := MarkTransient(errors.New("network blip after partial write"))
	d := Classify(err, 1, p, false)
	if d.Retry {
		t.Fatalf("non-idempotent transient must NOT retry")
	}
	if d.Reason != pipeline.ErrRetryUnsafe {
		t.Fatalf("expected retry_unsafe, got %s", d.Reason)
	}
	t.Logf("transient+non-idempotent -> %s (side effect not resubmitted)", d.Reason)
}

func TestClassify_PermanentFails(t *testing.T) {
	p, _ := NewPolicy(3, time.Millisecond, time.Second)
	d := Classify(errors.New("compile error"), 1, p, true)
	if d.Retry || d.Reason != pipeline.ErrTaskFailed {
		t.Fatalf("permanent error must fail, got %+v", d)
	}
}

func TestSleep_Interruptible(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Sleep(ctx, time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected immediate cancel, got %v", err)
	}
}
