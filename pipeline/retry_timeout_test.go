package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func fastCfg(parallel int) Config {
	return Config{
		Parallelism:     parallel,
		DefaultTimeout:  2 * time.Second,
		RetryBackoff:    time.Millisecond,
		MaxRetryBackoff: 5 * time.Millisecond,
		ShutdownGrace:   time.Second,
	}
}

func TestRetry_SucceedsAfterFailuresAndBounded(t *testing.T) {
	var logs strings.Builder
	var attempts int32
	cfg := fastCfg(1)
	cfg.Logf = logTo(&logs)
	e := newTestEngine(t, cfg)
	p := &Pipeline{Name: "p", Tasks: []TaskDef{
		{Name: "a", Retries: 2, Idempotent: true,
			Run: func(ctx context.Context) (*TaskOutput, error) {
				n := atomic.AddInt32(&attempts, 1)
				if n < 3 {
					return nil, fmt.Errorf("flaky-%d", n)
				}
				return &TaskOutput{Payload: []byte("ok")}, nil
			}},
	}}
	r, err := e.Run(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	tr := r.Tasks[0]
	if tr.Status != StatusSucceeded || tr.Attempts != 3 {
		t.Fatalf("status=%s attempts=%d, want succeeded/3", tr.Status, tr.Attempts)
	}
	if attempts != 3 {
		t.Fatalf("body ran %d times", attempts)
	}
	t.Logf("scheduler log:\n%s", logs.String())
}

func TestRetry_ExhaustsAndClassifiesFailure(t *testing.T) {
	var attempts int32
	e := newTestEngine(t, fastCfg(1))
	p := &Pipeline{Name: "p", Tasks: []TaskDef{
		{Name: "a", Retries: 2, Idempotent: true,
			Run: func(ctx context.Context) (*TaskOutput, error) {
				atomic.AddInt32(&attempts, 1)
				return nil, fmt.Errorf("always-broken")
			}},
	}}
	r, _ := e.Run(context.Background(), p)
	tr := r.Tasks[0]
	if tr.Status != StatusFailed || tr.Reason != ReasonFailed {
		t.Fatalf("status=%s reason=%s", tr.Status, tr.Reason)
	}
	if tr.Attempts != 3 {
		t.Fatalf("attempts=%d, want 3 (1 + 2 retries)", tr.Attempts)
	}
}

func TestTimeout_MarkedNotRetried(t *testing.T) {
	var logs strings.Builder
	var attempts int32
	cfg := fastCfg(1)
	cfg.DefaultTimeout = 40 * time.Millisecond
	cfg.ShutdownGrace = 200 * time.Millisecond
	cfg.Logf = logTo(&logs)
	e := newTestEngine(t, cfg)

	released := make(chan struct{})
	t.Cleanup(func() { close(released) })

	p := &Pipeline{Name: "p", Tasks: []TaskDef{
		{Name: "slow", Retries: 5, Idempotent: true,
			Run: func(ctx context.Context) (*TaskOutput, error) {
				atomic.AddInt32(&attempts, 1)
				<-ctx.Done() // cooperative: stops on timeout
				return nil, ctx.Err()
			}},
	}}
	r, err := e.Run(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	tr := r.Tasks[0]
	if tr.Status != StatusTimedOut || tr.Reason != ReasonTimeout {
		t.Fatalf("status=%s reason=%s, want timed_out/timed_out", tr.Status, tr.Reason)
	}
	if attempts != 1 {
		t.Fatalf("timeout task retried %d times; timeout must be terminal", attempts)
	}
	if !strings.Contains(tr.Message, "40ms") {
		t.Fatalf("message should carry the bound: %q", tr.Message)
	}
	t.Logf("scheduler log:\n%s", logs.String())
}

func TestTimeout_DownstreamSkippedUpstream(t *testing.T) {
	cfg := fastCfg(1)
	cfg.DefaultTimeout = 30 * time.Millisecond
	cfg.ShutdownGrace = 200 * time.Millisecond
	e := newTestEngine(t, cfg)
	p := &Pipeline{Name: "p", Tasks: []TaskDef{
		{Name: "slow", Idempotent: true, Run: func(ctx context.Context) (*TaskOutput, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}},
		{Name: "after", Deps: []string{"slow"}, Run: okTask()},
	}}
	r, _ := e.Run(context.Background(), p)
	by := map[string]*TaskResult{}
	for _, x := range r.Tasks {
		by[x.Name] = x
	}
	if by["slow"].Status != StatusTimedOut {
		t.Fatalf("slow=%s", by["slow"].Status)
	}
	if by["after"].Status != StatusSkipped || by["after"].Reason != ReasonUpstream {
		t.Fatalf("after=%s/%s", by["after"].Status, by["after"].Reason)
	}
}

func TestCancel_PropagatesAndConvergesBounded(t *testing.T) {
	var logs strings.Builder
	cfg := fastCfg(2)
	cfg.DefaultTimeout = 10 * time.Second
	cfg.ShutdownGrace = 500 * time.Millisecond
	cfg.Logf = logTo(&logs)
	e := newTestEngine(t, cfg)

	started := make(chan struct{}, 2)
	stopped := make(chan struct{}, 2)
	p := &Pipeline{Name: "p", Tasks: []TaskDef{
		{Name: "a", Run: func(ctx context.Context) (*TaskOutput, error) {
			started <- struct{}{}
			<-ctx.Done()
			stopped <- struct{}{}
			return nil, ctx.Err()
		}},
		{Name: "b", Run: func(ctx context.Context) (*TaskOutput, error) {
			started <- struct{}{}
			<-ctx.Done()
			stopped <- struct{}{}
			return nil, ctx.Err()
		}},
		// queued task: never started, must be marked canceled-skip
		{Name: "c", Run: func(ctx context.Context) (*TaskOutput, error) {
			t.Error("queued task c must not run after cancellation")
			return &TaskOutput{}, nil
		}},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan *Report, 1)
	go func() {
		r, _ := e.Run(ctx, p)
		done <- r
	}()

	<-started
	<-started
	cancel()

	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	select {
	case r := <-done:
		for _, x := range r.Tasks {
			if x.Name == "c" {
				if x.Status != StatusSkipped || x.Reason != ReasonCanceled {
					t.Fatalf("queued c = %s/%s, want skipped/canceled", x.Status, x.Reason)
				}
			} else if x.Status != StatusSkipped || x.Reason != ReasonCanceled {
				t.Fatalf("in-flight %s = %s/%s, want skipped/canceled", x.Name, x.Status, x.Reason)
			}
		}
	case <-deadline.C:
		t.Fatal("run did not converge within bounded time after cancel (leaked execution?)")
	}
	// Both cooperating bodies must have actually stopped.
	for i := 0; i < 2; i++ {
		select {
		case <-stopped:
		case <-time.After(time.Second):
			t.Fatal("task body did not observe cancellation")
		}
	}
	if !strings.Contains(logs.String(), "cancel signal received") {
		t.Fatalf("cancel must be explained in log:\n%s", logs.String())
	}
	t.Logf("scheduler log:\n%s", logs.String())
}

func TestCancel_NonCooperativeTaskConvergesWithinGrace(t *testing.T) {
	// A body that ignores ctx must still not leak the run: runOnce waits
	// bounded by ShutdownGrace for the abandoned body, and the scheduler
	// terminates. (The stray goroutine finishes later harmlessly.)
	cfg := fastCfg(1)
	cfg.DefaultTimeout = 10 * time.Second
	cfg.ShutdownGrace = 50 * time.Millisecond
	e := newTestEngine(t, cfg)

	var ran int32
	p := &Pipeline{Name: "p", Tasks: []TaskDef{
		{Name: "stubborn", Run: func(ctx context.Context) (*TaskOutput, error) {
			atomic.AddInt32(&ran, 1)
			time.Sleep(300 * time.Millisecond) // ignores ctx
			return &TaskOutput{}, nil
		}},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_, _ = e.Run(ctx, p)
		close(done)
	}()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("non-cooperative task prevented bounded shutdown")
	}
}
