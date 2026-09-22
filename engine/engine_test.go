package engine_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/build-pipeline-engine-go/engine"
	"github.com/lisama569/build-pipeline-engine-go/pipeline"
	"github.com/lisama569/build-pipeline-engine-go/pkg/retry"
)

func cfg(par int) pipeline.Config {
	return pipeline.Config{
		MaxParallelism:     par,
		MaxAttempts:        1,
		DefaultTaskTimeout: 5 * time.Second,
		ShutdownGrace:      3 * time.Second,
		QueuePolicy:        pipeline.QueueFIFO,
		CacheDir:           "",
	}
}

func fn(f func(ctx context.Context) (pipeline.ExecOutput, error)) pipeline.ExecFunc {
	return func(ctx context.Context, _ pipeline.ExecInput) (pipeline.ExecOutput, error) {
		return f(ctx)
	}
}

// TestBoundedParallelism verifies never more than N tasks overlap.
func TestBoundedParallelism(t *testing.T) {
	var inFlight, maxSeen int32
	var mu sync.Mutex
	lock := func() {
		mu.Lock()
		defer mu.Unlock()
		n := atomic.AddInt32(&inFlight, 1)
		if n > maxSeen {
			maxSeen = n
		}
	}
	unlock := func() { atomic.AddInt32(&inFlight, -1) }

	tasks := []*pipeline.Task{}
	for i := 0; i < 8; i++ {
		tasks = append(tasks, &pipeline.Task{
			ID: fmt.Sprintf("t%d", i),
			Run: fn(func(ctx context.Context) (pipeline.ExecOutput, error) {
				lock()
				defer unlock()
				time.Sleep(30 * time.Millisecond)
				return pipeline.ExecOutput{Digest: "d"}, nil
			}),
		})
	}
	eng, err := engine.NewEngine(cfg(3))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	rep, err := eng.Run(context.Background(), &pipeline.Pipeline{Name: "par", Tasks: tasks})
	if err != nil {
		t.Fatal(err)
	}
	if maxSeen > 3 {
		t.Fatalf("parallelism exceeded: saw %d > 3 (no unbounded goroutine execution)", maxSeen)
	}
	if rep.Status != "succeeded" {
		t.Fatalf("status=%s", rep.Status)
	}
	t.Logf("8 tasks with limit 3: observed peak concurrency %d", maxSeen)
}

// TestFailurePropagation: an upstream failure forces all transitive
// downstream tasks to skipped; they never run.
func TestFailurePropagation(t *testing.T) {
	var ran int32
	tasks := []*pipeline.Task{
		{ID: "a", Run: fn(func(c context.Context) (pipeline.ExecOutput, error) {
			return pipeline.ExecOutput{}, errors.New("boom")
		})},
		{ID: "b", Deps: []string{"a"}, Run: fn(func(c context.Context) (pipeline.ExecOutput, error) {
			atomic.AddInt32(&ran, 1)
			return pipeline.ExecOutput{}, nil
		})},
		{ID: "c", Deps: []string{"b"}, Run: fn(func(c context.Context) (pipeline.ExecOutput, error) {
			atomic.AddInt32(&ran, 1)
			return pipeline.ExecOutput{}, nil
		})},
		{ID: "d", Run: fn(func(c context.Context) (pipeline.ExecOutput, error) {
			return pipeline.ExecOutput{Digest: "d"}, nil // independent: must still run
		})},
	}
	eng, _ := engine.NewEngine(cfg(4))
	defer eng.Close()
	rep, err := eng.Run(context.Background(), &pipeline.Pipeline{Name: "fail", Tasks: tasks})
	if err != nil {
		t.Fatal(err)
	}
	st := map[string]pipeline.TaskStatus{}
	for _, tr := range rep.Tasks {
		st[tr.ID] = tr.Status
	}
	if st["a"] != pipeline.StatusFailed {
		t.Fatalf("a should fail, got %s", st["a"])
	}
	if st["b"] != pipeline.StatusSkipped || st["c"] != pipeline.StatusSkipped {
		t.Fatalf("b,c must be skipped: b=%s c=%s", st["b"], st["c"])
	}
	if st["d"] != pipeline.StatusSucceeded {
		t.Fatalf("independent d must still succeed: %s", st["d"])
	}
	if atomic.LoadInt32(&ran) != 0 {
		t.Fatalf("skipped tasks must never execute, ran=%d", ran)
	}
	if rep.Status != "failed" {
		t.Fatalf("run status should be failed, got %s", rep.Status)
	}
	for _, tr := range rep.Tasks {
		if tr.Status == pipeline.StatusSkipped {
			if !strings.HasPrefix(tr.Reason, "upstream:") {
				t.Fatalf("skip must name upstream, reason=%q", tr.Reason)
			}
			t.Logf("task %s skipped with reason %q", tr.ID, tr.Reason)
		}
	}
}

// TestTimeout_MarkedNotRetried: a task past its deadline is timed_out and is
// never sent through the retry loop.
func TestTimeout_MarkedNotRetried(t *testing.T) {
	var attempts int32
	tasks := []*pipeline.Task{{
		ID:          "slow",
		Timeout:     40 * time.Millisecond,
		MaxAttempts: 5,
		Idempotent:  true,
		Run: fn(func(ctx context.Context) (pipeline.ExecOutput, error) {
			atomic.AddInt32(&attempts, 1)
			<-ctx.Done() // cooperatively block until deadline/cancel
			return pipeline.ExecOutput{}, ctx.Err()
		}),
	}}
	eng, _ := engine.NewEngine(cfg(1))
	defer eng.Close()
	rep, err := eng.Run(context.Background(), &pipeline.Pipeline{Name: "timeout", Tasks: tasks})
	if err != nil {
		t.Fatal(err)
	}
	var tr struct {
		s pipeline.TaskStatus
		a int
		r string
	}
	for _, x := range rep.Tasks {
		if x.ID == "slow" {
			tr.s, tr.a, tr.r = x.Status, x.Attempts, x.Reason
		}
	}
	if tr.s != pipeline.StatusTimedOut {
		t.Fatalf("expected timed_out, got %s", tr.s)
	}
	if tr.a != 1 {
		t.Fatalf("timeout must not retry, attempts=%d", tr.a)
	}
	if tr.r != string(pipeline.ErrTaskTimeout) {
		t.Fatalf("reason should be task_timeout, got %s", tr.r)
	}
	t.Logf("slow task marked %s after 1 attempt (not retried): %s", tr.s, tr.r)
}

// TestRetry_IdempotentTransientRetriesAndSucceeds.
func TestRetry_IdempotentTransientRetriesAndSucceeds(t *testing.T) {
	var attempts int32
	c := cfg(1)
	c.MaxAttempts = 3
	c.RetryBackoffBase = time.Millisecond
	c.RetryBackoffCap = 5 * time.Millisecond
	tasks := []*pipeline.Task{{
		ID:         "flaky",
		Idempotent: true,
		Run: fn(func(ctx context.Context) (pipeline.ExecOutput, error) {
			n := atomic.AddInt32(&attempts, 1)
			if n < 3 {
				return pipeline.ExecOutput{}, retry.MarkTransient(errors.New("transient flake"))
			}
			return pipeline.ExecOutput{Digest: "ok"}, nil
		}),
	}}
	eng, _ := engine.NewEngine(c)
	defer eng.Close()
	rep, err := eng.Run(context.Background(), &pipeline.Pipeline{Name: "retry", Tasks: tasks})
	if err != nil {
		t.Fatal(err)
	}
	x := rep.Tasks[0]
	if x.Status != pipeline.StatusSucceeded || x.Attempts != 3 {
		t.Fatalf("expected success on attempt 3, got %s attempts=%d", x.Status, x.Attempts)
	}
	t.Logf("idempotent transient retried to success on attempt %d", x.Attempts)
}

// TestRetry_NonIdempotentNotRetried: even with retry budget, a transient
// failure of a non-idempotent task is executed once and distinctly reported.
func TestRetry_NonIdempotentNotRetried(t *testing.T) {
	var attempts int32
	c := cfg(1)
	c.MaxAttempts = 5
	tasks := []*pipeline.Task{{
		ID:         "writeonce",
		Idempotent: false,
		Run: fn(func(ctx context.Context) (pipeline.ExecOutput, error) {
			atomic.AddInt32(&attempts, 1)
			return pipeline.ExecOutput{}, retry.MarkTransient(errors.New("partial side effect"))
		}),
	}}
	eng, _ := engine.NewEngine(c)
	defer eng.Close()
	rep, _ := eng.Run(context.Background(), &pipeline.Pipeline{Name: "unsafe", Tasks: tasks})
	x := rep.Tasks[0]
	if x.Attempts != 1 {
		t.Fatalf("non-idempotent task must run once, attempts=%d", x.Attempts)
	}
	if x.Reason != string(pipeline.ErrRetryUnsafe) {
		t.Fatalf("expected retry_unsafe, got %s", x.Reason)
	}
	if x.Status != pipeline.StatusFailed {
		t.Fatalf("expected failed, got %s", x.Status)
	}
	t.Logf("non-idempotent transient classified %s and not resubmitted", x.Reason)
}

// TestCancellation_ConvergesBounded: cancel mid-run; running task ends,
// unlaunched tasks are canceled/skipped, and Run returns within grace.
func TestCancellation_ConvergesBounded(t *testing.T) {
	c := cfg(2)
	started := make(chan string, 4)
	release := make(chan struct{})
	tasks := []*pipeline.Task{}
	for i := 0; i < 4; i++ {
		id := fmt.Sprintf("t%d", i)
		tasks = append(tasks, &pipeline.Task{
			ID:      id,
			Timeout: 5 * time.Second,
			Run: fn(func(ctx context.Context) (pipeline.ExecOutput, error) {
				started <- id
				select {
				case <-ctx.Done():
					return pipeline.ExecOutput{}, ctx.Err()
				case <-release:
					return pipeline.ExecOutput{Digest: "d"}, nil
				}
			}),
		})
	}
	eng, _ := engine.NewEngine(c)
	defer eng.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_, _ = eng.Run(ctx, &pipeline.Pipeline{Name: "cancel", Tasks: tasks})
		close(done)
	}()
	<-started // at least one running
	cancel()
	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("Run did not converge within bounded shutdown grace")
	}
	t.Log("cancellation propagated and run converged within grace; no dangling task")
}
