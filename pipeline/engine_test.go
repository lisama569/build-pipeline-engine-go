package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func newTestEngine(t *testing.T, cfg Config) *Engine {
	t.Helper()
	if cfg.RepoRoot == "" {
		cfg.RepoRoot = t.TempDir()
	}
	e, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func okTask() func(ctx context.Context) (*TaskOutput, error) {
	return func(ctx context.Context) (*TaskOutput, error) { return &TaskOutput{}, nil }
}

func TestExecute_BasicSuccess(t *testing.T) {
	var logs strings.Builder
	e := newTestEngine(t, Config{Logf: logTo(&logs)})
	p := &Pipeline{Name: "p", Tasks: []TaskDef{
		{Name: "a", Run: okTask()},
		{Name: "b", Deps: []string{"a"}, Run: okTask()},
	}}
	r, err := e.Run(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != StatusSucceeded {
		t.Fatalf("status=%s reason=%s", r.Status, r.Reason)
	}
	if r.Tasks[0].Name != "a" || r.Tasks[1].Name != "b" {
		t.Fatalf("report task list not in graph order: %v %v", r.Tasks[0].Name, r.Tasks[1].Name)
	}
	t.Logf("scheduler log:\n%s", logs.String())
}

func TestExecute_BoundedParallelismAndConcurrentWave(t *testing.T) {
	var logs strings.Builder
	var inFlight int32
	var maxInFlight int32
	var mu sync.Mutex
	dispatched := []string{}

	makeRun := func() func(ctx context.Context) (*TaskOutput, error) {
		return func(ctx context.Context) (*TaskOutput, error) {
			n := atomic.AddInt32(&inFlight, 1)
			for {
				cur := atomic.LoadInt32(&maxInFlight)
				if n <= cur || atomic.CompareAndSwapInt32(&maxInFlight, cur, n) {
					break
				}
			}
			block := make(chan struct{})
			_ = block
			// short sleep without time import
			wait := make(chan struct{})
			go func() { sleepMs(20); close(wait) }()
			select {
			case <-wait:
			case <-ctx.Done():
			}
			atomic.AddInt32(&inFlight, -1)
			return &TaskOutput{}, nil
		}
	}

	p := &Pipeline{Name: "p", Tasks: []TaskDef{
		{Name: "a", Run: makeRun()},
		{Name: "b", Run: makeRun()},
		{Name: "c", Run: makeRun()},
		{Name: "d", Run: makeRun()},
	}}
	e := newTestEngine(t, Config{Parallelism: 2, Logf: logTo(&logs)})
	e.cfg.Logf = func(format string, args ...any) {
		if strings.HasPrefix(format, "dispatch") {
			mu.Lock()
			dispatched = append(dispatched, fmt.Sprintf(format, args...))
			mu.Unlock()
		}
		logTo(&logs)(format, args...)
	}

	r, err := e.Run(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != StatusSucceeded {
		t.Fatalf("status=%s", r.Status)
	}
	if maxInFlight > 2 {
		t.Fatalf("parallelism exceeded: observed %d > 2", maxInFlight)
	}
	if maxInFlight < 2 {
		t.Fatalf("independent tasks never ran concurrently (max=%d)", maxInFlight)
	}
	// Deterministic dispatch order: lexicographic under a slot limit.
	if !strings.Contains(dispatched[0], "task=a") ||
		!strings.Contains(dispatched[1], "task=b") {
		t.Fatalf("first dispatches not lexicographic: %v", dispatched[:2])
	}
	t.Logf("dispatch events:\n  %s", strings.Join(dispatched, "\n  "))
	t.Logf("max in-flight = %d (limit 2)", maxInFlight)
}

func TestExecute_FailurePropagatesAsSkipped(t *testing.T) {
	var logs strings.Builder
	e := newTestEngine(t, Config{Logf: logTo(&logs)})
	boom := fmt.Errorf("boom")
	p := &Pipeline{Name: "p", Tasks: []TaskDef{
		{Name: "a", Run: func(ctx context.Context) (*TaskOutput, error) {
			return nil, boom
		}},
		{Name: "b", Deps: []string{"a"}, Run: okTask()},
		{Name: "c", Deps: []string{"b"}, Run: okTask()},
		// d is independent and must still run and succeed.
		{Name: "d", Run: okTask()},
	}}
	r, err := e.Run(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != StatusFailed || r.Reason != ReasonFailed {
		t.Fatalf("overall = %s/%s", r.Status, r.Reason)
	}
	byName := map[string]*TaskResult{}
	for _, x := range r.Tasks {
		byName[x.Name] = x
	}
	if byName["b"].Status != StatusSkipped || byName["b"].Reason != ReasonUpstream {
		t.Fatalf("b = %s/%s, want skipped/upstream", byName["b"].Status, byName["b"].Reason)
	}
	if byName["c"].Status != StatusSkipped || byName["c"].Reason != ReasonUpstream {
		t.Fatalf("c = %s/%s, want skipped/upstream", byName["c"].Status, byName["c"].Reason)
	}
	if byName["d"].Status != StatusSucceeded {
		t.Fatalf("independent d = %s, want succeeded", byName["d"].Status)
	}
	if !strings.Contains(logs.String(), "skip task=b reason=upstream_failed") {
		t.Fatalf("log must explain skip decision:\n%s", logs.String())
	}
	t.Logf("scheduler log:\n%s", logs.String())
}

func logTo(b *strings.Builder) func(string, ...any) {
	var mu sync.Mutex
	return func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		fmt.Fprintf(b, format+"\n", args...)
	}
}
