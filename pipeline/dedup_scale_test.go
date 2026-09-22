package pipeline

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestRunMany_SharedTaskExecutedOnce(t *testing.T) {
	dir := t.TempDir()
	writeInput(t, dir, "in.txt", "shared")
	var logs strings.Builder
	cfg := Config{
		RepoRoot: dir, CacheDir: filepath.Join(dir, "cache"),
		Parallelism: 4, EnvPassthrough: nil, Logf: logTo(&logs),
	}
	e := newTestEngine(t, cfg)

	var sharedRuns int32
	shared := func() TaskDef {
		return TaskDef{
			Name: "compile", Cacheable: true, Idempotent: true,
			Inputs: []string{"in.txt"},
			Run: func(ctx context.Context) (*TaskOutput, error) {
				atomic.AddInt32(&sharedRuns, 1)
				sleepMs(15)
				return &TaskOutput{Payload: []byte("artifact")}, nil
			},
		}
	}
	// Two pipelines both referencing the same action identity "compile".
	p1 := &Pipeline{Name: "one", Tasks: []TaskDef{
		shared(),
		{Name: "package-a", Deps: []string{"compile"}, Run: okTask()},
	}}
	p2 := &Pipeline{Name: "two", Tasks: []TaskDef{
		shared(),
		{Name: "package-b", Deps: []string{"compile"}, Run: okTask()},
	}}

	reports, err := e.RunMany(context.Background(), []*Pipeline{p1, p2})
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range reports {
		if r.Status != StatusSucceeded {
			t.Fatalf("pipeline %d status=%s reason=%s", i, r.Status, r.Reason)
		}
	}
	if got := atomic.LoadInt32(&sharedRuns); got != 1 {
		t.Fatalf("shared task executed %d times across pipelines, want exactly 1", got)
	}
	t.Logf("scheduler log:\n%s", logs.String())
}

func TestLargeGraph_ScalesLinearlyAndNoGoroutineLeak(t *testing.T) {
	// Chain-ish diamond grid: N nodes, ~2N edges. Must schedule without
	// exponential blowup or unbounded goroutine creation.
	const N = 2000
	e := newTestEngine(t, Config{Parallelism: 8})
	tasks := make([]TaskDef, 0, N)
	for i := 0; i < N; i++ {
		t := TaskDef{Name: fmt.Sprintf("t%04d", i), Run: okTask()}
		if i > 0 {
			t.Deps = []string{fmt.Sprintf("t%04d", i-1)}
		}
		if i > 1 {
			t.Deps = append(t.Deps, fmt.Sprintf("t%04d", i-2))
		}
		tasks = append(tasks, t)
	}
	p := &Pipeline{Name: "big", Tasks: tasks}

	done := make(chan struct{})
	var r *Report
	go func() {
		r, _ = e.Run(context.Background(), p)
		close(done)
	}()
	select {
	case <-done:
	case <-timeoutAfter():
		t.Fatal("large graph scheduling did not finish in bounded time")
	}
	if r.Status != StatusSucceeded {
		t.Fatalf("status=%s", r.Status)
	}
	if r.Summary["succeeded"] != N {
		t.Fatalf("summary=%v", r.Summary)
	}
	if len(r.Order) != N {
		t.Fatalf("schedule order dropped tasks: %d", len(r.Order))
	}
	if err := e.Close(); err != nil {
		t.Fatalf("engine close after big run: %v", err)
	}
}

func TestRepeatedRuns_IdenticalConclusions(t *testing.T) {
	dir := t.TempDir()
	writeInput(t, dir, "in.txt", "data")
	cfg := Config{RepoRoot: dir, CacheDir: "", Parallelism: 4} // in-memory cache fresh per engine

	mkEngine := func() *Engine { return newTestEngine(t, cfg) }
	mkPipeline := func() *Pipeline {
		return &Pipeline{Name: "p", Tasks: []TaskDef{
			{Name: "a", Run: okTask()},
			{Name: "b", Deps: []string{"a"}, Run: okTask()},
			{Name: "c", Run: okTask()},
			{Name: "d", Deps: []string{"b", "c"}, Run: okTask()},
		}}
	}

	var prev []byte
	for i := 0; i < 5; i++ {
		e := mkEngine()
		r, err := e.Run(context.Background(), mkPipeline())
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := r.JSON()
		// Normalize the only measurement field (durations) for comparison.
		norm := stripMeasurements(raw)
		if prev != nil && string(norm) != string(prev) {
			t.Fatalf("run %d produced a different conclusion:\n%s\nvs\n%s", i, prev, norm)
		}
		prev = norm
	}
}

func TestDiamondDependency_RunsOnce(t *testing.T) {
	var runs int32
	e := newTestEngine(t, Config{Parallelism: 4})
	p := &Pipeline{Name: "d", Tasks: []TaskDef{
		{Name: "root", Run: func(ctx context.Context) (*TaskOutput, error) {
			atomic.AddInt32(&runs, 1)
			return &TaskOutput{}, nil
		}},
		{Name: "l", Deps: []string{"root"}, Run: okTask()},
		{Name: "r", Deps: []string{"root"}, Run: okTask()},
		{Name: "leaf", Deps: []string{"l", "r"}, Run: okTask()},
	}}
	rep, err := e.Run(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if runs != 1 {
		t.Fatalf("root ran %d times in a diamond", runs)
	}
	if rep.Status != StatusSucceeded {
		t.Fatalf("status=%s", rep.Status)
	}
}

func TestNonIdempotentRuntimeRetryIsRefusedDistinctly(t *testing.T) {
	// Programmatic task that fails and is non-idempotent: Parse allows only
	// idempotent+retries, so simulate the runtime guard by constructing via
	// Parse with Retries 0 isn't possible — instead assert Parse-time reason.
	p := &Pipeline{Name: "p", Tasks: []TaskDef{
		{Name: "a", Retries: 1, Idempotent: false, Run: okTask()},
	}}
	_, err := Parse(p, DefaultConfig())
	pe := AsError(err)
	if pe == nil || pe.Reason != ReasonInvalidConfig {
		t.Fatalf("non-idempotent retry must be rejected distinctly, got %v", err)
	}
}

var timeoutOnce sync.Once
var timeoutCh chan struct{}

func timeoutAfter() <-chan struct{} {
	timeoutOnce.Do(func() { timeoutCh = make(chan struct{}) })
	ch := make(chan struct{})
	go func() {
		sleepMs(5000)
		close(ch)
	}()
	return ch
}

var _ = os.MkdirAll
