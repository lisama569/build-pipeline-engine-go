package engine_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/build-pipeline-engine-go/engine"
	"github.com/lisama569/build-pipeline-engine-go/pipeline"
	"github.com/lisama569/build-pipeline-engine-go/pkg/report"
	"github.com/lisama569/build-pipeline-engine-go/pkg/retry"
)

func resumeCfg(dir string, par int) pipeline.Config {
	return pipeline.Config{
		MaxParallelism:     par,
		MaxAttempts:        1,
		DefaultTaskTimeout: 5 * time.Second,
		ShutdownGrace:      time.Second,
		QueuePolicy:        pipeline.QueueFIFO,
		CacheDir:           "", // exercise the resume checkpoint path, not cache
		StateDir:           dir,
		Resume:             true,
		HistoryRuns:        10,
	}
}

// TestResume_OnlyFailedAndClosureRerun: after a failure, the second run
// reuses the successful upstream branch, reruns only the failed task and its
// dependents, and reaches the same conclusion as a from-scratch clean run.
func TestResume_OnlyFailedAndClosureRerun(t *testing.T) {
	dir := t.TempDir()

	var aRuns, bRuns, cRuns, dRuns int32
	// a -> b -> c   and independent d
	tasks := func(bFails bool) []*pipeline.Task {
		return []*pipeline.Task{
			{ID: "a", Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
				atomic.AddInt32(&aRuns, 1)
				return pipeline.ExecOutput{Digest: "A"}, nil
			}},
			{ID: "b", Deps: []string{"a"}, Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
				atomic.AddInt32(&bRuns, 1)
				if bFails {
					return pipeline.ExecOutput{}, errors.New("boom")
				}
				return pipeline.ExecOutput{Digest: "B"}, nil
			}},
			{ID: "c", Deps: []string{"b"}, Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
				atomic.AddInt32(&cRuns, 1)
				return pipeline.ExecOutput{Digest: "C"}, nil
			}},
			{ID: "d", Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
				atomic.AddInt32(&dRuns, 1)
				return pipeline.ExecOutput{Digest: "D"}, nil
			}},
		}
	}

	// Run 1: b fails. a,b,c,d all execute (c skipped).
	eng, err := engine.NewEngine(resumeCfg(dir, 4))
	if err != nil {
		t.Fatal(err)
	}
	r1, err := eng.Run(context.Background(), &pipeline.Pipeline{Name: "res", Tasks: tasks(true)})
	if err != nil {
		t.Fatal(err)
	}
	eng.Close()
	if r1.Status != "failed" {
		t.Fatalf("run1 status=%s", r1.Status)
	}
	if got := atomic.LoadInt32(&aRuns); got != 1 {
		t.Fatalf("a ran %d times in run1", got)
	}

	// Run 2: same graph, b now succeeds, resume on.
	eng2, err := engine.NewEngine(resumeCfg(dir, 4))
	if err != nil {
		t.Fatal(err)
	}
	r2, err := eng2.Run(context.Background(), &pipeline.Pipeline{Name: "res", Tasks: tasks(false)})
	if err != nil {
		t.Fatal(err)
	}
	if r2.Status != "succeeded" {
		t.Fatalf("run2 status=%s", r2.Status)
	}
	eng2.Close()

	// a and independent d must be reused (never executed in run 2);
	// b reruns (prev_failed) and c reruns (prev_skipped / dep closure).
	if got := atomic.LoadInt32(&aRuns); got != 1 {
		t.Fatalf("a must be reused, total runs=%d want 1", got)
	}
	if got := atomic.LoadInt32(&dRuns); got != 1 {
		t.Fatalf("independent d must be reused, total runs=%d want 1", got)
	}
	if got := atomic.LoadInt32(&bRuns); got != 2 {
		t.Fatalf("b must rerun, total=%d want 2", got)
	}
	// c was skipped (never executed) in run 1, so it executes exactly once
	// now that its dependency succeeds.
	if got := atomic.LoadInt32(&cRuns); got != 1 {
		t.Fatalf("c must run once after dep fixed, total=%d want 1", got)
	}

	st := map[string]bool{}
	for _, tr := range r2.Tasks {
		st[tr.ID] = tr.Reused
		if tr.ID == "b" && tr.RerunReason != "prev_failed" {
			t.Fatalf("b rerun reason=%q want prev_failed", tr.RerunReason)
		}
	}
	if !st["a"] || !st["d"] {
		t.Fatalf("a,d must be marked reused: %v", st)
	}
	if st["b"] || st["c"] {
		t.Fatalf("b,c must not be reused: %v", st)
	}

	// Reference: clean from scratch with the same (successful) inputs in a
	// fresh state directory.
	cleanCfg := resumeCfg(t.TempDir(), 4)
	engClean, _ := engine.NewEngine(cleanCfg)
	defer engClean.Close()
	rClean, err := engClean.Run(context.Background(), &pipeline.Pipeline{Name: "res", Tasks: tasks(false)})
	if err != nil {
		t.Fatal(err)
	}
	if rClean.Conclusion != r2.Conclusion {
		t.Fatalf("resume conclusion %s != clean conclusion %s", r2.Conclusion, rClean.Conclusion)
	}
	t.Logf("resume and clean runs converge on %s", r2.Conclusion[:16])
}

// TestResume_InputChangeInvalidates: a changed input reruns exactly the
// changed task; stable tasks stay reused.
func TestResume_InputChangeInvalidates(t *testing.T) {
	dir := t.TempDir()
	var runsA, runsB int32
	build := func(va, vb string) *pipeline.Pipeline {
		return &pipeline.Pipeline{Name: "iv", Tasks: []*pipeline.Task{
			{ID: "a", CacheKeyInputs: map[string]string{"v": va},
				Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
					atomic.AddInt32(&runsA, 1)
					return pipeline.ExecOutput{Digest: "A-" + va}, nil
				}},
			{ID: "b", Deps: []string{"a"}, CacheKeyInputs: map[string]string{"v": vb},
				Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
					atomic.AddInt32(&runsB, 1)
					return pipeline.ExecOutput{Digest: "B-" + vb}, nil
				}},
		}}
	}
	eng, _ := engine.NewEngine(resumeCfg(dir, 2))
	defer eng.Close()
	if _, err := eng.Run(context.Background(), build("1", "1")); err != nil {
		t.Fatal(err)
	}
	r2, err := eng.Run(context.Background(), build("2", "1"))
	if err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&runsA) != 2 {
		t.Fatalf("changed a must rerun, got %d", runsA)
	}
	if atomic.LoadInt32(&runsB) != 2 {
		t.Fatalf("b must rerun (dep reran), got %d", runsB)
	}
	var reasonA, reasonB string
	for _, tr := range r2.Tasks {
		if tr.ID == "a" {
			reasonA = tr.RerunReason
		}
		if tr.ID == "b" {
			reasonB = tr.RerunReason
		}
	}
	if reasonA != "input_changed" {
		t.Fatalf("a reason=%s", reasonA)
	}
	if reasonB != "dep_reran" {
		t.Fatalf("b reason=%s", reasonB)
	}

	// third run with nothing changed: everything reused.
	r3, err := eng.Run(context.Background(), build("2", "1"))
	if err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&runsA) != 2 || atomic.LoadInt32(&runsB) != 2 {
		t.Fatalf("unchanged inputs must reuse all: a=%d b=%d", runsA, runsB)
	}
	if n := reusedCountRep(r3); n != 2 {
		t.Fatalf("want 2 reused, got %d", n)
	}
}

// TestResume_GraphMutationSafe: renaming a task / changing edges / deleting a
// task must never serve a stale verdict under the new graph.
func TestResume_GraphMutationSafe(t *testing.T) {
	dir := t.TempDir()
	counter := func(m map[string]*int32, id string) pipeline.ExecFunc {
		c := new(int32)
		m[id] = c
		return func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
			atomic.AddInt32(c, 1)
			return pipeline.ExecOutput{Digest: id}, nil
		}
	}
	c1 := map[string]*int32{}
	p1 := &pipeline.Pipeline{Name: "gm", Tasks: []*pipeline.Task{
		{ID: "a", Run: counter(c1, "a")},
		{ID: "b", Run: counter(c1, "b")},
		{ID: "c", Deps: []string{"a", "b"}, Run: counter(c1, "c")},
	}}
	eng, _ := engine.NewEngine(resumeCfg(dir, 4))
	defer eng.Close()
	if _, err := eng.Run(context.Background(), p1); err != nil {
		t.Fatal(err)
	}

	// mutate: "b" renamed to "b2", and c now depends on b2; "a" unchanged.
	c2 := map[string]*int32{}
	p2 := &pipeline.Pipeline{Name: "gm", Tasks: []*pipeline.Task{
		{ID: "a", Run: counter(c2, "a")},
		{ID: "b2", Run: counter(c2, "b2")},
		{ID: "c", Deps: []string{"a", "b2"}, Run: counter(c2, "c")},
	}}
	r2, err := eng.Run(context.Background(), p2)
	if err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(c2["a"]) != 0 {
		t.Fatalf("a should be reused (executed 0 times this run), got %d", *c2["a"])
	}
	if atomic.LoadInt32(c2["b2"]) != 1 {
		t.Fatalf("new b2 must run, got %d", *c2["b2"])
	}
	if atomic.LoadInt32(c2["c"]) != 1 {
		t.Fatalf("c with restructured deps must run, got %d", *c2["c"])
	}
	tr := taskByID(r2, "c")
	if tr.RerunReason != "deps_changed" {
		t.Fatalf("c reason=%s want deps_changed", tr.RerunReason)
	}
}

// TestResume_CacheDisabledDoesNotForceFullRerun: one cache-disabled task
// reruns but the independent branch is still reused.
func TestResume_CacheDisabledDoesNotForceFullRerun(t *testing.T) {
	dir := t.TempDir()
	var x, y int32
	build := func(disable bool) *pipeline.Pipeline {
		return &pipeline.Pipeline{Name: "cd", Tasks: []*pipeline.Task{
			{ID: "x", DisableCache: disable, Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
				atomic.AddInt32(&x, 1)
				return pipeline.ExecOutput{Digest: "X"}, nil
			}},
			{ID: "y", Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
				atomic.AddInt32(&y, 1)
				return pipeline.ExecOutput{Digest: "Y"}, nil
			}},
		}}
	}
	eng, _ := engine.NewEngine(resumeCfg(dir, 2))
	defer eng.Close()
	if _, err := eng.Run(context.Background(), build(false)); err != nil {
		t.Fatal(err)
	}
	r2, err := eng.Run(context.Background(), build(true))
	if err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&x) != 2 {
		t.Fatalf("cache-disabled x must rerun, got %d", x)
	}
	if atomic.LoadInt32(&y) != 1 {
		t.Fatalf("independent y must stay reused, got %d", y)
	}
	tr := taskByID(r2, "x")
	if tr.RerunReason != "cache_disabled" {
		t.Fatalf("x reason=%s", tr.RerunReason)
	}
}

// TestResume_RepeatedSameInputsSameConclusion: running repeatedly over
// identical inputs always yields the same conclusion hash.
func TestResume_RepeatedSameInputsSameConclusion(t *testing.T) {
	dir := t.TempDir()
	eng, _ := engine.NewEngine(resumeCfg(dir, 3))
	defer eng.Close()
	p := &pipeline.Pipeline{Name: "rep", Tasks: []*pipeline.Task{
		{ID: "a", CacheKeyInputs: map[string]string{"v": "1"}, Idempotent: true,
			Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
				return pipeline.ExecOutput{Digest: "A"}, nil
			}},
		{ID: "b", Deps: []string{"a"}, CacheKeyInputs: map[string]string{"v": "1"},
			Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
				return pipeline.ExecOutput{Digest: "B"}, nil
			}},
	}}
	var first string
	for i := 0; i < 4; i++ {
		r, err := eng.Run(context.Background(), p)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = r.Conclusion
		} else if r.Conclusion != first {
			t.Fatalf("run %d conclusion changed: %s", i, r.Conclusion)
		}
	}
	t.Logf("stable conclusion across resume runs: %s", first[:16])
}

// TestResume_TransientFailureThenSuccessReusesRest: a flaky idempotent task
// retried/failed once succeeds on resume; the rest is reused.
func TestResume_TransientFailureThenSuccessReusesRest(t *testing.T) {
	dir := t.TempDir()
	var flakyRuns, otherRuns int32
	mk := func(flakySucceeds bool) *pipeline.Pipeline {
		return &pipeline.Pipeline{Name: "fl", Tasks: []*pipeline.Task{
			{ID: "flaky", Idempotent: true,
				Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
					atomic.AddInt32(&flakyRuns, 1)
					if !flakySucceeds {
						return pipeline.ExecOutput{}, retry.MarkTransient(errors.New("flake"))
					}
					return pipeline.ExecOutput{Digest: "F"}, nil
				}},
			{ID: "other", Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
				atomic.AddInt32(&otherRuns, 1)
				return pipeline.ExecOutput{Digest: "O"}, nil
			}},
		}}
	}
	cfg := resumeCfg(dir, 2)
	cfg.MaxAttempts = 2
	cfg.RetryBackoffBase = time.Millisecond
	cfg.RetryBackoffCap = 2 * time.Millisecond
	eng, _ := engine.NewEngine(cfg)
	defer eng.Close()
	r1, err := eng.Run(context.Background(), mk(false))
	if err != nil {
		t.Fatal(err)
	}
	if r1.Status != "failed" {
		t.Fatalf("run1=%s", r1.Status)
	}
	r2, err := eng.Run(context.Background(), mk(true))
	if err != nil {
		t.Fatal(err)
	}
	if r2.Status != "succeeded" {
		t.Fatalf("run2=%s", r2.Status)
	}
	if atomic.LoadInt32(&otherRuns) != 1 {
		t.Fatalf("other must be reused, ran %d", otherRuns)
	}
	if atomic.LoadInt32(&flakyRuns) != 3 {
		// run1: 2 attempts (both failed), run2: 1 attempt success.
		t.Fatalf("flaky attempts=%d want 3", flakyRuns)
	}
}

func reusedCountRep(r *report.Report) int {
	n := 0
	for _, tr := range r.Tasks {
		if tr.Reused {
			n++
		}
	}
	return n
}

func taskByID(r *report.Report, id string) report.TaskReport {
	for _, tr := range r.Tasks {
		if tr.ID == id {
			return tr
		}
	}
	return report.TaskReport{}
}
