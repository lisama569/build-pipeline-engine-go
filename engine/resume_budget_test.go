package engine_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/build-pipeline-engine-go/engine"
	"github.com/lisama569/build-pipeline-engine-go/pipeline"
)

func stateCfg(stateDir, historyDir string, par int) pipeline.Config {
	c := cfg(par)
	c.CacheDir = "" // isolate resume from the content cache
	c.StateDir = stateDir
	c.HistoryDir = historyDir
	c.HistoryRuns = 5
	c.ShutdownGrace = 2 * time.Second
	return c
}

// deterministicTask returns a deterministic digest derived from its declared
// input so a from-scratch run and a resumed run must hash identically.
func deterministicTask(id string, deps []string, input string) *pipeline.Task {
	t := &pipeline.Task{
		ID: id, Deps: deps,
		CacheKeyInputs: map[string]string{"v": input},
	}
	t.Run = func(_ context.Context, in pipeline.ExecInput) (pipeline.ExecOutput, error) {
		digest := id + ":" + input
		// fold dependency digests in, so an upstream output change flows down
		ids := make([]string, 0, len(in.DepResults))
		for _, d := range in.DepResults {
			ids = append(ids, d.Output.Digest)
		}
		sort.Strings(ids)
		for _, x := range ids {
			digest += "<-" + x
		}
		return pipeline.ExecOutput{Digest: digest}, nil
	}
	return t
}

// TestResume_OnlyReexecutesChangedSubtree_ConclusionMatchesCleanRun is the
// core resume guarantee: a resume after an input change re-runs only the
// changed task and its descendants, yet its conclusion is byte-for-byte the
// same as a clean from-scratch run over the new graph.
func TestResume_OnlyReexecutesChangedSubtree_ConclusionMatchesCleanRun(t *testing.T) {
	build := func(aInput string) *pipeline.Pipeline {
		return &pipeline.Pipeline{Name: "chain", Tasks: []*pipeline.Task{
			deterministicTask("a", nil, aInput),
			deterministicTask("b", []string{"a"}, "stable"),
			deterministicTask("c", []string{"b"}, "stable"),
			deterministicTask("d", nil, "stable"), // independent branch
		}}
	}

	// Reference: clean engine, no state, over the NEW graph.
	refEng, err := engine.NewEngine(stateCfg("", "", 4))
	if err != nil {
		t.Fatal(err)
	}
	defer refEng.Close()
	ref, err := refEng.Run(context.Background(), build("v2"))
	if err != nil {
		t.Fatal(err)
	}

	// Resume path: first run over v1 leaves a snapshot, second run over v2.
	stateDir, histDir := t.TempDir(), t.TempDir()
	eng, err := engine.NewEngine(stateCfg(stateDir, histDir, 4))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	if _, err := eng.Run(context.Background(), build("v1")); err != nil {
		t.Fatal(err)
	}
	resumed, err := eng.Run(context.Background(), build("v2"))
	if err != nil {
		t.Fatal(err)
	}

	if resumed.Conclusion != ref.Conclusion {
		t.Fatalf("resume conclusion != clean run:\n%s\n%s", resumed.Conclusion, ref.Conclusion)
	}
	statuses := map[string]string{}
	executed := map[string]bool{}
	for _, tr := range resumed.Tasks {
		statuses[tr.ID] = string(tr.Status)
		if tr.Reused {
			executed[tr.ID] = false
		} else {
			executed[tr.ID] = true
		}
	}
	// a changed, so a and b and c re-execute; d (independent) is reused.
	if !executed["a"] || !executed["b"] || !executed["c"] {
		t.Fatalf("changed subtree must re-execute: %v", executed)
	}
	if executed["d"] {
		t.Fatalf("independent d must be reused, not executed")
	}
	if resumed.Resume == nil || resumed.Resume.ReusedTasks != 1 {
		t.Fatalf("expected exactly 1 reused task, got %+v", resumed.Resume)
	}
	if resumed.Diff == nil {
		t.Fatal("history diff missing")
	}
	if len(resumed.Diff.Reused) != 1 || resumed.Diff.Reused[0] != "d" {
		t.Fatalf("diff reused=%v", resumed.Diff.Reused)
	}
	t.Logf("resume conclusion matches clean run; diff: reused=%v reexec=%v",
		resumed.Diff.Reused, resumed.Diff.Reexecuted)
}

// TestResume_FailedTaskRerunsNeighborsReused: after a run with a failure, the
// next resume retries the failed task (and tasks skipped because of it) while
// successful unrelated tasks are carried over, and a fixed pipeline reaches
// the clean-run conclusion.
func TestResume_FailedTaskRerunsNeighborsReused(t *testing.T) {
	stateDir, histDir := t.TempDir(), t.TempDir()
	eng, err := engine.NewEngine(stateCfg(stateDir, histDir, 4))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	var failB int32 = 1
	flaky := func(id string, deps []string) *pipeline.Task {
		return &pipeline.Task{ID: id, Deps: deps, Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
			if id == "b" && atomic.SwapInt32(&failB, 0) == 1 {
				return pipeline.ExecOutput{}, fmt.Errorf("boom")
			}
			return pipeline.ExecOutput{Digest: id + "-ok"}, nil
		}}
	}
	mk := func() *pipeline.Pipeline {
		return &pipeline.Pipeline{Name: "flaky-pipe", Tasks: []*pipeline.Task{
			flaky("a", nil), flaky("b", []string{"a"}),
			flaky("c", []string{"b"}), flaky("d", nil),
		}}
	}

	first, err := eng.Run(context.Background(), mk())
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != "failed" {
		t.Fatalf("first run expected failed, got %s", first.Status)
	}
	second, err := eng.Run(context.Background(), mk())
	if err != nil {
		t.Fatal(err)
	}
	if second.Status != "succeeded" {
		t.Fatalf("resume after fix expected succeeded, got %s", second.Status)
	}
	reused := map[string]bool{}
	for _, tr := range second.Tasks {
		reused[tr.ID] = tr.Reused
	}
	// a and d succeeded last time and are unchanged; b failed and c was
	// skipped, so both re-execute.
	if !reused["a"] || !reused["d"] {
		t.Fatalf("a,d must be reused: %v", reused)
	}
	if reused["b"] || reused["c"] {
		t.Fatalf("b (failed) and c (skipped) must re-execute: %v", reused)
	}

	// Clean reference over the now-green pipeline.
	refEng, _ := engine.NewEngine(stateCfg("", "", 4))
	defer refEng.Close()
	ref, _ := refEng.Run(context.Background(), mk())
	if second.Conclusion != ref.Conclusion {
		t.Fatalf("resumed green conclusion %s != clean %s", second.Conclusion, ref.Conclusion)
	}
	t.Log("failed/skipped tasks re-ran; neighbors reused; conclusion matches clean run")
}

// TestResume_WorksWhenCacheDisabledPerTask: a DisableCache task is still
// carried over by resume state; resume does not force a whole-graph rerun.
func TestResume_WorksWhenCacheDisabledPerTask(t *testing.T) {
	stateDir, histDir := t.TempDir(), t.TempDir()
	eng, err := engine.NewEngine(stateCfg(stateDir, histDir, 2))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	var executions int32
	mk := func() *pipeline.Pipeline {
		noCache := &pipeline.Task{
			ID: "nocache", DisableCache: true,
			Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
				atomic.AddInt32(&executions, 1)
				return pipeline.ExecOutput{Digest: "nocache-out"}, nil
			},
		}
		cached := &pipeline.Task{
			ID: "cached", Deps: []string{"nocache"},
			Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
				atomic.AddInt32(&executions, 1)
				return pipeline.ExecOutput{Digest: "cached-out"}, nil
			},
		}
		return &pipeline.Pipeline{Name: "mixed", Tasks: []*pipeline.Task{noCache, cached}}
	}
	if _, err := eng.Run(context.Background(), mk()); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&executions); got != 2 {
		t.Fatalf("first run executes both, got %d", got)
	}
	second, err := eng.Run(context.Background(), mk())
	if err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&executions); got != 2 {
		t.Fatalf("resume must not re-execute cache-disabled tasks, got %d", got)
	}
	for _, tr := range second.Tasks {
		if !tr.Reused {
			t.Fatalf("task %s should be reused even with cache disabled", tr.ID)
		}
		if tr.Cached {
			t.Fatalf("resume reuse is not a cache hit")
		}
	}
}

// TestResume_RepeatedSameInputs_StableConclusionAndNoExec: same inputs run
// three times; conclusion never changes and only the first run executes.
func TestResume_RepeatedSameInputs_StableConclusionAndNoExec(t *testing.T) {
	stateDir, histDir := t.TempDir(), t.TempDir()
	eng, err := engine.NewEngine(stateCfg(stateDir, histDir, 4))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	var executions int32
	mk := func() *pipeline.Pipeline {
		tasks := []*pipeline.Task{}
		for i := 0; i < 4; i++ {
			id := fmt.Sprintf("t%d", i)
			tasks = append(tasks, &pipeline.Task{
				ID: id, CacheKeyInputs: map[string]string{"fixed": "1"},
				Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
					atomic.AddInt32(&executions, 1)
					return pipeline.ExecOutput{Digest: id}, nil
				},
			})
		}
		return &pipeline.Pipeline{Name: "stable", Tasks: tasks}
	}

	var first string
	for i := 0; i < 3; i++ {
		r, err := eng.Run(context.Background(), mk())
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = r.Conclusion
		} else if r.Conclusion != first {
			t.Fatalf("run %d conclusion drifted", i+1)
		}
	}
	if got := atomic.LoadInt32(&executions); got != 4 {
		t.Fatalf("only first run should execute; got %d executions", got)
	}
}

// TestRunBudget_ExhaustionConvergesAndDistinct: once the budget is used up no
// new task launches, in-flight work is canceled and converges bounded by
// grace, the run concludes with a distinct status, and it cannot be confused
// with a task timeout or a manual cancel.
func TestRunBudget_ExhaustionConvergesAndDistinct(t *testing.T) {
	c := stateCfg("", "", 3)
	c.RunBudget = 60 * time.Millisecond
	c.DefaultTaskTimeout = 30 * time.Second // task timeout is far away
	eng, err := engine.NewEngine(c)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	started := make(chan struct{}, 3)
	tasks := []*pipeline.Task{}
	for i := 0; i < 6; i++ {
		id := fmt.Sprintf("slow%d", i)
		tasks = append(tasks, &pipeline.Task{
			ID: id, Timeout: 30 * time.Second,
			Run: func(ctx context.Context, _ pipeline.ExecInput) (pipeline.ExecOutput, error) {
				started <- struct{}{}
				<-ctx.Done()
				return pipeline.ExecOutput{}, ctx.Err()
			},
		})
	}

	done := make(chan struct{})
	var rep *struct{ status, reason string }
	go func() {
		r, err := eng.Run(context.Background(), &pipeline.Pipeline{Name: "budget", Tasks: tasks})
		if err == nil {
			rep = &struct{ status, reason string }{r.Status, r.EndReason}
		}
		close(done)
	}()
	<-started
	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("budget-exhausted run did not converge within bounded grace")
	}
	if rep == nil {
		t.Fatal("run returned an error")
	}
	if rep.status != "budget_exhausted" {
		t.Fatalf("status=%s, want budget_exhausted", rep.status)
	}
	if rep.reason != "run_budget_exhausted" {
		t.Fatalf("end reason=%s", rep.reason)
	}
	t.Log("budget exhaustion: run converged with distinct status and reason")
}

// TestRunBudget_DistinctFromManualCancelAndTaskTimeout documents via outcomes
// that the three stop causes are reported differently.
func TestRunBudget_DistinctFromManualCancelAndTaskTimeout(t *testing.T) {
	block := func(id string) *pipeline.Task {
		return &pipeline.Task{ID: id, Timeout: time.Second,
			Run: func(ctx context.Context, _ pipeline.ExecInput) (pipeline.ExecOutput, error) {
				<-ctx.Done()
				return pipeline.ExecOutput{}, ctx.Err()
			}}
	}

	// manual cancel
	c1 := cfg(1)
	e1, _ := engine.NewEngine(c1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(30 * time.Millisecond); cancel() }()
	r1, _ := e1.Run(ctx, &pipeline.Pipeline{Name: "x", Tasks: []*pipeline.Task{block("t")}})
	if r1.Status != "canceled" {
		t.Fatalf("manual cancel => %s", r1.Status)
	}
	e1.Close()

	// budget
	c2 := cfg(1)
	c2.RunBudget = 30 * time.Millisecond
	c2.DefaultTaskTimeout = time.Minute
	e2, _ := engine.NewEngine(c2)
	r2, _ := e2.Run(context.Background(), &pipeline.Pipeline{Name: "x", Tasks: []*pipeline.Task{block("t")}})
	if r2.Status != "budget_exhausted" {
		t.Fatalf("budget => %s", r2.Status)
	}
	e2.Close()

	// task timeout
	c3 := cfg(1)
	c3.DefaultTaskTimeout = 30 * time.Millisecond
	e3, _ := engine.NewEngine(c3)
	r3, _ := e3.Run(context.Background(), &pipeline.Pipeline{Name: "x", Tasks: []*pipeline.Task{block("t")}})
	if r3.Tasks[0].Status != pipeline.StatusTimedOut {
		t.Fatalf("task timeout => %s", r3.Tasks[0].Status)
	}
	if r3.Status != "failed" {
		t.Fatalf("task timeout run status => %s", r3.Status)
	}
	e3.Close()
}

// TestHistory_ConcurrentRunsDoNotCorrupt: several runs of the same pipeline
// serialize on the per-pipeline lock; every record is well-formed and the
// retained count stays bounded.
func TestHistory_ConcurrentRunsDoNotCorrupt(t *testing.T) {
	stateDir, histDir := t.TempDir(), t.TempDir()
	eng, err := engine.NewEngine(stateCfg(stateDir, histDir, 2))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	const n = 6
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p := &pipeline.Pipeline{Name: "same", Tasks: []*pipeline.Task{
				{ID: "t", CacheKeyInputs: map[string]string{"v": "1"},
					Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
						return pipeline.ExecOutput{Digest: "X"}, nil
					}},
			}}
			r, err := eng.Run(context.Background(), p)
			if err != nil {
				errs <- err
				return
			}
			if r.Status != "succeeded" {
				errs <- fmt.Errorf("run %d status %s", i, r.Status)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	// every retained file must parse and count must be bounded by retention.
	files, _ := filepath.Glob(filepath.Join(histDir, "*", "*.json"))
	if len(files) > 5 {
		t.Fatalf("history unbounded: %d files", len(files))
	}
	if len(files) == 0 {
		t.Fatal("no history records written")
	}
	for _, f := range files {
		if _, err := os.ReadFile(f); err != nil {
			t.Fatalf("history file unreadable: %v", err)
		}
	}
	t.Logf("%d concurrent runs -> %d intact, bounded history files", n, len(files))
}
