package engine_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/build-pipeline-engine-go/engine"
	"github.com/lisama569/build-pipeline-engine-go/pipeline"
)

func newEng(t *testing.T, cacheDir string, par int) *engine.Engine {
	t.Helper()
	eng, err := engine.NewEngine(pipeline.Config{
		MaxParallelism:     par,
		MaxAttempts:        1,
		DefaultTaskTimeout: 5 * time.Second,
		ShutdownGrace:      2 * time.Second,
		QueuePolicy:        pipeline.QueueFIFO,
		CacheDir:           cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	return eng
}

// TestRepeatRun_SameConclusion: identical inputs across runs produce the
// identical conclusion hash (timestamps excluded).
func TestRepeatRun_SameConclusion(t *testing.T) {
	p := &pipeline.Pipeline{Name: "repeat", Tasks: []*pipeline.Task{
		{ID: "a", CacheKeyInputs: map[string]string{"v": "1"},
			Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
				return pipeline.ExecOutput{Digest: "A"}, nil
			}},
		{ID: "b", Deps: []string{"a"}, CacheKeyInputs: map[string]string{"v": "1"},
			Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
				return pipeline.ExecOutput{Digest: "B"}, nil
			}},
	}}
	dir := t.TempDir()
	var prev string
	for i := 0; i < 3; i++ {
		eng := newEng(t, dir, 2)
		rep, err := eng.Run(context.Background(), p)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			prev = rep.Conclusion
		} else if rep.Conclusion != prev {
			t.Fatalf("conclusion changed across runs:\n%s\n%s", prev, rep.Conclusion)
		}
		t.Logf("run %d conclusion=%s", i+1, rep.Conclusion[:16])
		eng.Close()
	}
}

// TestCache_InvalidatedByInputChange: changing an input changes the key and
// forces recomputation (no stale hit); stable input hits and has no effect.
func TestCache_InvalidatedByInputChange(t *testing.T) {
	dir := t.TempDir()
	var runs int32
	build := func(version string) *pipeline.Pipeline {
		return &pipeline.Pipeline{Name: "cc", Tasks: []*pipeline.Task{{
			ID: "compile",
			CacheKeyInputs: map[string]string{
				"sourcesha": version,
				"goversion": "x1",
			},
			Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
				atomic.AddInt32(&runs, 1)
				return pipeline.ExecOutput{Digest: "out-" + version}, nil
			},
		}}}
	}
	eng := newEng(t, dir, 1)
	defer eng.Close()

	_, _ = eng.Run(context.Background(), build("v1"))
	r2, _ := eng.Run(context.Background(), build("v1"))
	if got := atomic.LoadInt32(&runs); got != 1 {
		t.Fatalf("identical input must run once, got %d", got)
	}
	if r2.Tasks[0].Status != pipeline.StatusCacheHit {
		t.Fatalf("expected cache_hit, got %s", r2.Tasks[0].Status)
	}
	t.Log("identical inputs: second run cache_hit, no repeated side effect")

	// environment/input change -> different key -> recompute, no stale result
	r3, _ := eng.Run(context.Background(), build("v2"))
	if r3.Tasks[0].Status != pipeline.StatusSucceeded {
		t.Fatalf("changed input must recompute, got %s", r3.Tasks[0].Status)
	}
	if r3.Tasks[0].OutputDigest != "out-v2" {
		t.Fatalf("must observe fresh output, got %q", r3.Tasks[0].OutputDigest)
	}
	t.Log("changed sourcesha invalidated cache; fresh conclusion out-v2")
}

// TestCache_PollutedEntryRejected: a hand-corrupted entry that passes JSON
// but has a bad checksum must be recomputed, never served.
func TestCache_PollutedEntryRejected(t *testing.T) {
	dir := t.TempDir()
	var runs int32
	p := &pipeline.Pipeline{Name: "poll", Tasks: []*pipeline.Task{{
		ID: "t", CacheKeyInputs: map[string]string{"k": "1"},
		Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
			atomic.AddInt32(&runs, 1)
			return pipeline.ExecOutput{Digest: "good", Stdout: "good"}, nil
		}},
	}}
	eng := newEng(t, dir, 1)
	defer eng.Close()
	if _, err := eng.Run(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	// corrupt every file in the cache (flip payload byte)
	filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".json") {
			return nil
		}
		b, _ := os.ReadFile(path)
		for i := range b {
			if b[i] == 'g' { // inside "good"
				b[i] = 'X'
				break
			}
		}
		return os.WriteFile(path, b, 0o644)
	})
	rep, err := eng.Run(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	tr := rep.Tasks[0]
	if tr.Status != pipeline.StatusSucceeded {
		t.Fatalf("polluted cache must be recomputed, got %s reason=%s", tr.Status, tr.Reason)
	}
	if atomic.LoadInt32(&runs) != 2 {
		t.Fatalf("expected recompute (2 runs), got %d", runs)
	}
	if !strings.Contains(rep.Text(), "cache_corrupt") {
		t.Fatalf("trace must explicitly classify the polluted entry")
	}
	t.Log("polluted entry classified cache_corrupt and recomputed; no wrong conclusion")
}

// TestCrossPipeline_ExecutedOnce: the same task referenced by two pipelines
// run concurrently is executed once and both see the same conclusion.
func TestCrossPipeline_ExecutedOnce(t *testing.T) {
	dir := t.TempDir()
	var runs int32
	mkTask := func() *pipeline.Task {
		return &pipeline.Task{
			ID: "shared", CacheKeyInputs: map[string]string{"shared": "1"},
			Run: func(ctx context.Context, _ pipeline.ExecInput) (pipeline.ExecOutput, error) {
				time.Sleep(40 * time.Millisecond)
				atomic.AddInt32(&runs, 1)
				return pipeline.ExecOutput{Digest: "shared-out"}, nil
			},
		}
	}
	eng := newEng(t, dir, 4)
	defer eng.Close()

	var wg sync.WaitGroup
	conclusions := make([]string, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p := &pipeline.Pipeline{Name: fmt.Sprintf("pipe%d", i), Tasks: []*pipeline.Task{mkTask()}}
			r, err := eng.Run(context.Background(), p)
			if err != nil {
				t.Error(err)
				return
			}
			conclusions[i] = r.Tasks[0].OutputDigest
		}(i)
	}
	wg.Wait()
	if atomic.LoadInt32(&runs) != 1 {
		t.Fatalf("shared task must execute once across pipelines, got %d", runs)
	}
	if conclusions[0] != "shared-out" || conclusions[1] != "shared-out" {
		t.Fatalf("both pipelines must see identical output, got %v", conclusions)
	}
	t.Logf("two concurrent pipelines -> %d execution, identical conclusions", runs)
}

// TestLargeGraph_LinearScaling: scheduling a wide diamond graph completes
// without exponential blowup or unbounded goroutines.
func TestLargeGraph_LinearScaling(t *testing.T) {
	const n = 2000
	var ran int32
	tasks := make([]*pipeline.Task, 0, n+2)
	tasks = append(tasks, &pipeline.Task{ID: "root",
		Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
			return pipeline.ExecOutput{Digest: "r"}, nil
		}})
	for i := 0; i < n; i++ {
		tasks = append(tasks, &pipeline.Task{
			ID: fmt.Sprintf("n%d", i), Deps: []string{"root"},
			Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
				atomic.AddInt32(&ran, 1)
				return pipeline.ExecOutput{Digest: "x"}, nil
			}})
	}
	tasks = append(tasks, &pipeline.Task{ID: "join",
		Deps: func() []string {
			d := make([]string, n)
			for i := range d {
				d[i] = fmt.Sprintf("n%d", i)
			}
			return d
		}(),
		Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
			return pipeline.ExecOutput{Digest: "j"}, nil
		}})

	eng := newEng(t, "", 8)
	defer eng.Close()
	start := time.Now()
	rep, err := eng.Run(context.Background(), &pipeline.Pipeline{Name: "big", Tasks: tasks})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Status != "succeeded" {
		t.Fatalf("status=%s", rep.Status)
	}
	if atomic.LoadInt32(&ran) != int32(n) {
		t.Fatalf("expected %d leaf runs, got %d", n, ran)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("scaling regressed: %d nodes took %s", n, elapsed)
	}
	t.Logf("%d-node graph scheduled in %s (parallelism=8)", n+2, elapsed)
}

// TestPriorityQueue_DeterministicOrder prints and checks the priority policy
// schedules the task with more downstream dependents first.
func TestPriorityQueue_DeterministicOrder(t *testing.T) {
	cfg := pipeline.Config{
		MaxParallelism: 1, MaxAttempts: 1,
		DefaultTaskTimeout: time.Second, ShutdownGrace: time.Second,
		QueuePolicy: pipeline.QueuePriority,
	}
	eng, err := engine.NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	order := make(chan string, 8)
	noop := func(id string) *pipeline.Task {
		return &pipeline.Task{ID: id, Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
			order <- id
			return pipeline.ExecOutput{Digest: id}, nil
		}}
	}
	// a has one dependent (leafA); b has two (c and leafB-chain). With par=1
	// and both initially ready, the higher-downstream node should go first.
	tasks := []*pipeline.Task{
		noop("a"),
		noop("b"),
		{ID: "leafA", Deps: []string{"a"}, Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
			order <- "leafA"
			return pipeline.ExecOutput{}, nil
		}},
		{ID: "c", Deps: []string{"b"}, Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
			order <- "c"
			return pipeline.ExecOutput{}, nil
		}},
		{ID: "leafB", Deps: []string{"c"}, Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
			order <- "leafB"
			return pipeline.ExecOutput{}, nil
		}},
	}
	rep, err := eng.Run(context.Background(), &pipeline.Pipeline{Name: "prio", Tasks: tasks})
	if err != nil {
		t.Fatal(err)
	}
	first := <-order
	t.Logf("dispatch order starts with %s (downstream-priority)", first)
	t.Log("\n" + rep.Text())
	if first != "b" {
		t.Fatalf("priority policy should dispatch b (2 dependents) before a (1), got %s", first)
	}
}
