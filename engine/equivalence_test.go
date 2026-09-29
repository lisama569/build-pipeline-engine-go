package engine_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/build-pipeline-engine-go/engine"
	"github.com/lisama569/build-pipeline-engine-go/pipeline"
)

// TestResume_EquivalentToCleanRun_Diamond: a diamond pipeline fails on one
// branch mid-way, then a resuming run after the fix must produce the exact
// same conclusion hash as a from-scratch run over the final, healthy graph.
func TestResume_EquivalentToCleanRun_Diamond(t *testing.T) {
	dir := t.TempDir()

	// graph: root -> (left, right) -> join
	mk := func(rightFail bool) *pipeline.Pipeline {
		return &pipeline.Pipeline{Name: "diamond", Tasks: []*pipeline.Task{
			{ID: "root", CacheKeyInputs: map[string]string{"k": "1"},
				Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
					return pipeline.ExecOutput{Digest: "root"}, nil
				}},
			{ID: "left", Deps: []string{"root"},
				Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
					return pipeline.ExecOutput{Digest: "left"}, nil
				}},
			{ID: "right", Deps: []string{"root"},
				Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
					if rightFail {
						return pipeline.ExecOutput{}, errors.New("right broken")
					}
					return pipeline.ExecOutput{Digest: "right"}, nil
				}},
			{ID: "join", Deps: []string{"left", "right"},
				Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
					return pipeline.ExecOutput{Digest: "join"}, nil
				}},
		}}
	}

	// run 1: right fails (join skipped).
	eng1, _ := engine.NewEngine(resumeCfg(dir, 3))
	r1, err := eng1.Run(context.Background(), mk(true))
	if err != nil {
		t.Fatal(err)
	}
	eng1.Close()
	if r1.Status != "failed" {
		t.Fatalf("run1=%s", r1.Status)
	}

	// run 2: resume after fix.
	eng2, _ := engine.NewEngine(resumeCfg(dir, 3))
	r2, err := eng2.Run(context.Background(), mk(false))
	if err != nil {
		t.Fatal(err)
	}
	eng2.Close()
	if r2.Status != "succeeded" {
		t.Fatalf("run2=%s", r2.Status)
	}

	// reference: clean run in a fresh state dir.
	cleanEng, _ := engine.NewEngine(resumeCfg(t.TempDir(), 3))
	defer cleanEng.Close()
	rClean, err := cleanEng.Run(context.Background(), mk(false))
	if err != nil {
		t.Fatal(err)
	}
	if r2.Conclusion != rClean.Conclusion {
		t.Fatalf("resume conclusion\n%s\n!= clean\n%s", r2.Conclusion, rClean.Conclusion)
	}

	// and run 2 must physically have reused root and left only.
	wantReused := map[string]bool{"root": true, "left": true}
	gotReused := map[string]bool{}
	for _, tr := range r2.Tasks {
		if tr.Reused {
			gotReused[tr.ID] = true
		}
	}
	if len(gotReused) != 2 || !gotReused["root"] || !gotReused["left"] {
		t.Fatalf("expected only root,left reused; got %v", gotReused)
	}
	_ = wantReused
	t.Logf("resume reused root+left, reran right(prev_failed)+join; conclusion matches clean: %s",
		r2.Conclusion[:16])
}

// TestResume_CacheAndResumeCompose: with BOTH cache and state enabled,
// repeated identical runs converge; the second resume reuses everything
// without executing bodies.
func TestResume_CacheAndResumeCompose(t *testing.T) {
	dir := t.TempDir()
	cfg := resumeCfg(dir, 2)
	cfg.CacheDir = t.TempDir() // also enable the content cache
	eng, _ := engine.NewEngine(cfg)
	defer eng.Close()

	var runs int32
	p := &pipeline.Pipeline{Name: "both", Tasks: []*pipeline.Task{
		{ID: "a", CacheKeyInputs: map[string]string{"v": "1"},
			Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
				atomic.AddInt32(&runs, 1)
				return pipeline.ExecOutput{Digest: "A"}, nil
			}},
		{ID: "b", Deps: []string{"a"}, CacheKeyInputs: map[string]string{"v": "1"},
			Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
				atomic.AddInt32(&runs, 1)
				return pipeline.ExecOutput{Digest: "B"}, nil
			}},
	}}
	var conclusions []string
	for i := 0; i < 3; i++ {
		r, err := eng.Run(context.Background(), p)
		if err != nil {
			t.Fatal(err)
		}
		conclusions = append(conclusions, r.Conclusion)
		time.Sleep(2 * time.Millisecond)
	}
	if conclusions[0] != conclusions[1] || conclusions[1] != conclusions[2] {
		t.Fatalf("conclusions diverged: %v", conclusions)
	}
	if got := atomic.LoadInt32(&runs); got != 2 {
		// first run executes both; later runs reuse both from the checkpoint.
		t.Fatalf("expected exactly 2 body executions, got %d", got)
	}
}

// TestResume_FailureThenInputChangeRerunsCorrectSet: after a fixed failure,
// an unrelated input change still reruns exactly the changed closure on the
// following run.
func TestResume_FailureThenInputChangeRerunsCorrectSet(t *testing.T) {
	dir := t.TempDir()
	var aruns, bruns, cruns int32
	mk := func(aVer string, failB bool) *pipeline.Pipeline {
		return &pipeline.Pipeline{Name: "seq", Tasks: []*pipeline.Task{
			{ID: "a", CacheKeyInputs: map[string]string{"v": aVer},
				Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
					atomic.AddInt32(&aruns, 1)
					return pipeline.ExecOutput{Digest: "A" + aVer}, nil
				}},
			{ID: "b", Deps: []string{"a"},
				Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
					atomic.AddInt32(&bruns, 1)
					if failB {
						return pipeline.ExecOutput{}, fmt.Errorf("b boom")
					}
					return pipeline.ExecOutput{Digest: "B"}, nil
				}},
			{ID: "c", Deps: []string{"b"},
				Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
					atomic.AddInt32(&cruns, 1)
					return pipeline.ExecOutput{Digest: "C"}, nil
				}},
		}}
	}
	eng, _ := engine.NewEngine(resumeCfg(dir, 2))
	defer eng.Close()
	r1, _ := eng.Run(context.Background(), mk("1", true)) // b fails; c skipped
	if r1.Status != "failed" {
		t.Fatalf("r1=%s", r1.Status)
	}
	r2, _ := eng.Run(context.Background(), mk("1", false)) // b fixed; all succeed
	if r2.Status != "succeeded" {
		t.Fatalf("r2=%s", r2.Status)
	}
	if got := atomic.LoadInt32(&aruns); got != 1 {
		t.Fatalf("a reused in run2, total a=%d want 1", got)
	}
	r3, _ := eng.Run(context.Background(), mk("2", false)) // a input changes
	if r3.Status != "succeeded" {
		t.Fatalf("r3=%s", r3.Status)
	}
	if got := atomic.LoadInt32(&aruns); got != 2 {
		t.Fatalf("a must rerun on input change, total=%d want 2", got)
	}
	// b executed: run1 (failed) + run2 (fixed) + run3 (closure of changed a).
	if got := atomic.LoadInt32(&bruns); got != 3 {
		t.Fatalf("b total=%d want 3", got)
	}
	if got := atomic.LoadInt32(&cruns); got != 2 {
		t.Fatalf("c reruns (closure), total=%d want 2", got)
	}
}
