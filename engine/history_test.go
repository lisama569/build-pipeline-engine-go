package engine_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/build-pipeline-engine-go/engine"
	"github.com/lisama569/build-pipeline-engine-go/pipeline"
	"github.com/lisama569/build-pipeline-engine-go/pkg/history"
)

func stateCfg(dir string, history int) pipeline.Config {
	return pipeline.Config{
		MaxParallelism: 2, MaxAttempts: 1,
		DefaultTaskTimeout: 5 * time.Second,
		ShutdownGrace:      time.Second,
		StateDir:           dir,
		Resume:             false, // history does not require resume
		HistoryRuns:        history,
	}
}

// TestHistory_RecordsRetainedAndBounded: runs are retained per pipeline up
// to the configured bound, newest first.
func TestHistory_RecordsRetainedAndBounded(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.NewEngine(stateCfg(dir, 3))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	p := &pipeline.Pipeline{Name: "keep", Tasks: []*pipeline.Task{{
		ID: "a", Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
			return pipeline.ExecOutput{Digest: "A"}, nil
		},
	}}}
	for i := 0; i < 6; i++ {
		if _, err := eng.Run(context.Background(), p); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond) // distinct StartedAt ordering
	}
	recs, err := eng.History().List("keep")
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 {
		t.Fatalf("want 3 retained, got %d", len(recs))
	}
	// newest first ordering
	for i := 1; i < len(recs); i++ {
		if recs[i-1].StartedAt.Before(recs[i].StartedAt) {
			t.Fatalf("history not newest-first at %d", i)
		}
	}
}

// TestHistory_ConcurrentPipelinesDoNotCorrupt: many pipelines run
// concurrently against one engine; every pipeline keeps all its own records
// and conclusions remain readable.
func TestHistory_ConcurrentPipelinesDoNotCorrupt(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.NewEngine(stateCfg(dir, 50))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	var wg sync.WaitGroup
	const pipes = 6
	const runs = 8
	for pl := 0; pl < pipes; pl++ {
		for r := 0; r < runs; r++ {
			wg.Add(1)
			go func(pl, r int) {
				defer wg.Done()
				name := fmt.Sprintf("pipe-%d", pl)
				p := &pipeline.Pipeline{Name: name, Tasks: []*pipeline.Task{{
					ID: "a",
					Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
						return pipeline.ExecOutput{Digest: fmt.Sprintf("%d-%d", pl, r)}, nil
					},
				}}}
				rep, err := eng.Run(context.Background(), p)
				if err != nil {
					t.Error(err)
					return
				}
				if rep.Status != "succeeded" {
					t.Errorf("pipe %d run %d status %s", pl, r, rep.Status)
				}
			}(pl, r)
		}
	}
	wg.Wait()
	for pl := 0; pl < pipes; pl++ {
		name := fmt.Sprintf("pipe-%d", pl)
		recs, err := eng.History().List(name)
		if err != nil {
			t.Fatal(err)
		}
		if len(recs) != runs {
			t.Fatalf("%s lost/corrupted records: %d/%d", name, len(recs), runs)
		}
	}
}

// TestHistory_ResumeComparison: compare two retained runs across a failure
// and fix: b re-executes (prev_failed), a is reused, and the status change is
// visible in the diff.
func TestHistory_ResumeComparison(t *testing.T) {
	dir := t.TempDir()
	cfg := stateCfg(dir, 10)
	cfg.Resume = true
	eng, _ := engine.NewEngine(cfg)
	defer eng.Close()

	mk := func(fail bool) *pipeline.Pipeline {
		return &pipeline.Pipeline{Name: "cmp", Tasks: []*pipeline.Task{
			{ID: "a", CacheKeyInputs: map[string]string{"v": "1"},
				Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
					return pipeline.ExecOutput{Digest: "A"}, nil
				}},
			{ID: "b", Deps: []string{"a"},
				Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
					if fail {
						return pipeline.ExecOutput{}, fmt.Errorf("boom")
					}
					return pipeline.ExecOutput{Digest: "B"}, nil
				}},
		}}
	}
	r1, err := eng.Run(context.Background(), mk(true))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	r2, err := eng.Run(context.Background(), mk(false))
	if err != nil {
		t.Fatal(err)
	}
	_ = r1
	rec1, err := eng.History().Load("cmp", r1.RunID)
	if err != nil {
		t.Fatal(err)
	}
	rec2, err := eng.History().Load("cmp", r2.RunID)
	if err != nil {
		t.Fatal(err)
	}
	d := history.Compare(rec1, rec2)
	if !contains(d.Reused, "a") {
		t.Fatalf("a must be reused: %v", d.Reused)
	}
	var bReran string
	for _, rr := range d.Reran {
		if rr.ID == "b" {
			bReran = rr.Reason
		}
	}
	if bReran != "prev_failed" {
		t.Fatalf("b reran with %q", bReran)
	}
	var changedB bool
	for _, c := range d.StatusChanged {
		if c.ID == "b" {
			changedB = true
			if c.Before != pipeline.StatusFailed || c.After != pipeline.StatusSucceeded {
				t.Fatalf("b changed %s -> %s", c.Before, c.After)
			}
		}
	}
	if !changedB {
		t.Fatal("b status change must be reported")
	}
}

// TestHistory_ZeroDisablesPersist: HistoryRuns=0 writes nothing.
func TestHistory_ZeroDisablesPersist(t *testing.T) {
	dir := t.TempDir()
	eng, _ := engine.NewEngine(stateCfg(dir, 0))
	defer eng.Close()
	p := &pipeline.Pipeline{Name: "noh", Tasks: []*pipeline.Task{{
		ID: "a", Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
			return pipeline.ExecOutput{Digest: "A"}, nil
		},
	}}}
	if _, err := eng.Run(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	recs, err := eng.History().List("noh")
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatalf("history disabled but %d records retained", len(recs))
	}
}

func contains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}
