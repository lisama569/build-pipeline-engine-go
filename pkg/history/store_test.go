package history_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/build-pipeline-engine-go/pipeline"
	"github.com/lisama569/build-pipeline-engine-go/pkg/history"
)

func recWith(pipe, run string, at time.Time, tasks []history.TaskRecord) *history.Record {
	r := &history.Record{Pipeline: pipe, RunID: run, StartedAt: at,
		EndedAt: at.Add(time.Millisecond), StopReason: pipeline.StopCompleted,
		RunStatus: "succeeded", Conclusion: "c", Tasks: tasks}
	return r
}

func rec(pipe, run string, at time.Time) *history.Record {
	return recWith(pipe, run, at, []history.TaskRecord{
		{ID: "t0", Status: pipeline.StatusSucceeded, Reason: "ok"},
	})
}

func TestStore_RetentionBounded(t *testing.T) {
	s, err := history.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 10; i++ {
		if err := s.Save(rec("p", fmt.Sprintf("run-%02d", i), base.Add(time.Duration(i)*time.Minute)), 3); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.List("p")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("retention=3 but %d records kept: %v", len(got), ids(got))
	}
	if got[0].RunID != "run-09" || got[2].RunID != "run-07" {
		t.Fatalf("pruned wrong set: %v", ids(got))
	}
}

func TestStore_HardCapClamped(t *testing.T) {
	s, _ := history.Open(t.TempDir())
	base := time.Now()
	for i := 0; i < history.HardHistoryCap+5; i++ {
		if err := s.Save(rec("p", fmt.Sprintf("r%03d", i), base.Add(time.Duration(i)*time.Second)),
			history.HardHistoryCap+1000); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := s.List("p")
	if len(got) != history.HardHistoryCap {
		t.Fatalf("hard cap violated: %d > %d", len(got), history.HardHistoryCap)
	}
}

func TestStore_ConcurrentPipelinesAndRuns(t *testing.T) {
	s, _ := history.Open(t.TempDir())
	var wg sync.WaitGroup
	for p := 0; p < 4; p++ {
		for r := 0; r < 10; r++ {
			wg.Add(1)
			go func(p, r int) {
				defer wg.Done()
				name := fmt.Sprintf("pipe-%d", p)
				run := fmt.Sprintf("run-%d-%d", p, r)
				err := s.Save(rec(name, run, time.Now().Add(time.Duration(p*100+r)*time.Millisecond)), 50)
				if err != nil {
					t.Error(err)
				}
			}(p, r)
		}
	}
	wg.Wait()
	for p := 0; p < 4; p++ {
		got, err := s.List(fmt.Sprintf("pipe-%d", p))
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 10 {
			t.Fatalf("pipe-%d lost records: %d/10", p, len(got))
		}
	}
}

func TestStore_CorruptQuarantinedNotFatal(t *testing.T) {
	dir := t.TempDir()
	s, _ := history.Open(dir)
	if err := s.Save(rec("p", "good", time.Now()), 5); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(rec("p", "bad", time.Now().Add(time.Second)), 5); err != nil {
		t.Fatal(err)
	}
	// Tamper with the "good" record payload (files live under <pipeline>/).
	files, _ := filepath.Glob(filepath.Join(dir, "history", "*", "*.json"))
	var tampered int
	for _, path := range files {
		if !strings.Contains(path, "good") {
			continue
		}
		b, _ := os.ReadFile(path)
		// Flip a byte inside the embedded record payload (after the envelope
		// header) so the checksum must mismatch.
		idx := strings.Index(string(b), `"started_at"`)
		if idx < 0 {
			t.Fatal("payload marker not found")
		}
		b[idx+len(`"started_at"`)] = 'X'
		if err := os.WriteFile(path, b, 0o644); err != nil {
			t.Fatal(err)
		}
		tampered++
	}
	if tampered != 1 {
		t.Fatalf("tampered %d files", tampered)
	}
	got, err := s.List("p")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].RunID != "bad" {
		t.Fatalf("corrupt record must be skipped, got %v", ids(got))
	}
	badFiles, _ := filepath.Glob(filepath.Join(dir, "history", "*", "*.bad"))
	if len(badFiles) != 1 {
		t.Fatalf("corrupt record must be quarantined, got %v", badFiles)
	}
}

func TestCompare(t *testing.T) {
	older := recWith("p", "run-0", time.Now(), []history.TaskRecord{
		{ID: "a", Status: pipeline.StatusSucceeded},
		{ID: "b", Status: pipeline.StatusFailed, Reason: "task_failed"},
		{ID: "gone", Status: pipeline.StatusSucceeded},
	})
	newer := recWith("p", "run-1", time.Now().Add(time.Second), []history.TaskRecord{
		{ID: "a", Status: pipeline.StatusCacheHit, Reused: true},
		{ID: "b", Status: pipeline.StatusSucceeded, RerunReason: "prev_failed"},
		{ID: "c", Status: pipeline.StatusSucceeded},
	})
	d := history.Compare(older, newer)
	if len(d.Reused) != 1 || d.Reused[0] != "a" {
		t.Fatalf("reused=%v", d.Reused)
	}
	if len(d.New) != 1 || d.New[0] != "c" {
		t.Fatalf("new=%v", d.New)
	}
	if len(d.Removed) != 1 || d.Removed[0] != "gone" {
		t.Fatalf("removed=%v", d.Removed)
	}
	if len(d.Reran) != 1 || d.Reran[0].ID != "b" || d.Reran[0].Reason != "prev_failed" {
		t.Fatalf("reran=%+v", d.Reran)
	}
	if len(d.StatusChanged) != 1 || d.StatusChanged[0].ID != "b" {
		t.Fatalf("changed=%+v", d.StatusChanged)
	}
	// cache_hit normalizes to success so a is NOT a status change.
}

func ids(rs []*history.Record) []string {
	var out []string
	for _, r := range rs {
		out = append(out, r.RunID)
	}
	return out
}
