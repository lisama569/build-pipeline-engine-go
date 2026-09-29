package history_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/lisama569/build-pipeline-engine-go/pkg/history"
)

func mkRec(id string, tasks ...history.TaskRecord) *history.Record {
	return &history.Record{
		ID: id, Pipeline: "p", StartedAt: time.Now(),
		Conclusion: "h" + id, Status: "succeeded", Tasks: tasks,
	}
}

// TestRetention_Bounded: only the newest N records survive.
func TestRetention_Bounded(t *testing.T) {
	dir := t.TempDir()
	s, err := history.Open(dir, 3)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		lk, err := s.Lock("p")
		if err != nil {
			t.Fatal(err)
		}
		id := time.Now().Add(time.Duration(i) * 10 * time.Millisecond)
		_ = id
		rec := mkRec(
			string(rune('a'+i)),
			history.TaskRecord{ID: "x", Status: "succeeded"},
		)
		rec.StartedAt = time.Unix(1000, 0).Add(time.Duration(i) * time.Millisecond)
		if _, err := s.Append(rec); err != nil {
			t.Fatal(err)
		}
		lk.Release()
	}
	got, err := s.List("p")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("retention bound violated: %d records", len(got))
	}
	// newest-first: c d e
	if got[0].ID != "e" || got[2].ID != "c" {
		t.Fatalf("expected newest e..c retained, got %s %s %s", got[0].ID, got[1].ID, got[2].ID)
	}
	// exactly 3 json files on disk (no unbounded growth)
	files, _ := filepath.Glob(filepath.Join(dir, "*", "*.json"))
	if len(files) != 3 {
		t.Fatalf("expected 3 files, got %d", len(files))
	}
}

// TestOpen_ClampsRetention: zero defaults, over-cap clamps.
func TestOpen_ClampsRetention(t *testing.T) {
	s, err := history.Open(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if s.MaxRuns() != history.DefaultRuns {
		t.Fatalf("default = %d", s.MaxRuns())
	}
	s2, _ := history.Open(t.TempDir(), history.HardMaxRuns+50)
	if s2.MaxRuns() != history.HardMaxRuns {
		t.Fatalf("clamp = %d", s2.MaxRuns())
	}
	s0, _ := history.Open("", 5)
	if s0.Enabled() {
		t.Fatal("empty dir must disable history")
	}
}

// TestCompare_ReuseRerunStatusChange: the diff classifies reused, re-executed
// (with reason), status changes, added and removed tasks.
func TestCompare_ReuseRerunStatusChange(t *testing.T) {
	prev := mkRec("r1",
		history.TaskRecord{ID: "a", Status: "succeeded", OutputDigest: "A"},
		history.TaskRecord{ID: "b", Status: "failed"},
		history.TaskRecord{ID: "c", Status: "succeeded", OutputDigest: "C"},
		history.TaskRecord{ID: "gone", Status: "succeeded"},
	)
	cur := mkRec("r2",
		history.TaskRecord{ID: "a", Status: "succeeded", Reused: true, OutputDigest: "A"},
		history.TaskRecord{ID: "b", Status: "succeeded", ReuseInvalidation: "previous_not_success"},
		history.TaskRecord{ID: "c", Status: "succeeded", ReuseInvalidation: "input_changed", OutputDigest: "C2"},
		history.TaskRecord{ID: "d", Status: "succeeded"},
	)
	d := history.Compare(prev, cur)
	if len(d.Reused) != 1 || d.Reused[0] != "a" {
		t.Fatalf("reused=%v", d.Reused)
	}
	if len(d.Reexecuted) != 2 {
		t.Fatalf("reexecuted=%v", d.Reexecuted)
	}
	reasons := map[string]string{}
	for _, r := range d.Reexecuted {
		reasons[r.ID] = r.Reason
	}
	if reasons["b"] != "previous_not_success" || reasons["c"] != "input_changed" {
		t.Fatalf("reasons=%v", reasons)
	}
	if len(d.StatusChanged) != 1 || d.StatusChanged[0].ID != "b" ||
		d.StatusChanged[0].From != "failed" || d.StatusChanged[0].To != "succeeded" {
		t.Fatalf("status changes=%v", d.StatusChanged)
	}
	if len(d.Added) != 1 || d.Added[0] != "d" {
		t.Fatalf("added=%v", d.Added)
	}
	if len(d.Removed) != 1 || d.Removed[0] != "gone" {
		t.Fatalf("removed=%v", d.Removed)
	}
	if d.PreviousID != "r1" {
		t.Fatalf("previous id=%s", d.PreviousID)
	}
}

func TestCompare_FirstRunAllAdded(t *testing.T) {
	cur := mkRec("r1", history.TaskRecord{ID: "a"})
	d := history.Compare(nil, cur)
	if len(d.Added) != 1 || d.Added[0] != "a" || len(d.Reused) != 0 {
		t.Fatalf("first run diff=%+v", d)
	}
}

// TestAppend_ReturnsPreviousRecord for diffing.
func TestAppend_ReturnsPreviousRecord(t *testing.T) {
	s, _ := history.Open(t.TempDir(), 5)
	lk, _ := s.Lock("p")
	r1 := mkRec("one")
	r1.StartedAt = time.Unix(1000, 0)
	prev, err := s.Append(r1)
	if err != nil || prev != nil {
		t.Fatalf("first append prev=%v err=%v", prev, err)
	}
	r2 := mkRec("two")
	r2.StartedAt = time.Unix(2000, 0)
	prev, err = s.Append(r2)
	if err != nil || prev == nil || prev.ID != "one" {
		t.Fatalf("second append should return previous one, got %v err=%v", prev, err)
	}
	lk.Release()
}
