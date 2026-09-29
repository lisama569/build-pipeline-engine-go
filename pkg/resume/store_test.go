package resume_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lisama569/build-pipeline-engine-go/pipeline"
	"github.com/lisama569/build-pipeline-engine-go/pkg/resume"
)

func TestStore_RoundTrip(t *testing.T) {
	s, err := resume.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cp := &resume.Checkpoint{
		Pipeline:  "pipe",
		RunID:     "run-1",
		CreatedAt: time.Now().UTC(),
		Tasks: []resume.TaskSnapshot{{
			ID: "a", Fingerprint: "fp", Deps: []string{},
			Status: pipeline.StatusSucceeded, Reason: "ok",
			Output: pipeline.ExecOutput{Digest: "d"},
		}},
	}
	if err := s.Save(cp); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load("pipe")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.RunID != "run-1" || len(got.Tasks) != 1 || got.Tasks[0].Output.Digest != "d" {
		t.Fatalf("round trip mismatch: %+v", got)
	}
}

func TestStore_MissingIsCleanNil(t *testing.T) {
	s, _ := resume.OpenStore(t.TempDir())
	cp, err := s.Load("never-ran")
	if err != nil || cp != nil {
		t.Fatalf("missing checkpoint must be (nil,nil), got %v %v", cp, err)
	}
}

func TestStore_CorruptRejectedAndQuarantined(t *testing.T) {
	dir := t.TempDir()
	s, _ := resume.OpenStore(dir)
	cp := &resume.Checkpoint{Pipeline: "p", RunID: "r",
		Tasks: []resume.TaskSnapshot{{ID: "a", Status: pipeline.StatusSucceeded}}}
	if err := s.Save(cp); err != nil {
		t.Fatal(err)
	}
	// Tamper with the payload inside the envelope.
	files, _ := filepath.Glob(filepath.Join(s.Dir(), "*.json"))
	if len(files) != 1 {
		t.Fatalf("want 1 checkpoint file, got %v", files)
	}
	b, _ := os.ReadFile(files[0])
	b = []byte(strings.Replace(string(b), "succeeded", "succeedXX", 1))
	if err := os.WriteFile(files[0], b, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := s.Load("p")
	if pe := pipeline.AsError(err); pe == nil || pe.Code != pipeline.ErrStateCorrupt {
		t.Fatalf("want state_corrupt, got %v", err)
	}
	if _, err := os.Stat(files[0] + ".bad"); err != nil {
		t.Fatalf("corrupt checkpoint must be quarantined: %v", err)
	}
}

func TestStore_PipelineIsolation(t *testing.T) {
	s, _ := resume.OpenStore(t.TempDir())
	_ = s.Save(&resume.Checkpoint{Pipeline: "one", RunID: "r1"})
	_ = s.Save(&resume.Checkpoint{Pipeline: "two", RunID: "r2"})
	one, err := s.Load("one")
	if err != nil || one.RunID != "r1" {
		t.Fatalf("one=%v err=%v", one, err)
	}
	two, err := s.Load("two")
	if err != nil || two.RunID != "r2" {
		t.Fatalf("two=%v err=%v", two, err)
	}
}
