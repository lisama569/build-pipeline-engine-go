// Package history keeps the last N run records per pipeline for comparison:
// which tasks were reused, which re-executed and why, and which task statuses
// changed between two runs.
//
// Storage layout (<HistoryDir>/<safe pipeline>/):
//
//	<timestamp>-<shortid>.json   one immutable record per run
//
// Retention is bounded: after every append the directory is pruned so at most
// HistoryRuns records remain (newest kept). Appends, listing and pruning are
// serialized by a cross-process flock on the pipeline directory (shared with
// the state store when both use the same root), so concurrent pipelines - or
// concurrent runs of the same pipeline - never overwrite or interleave files.
package history

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/lisama569/build-pipeline-engine-go/pkg/filelock"
	"github.com/lisama569/build-pipeline-engine-go/pkg/state"
)

const (
	// DefaultRuns is used when HistoryRuns is unset.
	DefaultRuns = 10
	// HardMaxRuns caps retention so configuration cannot request unbounded
	// disk use.
	HardMaxRuns = 100
)

// Record is one retained run record.
type Record struct {
	ID         string       `json:"id"`
	Pipeline   string       `json:"pipeline"`
	StartedAt  time.Time    `json:"started_at"`
	Conclusion string       `json:"conclusion"`
	Status     string       `json:"status"`
	Tasks      []TaskRecord `json:"tasks"`
}

// TaskRecord is one task's outcome in a record.
type TaskRecord struct {
	ID                string `json:"id"`
	Status            string `json:"status"`
	Reused            bool   `json:"reused"`
	Cached            bool   `json:"cached"`
	Reason            string `json:"reason"`
	OutputDigest      string `json:"output_digest"`
	ReuseInvalidation string `json:"reuse_invalidation,omitempty"`
}

// Store retains recent records per pipeline with a bounded count.
type Store struct {
	dir string
	max int
}

// Open opens (creating if needed) the history root and clamps retention.
// Empty dir disables history.
func Open(dir string, maxRuns int) (*Store, error) {
	if dir == "" {
		return &Store{}, nil
	}
	if maxRuns <= 0 {
		maxRuns = DefaultRuns
	}
	if maxRuns > HardMaxRuns {
		maxRuns = HardMaxRuns
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("history: create %q: %w", dir, err)
	}
	return &Store{dir: dir, max: maxRuns}, nil
}

// Enabled reports whether history is retained.
func (s *Store) Enabled() bool { return s != nil && s.dir != "" }

// MaxRuns reports the clamped retention bound.
func (s *Store) MaxRuns() int {
	if s == nil || s.max <= 0 {
		return 0
	}
	return s.max
}

func (s *Store) pipelineDir(pipelineName string) string {
	return filepath.Join(s.dir, state.SafeName(pipelineName))
}

// recordFile returns the full path of the file holding rec.
func (s *Store) recordFile(rec *Record) string {
	// File name is sortable by time first, so lexicographic listing is
	// chronological; the unique id suffix prevents same-timestamp collisions.
	return filepath.Join(s.pipelineDir(rec.Pipeline),
		fmt.Sprintf("%s-%s.json", rec.StartedAt.UTC().Format("20060102T150405.000000000"), rec.ID))
}

// Lock acquires the per-pipeline exclusive history lock.
func (s *Store) Lock(pipelineName string) (filelock.Lock, error) {
	if !s.Enabled() {
		return nil, nil
	}
	return filelock.Acquire(s.pipelineDir(pipelineName))
}

// Append atomically writes rec, prunes beyond the retention bound, and
// returns the previous (most recent prior) retained record for diffing, or
// nil when this is the first retained run. Callers are expected to hold
// Lock for the pipeline across Load/Append-style sequences.
func (s *Store) Append(rec *Record) (*Record, error) {
	if !s.Enabled() || rec == nil {
		return nil, nil
	}
	if rec.StartedAt.IsZero() {
		rec.StartedAt = time.Now()
	}
	sortTasks(rec.Tasks)

	existing, err := s.readAll(rec.Pipeline)
	if err != nil {
		return nil, err
	}
	var prev *Record
	if len(existing) > 0 {
		if raw, err := os.ReadFile(existing[len(existing)-1]); err == nil {
			var p Record
			if json.Unmarshal(raw, &p) == nil {
				prev = &p
			}
		}
	}

	dir := s.pipelineDir(rec.Pipeline)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	raw, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("history encode: %w", err)
	}
	final := s.recordFile(rec)
	tmp, err := os.CreateTemp(dir, ".record-*")
	if err != nil {
		return nil, err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		cleanup()
		return nil, err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		cleanup()
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return nil, err
	}
	if err := os.Rename(tmpName, final); err != nil {
		cleanup()
		return nil, err
	}

	if err := s.prune(rec.Pipeline, append(existing, final)); err != nil {
		// Pruning failure must not lose or corrupt the just-written record.
		return prev, err
	}
	return prev, nil
}

// readAll loads every well-formed record for pipelineName in chronological
// order. Unreadable individual files are ignored (never block a new run).
func (s *Store) readAll(pipelineName string) ([]string, error) {
	dir := s.pipelineDir(pipelineName)
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var files []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		files = append(files, filepath.Join(dir, e.Name()))
	}
	sort.Strings(files)
	return files, nil
}

// prune keeps at most s.max newest files; everything older is removed.
func (s *Store) prune(pipelineName string, files []string) error {
	if len(files) <= s.max {
		return nil
	}
	sort.Strings(files)
	drop := files[:len(files)-s.max]
	var firstErr error
	for _, f := range drop {
		if err := os.Remove(f); err != nil && !os.IsNotExist(err) && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// List returns retained records newest-first.
func (s *Store) List(pipelineName string) ([]*Record, error) {
	files, err := s.readAll(pipelineName)
	if err != nil {
		return nil, err
	}
	out := make([]*Record, 0, len(files))
	for i := len(files) - 1; i >= 0; i-- {
		raw, err := os.ReadFile(files[i])
		if err != nil {
			continue
		}
		var rec Record
		if json.Unmarshal(raw, &rec) != nil {
			continue
		}
		out = append(out, &rec)
	}
	return out, nil
}

func sortTasks(tasks []TaskRecord) {
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].ID < tasks[j].ID })
}

// Dir returns the history root directory (empty when disabled).
func (s *Store) Dir() string { return s.dir }
