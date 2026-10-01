// Package state persists the last-run snapshot that powers "resume": a later
// run carries over only successful tasks whose per-task input basis is
// unchanged, and re-executes everything else. Tasks with caching disabled
// (DisableCache) are never carried over - their side effects must really run
// every time - but their dependents stay reuse candidates, verified against
// the re-executed digest at runtime. A changed/renamed/removed task can
// never be silently substituted with an old result.
//
// Per-task input basis (computed in topological order):
//
//	basis(t) = sha256( fingerprint(t)
//	                   || for d in t.Deps (declared order):
//	                        depID(d) || basis(d) || outputDigest(d) )
//
// fingerprint(t) is the same content fingerprint the cache uses (task id,
// canonical command, environment hash, declared inputs). The dep chain folds
// every upstream input change into downstream bases, so a dependency rename,
// edge change, upstream command change or upstream output change invalidates
// the whole affected subtree - never a stale subtree.
//
// Persistence: one JSON snapshot per pipeline under <StateDir>/<safe name>/
// written temp-file + fsync + rename, guarded by a cross-process flock on the
// pipeline directory. A missing/corrupt snapshot simply means "no resume".
package state

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"

	"github.com/lisama569/build-pipeline-engine-go/pipeline"
	"github.com/lisama569/build-pipeline-engine-go/pkg/cache"
	"github.com/lisama569/build-pipeline-engine-go/pkg/filelock"
)

// snapshotVersion is the on-disk format version; unknown versions are treated
// as "no snapshot" rather than misread.
const snapshotVersion = 1

// Snapshot is the persisted conclusion of one pipeline run, sufficient to
// plan a resume without re-executing anything.
type Snapshot struct {
	Version int `json:"version"`
	// RunID identifies the run that produced this snapshot, surfaced in the
	// next run's "resumed from" metadata.
	RunID     string                  `json:"run_id,omitempty"`
	Pipeline  string                  `json:"pipeline"`
	StartedAt time.Time               `json:"started_at,omitempty"`
	Tasks     map[string]TaskSnapshot `json:"tasks"`
}

// TaskSnapshot is one task's recorded result plus its input basis.
type TaskSnapshot struct {
	Result    pipeline.TaskResult `json:"result"`
	BasisHash string              `json:"basis_hash"`
	// Fingerprint is the content fingerprint key for this task; kept for
	// diagnostics. It is also derivable but cheap to store.
	Fingerprint string `json:"fingerprint"`
}

// Store persists one latest snapshot per pipeline.
type Store struct {
	dir string
}

// Open opens (creating if needed) the state root. Empty dir disables state.
func Open(dir string) (*Store, error) {
	if dir == "" {
		return &Store{}, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, pipeline.NewError(pipeline.ErrConfigInvalid,
			"cannot create state dir %q: %v", dir, err)
	}
	return &Store{dir: dir}, nil
}

// Enabled reports whether snapshots are persisted.
func (s *Store) Enabled() bool { return s != nil && s.dir != "" }

func (s *Store) pipelineDir(pipelineName string) string {
	return filepath.Join(s.dir, SafeName(pipelineName))
}

// Lock acquires the per-pipeline exclusive lock. Callers must Release it.
func (s *Store) Lock(pipelineName string) (filelock.Lock, error) {
	if !s.Enabled() {
		return nil, nil
	}
	return filelock.Acquire(s.pipelineDir(pipelineName))
}

// Load reads the latest snapshot for pipelineName. A missing snapshot yields
// (nil, nil). A corrupt or unsupported snapshot also yields (nil, nil) so a
// damaged state file degrades to a clean run, never to a wrong reuse.
func (s *Store) Load(pipelineName string) (*Snapshot, error) {
	if !s.Enabled() {
		return nil, nil
	}
	data, err := os.ReadFile(filepath.Join(s.pipelineDir(pipelineName), "snapshot.json"))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		// Never build on an unreadable snapshot.
		return nil, nil
	}
	if snap.Version != snapshotVersion {
		return nil, nil
	}
	return &snap, nil
}

// Save atomically replaces the latest snapshot for pipelineName.
func (s *Store) Save(pipelineName string, snap *Snapshot) error {
	if !s.Enabled() || snap == nil {
		return nil
	}
	snap.Version = snapshotVersion
	raw, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("state encode: %w", err)
	}
	dir := s.pipelineDir(pipelineName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	final := filepath.Join(dir, "snapshot.json")
	tmp, err := os.CreateTemp(dir, ".snapshot-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpName, final); err != nil {
		cleanup()
		return err
	}
	return nil
}

var unsafeNameChar = regexp.MustCompile(`[^a-zA-Z0-9._-]`)

// SafeName maps an arbitrary pipeline name to a collision-resistant,
// path-safe directory component. Two distinct names cannot map to the same
// directory: a human-readable sanitized prefix is combined with a short hash
// of the original name.
func SafeName(name string) string {
	safe := unsafeNameChar.ReplaceAllString(name, "_")
	if safe == "" || safe == "." || safe == ".." {
		safe = "pipeline"
	}
	if len(safe) > 40 {
		safe = safe[:40]
	}
	h := sha256.Sum256([]byte(name))
	return safe + "-" + hex.EncodeToString(h[:])[:10]
}

// BasisHash computes the per-task input basis for t given the resolved
// dependency nodes, their already-computed bases and their recorded results,
// plus this engine's base environment hash. Must be invoked in topo order so
// every dep basis is available.
func BasisHash(t *pipeline.Task, deps []DepInput, baseEnvHash string) string {
	fp := cache.FingerprintFromTask(t, baseEnvHash)
	key := cache.KeyFor(fp)
	h := sha256.New()
	h.Write([]byte("basis"))
	h.Write([]byte{0})
	h.Write([]byte(key))
	for _, d := range deps {
		h.Write([]byte{0})
		h.Write([]byte("dep"))
		h.Write([]byte{0})
		h.Write([]byte(d.ID))
		h.Write([]byte{0})
		h.Write([]byte(d.BasisHash))
		h.Write([]byte{0})
		h.Write([]byte(d.OutputDigest))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// FingerprintKey returns the content fingerprint cache key for t; it is the
// first component of BasisHash and is reused for trace/report diagnostics.
func FingerprintKey(t *pipeline.Task, baseEnvHash string) string {
	return cache.KeyFor(cache.FingerprintFromTask(t, baseEnvHash))
}

// DepInput is one dependency's contribution to a basis computation.
type DepInput struct {
	ID           string
	BasisHash    string
	OutputDigest string
}

// Invalidation reasons recorded when a task cannot be carried over. They are
// stable machine-readable codes surfaced in the report and history diff.
const (
	// ReasonNewTask: task did not exist in the previous graph.
	ReasonNewTask = "new_task"
	// ReasonNeverSucceeded: previous conclusion was failed/skipped/canceled/...
	ReasonNeverSucceeded = "previous_not_success"
	// ReasonBasisChanged: this task's own fingerprint or an upstream input
	// changed (input/command/env/dependency edit, or upstream output change).
	ReasonBasisChanged = "input_changed"
	// ReasonCacheDisabled: the task opts out of caching (DisableCache), so it
	// has side effects that must really run on every run; resume never
	// carries it over even when its inputs are unchanged. Downstream tasks
	// are still reuse-candidates: the stored digest seeds their plan basis
	// and the scheduler verifies the re-executed digest at runtime.
	ReasonCacheDisabled = "cache_disabled"
)

// PlanEntry is the resume decision for one task.
type PlanEntry struct {
	Reuse       bool
	Reason      string // invalidation code when Reuse is false
	Snapshot    *TaskSnapshot
	BasisHash   string
	Fingerprint string
}

// Plan is the resume decision for a whole graph: task id -> entry.
type Plan struct {
	Entries map[string]PlanEntry
}

// Reusable reports whether a previously recorded status may be carried over.
func Reusable(st pipeline.TaskStatus) bool {
	return pipeline.IsSuccess(st)
}

// SortedTaskIDs returns the snapshot's task ids in sorted order (for
// deterministic tests/reports).
func (s *Snapshot) SortedTaskIDs() []string {
	ids := make([]string, 0, len(s.Tasks))
	for id := range s.Tasks {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Dir returns the state root directory (empty when disabled).
func (s *Store) Dir() string { return s.dir }
