package resume

import (
	"crypto/rand"
	"encoding/hex"
	"sort"
	"time"

	"github.com/lisama569/build-pipeline-engine-go/pipeline"
	"github.com/lisama569/build-pipeline-engine-go/pkg/cache"
)

// BuildCheckpoint snapshots the current run's verdicts in task declaration
// order. Fingerprints are computed from the tasks as executed, so the next
// run can detect input/command/environment changes by comparison.
func BuildCheckpoint(pipelineName, runID string, tasks []*pipeline.Task,
	results map[string]pipeline.TaskResult, baseEnvHash string, now time.Time) *Checkpoint {
	cp := &Checkpoint{Pipeline: pipelineName, RunID: runID, CreatedAt: now}
	for _, t := range tasks {
		r, ok := results[t.ID]
		if !ok {
			// Never checkpoint an unfinished task; absence forces a rerun.
			continue
		}
		deps := append([]string(nil), t.Deps...)
		sort.Strings(deps)
		cp.Tasks = append(cp.Tasks, TaskSnapshot{
			ID:          t.ID,
			Fingerprint: cache.Key(cache.FingerprintFromTask(t, baseEnvHash)),
			Deps:        deps,
			Status:      r.Status,
			Attempts:    r.Attempts,
			Reason:      r.Reason,
			Detail:      r.Detail,
			Output:      r.Output,
			Cached:      r.Cached,
		})
	}
	return cp
}

// ToResult converts a reused snapshot into a task result. Reused is set and
// the task body was not executed (Attempts 0); cache-hit verdicts keep their
// original status so downstream readiness behaves identically.
func (s TaskSnapshot) ToResult() pipeline.TaskResult {
	return pipeline.TaskResult{
		TaskID:   s.ID,
		Status:   s.Status,
		Attempts: 0,
		Reason:   s.Reason,
		Detail:   "reused verdict from previous run",
		Output:   s.Output,
		Cached:   s.Cached,
		Reused:   true,
	}
}

// NewRunID returns an opaque, collision-resistant run identifier.
func NewRunID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failing is not recoverable; fall back to time-based id.
		return time.Now().Format("20060102T150405.000000000")
	}
	return time.Now().Format("20060102T150405") + "-" + hex.EncodeToString(b[:])
}
