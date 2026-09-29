// Package resume implements "continue from the last run": a tamper-evident
// per-pipeline checkpoint of the previous run's task verdicts, plus the pure
// decision logic that decides, for the current graph, which successful
// verdicts stay valid and which tasks must re-execute.
//
// A verdict is reusable only when ALL of the following hold:
//   - the task existed in the previous run and ended in a success status;
//   - its fingerprint (task id, command, env fingerprint, CacheKeyInputs) is
//     unchanged, so no input moved underneath it;
//   - its declared dependency list is unchanged (graph restructure safe);
//   - every (transitive) dependency is itself reusable;
//   - the task did not opt out of caching.
//
// Anything else re-executes: previously failed/timed-out/skipped/canceled
// tasks, new tasks, tasks with changed inputs or dependencies, cache-disabled
// tasks, and the closure of tasks that depend on any re-executed task.
package resume

import (
	"time"

	"github.com/lisama569/build-pipeline-engine-go/pipeline"
)

// Stable machine-readable reasons for re-executing a task in a resuming run.
const (
	// RerunNewTask: the task did not exist in the previous run.
	RerunNewTask = "new_task"
	// RerunInputChanged: the task fingerprint (inputs/command/env) differs.
	RerunInputChanged = "input_changed"
	// RerunDepsChanged: the declared dependency list was restructured.
	RerunDepsChanged = "deps_changed"
	// RerunPrevFailed: the previous verdict was failed.
	RerunPrevFailed = "prev_failed"
	// RerunPrevTimedOut: the previous verdict was timed_out.
	RerunPrevTimedOut = "prev_timed_out"
	// RerunPrevSkipped: the previous verdict was skipped.
	RerunPrevSkipped = "prev_skipped"
	// RerunPrevCanceled: the previous verdict was canceled (or unfinished).
	RerunPrevCanceled = "prev_canceled"
	// RerunCacheDisabled: the task opted out of caching and always executes.
	RerunCacheDisabled = "cache_disabled"
	// RerunDepReran: a dependency had to re-execute, so the closure reruns.
	RerunDepReran = "dep_reran"
)

// TaskSnapshot is one task's persisted verdict.
type TaskSnapshot struct {
	// ID is the task id.
	ID string `json:"id"`
	// Fingerprint is the content key at the time of the verdict
	// (cache.TaskKey over task + environment).
	Fingerprint string `json:"fingerprint"`
	// Deps is the sorted list of declared dependency ids.
	Deps []string `json:"deps"`
	// Status/Attempts/Reason/Output are the recorded conclusion.
	Status   pipeline.TaskStatus `json:"status"`
	Attempts int                 `json:"attempts"`
	Reason   string              `json:"reason"`
	Detail   string              `json:"detail,omitempty"`
	Output   pipeline.ExecOutput `json:"output"`
	// Cached records whether the original verdict was itself a cache hit.
	Cached bool `json:"cached"`
}

// Checkpoint is the resumable conclusion of one run of one pipeline.
type Checkpoint struct {
	// Pipeline is the pipeline name the checkpoint belongs to.
	Pipeline string `json:"pipeline"`
	// RunID identifies the run that produced the checkpoint.
	RunID string `json:"run_id"`
	// CreatedAt is wall-clock metadata, never used for reuse decisions.
	CreatedAt time.Time `json:"created_at"`
	// Tasks holds one snapshot per task, in declaration order.
	Tasks []TaskSnapshot `json:"tasks"`
}

// Decision is the resume verdict for one task of the current graph.
type Decision struct {
	TaskID string
	// Reuse is true when the previous verdict is carried over.
	Reuse bool
	// Snapshot is the previous verdict (zero value when Reuse is false).
	Snapshot TaskSnapshot
	// RerunReason is the stable code when Reuse is false and the task was
	// already known from the previous run; empty for first-ever tasks.
	RerunReason string
}

// Plan is the per-task resume decision set for one run.
type Plan struct {
	decisions map[string]Decision
	order     []string
}

// Empty reports whether no task can be reused.
func (p *Plan) Empty() bool {
	if p == nil {
		return true
	}
	return p.ReusedCount() == 0
}
