// Package history retains a bounded, comparable record of recent pipeline
// runs. Records live per pipeline under the engine state directory, are
// tamper-evident (checksummed envelopes, atomic writes) and are written to
// unique run-id files so concurrent pipelines never overwrite each other.
//
// Retention is configurable with a hard upper bound: after a record is
// appended the store prunes that pipeline's directory back to the newest N.
//
// Records support Compare, which answers between two runs: which verdicts
// were reused, which tasks re-executed and why (input change, previous
// failure, ...), and whose status changed.
package history

import (
	"time"

	"github.com/lisama569/build-pipeline-engine-go/pipeline"
)

// TaskRecord is one task's slice of a stored run record.
type TaskRecord struct {
	ID           string              `json:"id"`
	Status       pipeline.TaskStatus `json:"status"`
	Reason       string              `json:"reason"`
	Attempts     int                 `json:"attempts"`
	Cached       bool                `json:"cached"`
	Reused       bool                `json:"reused"`
	RerunReason  string              `json:"rerun_reason,omitempty"`
	OutputDigest string              `json:"output_digest,omitempty"`
}

// Record is the retained summary of one run.
type Record struct {
	Pipeline    string    `json:"pipeline"`
	RunID       string    `json:"run_id"`
	ParentRunID string    `json:"parent_run_id,omitempty"`
	StartedAt   time.Time `json:"started_at"`
	EndedAt     time.Time `json:"ended_at"`
	// StopReason distinguishes natural completion, manual cancel, fail-fast
	// and run-budget exhaustion.
	StopReason pipeline.StopReason `json:"stop_reason"`
	// RunStatus is the overall report status.
	RunStatus  string        `json:"run_status"`
	Conclusion string        `json:"conclusion"`
	Duration   time.Duration `json:"duration"`

	ReusedCount int `json:"reused_count"`
	ReranCount  int `json:"reran_count"`
	CachedCount int `json:"cached_count"`

	Tasks []TaskRecord `json:"tasks"`
}

// ReranTask identifies a task that re-executed and why.
type ReranTask struct {
	ID     string `json:"id"`
	Reason string `json:"reason"`
}

// StatusChange is a task whose normalized conclusion status changed.
type StatusChange struct {
	ID     string              `json:"id"`
	Before pipeline.TaskStatus `json:"before"`
	After  pipeline.TaskStatus `json:"after"`
}

// Diff compares two run records (older first, newer second).
type Diff struct {
	OlderRunID string `json:"older_run_id"`
	NewerRunID string `json:"newer_run_id"`
	// Reused: verdicts carried over in the newer run.
	Reused []string `json:"reused"`
	// New: tasks present only in the newer run.
	New []string `json:"new"`
	// Removed: tasks present only in the older run.
	Removed []string `json:"removed"`
	// Reran: tasks known in both that re-executed in the newer run, with
	// the stable rerun reason.
	Reran []ReranTask `json:"reran"`
	// StatusChanged: tasks present in both whose normalized status differs.
	StatusChanged []StatusChange `json:"status_changed"`
}
