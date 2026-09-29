// Package pipeline defines the public types for the local build pipeline
// orchestration engine: tasks, dependency graph, configuration and results.
package pipeline

import (
	"context"
	"time"
)

// TaskStatus is the final/observable state of a task within a run.
type TaskStatus string

const (
	// StatusPending means the task has not yet been scheduled.
	StatusPending TaskStatus = "pending"
	// StatusReady means the task is eligible but waiting for a slot.
	StatusReady TaskStatus = "ready"
	// StatusRunning means the task is currently executing.
	StatusRunning TaskStatus = "running"
	// StatusSucceeded means the task finished successfully.
	StatusSucceeded TaskStatus = "succeeded"
	// StatusFailed means the task failed permanently (or retries exhausted).
	StatusFailed TaskStatus = "failed"
	// StatusSkipped means an upstream dependency failed/was skipped.
	StatusSkipped TaskStatus = "skipped"
	// StatusCanceled means the run context was canceled.
	StatusCanceled TaskStatus = "canceled"
	// StatusTimedOut means the task exceeded its deadline; it is NOT retried.
	StatusTimedOut TaskStatus = "timed_out"
	// StatusCacheHit means the task output was restored from the cache.
	StatusCacheHit TaskStatus = "cache_hit"
)

// CommandSpec describes a local build/verification command to execute.
type CommandSpec struct {
	// Name identifies the command kind, e.g. "go-build" or "go-test".
	Name string
	// Args are the command arguments.
	Args []string
	// WorkDir is the working directory; empty means pipeline root.
	WorkDir string
	// Env is the task-specific environment overlay.
	Env map[string]string
}

// Task is a single node in the build pipeline.
type Task struct {
	// ID is the unique task identifier within a pipeline.
	ID string
	// Deps are the IDs this task depends on.
	Deps []string
	// Cmd optionally describes a local command executor.
	Cmd *CommandSpec
	// Run is an optional in-process executor (used instead of Cmd).
	Run ExecFunc
	// CacheKeyInputs are extra values folded into the cache key
	// (source hashes, tool versions, environment, ...).
	CacheKeyInputs map[string]string
	// Timeout bounds a single attempt; zero means Config.DefaultTaskTimeout.
	Timeout time.Duration
	// MaxAttempts overrides Config.MaxAttempts; must be >= 1.
	MaxAttempts int
	// Idempotent declares whether re-execution after a failed attempt is safe.
	// Non-idempotent tasks are executed at most once per run.
	Idempotent bool
	// DisableCache turns caching off for this task.
	DisableCache bool
}

// ExecFunc is an in-process task implementation.
// It must honor ctx cancellation promptly.
type ExecFunc func(ctx context.Context, in ExecInput) (ExecOutput, error)

// ExecInput is handed to a task executor.
type ExecInput struct {
	// TaskID is the executing task's id.
	TaskID string
	// Attempt is the 1-based attempt number.
	Attempt int
	// DepResults contains the results of direct dependencies.
	DepResults map[string]TaskResult
}

// ExecOutput is what an executor returns on success.
type ExecOutput struct {
	// Artifacts are paths or logical names produced (for reporting).
	Artifacts []string `json:"artifacts,omitempty"`
	// Stdout/Stderr are captured tail output for the report.
	Stdout string `json:"stdout,omitempty"`
	Stderr string `json:"stderr,omitempty"`
	// Digest identifies the produced result for caching.
	Digest string `json:"digest,omitempty"`
}

// TaskResult is the recorded conclusion of one task in one run.
type TaskResult struct {
	TaskID   string
	Status   TaskStatus
	Attempts int
	// Reason is a stable machine-readable explanation code.
	Reason string
	// Detail carries human readable, deterministic context.
	Detail string
	Output ExecOutput
	// StartedAt/EndedAt are recorded but excluded from conclusion hashing.
	StartedAt time.Time
	EndedAt   time.Time
	// Cached is true when the result was restored from cache.
	Cached bool
	// Reused is true when the verdict was carried over from a previous run's
	// resume checkpoint (task body not executed this run).
	Reused bool
	// RerunReason is set when the task was executed in a resuming run although
	// the previous run already knew it (e.g. "prev_failed", "input_changed");
	// empty for first-ever runs and clean (non-resuming) runs.
	RerunReason string
}

// QueuePolicy selects the queuing strategy when all worker slots are busy.
type QueuePolicy string

const (
	// QueueFIFO schedules ready tasks in declaration order.
	QueueFIFO QueuePolicy = "fifo"
	// QueuePriority schedules ready tasks in a deterministic priority order:
	// more remaining downstream dependents first, then declaration index.
	QueuePriority QueuePolicy = "priority"
)

// Config controls pipeline validation and execution.
type Config struct {
	// MaxParallelism bounds concurrently running tasks (>= 1).
	MaxParallelism int
	// MaxAttempts bounds per-task attempts (>= 1).
	MaxAttempts int
	// DefaultTaskTimeout bounds a single attempt when Task.Timeout is zero.
	DefaultTaskTimeout time.Duration
	// ShutdownGrace bounds cancellation convergence time.
	ShutdownGrace time.Duration
	// QueuePolicy selects ready-queue ordering.
	QueuePolicy QueuePolicy
	// CacheDir is where cached results live; empty disables caching.
	CacheDir string
	// RootDir is the working directory for relative command paths.
	RootDir string
	// FailFast stops launching new tasks after the first failure.
	FailFast bool
	// RetryBackoffBase / Cap bound exponential backoff between attempts.
	RetryBackoffBase time.Duration
	RetryBackoffCap  time.Duration

	// RunBudget bounds the total wall-clock duration of one run. Once it has
	// elapsed no new task is dispatched, running work is canceled and the run
	// converges bounded by ShutdownGrace with a distinguishable stop reason
	// (budget_exhausted). Zero means no run-level budget (task timeouts and the
	// parent context still apply).
	RunBudget time.Duration
	// StateDir is where per-pipeline resume checkpoints and run-history
	// records live. Empty disables both resume and history.
	StateDir string
	// Resume makes a run reuse the still-valid successful verdicts of the
	// previous run stored in StateDir; failed/skipped/canceled, changed-input,
	// new and dependency-restructured tasks are re-executed. A checkpoint is
	// written whenever StateDir is set, so a later run can always resume.
	Resume bool
	// HistoryRuns retains at most that many recent run records per pipeline;
	// zero disables history. Values above the engine hard cap are clamped.
	HistoryRuns int
}

// DefaultConfig returns conservative local defaults.
func DefaultConfig() Config {
	return Config{}
}

// Pipeline is a named, ordered task list (order = declaration index).
type Pipeline struct {
	Name  string
	Tasks []*Task
}
