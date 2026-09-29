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
	// StatusBudgetExhausted means the run-wide time budget was used up before
	// this task could complete (never launched, or canceled while running).
	// It is distinct from a task's own timeout (StatusTimedOut) and from
	// manual cancellation (StatusCanceled).
	StatusBudgetExhausted TaskStatus = "budget_exhausted"
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
	StartedAt time.Time `json:"started_at,omitempty"`
	EndedAt   time.Time `json:"ended_at,omitempty"`
	// Cached is true when the result was restored from cache.
	Cached bool `json:"cached,omitempty"`
	// Reused is true when the result was carried over from a previous run by
	// resume (inputs unchanged) rather than executed this run. It is orthogonal
	// to Cached: resume works even for tasks with caching disabled.
	Reused bool
	// ReuseInvalidation explains why a task was re-executed instead of reused;
	// empty for executed-for-the-first-time or normally cached tasks.
	ReuseInvalidation string
	// BasisHash is the per-task input fingerprint for the run that produced
	// this result; resume compares it with the current basis to decide reuse.
	BasisHash string
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
	// RunBudget bounds the wall-clock time of one whole run. Zero means no
	// run-level budget. Once the budget is used up no new task is launched;
	// in-flight tasks are canceled and converge bounded by ShutdownGrace, and
	// the run still concludes with a distinct "run_budget_exhausted" reason.
	// It is independent of (and reported separately from) per-task timeouts
	// and manual cancellation.
	RunBudget time.Duration
	// StateDir is where the last-run snapshot for resume is persisted, one
	// subdirectory per pipeline. Empty disables resume.
	StateDir string
	// HistoryDir is where recent run records are kept for comparison, one
	// subdirectory per pipeline. Empty disables history.
	HistoryDir string
	// HistoryRuns bounds how many recent run records are retained per pipeline
	// (0 defaults to 10; values above the hard cap are clamped).
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
