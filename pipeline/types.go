package pipeline

import (
	"context"
	"time"
)

// TaskStatus is the terminal (or current) state of a task in a run.
type TaskStatus string

const (
	StatusPending   TaskStatus = "pending"
	StatusReady     TaskStatus = "ready"
	StatusRunning   TaskStatus = "running"
	StatusSucceeded TaskStatus = "succeeded"
	// StatusFromCache means the task produced no new side effects because a
	// valid cache entry was reused.
	StatusFromCache TaskStatus = "from_cache"
	StatusFailed    TaskStatus = "failed"
	// StatusTimedOut is distinct from StatusFailed: a timed-out task is never
	// retried automatically and its reason is always ReasonTimeout.
	StatusTimedOut TaskStatus = "timed_out"
	// StatusSkipped covers downstream tasks whose upstream failed/timed out
	// (reason ReasonUpstream) and tasks not reached because the run was
	// canceled (reason ReasonCanceled).
	StatusSkipped TaskStatus = "skipped"
)

// TaskDef is the declarative definition of one pipeline node.
type TaskDef struct {
	// Name uniquely identifies the task within a pipeline.
	Name string
	// Deps are names of tasks that must succeed (or hit cache) before this
	// task becomes ready.
	Deps []string

	// Run executes the task body. It must honor ctx cancellation promptly.
	Run func(ctx context.Context) (*TaskOutput, error)

	// Cacheable enables cache lookup/store for this task. When false the task
	// always executes.
	Cacheable bool
	// Inputs are repository-relative file paths whose contents are part of
	// the cache key. Missing input files are treated as the empty content and
	// recorded as such, never as a silent zero hash.
	Inputs []string

	// Timeout bounds a single attempt. Zero means the run-level default. A
	// timeout is terminal (StatusTimedOut) and is never retried.
	Timeout time.Duration
	// Retries is the number of additional attempts after the first failure.
	// Only failures from an Idempotent task are retried.
	Retries int
	// Idempotent declares whether re-running the task after a failed attempt
	// is safe. Retry of a non-idempotent task is refused and reported with
	// ReasonNonIdempotentRetry (the task is marked failed on the first
	// failure without a second attempt).
	Idempotent bool

	// actionKey is an unexported stable identity of the action (set by spec
	// conversion) folded into the cache fingerprint. Function-backed tasks
	// leave it empty and get a name-derived constant identity.
	actionKey string
	// decode reconstructs the in-memory Value from a cached payload. Used by
	// command actions so cross-process cache hits still return typed output.
	decode func(payload []byte) any
}

// TaskOutput is what a task body returns on success. Payload is the
// canonical byte content that gets persisted/restored by the cache; Value
// is the in-memory form handed back to callers. Artifacts are relative file
// paths informational only.
type TaskOutput struct {
	Value     any
	Payload   []byte
	Artifacts []string
}

// TaskResult is the per-task outcome recorded in a Report.
type TaskResult struct {
	Name       string     `json:"name"`
	Status     TaskStatus `json:"status"`
	Reason     Reason     `json:"reason,omitempty"`
	Message    string     `json:"message,omitempty"`
	Attempts   int        `json:"attempts"`
	DurationMS int64      `json:"duration_ms"`
	CacheKey   string     `json:"cache_key,omitempty"`
	CacheHit   bool       `json:"cache_hit"`
	// DependsOn mirrors the declared dependency list for readability.
	DependsOn []string `json:"depends_on"`
	// Uncertainty records non-deterministic inputs explicitly (e.g. cache
	// corruption recovered by re-execution, timing-dependent scheduling).
	Uncertainty []string `json:"uncertainty,omitempty"`
}

// Config controls engine-wide behavior. All numeric fields are bounded by
// ValidateConfig.
type Config struct {
	// Parallelism is the maximum number of concurrently executing tasks.
	Parallelism int
	// DefaultRetries caps per-task retry counts.
	DefaultRetries int
	// MaxRetries is the hard upper bound accepted in a TaskDef.
	MaxRetries int
	// DefaultTimeout applies to tasks without an explicit timeout.
	DefaultTimeout time.Duration
	// MaxTimeout is the hard upper bound for any task timeout.
	MaxTimeout time.Duration
	// RetryBackoff is the base delay between attempts; attempt n waits
	// RetryBackoff * 2^(n-1), capped by MaxRetryBackoff.
	RetryBackoff time.Duration
	// MaxRetryBackoff bounds exponential backoff.
	MaxRetryBackoff time.Duration
	// ShutdownGrace bounds how long cancellation waits for in-flight tasks
	// to stop after their contexts are canceled.
	ShutdownGrace time.Duration
	// CacheDir is where task cache entries live. Empty disables persistence
	// (in-memory cache only).
	CacheDir string
	// RepoRoot is the root for resolving task Inputs/Artifacts and for
	// running command actions.
	RepoRoot string
	// EnvPassthrough lists environment variable names included in cache keys.
	// Nothing else from the ambient environment affects the key.
	EnvPassthrough []string
	// Logf, when non-nil, receives one structured line per scheduling
	// decision (ready determination, dispatch, completion, skip, cancel).
	// Tests wire it to a buffer to assert scheduling order; it never
	// influences results. It may be invoked concurrently from multiple
	// pipelines running under RunMany, so implementations must be safe for
	// concurrent use.
	Logf func(format string, args ...any)
}

// Report is the structured, machine-readable result of one pipeline run.
// It contains no wall-clock timestamps: identical inputs always produce
// byte-identical reports across repeated runs.
type Report struct {
	Pipeline string            `json:"pipeline"`
	Status   TaskStatus        `json:"status"`
	Reason   Reason            `json:"reason,omitempty"`
	Tasks    []*TaskResult     `json:"tasks"`
	Order    []string          `json:"schedule_order"`
	Summary  map[string]int    `json:"summary"`
	Meta     map[string]string `json:"meta,omitempty"`
}
