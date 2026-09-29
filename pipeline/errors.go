package pipeline

import "fmt"

// ErrorCode is a stable, machine-readable error classification.
type ErrorCode string

const (
	// ErrGraphCycle: a dependency cycle was detected.
	ErrGraphCycle ErrorCode = "graph_cycle"
	// ErrGraphMissingDep: a dependency references an unknown task.
	ErrGraphMissingDep ErrorCode = "graph_missing_dep"
	// ErrGraphDuplicateTask: the same task id is declared more than once.
	ErrGraphDuplicateTask ErrorCode = "graph_duplicate_task"
	// ErrGraphEmpty: the pipeline contains no tasks.
	ErrGraphEmpty ErrorCode = "graph_empty"
	// ErrConfigInvalid: the configuration violates its bounds.
	ErrConfigInvalid ErrorCode = "config_invalid"
	// ErrTaskFailed: a task returned a permanent error.
	ErrTaskFailed ErrorCode = "task_failed"
	// ErrTaskTimeout: a task exceeded its deadline.
	ErrTaskTimeout ErrorCode = "task_timeout"
	// ErrTaskCanceled: execution was canceled while the task ran.
	ErrTaskCanceled ErrorCode = "task_canceled"
	// ErrRetryUnsafe: retries were requested for a non-idempotent task/side effect.
	ErrRetryUnsafe ErrorCode = "retry_unsafe"
	// ErrCacheCorrupt: a cache entry failed validation and was rejected.
	ErrCacheCorrupt ErrorCode = "cache_corrupt"
	// ErrCachePolluted: cached result conflicts with observed inputs.
	ErrCachePolluted ErrorCode = "cache_polluted"
	// ErrRunBudget: the run-wide time budget was exhausted.
	ErrRunBudget ErrorCode = "run_budget_exhausted"
	// ErrStateCorrupt: a resume checkpoint or history record failed
	// validation and was rejected (the run proceeds without that state).
	ErrStateCorrupt ErrorCode = "state_corrupt"
)

// Error is a classified pipeline error with stable, distinguishable reason.
type Error struct {
	Code   ErrorCode
	Detail string
	cause  error
}

func (e *Error) Error() string {
	if e.Detail != "" {
		return fmt.Sprintf("%s: %s", e.Code, e.Detail)
	}
	return string(e.Code)
}

// Unwrap exposes the underlying cause so errors.As/Is keep working through
// classification (e.g. a wrapped retry.Transient stays retryable).
func (e *Error) Unwrap() error { return e.cause }

// NewError builds a classified error.
func NewError(code ErrorCode, format string, args ...any) error {
	return &Error{Code: code, Detail: fmt.Sprintf(format, args...)}
}

// WrapError classifies an existing error without losing it.
func WrapError(code ErrorCode, cause error, format string, args ...any) error {
	return &Error{Code: code, Detail: fmt.Sprintf(format, args...), cause: cause}
}

// AsError extracts a *pipeline.Error from err, or nil.
func AsError(err error) *Error {
	if err == nil {
		return nil
	}
	if pe, ok := err.(*Error); ok {
		return pe
	}
	return nil
}
