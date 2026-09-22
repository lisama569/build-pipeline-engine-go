package pipeline

import "fmt"

// Reason classifies a pipeline definition or execution failure into a
// distinct, machine-readable category. Every rejected graph or failed task
// carries exactly one reason so that callers never have to infer the cause
// from free-form log text.
type Reason string

const (
	// ReasonUnknown is the zero value and must never be reported knowingly.
	ReasonUnknown Reason = "unknown"

	// Definition-time reasons (Parse).
	ReasonDuplicateTask Reason = "duplicate_task"
	ReasonMissingDep    Reason = "missing_dependency"
	ReasonCycle         Reason = "cycle_detected"
	ReasonInvalidConfig Reason = "invalid_config"
	ReasonEmptyGraph    Reason = "empty_graph"

	// Execution-time reasons.
	ReasonFailed             Reason = "task_failed"
	ReasonUpstream           Reason = "upstream_failed"
	ReasonCanceled           Reason = "canceled"
	ReasonTimeout            Reason = "timed_out"
	ReasonExecError          Reason = "execution_error"
	ReasonNonIdempotentRetry Reason = "non_idempotent_retry_blocked"
)

// Error is the single error type used across the package. It pairs a
// machine-readable Reason with a human-readable message and, when relevant,
// the names of the tasks involved.
type Error struct {
	Reason  Reason
	Message string
	Tasks   []string
}

func (e *Error) Error() string {
	if len(e.Tasks) > 0 {
		return fmt.Sprintf("%s: %s (tasks: %v)", e.Reason, e.Message, e.Tasks)
	}
	return fmt.Sprintf("%s: %s", e.Reason, e.Message)
}

// newErr builds an *Error.
func newErr(r Reason, msg string, tasks ...string) *Error {
	return &Error{Reason: r, Message: msg, Tasks: tasks}
}

// AsError extracts an *Error from err, returning nil if err is not one.
func AsError(err error) *Error {
	if e, ok := err.(*Error); ok {
		return e
	}
	return nil
}
