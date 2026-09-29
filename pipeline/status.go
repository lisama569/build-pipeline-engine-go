package pipeline

// IsSuccess reports whether a status unblocks downstream tasks.
func IsSuccess(st TaskStatus) bool {
	return st == StatusSucceeded || st == StatusCacheHit
}

// IsTerminal reports whether a status will not change anymore.
func IsTerminal(st TaskStatus) bool {
	switch st {
	case StatusSucceeded, StatusCacheHit, StatusFailed,
		StatusSkipped, StatusCanceled, StatusTimedOut:
		return true
	}
	return false
}

// RunSucceeded reports whether the run as a whole succeeded: every task
// reached a success status.
func RunSucceeded(results map[string]TaskResult) bool {
	for _, r := range results {
		if !IsSuccess(r.Status) {
			return false
		}
	}
	return len(results) > 0
}

// StopReason explains why a run ended, distinguishing the run-wide budget
// from a manual cancellation, a fail-fast reaction and natural completion.
type StopReason string

const (
	// StopCompleted: the graph ran to its natural conclusion (success or a
	// task failure with downstream skips).
	StopCompleted StopReason = "completed"
	// StopCanceled: the caller-supplied context ended (manual interrupt).
	StopCanceled StopReason = "canceled"
	// StopFailFast: running work was stopped after the first failure.
	StopFailFast StopReason = "fail_fast"
	// StopBudgetExhausted: the run-wide time budget elapsed; no new tasks
	// were dispatched after it and running work was canceled within grace.
	StopBudgetExhausted StopReason = "budget_exhausted"
)
