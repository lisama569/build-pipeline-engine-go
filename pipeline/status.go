package pipeline

// IsSuccess reports whether a status unblocks downstream tasks.
func IsSuccess(st TaskStatus) bool {
	return st == StatusSucceeded || st == StatusCacheHit
}

// IsTerminal reports whether a status will not change anymore.
func IsTerminal(st TaskStatus) bool {
	switch st {
	case StatusSucceeded, StatusCacheHit, StatusFailed,
		StatusSkipped, StatusCanceled, StatusTimedOut, StatusBudgetExhausted:
		return true
	}
	return false
}

// Run-end reasons (used in the report).
const (
	// EndReasonCompleted: the graph reached a terminal conclusion normally.
	EndReasonCompleted = "completed"
	// EndReasonCanceled: the caller canceled the run (manual interrupt).
	EndReasonCanceled = "canceled"
	// EndReasonBudgetExhausted: the run-wide wall-clock budget was used up.
	EndReasonBudgetExhausted = "run_budget_exhausted"
)

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
