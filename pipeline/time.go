package pipeline

import "time"

// executionPlan is the per-task resolved runtime configuration after
// applying engine defaults and hard bounds.
type executionPlan struct {
	timeout    time.Duration
	retries    int
	backoff    time.Duration
	maxBackoff time.Duration
}

// nowMonotonic returns a clock reading used only for relative duration
// measurement. Durations are measurement data, never absolute timestamps,
// so they do not break report determinism.
func nowMonotonic() time.Time { return time.Now() }

// millisSince returns elapsed milliseconds since start.
func millisSince(start time.Time) int64 {
	return time.Since(start).Milliseconds()
}
