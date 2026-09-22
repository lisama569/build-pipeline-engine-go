package pipeline

import "encoding/json"

// buildReport assembles the final report from per-task results and the
// deterministic dispatch order. The report contains no wall-clock
// timestamps, so identical inputs serialize identically across repeated
// runs. Per-task durations are measurements and are emitted in a separate
// non-normative field family (they never affect status/reason).
func buildReport(g *Graph, results map[string]*TaskResult, order []string) *Report {
	tasks := make([]*TaskResult, 0, len(g.order))
	summary := map[string]int{}
	var overall TaskStatus = StatusSucceeded
	var overallReason Reason
	var reasons []string

	for _, name := range g.order {
		r := results[name]
		if r == nil {
			// Defensive: a node with no result is an engine bug, never a
			// silent success.
			r = &TaskResult{
				Name: name, Status: StatusFailed, Reason: ReasonUnknown,
				Message:   "internal error: task produced no result",
				DependsOn: append([]string(nil), g.deps[name]...),
			}
		}
		tasks = append(tasks, r)
		summary[string(r.Status)]++

		switch r.Status {
		case StatusFailed:
			if overall != StatusTimedOut {
				overall = StatusFailed
				overallReason = r.Reason
			}
		case StatusTimedOut:
			overall = StatusFailed
			overallReason = ReasonTimeout
		case StatusSkipped:
			if overall == StatusSucceeded {
				overall = StatusFailed
				overallReason = r.Reason
			}
		}
		if r.Reason != "" && (r.Status == StatusFailed || r.Status == StatusTimedOut ||
			r.Status == StatusSkipped) {
			reasons = appendUniq(reasons, string(r.Reason))
		}
	}

	dispatch := append([]string(nil), order...)
	meta := map[string]string{
		"dispatch_policy": "lexicographically smallest ready task per free slot",
		"nondeterminism":  "within a concurrent wave, completion interleaving is timing-dependent; statuses and reasons are not",
		"reasons":         joinComma(reasons),
	}

	return &Report{
		Pipeline: g.name,
		Status:   overall,
		Reason:   overallReason,
		Tasks:    tasks,
		Order:    dispatch,
		Summary:  summary,
		Meta:     meta,
	}
}

// JSON renders the report deterministically (fixed key ordering, no
// timestamps) for machine consumption or snapshot comparison.
func (r *Report) JSON() ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}

func appendUniq(s []string, v string) []string {
	for _, x := range s {
		if x == v {
			return s
		}
	}
	return append(s, v)
}

func joinComma(s []string) string {
	out := ""
	for i, v := range s {
		if i > 0 {
			out += ","
		}
		out += v
	}
	return out
}
