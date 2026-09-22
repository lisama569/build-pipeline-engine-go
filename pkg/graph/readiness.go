package graph

import (
	"sort"

	"github.com/lisama569/build-pipeline-engine-go/pipeline"
)

// terminal success-ish statuses that unblock downstream tasks.
func isSuccessStatus(st pipeline.TaskStatus) bool {
	switch st {
	case pipeline.StatusSucceeded, pipeline.StatusCacheHit:
		return true
	}
	return false
}

// terminal failure-ish statuses that force downstream tasks to be skipped.
func isFailureStatus(st pipeline.TaskStatus) bool {
	switch st {
	case pipeline.StatusFailed, pipeline.StatusTimedOut, pipeline.StatusCanceled, pipeline.StatusSkipped:
		return true
	}
	return false
}

// ReadyState tracks per-node terminal state for readiness decisions.
// It is bound to one graph; concurrent schedulers use distinct trackers.
// Operations are O(degree) per mark and O(nodes) per ready/blocked query;
// memory is O(nodes + edges).
type ReadyState struct {
	g         *Graph
	status    map[string]pipeline.TaskStatus
	remaining map[string]int    // unfinished deps per node
	failedDep map[string]string // one failing dep per blocked node (skip reason)
}

// NewReadyState creates an empty readiness tracker for g.
func NewReadyState(g *Graph) *ReadyState {
	rs := &ReadyState{
		g:         g,
		status:    make(map[string]pipeline.TaskStatus, len(g.nodes)),
		remaining: make(map[string]int, len(g.nodes)),
		failedDep: make(map[string]string, len(g.nodes)),
	}
	for _, n := range g.nodes {
		rs.remaining[n.Task.ID] = len(n.Deps)
	}
	return rs
}

// Mark records a terminal conclusion for a node and updates its direct
// dependents. A failure poisons every downstream node transitively: a node
// whose dependency failed/skipped is itself marked skip-eligible by the
// scheduler, and the poison propagates when it is marked skipped.
func (s *ReadyState) Mark(id string, status pipeline.TaskStatus) {
	if _, already := s.status[id]; already {
		return
	}
	s.status[id] = status
	n, ok := s.g.byID[id]
	if !ok {
		return
	}
	failure := isFailureStatus(status)
	for _, d := range n.Dependents {
		if failure {
			if _, set := s.failedDep[d.Task.ID]; !set {
				s.failedDep[d.Task.ID] = id
			}
		}
		s.remaining[d.Task.ID]--
	}
}

// Ready returns unmarked nodes whose deps all reached success status,
// ordered by declaration index for determinism.
func (s *ReadyState) Ready(g *Graph) []*Node {
	var out []*Node
	for _, n := range g.nodes {
		if _, done := s.status[n.Task.ID]; done {
			continue
		}
		if _, blocked := s.failedDep[n.Task.ID]; blocked {
			continue
		}
		if s.remaining[n.Task.ID] != 0 {
			continue
		}
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	return out
}

// BlockedByFailed returns unmarked nodes that now have a failed/skipped
// dependency and must be skipped. Ordered by declaration index.
func (s *ReadyState) BlockedByFailed(g *Graph) []*Node {
	var out []*Node
	for _, n := range g.nodes {
		if _, done := s.status[n.Task.ID]; done {
			continue
		}
		if _, blocked := s.failedDep[n.Task.ID]; blocked {
			out = append(out, n)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	return out
}

// Status reports the recorded status of a node and whether it is terminal.
func (s *ReadyState) Status(id string) (pipeline.TaskStatus, bool) {
	st, ok := s.status[id]
	return st, ok
}

// FailedDep returns the id of the dependency that blocks id, if any.
func (s *ReadyState) FailedDep(id string) string { return s.failedDep[id] }
