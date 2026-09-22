package graph

import (
	"strings"
	"testing"

	"github.com/lisama569/build-pipeline-engine-go/pipeline"
)

func mkTask(id string, deps ...string) *pipeline.Task {
	return &pipeline.Task{ID: id, Deps: deps}
}

func wantCode(t *testing.T, err error, code pipeline.ErrorCode) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error %s, got nil", code)
	}
	pe := pipeline.AsError(err)
	if pe == nil {
		t.Fatalf("expected *pipeline.Error, got %T: %v", err, err)
	}
	if pe.Code != code {
		t.Fatalf("expected code %s, got %s (%v)", code, pe.Code, err)
	}
	t.Logf("rejected as [%s]: %s", code, pe.Detail)
}

func TestNew_Empty(t *testing.T) {
	_, err := New(nil)
	wantCode(t, err, pipeline.ErrGraphEmpty)
}

func TestNew_Duplicate(t *testing.T) {
	// duplicates must ALL be reported, not just the first one.
	_, err := New([]*pipeline.Task{mkTask("a"), mkTask("b"), mkTask("a"), mkTask("b")})
	wantCode(t, err, pipeline.ErrGraphDuplicateTask)
	if c := strings.Count(err.Error(), "a@2"); c != 1 {
		t.Fatalf("duplicate 'a' not reported once: %v", err)
	}
	if !strings.Contains(err.Error(), "b@3") {
		t.Fatalf("duplicate 'b' missing: %v", err)
	}
}

func TestNew_MissingDep(t *testing.T) {
	// every missing edge must appear; a valid task must not be skipped.
	_, err := New([]*pipeline.Task{
		mkTask("a", "ghost1"),
		mkTask("b", "ghost2", "ghost1"),
		mkTask("c"),
	})
	wantCode(t, err, pipeline.ErrGraphMissingDep)
	for _, edge := range []string{"a->ghost1", "b->ghost1", "b->ghost2"} {
		if !strings.Contains(err.Error(), edge) {
			t.Fatalf("missing edge %s not reported: %v", edge, err)
		}
	}
}

func TestNew_SelfCycle(t *testing.T) {
	_, err := New([]*pipeline.Task{mkTask("a", "a")})
	wantCode(t, err, pipeline.ErrGraphCycle)
	if !strings.Contains(err.Error(), "a -> a") {
		t.Fatalf("self-cycle path missing: %v", err)
	}
}

func TestNew_Cycle(t *testing.T) {
	_, err := New([]*pipeline.Task{
		mkTask("a", "c"),
		mkTask("b", "a"),
		mkTask("c", "b"),
	})
	wantCode(t, err, pipeline.ErrGraphCycle)
	t.Logf("cycle reported: %v", err)
	// cycle path must be closed (first id appears again at the end)
	pe := pipeline.AsError(err)
	path := strings.TrimPrefix(pe.Detail, "dependency cycle: ")
	nodes := strings.Split(path, " -> ")
	if len(nodes) < 4 || nodes[0] != nodes[len(nodes)-1] {
		t.Fatalf("cycle path not closed: %v", path)
	}
}

func TestNew_ValidDAG(t *testing.T) {
	g, err := New([]*pipeline.Task{
		mkTask("a"),
		mkTask("b", "a"),
		mkTask("c", "a"),
		mkTask("d", "b", "c"),
	})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	topo := g.Topo()
	got := make([]string, len(topo))
	for i, n := range topo {
		got[i] = n.Task.ID
	}
	t.Logf("topological order: %v", got)
	pos := map[string]int{}
	for i, id := range got {
		pos[id] = i
	}
	if pos["a"] > pos["b"] || pos["a"] > pos["c"] || pos["b"] > pos["d"] || pos["c"] > pos["d"] {
		t.Fatalf("topological order invalid: %v", got)
	}
}

func TestReadiness_ReadyThenFailureSkips(t *testing.T) {
	g, _ := New([]*pipeline.Task{
		mkTask("a"),
		mkTask("b", "a"),
		mkTask("c", "b"),
	})
	rs := NewReadyState(g)
	ready := rs.Ready(g)
	if len(ready) != 1 || ready[0].Task.ID != "a" {
		t.Fatalf("expected only [a] ready, got %v", ids(ready))
	}
	t.Logf("initially ready: %v (judgement: zero pending deps)", ids(ready))

	rs.Mark("a", pipeline.StatusFailed)
	blocked := rs.BlockedByFailed(g)
	t.Logf("after a failed, blocked: %v", ids(blocked))
	if ids(blocked)[0] != "b" {
		t.Fatalf("expected b blocked, got %v", ids(blocked))
	}
	// mark b skipped -> poison must propagate transitively to c
	rs.Mark("b", pipeline.StatusSkipped)
	blocked = rs.BlockedByFailed(g)
	if len(blocked) != 1 || blocked[0].Task.ID != "c" {
		t.Fatalf("expected c transitively blocked, got %v", ids(blocked))
	}
	if rs.FailedDep("c") != "b" {
		t.Fatalf("c should be blocked by b, got %q", rs.FailedDep("c"))
	}
	t.Logf("transitive skip judgement: c blocked by upstream %q", rs.FailedDep("c"))
}

func TestReadiness_SuccessUnblocks(t *testing.T) {
	g, _ := New([]*pipeline.Task{mkTask("a"), mkTask("b"), mkTask("c", "a", "b")})
	rs := NewReadyState(g)
	if len(rs.Ready(g)) != 2 {
		t.Fatalf("expected a,b ready")
	}
	rs.Mark("a", pipeline.StatusSucceeded)
	if len(rs.Ready(g)) != 1 || rs.Ready(g)[0].Task.ID != "b" {
		t.Fatalf("only b should remain ready")
	}
	rs.Mark("b", pipeline.StatusCacheHit)
	ready := rs.Ready(g)
	if len(ready) != 1 || ready[0].Task.ID != "c" {
		t.Fatalf("cache_hit must unblock c like success, got %v", ids(ready))
	}
	t.Logf("cache_hit unblocks downstream identically to success")
}

func ids(ns []*Node) []string {
	out := make([]string, len(ns))
	for i, n := range ns {
		out[i] = n.Task.ID
	}
	return out
}
