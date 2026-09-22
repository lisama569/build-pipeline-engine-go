package pipeline

import (
	"context"
	"strings"
	"testing"
)

func noop(ctx context.Context) (*TaskOutput, error) { return &TaskOutput{}, nil }

func TestParse_ValidGraphDeterministicOrder(t *testing.T) {
	p := &Pipeline{Name: "g", Tasks: []TaskDef{
		{Name: "d", Deps: []string{"b", "c"}, Run: noop},
		{Name: "b", Deps: []string{"a"}, Run: noop},
		{Name: "c", Deps: []string{"a"}, Run: noop},
		{Name: "a", Run: noop},
	}}
	g, err := Parse(p, DefaultConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"a", "b", "c", "d"}
	if strings.Join(g.Order(), ",") != strings.Join(want, ",") {
		t.Fatalf("order = %v, want %v (lexicographic Kahn)", g.Order(), want)
	}
	// Repeated parses must yield identical order.
	g2, _ := Parse(p, DefaultConfig())
	if strings.Join(g2.Order(), ",") != strings.Join(g.Order(), ",") {
		t.Fatalf("non-deterministic order: %v vs %v", g2.Order(), g.Order())
	}
}

func TestParse_DuplicateTaskRejected(t *testing.T) {
	p := &Pipeline{Name: "g", Tasks: []TaskDef{
		{Name: "a", Run: noop},
		{Name: "a", Run: noop},
	}}
	_, err := Parse(p, DefaultConfig())
	if err == nil || AsError(err).Reason != ReasonDuplicateTask {
		t.Fatalf("want duplicate_task rejection, got %v", err)
	}
}

func TestParse_MissingDependencyRejected(t *testing.T) {
	p := &Pipeline{Name: "g", Tasks: []TaskDef{
		{Name: "a", Deps: []string{"ghost"}, Run: noop},
	}}
	_, err := Parse(p, DefaultConfig())
	if err == nil || AsError(err).Reason != ReasonMissingDep {
		t.Fatalf("want missing_dependency rejection, got %v", err)
	}
	if !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("error should name the missing dep: %v", err)
	}
}

func TestParse_SelfCycleRejected(t *testing.T) {
	p := &Pipeline{Name: "g", Tasks: []TaskDef{
		{Name: "a", Deps: []string{"a"}, Run: noop},
	}}
	_, err := Parse(p, DefaultConfig())
	if err == nil || AsError(err).Reason != ReasonCycle {
		t.Fatalf("want cycle rejection, got %v", err)
	}
}

func TestParse_CycleRejectedWithMembers(t *testing.T) {
	// a -> b -> c -> a ; plus an independent d
	p := &Pipeline{Name: "g", Tasks: []TaskDef{
		{Name: "a", Deps: []string{"c"}, Run: noop},
		{Name: "b", Deps: []string{"a"}, Run: noop},
		{Name: "c", Deps: []string{"b"}, Run: noop},
		{Name: "d", Run: noop},
	}}
	_, err := Parse(p, DefaultConfig())
	if err == nil {
		t.Fatal("expected cycle error")
	}
	pe := AsError(err)
	if pe.Reason != ReasonCycle {
		t.Fatalf("reason = %s, want %s", pe.Reason, ReasonCycle)
	}
	for _, member := range []string{"a", "b", "c"} {
		if !contains(pe.Tasks, member) {
			t.Fatalf("cycle error %v should identify member %s", pe.Tasks, member)
		}
	}
}

func TestParse_EmptyRejected(t *testing.T) {
	if _, err := Parse(nil, DefaultConfig()); err == nil || AsError(err).Reason != ReasonEmptyGraph {
		t.Fatalf("nil pipeline must be rejected as empty, got %v", err)
	}
	if _, err := Parse(&Pipeline{Name: "x"}, DefaultConfig()); err == nil {
		t.Fatal("empty task list must be rejected")
	}
}

func TestParse_NonIdempotentRetriesRejected(t *testing.T) {
	p := &Pipeline{Name: "g", Tasks: []TaskDef{
		{Name: "a", Run: noop, Retries: 1, Idempotent: false},
	}}
	_, err := Parse(p, DefaultConfig())
	if err == nil || AsError(err).Reason != ReasonInvalidConfig {
		t.Fatalf("retries on a non-idempotent task must be rejected at parse time, got %v", err)
	}
}

func TestParse_ReasonsAreDistinguishable(t *testing.T) {
	// Guard: the four definition-time reasons remain distinct strings.
	reasons := map[Reason]bool{}
	for _, r := range []Reason{ReasonDuplicateTask, ReasonMissingDep, ReasonCycle, ReasonInvalidConfig} {
		if reasons[r] {
			t.Fatalf("duplicate reason %s", r)
		}
		reasons[r] = true
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
