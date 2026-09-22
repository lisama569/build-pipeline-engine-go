package pipeline

import (
	"sort"
)

// Graph is a validated dependency DAG parsed from a Pipeline definition.
type Graph struct {
	name string

	// defs holds task definitions keyed by name.
	defs map[string]*TaskDef
	// order is the deterministic lexicographic Kahn ordering of all tasks.
	order []string
	// deps[t] are the declared dependencies of t (sorted, deduplicated).
	deps map[string][]string
	// dependents[t] are tasks that declare t as a dependency (sorted).
	dependents map[string][]string
	// indeg[t] is the dependency count used during topological sorting.
	indeg map[string]int
}

// Pipeline is a named, unvalidated set of task definitions.
type Pipeline struct {
	Name  string
	Tasks []TaskDef
}

// Parse validates a pipeline definition and returns its executable Graph.
// It rejects, with distinct reasons:
//   - nil / empty pipelines                -> ReasonEmptyGraph
//   - empty names or duplicate definitions -> ReasonInvalidConfig /
//     ReasonDuplicateTask
//   - references to undeclared tasks       -> ReasonMissingDep
//   - dependency cycles                    -> ReasonCycle
//   - per-task timeout/retries over bounds -> ReasonInvalidConfig
//
// Nothing is silently skipped: any defect rejects the whole graph.
func Parse(p *Pipeline, cfg Config) (*Graph, error) {
	if p == nil || len(p.Tasks) == 0 {
		return nil, newErr(ReasonEmptyGraph, "pipeline contains no tasks")
	}

	g := &Graph{
		name:       p.Name,
		defs:       make(map[string]*TaskDef, len(p.Tasks)),
		deps:       make(map[string][]string, len(p.Tasks)),
		dependents: make(map[string][]string, len(p.Tasks)),
		indeg:      make(map[string]int, len(p.Tasks)),
	}

	// 1. Index definitions; reject empty and duplicate names explicitly.
	for i := range p.Tasks {
		t := &p.Tasks[i]
		if t.Name == "" {
			return nil, newErr(ReasonInvalidConfig, "task at index has empty name")
		}
		if _, dup := g.defs[t.Name]; dup {
			return nil, newErr(ReasonDuplicateTask, "task defined more than once", t.Name)
		}
		g.defs[t.Name] = t
	}

	// 2. Normalize dependency lists (deduplicate, sort) and verify every
	// referenced dependency exists and no task depends on itself.
	names := make([]string, 0, len(g.defs))
	for n, t := range g.defs {
		names = append(names, n)

		if t.Timeout != 0 {
			if t.Timeout < 0 {
				return nil, newErr(ReasonInvalidConfig, "task timeout must not be negative", n)
			}
			if cfg.MaxTimeout > 0 && t.Timeout > cfg.MaxTimeout {
				return nil, newErr(ReasonInvalidConfig, "task timeout exceeds configured maximum", n)
			}
		}
		if t.Retries < 0 {
			return nil, newErr(ReasonInvalidConfig, "task retries must not be negative", n)
		}
		if cfg.MaxRetries > 0 && t.Retries > cfg.MaxRetries {
			return nil, newErr(ReasonInvalidConfig, "task retries exceed configured maximum", n)
		}
		if t.Retries > 0 && !t.Idempotent {
			return nil, newErr(ReasonInvalidConfig,
				"task requests retries but is not declared idempotent", n)
		}

		seen := make(map[string]struct{}, len(t.Deps))
		norm := make([]string, 0, len(t.Deps))
		for _, d := range t.Deps {
			if d == n {
				return nil, newErr(ReasonCycle, "task depends on itself", n)
			}
			if _, ok := g.defs[d]; !ok {
				return nil, newErr(ReasonMissingDep, "dependency is not defined in the pipeline", n, d)
			}
			if _, dup := seen[d]; dup {
				continue
			}
			seen[d] = struct{}{}
			norm = append(norm, d)
		}
		sort.Strings(norm)
		g.deps[n] = norm
		g.indeg[n] = len(norm)
	}
	sort.Strings(names)

	for n, ds := range g.deps {
		for _, d := range ds {
			g.dependents[d] = append(g.dependents[d], n)
		}
	}
	for d := range g.dependents {
		sort.Strings(g.dependents[d])
	}

	// 3. Topological sort with a lexicographic tie-break. The deterministic
	// order is O((V+E) log V): insertion of zero-indegree nodes is bounded
	// and no exponential search is involved.
	remaining := make(map[string]int, len(g.indeg))
	for n, deg := range g.indeg {
		remaining[n] = deg
	}
	ready := make([]string, 0)
	for _, n := range names {
		if remaining[n] == 0 {
			ready = append(ready, n)
		}
	}
	for len(ready) > 0 {
		// pick lexicographically smallest ready task
		pick := ready[0]
		ready = ready[1:]
		g.order = append(g.order, pick)
		for _, m := range g.dependents[pick] {
			remaining[m]--
			if remaining[m] == 0 {
				ready = insertSorted(ready, m)
			}
		}
	}

	if len(g.order) != len(g.defs) {
		cycle := findCycle(g)
		return nil, newErr(ReasonCycle, "dependency graph contains a cycle", cycle...)
	}

	return g, nil
}

// insertSorted inserts s into a sorted slice keeping order.
func insertSorted(sorted []string, s string) []string {
	i := sort.SearchStrings(sorted, s)
	sorted = append(sorted, "")
	copy(sorted[i+1:], sorted[i:])
	sorted[i] = s
	return sorted
}

// findCycle extracts one concrete cycle via DFS coloring and returns the
// names on it, so the rejection message points at the offending tasks.
func findCycle(g *Graph) []string {
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int, len(g.defs))
	var stack []string

	// Walk dependency edges (n -> each dep) from every node.
	var visit func(n string) []string
	visit = func(n string) []string {
		color[n] = gray
		stack = append(stack, n)
		for _, d := range g.deps[n] {
			switch color[d] {
			case white:
				if cyc := visit(d); cyc != nil {
					return cyc
				}
			case gray:
				// found a back edge: extract the cycle from the stack
				out := []string{}
				for i := 0; i < len(stack); i++ {
					if stack[i] == d {
						out = append(out, stack[i:]...)
						break
					}
				}
				return append(out, d)
			}
		}
		stack = stack[:len(stack)-1]
		color[n] = black
		return nil
	}

	names := make([]string, 0, len(g.defs))
	for n := range g.defs {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if color[n] == white {
			if cyc := visit(n); cyc != nil {
				return cyc
			}
		}
	}
	return nil
}

// Name returns the pipeline name.
func (g *Graph) Name() string { return g.name }

// Task returns the definition for name, or nil.
func (g *Graph) Task(name string) *TaskDef { return g.defs[name] }

// Order returns the deterministic topological order (lexicographic Kahn).
func (g *Graph) Order() []string { return g.order }
