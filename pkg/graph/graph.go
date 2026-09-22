// Package graph parses and validates the task dependency graph and answers
// readiness questions for the scheduler.
//
// Validation rejects, with distinguishable codes and ALL offending nodes in
// the detail:
//   - empty pipeline                 -> ErrGraphEmpty
//   - duplicate task id              -> ErrGraphDuplicateTask
//   - dependency on an unknown task  -> ErrGraphMissingDep
//   - dependency cycle               -> ErrGraphCycle (with the cycle path)
package graph

import (
	"sort"
	"strings"

	"github.com/lisama569/build-pipeline-engine-go/pipeline"
)

// Node is a validated graph node.
type Node struct {
	Task  *pipeline.Task
	Index int
	// Deps are the resolved dependency nodes, in declared order.
	Deps []*Node
	// Dependents are nodes that depend on this node.
	Dependents []*Node
	// RemainingDependents is the transitive number of dependents; it is used
	// only as a deterministic ready-queue priority hint.
	RemainingDependents int
}

// Graph is an immutable validated dependency graph.
type Graph struct {
	nodes []*Node
	byID  map[string]*Node
	topo  []*Node
}

// New parses the task list and validates it.
func New(tasks []*pipeline.Task) (*Graph, error) {
	if len(tasks) == 0 {
		return nil, pipeline.NewError(pipeline.ErrGraphEmpty, "pipeline has no tasks")
	}

	g := &Graph{
		nodes: make([]*Node, len(tasks)),
		byID:  make(map[string]*Node, len(tasks)),
	}

	// 1) duplicate detection + node construction (single pass, all dupes found).
	var dupes []string
	seen := make(map[string]int, len(tasks))
	for i, t := range tasks {
		if t == nil {
			return nil, pipeline.NewError(pipeline.ErrConfigInvalid, "task at index %d is nil", i)
		}
		if t.ID == "" {
			return nil, pipeline.NewError(pipeline.ErrConfigInvalid, "task at index %d has empty id", i)
		}
		if first, ok := seen[t.ID]; ok {
			dupes = append(dupes, t.ID+"@"+itoa(i)+"(first@"+itoa(first)+")")
			continue
		}
		seen[t.ID] = i
		n := &Node{Task: t, Index: i}
		g.nodes[i] = n
		g.byID[t.ID] = n
	}
	if len(dupes) > 0 {
		return nil, pipeline.NewError(pipeline.ErrGraphDuplicateTask,
			"duplicate task ids: %s", strings.Join(dupes, ", "))
	}

	// 2) missing dependency detection (collect every missing edge).
	type missing struct{ from, dep string }
	var missingDeps []missing
	knownMissing := map[string]struct{}{}
	for _, n := range g.nodes {
		for _, dep := range n.Task.Deps {
			dn, ok := g.byID[dep]
			if !ok {
				missingDeps = append(missingDeps, missing{n.Task.ID, dep})
				continue
			}
			n.Deps = append(n.Deps, dn)
			dn.Dependents = append(dn.Dependents, n)
		}
	}
	if len(missingDeps) > 0 {
		sort.Slice(missingDeps, func(i, j int) bool {
			if missingDeps[i].from != missingDeps[j].from {
				return missingDeps[i].from < missingDeps[j].from
			}
			return missingDeps[i].dep < missingDeps[j].dep
		})
		parts := make([]string, 0, len(missingDeps))
		for _, m := range missingDeps {
			key := m.from + "->" + m.dep
			if _, ok := knownMissing[key]; ok {
				continue
			}
			knownMissing[key] = struct{}{}
			parts = append(parts, key)
		}
		return nil, pipeline.NewError(pipeline.ErrGraphMissingDep,
			"tasks reference unknown dependencies: %s", strings.Join(parts, ", "))
	}

	// 3) cycle detection via deterministic DFS; report the actual cycle path.
	if cycle := findCycle(g); cycle != nil {
		return nil, pipeline.NewError(pipeline.ErrGraphCycle,
			"dependency cycle: %s", strings.Join(cycle, " -> "))
	}

	// 4) deterministic topological order: Kahn with a declaration-index tie
	// break, so scheduling is independent of map iteration.
	g.topo = topoKahn(g)

	// 5) transitive dependent counts for the priority queue policy.
	for _, n := range g.nodes {
		n.RemainingDependents = len(collectDescendants(n, nil))
	}
	return g, nil
}

// findCycle returns the ids of one dependency cycle, or nil. The cycle is
// found by iterative 3-color DFS in deterministic (declaration) order.
func findCycle(g *Graph) []string {
	const white, gray, black = 0, 1, 2
	color := make(map[string]int, len(g.nodes))
	parent := make(map[string]string, len(g.nodes))

	// sorted ids for deterministic traversal
	ids := make([]*Node, 0, len(g.nodes))
	ids = append(ids, g.nodes...)
	sort.Slice(ids, func(i, j int) bool { return ids[i].Index < ids[j].Index })

	for _, root := range ids {
		if color[root.Task.ID] != white {
			continue
		}
		type frame struct {
			n    *Node
			edge int
		}
		stack := []frame{{root, 0}}
		color[root.Task.ID] = gray

		for len(stack) > 0 {
			top := &stack[len(stack)-1]
			if top.edge < len(top.n.Deps) {
				dep := top.n.Deps[top.edge]
				top.edge++
				switch color[dep.Task.ID] {
				case white:
					color[dep.Task.ID] = gray
					parent[dep.Task.ID] = top.n.Task.ID
					stack = append(stack, frame{dep, 0})
				case gray:
					// Back edge top.n -> dep. Reconstruct the cycle by
					// walking parents from top.n up to dep, then closing it.
					chain := []string{top.n.Task.ID}
					cur := top.n.Task.ID
					for cur != dep.Task.ID {
						p := parent[cur]
						if p == "" {
							break
						}
						chain = append(chain, p)
						cur = p
					}
					// reverse: chain currently runs top.n ... dep; we want
					// dep ... top.n, then close with dep.
					for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
						chain[i], chain[j] = chain[j], chain[i]
					}
					chain = append(chain, dep.Task.ID)
					return chain
				}
			} else {
				color[top.n.Task.ID] = black
				stack = stack[:len(stack)-1]
			}
		}
	}
	return nil
}

// topoKahn is Kahn's algorithm with declaration-index tie break.
func topoKahn(g *Graph) []*Node {
	indeg := make(map[string]int, len(g.nodes))
	for _, n := range g.nodes {
		indeg[n.Task.ID] = len(n.Deps)
	}
	var ready []*Node
	push := func(n *Node) {
		ready = append(ready, n)
		// insertion by index keeps the slice sorted (inputs are processed in
		// index order, but dependents can arrive out of order -> sort on pop)
	}
	for _, n := range g.nodes {
		if indeg[n.Task.ID] == 0 {
			push(n)
		}
	}
	out := make([]*Node, 0, len(g.nodes))
	for len(ready) > 0 {
		// pick smallest declaration index
		si := 0
		for i := 1; i < len(ready); i++ {
			if ready[i].Index < ready[si].Index {
				si = i
			}
		}
		n := ready[si]
		ready = append(ready[:si], ready[si+1:]...)
		out = append(out, n)
		for _, d := range n.Dependents {
			indeg[d.Task.ID]--
			if indeg[d.Task.ID] == 0 {
				ready = append(ready, d)
			}
		}
	}
	return out
}

// collectDescendants returns the set of transitive dependents of n.
func collectDescendants(n *Node, acc map[string]struct{}) map[string]struct{} {
	if acc == nil {
		acc = make(map[string]struct{})
	}
	for _, d := range n.Dependents {
		if _, ok := acc[d.Task.ID]; ok {
			continue
		}
		acc[d.Task.ID] = struct{}{}
		collectDescendants(d, acc)
	}
	return acc
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}

// Nodes returns the nodes in declaration order.
func (g *Graph) Nodes() []*Node { return g.nodes }

// Node returns the node with the given id.
func (g *Graph) Node(id string) (*Node, bool) {
	n, ok := g.byID[id]
	return n, ok
}

// Topo returns the deterministic topological ordering.
func (g *Graph) Topo() []*Node { return g.topo }
