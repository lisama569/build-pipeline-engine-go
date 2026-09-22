package pipeline

import (
	"container/heap"
	"context"
)

// taskOutcome is delivered by a worker goroutine to the scheduler.
type taskOutcome struct {
	name   string
	result *TaskResult
}

// Scheduler runs a parsed Graph with bounded parallelism.
//
// Dispatch policy: whenever a worker slot is free, the lexicographically
// smallest currently-ready task is chosen, so dispatch is deterministic for
// a given graph. The interleaving inside one concurrent wave is
// timing-dependent; that fact is annotated in the report meta rather than
// hidden in logs.
type Scheduler struct {
	engine *Engine
	graph  *Graph
	cfg    Config

	results map[string]*TaskResult
	order   []string
	keys    map[string]string
}

// Execute runs the graph to completion, failure or cancellation and returns
// the full report. Downstream tasks of a failed/timed-out task are marked
// StatusSkipped / ReasonUpstream — never silently dropped, never marked
// successful. External cancellation marks pending tasks
// StatusSkipped / ReasonCanceled and waits for in-flight tasks to stop,
// bounded by Config.ShutdownGrace.
func (e *Engine) Execute(ctx context.Context, g *Graph) *Report {
	s := &Scheduler{
		engine:  e,
		graph:   g,
		cfg:     e.cfg,
		results: make(map[string]*TaskResult, len(g.defs)),
		keys:    make(map[string]string, len(g.defs)),
	}
	return s.run(ctx)
}

func (s *Scheduler) logf(format string, args ...any) {
	if s.cfg.Logf != nil {
		s.cfg.Logf(format, args...)
	}
}

// taskState tracks scheduling lifecycle of one node.
type taskState int

const (
	stWaiting taskState = iota
	stReady
	stRunning
	stDone // terminal result already in results
)

// run is the scheduling loop. Total goroutines are bounded at
// Parallelism (fixed worker pool) + 1 regardless of graph size.
func (s *Scheduler) run(ctx context.Context) *Report {
	g := s.graph
	P := s.cfg.Parallelism

	// 1. Compute every fingerprint in deterministic topological order so an
	// upstream key is always available when its dependent is fingerprinted.
	env := s.engine.envForKey()
	for _, name := range g.order {
		if !g.Task(name).Cacheable {
			continue
		}
		key, err := Fingerprint(g, name, s.keys, env, s.cfg.RepoRoot)
		if err != nil {
			s.results[name] = &TaskResult{
				Name: name, Status: StatusFailed, Reason: ReasonExecError,
				Message: err.Error(), DependsOn: append([]string(nil), g.deps[name]...),
			}
			// Mark every transitive dependent skipped (state maps are built
			// below; initialize results directly here).
			visited := map[string]bool{name: true}
			stack := []string{name}
			for len(stack) > 0 {
				n := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				for _, m := range g.dependents[n] {
					if visited[m] {
						continue
					}
					visited[m] = true
					stack = append(stack, m)
					s.results[m] = &TaskResult{
						Name: m, Status: StatusSkipped, Reason: ReasonUpstream,
						Message:   "upstream dependency could not be fingerprinted: " + name,
						DependsOn: append([]string(nil), g.deps[m]...),
					}
				}
			}
			return s.finalize()
		}
		s.keys[name] = key
	}

	// 2. Mutable bookkeeping.
	indeg := make(map[string]int, len(g.defs))
	state := make(map[string]taskState, len(g.defs))
	ready := &stringHeap{}
	heap.Init(ready)
	for name, d := range g.deps {
		indeg[name] = len(d)
		state[name] = stWaiting
		if len(d) == 0 {
			heap.Push(ready, name)
			state[name] = stReady
		}
	}

	remaining := len(g.defs)
	running := 0

	// skip marks a node terminal-skipped if it has not finished/started.
	skip := func(name string, reason Reason, msg string) {
		if state[name] == stDone || state[name] == stRunning {
			return
		}
		state[name] = stDone
		s.results[name] = &TaskResult{
			Name:      name,
			Status:    StatusSkipped,
			Reason:    reason,
			Message:   msg,
			DependsOn: append([]string(nil), g.deps[name]...),
		}
		s.logf("skip task=%s reason=%s detail=%q", name, reason, msg)
		remaining--
	}

	// markDownstream propagates skip status transitively through dependents.
	markDownstream := func(origin string, reason Reason, msg string) {
		visited := map[string]bool{origin: true}
		stack := []string{origin}
		for len(stack) > 0 {
			n := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			for _, m := range g.dependents[n] {
				if visited[m] {
					continue
				}
				visited[m] = true
				stack = append(stack, m)
				skip(m, reason, msg)
			}
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan string)
	outcomes := make(chan taskOutcome, P)
	for w := 0; w < P; w++ {
		go func() {
			for name := range jobs {
				key := s.keys[name]
				r := s.engine.runOrJoin(ctx, g, name, key, func(c context.Context) *TaskResult {
					return s.engine.runTask(c, g, name, key)
				})
				outcomes <- taskOutcome{name: name, result: r}
			}
		}()
	}

	// rebuildReady drops names that are no longer eligible (e.g. marked
	// skipped by failure propagation).
	rebuildReady := func() {
		kept := &stringHeap{}
		heap.Init(kept)
		for ready.Len() > 0 {
			n := heap.Pop(ready).(string)
			if state[n] == stReady {
				heap.Push(kept, n)
			}
		}
		ready = kept
	}

	dispatch := func() {
		for ready.Len() > 0 && running < P {
			name := heap.Pop(ready).(string)
			if state[name] != stReady {
				continue // was invalidated while queued
			}
			state[name] = stRunning
			running++
			s.order = append(s.order, name)
			s.logf("dispatch task=%s reason=lexicographically-smallest-ready ready=%d running=%d parallelism=%d",
				name, ready.Len(), running, P)
			jobs <- name
		}
	}

	for remaining > 0 || running > 0 {
		dispatch()

		select {
		case <-ctx.Done():
			// External cancellation: stop dispatch, cancel running bodies,
			// mark everything still waiting/queued as skipped/canceled, then
			// wait for workers to converge (bounded inside runTask).
			s.logf("cancel signal received: running=%d remaining=%d", running, remaining)
			cancel()
			for _, name := range g.order {
				if state[name] != stDone && state[name] != stRunning {
					skip(name, ReasonCanceled, "canceled before dispatch")
				}
			}
			// Drain the ready heap explicitly (state flips above already
			// invalidate entries; rebuild keeps the invariant clean).
			ready = &stringHeap{}
			heap.Init(ready)
			// Wait for all in-flight attempts to stop.
			for running > 0 {
				oc := <-outcomes
				running--
				remaining--
				state[oc.name] = stDone
				s.results[oc.name] = oc.result
				s.logf("complete-after-cancel task=%s status=%s reason=%s",
					oc.name, oc.result.Status, oc.result.Reason)
			}
			close(jobs)
			return s.finalize()

		case oc := <-outcomes:
			running--
			remaining--
			state[oc.name] = stDone
			s.results[oc.name] = oc.result
			s.logf("complete task=%s status=%s reason=%s attempts=%d cache_hit=%t",
				oc.name, oc.result.Status, oc.result.Reason, oc.result.Attempts, oc.result.CacheHit)

			if !isOK(oc.result.Status) {
				markDownstream(oc.name, ReasonUpstream,
					"upstream dependency did not succeed: "+oc.name)
				rebuildReady()
				continue
			}
			for _, m := range g.dependents[oc.name] {
				if state[m] != stWaiting {
					continue
				}
				indeg[m]--
				if indeg[m] == 0 {
					state[m] = stReady
					heap.Push(ready, m)
					s.logf("ready task=%s reason=all-dependencies-succeeded", m)
				}
			}
		}
	}

	close(jobs)
	return s.finalize()
}

// finalize assembles the deterministic report.
func (s *Scheduler) finalize() *Report {
	return buildReport(s.graph, s.results, s.order)
}

// isOK reports whether a status satisfies dependencies.
func isOK(st TaskStatus) bool {
	return st == StatusSucceeded || st == StatusFromCache
}

// stringHeap is a min-heap of task names.
type stringHeap []string

func (h stringHeap) Len() int           { return len(h) }
func (h stringHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h stringHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *stringHeap) Push(x any)        { *h = append(*h, x.(string)) }
func (h *stringHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
