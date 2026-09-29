package resume

import (
	"github.com/lisama569/build-pipeline-engine-go/pipeline"
	"github.com/lisama569/build-pipeline-engine-go/pkg/cache"
	"github.com/lisama569/build-pipeline-engine-go/pkg/graph"
)

// Build computes the resume decisions for tasks against the previous run's
// checkpoint. A nil checkpoint means "first run": every task is new. baseEnvHash
// is the engine's environment fingerprint (as used by the cache layer), so an
// ambient environment change is indistinguishable from an input change and
// forces recomputation rather than serving a stale verdict.
func Build(tasks []*pipeline.Task, cp *Checkpoint, baseEnvHash string) (*Plan, error) {
	g, err := graph.New(tasks)
	if err != nil {
		return nil, err
	}

	prev := make(map[string]TaskSnapshot, len(cpTasks(cp)))
	for _, s := range cpTasks(cp) {
		prev[s.ID] = s
	}

	decisions := make(map[string]Decision, len(tasks))
	order := make([]string, 0, len(tasks))

	// Topological order guarantees every dependency is decided before its
	// dependents, so the "all deps reusable" check is a map lookup.
	for _, n := range g.Topo() {
		t := n.Task
		id := t.ID
		order = append(order, id)
		depIDs := append([]string(nil), t.Deps...)
		sortStrings(depIDs)

		snap, known := prev[id]
		fp := cache.Key(cache.FingerprintFromTask(t, baseEnvHash))

		d := Decision{TaskID: id}
		switch {
		case !known:
			d.RerunReason = RerunNewTask
		case !equalSorted(snap.Deps, depIDs):
			d.RerunReason = RerunDepsChanged
		case snap.Fingerprint != fp:
			d.RerunReason = RerunInputChanged
		case !pipeline.IsSuccess(snap.Status):
			d.RerunReason = nonSuccessRerunReason(snap.Status)
		case t.DisableCache:
			d.RerunReason = RerunCacheDisabled
		default:
			// All transitive deps reusable iff every direct dep is reusable:
			// a dep that re-executes only does so when *its* deps are fine or
			// not, captured recursively; topo order makes this exact here.
			d.RerunReason = ""
			for _, depID := range t.Deps {
				if dep, ok := decisions[depID]; ok && !dep.Reuse {
					d.RerunReason = RerunDepReran
					break
				}
			}
		}

		if d.RerunReason == "" && known {
			d.Reuse = true
			d.Snapshot = snap
		}
		decisions[id] = d
	}
	return &Plan{decisions: decisions, order: order}, nil
}

// nonSuccessRerunReason maps a previous non-success status to the rerun code.
func nonSuccessRerunReason(st pipeline.TaskStatus) string {
	switch st {
	case pipeline.StatusTimedOut:
		return RerunPrevTimedOut
	case pipeline.StatusSkipped:
		return RerunPrevSkipped
	case pipeline.StatusCanceled:
		return RerunPrevCanceled
	default:
		return RerunPrevFailed
	}
}

func cpTasks(cp *Checkpoint) []TaskSnapshot {
	if cp == nil {
		return nil
	}
	return cp.Tasks
}

func equalSorted(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sortStrings(xs []string) {
	// insertion-free stdlib sort kept out of hot paths; simple sort.Slice use.
	for i := 1; i < len(xs); i++ {
		for j := i; j > 0 && xs[j-1] > xs[j]; j-- {
			xs[j-1], xs[j] = xs[j], xs[j-1]
		}
	}
}

// Decision returns the recorded decision for task id.
func (p *Plan) Decision(id string) (Decision, bool) {
	if p == nil {
		return Decision{}, false
	}
	d, ok := p.decisions[id]
	return d, ok
}

// Reusable reports whether the task's previous verdict is carried over.
func (p *Plan) Reusable(id string) bool {
	d, ok := p.Decision(id)
	return ok && d.Reuse
}

// ReusedCount counts carried-over verdicts.
func (p *Plan) ReusedCount() int {
	if p == nil {
		return 0
	}
	n := 0
	for _, d := range p.decisions {
		if d.Reuse {
			n++
		}
	}
	return n
}

// ReranCount counts tasks that must execute.
func (p *Plan) ReranCount() int {
	if p == nil {
		return 0
	}
	return len(p.decisions) - p.ReusedCount()
}

// ReusableTasks returns the ids of carried-over verdicts in declaration order.
func (p *Plan) ReusableTasks() []string {
	if p == nil {
		return nil
	}
	var out []string
	for _, id := range p.order {
		if p.decisions[id].Reuse {
			out = append(out, id)
		}
	}
	return out
}
