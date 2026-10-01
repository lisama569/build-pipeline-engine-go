package state

import (
	"github.com/lisama569/build-pipeline-engine-go/pipeline"
	"github.com/lisama569/build-pipeline-engine-go/pkg/graph"
)

// PlanResume compares the current graph (in topological order) against the
// last snapshot and decides, per task, whether its previous successful result
// can be carried over.
//
// A task is reused only when ALL hold:
//  1. the task id existed in the snapshot (rename/add => recompute),
//  2. the previous result was a success status (succeeded/cache_hit; a task
//     that failed, timed out, was canceled or was skipped is retried),
//  3. the task does not disable caching (DisableCache tasks have side
//     effects that must really run on every run),
//  4. the current basis equals the stored basis, i.e. neither this task's
//     fingerprint (id/command/env/declared inputs) nor any transitive
//     dependency's id/basis/output changed (edge edits included).
//
// When a dependency re-executes solely because it disables caching, its
// recorded success digest is still folded into downstream bases, so an
// unaffected downstream task stays reusable - but only CONDITIONALLY: the
// scheduler re-verifies the basis against the dependency's fresh output at
// run time and re-executes the task if the output actually changed.
//
// bases is task id -> current basis for already-planned (topo-prior) nodes;
// it is updated as the walk proceeds.
func PlanResume(g *graph.Graph, snap *Snapshot, baseEnvHash string) (*Plan, map[string]string) {
	plan := &Plan{Entries: make(map[string]PlanEntry, len(g.Nodes()))}
	bases := make(map[string]string, len(g.Nodes()))

	// Pre-seed bases from reused snapshots as we walk topo; a node whose dep
	// was not reused still contributes its CURRENT basis once recomputed in
	// the actual run, but for planning purposes the basis is fully
	// deterministic from graph + fingerprints + stored digests along reused
	// chains. When a dep has no reusable stored success, the child cannot
	// match anyway.
	for _, n := range g.Topo() {
		t := n.Task
		deps := make([]DepInput, 0, len(n.Deps))
		depOK := true
		depReexecutes := false
		for _, dn := range n.Deps {
			b, known := bases[dn.Task.ID]
			if !known {
				depOK = false
				break
			}
			// The digest folded in is the dep's previous stored digest. It is
			// exact when the dep is unconditionally reused; for a dep that
			// re-executes despite a reusable stored success (cache disabled)
			// it is a planning estimate the scheduler re-verifies at run
			// time, so the child becomes a conditional reuse.
			depEntry := plan.Entries[dn.Task.ID]
			digest := ""
			switch {
			case depEntry.Reuse && depEntry.Snapshot != nil:
				digest = depEntry.Snapshot.Result.Output.Digest
				if depEntry.Conditional {
					depReexecutes = true
				}
			case depEntry.Snapshot != nil && Reusable(depEntry.Snapshot.Result.Status):
				digest = depEntry.Snapshot.Result.Output.Digest
				depReexecutes = true
			default:
				depOK = false
			}
			deps = append(deps, DepInput{
				ID: dn.Task.ID, BasisHash: b, OutputDigest: digest,
			})
		}

		basis := BasisHash(t, deps, baseEnvHash)
		bases[t.ID] = basis
		fp := FingerprintKey(t, baseEnvHash)

		entry := PlanEntry{BasisHash: basis, Fingerprint: fp}
		old, existed := snapshotTask(snap, t.ID)
		switch {
		case !existed:
			entry.Reuse = false
			entry.Reason = ReasonNewTask
		case !Reusable(old.Result.Status):
			entry.Reuse = false
			entry.Reason = ReasonNeverSucceeded
			entry.Snapshot = &old
		case t.DisableCache:
			// Side effects must really run on every run; never carried
			// over. Its stored success digest still lets downstream tasks
			// plan a conditional reuse.
			entry.Reuse = false
			entry.Reason = ReasonCacheDisabled
			entry.Snapshot = &old
		case depOK && old.BasisHash == basis:
			entry.Reuse = true
			entry.Conditional = depReexecutes
			entry.Snapshot = &old
		default:
			entry.Reuse = false
			entry.Reason = ReasonBasisChanged
			entry.Snapshot = &old
		}
		plan.Entries[t.ID] = entry
	}
	return plan, bases
}

func snapshotTask(snap *Snapshot, id string) (TaskSnapshot, bool) {
	if snap == nil || snap.Tasks == nil {
		return TaskSnapshot{}, false
	}
	ts, ok := snap.Tasks[id]
	return ts, ok
}

// ReusedIDs returns the ids the plan reuses, in declaration order.
func (p *Plan) ReusedIDs(g *graph.Graph) []string {
	var out []string
	for _, n := range g.Nodes() {
		if e, ok := p.Entries[n.Task.ID]; ok && e.Reuse {
			out = append(out, n.Task.ID)
		}
	}
	return out
}

// Invalidation returns the stable reason for id not being reused.
func (p *Plan) Invalidation(id string) string {
	if e, ok := p.Entries[id]; ok {
		return e.Reason
	}
	return ""
}

// IsSuccess re-exported convenience for callers avoiding an extra import.
func IsSuccess(st pipeline.TaskStatus) bool { return pipeline.IsSuccess(st) }
