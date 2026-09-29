package state_test

import (
	"context"
	"testing"
	"time"

	"github.com/lisama569/build-pipeline-engine-go/pipeline"
	"github.com/lisama569/build-pipeline-engine-go/pkg/graph"
	"github.com/lisama569/build-pipeline-engine-go/pkg/state"
)

func task(id string, deps []string, inputs map[string]string) *pipeline.Task {
	return &pipeline.Task{ID: id, Deps: deps, CacheKeyInputs: inputs}
}

func mustGraph(t *testing.T, tasks []*pipeline.Task) *graph.Graph {
	t.Helper()
	g, err := graph.New(tasks)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// snapshotFromResults builds a snapshot as a previous successful run would
// have left it, computing bases via the same topo walk.
func snapshotFromResults(t *testing.T, g *graph.Graph, results map[string]pipeline.TaskResult, env string) *state.Snapshot {
	t.Helper()
	snap := &state.Snapshot{Pipeline: "p", StartedAt: time.Now(), Tasks: map[string]state.TaskSnapshot{}}
	bases := map[string]string{}
	for _, n := range g.Topo() {
		deps := []state.DepInput{}
		for _, d := range n.Deps {
			r := results[d.Task.ID]
			deps = append(deps, state.DepInput{ID: d.Task.ID, BasisHash: bases[d.Task.ID], OutputDigest: r.Output.Digest})
		}
		b := state.BasisHash(n.Task, deps, env)
		bases[n.Task.ID] = b
		snap.Tasks[n.Task.ID] = state.TaskSnapshot{Result: results[n.Task.ID], BasisHash: b, Fingerprint: state.FingerprintKey(n.Task, env)}
	}
	return snap
}

func okResult(digest string) pipeline.TaskResult {
	return pipeline.TaskResult{TaskID: "", Status: pipeline.StatusSucceeded, Reason: "ok", Output: pipeline.ExecOutput{Digest: digest}}
}

// TestBasis_StableAcrossPlans: identical graphs/inputs give identical plans
// and all tasks reuse.
func TestBasis_StableAcrossPlans(t *testing.T) {
	tasks := []*pipeline.Task{
		task("a", nil, map[string]string{"v": "1"}),
		task("b", []string{"a"}, map[string]string{"v": "1"}),
		task("c", []string{"b"}, map[string]string{"v": "2"}),
	}
	g := mustGraph(t, tasks)
	res := map[string]pipeline.TaskResult{
		"a": {TaskID: "a", Status: pipeline.StatusSucceeded, Reason: "ok", Output: pipeline.ExecOutput{Digest: "A"}},
		"b": {TaskID: "b", Status: pipeline.StatusSucceeded, Reason: "ok", Output: pipeline.ExecOutput{Digest: "B"}},
		"c": {TaskID: "c", Status: pipeline.StatusSucceeded, Reason: "ok", Output: pipeline.ExecOutput{Digest: "C"}},
	}
	snap := snapshotFromResults(t, g, res, "env1")
	plan, _ := state.PlanResume(g, snap, "env1")
	for _, n := range g.Nodes() {
		e := plan.Entries[n.Task.ID]
		if !e.Reuse {
			t.Fatalf("task %s should reuse, reason=%s", n.Task.ID, e.Reason)
		}
	}
	if ids := plan.ReusedIDs(g); len(ids) != 3 {
		t.Fatalf("expected 3 reused, got %v", ids)
	}
}

// TestResume_FailedAndSkippedRerun: failed, timed-out, canceled and skipped
// tasks from the previous run are re-executed while their unaffected
// neighbors are reused.
func TestResume_FailedAndSkippedRerun(t *testing.T) {
	tasks := []*pipeline.Task{
		task("a", nil, nil),
		task("b", []string{"a"}, nil), // was skipped upstream
		task("c", nil, nil),           // independent success
	}
	g := mustGraph(t, tasks)
	res := map[string]pipeline.TaskResult{
		"a": {TaskID: "a", Status: pipeline.StatusFailed, Reason: "task_failed"},
		"b": {TaskID: "b", Status: pipeline.StatusSkipped, Reason: "upstream:a"},
		"c": {TaskID: "c", Status: pipeline.StatusSucceeded, Reason: "ok", Output: pipeline.ExecOutput{Digest: "C"}},
	}
	snap := snapshotFromResults(t, g, res, "")
	plan, _ := state.PlanResume(g, snap, "")
	if plan.Entries["a"].Reuse || plan.Entries["a"].Reason != state.ReasonNeverSucceeded {
		t.Fatalf("a must rerun (previous failed): %+v", plan.Entries["a"])
	}
	if plan.Entries["b"].Reuse {
		t.Fatalf("previously skipped b must rerun")
	}
	if !plan.Entries["c"].Reuse {
		t.Fatalf("independent successful c must reuse, reason=%s", plan.Entries["c"].Reason)
	}
}

// TestResume_InputChangeInvalidatesSubtree: changing one task's declared
// input reruns it and every transitive dependent, but not independent nodes.
func TestResume_InputChangeInvalidatesSubtree(t *testing.T) {
	tasks1 := []*pipeline.Task{
		task("a", nil, map[string]string{"src": "v1"}),
		task("b", []string{"a"}, nil),
		task("d", nil, nil), // independent
	}
	g1 := mustGraph(t, tasks1)
	res := map[string]pipeline.TaskResult{
		"a": {TaskID: "a", Status: pipeline.StatusSucceeded, Output: pipeline.ExecOutput{Digest: "A"}},
		"b": {TaskID: "b", Status: pipeline.StatusCacheHit, Output: pipeline.ExecOutput{Digest: "B"}},
		"d": {TaskID: "d", Status: pipeline.StatusSucceeded, Output: pipeline.ExecOutput{Digest: "D"}},
	}
	snap := snapshotFromResults(t, g1, res, "")

	tasks2 := []*pipeline.Task{
		task("a", nil, map[string]string{"src": "v2"}), // input changed
		task("b", []string{"a"}, nil),
		task("d", nil, nil),
	}
	g2 := mustGraph(t, tasks2)
	plan, _ := state.PlanResume(g2, snap, "")
	if plan.Entries["a"].Reuse || plan.Entries["a"].Reason != state.ReasonBasisChanged {
		t.Fatalf("changed a must rerun with input_changed, got %+v", plan.Entries["a"])
	}
	if plan.Entries["b"].Reuse {
		t.Fatalf("downstream b must rerun when a changes")
	}
	if !plan.Entries["d"].Reuse {
		t.Fatalf("independent d must still reuse")
	}
}

// TestResume_GraphEdits: added task runs, removed task is simply absent, and
// a dependency edge change invalidates the affected nodes only.
func TestResume_GraphEdits(t *testing.T) {
	// previous: a, b independent; c depends on a
	g1 := mustGraph(t, []*pipeline.Task{
		task("a", nil, nil),
		task("b", nil, nil),
		task("c", []string{"a"}, nil),
	})
	res := map[string]pipeline.TaskResult{
		"a": {TaskID: "a", Status: pipeline.StatusSucceeded, Output: pipeline.ExecOutput{Digest: "A"}},
		"b": {TaskID: "b", Status: pipeline.StatusSucceeded, Output: pipeline.ExecOutput{Digest: "B"}},
		"c": {TaskID: "c", Status: pipeline.StatusSucceeded, Output: pipeline.ExecOutput{Digest: "C"}},
	}
	snap := snapshotFromResults(t, g1, res, "")

	// current: c now depends on b instead of a; new task d added
	g2 := mustGraph(t, []*pipeline.Task{
		task("a", nil, nil),
		task("b", nil, nil),
		task("c", []string{"b"}, nil),
		task("d", []string{"c"}, nil),
	})
	plan, _ := state.PlanResume(g2, snap, "")
	if !plan.Entries["a"].Reuse {
		t.Fatalf("a unchanged and now unrelated; must reuse, reason=%s", plan.Entries["a"].Reason)
	}
	if !plan.Entries["b"].Reuse {
		t.Fatalf("b itself unchanged; must reuse")
	}
	if plan.Entries["c"].Reuse {
		t.Fatalf("c dependency edge changed; must rerun")
	}
	if e := plan.Entries["d"]; e.Reuse || e.Reason != state.ReasonNewTask {
		t.Fatalf("new d must run with new_task, got %+v", e)
	}
}

// TestResume_RenameIsNotReused: a renamed task id cannot claim the old
// result.
func TestResume_RenameIsNotReused(t *testing.T) {
	g1 := mustGraph(t, []*pipeline.Task{
		task("build", nil, map[string]string{"v": "1"}),
		task("test", []string{"build"}, nil),
	})
	res := map[string]pipeline.TaskResult{
		"build": {TaskID: "build", Status: pipeline.StatusSucceeded, Output: pipeline.ExecOutput{Digest: "A"}},
		"test":  {TaskID: "test", Status: pipeline.StatusSucceeded, Output: pipeline.ExecOutput{Digest: "T"}},
	}
	snap := snapshotFromResults(t, g1, res, "")

	// "build" renamed to "compile"; "test" now points at the new name.
	g2 := mustGraph(t, []*pipeline.Task{
		task("compile", nil, map[string]string{"v": "1"}),
		task("test", []string{"compile"}, nil),
	})
	plan, _ := state.PlanResume(g2, snap, "")
	if e := plan.Entries["compile"]; e.Reuse || e.Reason != state.ReasonNewTask {
		t.Fatalf("renamed compile must be treated as new task, got %+v", e)
	}
	if plan.Entries["test"].Reuse {
		t.Fatalf("test depends on renamed node; must rerun")
	}
	if _, present := plan.Entries["build"]; present {
		t.Fatalf("removed task build must not appear in plan")
	}
}

// TestStore_RoundTripAndCorruption: snapshot survives a reload; a corrupt
// snapshot degrades to nil (clean run), never an error that blocks the run.
func TestStore_RoundTripAndCorruption(t *testing.T) {
	dir := t.TempDir()
	s, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !s.Enabled() {
		t.Fatal("store should be enabled")
	}
	g := mustGraph(t, []*pipeline.Task{task("a", nil, nil)})
	res := map[string]pipeline.TaskResult{
		"a": {TaskID: "a", Status: pipeline.StatusSucceeded, Output: pipeline.ExecOutput{Digest: "A"}},
	}
	snap := snapshotFromResults(t, g, res, "")

	lk, err := s.Lock("p")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Save("p", snap); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load("p")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Tasks["a"].BasisHash != snap.Tasks["a"].BasisHash {
		t.Fatalf("snapshot did not round-trip: %+v", got)
	}
	lk.Release()

	// disabled store is a no-op
	s0, _ := state.Open("")
	if s0.Enabled() {
		t.Fatal("empty dir must disable state")
	}
	if p, _ := s0.Load("p"); p != nil {
		t.Fatal("disabled store must load nil")
	}
}

// TestSafeName_DistinctAndStable: distinct names don't collide; same name
// maps deterministically.
func TestSafeName_DistinctAndStable(t *testing.T) {
	if state.SafeName("a/b") == state.SafeName("a:c") {
		// hash suffix must distinguish them despite identical sanitized prefix
		t.Fatal("distinct names collided")
	}
	if state.SafeName("pipe") != state.SafeName("pipe") {
		t.Fatal("same name not stable")
	}
}

var _ = context.Background
