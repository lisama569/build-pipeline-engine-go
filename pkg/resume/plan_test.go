package resume_test

import (
	"testing"

	"github.com/lisama569/build-pipeline-engine-go/pipeline"
	"github.com/lisama569/build-pipeline-engine-go/pkg/cache"
	"github.com/lisama569/build-pipeline-engine-go/pkg/resume"
)

func task(id string, deps []string, inputs map[string]string, disableCache bool) *pipeline.Task {
	return &pipeline.Task{ID: id, Deps: deps, CacheKeyInputs: inputs, DisableCache: disableCache}
}

func fingerprint(t *pipeline.Task) string {
	return cache.Key(cache.FingerprintFromTask(t, "env"))
}

func snap(t *pipeline.Task, st pipeline.TaskStatus) resume.TaskSnapshot {
	deps := append([]string(nil), t.Deps...)
	sortCopy(deps)
	return resume.TaskSnapshot{ID: t.ID, Fingerprint: fingerprint(t), Deps: deps, Status: st,
		Reason: "ok", Output: pipeline.ExecOutput{Digest: t.ID + "-out"}}
}

func sortCopy(x []string) {
	for i := 1; i < len(x); i++ {
		for j := i; j > 0 && x[j-1] > x[j]; j-- {
			x[j-1], x[j] = x[j], x[j-1]
		}
	}
}

// First run with no checkpoint: every task is new, none reusable.
func TestBuild_NoCheckpointAllNew(t *testing.T) {
	a := task("a", nil, map[string]string{"v": "1"}, false)
	b := task("b", []string{"a"}, map[string]string{"v": "1"}, false)
	p, err := resume.Build([]*pipeline.Task{a, b}, nil, "env")
	if err != nil {
		t.Fatal(err)
	}
	if p.ReusedCount() != 0 || p.ReranCount() != 2 {
		t.Fatalf("want 0 reused/2 reran, got %d/%d", p.ReusedCount(), p.ReranCount())
	}
	d, _ := p.Decision("a")
	if d.RerunReason != resume.RerunNewTask {
		t.Fatalf("a reason=%s", d.RerunReason)
	}
}

// Unchanged success: everything reusable.
func TestBuild_AllReusable(t *testing.T) {
	a := task("a", nil, map[string]string{"v": "1"}, false)
	b := task("b", []string{"a"}, map[string]string{"v": "1"}, false)
	cp := &resume.Checkpoint{Pipeline: "p", Tasks: []resume.TaskSnapshot{
		snap(a, pipeline.StatusSucceeded), snap(b, pipeline.StatusCacheHit),
	}}
	p, err := resume.Build([]*pipeline.Task{a, b}, cp, "env")
	if err != nil {
		t.Fatal(err)
	}
	if p.ReusedCount() != 2 {
		t.Fatalf("want 2 reused, got %d", p.ReusedCount())
	}
}

// Failed task reruns and so does its transitive closure; an independent branch
// stays reusable.
func TestBuild_FailureAndClosureRerun(t *testing.T) {
	a := task("a", nil, nil, false)
	b := task("b", []string{"a"}, nil, false)
	c := task("c", []string{"b"}, nil, false)
	d := task("d", nil, nil, false)
	cp := &resume.Checkpoint{Pipeline: "p", Tasks: []resume.TaskSnapshot{
		snap(a, pipeline.StatusSucceeded),
		snap(b, pipeline.StatusFailed),
		snap(c, pipeline.StatusSkipped),
		snap(d, pipeline.StatusSucceeded),
	}}
	p, err := resume.Build([]*pipeline.Task{a, b, c, d}, cp, "env")
	if err != nil {
		t.Fatal(err)
	}
	if !p.Reusable("a") {
		t.Fatal("a (succeeded) must be reusable")
	}
	if p.Reusable("b") || p.Reusable("c") {
		t.Fatal("b and c must rerun")
	}
	db, _ := p.Decision("b")
	if db.RerunReason != resume.RerunPrevFailed {
		t.Fatalf("b reason=%s", db.RerunReason)
	}
	// c was itself non-success (skipped): its own prior state is the rerun
	// reason; dep_reran propagation only applies to previously-success nodes.
	dc, _ := p.Decision("c")
	if dc.RerunReason != resume.RerunPrevSkipped {
		t.Fatalf("c reason=%s, want %s", dc.RerunReason, resume.RerunPrevSkipped)
	}
	if !p.Reusable("d") {
		t.Fatal("independent d must remain reusable")
	}
}

// Input change invalidates exactly the changed node (and downstream nodes that
// were previously successful; previously-non-success nodes report their own
// prior state).
func TestBuild_InputChangeInvalidatesClosure(t *testing.T) {
	a1 := task("a", nil, map[string]string{"v": "1"}, false)
	b := task("b", []string{"a"}, nil, false)
	cp := &resume.Checkpoint{Pipeline: "p", Tasks: []resume.TaskSnapshot{
		snap(a1, pipeline.StatusSucceeded), snap(b, pipeline.StatusSucceeded),
	}}
	a2 := task("a", nil, map[string]string{"v": "2"}, false)
	p, err := resume.Build([]*pipeline.Task{a2, b}, cp, "env")
	if err != nil {
		t.Fatal(err)
	}
	da, _ := p.Decision("a")
	if da.RerunReason != resume.RerunInputChanged {
		t.Fatalf("a=%s", da.RerunReason)
	}
	db, _ := p.Decision("b")
	if db.RerunReason != resume.RerunDepReran {
		t.Fatalf("b=%s", db.RerunReason)
	}
}

// Dependency restructure (edge added/removed, task renamed away) must never
// reuse the affected node.
func TestBuild_DependencyRestructure(t *testing.T) {
	a := task("a", nil, nil, false)
	b := task("b", nil, nil, false)
	cp := &resume.Checkpoint{Pipeline: "p", Tasks: []resume.TaskSnapshot{
		snap(a, pipeline.StatusSucceeded), snap(b, pipeline.StatusSucceeded),
	}}
	// b now depends on a
	b2 := task("b", []string{"a"}, nil, false)
	p, err := resume.Build([]*pipeline.Task{a, b2}, cp, "env")
	if err != nil {
		t.Fatal(err)
	}
	if p.Reusable("a") != true {
		t.Fatal("a still reusable")
	}
	db, _ := p.Decision("b")
	if db.RerunReason != resume.RerunDepsChanged {
		t.Fatalf("b=%s want deps_changed", db.RerunReason)
	}
}

// A cache-disabled task always runs but does not invalidate unrelated branches.
func TestBuild_CacheDisabledRunsAlone(t *testing.T) {
	a := task("a", nil, nil, false)
	b := task("b", []string{"a"}, nil, false)
	cp := &resume.Checkpoint{Pipeline: "p", Tasks: []resume.TaskSnapshot{
		snap(a, pipeline.StatusSucceeded), snap(b, pipeline.StatusSucceeded),
	}}
	a2 := task("a", nil, nil, true) // now DisableCache
	p, err := resume.Build([]*pipeline.Task{a2, b}, cp, "env")
	if err != nil {
		t.Fatal(err)
	}
	da, _ := p.Decision("a")
	if da.RerunReason != resume.RerunCacheDisabled {
		t.Fatalf("a=%s", da.RerunReason)
	}
	db, _ := p.Decision("b")
	if db.RerunReason != resume.RerunDepReran {
		t.Fatalf("b=%s", db.RerunReason)
	}
}

// Ambient environment change is indistinguishable from an input change.
func TestBuild_EnvChangeInvalidates(t *testing.T) {
	a := task("a", nil, nil, false)
	cp := &resume.Checkpoint{Pipeline: "p", Tasks: []resume.TaskSnapshot{
		snap(a, pipeline.StatusSucceeded),
	}}
	p, err := resume.Build([]*pipeline.Task{a}, cp, "different-env")
	if err != nil {
		t.Fatal(err)
	}
	da, _ := p.Decision("a")
	if da.RerunReason != resume.RerunInputChanged {
		t.Fatalf("a=%s want input_changed on env change", da.RerunReason)
	}
}
