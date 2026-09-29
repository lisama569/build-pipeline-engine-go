package report

import (
	"testing"

	"github.com/lisama569/build-pipeline-engine-go/pipeline"
)

// TestBuild_ReusedNormalizesLikeCacheHit: a reused verdict and a fresh
// success hash identically (resume conclusion == clean conclusion).
func TestBuild_ReusedNormalizesLikeCacheHit(t *testing.T) {
	clean := Build("p", map[string]pipeline.TaskResult{
		"a": res("a", "succeeded", "ok", "A", false, 1),
	}, nil)
	reused := BuildWithMeta("p", map[string]pipeline.TaskResult{
		"a": {TaskID: "a", Status: pipeline.StatusSucceeded, Reason: "ok",
			Output: out("A"), Reused: true},
	}, nil, Meta{Resume: ResumeMeta{Resumed: true, ReusedCount: 1}})
	if clean.Conclusion != reused.Conclusion {
		t.Fatalf("reused conclusion %s != clean %s", reused.Conclusion, clean.Conclusion)
	}
	tr := reused.Tasks[0]
	if !tr.Reused {
		t.Fatal("task must be marked reused")
	}
	if !reused.Resume.Resumed {
		t.Fatal("resume metadata must be surfaced")
	}
}

// TestBuild_BudgetExhaustedStatusAndReason.
func TestBuild_BudgetExhaustedStatusAndReason(t *testing.T) {
	r := BuildWithMeta("p", map[string]pipeline.TaskResult{
		"a": {TaskID: "a", Status: pipeline.StatusCanceled,
			Reason: string(pipeline.ErrRunBudget)},
	}, nil, Meta{StopReason: pipeline.StopBudgetExhausted, RunBudget: 10 * 60 * 1e9})
	if r.Status != "budget_exhausted" {
		t.Fatalf("status=%s", r.Status)
	}
	if r.StopReason != "budget_exhausted" {
		t.Fatalf("stop_reason=%s", r.StopReason)
	}
	if r.RunBudget != "10m0s" {
		t.Fatalf("budget=%s", r.RunBudget)
	}
}

// TestBuild_FailFastAndCanceledDistinguished.
func TestBuild_FailFastAndCanceledDistinguished(t *testing.T) {
	r := BuildWithMeta("p", map[string]pipeline.TaskResult{
		"a": {TaskID: "a", Status: pipeline.StatusCanceled,
			Reason: string(pipeline.ErrTaskCanceled)},
	}, nil, Meta{StopReason: pipeline.StopFailFast})
	if r.StopReason != "fail_fast" {
		t.Fatalf("stop_reason=%s", r.StopReason)
	}
	if r.Status != "canceled" {
		t.Fatalf("status=%s", r.Status)
	}
}

func out(digest string) pipeline.ExecOutput {
	return pipeline.ExecOutput{Digest: digest}
}
