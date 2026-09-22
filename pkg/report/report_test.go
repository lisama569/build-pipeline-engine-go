package report

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/lisama569/build-pipeline-engine-go/pipeline"
	"github.com/lisama569/build-pipeline-engine-go/pkg/scheduler"
)

func res(id, status, reason, digest string, cached bool, attempts int) pipeline.TaskResult {
	return pipeline.TaskResult{
		TaskID: id, Status: pipeline.TaskStatus(status), Reason: reason,
		Output:    pipeline.ExecOutput{Digest: digest},
		Cached:    cached,
		Attempts:  attempts,
		StartedAt: time.Now(), EndedAt: time.Now(),
	}
}

func TestBuild_ColdAndWarmSameConclusion(t *testing.T) {
	ev := []scheduler.Event{{Seq: 1, TaskID: "a", Kind: "attempt", Detail: "x"}}
	cold := Build("p", map[string]pipeline.TaskResult{
		"a": res("a", "succeeded", "ok", "A", false, 1),
	}, ev)
	warm := Build("p", map[string]pipeline.TaskResult{
		"a": res("a", "cache_hit", "cache_hit", "A", true, 0),
	}, []scheduler.Event{{Seq: 1, TaskID: "a", Kind: "cache_hit", Detail: "y"}})
	if cold.Conclusion != warm.Conclusion {
		t.Fatalf("cold/warm conclusions differ:\n%s\n%s", cold.Conclusion, warm.Conclusion)
	}
	t.Logf("cold and warm runs converge on conclusion %s", cold.Conclusion[:16])
}

func TestBuild_DifferentDigestDifferentConclusion(t *testing.T) {
	a := Build("p", map[string]pipeline.TaskResult{"a": res("a", "succeeded", "ok", "A", false, 1)}, nil)
	b := Build("p", map[string]pipeline.TaskResult{"a": res("a", "succeeded", "ok", "B", false, 1)}, nil)
	if a.Conclusion == b.Conclusion {
		t.Fatal("different output digest must change the conclusion")
	}
}

func TestJSON_StableAndMachineReadable(t *testing.T) {
	r := Build("p", map[string]pipeline.TaskResult{"a": res("a", "failed", "task_failed", "", false, 1)}, nil)
	raw, err := r.JSON()
	if err != nil {
		t.Fatal(err)
	}
	raw2, _ := r.JSON()
	if string(raw) != string(raw2) {
		t.Fatal("JSON must be stable across serializations")
	}
	var probe map[string]any
	if err := json.Unmarshal(raw, &probe); err != nil {
		t.Fatalf("report must be valid JSON: %v", err)
	}
	for _, key := range []string{"pipeline", "status", "tasks", "trace", "conclusion", "non_determinism"} {
		if _, ok := probe[key]; !ok {
			t.Fatalf("missing field %q", key)
		}
	}
	if r.Status != "failed" {
		t.Fatalf("expected failed status, got %s", r.Status)
	}
	if len(r.NonDeterminism) == 0 {
		t.Fatal("non-deterministic sources must be explicit")
	}
	t.Logf("machine-readable report; explicit uncertainty: %d source(s)", len(r.NonDeterminism))
}

func TestText_ShowsOrderAndTrace(t *testing.T) {
	r := Build("p", map[string]pipeline.TaskResult{
		"a": res("a", "succeeded", "ok", "A", false, 1),
		"b": res("b", "skipped", "upstream:a", "", false, 0),
	}, []scheduler.Event{{Seq: 1, TaskID: "a", Kind: "finish", Detail: "ok"}})
	txt := r.Text()
	if !strings.Contains(txt, "scheduling order") || !strings.Contains(txt, "trace:") {
		t.Fatalf("text report must show order and trace:\n%s", txt)
	}
	if !strings.Contains(txt, "upstream:a") {
		t.Fatalf("skip reason must appear:\n%s", txt)
	}
}
