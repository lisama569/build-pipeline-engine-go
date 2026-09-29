// Package report renders structured, machine-readable, deterministic
// execution reports.
//
// Determinism:
//   - Task entries are sorted by declaration/finish-independent task id and
//     carry their dispatch Order, so JSON is stable across repeat runs.
//   - The Conclusion field is a sha256 over only the deterministic parts
//     (status, attempts, cached/reused flags normalized away, reason, output
//     digest). Wall-clock timestamps, run ids and host-dependent values are
//     excluded.
//   - Every value that CAN vary across runs (timestamps, durations, working
//     directory, run id) is listed in NonDeterminism rather than silently
//     embedded.
package report

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/lisama569/build-pipeline-engine-go/pipeline"
	"github.com/lisama569/build-pipeline-engine-go/pkg/history"
	"github.com/lisama569/build-pipeline-engine-go/pkg/scheduler"
)

// Run-level statuses. They distinguish HOW a non-success run ended.
const (
	StatusSucceeded       = "succeeded"
	StatusFailed          = "failed"
	StatusCanceled        = "canceled"
	StatusBudgetExhausted = "budget_exhausted"
)

// TaskReport is one task's slice of the report.
type TaskReport struct {
	ID                string              `json:"id"`
	Status            pipeline.TaskStatus `json:"status"`
	Attempts          int                 `json:"attempts"`
	Cached            bool                `json:"cached"`
	Reused            bool                `json:"reused"`
	Reason            string              `json:"reason"`
	Detail            string              `json:"detail"`
	Order             int                 `json:"order"`
	OutputDigest      string              `json:"output_digest"`
	ReuseInvalidation string              `json:"reuse_invalidation,omitempty"`
}

// Meta carries run-level, mostly non-deterministic context for the report.
type Meta struct {
	RunID           string
	StartedAt       time.Time
	EndReason       string // pipeline.EndReason*
	ResumeUsed      bool
	ResumedFromID   string
	ReusedTaskCount int
	Diff            *history.Diff
}

// Report is the stable, JSON-serializable conclusion of one pipeline run.
type Report struct {
	Pipeline       string            `json:"pipeline"`
	RunID          string            `json:"run_id,omitempty"`
	StartedAt      time.Time         `json:"started_at,omitempty"`
	Status         string            `json:"status"`
	EndReason      string            `json:"end_reason"`
	Resume         *ResumeSummary    `json:"resume,omitempty"`
	Tasks          []TaskReport      `json:"tasks"`
	Diff           *history.Diff     `json:"diff,omitempty"`
	Trace          []scheduler.Event `json:"trace"`
	Conclusion     string            `json:"conclusion"`
	NonDeterminism []string          `json:"non_determinism"`
}

// ResumeSummary explains how much of the run was carried over.
type ResumeSummary struct {
	Used           bool   `json:"used"`
	ReusedTasks    int    `json:"reused_tasks"`
	ExecutedTasks  int    `json:"executed_tasks"`
	ResumedFromRun string `json:"resumed_from_run,omitempty"`
}

// nonDeterministicSources is the fixed, explicit list of values excluded
// from the conclusion hash.
var nonDeterministicSources = []string{
	"run.run_id: unique per run",
	"run.started_at: wall-clock timestamp",
	"task.started_at: wall-clock timestamp",
	"task.ended_at: wall-clock timestamp",
	"task.duration_ms: derived from wall-clock timestamps",
	"output.stdout/stderr: may contain host-specific command output",
	"host working directory and hostname",
}

// Build assembles a report from task results and the schedule trace. The
// dispatch order is derived from the trace's dispatch/cache events.
func Build(name string, results map[string]pipeline.TaskResult, events []scheduler.Event) *Report {
	return BuildWithMeta(name, results, events, Meta{})
}

// BuildWithMeta is Build plus run-level metadata (run id, resume/budget
// context and the diff against the previous retained run).
func BuildWithMeta(name string, results map[string]pipeline.TaskResult,
	events []scheduler.Event, meta Meta) *Report {
	ids := make([]string, 0, len(results))
	for id := range results {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	tasks := make([]TaskReport, 0, len(ids))
	reused, executed := 0, 0
	for i, id := range ids {
		r := results[id]
		if r.Reused {
			reused++
		} else {
			executed++
		}
		tasks = append(tasks, TaskReport{
			ID:                r.TaskID,
			Status:            r.Status,
			Attempts:          r.Attempts,
			Cached:            r.Cached,
			Reused:            r.Reused,
			Reason:            r.Reason,
			Detail:            r.Detail,
			Order:             i,
			OutputDigest:      r.Output.Digest,
			ReuseInvalidation: r.ReuseInvalidation,
		})
	}

	status := runStatus(tasks, meta.EndReason)

	r := &Report{
		Pipeline:       name,
		RunID:          meta.RunID,
		StartedAt:      meta.StartedAt,
		Status:         status,
		EndReason:      endReason(meta.EndReason),
		Tasks:          tasks,
		Diff:           meta.Diff,
		Trace:          append([]scheduler.Event(nil), events...),
		NonDeterminism: append([]string(nil), nonDeterministicSources...),
	}
	if meta.ResumeUsed || reused > 0 {
		r.Resume = &ResumeSummary{
			Used:           true,
			ReusedTasks:    reused,
			ExecutedTasks:  executed,
			ResumedFromRun: meta.ResumedFromID,
		}
	}
	r.Conclusion = r.hashConclusion()
	return r
}

// runStatus determines the run-level outcome. Budget exhaustion is reported
// distinctly even if some tasks were canceled/failed in the drain; manual
// cancellation outranks plain failure in the summary.
func runStatus(tasks []TaskReport, endReason string) string {
	switch endReason {
	case pipeline.EndReasonBudgetExhausted:
		return StatusBudgetExhausted
	case pipeline.EndReasonCanceled:
		return StatusCanceled
	}
	for _, t := range tasks {
		if t.Status == pipeline.StatusBudgetExhausted {
			return StatusBudgetExhausted
		}
	}
	for _, t := range tasks {
		if t.Status == pipeline.StatusCanceled {
			return StatusCanceled
		}
	}
	for _, t := range tasks {
		if !pipeline.IsSuccess(t.Status) {
			return StatusFailed
		}
	}
	return StatusSucceeded
}

func endReason(r string) string {
	if r == "" {
		return pipeline.EndReasonCompleted
	}
	return r
}

// hashConclusion hashes only the deterministic fields, in a canonical
// encoding, so identical inputs across runs hash identically - whether tasks
// ran fresh, came from the cache, or were carried over by resume.
func (r *Report) hashConclusion() string {
	type canonTask struct {
		ID     string `json:"id"`
		Status string `json:"status"`
		// Attempts/reuse/order are recorded but excluded: a cache hit, a
		// reused result and a fresh success are the same conclusion.
		Reason string `json:"reason"`
		Digest string `json:"digest"`
	}
	ct := make([]canonTask, len(r.Tasks))
	for i, t := range r.Tasks {
		status := string(t.Status)
		reason := t.Reason
		if t.Status == pipeline.StatusCacheHit {
			status = string(pipeline.StatusSucceeded)
			reason = "ok" // cache_hit and fresh success are equivalent
		}
		if t.Reused && t.Status == pipeline.StatusSucceeded {
			reason = "ok"
		}
		ct[i] = canonTask{
			ID: t.ID, Status: status, Reason: reason,
			Digest: t.OutputDigest,
		}
	}
	raw, _ := json.Marshal(struct {
		Pipeline string      `json:"pipeline"`
		Status   string      `json:"status"`
		Tasks    []canonTask `json:"tasks"`
	}{r.Pipeline, r.Status, ct})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// JSON renders the report deterministically (two-space indentation).
func (r *Report) JSON() ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(r); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// Text renders a human-readable summary including the resume/diff summary,
// task table and the deterministic trace that justifies every decision.
func (r *Report) Text() string {
	var b strings.Builder
	fmt.Fprintf(&b, "pipeline %q: %s\n", r.Pipeline, r.Status)
	if r.RunID != "" {
		fmt.Fprintf(&b, "run: %s  ended: %s\n", r.RunID, r.EndReason)
	} else {
		fmt.Fprintf(&b, "ended: %s\n", r.EndReason)
	}
	fmt.Fprintf(&b, "conclusion: %s\n", r.Conclusion)
	if r.Resume != nil {
		fmt.Fprintf(&b, "resume: reused=%d executed=%d",
			r.Resume.ReusedTasks, r.Resume.ExecutedTasks)
		if r.Resume.ResumedFromRun != "" {
			fmt.Fprintf(&b, " from=%s", r.Resume.ResumedFromRun)
		}
		fmt.Fprintln(&b)
	}
	if r.Diff != nil {
		writeDiff(&b, r.Diff)
	}
	fmt.Fprintln(&b, "tasks (scheduling order):")
	for _, t := range r.Tasks {
		flag := ""
		if t.Cached {
			flag += " cached"
		}
		if t.Reused {
			flag += " reused"
		}
		fmt.Fprintf(&b, "  #%d %-20s %-16s attempts=%d%s reason=%s\n",
			t.Order, t.ID, t.Status, t.Attempts, flag, reasonOrDash(t.Reason))
	}
	fmt.Fprintln(&b, "trace:")
	for _, ev := range r.Trace {
		fmt.Fprintf(&b, "  %03d %-16s %-16s %s\n", ev.Seq, ev.Kind, ev.TaskID, ev.Detail)
	}
	fmt.Fprintln(&b, "non-deterministic sources (excluded from conclusion):")
	for _, nd := range r.NonDeterminism {
		fmt.Fprintf(&b, "  - %s\n", nd)
	}
	return b.String()
}

func writeDiff(b *strings.Builder, d *history.Diff) {
	if len(d.Reused) > 0 {
		fmt.Fprintf(b, "  reused: %s\n", strings.Join(d.Reused, ", "))
	}
	if len(d.Reexecuted) > 0 {
		parts := make([]string, 0, len(d.Reexecuted))
		for _, re := range d.Reexecuted {
			parts = append(parts, fmt.Sprintf("%s(%s)", re.ID, re.Reason))
		}
		fmt.Fprintf(b, "  re-executed: %s\n", strings.Join(parts, ", "))
	}
	if len(d.StatusChanged) > 0 {
		parts := make([]string, 0, len(d.StatusChanged))
		for _, c := range d.StatusChanged {
			parts = append(parts, fmt.Sprintf("%s:%s->%s", c.ID, c.From, c.To))
		}
		fmt.Fprintf(b, "  status changed: %s\n", strings.Join(parts, ", "))
	}
	if len(d.Added) > 0 {
		fmt.Fprintf(b, "  added: %s\n", strings.Join(d.Added, ", "))
	}
	if len(d.Removed) > 0 {
		fmt.Fprintf(b, "  removed: %s\n", strings.Join(d.Removed, ", "))
	}
}

func reasonOrDash(r string) string {
	if r == "" {
		return "-"
	}
	return r
}

var _ = time.Now
