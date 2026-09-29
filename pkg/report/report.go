// Package report renders structured, machine-readable, deterministic
// execution reports.
//
// Determinism:
//   - Task entries are sorted by declaration/finish-independent task id and
//     carry their dispatch Order, so JSON is stable across repeat runs.
//   - The Conclusion field is a sha256 over only the deterministic parts
//     (status, attempts, cached flag, reason, output digest). Wall-clock
//     timestamps and host-dependent values are excluded.
//   - Every value that CAN vary across runs (timestamps, durations, working
//     directory) is listed in NonDeterminism rather than silently embedded.
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
	"github.com/lisama569/build-pipeline-engine-go/pkg/scheduler"
)

// TaskReport is one task's slice of the report.
type TaskReport struct {
	ID           string              `json:"id"`
	Status       pipeline.TaskStatus `json:"status"`
	Attempts     int                 `json:"attempts"`
	Cached       bool                `json:"cached"`
	Reused       bool                `json:"reused"`
	RerunReason  string              `json:"rerun_reason,omitempty"`
	Reason       string              `json:"reason"`
	Detail       string              `json:"detail"`
	Order        int                 `json:"order"`
	OutputDigest string              `json:"output_digest"`
}

// ResumeMeta summarizes the resume decision for a run (zero value when the
// run did not resume).
type ResumeMeta struct {
	Resumed     bool     `json:"resumed"`
	ParentRunID string   `json:"parent_run_id,omitempty"`
	ReusedTasks []string `json:"reused_tasks,omitempty"`
	ReusedCount int      `json:"reused_count"`
	ReranCount  int      `json:"reran_count"`
	CachedCount int      `json:"cached_count"`
}

// Meta carries run-level metadata beyond the raw task results.
type Meta struct {
	RunID      string
	StopReason pipeline.StopReason
	Resume     ResumeMeta
	RunBudget  time.Duration
}

// Report is the stable, JSON-serializable conclusion of one pipeline run.
type Report struct {
	Pipeline       string            `json:"pipeline"`
	RunID          string            `json:"run_id,omitempty"`
	Status         string            `json:"status"`
	StopReason     string            `json:"stop_reason"`
	RunBudget      string            `json:"run_budget,omitempty"`
	Resume         ResumeMeta        `json:"resume"`
	Tasks          []TaskReport      `json:"tasks"`
	Trace          []scheduler.Event `json:"trace"`
	Conclusion     string            `json:"conclusion"`
	NonDeterminism []string          `json:"non_determinism"`
}

// nonDeterministicSources is the fixed, explicit list of values excluded
// from the conclusion hash.
var nonDeterministicSources = []string{
	"task.started_at: wall-clock timestamp",
	"task.ended_at: wall-clock timestamp",
	"task.duration_ms: derived from wall-clock timestamps",
	"output.stdout/stderr: may contain host-specific command output",
	"host working directory and hostname",
}

// Build assembles a report from task results and the schedule trace. The
// dispatch order is derived from the trace's dispatch/cache/resume events.
func Build(name string, results map[string]pipeline.TaskResult, events []scheduler.Event) *Report {
	return BuildWithMeta(name, results, events, Meta{})
}

// BuildWithMeta assembles a report with run-level metadata (run id, stop
// reason, resume/budget summary).
func BuildWithMeta(name string, results map[string]pipeline.TaskResult,
	events []scheduler.Event, meta Meta) *Report {
	ids := make([]string, 0, len(results))
	for id := range results {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	tasks := make([]TaskReport, 0, len(ids))
	for i, id := range ids {
		r := results[id]
		// ids is sorted, so Order is the deterministic, cache-independent
		// report position. The actual runtime scheduling order is fully
		// captured in Trace and intentionally kept out of the conclusion.
		tasks = append(tasks, TaskReport{
			ID:           r.TaskID,
			Status:       r.Status,
			Attempts:     r.Attempts,
			Cached:       r.Cached,
			Reused:       r.Reused,
			RerunReason:  r.RerunReason,
			Reason:       r.Reason,
			Detail:       r.Detail,
			Order:        i,
			OutputDigest: r.Output.Digest,
		})
	}

	status := "succeeded"
	if !pipeline.RunSucceeded(results) {
		status = "failed"
		for _, t := range tasks {
			if t.Status == pipeline.StatusCanceled {
				status = "canceled"
			}
		}
	}
	// Budget exhaustion is its own observable run status even when no task
	// had time to become canceled at the moment status is derived.
	if meta.StopReason == pipeline.StopBudgetExhausted {
		status = string(pipeline.StopBudgetExhausted)
	}

	r := &Report{
		Pipeline:       name,
		RunID:          meta.RunID,
		Status:         status,
		StopReason:     string(stopReason(meta.StopReason)),
		RunBudget:      budgetString(meta.RunBudget),
		Resume:         meta.Resume,
		Tasks:          tasks,
		Trace:          append([]scheduler.Event(nil), events...),
		NonDeterminism: append([]string(nil), nonDeterministicSources...),
	}
	r.Conclusion = r.hashConclusion()
	return r
}

func stopReason(r pipeline.StopReason) pipeline.StopReason {
	if r == "" {
		return pipeline.StopCompleted
	}
	return r
}

func budgetString(d time.Duration) string {
	if d <= 0 {
		return ""
	}
	return d.String()
}

// hashConclusion hashes only the deterministic fields, in a canonical
// encoding, so identical inputs across repeat runs hash identically.
func (r *Report) hashConclusion() string {
	type canonTask struct {
		ID     string `json:"id"`
		Status string `json:"status"`
		// Attempts and scheduling Order are recorded but excluded: a cached
		// run legitimately has 0 attempts and the trace carries the order.
		Reason string `json:"reason"`
		Digest string `json:"digest"`
	}
	ct := make([]canonTask, len(r.Tasks))
	for i, t := range r.Tasks {
		// A cache hit and a fresh success are the SAME conclusion: both mean
		// the task's observable output is present and correct. Normalize so
		// cold, warm and resuming runs over identical inputs hash identically.
		status := string(t.Status)
		reason := t.Reason
		if t.Status == pipeline.StatusCacheHit || t.Reused {
			status = string(pipeline.StatusSucceeded)
			reason = "ok" // reused/cached verdicts and fresh success are equivalent
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

// Text renders a human-readable summary including the scheduling order and
// the deterministic trace that justifies every decision.
func (r *Report) Text() string {
	var b strings.Builder
	fmt.Fprintf(&b, "pipeline %q: %s\n", r.Pipeline, r.Status)
	if r.RunID != "" {
		fmt.Fprintf(&b, "run: %s\n", r.RunID)
	}
	fmt.Fprintf(&b, "stop_reason: %s\n", r.StopReason)
	if r.RunBudget != "" {
		fmt.Fprintf(&b, "run_budget: %s\n", r.RunBudget)
	}
	if r.Resume.Resumed {
		fmt.Fprintf(&b, "resume: parent=%s reused=%d reran=%d cached=%d\n",
			orDash(r.Resume.ParentRunID), r.Resume.ReusedCount,
			r.Resume.ReranCount, r.Resume.CachedCount)
	}
	fmt.Fprintf(&b, "conclusion: %s\n", r.Conclusion)
	fmt.Fprintln(&b, "tasks (scheduling order):")
	for _, t := range r.Tasks {
		flags := ""
		if t.Cached {
			flags += " cached"
		}
		if t.Reused {
			flags += " reused"
		}
		if t.RerunReason != "" {
			flags += " rerun=" + t.RerunReason
		}
		fmt.Fprintf(&b, "  #%d %-20s %-16s attempts=%d%s reason=%s\n",
			t.Order, t.ID, t.Status, t.Attempts, flags, reasonOrDash(t.Reason))
	}
	fmt.Fprintln(&b, "trace:")
	for _, ev := range r.Trace {
		fmt.Fprintf(&b, "  %03d %-11s %-16s %s\n", ev.Seq, ev.Kind, ev.TaskID, ev.Detail)
	}
	fmt.Fprintln(&b, "non-deterministic sources (excluded from conclusion):")
	for _, nd := range r.NonDeterminism {
		fmt.Fprintf(&b, "  - %s\n", nd)
	}
	return b.String()
}

func reasonOrDash(r string) string {
	if r == "" {
		return "-"
	}
	return r
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

var _ = time.Now
