package history

import (
	"sort"

	"github.com/lisama569/build-pipeline-engine-go/pipeline"
)

// Compare builds a Diff between two retained records (older first, newer
// second). It reports verdicts reused in the newer run, tasks new/removed
// between the graphs, tasks that re-executed with the stable rerun reason, and
// tasks whose normalized conclusion status changed.
func Compare(older, newer *Record) *Diff {
	d := &Diff{}
	if older != nil {
		d.OlderRunID = older.RunID
	}
	if newer != nil {
		d.NewerRunID = newer.RunID
	}
	if older == nil || newer == nil {
		if newer != nil {
			for _, t := range newer.Tasks {
				d.New = append(d.New, t.ID)
			}
			sort.Strings(d.New)
		}
		return d
	}

	old := make(map[string]TaskRecord, len(older.Tasks))
	for _, t := range older.Tasks {
		old[t.ID] = t
	}
	newm := make(map[string]TaskRecord, len(newer.Tasks))
	for _, t := range newer.Tasks {
		newm[t.ID] = t
	}

	for _, t := range newer.Tasks {
		prev, known := old[t.ID]
		if !known {
			d.New = append(d.New, t.ID)
			continue
		}
		if t.Reused {
			d.Reused = append(d.Reused, t.ID)
		} else {
			reason := t.RerunReason
			if reason == "" {
				// Older run existed but the newer run did not resume (clean
				// run): still classify relative to the prior status.
				reason = reasonFromPrior(prev.Status)
			}
			d.Reran = append(d.Reran, ReranTask{ID: t.ID, Reason: reason})
		}
		if normalizeStatus(prev.Status) != normalizeStatus(t.Status) {
			d.StatusChanged = append(d.StatusChanged, StatusChange{
				ID: t.ID, Before: prev.Status, After: t.Status,
			})
		}
	}
	for _, t := range older.Tasks {
		if _, known := newm[t.ID]; !known {
			d.Removed = append(d.Removed, t.ID)
		}
	}
	sort.Strings(d.New)
	sort.Strings(d.Removed)
	sort.Strings(d.Reused)
	sort.Slice(d.Reran, func(i, j int) bool { return d.Reran[i].ID < d.Reran[j].ID })
	sort.Slice(d.StatusChanged, func(i, j int) bool { return d.StatusChanged[i].ID < d.StatusChanged[j].ID })
	return d
}

// normalizeStatus treats cache hit and reused verdicts as success for status
// comparison purposes (they are observally the same conclusion).
func normalizeStatus(st pipeline.TaskStatus) pipeline.TaskStatus {
	if st == pipeline.StatusCacheHit {
		return "succeeded"
	}
	return st
}

// reasonFromPrior maps a prior task status to the rerun reason used when a
// newer clean run re-executed a task known from the older record.
func reasonFromPrior(st pipeline.TaskStatus) string {
	switch st {
	case pipeline.StatusTimedOut:
		return "prev_timed_out"
	case pipeline.StatusSkipped:
		return "prev_skipped"
	case pipeline.StatusCanceled:
		return "prev_canceled"
	case pipeline.StatusSucceeded, pipeline.StatusCacheHit:
		return "clean_run"
	default:
		return "prev_failed"
	}
}
