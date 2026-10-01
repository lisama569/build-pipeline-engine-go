package history

import "sort"

// Diff describes how a new run compares with the previous retained record.
// All slices are sorted by task id for deterministic output.
type Diff struct {
	PreviousID    string           `json:"previous_id,omitempty"`
	Reused        []string         `json:"reused,omitempty"`
	Reexecuted    []ReexecutedTask `json:"reexecuted,omitempty"`
	StatusChanged []StatusChange   `json:"status_changed,omitempty"`
	Added         []string         `json:"added,omitempty"`
	Removed       []string         `json:"removed,omitempty"`
}

// ReexecutedTask names a task that re-ran and why. The reason is the stable
// invalidation code from pkg/state (new_task / previous_not_success /
// input_changed / cache_disabled); "executed" means the task simply ran
// (cold/no resume).
type ReexecutedTask struct {
	ID     string `json:"id"`
	Reason string `json:"reason"`
}

// StatusChange records a task status transition between two runs. Reused
// tasks never appear here: reuse preserves the exact previous conclusion.
type StatusChange struct {
	ID   string `json:"id"`
	From string `json:"from"`
	To   string `json:"to"`
}

// Compare builds the diff from prev to rec. A nil prev means every task in
// rec is "added". A reused task is one flagged Reused on the new record.
func Compare(prev *Record, rec *Record) *Diff {
	d := &Diff{}
	if rec == nil {
		return d
	}
	newTasks := make(map[string]TaskRecord, len(rec.Tasks))
	for _, t := range rec.Tasks {
		newTasks[t.ID] = t
	}
	if prev == nil {
		for _, t := range rec.Tasks {
			d.Added = append(d.Added, t.ID)
		}
		sort.Strings(d.Added)
		return d
	}
	d.PreviousID = prev.ID
	oldTasks := make(map[string]TaskRecord, len(prev.Tasks))
	for _, t := range prev.Tasks {
		oldTasks[t.ID] = t
	}

	for _, t := range rec.Tasks {
		old, existed := oldTasks[t.ID]
		switch {
		case !existed:
			d.Added = append(d.Added, t.ID)
		case t.Reused:
			d.Reused = append(d.Reused, t.ID)
		default:
			reason := t.ReuseInvalidation
			if reason == "" {
				reason = "executed"
			}
			d.Reexecuted = append(d.Reexecuted, ReexecutedTask{ID: t.ID, Reason: reason})
			if old.Status != t.Status {
				d.StatusChanged = append(d.StatusChanged,
					StatusChange{ID: t.ID, From: old.Status, To: t.Status})
			}
		}
	}
	for id := range oldTasks {
		if _, ok := newTasks[id]; !ok {
			d.Removed = append(d.Removed, id)
		}
	}
	sort.Strings(d.Reused)
	sort.Strings(d.Added)
	sort.Strings(d.Removed)
	sort.Slice(d.Reexecuted, func(i, j int) bool { return d.Reexecuted[i].ID < d.Reexecuted[j].ID })
	sort.Slice(d.StatusChanged, func(i, j int) bool { return d.StatusChanged[i].ID < d.StatusChanged[j].ID })
	return d
}
