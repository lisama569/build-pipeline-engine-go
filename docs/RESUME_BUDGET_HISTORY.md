# Resume, run budgets and run history

This document covers three capabilities added on top of the single-run
engine: **resume from the last run**, a **run-wide time budget**, and
**bounded, comparable run history**. Everything stays fully local: no
network, credentials or paid services are involved.

## 1. Resume ("接着上次跑")

### What gets carried over

A run starts from the last successful conclusion of the same pipeline and
**reuses a task only when all hold**:

1. the task id existed in the previous graph;
2. its previous result was a success (`succeeded` / `cache_hit`);
3. its current **input basis** equals the recorded basis;
4. the task does **not** set `DisableCache` (see below).

Everything else is executed:

- a task that **failed, timed out, was canceled or was skipped** last time;
- a task whose own inputs changed;
- a **new** task or a **renamed** task (a rename is a new id);
- a task with **`DisableCache: true`** (its side effects must run every
  time), reported with invalidation reason `cache_disabled`;
- any task downstream of such a change.

Reuse is decided **lazily, the moment a task becomes ready**: the scheduler
recomputes the task's basis from the *actual* conclusions of its
dependencies and compares it with the planned basis. A reused task's body
never runs, it makes no attempts, and its downstream tasks unblock
immediately.

### Input basis (why graph edits can never be faked)

The basis is computed in topological order and chains dependencies:

```
basis(t) = sha256( fingerprint(t)
                   || for d in deps(t) (declared order):
                        depID(d) || basis(d) || outputDigest(d) )

fingerprint(t) = the existing content fingerprint:
                 taskID || canonical(command) || envHash || sorted(CacheKeyInputs)
```

Consequences:

- Editing a task's command, declared inputs, environment or id changes its
  fingerprint, hence its basis.
- Adding/removing/repointing a dependency edge changes the dep chain.
- A changed **upstream output digest** changes every downstream basis, even
  if that downstream task's own inputs are identical.
- Renaming a task changes both its own id and its dependents' dep chains.
- **Deleting** a task simply removes it; nothing can take its place, because
  reuse is keyed by id and stored bases never match an unrelated task.

The successful subset is always persisted, even when a run ends with a
failure or an exhausted budget, so the next run reuses everything that did
succeed and only re-executes the affected part.

### Resume and cache-disabled tasks

`DisableCache: true` means "this task has side effects and must really run
every time". Resume honors that: a cache-disabled task is **always
re-executed**, never carried over, even when its inputs are unchanged (its
report entry shows invalidation reason `cache_disabled`).

This does **not** cascade into a whole-graph rerun:

- The plan folds the task's *stored* output digest into its dependents'
  bases, so an unchanged downstream task stays a reuse candidate.
- When the re-executed task finishes, the scheduler verifies the fresh
  digest: if it matches the stored one, downstream tasks are reused as
  planned; if it differs, exactly the affected downstream subtree
  re-executes (invalidation `input_changed`) - precisely what a clean run
  would do.

Either way the final `conclusion` is identical to a clean from-scratch run
(`TestResume_DisableCacheTaskReexecutes_OthersReused_ConclusionMatchesCleanRun`,
`TestResume_DisableCacheDigestChangeCascades`).

### Same final conclusion as a clean run

A reused success and a fresh success are normalized identically in the
report (the `reused`/`cached`/attempt fields are excluded from the
`conclusion` hash), and the basis chain guarantees the reused outputs are
exactly the outputs a fresh run would have produced. Therefore **resuming
with the same inputs yields the same `conclusion` as a clean from-scratch
run**. This is asserted by
`TestResume_OnlyReexecutesChangedSubtree_ConclusionMatchesCleanRun` and
`TestResume_FailedTaskRerunsNeighborsReused`.

### Storage and concurrency

Snapshots live under `Config.StateDir / <pipeline>/snapshot.json`, written
temp-file + fsync + rename (a killed run leaves at most an ignored temp
file). A missing or corrupt snapshot degrades to a clean run, never to a
wrong reuse. Reads and writes are guarded by an exclusive cross-process
`flock` on the pipeline directory, so concurrent runs of the same pipeline
serialize rather than overwrite each other.

## 2. Run-wide time budget

`Config.RunBudget` (CLI `-run-budget`, e.g. `10m`) bounds the wall-clock
time of an entire run:

1. when the budget is used up, **no new task is dispatched**;
2. in-flight tasks have their contexts canceled and converge bounded by
   `Config.ShutdownGrace` (hard-capped 60s);
3. tasks that never launch, and in-flight tasks whose end was caused by the
   budget, are marked `budget_exhausted` with reason `run_budget_exhausted`;
4. the run **always returns a report within `RunBudget + ShutdownGrace`**,
   even when a task body ignores cancellation and stays stuck in its own
   loop: after the grace window the run force-converges, finalizes every
   unfinished task as `budget_exhausted` and returns without waiting for
   the uncooperative worker. Tasks that already reached a conclusion
   (succeeded/failed/skipped) are never rewritten by this finalization.

The budget is distinct from the other stop causes:

| Cause | Task status | Run status / `end_reason` |
|-------|-------------|---------------------------|
| run budget used up | `budget_exhausted` | `budget_exhausted` |
| per-task deadline | `timed_out` (`task_timeout`) | run continues/`failed` |
| manual interrupt (SIGINT/SIGTERM) / fail-fast | `canceled` (`task_canceled`) | `canceled` |

A `timed_out` task is the task's own deadline and is never retried; the run
budget is a single timer for the whole run and is independent of every task
timeout. The CLI exits with code `3` on budget exhaustion, distinct from
failure (`1`) and usage errors (`2`).

`TestRunBudget_ExhaustionConvergesAndDistinct` verifies convergence and the
distinct status; `TestRunBudget_UncooperativeTaskStillConcludesBounded`
pins the bounded conclusion when a task ignores cancellation (stable across
repeated `-race` runs); `TestRunBudget_DistinctFromManualCancelAndTaskTimeout`
locks in all three outcomes.

## 3. Run history

When `Config.HistoryDir` is set, each run appends one immutable JSON record
to `HistoryDir / <pipeline>/`:

```
<UTC timestamp>-<run id>.json
```

A record stores per task: id, status, `reused`/`cached`, reason, output
digest and the resume invalidation code; plus the run's status and
`conclusion`. The report's `diff` section compares the new run with the most
recent retained record:

- `reused`: carried over from the previous run;
- `re-executed`: with the reason `new_task`, `previous_not_success`,
  `input_changed`, `cache_disabled` (a `DisableCache` task always re-runs),
  or `executed` (a normal fresh execution);
- `status_changed`: tasks whose status differs, as `id:from->to`;
- `added` / `removed`: graph membership changes.

Retention is bounded: `Config.HistoryRuns` (CLI `-history-runs`) defaults to
10 and is hard-capped at 100; older files are deleted after each append, so
history cannot grow without limit. Appends and pruning are serialized by
the same cross-process flock model (one shared lock when state and history
use the same root), so overlapping runs never interleave or corrupt
records (`TestHistory_ConcurrentRunsDoNotCorrupt`).

## 4. Configuration

| `pipeline.Config` / CLI | Meaning | Default | Bound |
|---|---|---|---|
| `StateDir` / `-state` | resume snapshot root | `.pipeline-state` | `""` disables |
| `HistoryDir` / `-history` | run history root | `.pipeline-history` | `""` disables |
| `HistoryRuns` / `-history-runs` | records kept per pipeline | 10 | 1..100 |
| `RunBudget` / `-run-budget` | whole-run wall-clock budget | none | `0` disables |

State and history are **opt-out compatible**: with both directories empty
the engine behaves exactly as before (cache-backed single runs); existing
reports simply gain the `end_reason` field and omit the new sections.

## 5. Local verification (offline)

```bash
gofmt -l .
go vet ./...
go test ./...
go test -race ./...

# the two guarantees of this document, stressed (offline, in-process tasks):
go test ./engine/ -run 'TestRunBudget_UncooperativeTaskStillConcludesBounded' -count=10 -race
go test ./engine/ -run 'TestResume_DisableCache' -count=10 -race

# manual CLI checks using only the repo's own go toolchain (no services):
go run ./cmd/pipeline -spec examples/pipeline.json -state .ps -history .ph
go run ./cmd/pipeline -spec examples/pipeline.json -state .ps -history .ph   # reuse
go run ./cmd/pipeline -spec examples/pipeline.json -state "" -history "" \
  -run-budget 1ns                                            # budget path
```

All automated tests use in-process functions and POSIX shell commands;
nothing contacts an external service, uses real credentials or costs money.
The scratch directories (`.pipeline-cache`, `.pipeline-state`,
`.pipeline-history`) are gitignored.
