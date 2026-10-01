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
3. the task does **not** declare `DisableCache` (see below);
4. its current **input basis** equals the recorded basis.

Everything else is executed:

- a task that **failed, timed out, was canceled or was skipped** last time;
- a task whose own inputs changed;
- a **new** task or a **renamed** task (a rename is a new id);
- a task with **`DisableCache: true`** (invalidation code `cache_disabled`);
- any task downstream of such a change whose recorded basis no longer
  matches the fresh dependency outputs.

Unconditionally reused tasks are pre-concluded before the worker pool
starts: their bodies never run, they make no attempts, and their downstream
tasks unblock immediately.

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

### Cache-disabled tasks always re-execute on resume

`DisableCache: true` means the task has side effects and must really run on
every run. Resume honors that: a cache-disabled task is **never carried
over**, no matter how unchanged its inputs are; it re-executes with the
invalidation code `cache_disabled` (an empty `-cache` only disables the
content-addressed result cache, not resume).

That does **not** force a whole-graph rerun. The re-executed task's previous
success digest is still folded into downstream bases as a planning estimate,
so a downstream task stays reusable - but only **conditionally**: once the
cache-disabled dependency finishes, the scheduler recomputes the dependent's
basis from the *fresh* output digest.

- Output identical to the recorded one -> the dependent is carried over
  (its body never runs), exactly as a clean run would conclude.
- Output changed -> the dependent (and transitively its own dependents, by
  the same rule) re-executes, again exactly as a clean run would.

Independent and upstream tasks are unaffected either way. Because every
reuse is verified against the outputs a fresh run would have produced, the
final `conclusion` is identical to a clean from-scratch run in both cases
(`TestResume_CacheDisabledTaskReexecutes_OthersReused`,
`TestResume_CacheDisabledOutputChangeRerunsDownstream`).

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
2. in-flight tasks have their contexts canceled and are given
   `Config.ShutdownGrace` (hard-capped 60s, default 10s when unset) to
   finish;
3. **the run never hangs, even if a task ignores cancellation**: once the
   grace expires the run force-concludes *without waiting* for the
   uncooperative worker. The whole run is therefore bounded by
   `RunBudget + ShutdownGrace` no matter how a task misbehaves;
4. tasks that never launch, and in-flight tasks whose end was caused by the
   budget (including ones still stuck when the grace expired), are marked
   `budget_exhausted` with reason `run_budget_exhausted`;
5. tasks that already succeeded (or otherwise concluded) keep their
   recorded conclusion untouched - the wind-down never rewrites them;
6. the run **always returns a report**, with run status and `end_reason`
   set to `budget_exhausted`.

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
distinct status; `TestRunBudget_UncooperativeTaskConcludesBounded` runs a
task that never honors cancellation and asserts the run still concludes
within a bounded time with the right distinctions (repeated, also under
`-race`); `TestRunBudget_DistinctFromManualCancelAndTaskTimeout`
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
  `input_changed`, `cache_disabled`, or `executed` (a normal fresh
  execution);
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
