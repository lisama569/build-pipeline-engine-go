# Resume, run budget and run history

Three local-iteration features layer on top of the existing single-run engine
without changing its semantics: a resuming run over the same inputs reaches
the **exact same conclusion** as a clean run, a run-wide budget is a distinct
stop cause, and recent runs are retained for comparison. Nothing here
contacts an external service or uses credentials.

## 1. Resume ("continue from the last run")

Enable with `Config.Resume = true` plus a `Config.StateDir` (CLI: `-state
<dir> -resume`). Whenever `StateDir` is set, every run writes a per-pipeline
**checkpoint** of its verdicts; with `Resume` the next run loads it and
carries over still-valid successes instead of re-executing them.

### What is reused

A task's previous verdict is reused only when **all** of these hold:

1. the task existed in the previous run and ended in a success status
   (`succeeded` or `cache_hit`);
2. its **fingerprint is unchanged** — task id, command line, environment
   fingerprint and every `CacheKeyInputs` entry (see
   [CACHE_AND_RETRY.md](CACHE_AND_RETRY.md));
3. its declared **dependency list is identical**;
4. every (transitive) dependency is itself reusable;
5. the task has not opted out with `DisableCache: true`.

### What re-executes, and how it is reported

Each executed task carries a stable machine-readable `rerun_reason` in the
report/history:

| `rerun_reason` | When |
|----------------|------|
| `new_task` | task did not exist in the previous run |
| `input_changed` | fingerprint (inputs/command/environment) differs |
| `deps_changed` | declared dependency list was restructured |
| `prev_failed` / `prev_timed_out` / `prev_skipped` / `prev_canceled` | previous verdict was non-success |
| `cache_disabled` | task has `DisableCache: true` |
| `dep_reran` | a dependency had to re-execute (previously-success node in the changed closure) |

Consequences:

- **Graph edits are safe.** Renaming a task is a removal plus `new_task` (the
  old verdict is never served under the new id). Adding/removing an edge yields
  `deps_changed` on the node and `dep_reran` on the previously-successful
  closure. Deleting a task only invalidates nodes that depended on it.
- **One `DisableCache` task never forces a full rerun.** That task executes
  (and its downstream closure), while unrelated branches are reused.
- Previously **failed/timed-out/skipped/canceled** tasks always re-execute,
  and the closure behind a re-executed task re-executes too; this is what
  guarantees the final graph is consistent rather than pieced together from
  stale upstream outputs.
- Reused tasks keep their prior status (`cache_hit` stays `cache_hit`) and are
  additionally flagged `reused: true`; their bodies never run (no side
  effects), like cache hits.
- A checksum-invalid checkpoint is rejected (`state_corrupt`), quarantined
  (`*.bad`) and the run proceeds clean. It can never contribute a verdict.

### Equivalence guarantee

The `conclusion` hash normalizes `cache_hit` **and** reused verdicts to fresh
success (status plus `ok` reason plus output digest). Therefore

```
conclusion(resume run over inputs X) == conclusion(clean run over inputs X)
```

even though only part of the graph executed. Attempt counts, timestamps and
the schedule trace remain per-run and are excluded from the hash (see
[CACHE_AND_RETRY.md](CACHE_AND_RETRY.md)). This is covered by
`TestResume_EquivalentToCleanRun_Diamond` and the repeated-inputs tests.

### Interaction with the content cache

Resume and the content cache are independent layers and may be used together
or separately:

- resume state (`StateDir`): per-pipeline "last verdict" checkpoint, used only
  when `Resume` is on;
- content cache (`CacheDir`): content-addressed results shared across
  pipelines, consulted for every task that actually executes.

A reused resume task does not even reach the cache layer. A re-executed
`DisableCache` task still bypasses the content cache as before.

## 2. Run-wide time budget

`Config.RunBudget` (CLI `-run-budget 10m`; zero = unlimited) bounds the total
wall-clock duration of **one run**.

When the budget elapses:

1. no further task is dispatched;
2. running tasks receive cancellation through their attempt contexts (and OS
   process groups are SIGKILLed for command tasks), exactly like a manual
   cancel;
3. never-launched tasks are concluded `canceled` with reason
   `run_budget_exhausted`;
4. the run converges immediately when in-flight work ends, with
   `Config.ShutdownGrace` as a hard backstop if a task ignores cancellation.

The cause is always distinguishable in the report:

| Field | Natural end | Manual interrupt | Fail-fast | Budget exhausted |
|-------|-------------|------------------|-----------|------------------|
| `stop_reason` | `completed` | `canceled` | `fail_fast` | `budget_exhausted` |
| run `status` | `succeeded`/`failed` | `canceled` | `canceled` | `budget_exhausted` |

A single task exceeding its **own** `Timeout` is still `timed_out`
(`task_timeout`, never retried) and - with a generous budget - the run ends
naturally as `failed`; the two timeouts are never confused. The budget is
clamped to a 24h hard ceiling; negative values are `config_invalid`.

## 3. Bounded run history

`Config.HistoryRuns` (CLI `-history N`, default 10, hard cap 50, zero
disables) retains the newest N run records **per pipeline** under
`<StateDir>/history/<pipeline>/<run-id>.json`.

Each record stores run id, parent run id, start/end time, `stop_reason`,
overall status, conclusion hash, and per-task status/reason/cached/reused/
`rerun_reason`/digest. Records are tamper-evident (the same checksummed
envelope plus atomic temp/fsync/rename as checkpoints) and are written to
unique run-id files; per-pipeline directories and atomic renames mean
**concurrent runs - including several pipelines on one engine - never
overwrite or corrupt each other**. After each write the directory is pruned
back to N.

Compare two runs in-process via `history.Compare(older, newer)` or the CLI:

```bash
# list retained runs (newest first)
go run ./cmd/pipeline history -state .pipeline-state -pipeline self-check

# compare a run against the latest
go run ./cmd/pipeline history -state .pipeline-state \
    -pipeline self-check -diff <older-run-id>

# JSON for tooling
go run ./cmd/pipeline history -state .pipeline-state \
    -pipeline self-check -json
```

The diff reports `reused`, `new`, `removed`, `reran` (with the rerun reason:
input change vs. previous failure, ...) and `status_changed` tasks.
`cache_hit`/reused normalize to success when deciding whether a status
"changed".

## Configuration reference (new fields)

| `pipeline.Config` / CLI | Meaning | Default | Bound |
|-------------------------|---------|---------|-------|
| `RunBudget` / `-run-budget` | total run wall-clock budget | 0 (off) | <= 24h |
| `StateDir` / `-state` | checkpoint + history directory | `.pipeline-state` | `""` disables both |
| `Resume` / `-resume` | carry over valid verdicts | false | requires `StateDir` |
| (n/a) / `-no-resume` | force a clean run despite state | - | - |
| `HistoryRuns` / `-history` | records kept per pipeline | 10 | 0 off, <= 50 |

## Local verification (offline)

```bash
gofmt -l .
go vet ./...
go test ./...
go test -race ./...
```

All new tests use in-process `ExecFunc`s and POSIX `sleep` only; they need no
network, credentials or paid resources. Focus tests:

- resume only touches what it must and matches a clean conclusion:
  `go test ./engine -run TestResume`
- budget converges and is distinguishable:
  `go test ./engine -run TestRunBudget`
- identical inputs stay identical across repeats:
  `go test ./engine -run 'TestRepeatRun_SameConclusion|TestResume_Repeated'`
- retention/concurrency/comparison:
  `go test ./pkg/history ./engine -run TestHistory`
