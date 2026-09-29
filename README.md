# Local Build Pipeline Engine

A local build pipeline **orchestration and execution engine** for Go. It parses
a task dependency graph, runs tasks with bounded parallelism, propagates
failures and cancellation, caches results with invalidation semantics, applies
bounded retries/backoff, can **resume from the previous run**, enforces a
**run-wide time budget**, retains a **bounded history of comparable runs**, and
emits deterministic, machine-readable reports.

It is driven by the repository's own build/verification entry points
(`go build`, `go vet`, `go test`) and needs **no network, credentials or paid
resources**.

## Layout

| Path | Responsibility |
|------|----------------|
| `pipeline/` | Public types, task/config model, classified error codes |
| `pkg/graph/` | Graph parsing, validation (cycle/missing/duplicate), readiness |
| `pkg/cache/` | Content-addressed tamper-evident cache + in-flight deduplication |
| `pkg/resume/` | Previous-run checkpoints + resume decision logic |
| `pkg/history/` | Bounded, comparable, concurrency-safe run history |
| `pkg/exec/` | One attempt: in-process func or local command (process-group kill) |
| `pkg/retry/` | Bounded exponential backoff + error classification |
| `pkg/scheduler/` | Bounded-parallel deterministic scheduling, failure/cancel/budget handling |
| `pkg/report/` | Structured deterministic report + stable conclusion hash |
| `engine/` | Wiring, config validation/bounds, declarative spec loader |
| `cmd/pipeline/` | CLI (`run` / `history` subcommands) |

See [`docs/SCHEDULING.md`](docs/SCHEDULING.md),
[`docs/CACHE_AND_RETRY.md`](docs/CACHE_AND_RETRY.md) and
[`docs/RESUME_BUDGET_HISTORY.md`](docs/RESUME_BUDGET_HISTORY.md) for the full
rules.

## Quick start

```bash
# validate + build + vet + test the repo through the engine
go run ./cmd/pipeline run -spec examples/pipeline.json -parallelism 2

# machine-readable report
go run ./cmd/pipeline run -spec examples/pipeline.json -json

# continue from the previous run: reuse still-valid successes, rerun only
# failed/skipped, changed/new and dependency-restructured tasks
go run ./cmd/pipeline run -spec examples/pipeline.json -resume

# bound the whole run to ten minutes wall-clock (distinct from task timeout)
go run ./cmd/pipeline run -spec examples/pipeline.json -run-budget 10m

# inspect retained runs and compare two of them
go run ./cmd/pipeline history -pipeline self-check
go run ./cmd/pipeline history -pipeline self-check -diff <older-run-id>

# disable caching
go run ./cmd/pipeline run -spec examples/pipeline.json -cache ""
```

`run` is the default subcommand, so `go run ./cmd/pipeline -spec ...` keeps
working exactly as before. The process exits non-zero on any non-success
conclusion; budget-exhausted runs also exit non-zero.

## Declarative spec

```json
{
  "name": "self-check",
  "tasks": [
    { "id": "build",  "command": ["go", "build", "./..."], "idempotent": true },
    { "id": "vet",    "deps": ["build"], "command": ["go", "vet", "./..."], "idempotent": true },
    { "id": "test",   "deps": ["build"], "command": ["go", "test", "./..."],
      "idempotent": true, "timeout": "5m", "cache_inputs": { "suite": "unit" } }
  ]
}
```

You can also drive the engine in-process with `engine.NewEngine` +
`pipeline.Task.Run` (see `engine/*_test.go`).

## Local verification (offline)

```bash
gofmt -l .
go vet ./...
go test ./...            # unit + integration
go test -race ./...      # concurrency checks
```

All tests use in-process functions and POSIX shell commands only; nothing
contacts an external service.

## Guarantees at a glance

- **Graph validation rejects, with distinct codes:** `graph_cycle` (closed
  cycle path printed), `graph_missing_dep` (every bad edge listed),
  `graph_duplicate_task` (every duplicate listed), `graph_empty`. Nothing is
  silently skipped.
- **Bounded parallelism:** exactly `MaxParallelism` long-lived workers; ready
  surplus tasks queue deterministically (FIFO or downstream-priority). Never
  one goroutine per task.
- **Failure propagation:** any non-success upstream marks all transitive
  downstream tasks `skipped` with reason `upstream:<dep>`; they never run and
  are never reported as success.
- **Cancellation:** propagates into per-attempt contexts and OS process
  groups; converges immediately once running work ends, with `ShutdownGrace`
  as a hard backstop. No dangling processes or half-written cache entries.
- **Cache:** key covers task id, command, environment fingerprint and all
  declared inputs. Entries are checksummed and atomically written (temp +
  fsync + rename). Corruption -> `cache_corrupt`; digest mismatch ->
  `cache_polluted`; both are recomputed and can never produce a hit.
- **Retry/timeout:** attempts and timeouts are bounded and clamped to hard
  caps. Timeouts are `timed_out` and never retried; a transient failure on a
  non-idempotent task is `retry_unsafe` and is not resubmitted.
- **Deterministic reports:** `conclusion` hashes only the observable output
  (status normalized so cache hit / reused verdict == fresh success, output
  digest). All wall-clock/host-dependent values are listed explicitly in
  `non_determinism` instead of being hidden in logs.
- **Resume:** a checksummed per-pipeline checkpoint carries forward only
  unchanged successes (same fingerprint, same dependency edges, reusable
  transitive deps); failed/timed-out/skipped/canceled, new, changed-input,
  cache-disabled and edge-restructured tasks - plus the affected closure -
  re-execute with a stable `rerun_reason`. Graph renames/edits/deletes can
  never serve a stale verdict. A resume run and a clean run over the same
  inputs hash to the identical conclusion.
- **Run budget:** `RunBudget` stops new dispatch and cancels running work
  once the wall-clock budget elapses; convergence is bounded by
  `ShutdownGrace`. It is reported separately (`stop_reason=budget_exhausted`)
  from a manual `canceled`, `fail_fast` and a task's own `timed_out`.
- **Run history:** the newest N runs per pipeline (configurable, hard cap 50)
  are retained as checksummed records in unique files; concurrent pipelines
  never overwrite each other, and `history.Compare` shows reused/reran/new/
  removed/status-changed tasks with the rerun cause.
