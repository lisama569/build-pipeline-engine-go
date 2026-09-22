# Local Build Pipeline Engine

A local build pipeline **orchestration and execution engine** for Go. It parses
a task dependency graph, runs tasks with bounded parallelism, propagates
failures and cancellation, caches results with invalidation semantics, applies
bounded retries/backoff, and emits deterministic, machine-readable reports.

It is driven by the repository's own build/verification entry points
(`go build`, `go vet`, `go test`) and needs **no network, credentials or paid
resources**.

## Layout

| Path | Responsibility |
|------|----------------|
| `pipeline/` | Public types, task/config model, classified error codes |
| `pkg/graph/` | Graph parsing, validation (cycle/missing/duplicate), readiness |
| `pkg/cache/` | Content-addressed tamper-evident cache + in-flight deduplication |
| `pkg/exec/` | One attempt: in-process func or local command (process-group kill) |
| `pkg/retry/` | Bounded exponential backoff + error classification |
| `pkg/scheduler/` | Bounded-parallel deterministic scheduling, failure/cancel propagation |
| `pkg/report/` | Structured deterministic report + stable conclusion hash |
| `engine/` | Wiring, config validation/bounds, declarative spec loader |
| `cmd/pipeline/` | CLI |

See [`docs/SCHEDULING.md`](docs/SCHEDULING.md) and
[`docs/CACHE_AND_RETRY.md`](docs/CACHE_AND_RETRY.md) for the full rules.

## Quick start

```bash
# validate + build + vet + test the repo through the engine
go run ./cmd/pipeline -spec examples/pipeline.json -parallelism 2

# machine-readable report
go run ./cmd/pipeline -spec examples/pipeline.json -json

# disable caching
go run ./cmd/pipeline -spec examples/pipeline.json -cache ""
```

A task exits non-zero on any non-success conclusion.

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
  (status normalized so cache hit == fresh success, output digest). All
  wall-clock/host-dependent values are listed explicitly in
  `non_determinism` instead of being hidden in logs.
