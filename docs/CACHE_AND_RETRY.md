# Cache, retry and timeout semantics

## Cache key coverage

```
key = sha256(
  taskID ||
  canonical(command: name|args|workdir) ||
  envHash( sha256(sorted process environment) ++ task env overlay ) ||
  for k in sorted(CacheKeyInputs):  k || v
)
```

Consequences:

- Changing a declared input (e.g. a source hash), the command line or any
  environment variable changes the key, so a stale entry can never be hit
  across different inputs or environments.
- Map iteration order cannot influence the key (keys are sorted), so the key
  is stable across runs and machines with identical environments.
- Per task, `DisableCache: true` bypasses both read and write.

The process environment is fingerprinted once per engine (sorted `os.Environ`)
and combined with the task's env overlay, covering ambient differences
(`GOOS/GOARCH`, tool versions exposed as env, etc.). For stronger input
coverage, put source/tool hashes in `task.CacheKeyInputs`.

## Integrity and pollution safety

Each entry on disk is an envelope:

```json
{ "key": "<sha256>", "payload": "<sha256(compact entry JSON)>", "entry": { ... } }
```

- **Atomic write:** data is written to a temp file in the destination shard,
  `fsync`ed, then `os.Rename`d. A killed process leaves at most an ignored
  temp file, never a partially readable entry.
- **Corruption:** `Get` canonicalizes (`json.Compact`) the embedded entry and
  recomputes the checksum. Any decode or checksum failure is
  `cache_corrupt`; the bad file is quarantined (`*.bad`) and the task is
  recomputed.
- **Key binding:** the stored and embedded keys must equal the requested key;
  otherwise `cache_polluted`.
- **Digest mismatch:** the task can declare an observed `ExecOutput.Digest`;
  a consumer-supplied expected digest that differs is `cache_polluted`,
  never a hit.

A cache hit never invokes the task body, so hits produce no side effects and
do not re-run non-idempotent work.

## Retry policy

Bounded exponential backoff between attempts:

```
wait(attempt) = min(Cap, Base * 2^(attempt-1))
```

| Knob | Default | Hard bound |
|------|---------|------------|
| Max attempts (incl. first) | 1 | clamped to 10 |
| Backoff base | 50ms | — |
| Backoff cap | 2s | — |

Classification of an attempt error (`retry.Classify`):

| Error | Idempotent? | Decision |
|-------|-------------|----------|
| `task_timeout` / `context.DeadlineExceeded` | any | **no retry**, status `timed_out` |
| `task_canceled` / `context.Canceled` | any | **no retry**, status `canceled` |
| `retry.Transient` | true | retry with backoff while attempts remain |
| `retry.Transient` | false | **no retry**, reason `retry_unsafe` |
| any other error | any | **no retry**, status `failed` (`task_failed`) |

### Why non-idempotent tasks are not retried

A transient failure may already have produced an external side effect (a
partial write, a sent message). Resubmitting would duplicate it. Such tasks
must declare `Idempotent: false`; on a transient error they run exactly once
and are reported with the distinct code `retry_unsafe`, so the cause is
distinguishable from a plain permanent failure (`task_failed`).

### Why timeouts are not retried as failures

Exceeding the deadline is an explicit, distinguishable outcome: the task is
marked `timed_out` and the retry loop is bypassed even if attempts remain.
Cancellation during backoff sleep is likewise interruptible and classified as
`canceled` (or `timed_out` if the deadline ended), never wedging the run.

## Timeouts

- `task.Timeout` bounds a single attempt; unset tasks use
  `Config.DefaultTaskTimeout` (CLI `-task-timeout`). Zero means "no per-task
  deadline" and the run context still applies.
- Values are clamped to the 30-minute hard ceiling; negative values are a
  configuration error.

## Configuration reference

| `pipeline.Config` / CLI flag | Meaning | Default | Bound |
|------------------------------|---------|---------|-------|
| `MaxParallelism` / `-parallelism` | concurrent task cap | 4 | 1..64 |
| `MaxAttempts` / `-attempts` | attempts per task | 1 | 1..10 |
| `DefaultTaskTimeout` / `-task-timeout` | attempt deadline | 10m | <= 30m |
| `ShutdownGrace` / `-shutdown-grace` | cancel convergence backstop | 10s | <= 60s |
| `QueuePolicy` / `-queue` | `fifo` or `priority` | fifo | — |
| `CacheDir` / `-cache` | cache root | `.pipeline-cache` | `""` disables |
| `FailFast` / `-fail-fast` | cancel on first failure | false | — |
| `RetryBackoffBase` / — | backoff base | 50ms | >= 0 |
| `RetryBackoffCap` / — | backoff cap | 2s | >= base |

Invalid configuration returns `config_invalid` before any task runs;
out-of-range numeric values are either rejected (negative/zero attempts) or
clamped (parallelism, timeouts, grace).

## Deterministic report

`report.Report` JSON includes, per task: `id`, `status`, `attempts`,
`cached`, `reason`, `detail`, `order`, `output_digest`, plus the full
scheduling `trace` (ordered, numbered decisions) and `conclusion`.

`conclusion` is a sha256 over a canonical JSON of only the observable
outcome: per task it hashes id, normalized status (a `cache_hit` normalizes
to `succeeded`), normalized reason (`cache_hit` -> `ok`) and the output
digest. Attempt counts, dispatch order, timestamps and stdout/stderr are
excluded. Every excluded, potentially varying source is enumerated in
`non_determinism`. Therefore identical inputs produce identical
`conclusion` values on repeat runs even across cold/warm caches, while the
`trace` still explains exactly what happened on that particular run.
