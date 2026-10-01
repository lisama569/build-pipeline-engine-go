# Scheduling rules

## 1. Graph validation (before anything runs)

A task list is parsed exactly once into an immutable `graph.Graph`. The
following are rejected **before execution**, each with a distinct, stable
machine code; all offenders (not just the first) are included in the detail:

| Condition | Code | Detail contains |
|-----------|------|-----------------|
| No tasks | `graph_empty` | — |
| Same id declared twice | `graph_duplicate_task` | every duplicate (`id@index(first@i)`) |
| Dep references unknown id | `graph_missing_dep` | every missing edge (`from->dep`) |
| Dependency cycle (incl. self-loop) | `graph_cycle` | a closed cycle path (`a -> b -> a`) |

Cycles are found with an iterative 3-color DFS in declaration order; the
reported path is reconstructed from the DFS parent chain and closes back on
itself. Because validation is all-or-nothing, the engine never executes "part
of" an invalid pipeline.

## 2. Readiness

A task becomes **ready** when all its dependencies reached a success status
(`succeeded` or `cache_hit`). A dependency in `failed`, `timed_out`,
`canceled` or `skipped` poisons the node, which the scheduler then turns into
a terminal `skipped`. The poison propagates transitively: when node B is
skipped because A failed, B's dependents are skipped because of B, and so on.

A node's lifecycle is:

```
pending -> ready -> running -> succeeded | cache_hit
                            \-> failed | timed_out | canceled
ready/running ..............\-> skipped (upstream non-success) or canceled
```

## 3. Bounded concurrency

- Exactly `Config.MaxParallelism` long-lived worker goroutines are created
  (not one per task). Work travels over a buffered channel of capacity
  `MaxParallelism`; surplus ready tasks stay in an in-memory slice queue.
- The scheduler loop is the only goroutine that mutates graph-level state;
  workers execute and report completions. This removes races and keeps
  memory O(nodes + edges).

## 4. Ready-queue policy (deterministic)

When a slot is free the next task is chosen without any map iteration, so
repeat runs schedule identically:

- **`fifo` (default):** smallest declaration index.
- **`priority`:** greatest number of *transitive* downstream dependents
  (critical-path style), ties broken by declaration index. This tends to
  start "upstream of the most work" first, but the choice never affects the
  conclusion, only timing.

## 5. Failure propagation & fail-fast

- Without fail-fast, independent branches keep running; only the failing
  node's transitive closure is `skipped`. Each skipped task records
  `reason = "upstream:<direct blocker>"` and never invokes its body.
- With `Config.FailFast`, the first non-success completion cancels the run
  context: no new tasks launch and running tasks receive cancellation.

## 6. Cancellation & bounded convergence

Cancellation sources: parent `ctx` cancel (SIGINT/SIGTERM in the CLI) or
fail-fast. Mechanics:

1. `runCtx` is canceled; every attempt derives from it together with the
   task's own timeout.
2. In-process `ExecFunc`s observe `ctx.Done()`; local commands run in their
   own **process group** (`setpgid`) and the group is `SIGKILL`ed, with a
   2s `WaitDelay`, so children cannot be orphaned.
3. Once launching has stopped and the last in-flight task ends, every
   unlaunched task is marked `canceled` immediately.
4. `Config.ShutdownGrace` (default 10s, hard-capped at 60s) is a defensive
   backstop for a task that ignores cancellation: after it expires the run
   concludes **without waiting** for the uncooperative worker, so no run
   can hang indefinitely.

There is no path that returns while cache state is half-written (cache
writes are temp-file + fsync + rename). A worker whose task never honors
cancellation may be abandoned mid-execution when the grace expires; its
late result is discarded and never rewrites a concluded task.

A **run-wide budget** (`Config.RunBudget`) is a third, distinct stop cause in
addition to parent cancellation and fail-fast: when it is used up no new task
is dispatched, in-flight tasks are canceled and converge within the same
grace (again without waiting for uncooperative tasks past it), and the run
concludes as `budget_exhausted`. It is never reported as a
manual cancel or a task timeout. See
[RESUME_BUDGET_HISTORY.md](RESUME_BUDGET_HISTORY.md).

## 7. Cross-pipeline consistency

The engine owns one shared `cache.Store` and one `cache.InFlight` deduper.
When several pipelines (or several references to the same task) overlap with
the same cache key, only the **leader** performs the cache lookup and
execution; concurrent callers wait and receive the *identical* value/error.
Failures are shared too, so a task cannot succeed for one observer and fail
for another.

**Shared cancellation semantics.** The shared attempt is driven by the
leader's run context. Concurrent pipelines on one engine are expected to
share a common top-level context, so an external cancel (SIGINT/SIGTERM)
correctly stops shared work for everyone. If you instead need **independent
fail-fast isolation** between pipelines, run them on separate engines; they
still share the on-disk cache and therefore never recompute a finished task,
but one pipeline's fail-fast cannot cancel another's in-flight attempt. This
choice is deliberate and explicit rather than producing an unexplained
"canceled" task in a healthy pipeline.

## 8. Scaling

Validation is O(V+E); readiness counters are decremented per edge
(O(degree) per completion); the queue scan is O(queue length) per dispatch;
workers are bounded and buffers bounded. Memory stays O(V+E). The
`TestLargeGraph_LinearScaling` test schedules a 2002-node diamond with
parallelism 8 comfortably under a second.
