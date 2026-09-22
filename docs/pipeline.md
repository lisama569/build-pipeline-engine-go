# 本地构建流水线编排引擎 — 调度、缓存与重试规则

本引擎只依赖 Go 标准库，在本地以仓库既有的构建/验收入口（例如
`go build ./...`、`go vet ./...`、`go test ./...` 或任意本地可执行命令）
驱动任务，不访问任何外部服务、不需要真实凭据或付费资源。

## 1. 总体模型

- 一条流水线（`pipeline.Pipeline` / JSON `Spec`）是一组具名任务
  （`TaskDef`），每个任务声明零个或多个依赖。
- `pipeline.Parse` 把声明校验成依赖 DAG（`Graph`），随后 `Engine.Execute`
  用一个**固定大小的工作池**（大小 = `Config.Parallelism`）调度执行。
- 整个引擎在一次运行中最多创建 `Parallelism + 1` 个 goroutine（调度器 +
  固定 worker），与任务图规模无关，不会无界创建执行体。

## 2. 依赖图解析与就绪判定

解析阶段对任何缺陷都是**整体拒绝**（返回错误，不执行任何任务），并且不同
缺陷有可区分的机器可读原因（`pipeline.Reason`）：

| 缺陷 | Reason |
| --- | --- |
| 重复定义同名任务 | `duplicate_task` |
| 依赖未在流水线中定义 | `missing_dependency`（错误信息含缺失名） |
| 自依赖或任意环 | `cycle_detected`（错误信息列出环上的任务） |
| 空名、超时/重试越界、非幂等任务配置重试等 | `invalid_config` |
| 空流水线 / nil | `empty_graph` |

不会静默跳过非法项，也不会只执行图的“合法部分”。

就绪判定：一个任务的全部依赖都达到 `succeeded` 或 `from_cache` 时才就绪。
依赖计数在调度过程中单调维护。

## 3. 调度规则与并行上限

- 拓扑序采用**字典序 Kahn**（每一步选择字典序最小的零入度节点），因此
  给定同一张图，分发顺序确定，重复运行的 `schedule_order` 一致。
- 每当 worker 槽位空闲，就从就绪最小堆中取字典序最小的任务分发；超过
  `Parallelism` 的就绪任务在堆里排队，而不是另开 goroutine。
- 同一并发波内的**完成先后**天然受时序影响；这一事实被显式写入报告的
  `meta.nondeterminism`，不藏在日志里。任务的最终状态与原因不受其影响。
- 一个上游失败只阻断其**传递下游**；与之无依赖关系的分支继续执行。
- 菱形依赖（A 被 B、C 共同依赖，D 依赖 B、C）中 A 只执行一次。

### 失败传播

- 上游为 `failed` 或 `timed_out` 时，其所有传递下游立刻标记为
  `skipped` / `upstream_failed`，绝不会被当作成功，也不会被静默丢弃。
- 失败之后其他独立分支仍然继续。

### 取消传播与有界收敛

- 外部 `context` 取消时：不再分发新任务；尚未分发的任务标记
  `skipped` / `canceled`；正在运行的任务收到其 `ctx` 取消。
- 命令任务先收到 SIGTERM，2 秒后仍未退出则 SIGKILL（`exec.Cmd` 的
  `WaitDelay`），不遗留孤儿进程。
- 对**不配合取消**的函数任务，调度器最多等待 `Config.ShutdownGrace`
  （默认 10s，硬上限 2m）即收敛返回，不留下悬挂的执行体或半写缓存
  （缓存只在干净成功后写回）。

## 4. 任务状态与错误归类

任务状态：`pending → ready → running → succeeded | from_cache | failed |
timed_out | skipped`。

- `failed/task_failed`：任务体返回普通错误（命令退出非零也归此类，
  stderr 进入 message）。
- `timed_out/timed_out`：单次尝试超过时限。**超时是终态，不会被重试**，
  与普通失败严格区分。
- `skipped/upstream_failed`：上游失败导致未执行。
- `skipped/canceled`：运行被外部取消（排队中或运行中）。
- `failed/non_idempotent_retry`：理论上的运行期防御原因；非幂等任务
  配置重试在解析阶段即以 `invalid_config` 拒绝。
- `failed/execution_error`：缓存后端读写、输入哈希等基础设施错误，与
  任务自身失败区分，绝不假装成功。

## 5. 超时、重试与退避

- 单次尝试硬超时：任务级 `TaskDef.Timeout`，缺省用
  `Config.DefaultTimeout`（默认 5m，任何超时硬上限 30m）。每次尝试使用
  全新的超时上下文。
- 重试次数：`TaskDef.Retries` 表示**首次之外**的额外尝试次数；缺省 0，
  单任务硬上限 10（`Config.MaxRetries`）。
- 只有声明 `Idempotent: true` 的任务允许重试；否则解析阶段直接拒绝。
  重试因此不会重复提交非幂等副作用。
- 退避：`backoff * 2^(attempt-1)`，以 `Config.MaxRetryBackoff`
  （默认 2s，硬上限 5m）封顶；退避等待可被取消立即打断。
- 只有**最终干净成功**才写缓存；失败/超时绝不覆盖已有缓存。

## 6. 缓存与增量执行

- 仅 `Cacheable: true` 的任务参与缓存。缓存键（`Fingerprint`）为
  `sha256:` 前缀的十六进制摘要，覆盖：
  1. 任务名与动作稳定身份（命令的 program/args/dir/**完整显式环境**的
     规范序列化；函数任务为名字派生常量）；
  2. 声明的 `Inputs` 文件内容（相对 `RepoRoot`）；缺失文件以显式
     `<missing>` 标记参与哈希，不会与“空文件”混淆；
  3. 依赖闭包中各上游任务的缓存键（上游产物变化即令下游失效）；
  4. `Config.EnvPassthrough` 白名单中的环境变量（名字+值）。白名单之外
     的环境差异**不**影响缓存键。
- 命中缓存时任务体**完全不被调用**，因此副作用不会产生第二次；结果状态
  为 `from_cache`，`cache_hit: true`。
- 文件缓存写入采用“同目录临时文件 + fsync + 原子 rename”，读者只能看到
  旧的完整值或新的完整值，看不到半写文件。
- 防污染：每个条目内嵌载荷 SHA-256；键与文件名不一致、JSON 损坏或载荷
  校验和不符都**按未命中处理并重新执行**，同时在该任务结果的
  `uncertainty` 字段显式标注恢复原因。被污染缓存永远不可能产出错误的
  “命中”结论。
- 缓存目录两级分片（`xx/yy/<hash>.json`），避免单目录无界增长。

### 跨流水线 / 多处引用的一致性

单个 `Engine` 共享缓存与一张 in-flight 表：相同缓存键的任务即使被多个
并发流水线（`RunMany`）引用，函数体也只执行一次，其余引用等待同一结果，
杜绝重复执行、竞态与结论差异。非缓存任务不参与该去重（空键永不碰撞）。

## 7. 结构化报告

`Engine.Run` 返回 `*pipeline.Report`，可 `report.JSON()` 序列化为稳定
JSON：

- 任务按图的规范顺序排列；`schedule_order` 记录实际分发顺序。
- **不含任何墙上时钟时间戳**，因此相同输入多次运行的结论序列化一致；
  仅保留每次尝试数与 `duration_ms`（测量数据，不影响状态/原因）。
- `summary` 统计各状态数量，`reason` 给出总体失败原因，`meta` 标注调度
  策略、唯一不确定性来源与全部出现过的原因。
- 所有不确定性（缓存损坏后恢复、并发波内完成时序）都在结构化字段里显式
  标注。

## 8. 可配置项（`pipeline.Config`）

| 字段 | 默认 | 上限/约束 |
| --- | --- | --- |
| `Parallelism` | 4 | [1, 256]，0 取默认，负数拒绝 |
| `DefaultRetries` / `MaxRetries` | 0 / 10 | 重试硬上限 10 |
| `DefaultTimeout` / `MaxTimeout` | 5m / 30m | 超时硬上限 30m |
| `RetryBackoff` / `MaxRetryBackoff` | 100ms / 2s | 退避硬上限 5m |
| `ShutdownGrace` | 10s | 硬上限 2m |
| `CacheDir` | 空（仅内存缓存） | 非空时为文件缓存目录 |
| `RepoRoot` | `.`（CLI） | 输入/产物与命令相对路径根 |
| `EnvPassthrough` | 空 | 仅这些环境变量进入缓存键 |
| `Logf` | nil | 并发安全的调度决策日志钩子 |

越界配置以 `invalid_config` 明确拒绝，引擎不可能被配置成无界资源使用。

## 9. JSON 流水线规格

```json
{
  "name": "repo-checks",
  "tasks": [
    {"name": "vet", "program": "go", "args": ["vet", "./..."],
     "inputs": ["go.mod"], "cacheable": true, "idempotent": true},
    {"name": "test", "program": "go", "args": ["test", "./..."],
     "deps": ["vet"], "idempotent": true, "timeout_ms": 120000, "retries": 1}
  ]
}
```

- 命令在 `Dir`（相对 `RepoRoot`，缺省即根）执行，环境**完全显式**
  （`env`），不继承白名单外的环境变量；仅当规格未给出 `PATH`/`HOME`
  时为工具链可用性注入，二者同样进入缓存键。
- `retries > 0` 必须同时 `idempotent: true`。
- `artifacts` 声明的相对路径在成功后被哈希记录在缓存载荷中。

## 10. 本地验证方法（无外部依赖）

```bash
# 编译与静态检查（仓库自带验收入口）
go build ./...
go vet ./...

# 全量单测（含竞态检测；重点覆盖环依赖、缓存失效/污染、取消、超时、重试）
go test ./... -race -count=1

# CLI：对本仓库自身跑 vet/build/test
go build -o /tmp/pipeline ./cmd/pipeline
/tmp/pipeline -spec pipeline.example.json -cachedir .pipeline-cache -jobs 4 -root . -v

# 再跑一次：未变输入的 cacheable 任务应为 from_cache
/tmp/pipeline -spec pipeline.example.json -cachedir .pipeline-cache -root .
```

退出码：成功 0；流水线内有失败/超时/跳过 1；规格或配置非法 2。

单测日志通过 `Config.Logf` 打印每个调度决定（ready / dispatch / complete /
skip / cancel）及其判定依据，可用 `-v` 查看调度顺序。
