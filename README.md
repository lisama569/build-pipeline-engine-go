# build-pipeline-engine-go

一套纯 Go 标准库实现的**本地构建流水线编排与执行引擎**。它以仓库既有的
构建/验收入口（`go build` / `go vet` / `go test` 或任意本地命令）驱动任务，
提供依赖图解析、受限并行、失败传播与取消、带失效语义的缓存、超时/有界重试
以及确定性的结构化报告。不依赖任何外部服务、真实凭据或付费资源。

## 能力一览

- **依赖图**：重复定义（`duplicate_task`）、缺失依赖（`missing_dependency`）、
  环（`cycle_detected`，列出环成员）、非法配置（`invalid_config`）一律整体
  拒绝，原因可区分，绝不静默跳过。
- **受限并行**：固定大小 worker 池（默认 4，上限 256），字典序就绪最小堆
  排队，goroutine 总数不随图规模增长。
- **失败传播与取消**：上游失败的传递下游标记 `skipped/upstream_failed`；
  取消传播到运行中的任务（命令 SIGTERM→SIGKILL），并在 `ShutdownGrace`
  内有界收敛，不残留孤儿进程或半写缓存。
- **缓存与增量**：键覆盖动作身份、输入内容、依赖键与白名单环境变量；命中
  不重复执行；原子写 + 载荷校验和使被污染缓存只能变成“未命中 + 显式
  uncertainty 标注”，不会产出错误结论。
- **超时/重试/退避**：次数与时限可配且有硬上限；超时任务标记
  `timed_out` 且不重试；仅幂等任务可重试，非幂等重试在解析期拒绝。
- **确定性报告**：无时间戳 JSON，相同输入结论一致；不确定性显式结构化标注。
- **大规模与共享**：拓扑解析 O((V+E) log V)；`RunMany` 下相同任务跨流水线
  只执行一次（in-flight 去重），2000 节点图有规模化测试。

## 快速开始

```bash
go build ./...
go vet ./...
go test ./... -race -count=1

# 对本仓库自身执行流水线（第二次运行可观察 from_cache）
go run ./cmd/pipeline -spec pipeline.example.json -cachedir .pipeline-cache -jobs 4 -root . -v
```

详见 [docs/pipeline.md](docs/pipeline.md)（调度规则、缓存与重试策略、
配置项、JSON 规格与本地验证方法）。

## 代码结构

| 文件 | 职责 |
| --- | --- |
| `pipeline/graph.go` | 声明解析、去重、缺失/环检测、字典序 Kahn 拓扑 |
| `pipeline/scheduler.go` | 固定 worker 池、就绪堆、失败传播、取消收敛 |
| `pipeline/runner.go` | 缓存命中、单次超时、有界指数退避重试、幂等保护 |
| `pipeline/cache.go` | 指纹、内存/文件缓存、原子写、损坏检测 |
| `pipeline/action.go` | 仓库原生命令执行体（显式环境、进程级取消） |
| `pipeline/engine.go` | 引擎构造、跨流水线 in-flight 去重、Run/RunMany |
| `pipeline/report.go` | 无时间戳确定性结构化报告 |
| `pipeline/spec.go` | JSON 规格加载与命令装配 |
| `pipeline/config.go` | 默认值与硬边界校验 |
| `cmd/pipeline` | 本地 CLI |
