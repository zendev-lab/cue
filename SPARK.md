---
description: Cue 是单机、持久、可观察、支持人机共享现场的统一执行运行时；语言是客户端，cued 只接受 typed execution contract。
owner: zrr1999
created: 2026-04-26
updated: 2026-08-21
inspired_by:
  - bash
  - zsh
  - tmux
  - zellij
  - nushell
  - justfile
  - loom
---

## 当前方向

Cue 已从“shell 替代品”收敛为统一执行器。Shell-like DSL 仍然产品化，
但只属于 `cue-language` 和客户端；`cued` 的权威入口是 IPC v3 的
`ExecutionSpec`。

一次提交只有一个 `ExecutionId`（`E<n>`），每个实际进程型 pipeline
叶节点有稳定 `StepId`（`E<n>/S<n>`）。Execution reducer 唯一拥有
条件、并行、失败传播和聚合状态；ProcessManager 只运行 ready pipeline
step；TriggerService 只拥有 schedule/timer，并在触发时提交全新的
Execution。

## 产品目标

- 单机 daemon 持久托管 execution、session/scope、schedule、PTY 和输出。
- 人和 agent 连接同一命名 session、观察同一状态、显式交接 PTY 控制权。
- 客户端断开不等于 execution 结束；重连使用 typed query/wait/output 恢复。
- 精确 argv、cwd、env、resource、workspace view 和 wrapper 都可解释。
- 通用 SpawnAdapter 允许 host 在最终 spawn 边界施加策略，但不把 host
  语义放进 daemon。

## 权威边界

- `cue-core`：IPC v3、Execution/Step/Scope/Schedule 类型和纯 reducer。
- `cue-language`：Tokenizer、Parser、Resolver、Compiler、补全、高亮。
- `cue-daemon`：唯一 execution/session/resource/process/PTY/persistence owner。
- `cue-client`：transport、SSH、重连、版本检查和 daemon lifecycle。
- `cue-tui`：基于 client/language 的交互视图。

`cued` 不拥有 DSL、agent/workflow 策略、DSH 语义、秘密管理、远程 fleet
或通用 DAG/retry runtime。Retry 由客户端读取旧 spec 后提交新的 Execution，
并写 `retry_of`。

## 核心原则

- 每个有状态领域只有一个 owner，不建立第二套 store、scheduler 或 reducer。
- daemon API 不接收源代码文本；frontend 本地编译为 typed intent。
- schedule 是触发模板，不是执行状态容器。
- pipeline 每个 segment 只经过一次统一 `prepare_spawn`。
- wrapper 与 workspace view 保留公开语义，并在 SpawnAdapter 之前应用。
- SpawnAdapter 是短期本地 lease，不持久化、不进入 env/output/event。
- 旧 Eval/job/chain/script 协议不双轨兼容；升级失败应明确、尽早、可诊断。
- 机制留在 Cue；approval、policy、workflow 和 agent intent 留在 host/client。

## 不做

- 第二个执行 daemon 或通用插件框架。
- 完整 DAG、补偿、长时 workflow/retry 状态机。
- agent、model、planner、approval、denial signature 等专用概念。
- 跨机调度、秘密管理、浏览器终端分享。
- 用 `sh -c` 把 pipeline 降级成不可观察黑盒。
- 持久化 SpawnAdapter 或首版 confined SSH。

## 当前协议与数据

IPC v3 是单轨严格 schema。执行生命周期事件只有
`ExecutionCreated`、`ExecutionStateChanged`、`StepStateChanged`、
`OutputChunk`、`ExecutionFinished`；PTY attachment 有独立 typed 控制事件。

SQLite v21 只保存 scopes、sessions、executions、steps、schedules 和
operation idempotency facts。升级时取得 instance lock，把 v18 数据与输出
归档为只读，只导入安全 session/scope/config context；旧 J/CH/R 历史和 cron
不导入、不双读。

## 生态关系

- Spark 等 host-neutral 客户端直接提交 typed ExecutionSpec。
- `dsh-tool-cue` 单独拥有 DSH policy、approval、sandbox broker 和标准 facts。
- loom 或其他 workflow runtime 可以使用 Cue 跑进程，但不能把 workflow 状态
  写回 `cued`。
- bash/zsh/fish 仍负责通用 shell 交互；tmux/zellij 仍可负责 pane/layout。

## 验收信号

- 同一 execution/step 在 CLI、TUI、Spark、DSH 中身份一致。
- `VAR=value script` 只覆盖对应 segment 的环境，不污染 scope。
- 每个 segment 的 adapter prepare/settle 恰好一次，broker 消失 fail closed。
- daemon 重启后 operation idempotency fact 阻止重复副作用。
- TUI 与 client 共用 lifecycle/transport，daemon 不包含 parser/completion。
- 公开文档和安装产物只描述 Cue execution runtime；旧产品名只出现在迁移说明。

## 文档入口

- [README](README.md)
- [架构入口](ARCHITECTURE.md)
- [设计索引](docs/design/README.md)
- [IPC v3](docs/design/ipc-protocol.md)

## 修订记录

- 2026-08-21：硬切 IPC v3，统一 Execution/Step，拆出 cue-language 和
  TriggerService，引入 SpawnAdapter、v21 persistence、XDG `cue` 迁移与
  `cue-run` 分发名。
- 2026-07-22：确认命名 session 与共享 PTY 的多 observer/单 controller 方向。
- 2026-04-26：从 agent/workflow shell 收缩到底层进程机制。
