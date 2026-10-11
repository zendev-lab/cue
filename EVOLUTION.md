# 项目演进

## 初始意图

Cue 从 shell-like agent workflow 出发，希望让 Agent 的工作流能够可靠地运行本地进程。
最早的完整设计没有留存在现有起源记录中；后来的 daemon、命名 session、schedule
与执行内核是逐步形成的方向。

bash、zsh 与 Nushell 提供命令表达的参考，tmux、Zellij 提供持久交互的参考。
Cue 逐步把关注点收敛到可观察、可取消和可恢复的本地进程执行。

## 2026-04-26

### 触发

Agent 工作流与底层进程机制需要各自明确职责。

### 变化

从 agent/workflow shell 收缩到底层进程机制，供上层 Agent 或工作流系统调用。

### 理由

让进程机制可独立复用，避免把某一种 Agent 工作流固化到执行层。

<!-- 触发与理由根据现存起源及职责边界归纳；当日原始讨论未恢复。 -->

## 2026-07-22

### 触发

持久会话需要支持多个观察者，并明确交互输入的归属。

### 变化

确认命名 session 与共享 PTY 的方向：多个 observer 可以观察同一终端，一个 controller 持有输入控制权。

### 理由

将观察与控制分开，使重新连接和多人观察不产生竞争输入。

<!-- 触发与理由根据修订记录中的多 observer / 单 controller 决策归纳。 -->

## 2026-08-21

### 触发

执行身份、语言表达、触发和实际进程启动需要分离。

### 变化

硬切 IPC v3，统一 Execution/Step，拆出 `cue-language` 和 TriggerService，引入 SpawnAdapter、v21 persistence 与 XDG `cue` 路径。

### 理由

通过稳定的执行身份和独立的语言、触发、启动边界，为不同调用方复用同一进程机制。

<!-- 触发与理由根据这一轮拆分及后续内核设计归纳。 -->

## 2026-08-30

### 触发

daemon 已同时承担 execution semantics、交互 session、触发策略和 host policy。继续扩展它会让稳定的小内核不断依赖上层概念。

### 变化

确定持久本地执行内核方向：closed Execution ADT、显式 Scope、每个 Run 的 PTY
和启动时 Composition。session、schedule、resource、retry policy 移出内核。

这一轮建立纯 reducer、独立的 `cue-protocol` v4 与 `cue-store-sqlite` provider，
并完成 typed Assembly、local runner 和 surface compiler。Builtin/Run 持久记录
输入、输出 ScopeHash；Sequence 顺序传递，Parallel fork 且不 merge。
Captured/PTY 都实现 typed Pipeline，compiler 不再把上层策略 lower 到内核。

#### 产品与设计目标

Cue 接收完全解析的 typed `ExecutionSpec`，执行一棵有限、静态、结构化的
execution tree。一次提交拥有一个 `ExecutionId`；每个真正可观察的 Builtin 或
Run 叶节点拥有稳定 `StepId`。客户端断开不等于 execution 结束，之后可以继续
query、wait、读取 output 或重新 attach PTY。

Core 只定义程序“是什么意思”。它的 Execution ADT 是 closed semantics：扩展
不能增加 Retry、Schedule、DAG 或动态 Step，也不能改变 Sequence/Parallel 的
结果。Runtime Composition 定义这个语义“由谁实现”：store、spawner、workspace、
guard、transform 和 observer 可以替换或组合，但只允许改变 realization。

Scope 是显式、内容寻址的 `cwd + env + umask` 完整快照。Builtin 可以产生新
Scope，Sequence 顺序传递 Scope，Parallel 从输入 Scope fork 且永不 merge。
daemon 不再拥有 ambient session cursor；每个 ExecutionSpec 都必须携带起始
ScopeHash。

PTY 是单个 Run/Pipeline Step 的 I/O topology，而不是 Execution 全局开关。
Captured 产生独立 stdout/stderr；PTY 产生一个 terminal stream。一个 Pipeline
是一个 Step、一个生命周期和一个 process group，内部 PipeLink 仍然保留每个
process 的可观察性。

#### 目标用户

- 需要可靠运行、取消、观察和恢复本地进程的人类开发者。
- 需要 typed、幂等、无 shell 注入边界的 agent/host runtime，例如 Spark/DSH。
- 需要把 schedule、workflow 或 resource policy 放在自身领域，同时复用可靠
  进程内核的上层系统。
- 需要通过替换 store/spawner/workspace 等机制适配不同本地执行环境的集成方。

#### 核心原则

- Execution semantics 封闭；implementation graph 开放。
- Scope、Execution、Step、Event 都是显式可序列化事实，不依赖 session ambient state。
- 非法状态优先由 ADT 和 smart constructor 消灭，而不是运行到 daemon 后补校验。
- Builtin 和 Run 都是可观察 Step；组合节点只组织语义，不伪造工作身份。
- Pipeline 是一个 structured Step；process-local EnvPatch 不向相邻 process 泄漏。
- Composition 只在 daemon bootstrap resolve；运行热路径不查 service locator。
- 失败尽早、结构化、可恢复；敏感 env value 不落盘、不进入日志或事件。

#### 能力地图（方向性）

- Execution：Builtin、Run、Sequence、Parallel 的 closed ADT 与纯 reducer。
- Scope：显式完整快照、内容寻址、Builtin transition 和 client-owned cursor。
- Process：typed argv、EnvPatch、native Pipeline、process group 和 signal control。
- I/O：Captured stdout/stderr、单 terminal PTY、有界 output 与绝对 offset、attachment lease。
- Durability：execution/step facts、operation idempotency、drain-first restart 和 crash reconciliation。
- Composition：Port、Provider、Combine law、依赖解析、lifecycle 与 Assembly manifest。
- Frontend：Cue surface 编译、literal argv 构造、completion/highlight、CLI/TUI projection。
- Producer：cron、workflow、agent 或 queue 通过 `ExecutionSubmitter` 创建独立 Execution。

#### 成功信号

- 只阅读 Core ADT 就能确定任意 plan 的 Step、结果和 Scope 传播，不需要查看 daemon。
- 同一 execution/step 在 CLI、TUI、Spark 和 DSH 中身份与状态一致。
- `A=B left |> right` 只影响 left；`env set A=B -> ...` 才改变后续 Scope。
- `(cd a -> test-a) ||| (cd b -> test-b)` 的 branch cwd 不会泄漏到外层。
- 一个多进程 PTY Pipeline 可以被多人观察、单人控制，并能整体取消所有 descendants。
- daemon 启动前就报告缺失、歧义或成环的 Provider；ready 后不再动态解析依赖。
- schedule/resource/retry policy 可以独立演进而不修改 cue-core 或 IPC execution algebra。
- daemon 重启不会重复已确认的 side effect；volatile secret execution 明确不可 replay。

#### 生态关系

- `cue-language` 和 client 把 surface syntax 编译成 fully resolved ExecutionSpec；daemon
  永不接收原始 Cue source。
- Spark/DSH 负责 approval、resource/host 选择、retry policy 和 agent facts，然后
  通过 Cue 执行本地进程。
- `cue-ext-cron` 是外部 producer，拥有 timer/store，只依赖 ExecutionSubmitter。
- Volvox/Cordis 启发的是 Composition、Binding、Dependency Resolution 和 lifecycle；
  Cue 不复用它们的业务语义，也不把 World 与 execution Scope 混为一谈。
- bash/zsh/fish 继续负责通用 shell；tmux/zellij 继续负责 pane/layout。Cue 不重新
  实现完整 shell 或 terminal multiplexer。

#### 什么不是本项目要做的（Non-goals）

- 通用 DAG、动态 Step、补偿事务或长时 workflow state machine。
- daemon 内置 schedule/cron、自动 retry 或 resource scheduling policy。
- agent、model、planner、approval、fleet、remote host 或 secret manager 专用概念。
- 通过 `sh -c` 执行无法观察的字符串程序。
- Runtime service locator、运行中加载任意动态库或承诺 Rust plugin ABI。
- v3/v4 双栈 daemon 或长期保留两套 Execution semantics。
- 自动脱敏子进程主动写入 stdout/stderr 的内容。

#### 已考虑的替代方案 & 理由

- 继续扩展现有 IPC v3/daemon owner：迁移成本最低，但 session、schedule、resource
  和 execution 会继续互相牵制，因此放弃。
- 仅重写 ExecutionPlan，保留直接 runtime 依赖：会在下一步 Composition 时再次
  搬迁 owner 和构造路径，因此 Composition 从重构起点就进入设计。
- 让 extension 增加 ExecutionPlan variant：这会使持久化、reducer 和客户端无法
  再证明静态语义，因此 extension 只扩 implementation。
- 为 v3 数据逐项语义迁移：旧 Scope identity、session cursor 和 schedule contract
  与 IPC v4 不等价，因此选择在硬切时整体只读归档。
- daemon 运行时展开 `$VAR`/`~`：会让 plain argv 携带隐藏语义，因此当前只传递
  literal argv；compiler 也尚未实现这类展开。若未来增加，应由 frontend 从显式
  Scope 解析，不由 daemon 读取 ambient environment。

#### 后续问题（不阻塞内核）

- 静态 Rust provider 之外，哪些 sidecar provider protocol 值得成为稳定公共接口。
- 是否把 client-owned named session 做成独立可共享组件。
- Scope archive 与长期 output retention 是否需要独立的管理工具和 GC policy。

### 理由

Core 只定义程序的含义；Runtime Composition 决定由哪些机制实现。
把已经验证过的 Execution/Step/PTY/output/idempotency 提炼成稳定契约，
可以让语言、会话与调度独立演进，同时保持持久化和 reducer 的静态语义可判断。

扩展只能替换实现，不能添加动态 Step 或改变 Sequence/Parallel 的结果。
这也避免先重写 ExecutionPlan、随后又为 Composition 搬迁责任边界。

## 2026-08-31

### 触发

vNext 内核、协议、存储与 runner 已建立，需要让 daemon、client 和用户入口使用同一执行契约。

### 变化

完成 IPC v4 hard cut，`cue_core` 根 API 成为唯一 execution contract。

- daemon 启动时绑定 typed RuntimeAssembly；Hello/client identity、原子 operation claim、
  fact replay、live event、volatile secret store 和 PTY attachment 进入主路径。
- client 显式 snapshot cwd/env/umask，按 PutScope → compile → Submit 提交，使用
  RequestId/OperationId、strict v4 framing 和并发 response/event multiplexer。
- CLI/TUI 只使用 ExecutionId/StepId 与 ExecutionView/fact；删除旧 session、cron、
  resource、target 页面及 foreground epoch 状态机。
- 默认数据库改为独立的 `cued-v4.db`；旧 `cued.db` 只读归档，不导入。
  旧 Core/daemon/client/TUI 实现与兼容入口删除，公开文档、Skill、smoke 与架构守卫同步收口。

### 理由

旧 Scope identity、session cursor 和 schedule contract 与 v4 不等价。整体切换并归档旧数据，避免两套执行语义和无法证明正确的逐项迁移。
