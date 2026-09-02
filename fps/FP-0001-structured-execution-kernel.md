---
fp: 1
title: "Cue 结构化执行内核与 IPC v4"
type: Feature
authors:
  - "zrr1999"
created: 2026-09-01
supersedes: []
---

# FP-0001: Cue 结构化执行内核与 IPC v4

## 摘要

Cue 收敛为持久、可观察的本地结构化进程执行内核。Core 使用封闭的
`ExecutionPlan` 定义执行语义，以内容寻址的 `Scope` 在线性组合中传递状态；运行时
通过启动时 Composition 连接可替换机制。daemon 只提供严格 IPC v4，不在 kernel
内拥有 session、schedule、retry、resource、approval 或远程 target policy。

## 动机

旧模型同时承载 surface DSL、进程启动、session 生命周期、调度、资源分配、远程
transport 和 UI 状态，导致同一执行在 Core、daemon actor、client 与持久化层具有
不同表示。环境变量是当前命令局部覆盖还是沿 pipeline 继承、pipe 属性属于哪一端、
`cd` 是否改变后续命令、PTY 属于 process 还是 workflow 等问题都缺少唯一答案。

公开执行语义必须足够小，才能由纯 reducer 决定状态变化并稳定持久化；实现机制必须
保持开放，才能替换 process spawner、scope store、output store 等 provider。二者
需要明确分层，而不是用兼容 variant、service locator 或双协议继续扩大 Core。

## 设计

### Kernel ADT

Core 的完整值结构是以下乘积类型与和类型。`×` 表示所有字段必须同时存在，`+` 表示
只能选择一个 variant：

```text
Env             = Map<EnvKey, EnvValue>
EnvPatch        = Map<EnvKey, EnvEdit>
EnvEdit         = Set(EnvValue) + Unset

Scope           = AbsolutePath × Env × FileModeMask
ExecutionSpec   = ScopeHash × ExecutionPlan

ExecutionPlan   = Builtin(BuiltinCommand)
                + Run(Pipeline × IoMode)
                + Sequence(ExecutionPlan × ExecutionPlan × SequenceCondition)
                + Parallel(ParallelBranches × ParallelJoin)

BuiltinCommand  = Cd(CdPath)
                + Env(EnvMutation)
                + Umask(FileModeMask)

SequenceCondition = Success + Failure + Always
ParallelJoin      = All + AnySuccess
IoMode            = Captured + Pty

ParallelBranches  = Vec>=2<ExecutionPlan>
Pipeline          = Process × List<PipeContinuation>
PipeContinuation  = PipeLink × Process
PipeLink          = StdoutToStdin
                  + StderrToStdin
                  + StdoutAndStderrToStdin
Process           = Argv × EnvPatch
Argv              = NonEmpty<NulFreeString>

AbsolutePath      = Path where is_absolute && !contains_nul
CdPath            = Path where !is_empty && !contains_nul
FileModeMask      = u16 where value & !0o777 == 0
EnvKey            = String where !is_empty && !contains('=') && !contains_nul
EnvValue          = String where !contains_nul
```

对应的执行树骨架是：

```rust
enum ExecutionPlan {
    Builtin {
        command: BuiltinCommand,
    },
    Run {
        pipeline: Pipeline,
        io: IoMode,
    },
    Sequence {
        first: Box<ExecutionPlan>,
        then: Box<ExecutionPlan>,
        when: SequenceCondition,
    },
    Parallel {
        branches: ParallelBranches,
        join: ParallelJoin,
    },
}
```

这里有几项由结构直接保证的不变量：

- `Scope` 是 `cwd × env × umask` 的完整、不可变快照，并由内容哈希标识；不存在
  `parent`、`delta` 或未说明的环境来源。
- `BuiltinCommand` 恰好三个 variant；`EnvMutation` 至少包含一项 edit。同一个 EnvKey
  在 Map 中只能对应 `Set(value)` 或 `Unset` 之一，无法同时 set 与 unset。
- `Argv` 至少包含一个非空 executable，所有 word 都不得含 NUL；`Pipeline` 总有 `first`
  process，之后每增加一个 process 就必须同时增加一个 `(link, next)`，因此不存在悬空
  link 或长度软约束。
- `ParallelBranches` 至少有两个分支。extension 可以替换 leaf 的实现机制，但不能增加
  `ExecutionPlan`、`BuiltinCommand`、`PipeLink` 或 `IoMode` variant。
- stdio、PTY、credentials、resource limits、sandbox 与 workspace realization 不属于
  Scope；`Run` 持有 I/O 模式，一个 PTY Run/Pipeline 只有一个 terminal endpoint，
  builtin 不拥有 stdio 或 PTY。

Scope 流向由 plan variant 唯一决定：

| Plan | 子项输入 Scope | Plan 输出 Scope |
|---|---|---|
| `Builtin(command)` | 输入 `S` | 成功时为 `apply(command, S)`；失败时为 `S` |
| `Run(pipeline, io)` | 输入 `S` | 始终为 `S` |
| `Sequence(first, then, when)` | `first` 得到 `S`；被选择的 `then` 得到 `first` 的输出 | 实际执行路径的最终 Scope |
| `Parallel(branches, join)` | 每个 branch 都得到同一个 `S` | `S`；分支 Scope 永不隐式合并 |

`SequenceCondition::Success`、`Failure`、`Always` 分别在 first 成功、失败、任意非
`Skipped` 终态后选择 then；未选择的 leaf 标记 `Skipped`。`ParallelJoin::All` 等待所有分支，
`AnySuccess` 在首个成功分支出现后取消或跳过其余分支，但两种 join 都不合并 Scope。

### Reducer ADT

Plan 是静态结构；运行中的唯一持久状态由下列 ADT 表示：

```text
ExecutionSnapshot = ExecutionId
                  × ExecutionSpec
                  × Vec<StepRecord>
                  × Option<ExecutionCancelReason>

StepRecord       = StepId
                 × StepState
                 × Option<InputScopeHash>
                 × Option<OutputScopeHash>

StepState        = Pending
                 + Running
                 + Succeeded
                 + Failed(StepFailure)
                 + Skipped(SkipReason)
                 + Cancelled(StepCancelReason)

StepAction       = Builtin(BuiltinCommand)
                 + Run(Pipeline × IoMode)

ExecutionTransition = Vec<ReadyStep>
                    × Vec<StepIdToCancel>
                    × Vec<NewScope>
```

每个 `Builtin` 与 `Run` leaf 按 plan preorder 获得稳定 `StepId`；组合节点本身不是 Step。
`input_scope` 只在 reducer 把 leaf 推进为 ready 时写入；完成的 `Builtin`/`Run` 才有
`output_scope`，`Skipped`/`Cancelled` 保持为空。reducer 读取 Snapshot 和 typed completion，
原子地产生新 Snapshot、facts 与 `ExecutionTransition`；runtime 只能实现 `StepAction` 并
回报结果，不能自行决定分支、跳过规则或 Scope 流向。

### Surface language

文本 DSL 在 `cue-language` 中编译为 Core ADT，Core 不保存 token、mode 或语法糖。
环境前缀的语法与编译规则是：

```text
assignment = [A-Za-z_][A-Za-z0-9_]* "=" value
process    = assignment* command-word argument*
```

只连续识别 command 前的 assignment，并在第一个 `=` 处分隔 key/value；value 可以为空或
包含 `=`，不做 `$VAR`、命令替换或其他 shell 展开。同一 key 重复出现时最后一项生效。
所得 Map 编译为该 process 的 `EnvPatch`。它不影响同一 pipeline 的其他 process，也不
改变后续 Sequence 的 Scope；command 后的 `A=B` 是普通 argv，只有 assignment 而没有
command 的输入非法，assignment 也不能修饰 Cue builtin。需要跨多项共享环境时，surface
construct 必须显式表示 lexical scope，并在编译时展开，不能引入运行时的左到右环境继承。

例如：

```text
A=left printenv A |> grep left

=> Run(
     Pipeline(
       Process(["printenv", "A"], {A: Set("left")}),
       [(StdoutToStdin, Process(["grep", "left"], {}))]
     ),
     Captured
   )
```

第二个 process 的 EnvPatch 为空；它只从执行输入 Scope 取环境，不从第一个 process
继承 `A`。与此不同，显式 builtin 会产生新 Scope：

```text
env set A=left -> printenv A

=> Sequence(
     Builtin(Env({A: Set("left")})),
     Run(Pipeline(Process(["printenv", "A"], {}), []), Captured),
     Success
   )
```

`cd`、`env set/unset` 与 `umask` 编译为三个 Core builtin。schedule、retry、resource、
approval、session 与 remote target 命令可以由外部 producer 或 extension 提供，但不
得增加 `ExecutionPlan` variant。

### Composition 与运行时

Execution ADT 是 closed semantics；Composition 是 open implementation graph。provider
在 daemon bootstrap 时声明 capability、依赖和顺序，经校验后一次性绑定为 typed
runtime ports。运行中不得按字符串查找服务，也不得让 extension 注入新的 Core
variant。

runtime 只实现 reducer 产生的 action：应用 builtin、启动 captured pipeline 或 PTY
process group、写入绝对 offset output、传播 control/cancel，以及把结果回送 reducer。
policy owner 可以生成和提交 `ExecutionSpec`，但不能接管已有 Execution 的状态转换。

### IPC、持久化与 daemon

IPC v4 使用严格长度前缀消息和封闭 schema。修改状态的 Command 必须携带稳定
`OperationId` 以支持幂等重放；Query 只读。协议暴露 Scope、Execution/Step projection、
facts、绝对 output ranges、cancel/control 和显式 PTY attachment，不暴露 session、
schedule 或 resource request。

daemon 持久化非敏感 Scope、Execution projection、facts 与 operation outcome。包含敏感
环境的 Scope 和引用它的执行只能保留在易失存储，避免凭据落盘。重启恢复时，已处于
Running 的 step 以结构化 interruption 失败，然后 reducer 决定后续状态。

本地 host 使用单一 Unix socket 和独立 IPC v4 数据库，并保证 lifecycle command 的
ack 先于 draining event。restart 在旧 listener 排空并释放 socket/lock 后启动 successor。

## 兼容性

这是有意的 hard cut。IPC v3 请求、actor 状态与数据库不翻译成 v4；默认启动时，旧
`cued.db` 及 SQLite sidecar 只归档为只读文件，不导入不兼容语义。v4 使用独立数据库，
旧客户端不能连接 v4 daemon，删除的 session/schedule/resource/target 命令没有 kernel
兼容入口。

回滚边界是停止 v4 daemon、恢复旧二进制，并从只读 archive 的副本显式恢复旧数据；
v3 与 v4 不得同时拥有同一 socket。外部 policy owner 的迁移方式是把既有工作流编译为
`ExecutionSpec`，通过 IPC v4 观察 Execution/Step/fact，而不是要求 daemon 恢复旧状态机。

## 验证

- Core serialization 测试证明 `ExecutionPlan` 只有四个 variant，非法 pipeline、空并行
  和冲突 env edit 无法通过构造或反序列化。
- reducer 测试覆盖稳定 StepId、Sequence Scope threading、Parallel fork/no-merge、
  `All`/`AnySuccess`、skip/cancel、snapshot restore 与 restart interruption。
- Language 测试覆盖三个 builtin、process-local `A=B`、assignment-only 拒绝、pipe link
  和每个 Run 的 captured/PTY 选择。
- Runtime 测试以真实 process group 验证 pipeline wiring、captured output、PTY 单 terminal、
  control/cancel 与 writer failure。
- Protocol/store 测试覆盖 strict framing、unknown field 拒绝、幂等 operation、原子 projection
  与 fact commit、绝对 output offset，以及敏感 Scope 不落盘。
- daemon 与安装产物 smoke 覆盖 Unix socket lifecycle、restart successor、旧数据库归档、
  CLI/TUI submission、output 读取和 wheel/sdist 命令面。
- 架构检查拒绝恢复 IPC v3 模块、兼容 import、daemon 内 surface parser，以及 Core 对
  runtime、transport、storage 或 frontend 的依赖。
