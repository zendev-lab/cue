---
fp: 1
title: "Cue 结构化执行内核与 IPC v4"
type: Feature
authors:
  - "zrr1999"
created: 2026-09-01
supersedes: []
defines:
  - execution-plan
  - scope
  - step
  - reducer
  - fact
  - cancelling
  - runtime-action
  - run-completion
  - attempt
  - quiescent
  - operation-id
  - sensitive
  - volatile-execution
---

# FP-0001: Cue 结构化执行内核与 IPC v4

## 摘要

Cue 收敛为持久、可观察的本地结构化进程执行内核。Core 使用封闭的
`ExecutionPlan` 定义执行语义，以内容寻址的 `Scope` 在线性组合中传递状态；运行时
通过启动时 Composition 连接可替换的实现机制。daemon 只提供严格 IPC v4，不在内核
中拥有 session、schedule、retry、resource、approval 或远程 target policy。

本提案区分三类东西：

1. **语义状态**：`ExecutionPlan`、`StepState`、`Scope` 等，由 Core 定义；
2. **运行时动作**：Core 已决定、但尚未在操作系统中完成的动作，例如启动或取消 Step；
3. **运行时结果**：动作实际完成后返回 Core 的结果，例如成功、失败或取消完成。

状态变化与待执行动作必须先持久化，动作之后才能真正执行。运行时结果再次进入 reducer，
成为下一次状态变化的输入。这样“请求发生”和“结果已经发生”不会被混为一个事实。

## 动机

旧模型同时承载 surface DSL、进程启动、session 生命周期、调度、资源分配、远程
transport 和 UI 状态，导致同一执行在 Core、daemon、client 与持久化层具有不同表示。
环境变量是当前命令局部覆盖还是沿 pipeline 继承、pipe 属性属于哪一端、`cd` 是否改变
后续命令、PTY 属于 process 还是 workflow 等问题都缺少唯一答案。

公开执行语义必须足够小，才能由纯 reducer 决定状态变化并稳定持久化；实现机制必须
保持开放，才能替换 process spawner、scope store、output store 等 provider。二者需要
明确分层，而不是用兼容 variant、service locator 或双协议继续扩大 Core。

另外，`spawn`、signal、PTY control 等操作系统动作不能与 SQLite 提交组成一个 ACID
事务。如果先把预期结果写成事实，再“尽力执行”外部动作，就会出现持久状态已经终止、
现实进程却仍在运行等错误。因此本提案把“状态转换”和“运行时动作”显式分开。

## 术语与定义

`defines` frontmatter 与下列 `term-*` 锚点组成 zendev 的定义所有权契约。只有需要由
FP-0001 长期拥有的稳定领域概念才注册 ownership；事务发件箱、进程监护器等参考实现仍可
在表中解释，但不取得定义所有权。

| 术语 | 定义 |
|---|---|
| <a id="term-execution-plan"></a>**执行计划 `ExecutionPlan`** | Core 中封闭的执行树，只描述 Cue 允许的执行组合语义。 |
| <a id="term-scope"></a>**执行范围 `Scope`** | `cwd × env × umask` 的完整不可变快照，由内容哈希标识。 |
| <a id="term-step"></a>**步骤 `Step`** | `Builtin` 或 `Run` 叶子。组合节点本身不是 Step。 |
| <a id="term-reducer"></a>**归约器 `reducer`** | 纯状态转换函数。读取当前快照和一个已确认输入，计算下一快照、事实及待执行动作。 |
| <a id="term-fact"></a>**事实 `Fact`** | 已由 reducer 接受并持久化的状态变化记录；不能描述尚未完成的外部动作结果。 |
| <a id="term-cancelling"></a>**取消中 `Cancelling`** | reducer 已接受对活动 Step 或 Execution 的取消请求，但对应运行时尝试尚未报告最终结果的非终态。 |
| <a id="term-runtime-action"></a>**运行时动作 `RuntimeAction`** | reducer 要求 runtime 实现的外部动作，例如 `RealizeStep`、`CancelStep`。它不是代数效应系统中的 effect handler。 |
| <a id="term-run-completion"></a>**运行时结果 `RunCompletion`** | runtime 对一个已发生执行尝试的最终报告，例如成功、失败或取消完成。 |
| <a id="term-attempt"></a>**执行尝试 `attempt`** | 一个 Step 的一次物理实现，例如一组实际操作系统进程。当前 Execution 内不隐式 retry。 |
| **实现机制 `realization`** | 把 Core 的语义动作落实为实际进程、PTY、workspace、resource 等物理行为。 |
| **事务发件箱 `Transactional Outbox`** | 一种持久化实现方式：在同一数据库事务中写入状态和“之后要执行的动作”，提交后再由 dispatcher 执行动作。 |
| <a id="term-quiescent"></a>**静止 `quiescent`** | 某个执行尝试已经确定不再运行，也不会再产生属于该尝试的活动进程。 |
| **进程监护器 `guardian`** | 可选实现机制：独立于 daemon 持有子进程所有权，在 daemon 异常退出时负责终止并回收其进程。它不是 Core 概念。 |
| <a id="term-operation-id"></a>**操作标识 `OperationId`** | 一个逻辑 IPC Command 的稳定身份，用于断线重连后的幂等重放。 |
| <a id="term-sensitive"></a>**敏感值 `Sensitive`** | 不允许进入持久存储的环境值分类；安全性不能仅靠变量名猜测。 |
| <a id="term-volatile-execution"></a>**易失执行 `Volatile Execution`** | 含敏感值的 Execution；其 Scope、projection、facts、operation outcome 等仅存在于内存。 |

本文中的 `effect` 若用于一般说明，仅表示“对外部世界产生的副作用”。规范数据结构统一
使用“运行时动作 `RuntimeAction`”，避免与程序语言中的**代数效应（algebraic effects）**
混淆。代数效应是一种用 effect operation、handler、resumption 等结构描述和解释计算效应的
语言机制；Cue 当前的 `RuntimeAction` 只是 reducer 与 runtime 之间的动作协议，并未引入
这套语言级机制。

## 设计

### Kernel ADT

Core 的主要值结构如下。`×` 表示所有字段同时存在，`+` 表示和类型的不同 variant：

```text
Sensitivity     = Normal + Sensitive
EnvValue        = NulFreeString × Sensitivity
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
```

结构本身保证以下不变量：

- `Scope` 是完整快照，不存在隐含 `parent`、`delta` 或 ambient environment；
- Core builtin 恰好是 `Cd`、`Env`、`Umask`；
- `Pipeline` 每增加一个 process 必须同时带一个 link，不存在 `processes + links` 长度软约束；
- `ParallelBranches` 至少两个分支；
- extension 可以替换实现机制，但不能增加 `ExecutionPlan`、`BuiltinCommand`、`PipeLink`
  或 `IoMode` variant；
- `Sensitive` 是值的显式分类。变量名 heuristic 可以辅助自动标注或警告，但不能作为
  “不落盘”的安全边界。

### Scope 与组合语义

Scope 流向只由 plan variant 决定：

| Plan | 子项输入 Scope | Plan 输出 Scope |
|---|---|---|
| `Builtin(command)` | 输入 `S` | 成功为 `apply(command, S)`；失败为 `S` |
| `Run(pipeline, io)` | 输入 `S` | 始终为 `S` |
| `Sequence(first, then, when)` | `first` 得到 `S`；被选择的 `then` 得到 `first` 输出 | 实际执行路径的最终 Scope |
| `Parallel(branches, join)` | 每个 branch 都得到同一个 `S` | `S`；分支 Scope 不隐式合并 |

`SequenceCondition::Success`、`Failure`、`Always` 分别在 first 成功、失败、任意非
`Skipped` 终态后选择 then。未选择的叶子标记 `Skipped`。

`ParallelJoin::All` 等待所有分支。`AnySuccess` 在出现首个成功分支后，跳过尚未启动的
loser，并请求取消仍在运行的 loser。逻辑 winner 可以先确定，但整个 Parallel 只有在所有
已启动 loser 都进入终态后才成为终态，因此结构化执行不会遗留仍归属于该节点的活动进程。

### Reducer 与取消语义

运行中的主要状态为：

```text
ExecutionSnapshot = ExecutionId
                  × ExecutionSpec
                  × Vec<StepRecord>
                  × Option<ExecutionCancelRequest>

ExecutionCancelRequest = ExecutionCancelReason × CancelMode

StepRecord       = StepId
                 × StepState
                 × Option<InputScopeHash>
                 × Option<OutputScopeHash>

StepState        = Pending
                 + Running
                 + Cancelling(StepCancelReason × CancelMode)
                 + Succeeded
                 + Failed(StepFailure)
                 + Skipped(SkipReason)
                 + Cancelled(StepCancelReason)

ExecutionState   = Pending
                 + Running
                 + Cancelling(ExecutionCancelReason)
                 + Succeeded
                 + Failed
                 + Cancelled(ExecutionCancelReason)

RuntimeAction    = RealizeStep(ReadyStep)
                 + CancelStep(StepId × StepCancelReason × CancelMode)
```

`Running` 表示一个物理执行尝试已经被提交为需要实现；它可以仍处于启动过程中。
`Cancelling` 表示 reducer 已接受取消请求，但 runtime 尚未报告该 attempt 的最终结果。
`Cancelled` 才表示该 attempt 已确认因取消而结束，或 Pending Step 在从未启动的情况下被
用户取消。

取消遵循以下规则：

- Pending Step 被用户取消时可以直接进入 `Cancelled`；它没有需要清理的物理 attempt；
- Pending 的 `AnySuccess` loser 进入 `Skipped`；
- Running Step 收到取消请求后进入 `Cancelling`，并产生 `CancelStep`；
- `Cancelling` 后如果进程仍自然成功，则进入 `Succeeded`；自然失败则进入 `Failed`；
  只有 runtime 明确报告 cancellation completion 才进入 `Cancelled`；
- cancellation 是 best-effort，不要求 signal 一定抢赢自然完成；
- `Force` 可以强化先前的 `Graceful`；同等级重复请求幂等；
- 并发 race 只由 reducer **接受并提交输入的顺序**决定，不按墙钟时间猜测；
- 一个 Step 一旦已提交终态，之后到达的取消请求为 no-op；反之，先提交取消请求则进入
  `Cancelling`，随后第一个被 reducer 接受的 terminal completion 决定最终状态。

用户级 `ExecutionCancelRequest` 的含义是“不要再启动新的工作”，不是“整个 Execution 已经
取消完成”。只要还有 `Running` 或 `Cancelling` Step，对外状态就是 `Cancelling`。如果所有
正在取消的进程最后都自然成功，Execution 可以最终 `Succeeded`。

### 状态与运行时动作的持久化边界

reducer 在不可变快照上纯计算：

```text
current snapshot + input
        |
        v
      reducer
        |
        +--> next snapshot
        +--> facts
        +--> new scopes
        `--> RuntimeAction[]
```

所有持久实现都必须满足以下顺序：

```text
compute
  -> atomic commit
  -> publish committed state/facts
  -> execute committed RuntimeAction
  -> receive RunCompletion
  -> next reducer input
```

因此：

- store commit 成功之前，不得修改 authoritative live projection；
- store commit 成功之前，不得发布 fact；
- store commit 成功之前，不得启动进程或发送取消 signal；
- commit 失败时，本次 transition 对外完全不可见；
- runtime action 被成功派发，只说明“动作已经开始实现”，不能直接产生 Step 终态；
- 只有运行时结果才能驱动 `Succeeded`、`Failed` 或 `Cancelled`。

#### 事务发件箱

满足上述不变量的一种推荐实现是**事务发件箱（Transactional Outbox）**。

可以把它理解成：数据库事务除了写“新状态”，还同时往同一个数据库里放一封“待办信”：

```text
同一个 SQLite transaction

  execution = Cancelling
  fact      = Running -> Cancelling
  outbox    = "请向 E1/S1 发出 Force cancel"
```

只有整个事务提交后，“邮递员”才读取 outbox 并真正发送 signal。这样不会出现：

```text
状态提交失败，但 signal 已经发出
```

也不会出现：

```text
状态已经提交，但取消动作在 crash 前完全丢失且没有任何 durable 记录
```

**事务发件箱是持久层实现模式，不是新的 Core 执行代数。** 如果未来有另一种实现能证明
同样的原子性、提交前不执行动作、恢复时不重复物理 attempt 等不变量，可以替换它。
因此 `EffectId`、`Pending/Dispatched/Completed` 等发件箱内部状态不属于 Core 公共 ADT。

### 崩溃恢复与进程所有权

daemon crash 后最大的风险不是“数据库里有没有动作”，而是：旧 daemon 启动的操作系统
进程可能仍然活着。因此恢复必须满足：

> 在旧 attempt 是否仍活动无法确定时，successor 既不能盲目再次 spawn，也不能提前把该
> Step 宣布为 terminal failure。

只有确认旧 attempt 已经静止（quiescent）后，才能向 reducer 注入 restart interruption。
如何证明静止属于 runtime 实现机制，不属于 Core 语义。可用方案例如：

- **进程监护器 `guardian`**：一个独立小进程持有子进程所有权；daemon 意外退出时，
  guardian 自动终止并回收整组进程；
- **稳定 attempt handle**：successor 能重新定位旧 attempt，查询、接管或终止它，并等待
  它完全退出；
- 其他能证明“旧 attempt 不再运行”的机制。

如果 runtime 无法证明旧 attempt 已静止，则恢复必须 fail closed：不启动同一 Step 的第二个
attempt，也不启动依赖它的新工作，更不能发布虚假的 terminal fact。

`guardian` 因此只是一个**可选实现例子**，不是 Cue 必须新增的核心组件。

### Surface language

文本 DSL 在 `cue-language` 中编译为 Core ADT。Core 不保存 token、mode 或语法糖。
环境前缀：

```text
assignment = [A-Za-z_][A-Za-z0-9_]* "=" value
process    = assignment* command-word argument*
```

只连续识别 command 前的 assignment，并在第一个 `=` 处分隔 key/value；value 可以为空或
包含 `=`，不做 `$VAR`、命令替换或其他 shell 展开。同一 key 重复时最后一项生效。

`A=B command` 只产生该 Process 的 `EnvPatch`，不影响 pipeline 的其他 process，也不改变
后续 Sequence 的 Scope。command 后的 `A=B` 是普通 argv；只有 assignment 而没有 command
的输入非法；assignment 不能修饰 Cue builtin。

需要跨 Step 修改环境时必须显式使用 `Env` builtin，例如：

```text
env set A=left -> printenv A
```

### 敏感数据与持久性

初始 Scope、Process EnvPatch、Env builtin mutation 中的环境值都使用同一种 sensitivity
分类。任意位置出现 `Sensitive`，整个 Execution 从提交开始就是 `Volatile`。

`Volatile Execution` 的 Scope、projection、facts、operation outcome 以及持久化动作记录都
只能存在于内存，且生命周期内不能升级为 Durable。这样避免两类问题：

- `SECRET=x command` 的 secret 藏在 Process EnvPatch 中而被错误落盘；
- `env set SECRET=x -> command` 在 durable execution 中途生成 SQLite 无法引用的 volatile
  Scope。

变量名 heuristic 只能辅助分类，不能替代显式 sensitivity。长期更推荐 credential provider
在 realization 阶段根据 opaque reference 注入 secret bytes，使秘密值本身不进入 semantic
plan、Scope 或 facts。

### Composition 与运行时

Execution ADT 是封闭语义；Composition 是开放实现图。provider 在 daemon bootstrap 时声明
capability、依赖与顺序，经校验后绑定为 typed runtime ports。运行时不得按字符串 service
locator 动态决定 Core 语义。

provider 可以改变**如何实现**一个 Step，但不能改变**这个 Step 的语义身份**。因此以下
内容视为只读语义输入：

```text
StepId × Pipeline × IoMode × Scope
```

workspace、wrapper、resource handle、sandbox、secret injection 等属于物理实现上下文，可以
由 provider 构造或修改，但 provider 不能悄悄替换 StepId、logical argv/EnvPatch、IoMode 或
logical Scope。

### IPC、client 与 daemon

IPC v4 使用严格长度前缀消息和封闭 schema。修改状态的 Command 必须携带稳定
`OperationId`；Query 只读。

`RequestId` 只用于一条连接内的请求响应关联；`ClientId + OperationId` 才表示逻辑 Command
身份。自动断线重试同一 Command 时必须复用原 `ClientId + OperationId`，可以分配新的
`RequestId`。

Query 的 read-only 是端到端属性：`list/show/wait/output/help` 等查询不得为了获得 ScopeHash
先执行 `PutScope`。frontend 应先解析 intent，只有真正提交 Execution 时才执行：

```text
PutScope -> compile with ScopeHash -> SubmitExecution
```

protocol output range 使用绝对 offset。surface/client 的 `tail N` 必须返回最后 N bytes，不能
简单翻译成 `offset = 0, max_bytes = N`。

lifecycle command 的“ack 先于 draining”必须由真实 happens-before 保证：先提交 command
outcome，connection writer 写出并 flush ack，然后才通知 host 停止 accept 和 drain。固定
sleep 不能替代这个顺序。

### 权威顺序

所有 reducer transition 最终遵守一个统一顺序：

```text
计算下一状态
  -> 原子持久化状态、事实和待执行动作
  -> 更新内存中的权威 projection
  -> 发布已提交 facts
  -> 执行已提交 RuntimeAction
```

observer 只能看到已经持久化的 FactEvent。任何 store error 都不能留下“内存已经前进、SQLite
没有前进”的分叉状态。

## 兼容性

这是有意的 hard cut。IPC v3 请求、actor 状态与数据库不翻译成 v4；默认启动时，旧
`cued.db` 及 SQLite sidecar 只归档为只读文件，不导入不兼容语义。v4 使用独立数据库，
旧客户端不能连接 v4 daemon，删除的 session/schedule/resource/target 命令没有 kernel
兼容入口。

回滚边界是停止 v4 daemon、恢复旧二进制，并从只读 archive 的副本显式恢复旧数据；v3 与
v4 不得同时拥有同一 socket。外部 policy owner 的迁移方式是把既有工作流编译为
`ExecutionSpec`，通过 IPC v4 观察 Execution/Step/Fact，而不是要求 daemon 恢复旧状态机。

## 验证

- Core serialization 测试证明 `ExecutionPlan` 只有四个 variant，非法 pipeline、空并行和
  冲突 env edit 无法通过构造或反序列化；
- reducer 测试覆盖稳定 StepId、Sequence Scope threading、Parallel fork/no-merge、
  `Running -> Cancelling -> terminal`、cancel/completion race、Force escalation 与 restart
  interruption；
- 持久化测试证明 projection/facts/new scopes/RuntimeAction 原子记录，commit failure 不修改
  live state且不执行动作；
- 崩溃恢复测试证明无法确认旧 attempt 已静止时不会重复 spawn，也不会发布虚假终态；
- Language 测试覆盖三个 builtin、process-local `A=B`、assignment-only 拒绝、pipe link 和
  per-Run captured/PTY；
- Runtime 测试验证 pipeline wiring、captured output、PTY、control/cancel，并验证 provider
  无法修改 semantic Step 输入；
- Protocol/store/client 测试覆盖 strict framing、OperationId 重放、query 不触发 `PutScope`、
  `tail N` suffix，以及 Sensitive data 不落盘；
- daemon smoke 覆盖 Unix socket lifecycle、ack flush-before-drain、restart successor 与旧
  database archive；
- 架构检查拒绝恢复 IPC v3 模块、daemon 内 surface parser，以及 Core 反向依赖 runtime、
  transport、storage 或 frontend。
