---
fp: 1
title: "Cue 结构化执行内核与 IPC v4"
type: Feature
authors:
  - "zrr1999"
created: 2026-09-01
supersedes: []
defines:
  - execution
  - execution-plan
  - scope
  - step
  - fact
  - cancellation
  - composition
  - operation-id
  - sensitivity
---

# FP-0001: Cue 结构化执行内核与 IPC v4

## 摘要

Cue 收敛为持久、可观察的本地结构化进程执行内核。Core 用封闭的 `ExecutionPlan`
定义执行语义，用不可变 `Scope` 表达执行上下文，用纯归约器决定状态变化；运行时负责把
已经提交的 Step 状态落实为真实运行时状态。守护进程只提供严格 IPC v4，不在内核中拥有会话、
调度、重试、资源、审批或远程目标策略。

本提案坚持两个边界：**决定与事实分离，语义与交付分离**。Core 只提交“某个 Step 现在应该
处于什么状态”，并用 `StepId` 标记哪些 Step 需要运行时重新跟进；运行时每次都读取最新已提交
快照，把现实状态向该语义状态收敛。`StepId` 工作项不复制 action、输入 Scope 或取消模式。
已经进入 `Running` 的 Step 只有在运行时回报 completion 后才能成为终态；Core 负责解释该
completion 对状态和 Scope 的语义影响。

状态、事实和运行时跟进必须原子提交；提交引用的 Scope 必须已经持久可读。运行时外部动作
只能发生在提交之后。

## 动机

旧模型同时承载表面 DSL、进程启动、会话生命周期、调度、资源分配、远程传输和 UI 状态，
导致同一次执行在 Core、守护进程、客户端和持久层具有不同表示。环境变量是当前命令局部覆盖
还是沿 pipeline 继承、pipe 属性属于哪一端、`cd` 是否改变后续命令、PTY 属于进程还是工作流
等问题都缺少唯一答案。

公开执行语义必须足够小，才能由纯归约器决定并稳定持久化；实现机制必须保持开放，才能替换
进程启动器、Scope 存储、输出存储等实现。二者需要明确分层。

另外，`spawn`、进程信号、PTY 控制等操作系统动作不能和 SQLite 提交组成一个 ACID
事务。如果把“准备做什么”提前写成“已经发生什么”，就会产生持久状态与现实进程不一致的
错误。如果再把 action、Scope、取消模式复制进待执行命令，又会产生第二份可能过期的语义
事实源。因此持久状态负责表达“现在应该是什么”，运行时工作只负责可靠唤醒实现层去读取并
落实最新状态。

## 设计

### 通用术语

下列词沿用其通常的系统或函数式编程含义，不作为 Cue 自己拥有的领域定义：

| 术语 | 本文用法 |
|---|---|
| **归约器（reducer）** | 纯状态转换函数；输入当前值与一个已确认事件，计算下一值及派生输出。 |
| **状态收敛（reconciliation）** | 读取最新已提交状态，使运行时实体逐步符合该状态；过期唤醒本身不携带独立语义。 |
| **事务发件箱（Transactional Outbox）** | 把业务状态与待执行外部工作记录在同一数据库事务中的常见持久化模式。 |
| **静止（quiescent）** | 某个运行尝试已经确定不会继续运行，也不会再产生属于该尝试的活动进程。 |
| **失败关闭（fail closed）** | 无法证明操作安全时拒绝继续，而不是按乐观假设推进。 |

### <a id="term-execution"></a>执行 `Execution`

`Execution` 是 Cue 对一次结构化执行的唯一权威对象。它由不可变的 `ExecutionSpec`、稳定的
Step 记录和可选的执行级取消模式组成：

```text
ExecutionSnapshot = ExecutionId
                  × ExecutionSpec
                  × Vec<StepRecord>
                  × Option<CancelMode>
```

`ExecutionId` 是一次 Execution 的稳定不透明标识，在同一持久化执行命名空间内唯一，
恢复时保持不变；具体编码由协议规定。

`ExecutionState` 不是第二份可独立修改的状态，而是从计划、Step 状态和取消请求**派生出的
投影**。持久层不得同时维护另一套生命周期真相。

归约器是 Execution 生命周期的唯一决策者。Sequence 条件、Parallel 结束规则、Scope 流向、
取消请求和最终状态都只能由同一状态机决定；守护进程和运行时不能重新解释这些规则。

### <a id="term-execution-plan"></a>执行计划 `ExecutionPlan`

`ExecutionPlan` 是 Core 中封闭的结构化执行树。它只允许四种组合：

```text
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

结构本身保证：

- Core builtin 恰好只有 `Cd`、`Env`、`Umask`；
- Pipeline 每增加一个进程必须同时带一个 link，不依赖 `links.len() == processes.len()-1`
  之类的运行时软约束；
- Parallel 至少有两个分支；
- 扩展可以替换实现，但不能增加 `ExecutionPlan`、`BuiltinCommand`、`PipeLink` 或 `IoMode`
  的语义 variant。

`Argv` 的首项 executable 必须非空，其余参数允许空字符串；所有项都不得含 NUL。构造与
反序列化均须校验这些约束。叶子使用的数据类型见 [Scope](#term-scope)。

### <a id="term-scope"></a>执行范围 `Scope`

`Scope` 是 `cwd × env × umask` 的完整不可变快照，由内容哈希标识：

```text
NulFreeString = String where !contains_nul
AbsolutePath  = Path where is_absolute && !contains_nul
CdPath        = Path where !is_empty && !contains_nul
FileModeMask  = u16 where value & !0o777 == 0

EnvKey        = NulFreeString where !is_empty && !contains('=')
Sensitivity   = Normal + Sensitive
EnvValue      = NulFreeString × Sensitivity
Env           = Map<EnvKey, EnvValue>

EnvPatch      = Map<EnvKey, EnvEdit>
EnvEdit       = Set(EnvValue) + Unset
EnvMutation   = EnvPatch where !is_empty

Scope         = AbsolutePath × Env × FileModeMask
ExecutionSpec = ScopeHash × ExecutionPlan
```

`Env` builtin 至少包含一项 edit，`Env({})` 非法。同一 `EnvKey` 在 Map 中只能对应
`Set(value)` 或 `Unset` 之一，不能同时 set 与 unset。敏感性分类的语义见
[Sensitivity](#term-sensitivity)。

Scope 不含隐式 parent、delta 或 ambient environment，也不包含 stdio、PTY、credentials、
resource limits、sandbox 或 workspace realization。每个 `Run` 自己持有 I/O 模式；一个
PTY Run/Pipeline 只有一个 terminal endpoint，Builtin 不拥有 stdio 或 PTY。

`ScopeHash` 是 Scope 完整值的内容哈希，包含环境值的敏感性分类；改变分类不能复用旧哈希。
正常成功或失败时，Scope 的流向只由计划结构决定：

| Plan | 子项输入 Scope | Plan 输出 Scope |
|---|---|---|
| `Builtin(command)` | 输入 `S` | 成功为 `apply(command, S)`；失败为 `S` |
| `Run(pipeline, io)` | 输入 `S` | 始终为 `S` |
| `Sequence(first, then, when)` | `first` 得到 `S`；被选择的 `then` 得到 `first` 输出 | 实际执行路径的最终 Scope |
| `Parallel(branches, join)` | 每个分支都得到同一个 `S` | `S`；分支 Scope 不隐式合并 |

因此 `cd`、`env`、`umask` 的影响只能通过 Sequence 显式传播；Parallel 永远从同一个输入分叉，
不会偷偷合并分支状态。

### <a id="term-step"></a>步骤 `Step`

`Step` 是 `Builtin` 或 `Run` 叶子。组合节点不是 Step。每个叶子在 Execution 创建时按计划
前序遍历分配从 1 开始的序号：Sequence 先 first 后 then，Parallel 按分支存储顺序遍历。
`StepId` 包含所属 `ExecutionId` 和该序号，在 Execution 生命周期及恢复后保持不变；不同
Execution 的同序号叶子不是同一个 Step。StepId 足以定位所属 Execution 及其叶子。

Step 持久记录输入、输出 Scope 哈希和状态：

```text
StepId     = ExecutionId × (u32 where value >= 1)

StepRecord = StepId
           × StepState
           × Option<InputScopeHash>
           × Option<OutputScopeHash>

StepState  = Pending
           + Running
           + Cancelling(StepCancelCause × CancelMode)
           + Succeeded
           + Failed(StepFailure)
           + Skipped(SkipReason)
           + Cancelled(StepCancelCause)

StepFailure = Exit(i32)
            + Signal(i32)
            + Spawn(String)
            + Builtin(String)
            + Infrastructure(String)
SkipReason  = ConditionNotMet + AnySuccessSatisfied
```

`Exit` 表示非零退出码；零退出是成功，明确取消使用 cancellation completion 而不是伪造失败。
`InputScopeHash` / `OutputScopeHash` 都是 `ScopeHash`，只是字段角色不同。

| Step 状态 | input_scope | output_scope |
|---|---|---|
| `Pending` / `Skipped` | 无 | 无 |
| `Running` / `Cancelling` | 必须存在 | 无 |
| `Succeeded` / `Failed` | 必须存在 | 必须存在 |
| `Cancelled` | 从 Pending 取消时无；曾进入 Running 时保留 | 无 |

输入 Scope 在 `Pending -> Running` 时写入，之后不可修改。`Skipped` / `Cancelled` 不产生
输出 Scope；取消不构造一个假装成功的 Builtin 结果。恢复必须校验叶子数量、StepId 归属与
顺序、Scope 字段存在性和计划中的 Scope 关系，不接受仅在字段类型上合法的任意快照。

Builtin 和 Run 使用同一套 Step 生命周期。`Running` 的精确定义是：**该 Step 已经被归约器接纳为
活动工作，并且同一次已提交状态转换已经把该 `StepId` 标记为需要运行时跟进。** 它表示存在待
完成的 realization，不要求存在操作系统进程。

因此 ready 决策不能拆成“先返回完整启动命令、再由调用者另行 `mark_running()`”两个阶段。
归约器一旦决定 Step ready，就必须在同一个 transition 中完成：

```text
Pending -> Running
+
runtime_steps += StepId
```

这里的 `StepId` 只是唤醒引用。运行时从最新 committed `ExecutionPlan` 找到该 Step 对应的叶子，
从 `StepRecord.input_scope` 取得输入 Scope；不得从另一份冻结 payload 重新获得这些语义。

Builtin 不形成另一套状态机或 action 代数。运行时对三种 builtin 使用同一个 Step realization
入口：`Env` / `Umask` 的结果可由封闭语义确定，`Cd` 可以执行目录存在性、规范化或 workspace
相关的必要观察；随后都只把 typed completion 回送归约器，由归约器计算成功后的输出 Scope。
`Run` 则通过进程运行时落实 Pipeline。实现方式可以不同，但 Step 生命周期、提交边界和完成协议
不分叉。

成功的 `Env` / `Umask` completion 不携带替换 Scope，归约器从 command 和输入 Scope 计算
结果；`Cd` success 携带必要的已解析绝对目录观察，归约器只据此更新 cwd。completion 必须与
计划中的 Builtin variant 匹配，提供者不能回传任意 Scope。

Builtin realization 可以观察运行时环境，但不能修改守护进程自己的 ambient cwd/env/umask，
也不能留下独立的外部资源或物理 ownership。除 Step completion 及其产生的状态/Fact 外，它对
执行上下文的唯一语义输出是归约器产生的新 Scope。因此未提交 completion 的 Builtin 可以在
崩溃后从最新 committed 输入安全重试。需要留下外部资源或不可安全重放副作用的操作不应作为
Builtin 引入。

### <a id="term-cancellation"></a>取消 `Cancellation`

取消只保留两个正交维度：**来源**和**模式**。

```text
CancelMode       = Graceful + Force
StepCancelCause  = ExecutionRequested + AnySuccessSatisfied
```

执行级取消只有一个来源：`ExecutionRequested`。它要求不再启动新的工作，并按请求的
`CancelMode` 取消已有活动 Step。因此 `ExecutionSnapshot` 只需要 `Option<CancelMode>`，不再额外
维护 `User / Forced` 之类与模式重复的 reason enum；`Force` 是模式，不是另一种取消原因。

取消状态遵守：

- Pending Step 被 execution cancel 选中时可直接进入 `Cancelled(ExecutionRequested)`，因为没有
  需要清理的 realization；
- Pending 的 `AnySuccess` loser 进入 `Skipped(AnySuccessSatisfied)`；
- Running Step 被取消时进入 `Cancelling(cause, mode)`，并把该 `StepId` 加入运行时跟进集合；
- `Cancelling` 是非终态，只表示取消请求已提交；
- 若运行时随后报告正常成功，则进入 `Succeeded`；正常失败则进入 `Failed`；只有明确报告
  cancellation completion 才进入 `Cancelled(cause)`；
- runtime 若能证明该 Step 尚未产生需要清理的物理运行尝试，则观察到 `Cancelling` 后不得再
  开始 realization，并应直接回报 cancellation completion；Builtin 本身不拥有需要跨取消排空的
  物理进程，因此也遵循这一规则；
- `AnySuccessSatisfied` 对活动 loser 使用 `Force`；
- Step 的 cause 保留首次被归约器接受并提交的取消来源；后续来源不覆盖它。mode 则按
  `Graceful < Force` 单调取最大值，执行级取消模式也不得降级；
- 强化 mode 时再次标记同一个 `StepId` 需要运行时跟进；相同或更弱的请求不重复产生跟进；
- 终态不再接受生命周期变更；迟到的取消不能覆盖已提交的完成结果；
- 并发结果只按归约器接受并提交输入的顺序决定，不比较墙钟时间。

例如，先接受执行级 Graceful、再成为 AnySuccess loser，得到
`Cancelling(ExecutionRequested, Force)`；反过来则保留 `AnySuccessSatisfied` 来源。
来源的历史归属与当前终止强度互不混淆。

执行级 `ExecutionState` 也是派生值：

```text
ExecutionState = Pending + Running + Cancelling + Succeeded + Failed + Cancelled
```

只要 execution cancel 已请求且仍有活动 Step，对外就是 `Cancelling`。如果被请求取消的进程
最终自然成功，Execution 仍可最终 `Succeeded`。

`AnySuccess` 选择出 winner 后，只跳过尚未启动的 loser，并请求取消已经活动的 loser。整个
Parallel 必须等所有已启动 loser 进入终态后才能成为终态。由此保持结构化执行的核心不变量：

> terminal structured node 不拥有活动 child。

#### 组合节点与执行状态投影

组合节点的结果从计划与叶子递归计算，不另行持久化一套生命周期。全部叶子在启动前被排除的
子树投影为 `Skipped`，对应叶子标记相应的 SkipReason；已有终态不改写。

| Sequence 条件 | first 的哪些结果选择 then |
|---|---|
| `Success` | `Succeeded` |
| `Failure` | `Failed`，不含 `Cancelled` |
| `Always` | `Succeeded`、`Failed`、`Cancelled`，不含 `Skipped` |

first 未终结时不得启动 then；条件不满足时，then 的 Pending 叶子标为
`Skipped(ConditionNotMet)`，Sequence 沿用 first 的结果。条件满足时，`Success` / `Failure`
使用 then 的结果；`Always` 等待 then 终结，再按 `Failed > Cancelled > Succeeded` 聚合两侧
结果，不能因清理成功而抹掉原先的失败。then 没有正常输出时不贡献新 Scope，保留已执行路径
上的上下文；被取消叶子只能保留其输入上下文，不能产生成功输出。

执行级取消和祖先 AnySuccess 的 loser 排除优先于上述条件：它们阻止整个受影响子树启动新的
Step，包括 Failure/Always 后继。不能因某个活动 Step 在取消后自然成功而重新启动已排除的
后继；这不影响该 Step 本身如实记录成功。

实际推进的 then 至少有一个叶子进入过 `Running`，因而不会整体投影为 `Skipped`。
first 为 `Skipped` 时不选择 then；对于 `Always`，then 因祖先排除而整体为 `Skipped` 时
沿用 first 的结果，不参与上述三档聚合，也不得重新启动。

| Parallel join | 派生结果 |
|---|---|
| `All` | 等待全部分支终结；有失败则 Failed，否则有取消则 Cancelled，否则 Succeeded。 |
| `AnySuccess` | 出现成功分支后排空已启动 loser，再 Succeeded；loser 的失败或取消不覆盖成功。没有成功时等待全部分支；有失败则 Failed，否则 Cancelled。 |

winner 出现到 loser 排空之间，Parallel 仍非终态；它的父节点不能提前继续执行。
`ExecutionState` 先按根计划取终态结果；根仍非终态且执行级取消已请求、仍有活动 Step 时为
`Cancelling`；尚未推进、全部叶子为 Pending 时为 `Pending`，其余非终态为 `Running`。
仅 AnySuccess loser 的取消不会把整个 Execution 标为 `Cancelling`。正常归约不能留下
“执行级取消已提交，但仍有未处理的 Pending 叶子”的快照。

### <a id="term-fact"></a>事实 `Fact`

归约器可以产生候选 facts；**只有与对应 next snapshot 一起成功提交后，它们才成为可观察的
`Fact`**。Fact 描述已经发生的语义变化，不能把尚未完成的操作系统结果写成既成事实。例如
`Running -> Cancelling` 可以在提交后成为 Fact，但“已发出 SIGTERM”或“进程已经退出”只有在
相应运行时结果被接受后才能影响终态 Fact。

一次归约产生：

```text
current snapshot + input
        |
        v
      reducer
        |
        +--> next snapshot
        +--> facts
        `--> transition

transition.runtime_steps : Set<StepId>
transition.new_scopes    : Vec<Scope>
```

`runtime_steps` 只回答“哪些 Step 的 realization 需要重新检查”，不冻结“应该启动什么”或“应该
以什么模式取消”。`ExecutionPlan`、`StepRecord.input_scope` 和 `StepState` 才是这些语义的唯一
事实源。

因此 Core 不再需要通用 `RuntimeAction`，也不需要把 `ReadyStep(action, scope)`、
`CancelStep(cause, mode)` 作为独立持久化语义。未来若执行语义真的增加新的状态维度，应通过
新的 FP 修改 Step/Execution 模型，而不是先扩张一个开放 action 代数。

所有持久实现必须保证：

```text
计算下一状态
  -> 确保新 Scope 持久可读
  -> 原子提交 snapshot / facts / runtime follow-up
  -> 更新内存中的权威投影
  -> 发布已提交 facts
  -> 按 StepId 读取最新 committed snapshot 并收敛运行时状态
  -> 将真实结果作为下一次 reducer input
```

任何已提交 snapshot 或 Fact 引用的 Scope 都必须持久可读。Scope 与执行存储在同一事务
域时可以一起提交；使用独立内容寻址存储时，可以先幂等写入 Scope，再提交执行事务，不要求
跨存储 ACID。执行事务失败最多留下未引用 Scope，可由存储回收；回收不得删除已被引用或
仍受在途提交保护的 Scope。

Scope 预写不发布 Fact 或授予访问权限。执行事务提交之前不得修改权威内存投影、发布 Fact、
启动进程或发送取消信号；失败时不得暴露这次候选状态，也不得执行其运行时跟进。

### 运行时跟进与状态收敛

运行时消费一个已提交 `StepId` 时，必须重新读取该 Step 的**最新已提交状态、对应计划叶子和
输入 Scope**，而不是执行产生该唤醒时冻结的历史命令。Builtin 与 Run 都遵循同一规则：

| 最新 StepState | 运行时要求 |
|---|---|
| `Pending` | 不开始 realization。 |
| `Running` | 唯一持有并推进该 Step 当前 realization；Builtin 执行封闭 builtin 语义，Run 落实 Pipeline；不得盲目创建第二个物理运行尝试。 |
| `Cancelling(cause, Graceful)` | 不开始新的 realization；若不存在需要排空的物理运行尝试则直接回报 cancellation completion，否则请求 Graceful 终止并等待结果。 |
| `Cancelling(cause, Force)` | 不开始新的 realization；若不存在需要排空的物理运行尝试则直接回报 cancellation completion，否则请求 Force 终止并等待结果；旧 Graceful 唤醒必须服从最新 Force 状态。 |
| terminal state | 不开始 realization；只允许完成必要的 runtime ownership 清理。 |

这使**尚未开始处理的过期唤醒**天然安全：

```text
Running + runtime_steps={S}
        |
        | 尚未落实时收到取消
        v
Cancelling(Force) + runtime_steps={S}
```

worker 即使由较早的 `Running` transition 被唤醒，只要是在取消提交后才取得并处理该工作，也
必须读取最新 `Cancelling(Force)`，不得再开始 realization；如果还能证明尚未创建物理运行尝试，
就直接回报取消完成。如果 Run 已经在取消提交前跨过物理启动边界，则它是需要排空的活动 attempt，
不能再按“尚未开始”处理。Graceful -> Force 同样只服从最新 committed StepState。

`StepId` 本身不能表达运行时工作的**交付进度**。持久实现仍必须保存无法从 snapshot 推导出的
交付元数据，以防重复 worker、丢失唤醒或旧 worker 覆盖新状态。规范要求是：状态变化与待跟进
记录原子提交，重复交付不能创建第二个物理运行尝试，旧 worker 不能确认掉更新后的跟进。

以下 generation/claim 结构只是非规范实现示例，不限制存储布局或要求 Core 暴露这些字段：

```text
runtime_work = StepId × desired_generation × applied_generation × claim?
```

在这个示例中，归约器 transition 每次把 Step 加入 `runtime_steps`，持久层都在同一事务中推进
`desired_generation`。worker 只能把自己已处理的 generation 推进到 `applied_generation`；若
处理期间状态再次变化，新的 `desired_generation` 仍然大于 applied，不会被旧 worker 清掉。

generation、claim、worker 状态都属于实现层，不进入 Core ADT、Fact 或定义所有权。事务发件箱
仍可用于实现这个原子边界，但此时它更接近事务化工作队列：记录 `StepId` 与交付版本，而不是
复制 action、Scope、cause 或 mode。

### 崩溃恢复与物理所有权

状态收敛不会消除 `Run` 的不可幂等操作系统边界。例如：

```text
spawn 成功
  -> 守护进程在记录 process handle 前崩溃
```

此时新守护进程只看到 `StepState::Running`，无法据此判断旧进程是否已经存在。因此
`StepId + generation` 只能解决可靠唤醒，不能证明物理 attempt 的唯一性。

对可能留下物理运行尝试的 realization，恢复必须遵守：

> 若既不能证明旧运行尝试已经静止，也不能重新取得其唯一控制权，则不得继续 realization
> 或发布终态。

运行时可以通过独立进程监护器、可重新定位的稳定进程句柄或其他机制证明旧运行尝试已经静止，
或重新取得控制权。FP 不规定具体实现。若无法证明，则必须失败关闭：不重复启动、不启动依赖
工作，也不发布虚假的终态事实。接管只允许继续控制既有 attempt，不授权再次 spawn；静止
证明只允许报告已知结果或重启中断，不表示原操作从未执行，也不授权自动重放 Run。

Builtin 不留下这种物理 ownership，因此不受上述重复 spawn 限制；若其 completion 尚未提交，
恢复后可以从最新 committed Step 与 Scope 重新执行 realization。Core 也可以提供“把活动 Run
解释为重启中断失败”的纯状态转换，但守护进程只有在旧运行尝试已经静止后才能调用它。

### 表面语言

文本 DSL 在 `cue-language` 中编译为 Core ADT。Core 不保存 token、mode 或语法糖。
环境前缀：

```text
assignment = [A-Za-z_][A-Za-z0-9_]* "=" value
process    = assignment* command-word argument*
```

只连续识别 command 前的 assignment，并在第一个 `=` 处分隔 key/value；value 可以为空或
包含 `=`，不做 `$VAR`、命令替换或其他 shell 展开。同一 key 重复时最后一项生效。

`A=B command` 只产生该 Process 的 `EnvPatch`，不影响 pipeline 的其他进程，也不改变后续
Sequence 的 Scope。command 后的 `A=B` 是普通 argv；只有 assignment 而没有 command 的
输入非法；assignment 不能修饰 Cue builtin。

需要跨 Step 修改环境时必须显式使用 `Env` builtin，例如：

```text
env set A=left -> printenv A
```

### <a id="term-sensitivity"></a>敏感性 `Sensitivity`

`Sensitivity` 是环境值的显式敏感性分类：`Normal` 表示未声明敏感，`Sensitive` 表示声明敏感；
`Normal` 不是“已证明不含秘密”。ADT 记法与环境数据类型集中定义于 [Scope](#term-scope)，
本节定义分类的语义与处理边界。

该分类不在 Core 中引入第二套 Execution 或持久化状态。变量名猜测可以
辅助自动标注或警告，但不能替代显式分类，也不能成为安全边界。

该分类使实现能够提供 Sensitive Execution，例如采用易失存储或不透明凭证引用。**本 FP 不规定
Sensitive Execution 的持久化、恢复、重连或崩溃处理协议，也不要求第一阶段实现必须提供这些
能力。** 不支持 Sensitive Execution 的实现应显式拒绝，而不能静默把 `Sensitive` 降级为
`Normal`。具体保护策略可以由后续 FP 单独定义。

### <a id="term-composition"></a>组合 `Composition`

`Composition` 是 Cue 的开放实现图：提供者在守护进程启动时声明能力、依赖和顺序，经校验后
绑定为带类型的运行时端口。它负责**如何实现** ExecutionPlan，而不能扩展或改写
ExecutionPlan 的语义。

运行时 follow-up 的**语义引用**只有 `StepId`；generation / claim 等只属于交付元数据，不是
另一份执行语义。worker 从最新 committed 状态解析该 Step 的输入：

```text
Builtin : StepId × BuiltinCommand × Scope
Run     : StepId × Pipeline × IoMode × Scope
```

这只是同一个 Step realization 在两种封闭计划叶子上的输入形状，不是第二份 action 状态或持久
命令。Builtin 和 Run 的生命周期、交付与取消协议保持一致；Composition 可以替换实现机制，但
不能改变 `BuiltinCommand` / Pipeline / IoMode / Scope 的逻辑含义。

提供者收到的语义输入必须保持只读。工作目录材料、包装器、资源句柄、沙箱、秘密注入等属于物理
实现上下文，可以被构造或调整；但 StepId、BuiltinCommand、逻辑 argv/EnvPatch、IoMode 和
逻辑 Scope 不能在归约器决定之后被静默改写。

Composition 的排序属于端口贡献关系，而不是提供者全局关系；一个提供者参与多个端口时，不能
因为某个端口的排序声明要求目标也贡献其他无关端口。

### <a id="term-operation-id"></a>操作标识 `OperationId`

IPC v4 使用严格长度前缀消息和封闭 schema。修改状态的 Command 必须携带稳定
`ClientId + OperationId`，它表示一个**逻辑命令身份**；`RequestId` 只负责单条连接上的请求
响应关联。

客户端自动断线重试同一个 Command 时必须复用原 `ClientId + OperationId`，可以重新分配
`RequestId`。因此“服务端支持 OperationId 去重”还不够，官方客户端也必须把这个身份保留到
重连之后。

Query 是端到端只读操作：`list/show/wait/output/help` 等查询不得为了获得 ScopeHash 而先执行
`PutScope`。前端应先解析用户意图，只有真正提交 Execution 时才执行：

```text
PutScope -> compile with ScopeHash -> SubmitExecution
```

输出协议使用绝对 offset；surface/client 的 `tail N` 必须返回最后 N bytes，不能翻译成
`offset = 0, max_bytes = N`。

lifecycle command 的“ack 先于 draining”必须由真实 happens-before 保证：先提交 command
outcome，由连接写端写出并 flush ack，再通知 host 停止 accept 和 drain。固定 sleep 不能替代
这个顺序。

## 兼容性

这是有意的直接切断兼容。IPC v3 请求、actor 状态与数据库不翻译成 v4；默认启动时，旧
`cued.db` 及 SQLite sidecar 只归档为只读文件，不导入不兼容语义。v4 使用独立数据库，旧
客户端不能连接 v4 daemon，删除的 session/schedule/resource/target 命令没有 kernel 兼容
入口。

回滚边界是停止 v4 daemon、恢复旧二进制，并从只读 archive 的副本显式恢复旧数据；v3 与
v4 不得同时拥有同一 socket。外部 policy owner 的迁移方式是把既有工作流编译为
`ExecutionSpec`，通过 IPC v4 观察 Execution/Step/Fact，而不是要求 daemon 恢复旧状态机。

## 验证

- 定义所有权检查验证 `defines` 与 `term-*` 锚点对应，参考实现术语不取得定义所有权；
  基础数据类型只有一个 ADT 定义位置，Scope 与 Sensitivity 的交叉引用可解析；
- Core serialization 测试覆盖四种 plan variant，以及空 executable、含 NUL 字符串、非法路径、
  非法 EnvKey、越界 umask、空 EnvMutation、非法 pipeline 和不足两个分支的 Parallel 拒绝；
  普通空参数与空环境值合法，同一 EnvKey 不会同时具有 Set/Unset；
- reducer 测试证明 Builtin/Run ready 决策与 `Pending -> Running`、`runtime_steps += StepId`
  属于同一个 transition，并覆盖跨 Execution 的 StepId 区分、前序遍历稳定性、Scope 字段约束、
  Sequence 条件与 Scope threading、Parallel fork/no-merge，以及非法 snapshot restore 拒绝；
- cancellation 测试覆盖 `Running -> Cancelling -> terminal`、未开始 realization 的直接取消
  完成、正常完成与取消竞争、两种来源的先后顺序、mode 不降级与重复请求幂等、AnySuccess
  Force loser draining；组合测试覆盖 Always 不吞失败、loser 失败不覆盖 winner、无 winner 的
  失败聚合，以及 execution cancel/祖先 loser 排除后不再启动后继；
- 持久化测试证明 snapshot/facts/runtime follow-up 原子记录，以及引用 Scope 先持久可读；
  分别覆盖 Scope 写入失败、预写后执行事务失败、崩溃恢复和回收与在途提交竞争，确保无悬空引用、
  不发布候选 Fact、不修改 live state、不执行未提交的外部动作；
- 运行时收敛测试证明过期 Running 唤醒在最新状态为 Cancelling/terminal 时不会开始 realization，
  Graceful 旧唤醒服从最新 Force 状态；
- 交付测试证明同一 Step 在 worker 执行期间再次变化时不会丢失后续跟进，旧 worker 不能确认
  更新后的工作，重复交付不产生重复物理运行尝试；
- 崩溃恢复测试证明 Builtin 可安全重放，同时无法确认旧 Run attempt 静止或重新取得控制权时不会
  重复 spawn，也不会发布虚假终态；
- Language 测试覆盖三个 builtin、process-local `A=B`、assignment-only 拒绝、pipe link 和
  per-Run captured/PTY；
- Runtime 测试验证 Builtin/Run 使用同一 Step realization contract，并覆盖 pipeline wiring、
  captured output、PTY、control/cancel，以及 provider 无法修改 semantic Step 输入；
- Core/protocol 测试证明 `Sensitivity` 分类可稳定往返；Sensitive Execution 的持久化与恢复能力
  不属于本 FP 的必选验证项；
- Protocol/store/client 测试覆盖 strict framing、OperationId 重放、query 不触发 `PutScope`
  与 `tail N` suffix；
- daemon smoke 覆盖 Unix socket lifecycle、ack flush-before-drain、restart successor 与旧
  database archive；
- 架构检查拒绝恢复 IPC v3 模块、daemon 内 surface parser，以及 Core 反向依赖 runtime、
  transport、storage 或 frontend。
