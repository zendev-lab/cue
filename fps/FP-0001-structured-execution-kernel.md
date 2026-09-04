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
定义执行语义，用不可变 `Scope` 表达执行上下文，用纯归约器决定状态变化；运行时只负责把
已经提交的 Step 状态落实为真实进程状态。守护进程只提供严格 IPC v4，不在内核中拥有会话、
调度、重试、资源、审批或远程目标策略。

本提案坚持两个边界：**决定与事实分离，语义与交付分离**。Core 只提交“某个 Step 现在应该
处于什么状态”，并用 `StepId` 标记哪些 Step 需要运行时重新跟进；运行时每次都读取最新已提交
快照，把现实状态向该语义状态收敛。`StepId` 工作项不复制 action、输入 Scope 或取消模式。
只有运行时返回的实际结果才能证明进程成功、失败或被取消。

状态、事实、新 Scope 和运行时跟进必须先原子提交，外部动作只能发生在提交之后。

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

### <a id="term-scope"></a>执行范围 `Scope`

`Scope` 是 `cwd × env × umask` 的完整不可变快照，由内容哈希标识：

```text
Scope = AbsolutePath × Env × FileModeMask
ExecutionSpec = ScopeHash × ExecutionPlan
```

Scope 不含隐式 parent、delta 或 ambient environment。Scope 的流向只由计划结构决定：

| Plan | 子项输入 Scope | Plan 输出 Scope |
|---|---|---|
| `Builtin(command)` | 输入 `S` | 成功为 `apply(command, S)`；失败为 `S` |
| `Run(pipeline, io)` | 输入 `S` | 始终为 `S` |
| `Sequence(first, then, when)` | `first` 得到 `S`；被选择的 `then` 得到 `first` 输出 | 实际执行路径的最终 Scope |
| `Parallel(branches, join)` | 每个分支都得到同一个 `S` | `S`；分支 Scope 不隐式合并 |

因此 `cd`、`env`、`umask` 的影响只能通过 Sequence 显式传播；Parallel 永远从同一个输入分叉，
不会偷偷合并分支状态。

### <a id="term-step"></a>步骤 `Step`

`Step` 是 `Builtin` 或 `Run` 叶子。组合节点不是 Step。每个叶子在 Execution 创建时按稳定顺序
获得一个 `StepId`，并持久记录输入、输出 Scope 哈希和状态：

```text
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
```

`Running` 的精确定义是：**该 Step 已经被归约器接纳为活动工作，并且同一次已提交状态转换已经
把该 `StepId` 标记为需要运行时跟进。** 它不要求操作系统进程已经完成 `spawn`。

因此 ready 决策不能拆成“先返回完整启动命令、再由调用者另行 `mark_running()`”两个阶段。
归约器一旦决定 Step ready，就必须在同一个 transition 中完成：

```text
Pending -> Running
+
runtime_steps += StepId
```

这里的 `StepId` 只是唤醒引用。Step 的 action 来自不可变 `ExecutionPlan`，输入 Scope 来自已
提交 `StepRecord.input_scope`；运行时不得从另一份冻结 payload 重新获得这些语义。

### <a id="term-cancellation"></a>取消 `Cancellation`

取消只保留两个正交维度：**来源**和**模式**。

```text
CancelMode       = Graceful + Force
StepCancelCause  = ExecutionRequested + AnySuccessSatisfied
```

执行级取消只有一个语义：“不要再启动新的工作”。因此 `ExecutionSnapshot` 只需要
`Option<CancelMode>`，不再额外维护 `User / Forced` 之类与模式重复的 reason enum。
`Force` 是取消模式，不是另一种取消原因。

取消状态遵守：

- Pending Step 被 execution cancel 选中时可直接进入 `Cancelled(ExecutionRequested)`，因为没有
  需要清理的运行尝试；
- Pending 的 `AnySuccess` loser 进入 `Skipped(AnySuccessSatisfied)`；
- Running Step 被取消时进入 `Cancelling(cause, mode)`，并把该 `StepId` 加入运行时跟进集合；
- `Cancelling` 是非终态，只表示取消请求已提交；
- 若运行时随后报告正常成功，则进入 `Succeeded`；正常失败则进入 `Failed`；只有明确报告
  cancellation completion 才进入 `Cancelled(cause)`；
- `Force` 可以强化先前的 `Graceful`，强化时再次标记同一个 `StepId` 需要运行时跟进；
- 同等级重复请求幂等；
- 并发结果只按归约器接受并提交输入的顺序决定，不比较墙钟时间。

执行级 `ExecutionState` 也是派生值：

```text
ExecutionState = Pending + Running + Cancelling + Succeeded + Failed + Cancelled
```

只要 execution cancel 已请求且仍有活动 Step，对外就是 `Cancelling`。如果被请求取消的进程
最终自然成功，Execution 仍可最终 `Succeeded`。

`AnySuccess` 选择出 winner 后，只跳过尚未启动的 loser，并请求取消已经活动的 loser。整个
Parallel 必须等所有已启动 loser 进入终态后才能成为终态。由此保持结构化执行的核心不变量：

> terminal structured node 不拥有活动 child。

### <a id="term-fact"></a>事实 `Fact`

`Fact` 是**已经由归约器接受并成功提交**的可观察状态变化。它不能描述尚未完成的操作系统结果。
例如 `Running -> Cancelling` 可以成为事实，但“已发出 SIGTERM”或“进程已经退出”只有在相应
运行时结果被接受后才能影响终态事实。

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

`runtime_steps` 只回答“哪些 Step 的物理实现需要重新检查”，不冻结“应该启动什么”或“应该以
什么模式取消”。`ExecutionPlan`、`StepRecord.input_scope` 和 `StepState` 才是这些语义的唯一
事实源。

因此 Core 不再需要通用 `RuntimeAction`，也不需要把 `ReadyStep(action, scope)`、
`CancelStep(cause, mode)` 作为独立持久化语义。未来若执行语义真的增加新的状态维度，应通过
新的 FP 修改 Step/Execution 模型，而不是先扩张一个开放 action 代数。

所有持久实现必须保证：

```text
计算下一状态
  -> 原子提交 snapshot / facts / new scopes / runtime follow-up
  -> 更新内存中的权威投影
  -> 发布已提交 facts
  -> 按 StepId 读取最新 committed snapshot 并收敛运行时状态
  -> 将真实结果作为下一次 reducer input
```

因此提交之前不得修改权威内存投影、发布 fact、启动进程或发送取消信号。提交失败时，本次
transition 对外完全不可见。

### 运行时跟进与状态收敛

运行时消费一个已提交 `StepId` 时，必须重新读取该 Step 的**最新已提交状态**，而不是执行
产生该唤醒时冻结的历史命令：

| 最新 StepState | 运行时应满足的物理状态 |
|---|---|
| `Pending` | 不创建新的物理实现。 |
| `Running` | 确保该 Step 的当前运行尝试被唯一持有并继续实现；不得盲目创建第二个尝试。 |
| `Cancelling(cause, Graceful)` | 不再创建新尝试；若已有活动尝试则请求 Graceful 终止并等待结果；若尚未开始则直接收敛为“不运行”。 |
| `Cancelling(cause, Force)` | 不再创建新尝试；若已有活动尝试则请求 Force 终止并等待结果；旧 Graceful 唤醒必须服从最新 Force 状态。 |
| terminal state | 不得创建新尝试；只允许完成必要的运行时 ownership 清理。 |

这使以下过期工作天然安全：

```text
Running + runtime_steps={S}
        |
        | 尚未落实时收到取消
        v
Cancelling(Force) + runtime_steps={S}
```

worker 最终即使由较早的 `Running` transition 被唤醒，也必须读取最新 `Cancelling(Force)`，因此
不会先启动一个已经不再需要的进程再去取消它。同理，Graceful -> Force 不依赖两条取消命令的
消费顺序，只依赖最新 committed StepState。

`StepId` 本身不能表达运行时工作的**交付进度**。持久实现仍必须保存无法从 snapshot 推导出的
交付元数据，以防重复 worker、丢失唤醒或旧 worker 覆盖新状态。推荐实现是每个 Step 使用单调
递增的 generation：

```text
runtime_work = StepId × desired_generation × applied_generation × claim?
```

归约器 transition 每次把 Step 加入 `runtime_steps`，持久层都在同一事务中推进
`desired_generation`。worker 只能把自己已处理的 generation 推进到 `applied_generation`；若
处理期间状态再次变化，新的 `desired_generation` 仍然大于 applied，不会被旧 worker 清掉。

generation、claim、worker 状态都属于实现层，不进入 Core ADT、Fact 或定义所有权。事务发件箱
仍可用于实现这个原子边界，但此时它更接近事务化工作队列：记录 `StepId` 与交付版本，而不是
复制 action、Scope、cause 或 mode。

### 崩溃恢复与物理所有权

状态收敛不会消除不可幂等的操作系统边界。例如：

```text
spawn 成功
  -> 守护进程在记录 process handle 前崩溃
```

此时新守护进程只看到 `StepState::Running`，无法据此判断旧进程是否已经存在。因此
`StepId + generation` 只能解决可靠唤醒，不能证明物理 attempt 的唯一性。

恢复必须遵守：

> 在无法证明旧运行尝试已经静止，或无法重新取得其唯一控制权时，既不能再次启动同一个 Step，
> 也不能把它提前宣布为终态。

运行时可以通过独立进程监护器、可重新定位的稳定进程句柄或其他机制证明旧运行尝试已经静止，
或重新取得控制权。FP 不规定具体实现。若无法证明，则必须失败关闭：不重复启动、不启动依赖
工作，也不发布虚假的终态事实。

Core 可以提供“把活动 Step 解释为重启中断失败”的纯状态转换，但守护进程只有在上述静止条件
已经成立后才能调用它。

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

环境值显式携带敏感性：

```text
Sensitivity = Normal + Sensitive
EnvValue    = NulFreeString × Sensitivity
Env         = Map<EnvKey, EnvValue>
EnvPatch    = Map<EnvKey, EnvEdit>
EnvEdit     = Set(EnvValue) + Unset
```

变量名猜测只能辅助自动标注或警告，不能成为安全边界。初始 Scope、Process EnvPatch 或 Env
builtin mutation 的任何位置出现 `Sensitive`，该 Execution 从提交开始就是易失执行：Scope、
投影、事实、操作结果以及运行时跟进记录都只能存在于内存，并且生命周期中不能从易失升级为
持久。

“易失执行”因此是 `Sensitivity` 的派生性质，不再作为独立领域概念。

长期更推荐凭证提供者在运行时根据不透明引用注入秘密字节，使秘密值本身不进入语义计划、Scope
或事实。

### <a id="term-composition"></a>组合 `Composition`

`Composition` 是 Cue 的开放实现图：提供者在守护进程启动时声明能力、依赖和顺序，经校验后
绑定为带类型的运行时端口。它负责**如何实现** ExecutionPlan，而不能扩展或改写
ExecutionPlan 的语义。

提供者收到的下列语义输入必须保持只读：

```text
StepId × Pipeline × IoMode × Scope
```

这些语义输入从最新已提交 Execution/Step 状态解析；运行时工作项本身不携带另一份可变副本。
工作目录材料、包装器、资源句柄、沙箱、秘密注入等属于物理实现上下文，可以被构造或调整；但
StepId、逻辑 argv/EnvPatch、IoMode 和逻辑 Scope 不能在归约器决定之后被静默改写。

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

- 定义所有权检查证明 9 个 Cue 稳定概念与 `term-*` 小标题一一对应，参考实现术语不取得
  定义所有权；
- Core serialization 测试证明 `ExecutionPlan` 只有四个 variant，非法 pipeline、空并行和
  冲突 env edit 无法构造或反序列化；
- reducer 测试证明 ready 决策与 `Pending -> Running`、`runtime_steps += StepId` 属于同一个
  transition，并覆盖稳定 StepId、Sequence Scope threading、Parallel fork/no-merge；
- cancellation 测试覆盖 `Running -> Cancelling -> terminal`、正常完成与取消竞争、
  Graceful -> Force 强化、AnySuccess loser draining，并证明取消来源与模式互不重复编码；
- 持久化测试证明 snapshot/facts/new scopes/runtime follow-up 原子记录，commit failure 不修改
  live state且不执行外部动作；
- 运行时收敛测试证明过期 Running 唤醒在最新状态为 Cancelling/terminal 时不会启动进程，
  Graceful 旧唤醒服从最新 Force 状态；
- 交付测试证明同一 Step 在 worker 执行期间再次变化时不会丢失新一代 follow-up，旧 worker
  不能把更新后的 generation 标记为已完成；
- 崩溃恢复测试证明无法确认旧运行尝试静止或重新取得控制权时不会重复 spawn，也不会发布虚假
  终态；
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
