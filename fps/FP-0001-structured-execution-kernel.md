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

Core 的 reducer 只决定状态、事实与待实现的 effect intent；daemon 必须先把新的
projection、facts 与 effect outbox 原子提交，再允许 runtime realization。外部副作用的
“请求发生”与“结果已经发生”是两个不同事实，尤其 cancellation 不能在发出 signal 前
提前写成 terminal `Cancelled`。

## 动机

旧模型同时承载 surface DSL、进程启动、session 生命周期、调度、资源分配、远程
transport 和 UI 状态，导致同一执行在 Core、daemon actor、client 与持久化层具有
不同表示。环境变量是当前命令局部覆盖还是沿 pipeline 继承、pipe 属性属于哪一端、
`cd` 是否改变后续命令、PTY 属于 process 还是 workflow 等问题都缺少唯一答案。

公开执行语义必须足够小，才能由纯 reducer 决定状态变化并稳定持久化；实现机制必须
保持开放，才能替换 process spawner、scope store、output store 等 provider。二者
需要明确分层，而不是用兼容 variant、service locator 或双协议继续扩大 Core。

持久化事实还必须与不可事务化的 OS 副作用划清边界。`spawn`、signal、PTY control
不能和 SQLite commit 组成一个 ACID transaction，因此不能先把 effect 的预期结果写成
事实再“尽力实现”。Cue 使用 durable effect outbox 保存已经提交的 realization intent，
runtime completion 再作为新的 typed input 回到 reducer；这样 durable projection 永远只
描述已经由 reducer 接受的事实，而不把未完成的副作用伪装成事实。

## 设计

### Kernel ADT

Core 的完整值结构是以下乘积类型与和类型。`×` 表示所有字段必须同时存在，`+` 表示
只能选择一个 variant：

```text
ValueClass      = Persistable + Sensitive
EnvValue        = NulFreeString × ValueClass
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
- `Sensitive` 是显式数据分类，不由 store 根据变量名猜测。变量名 heuristic 可以作为
  frontend 警告或自动标注辅助，但不能成为“不落盘”的安全边界。

Scope 流向由 plan variant 唯一决定：

| Plan | 子项输入 Scope | Plan 输出 Scope |
|---|---|---|
| `Builtin(command)` | 输入 `S` | 成功时为 `apply(command, S)`；失败时为 `S` |
| `Run(pipeline, io)` | 输入 `S` | 始终为 `S` |
| `Sequence(first, then, when)` | `first` 得到 `S`；被选择的 `then` 得到 `first` 的输出 | 实际执行路径的最终 Scope |
| `Parallel(branches, join)` | 每个 branch 都得到同一个 `S` | `S`；分支 Scope 永不隐式合并 |

`SequenceCondition::Success`、`Failure`、`Always` 分别在 first 成功、失败、任意非
`Skipped` 终态后选择 then；未选择的 leaf 标记 `Skipped`。`ParallelJoin::All` 等待所有分支，
`AnySuccess` 在首个成功分支出现后请求取消或跳过其余分支，但两种 join 都不合并 Scope。
`AnySuccess` 的逻辑 winner 可以先确定；整个 Parallel 只有在仍运行或取消中的 loser 都
进入 terminal state 后才成为 terminal，structured scope 不允许留下仍归属于该执行的
orphan work。

### Reducer ADT

Plan 是静态结构；运行中的唯一持久状态由下列 ADT 表示：

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

StepAction       = Builtin(BuiltinCommand)
                 + Run(Pipeline × IoMode)

EffectIntent     = RealizeStep(ReadyStep)
                 + CancelStep(StepId × StepCancelReason × CancelMode)

ExecutionTransition = NextExecutionSnapshot
                    × Vec<FactDraft>
                    × Vec<NewScope>
                    × Vec<EffectIntent>
```

每个 `Builtin` 与 `Run` leaf 按 plan preorder 获得稳定 `StepId`；组合节点本身不是 Step。
`input_scope` 只在 reducer 把 leaf 推进为 active realization 时写入；完成的
`Builtin`/`Run` 才有 `output_scope`，`Skipped`/`Cancelled` 保持为空。

`Running` 表示该 Step 已拥有一个已提交的 realization attempt；对应 OS process 可能仍在
launch handshake 内。`Cancelling` 表示 reducer 已接受 cancellation intent，但 runtime
尚未报告 terminal completion。`Cancelled` 只表示 runtime 已确认该 attempt 因 cancellation
结束，或一个尚未 realization 的 Pending step 被取消而无需执行外部副作用。

取消遵循以下规则：

- Pending step 被 execution cancel 选中时可以直接进入 `Cancelled`；它没有需要清理的
  realization。被条件或 `AnySuccess` 选出的 Pending loser 进入 `Skipped`。
- Running step 收到用户 cancel 或 `AnySuccess` loser 决策时进入 `Cancelling`，并产生
  `CancelStep` effect；不得提前进入 terminal `Cancelled`。
- `Cancelling` 后 runtime 若报告正常成功，则进入 `Succeeded`；若报告正常失败，则进入
  `Failed`；只有 runtime 报告 cancellation completion 时才进入 `Cancelled`。Cue 的
  cancellation 是 best-effort intent，不要求 signal 一定抢赢进程的自然完成。
- 同一状态下重复的同级 cancellation intent 是幂等的；`Force` 可以强化此前的
  `Graceful` cancellation，并产生新的强化 effect。
- race 只由 reducer 接受输入的顺序决定，不按墙钟时间猜测。若 terminal completion
  先被 commit，之后的 cancel 是 no-op；若 cancel intent 先被 commit，则先进入
  `Cancelling`，随后 reducer 接受的第一个 terminal completion 决定最终
  `Succeeded`/`Failed`/`Cancelled`，后到的 stale completion 被拒绝或忽略。

用户级 `ExecutionCancelRequest` 是“不要再启动新的工作”的持久 intent，不等价于 terminal
`ExecutionState::Cancelled`。cancel 时 Pending work 被终止或选出，Running work 进入
`Cancelling`；只要还有 active realization，Execution 对外显示 `Cancelling`。最终状态仍由
实际 terminal Step 结果和 plan 代数决定，因此一个只有单个 Running step 的 Execution 在
cancel signal 发出后仍自然成功时可以最终 `Succeeded`。

reducer 读取 Snapshot 和 typed input，纯计算出新的 Snapshot、facts、Scope 与
`EffectIntent`；runtime 只能实现 effect 并回报 typed completion，不能自行决定分支、
跳过规则、取消理由或 Scope 流向。

### Durable effect outbox

`spawn`、signal、PTY control 等 OS effect 无法和 SQLite projection 组成一个真正的 ACID
transaction。Cue 因此采用 transactional outbox：**先持久化 intent，再 realization；
realization 的结果永远通过下一次 reducer transition 成为事实。**

一次 durable transition 的顺序固定为：

```text
input event / command
        |
        v
pure reducer on an immutable snapshot
        |
        +--> next snapshot
        +--> facts
        +--> new scopes
        `--> effect intents
                 |
                 v
single store transaction
  projection + facts + scopes + outbox
  (+ OperationId claim/outcome when applicable)
                 |
          commit succeeds
                 |
        +--------+--------+
        |                 |
        v                 v
publish facts       dispatch outbox effects
                          |
                          v
                   runtime completion
                          |
                          `----> next reducer input
```

必须满足以下事务不变量：

- reducer 在 immutable/clone snapshot 上计算。store commit 成功之前，不替换 daemon 的
  live projection，不发布 facts，也不调用 runtime；commit 失败时 live state 保持原样。
- 新 Scope 必须在任何引用其 hash 的 durable projection 同一个 transaction 中先变得可用，
  或者整个 Execution 从提交开始就是 volatile；不能出现 durable snapshot 引用只存在于
  memory 的 Scope。
- command 导致状态变化时，`OperationId` claim、response outcome、projection、facts 与
  outbox intent 属于同一个 store transaction。
- outbox effect 只表示“应该 realization 什么”，effect 被 dispatch/ack 不会直接把 Step
  改成成功、失败或取消；只有 typed runtime completion 能驱动 terminal reducer state。

store 为每条 committed effect 分配稳定 `EffectId` 并保存 delivery 状态。dispatcher 在调用
不可事务化副作用前，必须先把 `Pending` effect 原子 claim 为当前 daemon instance 的
`Dispatched`；同一 Step 的 effects 按 commit 顺序消费。

进程 effect 不宣称跨 daemon crash 的 exactly-once。恢复规则显式区分：

- 尚未 claim 的 `Pending` effect 从未被允许执行，可以由 successor 继续消费；
- 已由死亡 daemon claim、但没有 terminal completion 的 `Dispatched` process effect 处于
  outcome unknown，successor 不盲目重复 spawn/signal，而把对应 `Running`/`Cancelling`
  attempt 按结构化 restart interruption 归并为 infrastructure failure，并把旧 effect
  标记为 abandoned；
- 未来若某类 effect 本身具有外部幂等 key，可以单独声明 replay-safe policy；不能把这一
  假设泛化到本地 process spawn。

live daemon 为每个 active Step 建立 `RunSlot`，并在真正调用 spawner 前成为 cancel 的
序列化点。`RunSlot` 至少能表示 launching/active/finished 与 pending cancel intent。
因此 `Running -> Cancelling` 发生在 process control 尚未建立的窗口时，cancel 不会丢失：
若尚未 spawn 可以抑制 realization；若 spawn 正在进行，则 control 建立后立即应用已记录的
cancel intent。Runtime 不得用“当前 HashMap 里暂时没有 RunControl”解释为取消成功。

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

环境值的 sensitivity 必须随值进入 typed ADT，而不是在 store 层根据 EnvKey 反推。
process-local `A=B` 与 `env set A=B` 和初始 Scope 使用同一种 `EnvValue` 分类。若 surface
没有显式 secret syntax，frontend 可以提供 policy/config 决定分类；任何 heuristic 只能
用于保守自动标注或警告，不能覆盖显式 `Sensitive`。

### Durability 与敏感数据

`ExecutionDurability = Durable + Volatile` 在 submission 时由完整输入确定：初始 Scope 或
ExecutionPlan 中任意 `Sensitive` EnvValue（包括 Process EnvPatch 与 Env builtin mutation）
都会使整个 Execution 为 `Volatile`。该分类在 Execution 生命周期内单调不升级：volatile
Execution 后续产生的 Scope、projection、facts、operation response 与 outbox payload 都只
进入 memory store，即使某个后续 Scope 已不再携带 sensitive value。

这样 `env set SECRET=... -> command` 不会先创建 durable execution 再在中途产生一个
SQLite 无法引用的 volatile Scope；`SECRET=... command` 也不会因为 secret 位于
Process EnvPatch 而被遗漏。durable Execution 的 reducer 产生的新 Scope 只能包含已经在
submission 时被证明 persistable 的环境数据。

真正的 credential provider 更推荐使用 opaque `SecretRef`：ref 可以进入 semantic plan，
secret bytes 只在 realization overlay 中解析并注入，不进入 Scope、ExecutionSpec、facts、
outbox 或 output metadata。`Sensitive EnvValue` 是需要直接传递原始环境值时的安全后备，
不是秘密管理系统。

### Composition 与运行时

Execution ADT 是 closed semantics；Composition 是 open implementation graph。provider
在 daemon bootstrap 时声明 capability、依赖和顺序，经校验后一次性绑定为 typed
runtime ports。运行中不得按字符串查找服务，也不得让 extension 注入新的 Core
variant。

runtime 只实现 reducer 产生的 effect：应用 builtin、启动 captured pipeline 或 PTY
process group、写入绝对 offset output、传播 control/cancel，以及把结果回送 reducer。
policy owner 可以生成和提交 `ExecutionSpec`，但不能接管已有 Execution 的状态转换。

open realization 不等于 provider 可以重写 semantic request。Runtime boundary 分成只读的
semantic input 与可变的 realization overlay：

```text
SemanticSpawn     = StepId × Pipeline × IoMode × Scope
RealizationOverlay = PhysicalWorkspace
                   × ResolvedExecutable
                   × WrapperChain
                   × ResourceHandles
                   × SandboxHandles
                   × RuntimeEnvInjection
                   × ...
```

workspace/transform provider 可以构造或修改 `RealizationOverlay`，guard 可以读取两者，但
provider 不能修改 `StepId`、logical Pipeline/argv/EnvPatch、`IoMode` 或 logical Scope。
需要 wrapper、workspace 映射、资源句柄或 secret injection 时，它们作为 realization
mechanism 可观察地附加，而不是悄悄改写 Core 已持久化的 execution meaning。

### IPC、client 与 daemon

IPC v4 使用严格长度前缀消息和封闭 schema。修改状态的 Command 必须携带稳定
`OperationId` 以支持幂等重放；Query 只读。协议暴露 Scope、Execution/Step projection、
facts、绝对 output ranges、cancel/control 和显式 PTY attachment，不暴露 session、
schedule 或 resource request。

`RequestId` 是 connection-local correlation；`ClientId + OperationId` 是 logical command
identity。自动 reconnect/retry 同一 Command 时，client 必须复用原 `ClientId` 与
`OperationId`，可以分配新的 `RequestId`。官方 client 必须允许 caller 保留/重放 operation
identity；“每次 connect 生成新 identity”不能被称为 end-to-end reconnect idempotency。
client process 自身崩溃后若没有持久保存 operation identity，则不能声称自动 exactly-once。

Query 的 read-only 是端到端属性：解析 `list/show/wait/output/help` 等命令不得为了获得
ScopeHash 先执行 `PutScope`。frontend 应先解析 intent，只有真正提交 Execution 的路径才做
`PutScope -> compile with ScopeHash -> SubmitExecution`。

protocol 的 output range 使用绝对 offset；surface/client 的 `tail N` 必须返回当前 retained
output 的最后 N bytes，而不是把 `N` 翻译成 `offset = 0, max_bytes = N`。实现可以先查询
retained range 或使用 typed tail query，但语义必须保持 suffix。

daemon 持久化 Durable Execution 的 Scope、projection、facts、operation outcome 与 effect
outbox；Volatile Execution 的这些对象全部留在 memory。重启恢复时，死亡 instance 已
claim 的 active process effects按前述 outbox recovery 规则成为结构化 interruption，再由
同一个 reducer 决定 Failure/Always 等后续语义。

本地 host 使用单一 Unix socket 和独立 IPC v4 数据库。lifecycle command 的“ack 先于
Draining”是 flush ordering，不是 timing assumption：daemon 必须先 durable commit command
outcome，connection writer 写出并 flush `RestartAccepted`/shutdown ack，随后才发布 deferred
lifecycle intent 让 host 停止 accept 和 drain。固定 sleep 不能替代该 happens-before。
restart 只在旧 listener 排空并释放 socket/instance lock 后启动 successor。

### Commit、publish 与 observation 顺序

对于所有 reducer transition，权威顺序是：

```text
compute next state
    -> atomic store commit
    -> swap live projection
    -> publish durable facts
    -> dispatch committed effects
```

store error 必须使本次 transition 对 live state、fact subscribers 与 runtime 完全不可见；
不得先 mutate `state.execution` 再在 commit error 后继续持有该内存状态。observer 看到的
FactEvent 必须来自已经 commit 的 projection，而不是 speculative transition。

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
  `All`/`AnySuccess`、`Running -> Cancelling -> terminal`、cancel/completion race、
  cancellation escalation、snapshot restore 与 restart interruption。
- effect/outbox 测试证明 projection/facts/scopes/effects 原子提交；commit failure 不修改 live
  state且不 dispatch；Pending effect 可恢复消费，dead-instance Dispatched process effect
  不会盲目 replay，并由 restart reconciliation 收敛。
- RunSlot 并发测试覆盖 cancel 发生在 spawn 前、spawn 中和 control 建立后，保证 intent
  不丢失且不会出现 durable `Cancelled` 而新 process 随后启动。
- Language 测试覆盖三个 builtin、process-local `A=B`、assignment-only 拒绝、pipe link
  和每个 Run 的 captured/PTY 选择，并覆盖 EnvValue sensitivity 保留。
- Runtime 测试以真实 process group 验证 pipeline wiring、captured output、PTY 单 terminal、
  control/cancel 与 writer failure，并证明 realization provider 无法修改 semantic spawn。
- Protocol/store 测试覆盖 strict framing、unknown field 拒绝、稳定 reconnect OperationId、
  原子 operation/transition/outbox commit、绝对 output offset，以及 Sensitive data 不落盘。
- client 测试覆盖 query 不触发 `PutScope`、同一 command 断线重试复用 operation identity、
  `tail N` 返回 suffix。
- daemon 与安装产物 smoke 覆盖 Unix socket lifecycle、ack flush-before-drain、restart
  successor、旧数据库归档、CLI/TUI submission、output 读取和 wheel/sdist 命令面。
- 架构检查拒绝恢复 IPC v3 模块、兼容 import、daemon 内 surface parser，以及 Core 对
  runtime、transport、storage 或 frontend 的依赖。
