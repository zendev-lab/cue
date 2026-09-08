---
fp: 2
title: "执行级资源管理"
type: Feature
authors:
  - "zrr1999"
created: 2026-09-08
supersedes: []
---

# FP-0002: 执行级资源管理

## 摘要

Cue 通过 cued 内置的 Composition 资源服务提供 NVIDIA GPU/显存分配、通用资源
provider、准入等待和查询。资源在 Execution 开始执行前一次性申请，所有 Run 共享
同一批分配，直到 Execution 终结且物理进程全部确认退出后释放。

资源需求、队列和 reservation 属于组合服务，不进入 Core 的执行状态或
`ExecutionPlan`。本提案在 [FP-0001](FP-0001-structured-execution-kernel.md)
之上定义 daemon/Composition 的资源服务边界，不整体替代该提案。
本文只记录设计；合入不表示功能已实现。

## 动机

多步骤训练需要在准备、训练和结果处理之间保持同一组 GPU。把申请交给每个客户端，
既无法协调并发提交，也留下客户端断开后谁释放资源的问题；按 Step 释放则可能在步骤间
更换设备。Cue 已有 daemon 和物理执行所有权，适合由组合服务承担本地资源生命周期。

旧实现可追溯至资源调度提交 `5ceff613a53560b75e16027b8c3666c7a5f1b9b0`（PR #16）
及 v4 切换前的 `50beab7^`。其行为与本提案的对应关系如下：

| 旧实现 | 本提案 |
| --- | --- |
| Count/Bytes 数量、按资源键路由到 provider | 保留类型与路由原则，严格拒绝错误输入 |
| 按 StepId 申请，在步骤终结时释放 | 改为 Execution 级申请，跨步骤持有 |
| 按 provider ID 申请，部分拒绝后逆序释放 | 保留顺序；未确认回滚必须持久化，不宣称回滚成功 |
| 内存 reservation 表、best-effort release | 持久化申请身份与清理进度，区分结果未知和已释放 |
| NVML 探测及设备环境变量注入 | 保留 GPU 能力，通过物理 SpawnContext 应用 |
| JSON stdio probe/reserve/release | 增加幂等身份及查询，处理响应丢失 |
| `:providers`、`:resources`、Step 级 `need.*` | 提交参数和 daemon 查询，旧 Step 语法提供迁移提示 |

当前 v4 的语言层仍识别部分旧资源语法，但编译为 ExternalOwner 错误；它不是已可用的
资源服务。旧代码只能作为能力和测试参考，不能把旧 scheduler 或 reservation 表直接恢复。

## 设计

### 提交与查询

资源需求通过提交参数声明，不增加文件级资源语法：

```sh
cue run train.cue --need gpu=2 --need gpu_mem=24GiB
cue client run train.cue --need gpu=2 --need gpu_mem=24GiB
cue client exec --need gpu=1 -- python train.py
cue resources --json
cue providers --json
cue client show E1
cue client cancel E1
```

`run` 接受文件路径前后的重复 `--need KEY=QUANTITY`；`exec` 在 `--` 前解析
提交选项，分隔符之后按现有 exec SOURCE 规则处理命令。资源键不能重复，未知键、空键、
布尔数量、零值、溢出或 provider 不接受的 Count/Bytes 类型都在创建 Execution 和入队前
拒绝。沿用数量类型的整数计数及字节单位解析规则，不接受负数。

`.cue` 文件和交互输入中的 `:run(need.*)` 继续拒绝，但指出资源已改为 Execution
提交参数；不能自动把某一个 Step 的需求提升到整个 Execution。首版交互 TUI 不增加资源
申请入口或资源专页。

`cue resources` 展示容量快照、已分配资源、等待的 Execution、阻塞原因和清理状态；
`cue providers` 展示 provider ID、资源键归属、可用性与故障原因。两者提供稳定 JSON
投影，查询不创建 Scope、Execution 或触发申请。`cue client show` 附带资源投影，内核
执行状态保持原义，不能用执行成功掩盖待清理 reservation。

CLI 的等待和执行退出码保持现有约定；输入或提交错误返回非零。等待中的任务已有
Execution ID，沿用 cancel/kill；断开客户端不取消任务、不释放资源。资源清理失败不
篡改程序退出码，而是独立显示并记录 daemon 诊断。

### 组合服务与准入

组合服务持有 provider registry、持久化资源状态和可取消的准入队列。每个资源键只能
由一个 provider 拥有，重复 provider ID 或键归属是启动配置错误。服务可复用数量类型与
路由算法，但资源策略和状态类型不由 cue-core 定义。

提交接口接收 ExecutionSpec 及独立的资源需求。创建 Execution、记录需求与入队顺序、
保存操作去重结果必须处于同一持久事务；去重比较包含资源需求。同一逻辑提交重放返回
原 Execution，不重复入队或申请。不能以客户端先 reserve、再 submit 的两次调用替代。

资源不足时按持久化入队顺序扫描，允许最早可满足的请求先运行；等待不占用部分 provider
的 grant。不声明资源的任务不进入队列。资源释放、provider 恢复和有界周期探测触发重试；
首版不保证无饥饿，也不引入优先级、抢占或预约。

资源就绪前禁止驱动执行步骤，包括 Builtin；取消及取消所需的归约和清理不受准入阻塞。
服务在进入执行驱动前与取消状态同步检查，申请与取消竞态只能产生“未启动并清理申请”
或“已启动并按正常取消流程终止”，不能在取消后启动新的物理进程。

跨 provider 按稳定 ID 顺序申请，全部成功并持久化后才允许执行。部分拒绝按逆序释放；
任一释放结果不明时保留占用，进入清理状态，不携带残留申请重新排队。容量不足是等待，
非法请求是拒绝，provider 故障是独立阻塞原因，不能把探测失败当成零容量。

### 执行级分配

`gpu` 表示设备数量，`gpu_mem` 表示每张卡的显存需求。仅声明显存时默认申请一张卡；
仅声明 GPU 数量时独占所选设备；两者同时声明时允许在满足预留预算的设备上共享。
显存预算用于准入，不是对应用实际分配的硬限制。

同一 Execution 的所有 Run、pipeline 进程和 PTY 使用同一组设备。顺序步骤之间不释放，
并行分支共享整批分配，不复制预算或自动为分支切分设备；需要独立配额的工作应分别提交。

分配结果进入物理 SpawnContext，提供资源句柄和受资源服务管理的环境覆盖，例如
`CUDA_VISIBLE_DEVICES`、`CUDA_DEVICE_ORDER`。不修改逻辑 Scope、argv、EnvPatch
或内核计划；逻辑环境构造完成后应用物理覆盖，步骤的 env set/unset 不能覆盖已分配设备。
多个 provider 声明同一物理环境键应拒绝配置或分配，不以执行顺序静默覆盖。

### 持久化与释放

服务按 Execution ID 记录需求、排队序号、稳定申请 ID、各 provider grant、分配设备和
清理进度。资源投影区分等待、申请中、已分配、清理中、已释放及结果不明的隔离状态；
这些是物理管理状态，不是另一套 Execution 状态机。

每次跨 provider 的申请尝试先持久化身份，再调用 provider。响应丢失时按原申请身份查询，
不能换 ID 重试；只有原尝试已确认无占用，才允许开始新尝试。reserve 结果持久化前发生
崩溃也必须能查回原 grant。

正常结束、运行失败、spawn 失败和取消均要求 Execution 已终结，并且所有已开始的物理
执行均确认静止，才可释放。不存在物理启动的已取消任务可以直接清理 grant。单个 Step
完成、客户端断连和执行终态本身都不是充分的释放证据。

release 按 grant/申请身份幂等调用；失败或响应不明保留占用和错误，后台重试并通过查询
暴露。执行事实的提交重试不得重新 spawn 或重复分配；已确认释放的记录保留用于去重与
恢复，不因一次查询就删除。

恢复先核对持久化申请与 provider，再允许执行驱动或重新准入。不能仅凭 PID 消失、超时
或数据库中的终态推定可以回收。无法确认物理执行所有权或申请结果时保留隔离占用，
遵守当前 daemon 对未知物理执行拒绝恢复的边界，不自动接管或重启孤儿进程。启动拒绝的
诊断必须包含相关 Execution 和申请身份；恢复正常前不得清空资源账本或提供强制释放捷径。

### Provider 契约

通用 JSON stdio provider 提供版本化的 probe、reserve、lookup、release 操作。
reserve 输入至少包含 daemon 持久身份、稳定申请 ID、Execution ID 和归属该 provider
的需求；grant 包含稳定身份、分配信息及物理环境贡献。lookup 能区分已分配、已释放、
确定未申请和未知结果，未知不能按未申请处理。

相同身份、相同参数的 reserve 返回原结果；相同身份、不同参数必须拒绝。release 可
重复且结果可查询。各操作有界执行，进程失败、超时和畸形 JSON 作为 provider 故障报告。
provider 对其资源负责串行化及持久去重，不能依赖一次 CLI 进程的内存维持占用。

NVIDIA provider 使用设备稳定身份关联实时 NVML 探测与持久化预留，明确区分物理空闲
和 Cue 预算，避免对同一用量重复扣减。重启后不能仅凭设备序号重新解释已分配设备。
无 NVIDIA 驱动或设备时普通任务与其他 provider 仍可用，显式 GPU 请求返回不可用原因，
不能无限伪装成等待容量。

### 协议与范围

daemon 协议新增提交需求和只读资源投影，由 Composition 服务处理；Core 不新增资源
节点、资源等待状态或策略归约。资源元数据随提交原子保存，物理实现通过运行时端口接入。

当前 IPC 是封闭 schema，本功能实施时升级协议版本，同步官方 CLI/client/daemon，
旧版本握手明确拒绝。提案不提前修改当前协议常量，也不把新增请求称为透明兼容 v4。

首版仅管理单一 daemon 协调的本地资源，不包含跨 daemon 共享分配、多机调度、会话、
cron、重试编排、CPU/内存硬限制或 TUI 资源专页。

## 兼容性

这是新的 Execution 级契约，不恢复 v3 的 Step 资源语义，不导入旧内存 reservation、
旧资源状态或旧 provider 协议。旧脚本必须把需求移到提交参数；需要每步骤不同资源时，
拆为独立 Execution，而不是在同一 Execution 中隐式重新分配。

实施时必须为新资源账本和现有执行存储设计同事务迁移；升级前停止旧 daemon 并确认
无未解决的物理执行。降级不能让旧 daemon 打开包含未清理资源的新存储；应先完成资源
清理，再显式恢复升级前备份。不得丢弃新账本来绕过恢复失败。

FP-0001 的 v4 切换记录及核心不变量继续成立；本提案只修订 Composition 可以由 cued
承载资源服务、并通过新版 daemon 协议暴露的边界。当前行为文档在功能实现时再更新。

## 验证

以下是后续实现的验收要求，不是本提案 PR 已完成的运行测试：

- 实际 CLI 与隔离 daemon 覆盖提交参数、JSON、错误退出码、旧语法迁移提示、只读查询
  无副作用及版本握手拒绝；断开提交客户端后任务仍可查询和取消。
- 多客户端竞争同一 GPU 不超分；步骤间设备保持不变且不释放；并行分支共享预算，
  普通进程、pipeline 和 PTY 均采用相同物理设备设置，env set/unset 不绕过分配。
- 队首不足时后续可满足任务可运行；等待取消及取消与申请成功竞态不启动违规进程，
  无需求任务不受队列阻塞，provider 故障与容量不足可区分。
- 多 provider 部分拒绝、回滚失败、reserve/release 响应丢失及重复调用，均验证持久
  身份、查询恢复和无重复 grant；错误输入不留下 Execution 或申请记录。
- 在申请调用前后、提交事务前后、进程退出与释放之间注入崩溃；验证不重复启动、不
  提前回收，未知物理所有权拒绝恢复，清理失败不改变执行退出码且占用仍可见。
- 用可控 GPU 探测替身验证显存预算、独占、设备身份与重启恢复；另在 NVIDIA 实机
  验证设备注入、实时占用、预算分配及完整释放。替身测试不替代实机验收。

提案 PR 只运行仓库的 proposal/index 校验及适用于 Markdown 的检查，并同步
确定性索引。后续实现 PR 关联本 FP，提交上述可观察证据。
