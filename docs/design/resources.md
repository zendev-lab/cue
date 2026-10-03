# 执行级资源扩展

`cue-resources` 是 cued Composition 中的内置扩展。它按整个 Execution 管理需求、
排队、分配和清理，Core 的执行计划、状态与 Scope 不包含资源策略。

## 配置与提交

默认配置位置为 `$XDG_CONFIG_HOME/cue/daemon.toml`，未设置 XDG 时使用
`~/.config/cue/daemon.toml`。文件不存在时不启用 provider。显式指定的
`cued start --config PATH` 必须存在；后台启动和 restart 保留绝对配置路径。
修改配置后重启生效。配置拒绝未知字段、重复 provider ID 和资源键归属冲突。

```toml
[[resources.providers]]
id = "workers"
kind = "static"
[resources.providers.capacity]
worker = "2"
scratch = "100GiB"

[[resources.providers]]
id = "licenses"
kind = "command"
argv = ["/opt/licenses/provider"]
timeout_ms = 3000
[resources.providers.keys]
license = "count"

[[resources.providers]]
id = "nvidia"
kind = "nvidia"
safety_margin_bytes = 1073741824
```

数量以字符串表示。无单位正整数表示计数；字节接受 `B`、`KB` 至 `TB`（十进制）
及 `KiB` 至 `TiB`（二进制）。零、负数、小数、溢出和未知单位均被拒绝。
命令 provider 的键类型为 `count` 或 `bytes`。

```sh
cue run train.cue --need gpu=2 --need gpu_mem=24GiB
cue client run task.cue --need worker=1
cue client exec --need worker=1 -- "sleep 1 -> printf done"
cue client exec --need worker=1 -- printf hello
cue resources --json
cue providers --json
cue client show E1
cue client cancel E1
```

`exec` 的单个 SOURCE 参数按 Cue 语言编译；`--` 后多个参数按直接 argv 编译。
重复需求键在客户端拒绝；未知键、类型不符和静态容量超限在提交前拒绝。
没有需求的提交沿用普通 Execution 接口。Step 级 `:run(need.*)` 会提示改用提交参数。

需求原子入队后已有 Execution ID。CLI 等待执行完成并返回程序状态，断连不取消；
可从另一个客户端通过 list/show 查询并 cancel/kill。`resources` 和 `providers`
默认也输出 JSON；查询不触发申请或清理。show 的 `resources` 字段是独立资源投影。

## 准入与物理执行

协调器按 Execution ID 的入队顺序尝试最早可满足的请求，允许较小请求越过容量不足
的请求，不保证大请求无饥饿。全部 provider 成功且持久化后才推进 Builtin 或 Run。
无需求执行独立推进。多 provider 按 ID 排序申请，部分失败逆序回滚。

同一 Execution 的顺序步骤、并行分支、pipeline 和 PTY 共享分配。物理环境贡献
通过 SpawnTransform 写入 SpawnContext，在逻辑环境与进程 EnvPatch 之后应用。
两个 provider 贡献同名环境键会回滚分配；步骤不能覆盖或 unset 设备环境。
环境贡献属于本地受信配置的持久分配数据，不应用于存放秘密凭据。

资源状态包括 `waiting`、`allocating`、`allocated`、`cleaning`、`released` 和
`isolated`。`reason` 保留容量不足或故障原因，`attempts` 展示稳定申请身份、grant
和清理进度。执行成功与资源释放分别判断；清理失败不改写程序退出码。

## JSON stdio 协议

每次调用启动配置的 argv，在 stdin 写入一个 JSON 对象并关闭 stdin；stdout 必须
是一个严格 JSON 响应。`timeout_ms` 默认 3000，范围为 1 至 60000 毫秒。
stdout 限制 1 MiB，stderr 限制 64 KiB。非零退出、超时、超量输出和畸形响应均
作为未知结果处理。子进程使用独立进程组；超时或调用取消会终止该进程组。

```json
{
  "version": 1,
  "method": "reserve",
  "daemon_id": "persistent-daemon-uuid",
  "request_id": "stable-request-uuid",
  "execution": 7,
  "needs": {"license": "1"}
}
```

方法为 `probe`、`reserve`、`lookup`、`release`。probe 使用空 request_id、execution 0
和空需求；它只观察，不申请。其他方法沿用同一 daemon_id、request_id、execution
与 needs。provider 必须跨进程持久化去重并串行分配，相同身份不同参数必须拒绝。

| 响应 | 含义 |
| --- | --- |
| `{"status":"snapshot","data":{}}` | probe 的只读容量快照 |
| `{"status":"granted","grant":{"id":"grant-id","environment":{},"devices":[]}}` | 稳定分配及物理环境贡献 |
| `{"status":"rejected","reason":"capacity"}` | reserve 确认未产生占用 |
| `{"status":"absent"}` | lookup 确认该身份未产生申请 |
| `{"status":"released"}` | 已释放且保留禁止迟到 reserve 重新分配的持久墓碑 |
| `{"status":"unknown","reason":"backend unavailable"}` | 当前无法确认归属 |

调用前先持久化申请身份及未知标记。reserve 响应丢失后先 lookup 原申请，随后清理，
不换 ID 盲目 reserve。release 必须幂等；即使申请 absent，release 也要建立墓碑，
防止迟到申请重新生效。释放响应丢失时 lookup 可确认 released；只有确认清理完毕，
才允许为等待执行创建新的申请身份。

## NVIDIA

仅配置 `kind = "nvidia"` 时启用，资源键固定为 `gpu` 和 `gpu_mem`。
通过 NVML 支持的 `nvidia-smi` 查询 UUID、总显存和物理空闲显存，按 UUID 稳定排序。
可用 `argv` 指定探测程序，测试用它返回受控快照。无驱动或设备不阻塞其他 provider
与普通执行；GPU 需求报告不可用。

`gpu_mem` 为每个设备的预算；未写 `gpu` 时默认为一个。只有 `gpu` 的请求独占设备，
有 `gpu_mem` 的请求可共享。可用预算为
`min(物理空闲, 总显存 - 未释放预留) - safety_margin_bytes`，避免把已实现的显存
使用重复扣减。预算不构成 CUDA 分配硬限制，范围是单 daemon 的协作式本地资源。

分配写入 `CUDA_VISIBLE_DEVICES` 的 UUID 列表和 `CUDA_DEVICE_ORDER=PCI_BUS_ID`；
重启按 UUID 检查设备归属，不能用新序号重新解释分配。该实现按完整 GPU 分配，不对
MIG 实例切片。接口依据见 [nvidia-smi 文档](https://docs.nvidia.com/deploy/nvidia-smi/index.html)
和 [CUDA 环境变量](https://docs.nvidia.com/cuda/cuda-programming-guide/05-appendices/environment-variables.html)。

## 扩展 IPC 与存储恢复

IPC v5 的 Query/Command `extension` payload 包含 `namespace`、`version`、
`method`、`data`。资源命名空间为 `resources`、扩展版本为 1。查询方法为
`list`、`providers`（data 为 `{}`）与 `show`（`{"execution":7}`）；命令方法
为 `submit`（`{"spec":ExecutionSpec,"needs":{"worker":"1"}}`）。
查询返回 `ResultPayload::Extension`，提交返回普通 ExecutionSubmitted。
未知命名空间、版本、方法和多余数据字段均拒绝。

通用 SubmissionEffect 与 Execution、Fact、OperationId 去重记录共用事务；
完整扩展请求参与命令指纹。重放直接返回原执行，不再次验证容量或分配。
`resource_executions` 和 `resource_identity` 属于扩展，保存在现有 `cued-v4.db`。
schema 2 在物理恢复检查通过后，事务性增加扩展表并提升至 schema 3，已有执行历史
不转换。旧 daemon 拒绝 schema 3，没有人工数据迁移或账本清空命令。

Run 只有在运行时确认物理静止后才提交完成；未知进程所有权保持非终态并阻止恢复。
协调器据此释放终结 Execution 的资源，单个步骤退出不会释放。申请与释放故障会
持久保留并后台重试。启动先检查未解决申请的原配置、稳定身份和 provider lookup，
再恢复执行；删除或重绑定等待执行的 provider，或修改仍有占用的 provider，均拒绝
恢复。恢复到可确认的清理状态后继续清理，不重复运行已终结的程序。
