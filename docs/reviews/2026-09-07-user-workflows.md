# 2026-09-07 用户流程审查

审查基线为 `15dd09e`（IPC v4 hard cut 与 stop recovery 已合入）。本文记录该次
审查的证据和处理范围，不替代 FP 或当前行为文档。

## 范围与依据

逐份核对基线中全部 22 份受版本控制的 Markdown，包括 README、SPARK、架构与
设计文档、testing、Skill、FP、模板和历史研究。实现检查沿安装包入口 → 聚合 CLI
→ client/compiler → daemon/store/runtime 的用户调用路径展开，另检查 TUI 的提交、
刷新、连接与 PTY 分工。独立临时 socket/database 用于进程复现；没有对用户默认
socket 执行停止、重启、迁移或安装。

历史研究不是当前产品承诺。Core、store、protocol 和 runtime 的完整行为 suite
用于回归验证；这不是对每个实现分支完成形式化验证或对全部 TUI 交互做过屏幕测试的声明。

[FP-0001](../../fps/FP-0001-structured-execution-kernel.md#兼容性) 明确切断 v3
协议以及 session/schedule/resource/target 的内核兼容入口，但没有要求删除 `--fg`
或改成默认前台。[FP-0000](../../fps/FP-0000-governance.md#设计) 明确允许恢复
既有契约的缺陷修复不新增 FP。本次恢复启动行为，不修改执行代数或恢复 v3 codec。

## 本次修复

| 用户流程 | 基线证据与影响 | 修复与验证 |
| --- | --- | --- |
| 启动后继续使用终端 | `cued start` 的进程一直未退出；旧版默认 fork 后台，v4 直接 await serve。 | 默认 detached 后台；`--fg`/`-f` 显式前台。只接受自己生成的实例 ID 的 Hello，保留日志和启动错误，超时不报成功。 |
| `stop` 后马上 `start` | 有忽略 TERM 的工作时，stop 约 0.010 秒返回 0，但 daemon 和 socket 仍存在。 | 等待 socket 不再监听及 ownership lock 释放；测试验证 TERM-resistant 工作已被回收且可以立即再启动。 |
| `restart` 后马上使用 | host 只打印 RestartAccepted，未观察 successor 是否启动。 | 等待目标 instance ID 就绪；测试在返回后不重试直接连接，校验 target ID。 |
| force stop 与 drain | force stop 的 5 秒 deadline 小于 host 最长 10 秒 drain，可能在正常排空时误报失败。 | 统一使用 15 秒完成等待；仍不发送 SIGKILL 给 daemon。 |
| 自定义路径 | 临时父目录启动前为 `0755`，启动后变为 `0700`。直接改变了用户已有目录的访问方式。 | 只给新建目录设置私有权限；socket、数据库、锁和日志文件保持私有。相对路径在 spawn 前解析。 |
| 多实例数据库所有权 | 原来只有 socket lock；不同 socket 可绕开同一数据库的 host 排他要求。 | canonical database 路径的独立 ownership lock；第二个 socket 不能打开同一数据库。锁不加在数据库 inode 上，避免干扰 SQLite 的 VFS 锁。 |
| 不存在的命令 / 无效 cd | 两者都返回 1，stdout 为 0 字节、stderr 为空，用户无法知道失败原因。 | runner 输出存储的 Spawn/Builtin/Infrastructure/Signal 原因和 Step ID；已被组合逻辑恢复的失败不额外报错。 |
| 输出超过保留上限 | 生成 1,048,640 字节，只收到 1,048,576 字节，退出 0 且 stderr 为空。 | offset-zero 读取返回较大 offset 时警告截断；命令退出状态保持其执行结果。没有把此修复描述为完整输出保留。 |
| 客户端连接错误 | ExecutionClient 的 Hello 没有时限；无 daemon 只给出 socket I/O 错误。 | 本地 connect/Hello 有两秒时限，并指向 start/status；执行 Wait 不受此时限影响。 |
| 安装后的首次示例 | Quick start 引用了只有仓库中存在的 `examples/hello.cue`。 | 示例先创建本地 hello.cue；安装包 smoke 直接验证后台 start 和 restart 后的即时使用，不用 shell `&` 掩盖阻塞。 |
| 文档能力边界 | 研究文档仍有“当前 wrapper/调度器”等旧描述，SPARK 暗示 argv expansion 已实现。 | 历史研究加明确标识；当前 README/Skill/design 说明 literal argv、输出保留和恢复限制。 |

对应行为测试：

- [daemon lifecycle](../../crates/cue-daemon/tests/lifecycle.rs)：后台/前台、就绪身份、启动错误、竞争启动、数据库排他与 drain；
- [control recovery](../../crates/cue-daemon/tests/control_recovery.rs)：不兼容/无响应 listener、信号退出失败、丢失 ACK；
- [CLI workflows](../../crates/cue-cli/tests/user_workflows.rs)：错误原因、截断提示、聚合命令、无 HOME 的显式路径；
- [installed package smoke](../../scripts/smoke_package.sh)：wheel/sdist 的真实命令入口，显式隔离 CUE_SOCKET。

## 仍需单独处理的用户体验缺口

### 高优先级：等待期间没有实时输出，也不传 stdin

[script runner](../../crates/cue-client/src/script_runner.rs) 与
[exec](../../crates/cue-client/src/cli.rs) 仍先 WaitExecution，再读取输出。
隔离运行 `print → sleep 1s → print` 时，前 400 ms 看不到首段输出，约 1.034 秒后
才一次得到两行。需要交互输入的 PTY 程序也不会被这个等待路径自动 attach。

目前可通过 TUI 提交，再用另一个终端的 `cue fg E…/S…` 交互。完整修复需要明确
runner 的流式输出、stdin、Ctrl-C 与 detach 契约；不能仅靠把等待换成短超时或静默重跑。

### 高优先级：输出字节不是持久日志

[MemoryOutputStore](../../crates/cue-runtime/src/output.rs) 每个 Step/stream
只保留最后 1 MiB。daemon 重启后缓冲区为空，执行事实与 OutputAppended ranges 则
仍持久存在。当前截断告警只解决仍可由返回 offset 证明的前缀丢失；重启后的空缓冲
不能证明原执行没有输出。

这一限制原来只藏在 runtime 设计文档里，现已提升到 README 与 Skill。持久输出
provider、保留策略和“已不可用”的明确查询表达需要独立设计与实现。

### 高优先级：有未知物理 attempt 的崩溃可能阻断再次启动

[daemon bootstrap](../design/daemon.md#bootstrap) 与 store recovery 拒绝对
未知 Run attempt 重放。这符合 FP 的 quiescence 要求，但当前没有 operator repair /
abandon 入口，因此“持久执行”不能被理解为任意 crash 后都可以无操作恢复。

不能为让 start 成功而清除 attempt marker 或捏造终态。需要能够证明旧工作已静止
或重新取得控制权的机制；本次保持这一边界并明确限制。

### 中优先级：TUI 对其他客户端提交的工作没有全局自动刷新

[TUI](../../crates/cue-tui/src/lib.rs) 只对本连接新提交的 Execution 调用
WatchExecution。初次 list 不会为已有执行建 watch，也没有定时全局刷新；daemon
只向已 watch 的连接推送对应 facts。因此共享使用时，其他客户端的新任务或已有
任务的变化需要手动 `:jobs`。这是调用路径检查结论，没有宣称完成视觉交互验证。

补全菜单、旧卡片交互、侧栏/详情和剪贴板功能也没有移植。实现文档已声明缩减，但
FP 没有要求必须删掉这些前端便利性；可在 v4 projection 上恢复。

### 中优先级：版本号不能直接区分本次协议切换

本次现场 v3 daemon 与 v4 可执行文件都报 `0.1.2`。协议与进程身份才区分了两者。
恢复诊断已明确检查 IPC v4；发行版本和升级说明仍需在正式发布流程中处理，不能
把 main 上的实现等同于用户从包 registry 安装到的版本。本次没有发布新版本。
