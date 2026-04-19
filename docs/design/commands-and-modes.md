# Cue Shell — 基础命令与模式最终设计方案 v2

> 已完成三轮评审、竞品调研和命名调研后的最终设计。
> 关键变更（v1→v2）：`:` 前缀替代 `cmd:` 分隔符、去掉 CMD 模式、`:cron` 内部语法、新增 `:probe`/`:confirm`/`:escalate`。

---

## 一、核心语法：`:` 前缀

**所有内建命令以 `:` 开头，后接命令名和参数。**

```
:run cargo build          # 发射 job
:kill J1                  # 终止 job
:jobs                     # 列出所有 job
:scope list --tree        # 列出 scope
:env                      # 查看 env（无参数也用 :）
:env set FOO=bar          # 设置 env
:help                     # 帮助
?                         # 当前模式帮助
:help run                 # run 命令的帮助
```

**设计理由**（`:` 前缀 vs `/` 前缀 vs `cmd:` 分隔符）：
- `:` 在所有模式下零冲突：shell 命令、自然语言、cron 表达式都不以 `:` 开头
- Vim/Helix/lazygit 用户有肌肉记忆（TUI 文化一致）
- `/` 在 JOB 模式下与绝对路径 `/usr/bin/...` 冲突
- Agent 友好：`:` 不是任何编程语言的转义字符
- 解析规则极简：**首字符 `:` → 内建命令，否则 → 模式默认包装**

**冒号后空格可选**：`:run cargo build` 和 `:run  cargo build` 都合法（trim）。

> 完整的前缀选择调研与评分见 [research/syntax-decisions.md](../research/syntax-decisions.md)。

---

## 二、三模式设计

### 只有三个模式（无 CMD）

| 模式 | 默认包装 | 含义 | 对应核心原语 |
|------|---------|------|-------------|
| **JOB** ⚡ | → `:run <input>` | 输入即执行 | Job |
| **AGENT** 🤖 | → `:ask <input>` | 输入即对话 | Agent |
| **CRON** ⏰ | → `:cron <input>` | 输入即调度 | Cron |

- Shift+Tab 循环切换：JOB → AGENT → CRON → JOB
- **不需要 CMD 模式**：`:` 前缀在任何模式下都能直接执行内建命令
- 想"纯手动"？在任意模式下全用 `:` 前缀即可

### 解析规则

```rust
fn dispatch(input: &str, mode: Mode) -> Result<Action> {
    let input = input.trim();
    if input.is_empty() { return Ok(Action::Noop); }

    // Rule 1: `:` 前缀 → 内建命令（所有模式下一致）
    if input.starts_with(':') {
        let rest = input[1..].trim_start();
        let (cmd, args) = split_first_word(rest);
        if BUILTINS.contains(cmd) {
            return Ok(Action::Builtin { cmd, args });
        } else {
            return Err(format!("unknown builtin: :{cmd}"));
        }
    }

    // Rule 2: 模式默认包装
    match mode {
        Mode::JOB   => Ok(Action::Builtin { cmd: "run", args: input }),
        Mode::AGENT => Ok(Action::Builtin { cmd: "ask", args: input }),
        Mode::CRON  => Ok(Action::Builtin { cmd: "cron", args: input }),
    }
}
```

### 模式转换示例

| 输入 | JOB ⚡ | AGENT 🤖 | CRON ⏰ |
|------|--------|----------|---------|
| `cargo build` | `:run cargo build` | `:ask cargo build` | `:cron cargo build` |
| `run the tests` | `:run run the tests` | `:ask run the tests` | `:cron run the tests` |
| `:kill J1` | 内建 kill ✅ | 内建 kill ✅ | 内建 kill ✅ |
| `:jobs` | 内建 jobs ✅ | 内建 jobs ✅ | 内建 jobs ✅ |
| `/usr/bin/python a.py` | `:run /usr/bin/python a.py` ✅ | `:ask /usr/bin/python a.py` | — |
| `every 5m cargo test` | `:run every 5m cargo test` | `:ask every 5m cargo test` | `:cron every 5m cargo test` ✅ |

**零歧义**：`:` 开头 = 内建，否则 = 模式默认。没有命名冲突，没有 fallthrough，没有上下文依赖。

---

## 三、基础命令完整列表

### 3.1 Job 管理

| 命令 | 语法 | 语义 | Planner | Executor |
|------|------|------|---------|----------|
| `:run` | `:run <cmd> [chain...]` | 发射 job | ❌ | ✅ |
| `:jobs` | `:jobs [--json]` | 列出所有 job 摘要 | ✅ | ✅ |
| `:wait` | `:wait J1` / `:wait A1` | 等待 job/agent 进入终态 | ❌ | ✅ |
| `:out` | `:out J1` | 查看 job stdout snapshot | ❌ | ✅ |
| `:tail` | `:tail J1 [bytes]` | 打开并持续 follow job stdout | ❌ | ✅ |
| `:err` | `:err J1` | 查看 job stderr snapshot | ❌ | ✅ |
| `:send` | `:send J1 <input>` / `:send A1 <prompt>` | 向 running job 写 stdin；或向 waiting agent 发送 follow-up prompt | ❌ | ✅ |
| `:kill` | `:kill J1` / `:kill A1` | 终止 running job；或结束整个 agent session | ❌ | ✅ |
| `:cancel` | `:cancel J3` / `:cancel A1` | 取消 queued job；或 abort 当前 agent turn | ❌ | ✅ |
| `:fg` | `:fg J2` / `:fg A1` | Job 进入前台 pty；Agent 打开会话前台视图 | ❌ | ✅ |

`:run` / JOB bare input 在发射前会基于当前 scope snapshot 做**显式 word expansion**：支持前导 `~`、`$VAR`、`${VAR}`；仍保持 direct exec，不隐式走 shell，也不做 glob / command substitution / field splitting。

另外，`cue-shell` 现在把两类输入当作**原生 scope-transform job** 处理，而不是交给外部 shell：

- `:run cd <path>`
- `:run env set KEY=VALUE ...`

其语义是：

- 立即生成该 job 的 `end_scope`
- **不会**自动移动默认 HEAD
- serial chain 中，后一 leaf 会继承前一 leaf 的 `end_scope`
- parallel / pipeline 中若出现这类 scope-transform leaf，当前直接拒绝，避免作用域歧义

### 3.2 Scope 管理

| 命令 | 语法 | 语义 | Planner | Executor |
|------|------|------|---------|----------|
| `:scope list` | `:scope list [--tree]` | 列出所有 scope | ✅ | ✅ |
| `:scope new` | `:scope new [--profile rust]` | 创建新 scope | ❌ | ✅ |
| `:scope env` | `:scope env S1` | 查看 scope env | ❌ | ✅ |
| `:scope fork` | `:scope fork S1 [--name exp]` | 从 scope 派生（delta 存储） | ❌ | ✅ |
| `:scope close` | `:scope close S1` | 归档 scope | ❌ | ✅ |

### 3.3 Agent 管理

| 命令 | 语法 | 语义 | Planner | Executor |
|------|------|------|---------|----------|
| `:ask` | `:ask 帮我跑完整个 CI` | 用户→Planner 入口 | N/A (用户) | N/A |
| `:spawn` | `:spawn --plan <json> [--inherit-scope S1]` | Planner→Executor | ✅ | ✅(sub) |
| `:agents` | `:agents` | 列出活跃 agent | ✅ | ✅ |
| `:confirm` | `:confirm "是否继续部署 production？"` | Planner→用户确认 | ✅ | ❌ |
| `:escalate` | `:escalate "任务超出范围，需要改 CI 配置"` | Executor→Planner 上报 | ❌ | ✅ |
| `:probe` | `:probe out J1 --tail 50` | Planner 轻量只读窥探 | ✅ | N/A |

**`:probe` 约束**：
- 只读，绝对没有副作用
- 硬性输出上限（4KB），超出自动截断
- 不阻塞（不能 `:probe wait`）
- 同步返回，不创建 scope
- 当前可用子命令：`status`, `out`, `err`, `env`
- `out` / `err` 目前都读取 phase-1 PTY 合并日志；`err` 暂时不是独立 stderr ring
- `env` 当前支持 `head` 或 job ID（读取 job 的 start/end scope 快照），暂不支持任意 `S@...` 查找

**`:spawn` 新增 `--inherit-scope`**：
- Executor B 继承 Executor A 的 scope（默认只读）
- A 完成后 scope 冻结，B 读取 A 写入的 env 数据
- 避免 Planner 成为大数据中转站

**Executor 可 spawn sub-executor**（深度限制，默认最多 3 层）

**当前 AGENT follow-up 语义**：
- `:ask` / `:spawn` 会启动一个长寿命 ACP backend session
- backend 每完成一轮输出后进入 `WaitingInput`
- `:send A1 ...` 会在同一 session 上追加下一轮 prompt
- `:fg A1` 会把该 agent session 带入前台会话视图；此时 Enter 直接提交下一轮 prompt，Ctrl+C 只取消当前轮，Ctrl+Z 仅做本地 detach
- TUI 中点击侧边栏 AGENT 行会直接请求进入该前台会话
- `:cancel A1` 会向 backend 发送 `session/cancel`，只中断当前轮，不销毁整个 session
- `:kill A1` 会终止整个 agent session
- `:ask(session=sess_xxx) ...` / `:spawn(session=sess_xxx) ...` 会在 backend 声明 `loadSession` capability 时续传已有 ACP session
- daemon 重启时会基于持久化的 `backend / scope_hash / session_id` 尝试恢复未终态 agent；恢复失败会把该 agent 标记为 `Failed`，而不是让它静默从 `:agents` / UI 中消失
- agent transcript 现在也会随 daemon 一起持久化；因此退出 `cue-tui` 后重新连接，或 `cued` 重启恢复后，前台/侧栏里的 agent 会话内容会继续存在，而不只是剩一个空的 `A<n>` 条目
- 终态 agent 现在也会保留在 `:agents` 历史里；daemon 重启后仍可见，不再只恢复活跃 session
- `:confirm <msg>` 当前返回结构化 `ConfirmRequest { prompt }` 给 client，由前端决定如何展示/回填
- `:escalate <msg>` 当前会新建一个 Planner agent，并把消息作为 escalation prompt 转发过去

### 3.4 Cron/定时管理

| 命令 | 语法 | 语义 | Planner | Executor |
|------|------|------|---------|----------|
| `:cron` | `:cron <schedule> <cmd>` | 添加定时/延迟任务 | ❌ | ✅ |
| `:crons` | `:crons` | 列出所有定时任务 | ✅ | ✅ |

- `:crons` 现在展示**持久化 cron 历史**，而不只是在内存中的活跃注册项
- one-shot cron 触发后会保留为 `completed`；daemon 重启时已错过的 one-shot 会保留为 `expired`，都不会再补跑

**`:cron` 内部语法（B+C 混合）**：

全局只有 `:cron` 一个命令，`every`/`at`/`in` 等是其内部关键字，不是独立内建命令。

```
# ── 关键字路径（日常 90%，零冗余） ──
:cron every 5m cargo build               # 间隔：每 5 分钟
:cron every 2h make test                  # 间隔：每 2 小时
:cron at 14:30 ./deploy.sh               # 定时：每天 14:30
:cron at midnight ./backup.sh            # 定时：每天午夜
:cron at 9am on weekdays cargo test      # 组合：工作日 9 点
:cron on mon,wed,fri at 15:00 ./report   # 组合：周一三五下午 3 点
:cron in 5m cargo build                  # 一次性：5 分钟后执行
:cron in 30s cargo test                  # 一次性：30 秒后执行
:cron daily cargo clippy                 # 预设别名
:cron hourly ./health-check.sh           # 预设别名
:cron weekly ./cleanup.sh                # 预设别名
:cron monthly ./report.sh               # 预设别名
:cron cron */5 * * * * curl api/health   # 原生 crontab（cron 后固定 5 字段）

# ── do 回退路径（复杂/动态场景 10%） ──
:cron */5 * * * * do curl api/health     # 原生 crontab + do 分界
:cron every 30m 9am-5pm weekdays do ./check.sh  # 复杂调度
:cron $MY_SCHEDULE do $MY_CMD            # 动态调度
```

**关键字解析规则**：

| 首 token | 消耗 token 数 | 语法 |
|---------|-------------|------|
| `every` | 1（duration） | `every <dur> <cmd...>` |
| `at` | 1-3（time [on dayspec]） | `at <time> [on <days>] <cmd...>` |
| `on` | 3（dayspec at time） | `on <days> at <time> <cmd...>` |
| `in` | 1（duration） | `in <dur> <cmd...>` |
| `cron` | 5（cron fields） | `cron <f1> <f2> <f3> <f4> <f5> <cmd...>` |
| `daily`/`hourly`/... | 0 | `<preset> <cmd...>` |
| 其他 | 扫描 `do` | `<free-schedule> do <cmd...>` |

> 完整的 cron 语法设计过程与备选方案对比见 [research/syntax-decisions.md](../research/syntax-decisions.md)。

> 当前 runtime 已支持：`every <dur>`、`in <dur>`、`at <time> [on <days>]`、`on <days> at <time>`、`daily/hourly/weekly/monthly`、`cron <5f>`，以及 `<5-field-crontab> do <cmd>`。
> 更自由的 `do` 回退（如 `every 30m 9am-5pm weekdays do ...` / 动态变量 schedule）目前仍显式保留为后续能力，不再假装已实现。

### 3.5 通用命令

| 命令 | 语法 | 语义 | Planner | Executor |
|------|------|------|---------|----------|
| `:env` | `:env` | 查看当前持久化 HEAD env | ✅(只读) | ✅ |
| `:env set` | `:env set FOO=bar` | 设置 env 并打印实际副作用 | ❌ | ✅ |
| `:help` | `:help` / `:help run` | 帮助 | ✅ | ✅ |
| `?` | `?` | 当前 mode 的详细帮助 | ✅ | ✅ |
| `:config` | `:config` / `:config show` | 查看配置 | ✅ | ✅ |
| `:exit` | `:exit` | 退出 TUI | N/A | N/A |

这里需要区分：

- 顶层 `:cd ...` / `:env set ...`：修改默认 HEAD scope，并持久化
  - 响应返回新的 scope hash，并打印**实际生效的副作用**（如 `cwd old -> new`、`KEY: old -> new`）
  - `:env set` 对重复变量按最终 key 去重，只展示最终实际变化
- `:run cd ...` / `:run env set ...`：只修改该 job 的 `end_scope`

前端补充：

- `?` / `:help` 仍由 daemon 内建处理
- copy、target 切换、页面点按等属于前端本地 UI 语义，不属于 `cued` 内建命令面

---

## 四、Planner vs Executor 权限边界

### 设计原则
- **Planner 是事件驱动的决策器**，不轮询，不阻塞
- **Planner 看全景 + 可窥探细节**（通过 `:probe`，有输出上限）
- **Planner 不产生副作用**（`:spawn` 和 `:confirm` 除外）
- **写操作全部通过 Executor**

### Planner 唤醒事件

```
user_input:     用户的新请求或追问
executor_done:  Executor 完成（带结构化摘要）
executor_error: Executor 异常退出
escalate:       Executor 上报需要决策
cron_trigger:   定时任务触发
```

### Planner 可执行

```
# 宏观摘要（只读）
:jobs           — job 列表摘要
:agents         — agent 列表
:crons          — 定时任务列表
:scope list     — scope 列表
:env            — 当前 env（只读）
:help           — 帮助
?               — 当前 mode 的详细帮助
:config         — 配置查看

# 轻量窥探（只读，4KB 上限）
:probe out J1 --tail 50
:probe err J1 --grep ERROR
:probe status J1
:probe env head PATH HOME
:probe env J1 PATH HOME

# 决策动作
:spawn          — 创建 Executor（唯一的"写"操作）
:confirm        — 请求用户确认（高风险操作前）
```

### Executor 可执行

```
# 在自己的 scope 内的全部读写操作
:run  :wait  :out  :tail  :err  :send  :kill  :cancel  :fg
:scope new/env/fork/close
:env set
:cron

# 分治与上报
:spawn          — sub-executor（深度限制 ≤3）
:escalate       — 上报 Planner 做决策
```

### Executor 结构化上报

Executor 完成时向 Planner 上报结构化摘要（而非让 Planner 自己读 stdout）：

```json
{
  "status": "failed",
  "category": "test_failure",
  "failed_tests": ["test_auth_login", "test_db_connect"],
  "error_summary": "2/47 tests failed, both in auth module",
  "suggestion": "auth module regression"
}
```

---

## 五、Scope 持久化策略

### Delta 存储
- fork 出的 scope 只存储 `parent_id` + `env_delta`
- 读取时沿 parent 链合并得到完整 env
- 大幅减少磁盘开销

### Scope 继承（Executor 间数据共享）
- `:spawn --inherit-scope S1`：B 继承 A 的 scope（默认只读）
- A 完成后 scope 冻结 → B 读取 A 的 env 数据
- Planner 只做调度，不搬运大数据

### LRU 淘汰 + 引用保护
- 配置 `max_persisted_scopes`
- 超限时淘汰 `last_used` 最旧的 scope
- **引用保护**：被 active/idle 子 scope 依赖的 parent 不可淘汰
- 淘汰前先把依赖它的子 scope 的 delta 展平为全量快照

### 生命周期
```
Active → Idle (队列空) → Persisted (TTL 到期，落盘)
                                    ↓
                              Archived (LRU 淘汰 or :scope close)
                                    ↓
                              Deleted (超出保留限制)
```

---

## 六、模式参数 `()` 语法

> v2 新增：用括号分离内建配置与被执行命令的参数，消除歧义。

```
:run(retry=3, timeout=30s) cargo test --release
:ask(model=gpt-4) explain this error
:cron(scope=S0@a3f1) every 5m cargo clippy
:spawn(agent=acp, role=executor) implement the next step
```

- `()` 紧跟命令名 = 模式参数（执行行为配置）
- `()` 出现在其他位置 = chain 分组括号
- Tokenizer 根据**位置规则**消歧（前一个 token 是 Command → 模式参数）
- 模式参数可在 `server.toml`（迁移期仍兼容旧 `config.toml`）中设置默认值，调用时覆盖

### 当前 AGENT backend 配置落地

当前实现先采用**配置选择 backend**，并内建 Copilot CLI 的 ACP 配置作为默认值。`cued`
优先读取 `server.toml`，迁移期仍兼容旧 `config.toml`：

```toml
[agent]
default_backend = "copilot"

[agent.backends.copilot]
command = "copilot"
args = ["--acp", "--stdio"]
```

- `:ask` 默认使用 planner 角色
- `:spawn` 默认使用 executor 角色
- `:ask(agent=copilot, model=...)` / `:spawn(agent=copilot, model=...)` 可覆盖默认 backend / model
- 当前只支持 ACP backend profile，不再额外区分 `kind`
- 当前 ACP 集成采用**多轮 session**：同一个 agent session 会在 `Running ↔ WaitingInput` 之间切换，而不是每轮 prompt 后立即退出

### 支持模式参数的命令

| 命令 | 可用参数 |
|------|---------|
| `:run()` | `retry`, `timeout`, `shell`, `env`, `scope` |
| `:ask()` | `model`, `temperature`, `max_tokens`, `agent`, `session` |
| `:cron()` | `label`, `scope` |
| `:spawn()` | `agent`, `role`, `inherit_scope`, `depth_limit`, `model`, `session` |
| `:scope new()` | `profile` |

其他命令只有位置/标志参数，无 `()` 语法。

---

## 七、操作符（两层模型）

### Pipeline（Job 内部的进程管道）

Pipeline 连接 **进程**，运行在同一个 Job 内部，类似 bash 的 `|`：

| 操作符 | 语义 | 说明 |
|--------|------|------|
| `\|>` | stdout 管道 | 前者 stdout → 后者 stdin |
| `\|&>` | stdout+stderr 管道 | 前者 stdout+stderr → 后者 stdin |
| `\|!>` | stderr 管道 | 前者仅 stderr → 后者 stdin |

### Chain（Job 间的编排）

Chain 连接 **Job**，由 Scheduler 调度执行：

| 操作符 | 语义 | 说明 |
|--------|------|------|
| `->` | 串行-成功继续 | 前者 exit 0 才执行后者 |
| `~>` | 串行-忽略结果 | 无论前者结果都执行后者 |
| `\|\|` | 并行-全部 | 同时发射所有分支 |
| `\|\|?` | 并行-竞速 | 任一成功即视为成功 |

### 优先级

```
pipe (1, 最高) > parallel (2) > serial (3, 最低)
```

### 解析示例

```
a |> b -> c || d ~> e
= (a |> b) -> (c || d) ~> e
= Job1(a|>b) -> (Job2(c) || Job3(d)) ~> Job4(e)

cargo build |> grep error -> cargo test || cargo clippy
= Job1(cargo build |> grep error) -> (Job2(cargo test) || Job3(cargo clippy))
```

### 关键语义

- Pipeline (`|>`) = **Job 内部**，共享同一 scope
- Chain (`->` `||`) = **Job 之间**，Scheduler 调度
- `(a -> b) |> c` **非法** — chain 输出不能作为管道输入
- `()` 在 chain 层面用于分组：`(a || b) -> c`

### Exit code 聚合

```
[0; 0; 0, 1]
 ^   ^   ^^^
 │   │   └── 并行步骤（逗号分隔）
 │   └────── 串行步骤（分号分隔）
 └────────── 串行步骤

Pipeline 内退出码 = 最后一个进程的退出码
```

### 重试

- `:run(retry=3) cargo test` → 失败时自动重试，最多 3 次
- 重试成功 → ChainAborted 的后续步骤自动重启
- `:retry J3` → 当前会用原 `start_scope` 和 pipeline 重新发射一个 fresh job
- downstream chain 续接 / 自动重启后继 leaf 仍未完成，语义已显式收窄，避免保留模糊 stub

---

## 八、命令速查表

```
┌── Job ────────────────────────────────────┐
│ :run <cmd>     发射 job                    │
│ :jobs          列出 job                    │
│ :wait <id>     等待完成                    │
│ :out <id>      查看 stdout snapshot        │
│ :tail <id>     follow stdout               │
│ :err <id>      查看 stderr snapshot        │
│ :send <id>     写入 stdin                  │
│ :kill <id>     终止 job                    │
│ :cancel <id>   取消排队 job                │
│ :fg <id>       前台（pty）                 │
│ :retry <id>    重试 failed job             │
│ :log [id]      查看 job 历史日志           │
│ :pause <id>    暂停 cron/agent             │
│ :resume <id>   恢复 cron/agent             │
├── Scope ──────────────────────────────────┤
│ :scope list    列出 scope                  │
│ :scope new     创建 scope                  │
│ :scope env     查看 scope env              │
│ :scope fork    派生 scope（delta）          │
│ :scope close   归档 scope                  │
│ :scopes        列出所有 scope（简写）       │
│ :cd <path>     修改默认 scope 的 cwd       │
├── Agent ──────────────────────────────────┤
│ :ask <prompt>  用户→Planner                │
│ :spawn <plan>  Planner→Executor            │
│ :agents        列出 agent                  │
│ :confirm <msg> Planner→用户确认            │
│ :escalate <msg> Executor→Planner 上报      │
│ :probe <sub>   Planner 轻量窥探（4KB 限）  │
├── Cron ───────────────────────────────────┤
│ :cron <sched> <cmd>  添加定时/延迟任务     │
│ :crons         列出定时任务                │
│   内部关键字:                              │
│   every 5m / at 9am / on weekdays         │
│   in 5m / daily / cron */5 * * * *        │
│   <free> do <cmd>   (通用回退)             │
├── General ────────────────────────────────┤
│ :env           查看 env                    │
│ :env set K=V   设置 env                    │
│ :help [cmd]    帮助                        │
│ ?              当前 mode 详细帮助          │
│ :config [sub]  查看/修改配置               │
│ :clear         清空 REPL 区域              │
│ :quit          退出 TUI                    │
└───────────────────────────────────────────┘

总计：28+ 内建命令（含 scope/cron 子命令）
```
