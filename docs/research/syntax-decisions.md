# Cue Shell 语法决策记录

> 本文档合并了前缀语法调研、cron 语法设计和模式转换设计的研究过程与决策理由。
> 最终决策已体现在 [commands-and-modes.md](../commands-and-modes.md) 中，本文档保留研究过程供参考。

---

## Part 1: 前缀语法研究与决策

### 1.1 调研范围

跨 4 个领域调研了 20+ 工具的命令前缀设计：Chat/协作工具、TUI 工具、REPL/Shell、AI Chat 接口。

### 1.2 跨领域调研总结

| 领域 | 主流前缀 | 核心发现 |
|------|---------|---------|
| **Chat 工具** (Slack/Discord/IRC) | `/` | 绝对统治性约定。IRC 1993 年确立先例，所有后来者跟随 |
| **TUI 工具** (Vim/tmux/Zellij) | `:` (Vim), `Ctrl-<key>` (tmux) | `:` 是 Vim 系的事实标准；tmux 用修饰键避免与内部程序冲突 |
| **REPL/Shell** (IPython/GHCi/psql/SQLite) | `%` / `:` / `\` / `.` | 核心原则：前缀字符不能与主语言的合法起始字符冲突 |
| **AI Chat** (ChatGPT/Cursor/Copilot/Aider) | `/` + `@` (引用) | `/` 再次主流；`@` 正成为"引用/寻址"的新标准 |

### 1.3 七候选方案评分

评分维度：击键成本、消歧能力、心智模型、跨模式一致性、Agent 友好度（各 1-5 分）。

| 排名 | 方案 | 综合 | 核心优势 | 核心风险 |
|------|------|------|---------|---------|
| 🥇 | **`:` (colon) 前缀** | **4.4** | 消歧力最强 + 跨模式零冲突 + TUI 用户群体熟悉 | 需要 Shift |
| 🥈 | **`/` (slash) 前缀** | **4.0** | 心智模型最强（全行业共识） | JOB 模式路径冲突 |
| 🥉 | **`cmd: args` (v1 设计)** | **3.8** | 消歧极强 + 独特性 | 无先例，学习成本 |
| 4 | `!` (bang) | 3.4 | 消歧好 | 语义反转 + 击键贵 |
| 4 | `@` (at) | 3.4 | 消歧好 | 语义模糊（命令 vs 引用） |
| 6 | `\` (backslash) | 3.0 | 免 Shift | 转义冲突，Agent 不友好 |
| 7 | `.` (dot) | 2.8 | 击键最低 | `./` 灾难性冲突 |

### 1.4 歧义/冲突矩阵

| 字符 | JOB 模式冲突 | AGENT 模式冲突 | CRON 模式冲突 | 冲突风险 |
|------|------------|--------------|--------------|---------|
| `/` | ⚠️ 中（`/usr/bin/...`） | 低 | 低 | **中** |
| `:` | 低 | 低 | 低 | **低** |
| `\` | ⚠️ 中（转义字符） | 低 | 低 | **中** |
| `.` | ⚠️ **高**（`./script.sh`） | 低 | 低 | **高** |
| `!` | 低 | 低 | 低 | **低** |

### 1.5 决策：`:` 前缀

**选择 `:` 的理由**：

1. **消歧能力最强**：在所有 3 种模式下，用户正常输入都不以 `:` 开头。解析器 100% 确定性。
2. **目标用户适配**：Cue Shell 目标用户是终端重度用户和开发者，与 Vim/Neovim 用户高度重叠。`:w`, `:q`, `:help` 是肌肉记忆。
3. **TUI 一致性**：作为 TUI 应用，`:` 与 Vim/Helix/lazygit 的命令行约定一致。
4. **Agent 友好**：`:` 不是任何编程语言/标记语言的转义字符，Agent 生成 `:command arg` 不会遇到转义问题。
5. **Shift 键成本可接受**：目标用户已对 Shift+`;` → `:` 建立条件反射。且 TUI 提供补全。

**排除 `/` 的原因**：JOB 模式下与绝对路径 `/usr/bin/...` 冲突，需要上下文依赖的消歧逻辑，违反 KISS 原则。

### 1.6 前缀设计的底层原则

| 原则 | 解释 | 对 Cue Shell 的应用 |
|------|------|-------------------|
| **正交性** | 前缀字符不能是主语言/主内容的合法起始字符 | 排除 `.`（`./`）和 `\`（转义），指向 `:` 或 `/` |
| **人体工学** | 高频操作的击键成本必须低 | `:` 对目标用户群差距很小 |
| **文化惯性** | 后来者应尊重已建立的约定 | 作为 TUI，`:` 的文化归属（Vim 系）比 `/`（Chat 系）更匹配 |

---

## Part 2: Cron 语法研究与决策

### 2.1 问题陈述

`:cron <schedule-spec> <command...>` 需要解决的核心问题：**解析器必须能确定性地判断 schedule 在哪里结束、command 从哪里开始**。

约束：无引号包裹、无 `--` 分隔符、自文档化、最小覆盖（间隔/指定时间/crontab 表达式）。

### 2.2 现有工具调研

| 类别 | 工具 | 分隔机制 | 问题 |
|------|------|----------|------|
| 传统 crontab | crontab, K8s CronJob | 固定字段数 / 引号隔离 | 引号不可接受，字段计数脆弱 |
| 结构化 | systemd, APScheduler, Celery Beat | 命名参数 / 结构化 JSON | 太 verbose |
| 流式/自然语言 | gocron, every(Python), Ofelia | `.Do()` / `@` 前缀 | `do` 边界和 `@` 前缀在多工具中独立涌现 |

**核心发现**：
- `.do()` / `do` 作为 schedule↔action 边界是跨工具涌现的模式
- `@` 前缀标记调度 token 是另一个涌现模式
- 关键字驱动的确定性语法在 systemd/APScheduler 中验证过
- duration 字面量无空格（`5m`, `1h30m`）是事实标准

### 2.3 三种候选方案

#### 方案 A: 单 Token 调度（Zero-Ambiguity Compact）

```
5m cargo build                    # interval
@2:30pm ./deploy.sh              # timepoint
cron:*/5:*:*:*:* curl api/health # crontab (用 : 代替空格)
weekdays@9am cargo test          # complex
```

规则：`token[0]` = schedule，`token[1:]` = command。O(1) 解析。

| 维度 | 评分 |
|------|------|
| 解析确定性 | ⭐⭐⭐⭐⭐ |
| 输入效率 | ⭐⭐⭐⭐⭐ |
| 可读性 | ⭐⭐⭐ |
| 可扩展性 | ⭐⭐ |

#### 方案 B: 关键字文法驱动（Keyword-Grammar Driven）

```
every 5m cargo build                    # 关键字 "every" 消耗 1 token
at 2:30pm ./deploy.sh                  # 关键字 "at" 消耗 1 token
at 9am on weekdays cargo test          # "at" + lookahead "on"
cron */5 * * * * curl api/health       # 关键字 "cron" 消耗 5 token
daily cargo clippy                     # 预设关键字，消耗 0 token
```

| 维度 | 评分 |
|------|------|
| 解析确定性 | ⭐⭐⭐⭐ |
| 输入效率 | ⭐⭐⭐⭐ |
| 可读性 | ⭐⭐⭐⭐⭐ |
| 可扩展性 | ⭐⭐⭐⭐ |

#### 方案 C: `do` 终结符（Natural-Language Terminated）

```
every 5m do cargo build                # "do" 分界
*/5 * * * * do curl api/health        # 原生 crontab + do
every 30m between 9am-5pm on mon-fri do ./check.sh  # 复杂自由格式
```

| 维度 | 评分 |
|------|------|
| 解析确定性 | ⭐⭐⭐⭐⭐ |
| 输入效率 | ⭐⭐⭐ |
| 可读性 | ⭐⭐⭐⭐⭐ |
| 可扩展性 | ⭐⭐⭐⭐⭐ |

### 2.4 同场景并排对比

| 场景 | A (单Token) | B (关键字) | C (do 终结) |
|------|------------|-----------|------------|
| 每 5 分钟 | `5m cargo build` | `every 5m cargo build` | `every 5m do cargo build` |
| 下午 2:30 | `@2:30pm ./deploy.sh` | `at 2:30pm ./deploy.sh` | `at 2:30pm do ./deploy.sh` |
| Crontab | `cron:*/5:*:*:*:* curl api` | `cron */5 * * * * curl api` | `*/5 * * * * do curl api` |
| 工作日 9am | `weekdays@9am cargo test` | `at 9am on weekdays cargo test` | `every weekday at 9am do cargo test` |

### 2.5 决策：方案 B + C 混合

**不是三选一，而是 B+C 融合。**

> **关键字模式覆盖 90% 的日常场景（简洁高效），`do` 终结符作为通用回退覆盖剩余 10%（完备兜底）。**

```
<cron-input> := <keyword-schedule> <command>    # 主路径：关键字驱动
              | <free-schedule> "do" <command>   # 回退：do 终结符
```

**选择理由**：

1. **日常场景零冗余**：`every 5m cargo build`，无 `do`，简洁如自然语言
2. **复杂场景有兜底**：`*/5 * * * * do curl api`，直接写 crontab 加 `do`
3. **可扩展性最大化**：新调度语法可扩展关键字规则，或用 `do` 回退
4. **与 shell 传统一致**：`do` 与 Bash `for ... do ... done` 语义一致
5. **保留字冲突最小化**：关键字仅在 CRON 模式生效，`do` 是唯一回退保留字

**实现优先级**：

| 阶段 | 实现内容 |
|------|---------|
| v0.1 | `every <duration>` + `do` 回退 |
| v0.2 | `at <time>` + `cron <5f>` + presets |
| v0.3 | `on <days> at <time>` + `at <time> on <days>` |
| v1.0 | `between`/`except`/`until` 等扩展 |

---

## Part 3: 模式设计演进（v1 → v2）

> 本文只保留最终留下来的语法 / 模式演进结论；已经整体迁出到 weft
> 的 AGENT bridge 试验不再作为 cue-shell 当前文档的一部分。

### 3.1 v1 设计（已弃用）

v1 使用 **3 个模式** + **`cmd:` 分隔符语法**：

| 模式 | 含义 |
|------|------|
| CMD | 仅执行内建命令 |
| JOB | 自动包装为 `run` |
| SCHED | 定时调度 |

内建命令使用后缀冒号：`kill: J1`, `run: cargo build`, `env: set FOO=bar`。

解析规则：
1. 如果第一个 token 以 `:` 结尾 → 内建命令
2. 如果单个 token 精确匹配无参命令 → 内建命令
3. 否则 → 模式默认包装（CMD 模式报错）

**v1 的问题**：
- `cmd:` 语法无先例，需要用户"学习"新语法
- CMD 模式与 `:` 前缀功能重复
- `echo "foo:bar"` 可能被误判为内建命令（需额外检测逻辑）
- SCHED 命名与 `:cron` 不一致

### 3.2 v2 设计（当前）

v2 改为 **2 个模式** + **`:` 前缀语法**：

| 模式 | 默认包装 |
|------|---------|
| JOB ⚡ | → `:run <input>` |
| CRON ⏰ | → `:cron <input>` |

内建命令使用前缀冒号：`:kill J1`, `:run cargo build`, `:env set FOO=bar`。

解析规则（极简化）：
1. 首字符 `:` → 内建命令
2. 否则 → 模式默认包装

### 3.3 v1→v2 变更理由

| 变更 | 理由 |
|------|------|
| `cmd:` → `:cmd` | TUI 文化一致（Vim）；首字符判断 vs 扫描冒号位置更简单；消除 `foo:bar` 误判 |
| 去掉 CMD 模式 | `:` 前缀在任何模式下都能触发内建命令，CMD 模式冗余 |
| SCHED → CRON | 与 `:cron` 命令命名一致 |

### 3.4 解析器对比

**v1** — 需要扫描 `:` 位置、检查是否在引号内、验证前缀是否为纯字母：

```rust
// v1: 复杂
fn try_extract_builtin(input: &str) -> Option<(&str, &str)> {
    if let Some((prefix, rest)) = input.split_once(':') {
        let cmd = prefix.trim();
        if cmd.chars().all(|c| c.is_ascii_alphabetic())
           && BUILTINS_WITH_ARGS.contains(cmd) {
            return Some((cmd, rest.trim()));
        }
    }
    None
}
```

**v2** — 首字符判断，零歧义：

```rust
// v2: 极简
if input.starts_with(':') {
    let rest = input[1..].trim_start();
    let (cmd, args) = split_first_word(rest);
    // ...
}
```

v2 解析器不可能误判——输入要么以 `:` 开头（内建），要么不是（模式默认）。没有引号问题、没有冒号位置问题、没有纯字母验证。

---

## 附录 A：与 APScheduler 触发器类型的映射

| APScheduler 触发器 | Cue Shell 语法 | 示例 |
|-------------------|---------------|------|
| `interval` | `every <duration>` | `:cron every 5m cargo test` |
| `cron` | `cron <5f>` 或 `<5f> do` | `:cron cron 0 9 * * 1-5 cargo test` |
| `date` (one-shot) | `in <duration>` / `at <datetime>` | `:cron in 5m ./release.sh` |

## 附录 B：跨键盘布局的 `:` vs `/` 成本

| 布局 | `/` | `:` |
|------|-----|-----|
| US QWERTY | 无 Shift | Shift |
| UK QWERTY | 无 Shift | Shift |
| German QWERTZ | Shift (7键) | Shift |
| French AZERTY | Shift | 无 Shift |
| JIS (日语) | 无 Shift | Shift |

> 终端用户几乎都使用 US QWERTY 或本地化类似布局。对于 `:` vs `/`，差异最小。

---

*本文档合并自前缀调研（2025-07）、cron 语法设计（2025-07）和模式转换设计文档。保留研究过程供设计决策溯源。*
