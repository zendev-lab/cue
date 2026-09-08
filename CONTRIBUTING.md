# 贡献指南

本仓库维护 Cue 实现与功能提案。协作规则见 [AGENTS.md](AGENTS.md)，提案流程以
[FP-0000](fps/FP-0000-governance.md) 为准。

## 设计变更

改变公开契约前使用 [FP 模板](templates/fp.md) 提交独立提案 PR。候选直接放在
`fps/`；不增加单独的草案目录或采纳状态。实现 PR 关联对应提案，并分别提供验证证据。

提案标题及正文使用中文，PR 标题使用英文、正文沿用中文模板。代码标识与 zendev
语法按原样保留，具体写作规则见 AGENTS.md。

## 本地准备

从独立 worktree 开始工作，不迁移其他任务的未提交内容。Rust 版本和依赖以
`rust-toolchain.toml`、Cargo manifest 与锁文件为准；检查入口在 `justfile`。

安装仓库现有 hooks：

```shell
uvx prek install --hook-type pre-commit --hook-type commit-msg
```

变更提案或元数据后，直接使用最新版工具生成并检查索引：

```shell
uvx zendev-proposal check --fix
uvx zendev-proposal check
```

仓库 hooks 使用 `prek.toml` 中的 v0.4.0，提案索引采用 v2 格式，以整数编号表示身份
和关系，不额外生成格式化 id。流程、schema 和索引配置以 `proposal.toml` 与
`schemas/fp.schema.json` 为准；不引入另一份验证脚本或状态文件。

## 验证

只修改文档时，对改动文件运行 `uvx prek run --files <文件列表>`，并检查
`git diff --check`。提案检查、链接和拼写均应通过；不把未来的功能验收写成已运行测试。

实现变更按 [测试规范](docs/testing.md) 验证所影响的行为。仓库完整检查入口为：

```shell
just ci
uvx prek run --all-files
```

完整检查覆盖架构、格式、clippy、测试、MSRV 和发行产物。CLI 与 daemon 变更需在隔离
socket/数据库上验证真实命令与故障路径；不要用工作中的用户 daemon 做破坏性测试。

## 提交与 PR

提交和 PR 遵循 zendev profile，保留 emoji、type 和可选 scope。PR 标题使用英文：

```text
📝 docs(fp): propose execution-level resource management
📝 docs(repo): add collaboration and contribution guides
🐛 fix(client): correct wait command exit codes
```

提案 PR 新增、修订、替代分别使用 `propose`、`revise`、`supersede`，不表示生命周期
状态。提交描述继续使用中文。
使用 [仓库 PR 模板](.github/pull_request_template.md)，保留所需章节，说明问题、最终
变更、实际验证及后续工作。默认以 Draft PR 交付，CI、合并和发布状态分别报告。
