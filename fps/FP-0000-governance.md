---
fp: 0
title: "Cue 功能提案治理"
type: Governance
authors:
  - "zrr1999"
created: 2026-08-31
supersedes: []
---

# FP-0000: Cue 功能提案治理

## 摘要

每项改变 Cue 公开契约的功能都从一份轻量 Feature Proposal（FP）开始。FP 保存
提案文本，Git 和对应的 pull request 记录讨论与变更；FP 不编码提案是否采纳或功能
是否实现。

## 动机

Cue 的 CLI、IPC、语言、持久化和执行语义共同构成跨进程、跨版本的公开契约。短小
而持久的提案能让这些边界保持可评审，同时不引入数据库、审批服务或额外的状态流程。

## 设计

新增或改变 CLI 与配置、IPC 或数据格式、Cue 语言语法、持久化与迁移语义、发行包
边界、daemon 拥有的状态，以及用户可见的执行、PTY、调度、资源或安全行为时，必须
先提交 FP。恢复既有契约的缺陷修复、测试、文档、依赖维护和不改变公开行为的内部
重构不需要 FP；无法确定时，优先撰写短提案。

候选 FP 直接在 `fps/` 下分配编号，并通过只包含提案的 pull request 评审。仓库不
维护 `drafts/` 目录。合并只表示提案文本进入版本库，不代表采纳、排期或实现；未合并
的候选保留在关闭的 pull request 中。实现 pull request 关联对应 FP，并独立评审和
合入。

`Feature` FP 描述公开功能，`Governance` FP 描述 Cue 自身的提案、发布或协作规则。
FP 默认使用中文正文，但作者可以根据读者选择英文；编号、类型、代码、命令和技术
标识保持英文。schema 和校验器不检查自然语言，也不维护 `language` 字段。

FP pull request 复用仓库统一中文模板和 `zendev` title profile。新提案、修订和
替代分别使用 `propose`、`revise` 和 `supersede`；这些动词帮助人类识别变更意图，
不构成机器状态或合并门禁。

提案永久保留在 Git 中。后续提案替代旧提案时，在新文档的 `supersedes` 中引用旧
提案；索引据此派生 `superseded_by`。不改变提案含义的编辑修订可以直接更新原记录。

## 兼容性

本治理规则只适用于 FP-0000 之后提出的功能。现有的 IPC v3、语言、存储和发行契约
不需要追溯补写 FP。FP 不取代当前行为文档或源代码：实现后的公开行为仍由代码、严格
schema 和对应文档共同定义。

## 验证

固定版本的 `zendev-proposal check` 校验当前 FP 元数据、模板章节、关系和确定性索引。
仓库的 prek/CI 运行相同检查，但不会从 Git 历史、实现 diff 或 pull request 文本推断
提案状态与采纳结果。
