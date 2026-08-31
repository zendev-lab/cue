# Feature Proposals

FP 是记录 Cue 公开功能与治理设计的轻量、持久提案。提出或实现改变公开契约的功能
前，先阅读 [FP-0000](./FP-0000-governance.md)。

候选 FP 直接作为 `fps/` 下带编号文档的 pull request 提交；仓库不维护单独的
`drafts/` 目录。FP 不编码采纳或实现状态，合并只表示提案文本进入版本库。未合并的
候选保留在关闭的 pull request 中；实现 pull request 关联对应 FP，并独立评审和
合入。

FP pull request 复用仓库统一中文模板和 `zendev` title profile。title 使用
`docs(fp)` scope，并以 `propose`、`revise` 或 `supersede` 表达本次文档变更；这些
动词是评审惯例，不是机器状态。

提交的[索引](../fps-index.json)由提案 frontmatter 确定性生成，可用以下命令检查：

```console
$ just proposal-check
$ just proposal-index-check
```
