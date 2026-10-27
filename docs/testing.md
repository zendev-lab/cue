# Test architecture

Cue tests protect observable product contracts at the closest owning boundary. Repository policy is checked by dedicated static tools, not by tests that mirror the current source tree.

## Lanes

| Lane | Location or command | Contract |
| --- | --- | --- |
| Crate unit and contract | `crates/*/src/**/*.rs` | Pure behavior, parsing, schemas, state transitions, and adapter contracts |
| Crate integration and process | `crates/*/tests/**/*.rs` | IPC, persistence, process lifecycle, PTY, and daemon/client composition |
| Package smoke | `scripts/smoke_package.sh` | Commands installed from the built wheel or source distribution |
| npm Skill package | `scripts/smoke_npm_package.mjs` | Packed file allowlist, version alignment, exported root, and canonical Skill bytes |
| Repository policy | `just pre-commit` | Formatting, lint, workflow syntax and security, links, spelling, and commit messages |

Keep Linux-only process behavior in the integration lane instead of replacing it with a mock that cannot exercise `/proc`, pidfd, signal, socket, or lock semantics. Keep package smoke separate from source execution: it must install the produced artifact and invoke the public command names.

Daemon process tests cover default background readiness, explicit foreground mode, log-visible startup failure, competing starts, independent database ownership, and stop/restart completion. CLI process tests cover surfaced failure reasons and retained-output truncation. Package smoke uses the default background command without a shell `&` or readiness retry, then verifies immediate use after restart. Its explicit `CUE_SOCKET` keeps inherited endpoints outside the test.

## Tests versus static policy

Code tests assert return values, state transitions, persisted effects, boundary calls, process exit status, output, and compatibility behavior. They do not read the repository's current Rust source, workflow YAML, manifests, Just recipes, or documentation and assert that selected text fragments still exist.

If a constraint is about repository shape, use the standard static tool that owns that format. If wording in a design document is not itself a public serialized contract, review or generate the document instead of mirroring its prose in a unit test. Use complete golden files only when the whole representation is the contract.

## 持续质量检查

`just quality` 使用 cargo-machete 检查未使用的直接依赖，并通过 cargo-deny 分别检查运行时与 fuzz 锁文件的漏洞。许可证目前只生成清单，不增加未经讨论的准入政策。公开 crate 的 cargo-semver-checks 以本次 PR 的基线提交比较导出 API；版本变化仍遵循 Cargo manifest。CodeQL 沿用仓库已有的 Rust、JavaScript 和 Actions 默认分析。

`just loom` 将输出存储的实际 Mutex 替换为 Loom 模型，遍历两个写入者的交错，检查偏移不重叠以及容量淘汰后的最终内容。`cue_loom` 只用于这一测试入口；普通构建继续使用标准库 Mutex。性质测试随原有 crate 测试运行，覆盖环境补丁序列化、协议帧身份与长度、输出保留后缀。

每周一及手动触发的 Quality Analysis 运行以下有界检查：

- `just mutation`：Core 环境补丁、协议 framing 与运行时输出存储。使用库单元测试，并跳过 runner 进程测试，避免变异引发的存储 panic 让 PTY 任务挂住；完整进程测试仍由原生测试入口运行。保留完整报告，存活变异作为待审阅证据；只把 cargo-mutants 的存活状态码 2 转为报告结果，基线失败和工具错误仍失败。该目标不代表 reducer、SQLite 或全部进程代码的变异覆盖。
- `just fuzz`：固定 nightly 与 libFuzzer，对任意 IPC 帧和 UTF-8 命令运行 60 秒 AddressSanitizer 检查；保存 corpus 和崩溃样例。
- `just miri`：保持 Miri 隔离，解释执行 Core 和协议测试。性质测试使用固定种子及 8 个案例；16 MiB 编码边界由普通测试运行，避免解释器重复处理大分配。原生进程、PTY 和系统调用继续由真实进程测试负责。
- `just reports`：jscpd 重复代码与 Lizard 复杂度报告，不设置未经基线支持的阻塞分数。
- `just links`：检查当前文档的外部链接；历史研究笔记保留原始引用。离线相对链接仍在 prek 中检查。

`just bench` 测量真实 Hello 帧的编码和解码，并先验证往返结果。普通执行产生本机 walltime，CI 的 CodSpeed CPU 模拟提供云端测量；没有兼容基线时不推断性能提升或退化。

Codecov 上传失败会使测试任务失败，LCOV 仍保留为构建产物。Sonar 使用独立 CI 分析导入 Rust 和 LCOV，需要仓库或组织配置 `SONAR_TOKEN`；自动分析不支持 Rust。配置提交、上传成功和质量门禁通过分别验收。

## Gates

- `just check` runs Rust formatting and Clippy on the repository-pinned primary toolchain.
- `just test` runs the workspace behavior suite.
- `just msrv` compiles all targets with Rust 1.95 through rustup.
- `just package-smoke` builds wheel and source distributions and exercises every installed command.
- `just npm-package-smoke` packs and installs `@zendev-lab/cue`, then verifies the canonical Skill.
- `just ci` is the serial local aggregate of those gates; hosted CI keeps independent lanes parallel.

The primary toolchain is pinned in `rust-toolchain.toml` and advances through a dedicated Renovate change instead of making unrelated pull requests absorb a new stable compiler implicitly. Hosted CI also runs the Rust 1.95 MSRV check as its own job, so primary-toolchain and compatibility failures remain attributable to the lane that owns them.

When reviewing a test, ask what observable regression becomes invisible if it is removed, whether a wording-only refactor would break it, whether fail-closed and recovery behavior have negative paths, and whether the test runs at the boundary that owns the contract.
