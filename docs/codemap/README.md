# 交互式代码地图

Open [codemap.html](codemap.html) directly in a JavaScript-enabled browser after
cloning the repository. It embeds its own data, makes no fetch requests and needs
no daemon, build, network connection or third-party library. When viewing this
repository on GitHub, download/clone it first: GitHub displays HTML source rather
than running the map.

## 阅读地图

1. Start with a workflow: execute a file, observe/cancel, attach to a PTY, or
   start/recover. The numbered steps are control/data flow, not a dependency DAG.
2. Select a module. The detail panel shows its responsibility, callers,
   dependencies, downstream consequences and evidence. Use Tab and Enter/Space
   for module and workflow buttons. On narrow screens, details follow the list.
3. Switch **Relationship lens** to distinguish runtime data flow from production
   Cargo dependencies. The latter excludes dev-dependencies. An upstream caller
   may also depend on a contract crate; those relationships have different meanings.
4. Expand source or test evidence. When opened as a file, local links open the working copy; **baseline
   source** links open the recorded commit on GitHub. Hosted pages show baseline
   links only, because a documentation host does not contain repository source.
   Uncommitted changes can differ from these baseline links. Paths and SHA-256 hashes are
   checked independently, so stale local files cannot silently pass validation.

The initial map contains fifteen primary nodes: nine crates, the SQLite store,
POSIX facilities, three dependency groups and distribution. Each crate groups
its lower-level files. External groups intentionally omit minor libraries; the
complete dependency/version inventory remains in `Cargo.toml` and `Cargo.lock`.
There is no remote service or general scheduling engine in this kernel.

## 定位责任模块

| Problem | Start here | Evidence to check next |
| --- | --- | --- |
| A CLI command dispatches to the wrong executable | `cue-cli` | `main.rs`, `companion_binary.rs`, process tests |
| A surface expression becomes the wrong plan | `cue-language` | compiler and parser tests; Core plan contract |
| Wrong success, failure, cancellation or Scope propagation | `cue-core` | reducer transition tests |
| Request correlation, attach or wait fails | `cue-client` / `cue-protocol` | framing and multiplexing tests |
| Accepted execution stops progressing | `cue-daemon` | service regressions, durable runtime-work generations |
| Physical process, pipeline, PTY or output cleanup fails | `cue-runtime` | runner tests, process-level lifecycle tests |
| Facts, operation replay or recovery differ after restart | `cue-store-sqlite` / `cue-daemon` | atomic store tests and recovery admission |
| Installed commands or exported Skill files are missing | distribution | wheel/sdist and npm smoke scripts |

See [design](../design/README.md) for the implementation narrative,
[testing](../testing.md) for executable gates, and
[FP-0001](../../fps/FP-0001-structured-execution-kernel.md) for normative contracts.
The map does not supersede those contracts.

## 生成与维护

From the repository root, with Python 3.11 or newer and Git available:

```sh
python3 scripts/codemap.py check
```

The check resolves every module/file/test/edge/workflow reference, validates test
symbols and declaration lines, checks complete crate/file/test inventory and production dependency edges against Cargo
manifests, checks the baseline commit, verifies source/test
SHA-256 hashes and checks that embedded HTML equals the JSON plus template. This
is a documentation integrity check, **not** a Rust test run or a semantic proof of
an arrow. Relationship descriptions still require source review.

After changing source:

1. Review affected nodes, edges and all four workflows against actual call sites.
   Update `codemap.json`, including test declaration line numbers and its baseline
   commit. Never infer runtime calls from filenames or crate dependency presence.
2. Edit `template.html` for presentation changes; do not hand-edit generated HTML.
3. Run `python3 scripts/codemap.py build`, then `python3 scripts/codemap.py check`.
   Build records current evidence hashes in `codemap.lock`; it does not claim that
   the working tree is identical to the baseline commit.
4. Open the HTML in a browser. Exercise all module buttons, both lenses, four
   workflows, search (including an empty result), evidence links and keyboard
   focus at desktop and narrow widths. Record any unavailable validation honestly.
5. Run the relevant behavior gates from [testing](../testing.md) if source changed.

The JSON is the editable semantic source. The lock records the immutable Git
baseline plus exact current evidence hashes; it deliberately does not hash itself.
Test declarations are source evidence only. A missing test is recorded as missing,
not replaced by an invented test name. Distribution uses smoke-script evidence
rather than Rust test declarations.

## 运行边界

- The daemon receives typed plans; it never parses Cue source.
- Sequence threads Scope. Parallel forks Scope and does not merge mutations.
- A committed operation response makes retries idempotent; it does not make a
  physical Run safe to replay after loss of ownership.
- Execution history and output-range facts survive restart; memory output bytes
  do not. Old empty output is not proof that a command produced no output.
- An unknown physical Run attempt can block daemon startup. There is no supported
  abandon/repair command; do not clear markers to force acceptance.
- PTY lease loss releases input control, not execution ownership. Disconnecting a
  client does not cancel its work.

## 浏览器验收

See [the validation record](VALIDATION.md) for tested viewports, real interactions,
repairs and limits. Map acceptance does not imply application or deployed-site
acceptance.
