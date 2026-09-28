# Releasing Cue

Cargo's workspace version is the product version. Maturin reads it for
`cue-run`; `npm run pack` generates the npm manifest from Cargo metadata in
a temporary directory. The source `package.json` is private and has no version. All nine Rust
crates share the `cue` release-plz version group. Every product release bumps and
publishes all nine crates at the same version, including crates whose source code
has not changed. The version identifies the complete Cue release; it does not
imply that every component gained a feature or a fix. Internal dependency versions
advance with the workspace version.

Publishing does not filter crates by source changes. It only skips crate versions
already uploaded successfully, so a partial publish can be retried.

## Workflow 职责与触发

`cd-release.yml` 只订阅 `main` 的 push，包含两个 job：

- `release-pr` 运行 `release-plz release-pr`，有待发布变化时自动创建或更新 Release PR。
- `create-tags` 运行 `release-plz release`。`release_always = false` 使其查询当前 commit
  关联的 PR，只有关联 PR 的分支以 `release-plz-` 开头时才继续；普通 PR 合并后跳过打 tag。
  这是工具原生的判断，不解析 commit message，也不额外监听 PR closed 事件。

`release-pr` 等待 `create-tags` 完成后才运行。否则两个 job 可能并行 checkout 同一个
刚合并的 release commit，而 `release-pr` 尚未看到新 tag 时会把这次 release 再计算成下一
个版本，产生没有实际变更的连续 Release PR。

审核并合并 Release PR 后，合并产生的 main push 走同一个 workflow。配置中的
`git_only = true`、`publish = false` 和 `git_release_enable = false` 让它只创建 Git tag。
release-plz 只为 `cue-cli` 创建产品 `v<version>` tag；其他 crate 不创建独立 tag。
九个 crate 仍在同一 version group 中更新和发布，产品 tag 是统一 release 的唯一 Git 基线。
GitHub App token 使推送 tag 能继续触发其他 workflow。

```text
普通 PR 合并 → push main → 自动创建/更新 Release PR
                               ↓ 审核并合并
                          push main → release-plz 识别关联的 Release PR
                                          ↓
                                       push tag v*
                                          ↓
                                    cd-publish.yml
                                          ↓
                          构建与安装 smoke ───────────────→ GitHub Release
                               ├→ 上传 PyPI
                               ├→ 上传 npm
                               └→ 上传 crates.io
```

`cd-publish.yml` 只订阅 `v*` tag push，直接包含版本检查、产物构建、安装 smoke、
PyPI/npm/crates.io 上传，以及最后的 GitHub Release。crates.io 使用 release-plz
的原生 `release` 命令，查询并只发布尚未上传的 workspace crate 版本；配置文件
`release-plz-publish.toml` 关闭 tag 和 GitHub Release，避免再次创建 release tag。
PyPI 使用 PyPA 官方发布 Action，
保留 Trusted Publishing 和默认的 PEP 740 发布证明。上传 jobs 与构建 jobs 分离，
仅上传 jobs 获得对应 registry 的 OIDC 权限。GitHub Release 只依赖版本检查、打包和
smoke 检查；三个 registry 上传相互独立，某个 registry 暂时失败时不会阻止 GitHub
Release 创建或其他 registry 的重试。

`ci-package-smoke.yml` 直接运行 PR 阶段的 Cargo 打包和 wheel/sdist/npm 安装 smoke，
提前发现发行包缺文件、无法安装或启动等问题。它不上传 registry，也不调用共用 workflow。
`ci-static-checks.yml` 和 `ci-tests.yml` 分别负责静态检查与代码测试。
`policy-pr.yml` 只监听 PR 事件，检查 PR 格式。

版本检查直接使用 `cargo metadata` 和 `jq -e` 断言 tag 与全部 crate 的版本一致。
`cargo package` 验证的是包能否构建，不能替代仓库的 tag 命名约定。

`create-tags` 与 `release-pr` 通过 job 依赖串行运行。只有 `release-pr` 使用共享并发组，
避免后续 main push 取消等待中的打 tag job。

The repository's `💥 breaking:` commit prefix requests a minor bump in 0.x,
including protocol/CLI incompatibilities that Rust API checks cannot detect.

There is no `just release` command. Do not edit a product tag to retry a failed
release. Rerun failed jobs on the original run/commit and reuse the original build
artifacts. The PyPA Action uses `skip-existing` to tolerate files already uploaded;
this is duplicate-upload handling, not a content-equality check. Do not use it to
replace existing files with a rebuild.
release-plz handles Cargo dependency ordering, index availability, and already-published
versions, allowing retries after a partial Rust upload. Its Trusted Publishing flow is
used after each crate has completed its first account-token bootstrap. A newly created
crate still needs that one-time manual publish; yanked versions and registry errors
remain publish failures.
For npm, rerun only failed jobs so successful uploads are not repeated; npm
rejects attempts to overwrite an existing version.

Keep upgrade notes for a breaking release in `docs/releases/<version>.md`;
when present, the publishing workflow includes them in the GitHub Release
alongside GitHub's generated change list.

## Credentials and publisher identities

Install the private `zendev-cue-release` GitHub App only on `zendev-lab/cue`, with Contents and
Pull requests read/write. Actions use `RELEASE_APP_ID` (repository variable)
and `RELEASE_APP_PRIVATE_KEY` (repository secret) to obtain short-lived
installation tokens. These tokens allow bot PRs and product tags to trigger CI.
Release PRs use the default release-plz title/body. Only the title/body policy
exempts this bot (and Renovate); build and test checks still run.

| Registry | Project | Workflow | GitHub environment |
| --- | --- | --- | --- |
| crates.io | Each of the nine workspace crate names | `cd-publish.yml` | `crates-release` |
| PyPI | `cue-run` | `cd-publish.yml` | `pypi-release` |
| npm | `@zendev-lab/cue` | `cd-publish.yml` | `npm-release` |

All publisher identities use owner `zendev-lab`, repository `cue`. No long-lived
registry token is stored in Actions. Configure the Trusted Publisher on the existing PyPI project `cue-run`.

## First crates.io release

crates.io requires the first release of a new crate to use an account token;
Trusted Publishing can be configured after the crate exists. From the fixed,
reviewed release commit, bootstrap any new crate with an account token:

```sh
cargo publish --workspace --registry crates-io --locked
```

Cargo publishes in dependency order and waits for registry availability. If the
command partially succeeds, retry the remaining packages with `-p <crate>`.
Configure each crate's Trusted Publisher for `cd-publish.yml` and `crates-release`.
After that bootstrap, the tag-triggered publish job runs release-plz and handles
partial retries without a repository-maintained registry lookup script.
Never publish from a dirty checkout or from the old v0.1.2 checkout.

Before publishing, run `just ci` and `just crate-package-smoke`.
`cargo package --workspace` uses a temporary registry to verify unpublished
workspace dependencies. Run this check with a clean Cargo configuration;
a crates.io source replacement may bypass that temporary registry and produce
misleading missing-package errors.
