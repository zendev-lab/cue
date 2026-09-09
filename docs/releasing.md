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

## Workflow 职责

- `cd-release.yml`：自动准备 Release PR，并在合并后打 tag；`release-plz release`
  是工具的命令名，`git_only = true` 和 `publish = false` 禁止它上传 registry。
- `build-packages.yml`：通过 GitHub 原生 `workflow_call` 供 CI 和 Publish 共用，
  执行 Cargo 打包、wheel/sdist/npm 构建与安装 smoke，并上传产物。
- `ci-package-smoke.yml`：在 PR、main 和 merge queue 中调用相同的构建流程。
- `cd-publish.yml`：校验 tag，调用构建流程，随后上传三个 registry 并创建 GitHub Release。
- `ci-static-checks.yml`、`ci-tests.yml`、`policy-pr.yml`：分别负责静态检查、测试及 PR 格式。

版本检查直接使用 `cargo metadata` 和 `jq -e` 断言 tag 与全部 crate 的版本一致。
`cargo package` 验证的是包能否构建，不能替代仓库的 tag 命名约定。

## Normal releases

1. Merging development changes into `main` updates the release-plz PR.
2. Review its version, breaking changes and CI, then merge it manually.
3. `cd-release.yml` uses release-plz's `git_only` mode to create tags, without
   publishing packages. `cue-cli` owns the product `v<version>` tag; the other
   crates use `<crate>-v<version>` tags as their release-plz version baselines.
4. The product tag triggers `cd-publish.yml`: Cargo publishes Rust crates,
   Maturin builds the Python distributions for `uv publish`, and npm publishes
   the Skill package. GitHub Release creation waits for all three publishers.

The `release` and `release-pr` jobs are independent. Only `release-pr` has a
shared concurrency group, so a later main commit cannot cancel a pending release.

The repository's `💥 breaking:` commit prefix requests a minor bump in 0.x,
including protocol/CLI incompatibilities that Rust API checks cannot detect.

There is no `just release` command. Do not edit a product tag to retry a failed
release. Rerun failed jobs on the original run/commit. `uv publish` skips identical
PyPI files and rejects different bytes; reuse the original build artifacts.
Cargo handles dependency ordering and index availability. A small registry check
selects versions not yet published, allowing retries after a partial Rust upload
and the initial account-token bootstrap. Yanked versions and lookup errors fail.
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
reviewed release commit, publish the workspace using an account token:

```sh
cargo publish --workspace --registry crates-io --locked
```

Cargo publishes in dependency order and waits for registry availability. If the
command partially succeeds, retry with `-p <crate>` for the remaining packages.
Configure each crate's Trusted Publisher for `cd-publish.yml` and `crates-release`,
then let release-plz create the release tags from the reviewed release commit.
Never publish from a dirty checkout or from the old v0.1.2 checkout.

Before publishing, run `just ci` and `just crate-package-smoke`.
`cargo package --workspace` uses a temporary registry to verify unpublished
workspace dependencies. Run this check with a clean Cargo configuration;
a crates.io source replacement may bypass that temporary registry and produce
misleading missing-package errors.
