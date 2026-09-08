# Releasing Cue

Cargo's workspace version is the product version. Maturin reads it for
`cue-run`; `npm run pack` generates the npm manifest from Cargo metadata in
a temporary directory. The source `package.json` is private and has no version. All nine Rust
crates share a release-plz version group and publish to crates.io.

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
