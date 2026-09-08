# Releasing Cue

Cargo's workspace version is the product version. Maturin reads it for
`cue-run`; `package.json` is synchronized in each release PR. All nine Rust
crates share a release-plz version group and publish to crates.io.

## Normal releases

1. Merging development changes into `main` updates the release-plz PR.
2. Review its version, breaking changes and CI, then merge it manually.
3. `release-plz.yml` publishes the crates using OIDC and creates their
   `<crate>-v<version>` tags. It creates the product `v<version>` tag only
   after every crate/version exists and every crate tag identifies that commit.
4. `cd-publish.yml` validates versions, builds and tests the artifacts, then
   publishes PyPI and npm. GitHub Release creation waits for both publishers.

There is no `just release` command. Do not edit a product tag to retry a failed
release. Rerun failed jobs on the original run/commit. Existing PyPI files must
match the staged SHA-256 before being skipped. Different bytes are an error;
reuse the original artifacts or publish a new version. Existing npm versions
must likewise match the staged tarball integrity.

Keep upgrade notes for a breaking release in `docs/releases/<version>.md`;
when present, the publishing workflow includes them in the GitHub Release
alongside GitHub's generated change list.

## Credentials and publisher identities

The private GitHub App is installed only on `zendev-lab/cue`, with Contents and
Pull requests read/write. Actions use `RELEASE_APP_ID` (repository variable)
and `RELEASE_APP_PRIVATE_KEY` (repository secret) to obtain short-lived
installation tokens. These tokens allow bot PRs and product tags to trigger CI.

| Registry | Project | Workflow | GitHub environment |
| --- | --- | --- | --- |
| crates.io | Each of the nine workspace crate names | `release-plz.yml` | `crates-release` |
| PyPI | `cue-run` | `cd-publish.yml` | `pypi-release` |
| npm | `@zendev-lab/cue` | `cd-publish.yml` | `npm-release` |

All publisher identities use owner `zendev-lab`, repository `cue`. No long-lived
registry token is stored in Actions. Configure the PyPI pending publisher with
exactly `cue-run`; a different project name cannot create this distribution.

## First crates.io release

crates.io requires the first release of a new crate to use an account token;
Trusted Publishing can be configured after the crate exists. From the fixed,
reviewed release commit, publish `0.2.0` in this order:

```text
cue-core → cue-language → cue-protocol → cue-runtime → cue-store-sqlite
→ cue-daemon → cue-client → cue-tui → cue-cli
```

For each package run `cargo publish --registry crates-io --locked -p <crate>`
and wait until its version is available before publishing dependents. Then
configure each crate's Trusted Publisher, create each `<crate>-v0.2.0` tag on
the same commit, and create the product `v0.2.0` tag. Never publish from a dirty
checkout or from the old v0.1.2 checkout.

Before publishing, run `just ci`, `just crate-package-smoke` and
`uv run --no-project --python 3.14 python -m unittest discover -s scripts -p test_release.py`.
`cargo package --workspace` uses a temporary registry to verify unpublished
workspace dependencies. Run this check with a clean Cargo configuration;
a crates.io source replacement may bypass that temporary registry and produce
misleading missing-package errors.
