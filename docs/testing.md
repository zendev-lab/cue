# Test architecture

Cue tests protect observable product contracts at the closest owning boundary. Repository policy
is checked by dedicated static tools, not by tests that mirror the current source tree.

## Lanes

| Lane | Location or command | Contract |
| --- | --- | --- |
| Crate unit and contract | `crates/*/src/**/*.rs` | Pure behavior, parsing, schemas, state transitions, and adapter contracts |
| Crate integration and process | `crates/*/tests/**/*.rs` | IPC, persistence, process lifecycle, PTY, and daemon/client composition |
| Package smoke | `scripts/smoke_package.sh` | Commands installed from the built wheel or source distribution |
| npm Skill package | `scripts/smoke_npm_package.mjs` | Packed file allowlist, version alignment, exported root, and canonical Skill bytes |
| Repository policy | `just pre-commit` | Formatting, lint, workflow syntax and security, links, spelling, and commit messages |

Keep Linux-only process behavior in the integration lane instead of replacing it with a mock that
cannot exercise `/proc`, pidfd, signal, socket, or lock semantics. Keep package smoke separate from
source execution: it must install the produced artifact and invoke the public command names.

## Tests versus static policy

Code tests assert return values, state transitions, persisted effects, boundary calls, process exit
status, output, and compatibility behavior. They do not read the repository's current Rust source,
workflow YAML, manifests, Just recipes, or documentation and assert that selected text fragments
still exist.

If a constraint is about repository shape, use the standard static tool that owns that format. If
wording in a design document is not itself a public serialized contract, review or generate the
document instead of mirroring its prose in a unit test. Use complete golden files only when the
whole representation is the contract.

Mutation testing is a useful continuous-evaluation lane once it has an owner, command, budget, and
reporting workflow. Until then, Cue does not commit a partial configuration that excludes the most
important modules while implying that mutation testing is an active gate.

## Gates

- `just check` runs Rust formatting and Clippy on the repository-pinned primary toolchain.
- `just test` runs the workspace behavior suite.
- `just msrv` compiles all targets with Rust 1.95 through rustup.
- `just package-smoke` builds wheel and source distributions and exercises every installed command.
- `just npm-package-smoke` packs and installs `@zendev-lab/cue`, then verifies the canonical Skill.
- `just ci` is the serial local aggregate of those gates; hosted CI keeps independent lanes parallel.

The primary toolchain is pinned in `rust-toolchain.toml` and advances through a dedicated Renovate
change instead of making unrelated pull requests absorb a new stable compiler implicitly. Hosted CI
also runs the Rust 1.95 MSRV check as its own job, so primary-toolchain and compatibility failures
remain attributable to the lane that owns them.

When reviewing a test, ask what observable regression becomes invisible if it is removed, whether a
wording-only refactor would break it, whether fail-closed and recovery behavior have negative paths,
and whether the test runs at the boundary that owns the contract.
