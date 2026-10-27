# List all available commands
default:
    @just --list

# Format all code
format:
    just --fmt --unstable
    cargo fmt

# Run all static checks (fmt check + clippy)
check: architecture
    cargo fmt --all -- --check
    cargo clippy --all-targets -- -D warnings

# Enforce crate boundaries that keep execution semantics closed.
architecture:
    uvx prek run alint --all-files

# Run tests
test *ARGS:
    cargo test {{ ARGS }}

# Run tests with coverage (requires cargo-llvm-cov)
cov:
    cargo llvm-cov test --lcov --output-path lcov.info -- --no-capture

# Open coverage HTML report
cov-open:
    cargo llvm-cov test --html -- --no-capture
    open target/llvm-cov/html/index.html || xdg-open target/llvm-cov/html/index.html

# Compile with the actual minimum supported Rust toolchain.
msrv:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! command -v rustup >/dev/null 2>&1; then
        echo "rustup is required to verify MSRV 1.95" >&2
        exit 1
    fi
    rustup run 1.95 cargo check --workspace --all-targets

# Build and exercise the public wheel command surface.
package-smoke:
    #!/usr/bin/env bash
    set -euo pipefail
    package_dir="$(mktemp -d "${TMPDIR:-/tmp}/cue-package-smoke.XXXXXX")"
    trap 'rm -rf "$package_dir"' EXIT
    uvx --from maturin==1.13.3 maturin build --release --locked --out "$package_dir"
    uvx --from maturin==1.13.3 maturin sdist --out "$package_dir"
    ./scripts/smoke_package.sh "$package_dir"/*.whl
    ./scripts/smoke_package.sh "$package_dir"/*.tar.gz

# Pack and exercise the canonical Cue Skill npm package.
npm-package-smoke:
    node scripts/smoke_npm_package.mjs

# Clean build artifacts
clean:
    rm -rf target/
    rm -f lcov.info

# Verify normalized crate packages, using Cargo's temporary workspace registry.
crate-package-smoke:
    cargo package --workspace --registry crates-io --locked

# Full local CI gate.
ci: check test msrv package-smoke npm-package-smoke

# Dependency checks; install the versions pinned by Quality Analysis.
quality:
    cargo machete
    cargo deny check advisories
    cargo deny --manifest-path fuzz/Cargo.toml check advisories

# Model the production output store's mutex under exhaustive thread scheduling.
loom:
    RUSTFLAGS='--cfg cue_loom' CARGO_TARGET_DIR=target/loom cargo test -p cue-runtime --lib output::loom_tests --locked

# Report surviving mutants separately from runner or baseline failures.
mutation:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p reports/mutation
    status=0
    # Mutated storage panics can strand PTY tasks; process tests run in the native suite.
    cargo mutants --package cue-core --package cue-protocol --package cue-runtime --file crates/cue-core/src/kernel/env.rs --file crates/cue-protocol/src/framing.rs --file crates/cue-runtime/src/output.rs --jobs 2 --cargo-arg=--locked --output reports/mutation -- --lib -- --skip runner::tests || status=$?
    if [[ "$status" -ne 0 && "$status" -ne 2 ]]; then
        exit "$status"
    fi

# Bound each fuzz run and preserve its generated corpus and crash artifacts.
fuzz:
    cargo +nightly-2026-10-26 fetch --manifest-path fuzz/Cargo.toml --locked
    mkdir -p fuzz/corpus/boundaries
    CARGO_NET_OFFLINE=true cargo +nightly-2026-10-26 fuzz run boundaries fuzz/corpus/boundaries fuzz/seeds/boundaries -- -max_total_time=60 -max_len=4096

# Pure Core/protocol tests; the native suite owns the 16 MiB allocation boundary.
miri:
    MIRIFLAGS='-Zmiri-env-set=PROPTEST_CASES=8 -Zmiri-env-set=PROPTEST_RNG_SEED=1 -Zmiri-env-set=PROPTEST_DISABLE_FAILURE_PERSISTENCE=1' cargo +nightly-2026-10-26 miri test -p cue-core -p cue-protocol --lib --locked -- --skip encoder_accepts_limit_and_rejects_one_byte_more

# Local walltime; CodSpeed's hosted simulation provides comparable CPU results.
bench:
    cargo bench -p cue-protocol --bench framing --locked

# Advisory structure metrics without an invented project-wide score threshold.
reports:
    mkdir -p reports
    npx --yes jscpd@5.4.2 --reporters json --output reports/jscpd --ignore '**/target/**,**/tests/**' crates scripts
    uvx lizard==1.24.3 --ignore_warnings -1 --xml crates > reports/complexity.xml

# Active public documentation; historical research keeps its original references.
links:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p reports
    git ls-files -z '*.md' ':(exclude).agents/notes/**' | xargs -0 uvx --from lychee-bin==0.24.2 lychee --scheme https --scheme http --timeout 20 --max-retries 1 --no-progress --format json --output reports/links.json

# Run pre-commit on all files
pre-commit:
    uvx prek run --all-files

# Install local git hooks via prek
pre-commit-install:
    uvx prek install --prepare-hooks

# Remove local git hooks installed by prek
pre-commit-uninstall:
    uvx prek uninstall

# Display project information
info:
    @echo "=== Cue ==="
    @echo "Rust: $(rustc --version)"
    @echo "Cargo: $(cargo --version)"
    @echo ""
    @echo "Workspace members:"
    @cargo metadata --no-deps --format-version 1 2>/dev/null | jq -r '.packages[].name' 2>/dev/null || echo "  (install jq for detailed info)"
