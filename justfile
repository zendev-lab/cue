default:
    @just --list

# Prepare Git hooks without pinning the local prek runner.
install:
    uvx --isolated prek install --prepare-hooks

# Format Rust sources and this command file.
format:
    just --fmt --unstable
    cargo fmt

# Run formatting, Clippy, architecture and repository policy checks.
check:
    uvx --isolated prek run --all-files

# Build the workspace binaries.
build *args:
    cargo build --workspace --locked {{ args }}

# Run workspace behavior tests or a selected Cargo test target.
test *args:
    cargo test --locked {{ args }}

# Generate measured line coverage.
coverage:
    cargo llvm-cov test --lcov --output-path lcov.info -- --no-capture

# Measure protocol framing on the local machine.
benchmark:
    cargo bench -p cue-protocol --bench framing --locked

# Reproduce the source, compatibility and installed-package gates.
ci: check test
    rustup run 1.95 cargo check --workspace --all-targets
    bash scripts/check_packages.sh
    node scripts/smoke_npm_package.mjs
