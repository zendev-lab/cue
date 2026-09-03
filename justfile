# List all available commands
default:
    @just --list

# Format all code
format:
    just --fmt --unstable
    cargo fmt

# Run all static checks (fmt check + clippy)
check:
    cargo fmt --all -- --check
    cargo clippy --all-targets -- -D warnings

# Run tests
test *ARGS:
    cargo test {{ ARGS }}

# Exercise cue-tui's first-party debug socket in a real PTY
tui-debug-smoke:
    uv run scripts/cue_tui_debug_smoke.py

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

# Full local CI gate.
ci: check test msrv package-smoke npm-package-smoke

# Run pre-commit on all files
pre-commit:
    uvx prek run --all-files

# Install local git hooks via prek
pre-commit-install:
    uvx prek install --install-hooks --hook-type pre-commit --hook-type commit-msg

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
