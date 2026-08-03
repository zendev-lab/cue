# List all available commands
default:
    @just --list

# Format all code
format:
    just --fmt --unstable
    cargo fmt

# Run all static checks (fmt check + clippy)
check:
    #!/usr/bin/env bash
    set -euo pipefail
    source scripts/prepare-ghostty.sh
    cargo fmt --all -- --check
    scripts/verify-ghostty-deps.sh
    scripts/verify-ghostty-fail-closed.sh
    cargo clippy --locked --all-targets -- -D warnings

# Run tests
test *ARGS:
    #!/usr/bin/env bash
    set -euo pipefail
    source scripts/prepare-ghostty.sh
    cargo test --locked {{ ARGS }}

# Exercise cue-tui's first-party debug socket in a real PTY
tui-debug-smoke:
    #!/usr/bin/env bash
    set -euo pipefail
    source scripts/prepare-ghostty.sh
    python3 scripts/cue_tui_debug_smoke.py

# Run tests with coverage (requires cargo-llvm-cov)
cov:
    #!/usr/bin/env bash
    set -euo pipefail
    source scripts/prepare-ghostty.sh
    cargo llvm-cov test --locked --lcov --output-path lcov.info -- --no-capture

# Open coverage HTML report
cov-open:
    #!/usr/bin/env bash
    set -euo pipefail
    source scripts/prepare-ghostty.sh
    cargo llvm-cov test --locked --html -- --no-capture
    open target/llvm-cov/html/index.html || xdg-open target/llvm-cov/html/index.html

# MSRV check. Cargo enforces workspace.package.rust-version (1.95), so this
# works with both rustup-managed and standalone/Homebrew toolchains.
msrv:
    #!/usr/bin/env bash
    set -euo pipefail
    source scripts/prepare-ghostty.sh
    cargo check --locked --workspace --all-targets

# Install or re-verify the pinned Zig/Ghostty build cache
ghostty-prepare:
    #!/usr/bin/env bash
    set -euo pipefail
    source scripts/prepare-ghostty.sh

# Clean build artifacts
clean:
    rm -rf target/
    rm -f lcov.info

# Full CI check (format check + clippy + test + MSRV)
ci: check test msrv

# Run pre-commit on all files
pre-commit:
    #!/usr/bin/env bash
    set -euo pipefail
    source scripts/prepare-ghostty.sh
    uvx prek run --all-files

# Install local git hooks via prek
pre-commit-install:
    uvx prek install --install-hooks --hook-type pre-commit --hook-type commit-msg

# Remove local git hooks installed by prek
pre-commit-uninstall:
    uvx prek uninstall

# Display project information
info:
    @echo "=== cue-shell ==="
    @echo "Rust: $(rustc --version)"
    @echo "Cargo: $(cargo --version)"
    @echo ""
    @echo "Workspace members:"
    @cargo metadata --no-deps --format-version 1 2>/dev/null | jq -r '.packages[].name' 2>/dev/null || echo "  (install jq for detailed info)"
