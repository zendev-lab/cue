#!/usr/bin/env bash
set -euo pipefail
package_dir="$(mktemp -d "${TMPDIR:-/tmp}/cue-package-smoke.XXXXXX")"
trap 'rm -rf "$package_dir"' EXIT
uvx --from maturin==1.13.3 maturin build --release --locked --out "$package_dir"
uvx --from maturin==1.13.3 maturin sdist --out "$package_dir"
./scripts/smoke_package.sh "$package_dir"/*.whl
./scripts/smoke_package.sh "$package_dir"/*.tar.gz
