#!/usr/bin/env bash

set -euo pipefail

script_dir="$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=prepare-ghostty.sh
source "${script_dir}/prepare-ghostty.sh"
exec cargo clippy --locked --all-targets -- -D warnings
