#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"

fail_if_match() {
    local pattern="$1"
    local message="$2"
    shift 2
    if rg -n "${pattern}" "$@"; then
        echo "architecture check failed: ${message}" >&2
        exit 1
    fi
}

fail_if_match \
    '^[[:space:]]*(cue-(client|daemon|language|protocol|runtime)|rusqlite|tokio)[[:space:]]*=' \
    'cue-core must remain independent of runtime, transport, storage, and frontend crates' \
    "${repo_root}/crates/cue-core/Cargo.toml"

fail_if_match \
    '^[[:space:]]*(cue-(client|daemon|language|protocol)|rusqlite|tokio)[[:space:]]*=' \
    'cue-runtime composition must remain independent of daemon, transport, storage, and frontends' \
    "${repo_root}/crates/cue-runtime/Cargo.toml"

fail_if_match \
    'crate::(command|cron|ipc|launch|resource|scope|spawn_adapter|tui_debug)' \
    'cue_core::vnext must not depend on IPC v3 or policy modules' \
    "${repo_root}/crates/cue-core/src/vnext"

fail_if_match \
    '^[[:space:]]*cue-language[[:space:]]*=' \
    'cue-daemon must not parse the surface language' \
    "${repo_root}/crates/cue-daemon/Cargo.toml"
