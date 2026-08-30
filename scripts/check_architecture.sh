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
    '^[[:space:]]*(cue-(client|daemon|language|protocol|store-sqlite)|rusqlite)[[:space:]]*=' \
    'cue-runtime must remain independent of daemon, transport, persistence, and frontends' \
    "${repo_root}/crates/cue-runtime/Cargo.toml"

fail_if_match \
    '^[[:space:]]*(cue-(client|daemon|language|runtime|store-sqlite)|rusqlite|tokio)[[:space:]]*=' \
    'cue-protocol must depend only on Core data and transport serialization' \
    "${repo_root}/crates/cue-protocol/Cargo.toml"

fail_if_match \
    '^[[:space:]]*(cue-(client|daemon|language|runtime)|tokio)[[:space:]]*=' \
    'cue-store-sqlite must remain an independent Core/protocol store provider' \
    "${repo_root}/crates/cue-store-sqlite/Cargo.toml"

fail_if_match \
    'crate::(command|cron|ipc|launch|resource|scope|spawn_adapter|tui_debug)' \
    'cue_core::vnext must not depend on IPC v3 or policy modules' \
    "${repo_root}/crates/cue-core/src/vnext"

fail_if_match \
    '^[[:space:]]*cue-language[[:space:]]*=' \
    'cue-daemon must not parse the surface language' \
    "${repo_root}/crates/cue-daemon/Cargo.toml"

fail_if_match \
    'cue_core::(execution|ipc|launch|resource|scope|spawn_adapter)' \
    'the vNext language compiler must not depend on IPC v3 or legacy execution policy' \
    "${repo_root}/crates/cue-language/src/vnext_compiler.rs"

fail_if_match \
    'crate::(actor|storage)|cue_core::(execution|ipc|launch|resource|scope|spawn_adapter)' \
    'the vNext daemon service must not depend on the IPC v3 actor tree or legacy execution policy' \
    "${repo_root}/crates/cue-daemon/src/vnext.rs"

fail_if_match \
    'cue_core::(execution|ipc|launch|resource|scope|spawn_adapter)' \
    'the vNext client must use only Core vNext and the strict IPC v4 protocol' \
    "${repo_root}/crates/cue-client/src/vnext.rs"

fail_if_match \
    'cue_core::(execution|ipc|launch|resource|scope|spawn_adapter)' \
    'vNext executable frontends must not restore IPC v3 or legacy execution policy' \
    "${repo_root}/crates/cue-client/src/cli.rs" \
    "${repo_root}/crates/cue-client/src/script_runner.rs" \
    "${repo_root}/crates/cue-tui/src"
