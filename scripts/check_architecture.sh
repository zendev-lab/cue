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
    'cue-core must not depend on removed transport or policy modules' \
    "${repo_root}/crates/cue-core/src/kernel"

fail_if_match \
    '^[[:space:]]*cue-language[[:space:]]*=' \
    'cue-daemon must not parse the surface language' \
    "${repo_root}/crates/cue-daemon/Cargo.toml"

fail_if_match \
    'cue_core::(execution|ipc|launch|resource|scope|spawn_adapter)' \
    'the language compiler must not depend on removed execution policy' \
    "${repo_root}/crates/cue-language/src/compiler.rs"

fail_if_match \
    'crate::(actor|storage)|cue_core::(execution|ipc|launch|resource|scope|spawn_adapter)' \
    'the daemon service must not depend on the removed actor tree or execution policy' \
    "${repo_root}/crates/cue-daemon/src/service.rs"

fail_if_match \
    'cue_core::(execution|ipc|launch|resource|scope|spawn_adapter)' \
    'the client must use only Core and the strict IPC v4 protocol' \
    "${repo_root}/crates/cue-client/src/execution.rs"

fail_if_match \
    'cue_core::(execution|ipc|launch|resource|scope|spawn_adapter)' \
    'executable frontends must not restore removed execution policy' \
    "${repo_root}/crates/cue-client/src/cli.rs" \
    "${repo_root}/crates/cue-client/src/script_runner.rs" \
    "${repo_root}/crates/cue-tui/src"

fail_if_match \
    'cue_core::(command|cron|execution|ipc|launch|pipeline|process_status|resource|scope|spawn_adapter|tui_debug)' \
    'workspace source must not import removed cue-core modules' \
    "${repo_root}/crates"

legacy_paths=(
    "crates/cue-core/src/ipc.rs"
    "crates/cue-core/src/execution.rs"
    "crates/cue-core/src/resource.rs"
    "crates/cue-language/src/vnext_compiler.rs"
    "crates/cue-daemon/src/actor/mod.rs"
    "crates/cue-daemon/src/vnext.rs"
    "crates/cue-client/src/vnext.rs"
    "crates/cue-client/src/transport_config.rs"
)
for path in "${legacy_paths[@]}"; do
    if [[ -f "${repo_root}/${path}" ]]; then
        echo "architecture check failed: removed compatibility path returned: ${path}" >&2
        exit 1
    fi
done
