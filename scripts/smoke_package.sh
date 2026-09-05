#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 || ! -f "$1" ]]; then
    echo "usage: $0 <wheel-or-sdist>" >&2
    exit 2
fi

package_path="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
smoke_root="$(mktemp -d "${TMPDIR:-/tmp}/cue-installed-package.XXXXXX")"
daemon_pid=""

cleanup() {
    if [[ -n "$daemon_pid" ]]; then
        kill "$daemon_pid" 2>/dev/null || true
        wait "$daemon_pid" 2>/dev/null || true
    fi
    rm -rf "$smoke_root"
}
trap cleanup EXIT

export CARGO_HOME="${CARGO_HOME:-$HOME/.cargo}"
export RUSTUP_HOME="${RUSTUP_HOME:-$HOME/.rustup}"
export XDG_CONFIG_HOME="$smoke_root/config"
export XDG_DATA_HOME="$smoke_root/data"
export XDG_RUNTIME_DIR="$smoke_root/runtime"
export XDG_STATE_HOME="$smoke_root/state"
mkdir -p "$XDG_CONFIG_HOME" "$XDG_DATA_HOME" "$XDG_RUNTIME_DIR" "$XDG_STATE_HOME"

uv tool run --from "$package_path" cue --version
uv tool run --from "$package_path" cue --help
uv tool run --from "$package_path" cue-client --version
uv tool run --from "$package_path" cue-tui --version
uv tool run --from "$package_path" cued --version

uv tool run --from "$package_path" cued start >"$smoke_root/cued.log" 2>&1 &
daemon_pid=$!
for _attempt in $(seq 1 100); do
    if uv tool run --from "$package_path" cued status >/dev/null 2>&1; then
        break
    fi
    sleep 0.05
done
uv tool run --from "$package_path" cued status
uv tool run --from "$package_path" cue-client exec "printf package-ok"
uv tool run --from "$package_path" cue client list
uv tool run --from "$package_path" cue daemon status

if uv tool run --from "$package_path" cue target list; then
    echo "cue target unexpectedly succeeded" >&2
    exit 1
fi

uv tool run --from "$package_path" cue daemon stop
wait "$daemon_pid"
daemon_pid=""
