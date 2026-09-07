#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 || ! -f "$1" ]]; then
    echo "usage: $0 <wheel-or-sdist>" >&2
    exit 2
fi

package_path="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
smoke_root="$(mktemp -d "${TMPDIR:-/tmp}/cue-installed-package.XXXXXX")"
daemon_started=false

cleanup() {
    if [[ "$daemon_started" == true ]]; then
        if ! uv tool run --from "$package_path" cued stop >/dev/null 2>&1; then
            echo "could not confirm test daemon shutdown; keeping $smoke_root" >&2
            return
        fi
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
export CUE_SOCKET="$XDG_RUNTIME_DIR/cue/cued.sock"
mkdir -p "$XDG_CONFIG_HOME" "$XDG_DATA_HOME" "$XDG_RUNTIME_DIR" "$XDG_STATE_HOME"

uv tool run --from "$package_path" cue --version
uv tool run --from "$package_path" cue --help
uv tool run --from "$package_path" cue-client --version
uv tool run --from "$package_path" cue-tui --version
uv tool run --from "$package_path" cued --version

daemon_started=true
uv tool run --from "$package_path" cued start
uv tool run --from "$package_path" cued status
uv tool run --from "$package_path" cue-client exec "printf package-ok"
uv tool run --from "$package_path" cue client list
uv tool run --from "$package_path" cue daemon status

if uv tool run --from "$package_path" cue target list; then
    echo "cue target unexpectedly succeeded" >&2
    exit 1
fi

uv tool run --from "$package_path" cue daemon restart
uv tool run --from "$package_path" cue daemon status
uv tool run --from "$package_path" cue daemon stop
daemon_started=false
