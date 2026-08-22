#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 || ! -f "$1" ]]; then
    echo "usage: $0 <wheel-or-sdist>" >&2
    exit 2
fi

package_path="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
smoke_root="$(mktemp -d "${TMPDIR:-/tmp}/cue-installed-package.XXXXXX")"
trap 'rm -rf "$smoke_root"' EXIT
export CARGO_HOME="${CARGO_HOME:-$HOME/.cargo}"
export RUSTUP_HOME="${RUSTUP_HOME:-$HOME/.rustup}"
export HOME="$smoke_root/home"
export XDG_CONFIG_HOME="$smoke_root/config"
export XDG_DATA_HOME="$smoke_root/data"
export XDG_RUNTIME_DIR="$smoke_root/runtime"
export XDG_STATE_HOME="$smoke_root/state"
mkdir -p "$HOME" "$XDG_CONFIG_HOME" "$XDG_DATA_HOME" "$XDG_RUNTIME_DIR" "$XDG_STATE_HOME"

uv tool run --from "$package_path" cue --version
uv tool run --from "$package_path" cue --help
uv tool run --from "$package_path" cue-client --version
uv tool run --from "$package_path" cue-client target resolve --json
uv tool run --from "$package_path" cue-client target list --json
uv tool run --from "$package_path" cue-tui --version
uv tool run --from "$package_path" cued --version
uv tool run --from "$package_path" cue client target resolve --json
uv tool run --from "$package_path" cue daemon --version
if uv tool run --from "$package_path" cue target resolve --json; then
    echo "cue target unexpectedly succeeded" >&2
    exit 1
fi
uv tool run --from "$package_path" cued status
