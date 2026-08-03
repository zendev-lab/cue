#!/usr/bin/env bash
#
# Prove that a raw Cargo build cannot trigger libghostty-vt-sys network fetches.

set -euo pipefail

probe_root="$(mktemp -d "${TMPDIR:-/tmp}/cue-shell-no-ghostty.XXXXXX")"
trap 'rm -rf -- "${probe_root}"' EXIT
probe_log="${probe_root}/cargo.log"

if env \
  -u GHOSTTY_SOURCE_DIR \
  -u GHOSTTY_ZIG_SYSTEM_DIR \
  CARGO_NET_OFFLINE=true \
  CARGO_TARGET_DIR="${probe_root}/target" \
  GIT_ALLOW_PROTOCOL=file \
  cargo check --locked -p cue-terminal --lib >"${probe_log}" 2>&1; then
  echo "raw Cargo unexpectedly built cue-terminal without Ghostty preparation" >&2
  exit 1
fi

if ! grep -Fq "GHOSTTY_SOURCE_DIR does not contain build.zig" "${probe_log}"; then
  echo "raw Cargo did not fail at the expected Ghostty source sentinel" >&2
  cat "${probe_log}" >&2
  exit 1
fi
if grep -Fq "Fetching ghostty" "${probe_log}" \
  || find "${probe_root}" -type d -name ghostty-src -print -quit | grep -q .; then
  echo "raw Cargo attempted to fetch Ghostty" >&2
  cat "${probe_log}" >&2
  exit 1
fi

echo "verified raw Cargo fails closed before Ghostty network access"
