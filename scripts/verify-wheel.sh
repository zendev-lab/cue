#!/usr/bin/env bash
#
# Verify that release wheels statically embed Ghostty and advertise the
# platform contract used by the release matrix.

set -euo pipefail

if [[ "$#" -ne 2 ]]; then
  echo "usage: scripts/verify-wheel.sh <wheel> <linux-x86_64|macos-x86_64|macos-arm64>" >&2
  exit 2
fi

wheel_path="$1"
expected_platform="$2"
if [[ ! -f "${wheel_path}" ]]; then
  echo "wheel does not exist: ${wheel_path}" >&2
  exit 2
fi
if ! command -v unzip >/dev/null 2>&1 || ! command -v file >/dev/null 2>&1; then
  echo "wheel verification requires unzip and file" >&2
  exit 2
fi

wheel_name="$(basename -- "${wheel_path}")"
verify_root="$(mktemp -d "${TMPDIR:-/tmp}/cue-shell-wheel.XXXXXX")"
trap 'rm -rf -- "${verify_root}"' EXIT
unzip -q "${wheel_path}" -d "${verify_root}"

for license_file in \
  LICENSE \
  THIRD_PARTY_NOTICES.md \
  Apache-2.0.txt \
  Highway-BSD-3-Clause.txt \
  Unicode-3.0.txt \
  Zlib.txt; do
  if ! find "${verify_root}" -type f -name "${license_file}" -print -quit | grep -q .; then
    echo "wheel is missing required license file: ${license_file}" >&2
    exit 1
  fi
done
notice_path="$(find "${verify_root}" -type f -name THIRD_PARTY_NOTICES.md -print -quit)"
for component in Ghostty libghostty-vt uucode simdutf Highway zlib Zig; do
  if ! grep -Fq "${component}" "${notice_path}"; then
    echo "wheel notice is missing component: ${component}" >&2
    exit 1
  fi
done

binary_count=0
case "${expected_platform}" in
  macos-x86_64 | macos-arm64)
    if [[ "$(uname -s)" != "Darwin" ]]; then
      echo "${expected_platform} wheel must be verified on macOS" >&2
      exit 2
    fi
    expected_arch="${expected_platform#macos-}"
    if [[ "${wheel_name}" != *"macosx_13_0_${expected_arch}.whl" ]]; then
      echo "unexpected ${expected_platform} wheel tag: ${wheel_name}" >&2
      exit 1
    fi
    if ! command -v vtool >/dev/null 2>&1 || ! command -v otool >/dev/null 2>&1; then
      echo "macOS wheel verification requires vtool and otool" >&2
      exit 2
    fi

    while IFS= read -r -d '' candidate; do
      if file -b "${candidate}" | grep -q "Mach-O.*executable"; then
        binary_count=$((binary_count + 1))
        if ! file -b "${candidate}" | grep -Fq "${expected_arch}"; then
          echo "unexpected Mach-O architecture in ${candidate}" >&2
          file "${candidate}" >&2
          exit 1
        fi
        build_info="$(vtool -show-build "${candidate}")"
        if ! grep -Eq '^[[:space:]]*minos 13\.0$' <<< "${build_info}"; then
          echo "Mach-O binary does not target macOS 13.0: ${candidate}" >&2
          echo "${build_info}" >&2
          exit 1
        fi
        if otool -L "${candidate}" | grep -qi "libghostty"; then
          echo "wheel dynamically links Ghostty: ${candidate}" >&2
          exit 1
        fi
      fi
    done < <(find "${verify_root}" -type f -print0)
    ;;
  linux-x86_64)
    if [[ "$(uname -s)" != "Linux" ]]; then
      echo "linux-x86_64 wheel must be verified on Linux" >&2
      exit 2
    fi
    if [[ "${wheel_name}" != *"manylinux_2_17_x86_64"* ]] \
      || [[ "${wheel_name}" != *"manylinux2014_x86_64.whl" ]]; then
      echo "unexpected manylinux2014 x86_64 wheel tag: ${wheel_name}" >&2
      exit 1
    fi
    if ! command -v readelf >/dev/null 2>&1; then
      echo "Linux wheel verification requires readelf" >&2
      exit 2
    fi

    if command -v auditwheel >/dev/null 2>&1; then
      auditwheel show "${wheel_path}"
    elif command -v uvx >/dev/null 2>&1; then
      uvx auditwheel show "${wheel_path}"
    else
      echo "Linux wheel verification requires auditwheel or uvx" >&2
      exit 2
    fi

    while IFS= read -r -d '' candidate; do
      if file -b "${candidate}" | grep -q "ELF.*executable"; then
        binary_count=$((binary_count + 1))
        if ! file -b "${candidate}" | grep -Fq "x86-64"; then
          echo "unexpected ELF architecture in ${candidate}" >&2
          file "${candidate}" >&2
          exit 1
        fi
        if readelf -d "${candidate}" | grep -qi "libghostty"; then
          echo "wheel dynamically links Ghostty: ${candidate}" >&2
          exit 1
        fi
      fi
    done < <(find "${verify_root}" -type f -print0)
    ;;
  *)
    echo "unsupported expected platform: ${expected_platform}" >&2
    exit 2
    ;;
esac

if (( binary_count == 0 )); then
  echo "wheel contains no native cue-shell executables: ${wheel_name}" >&2
  exit 1
fi

echo "verified ${binary_count} cue-shell binaries with no dynamic Ghostty dependency in ${wheel_name}"
