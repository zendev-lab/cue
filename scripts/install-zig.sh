#!/usr/bin/env bash
#
# Install the exact Zig toolchain used by libghostty-vt and add it to PATH.
# Source this file when the caller needs the PATH change to persist:
#
#   source scripts/install-zig.sh

if [[ -n "${BASH_VERSION:-}" ]]; then
  cue_zig_wrapper_source="${BASH_SOURCE[0]}"
  cue_zig_wrapper_sourced=false
  [[ "${BASH_SOURCE[0]}" != "$0" ]] && cue_zig_wrapper_sourced=true
elif [[ -n "${ZSH_VERSION:-}" ]]; then
  cue_zig_wrapper_source="${(%):-%N}"
  cue_zig_wrapper_sourced=false
  [[ "${ZSH_EVAL_CONTEXT:-}" == *:file ]] && cue_zig_wrapper_sourced=true
else
  echo "install-zig.sh requires Bash or Zsh" >&2
  return 1 2>/dev/null || exit 1
fi

# Keep strict-mode and helper variables inside a child Bash process when this
# file is sourced from an interactive shell. Only the audited environment
# assignments printed by --emit-env are evaluated in the caller.
if [[ "${cue_zig_wrapper_sourced}" == true && "${1:-}" != "--emit-env" ]]; then
  cue_zig_import_env() {
    local source_path="$1"
    local imported_env
    imported_env="$("${source_path}" --emit-env)" || return
    eval "${imported_env}"
  }
  if cue_zig_import_env "${cue_zig_wrapper_source}"; then
    unset -f cue_zig_import_env
    unset cue_zig_wrapper_source cue_zig_wrapper_sourced
    return 0
  fi
  unset -f cue_zig_import_env
  unset cue_zig_wrapper_source cue_zig_wrapper_sourced
  return 1
fi
unset cue_zig_wrapper_source cue_zig_wrapper_sourced

set -euo pipefail

if [[ -n "${BASH_VERSION:-}" ]]; then
  zig_script_source="${BASH_SOURCE[0]}"
elif [[ -n "${ZSH_VERSION:-}" ]]; then
  zig_script_source="${(%):-%N}"
else
  echo "install-zig.sh requires Bash or Zsh" >&2
  return 1 2>/dev/null || exit 1
fi
zig_script_dir="$(CDPATH='' cd -- "$(dirname -- "${zig_script_source}")" && pwd)"

zig_version="0.15.2"
kernel="$(uname -s)"
machine="$(uname -m)"

case "${kernel}:${machine}" in
  Darwin:arm64)
    zig_platform="aarch64-macos"
    zig_sha256="3cc2bab367e185cdfb27501c4b30b1b0653c28d9f73df8dc91488e66ece5fa6b"
    zig_tree_sha256="6264642503cd8381d735eecb014b57b8a773f25de006209554818685e43e4449"
    ;;
  Darwin:x86_64)
    zig_platform="x86_64-macos"
    zig_sha256="375b6909fc1495d16fc2c7db9538f707456bfc3373b14ee83fdd3e22b3d43f7f"
    zig_tree_sha256="197247ccad1c01761fd5fca5377b31129df96c1f2437b25eb6b284fa7fe4e62a"
    ;;
  Linux:aarch64 | Linux:arm64)
    zig_platform="aarch64-linux"
    zig_sha256="958ed7d1e00d0ea76590d27666efbf7a932281b3d7ba0c6b01b0ff26498f667f"
    zig_tree_sha256="7f7f23701e632c779a8fc5569f702ee28753b4568f2ffd4c8c33935d8fdbf64f"
    ;;
  Linux:x86_64)
    zig_platform="x86_64-linux"
    zig_sha256="02aa270f183da276e5b5920b1dac44a63f1a49e55050ebde3aecc9eb82f93239"
    zig_tree_sha256="f2dfbb732b4ecf617a1cf99fb4a8b658f452b4d59c65ec87df29891ad4b44f1f"
    ;;
  *)
    echo "unsupported Zig host: ${kernel} ${machine}" >&2
    return 1 2>/dev/null || exit 1
    ;;
esac

zig_cache_root="${CUE_ZIG_INSTALL_ROOT:-${XDG_CACHE_HOME:-${HOME}/.cache}/cue-shell/toolchains}"
zig_install_dir="${zig_cache_root}/zig-${zig_version}-${zig_platform}-${zig_sha256}"
zig_tree_digest_script="${zig_script_dir}/tree_digest.py"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required to verify the Zig toolchain tree" >&2
  exit 1
fi

if [[ ! -x "${zig_install_dir}/zig" ]]; then
  mkdir -p "${zig_cache_root}"
  zig_install_lock="${zig_install_dir}.prepare-lock"
  zig_lock_acquired=false
  zig_tmp_dir=""
  zig_cleanup() {
    if [[ -n "${zig_tmp_dir}" && -d "${zig_tmp_dir}" ]]; then
      chmod -R u+w "${zig_tmp_dir}" 2>/dev/null || true
      rm -rf "${zig_tmp_dir}"
    fi
    if [[ "${zig_lock_acquired}" == true && -d "${zig_install_lock}" ]]; then
      rmdir "${zig_install_lock}" 2>/dev/null || true
    fi
  }
  trap zig_cleanup EXIT
  for ((zig_wait = 0; zig_wait < 300; zig_wait += 1)); do
    if mkdir "${zig_install_lock}" 2>/dev/null; then
      zig_lock_acquired=true
      break
    fi
    if [[ -x "${zig_install_dir}/zig" && ! -d "${zig_install_lock}" ]]; then
      break
    fi
    sleep 1
  done
  if [[ "${zig_lock_acquired}" == false && ! -x "${zig_install_dir}/zig" ]]; then
    echo "timed out waiting for Zig toolchain preparation: ${zig_install_lock}" >&2
    exit 1
  fi

  if [[ "${zig_lock_acquired}" == true && ! -x "${zig_install_dir}/zig" ]]; then
    zig_tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/cue-shell-zig.XXXXXX")"
    zig_archive="${zig_tmp_dir}/zig.tar.xz"
    zig_archive_dir="zig-${zig_platform}-${zig_version}"
    zig_url="https://ziglang.org/download/${zig_version}/${zig_archive_dir}.tar.xz"

    curl --fail --location --silent --show-error --retry 3 \
      --connect-timeout 30 --max-time 120 \
      "${zig_url}" \
      --output "${zig_archive}"
    if command -v sha256sum >/dev/null 2>&1; then
      printf '%s  %s\n' "${zig_sha256}" "${zig_archive}" | sha256sum --check - >&2
    else
      actual_sha256="$(shasum -a 256 "${zig_archive}" | awk '{print $1}')"
      if [[ "${actual_sha256}" != "${zig_sha256}" ]]; then
        echo "Zig archive checksum mismatch: expected ${zig_sha256}, got ${actual_sha256}" >&2
        exit 1
      fi
    fi

    tar -xJf "${zig_archive}" -C "${zig_tmp_dir}"
    extracted_zig_dir="${zig_tmp_dir}/${zig_archive_dir}"
    extracted_tree_sha256="$(python3 "${zig_tree_digest_script}" "${extracted_zig_dir}")"
    if [[ "${extracted_tree_sha256}" != "${zig_tree_sha256}" ]]; then
      echo "Zig tree digest mismatch: expected ${zig_tree_sha256}, got ${extracted_tree_sha256}" >&2
      exit 1
    fi
    mv "${extracted_zig_dir}" "${zig_install_dir}"
    if ! chmod -R a-w "${zig_install_dir}"; then
      chmod -R u+w "${zig_install_dir}" 2>/dev/null || true
      rm -rf "${zig_install_dir}"
      echo "failed to protect installed Zig toolchain" >&2
      exit 1
    fi
  fi
  zig_cleanup
  trap - EXIT
  unset -f zig_cleanup
fi

actual_zig_tree_sha256="$(python3 "${zig_tree_digest_script}" "${zig_install_dir}")"
if [[ "${actual_zig_tree_sha256}" != "${zig_tree_sha256}" ]]; then
  echo "cached Zig tree failed verification: expected ${zig_tree_sha256}, got ${actual_zig_tree_sha256}" >&2
  exit 1
fi
export CUE_ZIG_TREE_SHA256="${actual_zig_tree_sha256}"

export PATH="${zig_install_dir}:${PATH}"
if [[ "$(zig version)" != "${zig_version}" ]]; then
  echo "expected Zig ${zig_version}, found $(zig version)" >&2
  return 1 2>/dev/null || exit 1
fi

if [[ "${kernel}" == "Darwin" ]]; then
  zig_sdk_version="$(xcrun --show-sdk-version 2>/dev/null || true)"
  zig_sdk_major="${zig_sdk_version%%.*}"
  if [[ "${zig_sdk_major}" =~ ^[0-9]+$ ]] && (( zig_sdk_major >= 26 )); then
    zig_compatible_sdk="$(
      find /Library/Developer/CommandLineTools/SDKs \
        -maxdepth 1 \
        -type d \
        -name 'MacOSX15*.sdk' \
        -print 2>/dev/null \
        | sort -V \
        | tail -n 1
    )"
    if [[ -z "${zig_compatible_sdk}" ]]; then
      echo "Zig ${zig_version} cannot link against macOS SDK ${zig_sdk_version}; install a macOS 15 Command Line Tools SDK" >&2
      return 1 2>/dev/null || exit 1
    fi

    export CUE_ZIG_MACOS_SDK="${zig_compatible_sdk}"
    export SDKROOT="${zig_compatible_sdk}"
    export PATH="${zig_script_dir}/zig-macos-compat:${PATH}"
    echo "using macOS SDK ${zig_compatible_sdk} for Zig ${zig_version} compatibility" >&2
  fi
fi

echo "using Zig $(zig version) from ${zig_install_dir}" >&2

if [[ "${1:-}" == "--emit-env" ]]; then
  zig_emit_export() {
    local name="$1"
    local value="$2"
    value="${value//\'/\'\\\'\'}"
    printf "export %s='%s'\n" "${name}" "${value}"
  }
  zig_emit_export PATH "${PATH}"
  zig_emit_export CUE_ZIG_TREE_SHA256 "${CUE_ZIG_TREE_SHA256}"
  if [[ -n "${CUE_ZIG_MACOS_SDK:-}" ]]; then
    zig_emit_export CUE_ZIG_MACOS_SDK "${CUE_ZIG_MACOS_SDK}"
  fi
  if [[ -n "${SDKROOT:-}" ]]; then
    zig_emit_export SDKROOT "${SDKROOT}"
  fi
fi
