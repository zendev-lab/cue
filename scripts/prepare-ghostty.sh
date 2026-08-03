#!/usr/bin/env bash
#
# Prepare the pinned Ghostty source and the minimal offline Zig package closure.
# Source this file so Cargo inherits GHOSTTY_SOURCE_DIR and
# GHOSTTY_ZIG_SYSTEM_DIR:
#
#   source scripts/prepare-ghostty.sh

if [[ -n "${BASH_VERSION:-}" ]]; then
  cue_prepare_wrapper_source="${BASH_SOURCE[0]}"
  cue_prepare_wrapper_sourced=false
  [[ "${BASH_SOURCE[0]}" != "$0" ]] && cue_prepare_wrapper_sourced=true
elif [[ -n "${ZSH_VERSION:-}" ]]; then
  cue_prepare_wrapper_source="${(%):-%N}"
  cue_prepare_wrapper_sourced=false
  [[ "${ZSH_EVAL_CONTEXT:-}" == *:file ]] && cue_prepare_wrapper_sourced=true
else
  echo "prepare-ghostty.sh requires Bash or Zsh" >&2
  return 1 2>/dev/null || exit 1
fi

# Sourcing must not leak strict shell options or internal helpers into a
# developer's interactive shell. Run preparation in a child Bash process and
# import only the explicitly emitted environment.
if [[ "${cue_prepare_wrapper_sourced}" == true && "${1:-}" != "--emit-env" ]]; then
  cue_prepare_import_env() {
    local source_path="$1"
    local imported_env
    imported_env="$("${source_path}" --emit-env)" || return
    eval "${imported_env}"
  }
  if cue_prepare_import_env "${cue_prepare_wrapper_source}"; then
    unset -f cue_prepare_import_env
    unset cue_prepare_wrapper_source cue_prepare_wrapper_sourced
    return 0
  fi
  unset -f cue_prepare_import_env
  unset cue_prepare_wrapper_source cue_prepare_wrapper_sourced
  return 1
fi
unset cue_prepare_wrapper_source cue_prepare_wrapper_sourced

set -euo pipefail

if [[ -n "${BASH_VERSION:-}" ]]; then
  cue_script_source="${BASH_SOURCE[0]}"
elif [[ -n "${ZSH_VERSION:-}" ]]; then
  cue_script_source="${(%):-%N}"
else
  echo "prepare-ghostty.sh requires Bash or Zsh" >&2
  return 1 2>/dev/null || exit 1
fi
cue_script_dir="$(CDPATH='' cd -- "$(dirname -- "${cue_script_source}")" && pwd)"
# shellcheck source=install-zig.sh
source "${cue_script_dir}/install-zig.sh"

cue_ghostty_commit="a887df42c56f6de86c0fe6da9c4eeca37931e083"
cue_ghostty_archive_sha256="fb4b2f9ffa0af125983041fdbe4ef94d3fa79fb9f2d22b9c213c0e3847a866b6"
cue_ghostty_patch_sha256="79472a321de3ddd1065b008e25241b63ddac2b0ef933049d5f73429b384d5d76"
cue_ghostty_patched_build_sha256="d9789dfa23790cc0e49ce35abf33fda6c615da3725a81210a61d198509972bca"
cue_ghostty_tree_sha256="53a4f4e917e0d21f4537eb52686985d3f9c78536fb98752e240c7d6f4f8a66a8"
cue_uucode_archive_sha256="d0abee0f4f8bd6eae3c051777e16e7c42d8964aaaa015591c4e565703f465f95"
cue_uucode_package_hash="uucode-0.2.0-ZZjBPqZVVABQepOqZHR7vV_NcaN-wats0IB6o-Exj6m9"
cue_zlib_archive_sha256="17e88863f3600672ab49182f217281b6fc4d3c762bde361935e436a95214d05c"
cue_zlib_package_hash="N-V-__8AAB0eQwD-0MdOEBmz7intriBReIsIDNlukNVoNu6o"
cue_highway_archive_sha256="87d4f8893ef4e08f224973608ffebf94268a81380ba79c12e8841968c80aa212"
cue_highway_package_hash="N-V-__8AAGmZhABbsPJLfbqrh6JTHsXhY6qCaLAQyx25e0XE"
cue_ghostty_patch="${cue_script_dir}/../patches/ghostty-lib-vt-minimal.patch"
cue_tree_digest_script="${cue_script_dir}/tree_digest.py"

cue_ghostty_cache_root="${CUE_GHOSTTY_CACHE_ROOT:-${XDG_CACHE_HOME:-${HOME}/.cache}/cue-shell/ghostty}"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required to verify the Ghostty source tree" >&2
  exit 1
fi

cue_verify_sha256() {
  local expected_sha256="$1"
  local archive_path="$2"
  local actual_sha256

  if command -v sha256sum >/dev/null 2>&1; then
    actual_sha256="$(sha256sum "${archive_path}" | awk '{print $1}')"
  else
    actual_sha256="$(shasum -a 256 "${archive_path}" | awk '{print $1}')"
  fi
  if [[ "${actual_sha256}" != "${expected_sha256}" ]]; then
    echo "checksum mismatch for ${archive_path}: expected ${expected_sha256}, got ${actual_sha256}" >&2
    return 1
  fi
}

cue_sha256_stdin() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum | awk '{print $1}'
  else
    shasum -a 256 | awk '{print $1}'
  fi
}

if [[ -z "${CUE_ZIG_TREE_SHA256:-}" ]]; then
  echo "install-zig.sh did not export its verified tree digest" >&2
  exit 1
fi
cue_zig_closure_sha256="$(
  printf '%s\n' \
    "zig=${CUE_ZIG_TREE_SHA256}" \
    "uucode=${cue_uucode_package_hash}" \
    "zlib=${cue_zlib_package_hash}" \
    "highway=${cue_highway_package_hash}" \
    | cue_sha256_stdin
)"
cue_ghostty_source_dir="${cue_ghostty_cache_root}/source-${cue_ghostty_commit}-${cue_ghostty_patch_sha256}"
cue_ghostty_zig_cache_dir="${cue_ghostty_cache_root}/zig-cache-${cue_ghostty_commit}-${cue_zig_closure_sha256}"

mkdir -p "${cue_ghostty_cache_root}"
cue_verify_sha256 "${cue_ghostty_patch_sha256}" "${cue_ghostty_patch}"

if [[ ! -d "${cue_ghostty_source_dir}" ]]; then
  cue_ghostty_tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/cue-shell-ghostty.XXXXXX")"
  cue_ghostty_archive="${cue_ghostty_tmp_dir}/ghostty.tar.gz"
  cue_ghostty_archive_dir="ghostty-${cue_ghostty_commit}"
  cue_ghostty_source_lock="${cue_ghostty_source_dir}.prepare-lock"
  cue_ghostty_lock_acquired=false
  cue_cleanup_ghostty_source() {
    if [[ -d "${cue_ghostty_tmp_dir}" ]]; then
      chmod -R u+w "${cue_ghostty_tmp_dir}" 2>/dev/null || true
      rm -rf "${cue_ghostty_tmp_dir}"
    fi
    if [[ "${cue_ghostty_lock_acquired}" == true && -d "${cue_ghostty_source_lock}" ]]; then
      rmdir "${cue_ghostty_source_lock}" 2>/dev/null || true
    fi
  }
  trap cue_cleanup_ghostty_source EXIT

  curl --fail --location --silent --show-error --retry 3 \
    --connect-timeout 30 --max-time 120 \
    "https://github.com/ghostty-org/ghostty/archive/${cue_ghostty_commit}.tar.gz" \
    --output "${cue_ghostty_archive}"
  cue_verify_sha256 "${cue_ghostty_archive_sha256}" "${cue_ghostty_archive}"
  tar -xzf "${cue_ghostty_archive}" -C "${cue_ghostty_tmp_dir}"
  printf '%s\n' "${cue_ghostty_archive_sha256}" \
    > "${cue_ghostty_tmp_dir}/${cue_ghostty_archive_dir}/.cue-source-sha256"
  if ! command -v patch >/dev/null 2>&1; then
    echo "the patch command is required to prepare the minimal libghostty-vt source" >&2
    return 1 2>/dev/null || exit 1
  fi
  patch \
    --batch \
    --forward \
    --directory "${cue_ghostty_tmp_dir}/${cue_ghostty_archive_dir}" \
    --strip 1 \
    < "${cue_ghostty_patch}" >&2
  if ! grep -q "cue-shell embeds only libghostty-vt" \
    "${cue_ghostty_tmp_dir}/${cue_ghostty_archive_dir}/build.zig"; then
    echo "Ghostty lib-vt-only patch did not apply cleanly" >&2
    return 1 2>/dev/null || exit 1
  fi
  printf '%s\n' "${cue_ghostty_patch_sha256}" \
    > "${cue_ghostty_tmp_dir}/${cue_ghostty_archive_dir}/.cue-patch-sha256"
  cue_ghostty_staging_dir="${cue_ghostty_tmp_dir}/${cue_ghostty_archive_dir}"
  cue_ghostty_staging_sha256="$(
    python3 "${cue_tree_digest_script}" "${cue_ghostty_staging_dir}"
  )"
  if [[ "${cue_ghostty_staging_sha256}" != "${cue_ghostty_tree_sha256}" ]]; then
    echo "prepared Ghostty tree digest mismatch: expected ${cue_ghostty_tree_sha256}, got ${cue_ghostty_staging_sha256}" >&2
    return 1 2>/dev/null || exit 1
  fi

  for ((cue_ghostty_wait = 0; cue_ghostty_wait < 120; cue_ghostty_wait += 1)); do
    if mkdir "${cue_ghostty_source_lock}" 2>/dev/null; then
      cue_ghostty_lock_acquired=true
      break
    fi
    if [[ -f "${cue_ghostty_source_dir}/build.zig" ]] \
      && [[ "$(cat "${cue_ghostty_source_dir}/.cue-source-sha256" 2>/dev/null || true)" == "${cue_ghostty_archive_sha256}" ]] \
      && [[ "$(cat "${cue_ghostty_source_dir}/.cue-patch-sha256" 2>/dev/null || true)" == "${cue_ghostty_patch_sha256}" ]] \
      && cue_verify_sha256 "${cue_ghostty_patched_build_sha256}" "${cue_ghostty_source_dir}/build.zig" >/dev/null 2>&1 \
      && [[ "$(python3 "${cue_tree_digest_script}" "${cue_ghostty_source_dir}" 2>/dev/null || true)" == "${cue_ghostty_tree_sha256}" ]]; then
      break
    fi
    sleep 1
  done

  if [[ "${cue_ghostty_lock_acquired}" == true ]]; then
    if [[ ! -d "${cue_ghostty_source_dir}" ]]; then
      if ! mv "${cue_ghostty_staging_dir}" "${cue_ghostty_source_dir}"; then
        echo "failed to publish prepared Ghostty source" >&2
        return 1 2>/dev/null || exit 1
      fi
      if ! chmod -R a-w "${cue_ghostty_source_dir}"; then
        chmod -R u+w "${cue_ghostty_source_dir}" 2>/dev/null || true
        rm -rf "${cue_ghostty_source_dir}"
        echo "failed to protect prepared Ghostty source" >&2
        return 1 2>/dev/null || exit 1
      fi
    fi
  fi
  cue_cleanup_ghostty_source
  trap - EXIT
  unset -f cue_cleanup_ghostty_source
fi

if [[ ! -f "${cue_ghostty_source_dir}/build.zig" ]] \
  || [[ "$(cat "${cue_ghostty_source_dir}/.cue-source-sha256" 2>/dev/null || true)" != "${cue_ghostty_archive_sha256}" ]] \
  || [[ "$(cat "${cue_ghostty_source_dir}/.cue-patch-sha256" 2>/dev/null || true)" != "${cue_ghostty_patch_sha256}" ]]; then
  echo "cached Ghostty source failed verification: ${cue_ghostty_source_dir}" >&2
  return 1 2>/dev/null || exit 1
fi
cue_verify_sha256 \
  "${cue_ghostty_patched_build_sha256}" \
  "${cue_ghostty_source_dir}/build.zig"
cue_ghostty_cached_tree_sha256="$(
  python3 "${cue_tree_digest_script}" "${cue_ghostty_source_dir}"
)"
if [[ "${cue_ghostty_cached_tree_sha256}" != "${cue_ghostty_tree_sha256}" ]]; then
  echo "cached Ghostty tree failed verification: expected ${cue_ghostty_tree_sha256}, got ${cue_ghostty_cached_tree_sha256}" >&2
  return 1 2>/dev/null || exit 1
fi

cue_prepare_zig_package() {
  local package_name="$1"
  local package_url="$2"
  local archive_sha256="$3"
  local package_hash="$4"
  local package_dir="${cue_ghostty_zig_cache_dir}/p/${package_hash}"

  if [[ ! -d "${package_dir}" ]]; then
    local package_lock="${package_dir}.prepare-lock"
    local package_lock_acquired=false
    local package_wait
    mkdir -p "${cue_ghostty_zig_cache_dir}/p"
    for ((package_wait = 0; package_wait < 120; package_wait += 1)); do
      if mkdir "${package_lock}" 2>/dev/null; then
        package_lock_acquired=true
        break
      fi
      if [[ -d "${package_dir}" && ! -d "${package_lock}" ]]; then
        break
      fi
      sleep 1
    done
    if [[ "${package_lock_acquired}" == false && ! -d "${package_dir}" ]]; then
      echo "timed out waiting for ${package_name} package preparation" >&2
      return 1
    fi

    if [[ "${package_lock_acquired}" == true && ! -d "${package_dir}" ]]; then
      if ! (
        set -euo pipefail
        package_tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/cue-shell-${package_name}.XXXXXX")"
        trap 'rm -rf "${package_tmp_dir}"' EXIT
        package_archive="${package_tmp_dir}/${package_name}.tar.gz"
        curl --fail --location --silent --show-error --retry 3 \
          --connect-timeout 30 --max-time 120 \
          "${package_url}" \
          --output "${package_archive}"
        cue_verify_sha256 "${archive_sha256}" "${package_archive}"
        fetched_hash="$(
          zig fetch \
            --global-cache-dir "${cue_ghostty_zig_cache_dir}" \
            "${package_archive}"
        )"
        if [[ "${fetched_hash}" != "${package_hash}" ]]; then
          echo "unexpected ${package_name} Zig package hash: ${fetched_hash}" >&2
          exit 1
        fi
        chmod -R a-w "${package_dir}"
      ); then
        rmdir "${package_lock}"
        return 1
      fi
    fi
    if [[ "${package_lock_acquired}" == true ]]; then
      rmdir "${package_lock}"
    fi
  fi

  local verify_cache_dir
  local verified_hash
  verify_cache_dir="$(mktemp -d "${TMPDIR:-/tmp}/cue-shell-zig-verify.XXXXXX")"
  verified_hash="$(
    zig fetch \
      --global-cache-dir "${verify_cache_dir}" \
      "${package_dir}"
  )"
  rm -rf "${verify_cache_dir}"
  if [[ "${verified_hash}" != "${package_hash}" ]]; then
    echo "cached ${package_name} package failed verification: ${package_dir}" >&2
    return 1
  fi
}

cue_prepare_zig_package \
  "uucode" \
  "https://deps.files.ghostty.org/uucode-0.2.0-ZZjBPqZVVABQepOqZHR7vV_NcaN-wats0IB6o-Exj6m9.tar.gz" \
  "${cue_uucode_archive_sha256}" \
  "${cue_uucode_package_hash}"
cue_prepare_zig_package \
  "zlib" \
  "https://deps.files.ghostty.org/zlib-1220fed0c74e1019b3ee29edae2051788b080cd96e90d56836eea857b0b966742efb.tar.gz" \
  "${cue_zlib_archive_sha256}" \
  "${cue_zlib_package_hash}"
cue_prepare_zig_package \
  "highway" \
  "https://deps.files.ghostty.org/highway-66486a10623fa0d72fe91260f96c892e41aceb06.tar.gz" \
  "${cue_highway_archive_sha256}" \
  "${cue_highway_package_hash}"

export GHOSTTY_SOURCE_DIR="${cue_ghostty_source_dir}"
export GHOSTTY_ZIG_SYSTEM_DIR="${cue_ghostty_zig_cache_dir}/p"

echo "using Ghostty ${cue_ghostty_commit} from ${GHOSTTY_SOURCE_DIR}" >&2
echo "using offline Zig package closure from ${GHOSTTY_ZIG_SYSTEM_DIR}" >&2

if [[ "${1:-}" == "--emit-env" ]]; then
  cue_emit_export() {
    local name="$1"
    local value="$2"
    value="${value//\'/\'\\\'\'}"
    printf "export %s='%s'\n" "${name}" "${value}"
  }
  cue_emit_export PATH "${PATH}"
  cue_emit_export GHOSTTY_SOURCE_DIR "${GHOSTTY_SOURCE_DIR}"
  cue_emit_export GHOSTTY_ZIG_SYSTEM_DIR "${GHOSTTY_ZIG_SYSTEM_DIR}"
  if [[ -n "${CUE_ZIG_MACOS_SDK:-}" ]]; then
    cue_emit_export CUE_ZIG_MACOS_SDK "${CUE_ZIG_MACOS_SDK}"
  fi
  if [[ -n "${SDKROOT:-}" ]]; then
    cue_emit_export SDKROOT "${SDKROOT}"
  fi
fi
