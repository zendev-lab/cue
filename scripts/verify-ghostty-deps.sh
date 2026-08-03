#!/usr/bin/env bash
#
# Keep cue-shell on the audited static libghostty-vt dependency contract.

set -euo pipefail

python3 scripts/tree_digest.py --self-test

feature_tree="$(cargo tree --locked -e features -i libghostty-vt-sys)"
printf '%s\n' "${feature_tree}"

if ! grep -Fq "libghostty-vt-sys v0.2.1" <<< "${feature_tree}"; then
  echo "expected exactly libghostty-vt-sys 0.2.1" >&2
  exit 1
fi
if ! grep -Fq 'libghostty-vt-sys feature "vendored"' <<< "${feature_tree}"; then
  echo "libghostty-vt-sys must use the vendored static build" >&2
  exit 1
fi

for forbidden_feature in kitty-graphics link-dynamic pkg-config; do
  if grep -Fq "feature \"${forbidden_feature}\"" <<< "${feature_tree}"; then
    echo "forbidden libghostty-vt feature enabled: ${forbidden_feature}" >&2
    exit 1
  fi
done
