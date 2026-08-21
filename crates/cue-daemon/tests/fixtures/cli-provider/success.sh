#!/bin/sh
set -eu

case "${1:-}" in
  probe)
    printf '%s\n' '{"units":[{"id":"pool","attrs":{"free":{"kind":"count","value":3}}}]}'
    ;;
  reserve)
    input=$(cat)
    case "$input" in
      *'"step_id":"E7/S1"'* ) ;;
      *) echo "missing step_id E7/S1" >&2; exit 42 ;;
    esac
    case "$input" in
      *'"license":{"kind":"count","value":1}'* ) ;;
      *) echo "missing license need" >&2; exit 43 ;;
    esac
    printf '%s\n' '{"ok":true,"grant_id":"g1","env":{"LICENSE_TOKEN":"abc"},"info":{"license":{"kind":"count","value":1}}}'
    ;;
  release)
    cat >/dev/null
    printf '%s\n' '{}'
    ;;
  *)
    echo "usage: $0 probe|reserve|release" >&2
    exit 64
    ;;
esac
