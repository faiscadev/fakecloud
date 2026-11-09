#!/usr/bin/env bash
# Check that a Linux release binary runs on every host the release supports:
# glibc builds may only link glibc's own shared objects and reference symbol
# versions up to GLIBC_2.17; musl builds must be fully static.
#
# Usage: scripts/verify-linux-binary.sh <path-to-binary> <rust-target-triple>
# Used by release.yml and release-build-check.yml for both the stripped
# release binary and the release-debug binary.
set -euo pipefail

BIN="$1"
TARGET="$2"

file "$BIN"
NEEDED="$(readelf -d "$BIN" | sed -n 's/.*Shared library: \[\(.*\)\]/\1/p')"
echo "NEEDED: ${NEEDED:-<none>}"
BAD="$(printf '%s\n' "$NEEDED" | grep -vE '^(libc|libm|libdl|libpthread|librt|libgcc_s|ld-linux-(x86-64|aarch64))\.so' | grep . || true)"
if [ -n "$BAD" ]; then
  echo "::error::$BIN: unexpected shared library dependencies: $BAD"
  exit 1
fi
case "$TARGET" in
  *-musl)
    if [ -n "$NEEDED" ]; then
      echo "::error::$BIN: musl build is not fully static"
      exit 1
    fi
    ;;
  *-gnu)
    MAX="$(objdump -T "$BIN" | grep -o 'GLIBC_[0-9.]*' | sed 's/GLIBC_//' | sort -uV | tail -1)"
    echo "max GLIBC symbol version: $MAX"
    if [ "$(printf '%s\n2.17\n' "$MAX" | sort -V | tail -1)" != "2.17" ]; then
      echo "::error::$BIN requires GLIBC_$MAX (> 2.17)"
      exit 1
    fi
    ;;
  *)
    echo "::error::unknown target $TARGET"
    exit 1
    ;;
esac
"$BIN" --version
