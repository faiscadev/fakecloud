#!/usr/bin/env bash
# Check that a release binary runs on every host the release supports, then
# smoke-run it when the runner can execute it.
#
# - Linux glibc: may only link glibc's own shared objects and reference
#   symbol versions up to GLIBC_2.17.
# - Linux musl: must be fully static.
# - macOS: may only link system libraries (/usr/lib, /System), never a
#   Homebrew or other runner-local dylib.
#
# Usage: scripts/verify-release-binary.sh <path-to-binary> <rust-target-triple>
# Called from build-binaries.yml for both the stripped release binary and the
# release-debug binary.
set -euo pipefail

if [ $# -ne 2 ]; then
  echo "usage: $0 <path-to-binary> <rust-target-triple>" >&2
  exit 2
fi
BIN="$1"
TARGET="$2"

fail() {
  echo "::error::$BIN: $*"
  exit 1
}

file "$BIN"
case "$TARGET" in
  *-linux-*)
    NEEDED="$(readelf -d "$BIN" | sed -n 's/.*Shared library: \[\(.*\)\]/\1/p')"
    echo "NEEDED: ${NEEDED:-<none>}"
    BAD="$(printf '%s\n' "$NEEDED" | grep -vE '^(libc|libm|libdl|libpthread|librt|libgcc_s|ld-linux-(x86-64|aarch64))\.so' | grep . || true)"
    [ -z "$BAD" ] || fail "unexpected shared library dependencies: $BAD"
    case "$TARGET" in
      *-musl)
        [ -z "$NEEDED" ] || fail "musl build is not fully static"
        ;;
      *-gnu)
        MAX="$(objdump -T "$BIN" | grep -o 'GLIBC_[0-9.]*' | sed 's/GLIBC_//' | sort -uV | tail -1 || true)"
        [ -n "$MAX" ] || fail "no GLIBC symbol versions found; is this a dynamically linked glibc binary?"
        echo "max GLIBC symbol version: $MAX"
        [ "$(printf '%s\n2.17\n' "$MAX" | sort -V | tail -1)" = "2.17" ] \
          || fail "requires GLIBC_$MAX (> 2.17)"
        ;;
      *) fail "unknown Linux target $TARGET" ;;
    esac
    ;;
  *-apple-darwin)
    # First line of otool -L is the binary itself.
    LIBS="$(otool -L "$BIN" | tail -n +2 | awk '{print $1}')"
    printf 'linked:\n%s\n' "$LIBS"
    BAD="$(printf '%s\n' "$LIBS" | grep -vE '^(/usr/lib/|/System/Library/)' | grep . || true)"
    [ -z "$BAD" ] || fail "links non-system libraries: $BAD"
    ;;
  *) fail "unknown target $TARGET" ;;
esac

# Smoke-run only when the runner's architecture matches (the x86_64 macOS
# leg is cross-built on an arm64 runner).
case "$(uname -m)" in
  x86_64 | amd64) HOST_ARCH=x86_64 ;;
  arm64 | aarch64) HOST_ARCH=aarch64 ;;
  *) HOST_ARCH="$(uname -m)" ;;
esac
if [ "${TARGET%%-*}" = "$HOST_ARCH" ]; then
  "$BIN" --version
else
  echo "skipping --version: $TARGET does not run natively on $(uname -m)"
fi
