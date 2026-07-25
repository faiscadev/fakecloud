#!/usr/bin/env bash
#
# Tests for scripts/publish-crates.sh — the crates.io rate-limit handling in
# particular, which is otherwise only exercised by a real release.
#
# Hermetic: `cargo publish` and the sparse-index `curl` are both stubbed on PATH,
# so nothing here touches crates.io. `cargo metadata` is forwarded to the real
# cargo (the script validates the publish list against the workspace on every
# run). MAX_SLEEP=1 clamps every wait to a second so a test that would idle for a
# rate-limit refill finishes immediately.
#
#   bash scripts/tests/publish-crates-test.sh
set -uo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
SCRIPT="$ROOT/scripts/publish-crates.sh"
CARGO_REAL=$(command -v cargo) || {
  echo "cargo not found" >&2
  exit 1
}
export CARGO_REAL

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
STUB="$TMP/bin"
mkdir -p "$STUB"

export STUB_STATE="$TMP/index"   # one "<crate>@<vers>" or "<crate>@EXISTS" per line
export STUB_COUNT="$TMP/calls"   # number of `cargo publish` invocations
# The script refuses to publish a version other than the checked-out one, so the
# stub registry speaks the workspace version.
STUB_VERSION=$(grep -m1 '^version' "$ROOT/Cargo.toml" | sed 's/.*"\(.*\)".*/\1/')
export STUB_VERSION

# Stub sparse index. Reports the published version, an older version (the crate
# exists but not at this version), or 404 (curl exit 22, crate unknown).
cat > "$STUB/curl" <<'STUBEOF'
#!/usr/bin/env bash
url=""
for arg in "$@"; do case "$arg" in https://*) url="$arg" ;; esac; done
name="${url##*/}"
if grep -qx "${name}@${STUB_VERSION}" "$STUB_STATE" 2>/dev/null; then
  printf '{"name":"%s","vers":"%s","deps":[]}\n' "$name" "$STUB_VERSION"
  exit 0
fi
if grep -qx "${name}@EXISTS" "$STUB_STATE" 2>/dev/null; then
  printf '{"name":"%s","vers":"0.0.1","deps":[]}\n' "$name"
  exit 0
fi
exit 22
STUBEOF

# Stub cargo. Fails invocations (SKIP, SKIP+TIMES] with the chosen error, then
# succeeds and records the publish in the stub index.
cat > "$STUB/cargo" <<'STUBEOF'
#!/usr/bin/env bash
[ "${1:-}" = metadata ] && exec "$CARGO_REAL" "$@"
name="$3"
n=$(cat "$STUB_COUNT" 2>/dev/null || echo 0)
n=$((n + 1))
echo "$n" > "$STUB_COUNT"
if [ "$n" -gt "${STUB_FAIL_SKIP:-0}" ] &&
  [ "$n" -le $((${STUB_FAIL_SKIP:-0} + ${STUB_FAIL_TIMES:-0})) ]; then
  case "${STUB_FAIL_MODE:-429}" in
    429)
      when=$(date -u -d "+${STUB_RETRY_IN:-30} seconds" +'%a, %d %b %Y %H:%M:%S GMT' 2>/dev/null ||
        date -u -v"+${STUB_RETRY_IN:-30}S" +'%a, %d %b %Y %H:%M:%S GMT')
      echo "error: failed to publish $name to registry at https://crates.io"
      echo "Caused by:"
      echo "  the remote server responded with an error (status 429 Too Many Requests):" \
        "You have published too many updates to existing crates in a short period of time." \
        "Please try again after $when and see https://crates.io/docs/rate-limits for more details."
      exit 1
      ;;
    429new)
      when=$(date -u -d "+${STUB_RETRY_IN:-30} seconds" +'%a, %d %b %Y %H:%M:%S GMT' 2>/dev/null ||
        date -u -v"+${STUB_RETRY_IN:-30}S" +'%a, %d %b %Y %H:%M:%S GMT')
      echo "error: failed to publish $name to registry at https://crates.io"
      echo "  the remote server responded with an error (status 429 Too Many Requests):" \
        "You have published too many new crates in a short period of time." \
        "Please try again after $when and see https://crates.io/docs/rate-limits for more details."
      exit 1
      ;;
    429bare)
      echo "error: the remote server responded with an error (status 429 Too Many Requests): slow down"
      exit 1
      ;;
    503)
      echo "error: failed to publish $name"
      echo "Caused by: the remote server responded with an error (status 503 Service Unavailable)"
      exit 1
      ;;
    stale-index)
      echo "error: failed to prepare local package for uploading"
      echo "Caused by: failed to select a version for the requirement \`fakecloud-core = \"^$STUB_VERSION\"\`"
      exit 1
      ;;
    fatal)
      echo "error: failed to verify package tarball"
      echo "Caused by: mismatched types: expected struct Foo"
      exit 1
      ;;
    silent-success)
      # crates.io accepted the upload but cargo still failed (it gave up waiting
      # for the version to appear in the index).
      echo "$name@$STUB_VERSION" >> "$STUB_STATE"
      echo "error: timed out waiting for $name to be available in the registry"
      exit 1
      ;;
  esac
fi
echo "$name@$STUB_VERSION" >> "$STUB_STATE"
echo "    Uploading $name v$STUB_VERSION"
exit 0
STUBEOF

chmod +x "$STUB/curl" "$STUB/cargo"

FAILURES=0
OUT="$TMP/out"

# run <index-state-lines> [env=value ...] -- <script args>
run() {
  printf '%s\n' "$1" > "$STUB_STATE"
  : > "$STUB_COUNT"
  shift
  env PATH="$STUB:$PATH" MAX_SLEEP=1 "$@" bash "$SCRIPT" "$STUB_VERSION" > "$OUT" 2>&1
  echo $?
}

check() { # check <label> <condition-description> <0|1 result>
  if [ "$3" = 0 ]; then
    printf 'ok   %s: %s\n' "$1" "$2"
  else
    printf 'FAIL %s: %s\n' "$1" "$2"
    FAILURES=$((FAILURES + 1))
    sed 's/^/       | /' "$OUT"
  fi
}

saw() { grep -q "$1" "$OUT"; }

echo "== publish-crates.sh =="

# A single 429 is waited out and the crate is retried, not failed.
code=$(run 'fakecloud@EXISTS' START_AT=fakecloud STUB_FAIL_TIMES=1 STUB_FAIL_MODE=429)
check A "429 retried to success" "$([ "$code" = 0 ] && saw 'rate limit' && saw 'published fakecloud' && echo 0 || echo 1)"

# Repeated 429s teach the script the refill interval, which then paces the next
# crate instead of waiting for another rejection.
code=$(run 'fakecloud-cloudformation@EXISTS
fakecloud-cloudcontrol@EXISTS
fakecloud@EXISTS' START_AT=fakecloud-cloudformation STUB_FAIL_SKIP=1 STUB_FAIL_TIMES=2 STUB_FAIL_MODE=429)
check B "refill interval learned from 429s and used to pace" \
  "$([ "$code" = 0 ] && saw 'learned crates.io existing-crate limit' && saw 'pacing inside the existing-crate rate limit' && echo 0 || echo 1)"

# A new-crate 429 is attributed to the new-crate bucket, not the existing one.
code=$(run '' START_AT=fakecloud STUB_FAIL_TIMES=1 STUB_FAIL_MODE=429new)
check C "new-crate 429 attributed to the new-crate bucket" \
  "$([ "$code" = 0 ] && saw 'new-crate rate limit' && echo 0 || echo 1)"

# A 429 without a parseable instant still backs off instead of failing.
code=$(run 'fakecloud@EXISTS' START_AT=fakecloud STUB_FAIL_TIMES=1 STUB_FAIL_MODE=429bare)
check D "429 with no retry instant falls back to a fixed wait" \
  "$([ "$code" = 0 ] && saw 'without a parseable retry instant' && saw 'published fakecloud' && echo 0 || echo 1)"

# Already-indexed versions are skipped without calling the API at all — a
# republish attempt would consume a rate-limit token for nothing.
code=$(run "fakecloud@$STUB_VERSION" START_AT=fakecloud)
check E "already-published version skipped without publishing" \
  "$([ "$code" = 0 ] && saw 'already published — skipping' && [ ! -s "$STUB_COUNT" ] && echo 0 || echo 1)"

# cargo failing after crates.io accepted the upload is not a release failure.
code=$(run 'fakecloud@EXISTS' START_AT=fakecloud STUB_FAIL_TIMES=1 STUB_FAIL_MODE=silent-success)
check F "upload that landed despite a cargo error is treated as published" \
  "$([ "$code" = 0 ] && saw 'landed on crates.io despite the error' && echo 0 || echo 1)"

# A transient registry error is retried with backoff.
code=$(run 'fakecloud@EXISTS' START_AT=fakecloud STUB_FAIL_TIMES=1 STUB_FAIL_MODE=503)
check G "503 retried to success" \
  "$([ "$code" = 0 ] && saw 'transient registry error' && saw 'published fakecloud' && echo 0 || echo 1)"

# ...but not forever: a persistent error surfaces instead of looping for an hour.
code=$(run 'fakecloud@EXISTS' START_AT=fakecloud STUB_FAIL_TIMES=99 STUB_FAIL_MODE=503 MAX_TRANSIENT=2)
check H "persistent transient error gives up at MAX_TRANSIENT" \
  "$([ "$code" = 1 ] && saw '::error::failed to publish fakecloud' && [ "$(cat "$STUB_COUNT")" = 3 ] && echo 0 || echo 1)"

# A dependency the CDN-cached index has not caught up on yet is retried, not
# treated as a release-ending ordering bug.
code=$(run 'fakecloud@EXISTS' START_AT=fakecloud STUB_FAIL_TIMES=1 STUB_FAIL_MODE=stale-index)
check I2 "stale index read on a just-published dep is retried" \
  "$([ "$code" = 0 ] && saw 'failed to select a version' && saw 'published fakecloud' && echo 0 || echo 1)"

# A real failure is never retried or swallowed.
code=$(run 'fakecloud@EXISTS' START_AT=fakecloud STUB_FAIL_TIMES=1 STUB_FAIL_MODE=fatal)
check I "genuine publish failure aborts the release" \
  "$([ "$code" = 1 ] && saw 'mismatched types' && [ "$(cat "$STUB_COUNT")" = 1 ] && echo 0 || echo 1)"

# Running out of wall clock reports what is left and how to resume.
code=$(run 'fakecloud@EXISTS' START_AT=fakecloud DEADLINE_MINUTES=0 STUB_FAIL_TIMES=1 STUB_FAIL_MODE=429)
check J "deadline exit names the remaining crates and the resume command" \
  "$([ "$code" = 1 ] && saw 'not yet published: fakecloud' && saw 'gh workflow run release.yml' && echo 0 || echo 1)"

echo
if [ "$FAILURES" -ne 0 ]; then
  echo "$FAILURES test(s) failed"
  exit 1
fi
echo "all tests passed"
