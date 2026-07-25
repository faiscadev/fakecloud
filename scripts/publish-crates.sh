#!/usr/bin/env bash
#
# Publish every publishable workspace crate to crates.io, in dependency order,
# respecting crates.io's rate limits.
#
# crates.io meters publishes with two independent token buckets: one for brand
# new crates (slow — minutes per token) and one for new versions of crates that
# already exist (faster, but a 100+ crate workspace still drains it partway
# through a release). When a bucket is empty the API answers 429 and names the
# exact instant the next token frees up:
#
#   the remote server responded with an error (status 429 Too Many Requests):
#   You have published too many updates to existing crates in a short period of
#   time. Please try again after Sat, 25 Jul 2026 12:35:41 GMT and see
#   https://crates.io/docs/rate-limits for more details.
#
# So nothing here hardcodes crates.io's limits. On a 429 the script parses that
# instant, sleeps until it passes, and retries the same crate; it also learns
# the bucket's refill interval from the 429 (next-token instant minus the last
# accepted upload of that kind) and paces subsequent publishes by it, so the
# steady state is one upload attempt per crate instead of an attempt plus a
# rejection. Each new 429 refines the estimate upward.
#
# Idempotent and resumable: a crate whose version is already in the sparse index
# is skipped without touching the API. That matters for more than speed — a
# republish attempt is metered *before* crates.io notices the version already
# exists, so blindly re-running a partially-finished release burns the whole
# bucket on no-ops and then 429s forever. Re-run after any failure and the
# script picks up where it stopped.
#
# Usage:
#   scripts/publish-crates.sh <version>   Publish/resume version <version>.
#   scripts/publish-crates.sh --check     Validate the crate list against the
#                                         workspace (membership + topo order).
#                                         No network, no publishing.
#
# Environment:
#   CARGO_REGISTRY_TOKEN  crates.io token (required to publish).
#   DRY_RUN=1             Log what would happen; never call cargo publish.
#   DEADLINE_MINUTES=330  Give up (with a resume hint) before a GitHub Actions
#                         job would be killed at its 6h ceiling.
#   MAX_ATTEMPTS=40       Per-crate attempt cap.
#   MAX_TRANSIENT=6       Per-crate cap on retries of non-429 registry errors.
#   MAX_SLEEP=1800        Cap on any single wait, in seconds.
#   MAX_REFILL=3600       Sanity bound on a learned rate-limit interval.
#   START_AT=<crate>      Skip ahead to this crate (manual recovery).
#
# Publish order is hand-maintained: a crate must come after every workspace
# crate it depends on, because crates.io resolves the dependency from the index
# at publish time. `--check` enforces that, so CI fails on a misplaced or
# missing crate instead of a release failing halfway through.
set -uo pipefail

CRATES=(
  # Layer 1: no internal deps
  fakecloud-aws
  fakecloud-sdk
  fakecloud-persistence
  fakecloud-k8s   # only external deps (kube/k8s-openapi); dependents: lambda/elasticache/rds/ecs/server

  # Layer 2: depends on fakecloud-aws
  fakecloud-core

  # Layer 3a: depend on aws + core + persistence only
  fakecloud-sqs
  fakecloud-iam
  fakecloud-organizations
  fakecloud-ec2            # depends on aws + core only
  fakecloud-lambda
  fakecloud-logs
  fakecloud-kms
  fakecloud-ses
  fakecloud-rds
  fakecloud-rds-data        # depends on rds
  fakecloud-dsql
  fakecloud-resource-groups  # only core/persistence/aws deps
  fakecloud-resource-groups-tagging  # only core/persistence/aws deps
  fakecloud-elasticbeanstalk # only core/persistence/aws deps
  fakecloud-memorydb         # only core/persistence/aws deps
  fakecloud-kinesisanalyticsv2 # only core/persistence/aws deps
  fakecloud-eks              # only core/persistence/aws deps
  fakecloud-efs              # depends on ec2 (subnet AZ/VPC resolution)
  fakecloud-mq               # only core/persistence/aws deps
  fakecloud-kafka            # only core/persistence deps
  fakecloud-mwaa             # only core/persistence deps
  fakecloud-fis              # only core/persistence deps
  fakecloud-xray             # only core/persistence deps
  fakecloud-appsync          # only core/persistence deps
  fakecloud-amplify          # only core/persistence deps
  fakecloud-mediaconvert     # only core/persistence deps
  fakecloud-serverlessrepo   # only core/persistence deps
  fakecloud-iotdata          # only core/persistence deps
  fakecloud-pinpoint         # only core/persistence deps
  fakecloud-iot              # only core/persistence deps
  fakecloud-iotwireless      # only core/persistence deps
  fakecloud-sagemaker        # only core/persistence deps
  fakecloud-managedblockchain # only core/persistence/aws deps
  fakecloud-servicediscovery # only core/persistence/aws deps
  fakecloud-account          # only core/persistence/aws deps
  fakecloud-identitystore    # only core/persistence/aws deps
  fakecloud-ssoadmin         # only core/persistence/aws deps
  fakecloud-verifiedpermissions # core/persistence/aws + cedar-policy
  fakecloud-redshift         # only core/persistence/aws deps
  fakecloud-dms              # only core/persistence/aws deps
  fakecloud-docdb            # only core/persistence/aws deps
  fakecloud-neptune          # only core/persistence/aws deps
  fakecloud-opensearch       # only core/persistence/aws deps (ES + OpenSearch)
  fakecloud-backup           # only core/persistence/aws deps
  fakecloud-glacier          # only core/persistence/aws deps
  fakecloud-transfer         # only core/persistence/aws deps
  fakecloud-appconfig        # only core/persistence/aws deps
  fakecloud-cloudtrail       # only core/persistence/aws deps
  fakecloud-ram               # only core/persistence/aws deps
  fakecloud-ce                # only core/persistence/aws deps
  fakecloud-s3tables          # only core/persistence/aws deps
  fakecloud-lakeformation     # only core/persistence/aws deps
  fakecloud-codeconnections   # only core/persistence/aws deps
  fakecloud-codecommit        # only core/persistence/aws deps
  fakecloud-codedeploy        # only core/persistence/aws deps
  fakecloud-codepipeline      # only core/persistence/aws deps
  fakecloud-codeartifact      # only core/persistence/aws deps
  fakecloud-emr               # only core/persistence/aws deps
  fakecloud-textract          # only core/persistence/aws deps
  fakecloud-transcribe          # only core/persistence/aws deps
  fakecloud-translate           # only core/persistence/aws deps
  fakecloud-shield              # only core/persistence/aws deps
  fakecloud-comprehend             # only core/persistence/aws deps
  fakecloud-swf                    # only core/persistence/aws deps
  fakecloud-timestream             # only core/persistence/aws deps
  fakecloud-support             # only core/persistence/aws deps
  fakecloud-kinesis
  fakecloud-scheduler
  fakecloud-bedrock
  fakecloud-cloudfront
  fakecloud-acm
  fakecloud-application-autoscaling
  fakecloud-autoscaling
  fakecloud-wafv2
  fakecloud-cloudwatch
  fakecloud-glue
  fakecloud-bedrock-agent

  # Layer 3b: depend on layer 3a
  fakecloud-sns            # depends on ses
  fakecloud-secretsmanager # depends on iam
  fakecloud-cognito        # depends on iam
  fakecloud-elbv2          # depends on wafv2
  fakecloud-apigatewayv2   # depends on wafv2
  fakecloud-bedrock-agent-runtime  # depends on bedrock-agent
  fakecloud-eventbridge    # depends on iam, lambda, logs

  # Layer 4: depend on layer 3a/3b crates
  fakecloud-s3             # depends on kms
  fakecloud-elasticache    # depends on s3
  fakecloud-route53resolver  # depends on ec2, s3 (ImportFirewallDomains)
  fakecloud-acmpca         # depends on s3 (audit-report delivery), acm, kms
  fakecloud-ecr            # depends on iam, kms
  fakecloud-ssm            # depends on secretsmanager
  fakecloud-codebuild      # depends on secretsmanager, ssm, logs (real buildspec exec)
  fakecloud-apigateway     # depends on elbv2, wafv2

  # Layer 5: depend on layer 4
  fakecloud-dynamodb       # depends on s3
  fakecloud-firehose       # depends on s3
  fakecloud-athena         # depends on glue, s3
  fakecloud-route53        # depends on cloudfront, elbv2, logs, s3
  fakecloud-ecs            # depends on logs, secretsmanager, ssm
  fakecloud-batch          # depends on ecs (drives ECS for real job execution)
  fakecloud-config         # depends on s3, iam, ec2, lambda (records cross-service state)

  # Layer 6: depends on layer 5
  fakecloud-stepfunctions  # depends on dynamodb

  # EventBridge Pipes: control plane depends only on core/aws/persistence,
  # but cloudformation provisions AWS::Pipes::Pipe, so publish it before
  # cloudformation.
  fakecloud-pipes

  # Layer 7: depends on all services
  fakecloud-cloudformation

  # Layer 8: depends on cloudformation (drives its resource provisioners)
  fakecloud-cloudcontrol

  # Layer 9: the binary
  fakecloud
)

readonly INDEX_HOST="https://index.crates.io"

# cargo publish -p and cargo metadata both need the workspace root.
SELF=$(cd "$(dirname "$0")" && pwd)/$(basename "$0")
readonly SELF
cd "$(dirname "$SELF")/.." || exit 1

DEADLINE_MINUTES=${DEADLINE_MINUTES:-330}
MAX_ATTEMPTS=${MAX_ATTEMPTS:-40}
MAX_TRANSIENT=${MAX_TRANSIENT:-6}
MAX_SLEEP=${MAX_SLEEP:-1800}
# Sanity bound on a learned refill interval, so a nonsense timestamp cannot
# stall the release. Deliberately separate from MAX_SLEEP, which caps one sleep.
MAX_REFILL=${MAX_REFILL:-3600}
DRY_RUN=${DRY_RUN:-0}
START_AT=${START_AT:-}
# Only used when a 429 arrives without a parseable "try again after" instant.
FALLBACK_WAIT_EXISTING=${FALLBACK_WAIT_EXISTING:-60}
FALLBACK_WAIT_NEW=${FALLBACK_WAIT_NEW:-600}

DEADLINE=$(( $(date -u +%s) + DEADLINE_MINUTES * 60 ))
VERSION=""

# Seconds between rate-limit tokens, learned from 429s. 0 = never observed, so
# no pacing yet — the first publishes run at full speed off the burst budget.
refill_existing=0
refill_new=0
# Epoch of the last upload crates.io accepted from each bucket.
last_existing=0
last_new=0

# Crates still to go, for the resume hint if we run out of wall clock. Seeded
# with the full list because bash 3.2 (macOS, for a local publish) treats
# ${remaining[*]} on an empty array as an unbound variable under `set -u`.
remaining=("${CRATES[@]}")

now() { date -u +%s; }
log() { printf '%s  %s\n' "$(date -u +%H:%M:%S)" "$*"; }
die() { printf '::error::%s\n' "$*" >&2; exit 1; }

refill_of() { case "$1" in new) printf '%s' "$refill_new" ;; *) printf '%s' "$refill_existing" ;; esac; }
last_of() { case "$1" in new) printf '%s' "$last_new" ;; *) printf '%s' "$last_existing" ;; esac; }
set_refill() { case "$1" in new) refill_new=$2 ;; *) refill_existing=$2 ;; esac; }
set_last() { case "$1" in new) last_new=$2 ;; *) last_existing=$2 ;; esac; }

# Sparse-index path layout: 1/x, 2/xy, 3/x/xyz, then xx/yy/name.
index_url() {
  local n=$1
  case ${#n} in
    1) printf '%s/1/%s' "$INDEX_HOST" "$n" ;;
    2) printf '%s/2/%s' "$INDEX_HOST" "$n" ;;
    3) printf '%s/3/%s/%s' "$INDEX_HOST" "${n:0:1}" "$n" ;;
    *) printf '%s/%s/%s/%s' "$INDEX_HOST" "${n:0:2}" "${n:2:2}" "$n" ;;
  esac
}

INDEX_BODY=""

# Fetch a crate's index entry into INDEX_BODY. 0 = fetched, 1 = the registry
# says the crate does not exist, 2 = the index was unreachable. The three cases
# are kept distinct because "unknown" must not be mistaken for "new": pacing a
# 100-crate release against the new-crate bucket would idle it for hours.
fetch_index() {
  local n=$1 attempt code
  for attempt in 1 2 3; do
    INDEX_BODY=$(curl -sf --max-time 30 -H 'cache-control: no-cache' "$(index_url "$n")" 2>/dev/null)
    code=$?
    case $code in
      0) return 0 ;;
      22) return 1 ;; # HTTP >= 400 under curl -f: not in the index
      *) sleep $((attempt * 3)) ;;
    esac
  done
  INDEX_BODY=""
  return 2
}

# published | existing | new | unknown
index_state() {
  local rc
  fetch_index "$1"
  rc=$?
  case $rc in
    2) printf 'unknown' ;;
    1) printf 'new' ;;
    *)
      if printf '%s' "$INDEX_BODY" | grep -Fq "\"vers\":\"$2\""; then
        printf 'published'
      else
        printf 'existing'
      fi
      ;;
  esac
}

deadline_exit() {
  printf '::error::out of wall clock after %s minutes (%s)\n' "$DEADLINE_MINUTES" "$1" >&2
  printf '::error::%s crate(s) not yet published: %s\n' "${#remaining[@]}" "${remaining[*]}" >&2
  printf '::error::this is resumable — re-run the crates publish for the same tag and it will skip everything already indexed:\n' >&2
  printf '::error::  gh workflow run release.yml -f tag=v%s -f crates=true -f npm=false\n' "$VERSION" >&2
  exit 1
}

snooze() {
  local secs=$1 reason=$2
  [ "$secs" -lt 1 ] && return 0
  [ "$secs" -gt "$MAX_SLEEP" ] && secs=$MAX_SLEEP
  if [ $(( $(now) + secs )) -gt "$DEADLINE" ]; then
    deadline_exit "$reason"
  fi
  log "waiting ${secs}s — $reason"
  sleep "$secs"
}

# Sleep so the next upload of this kind lands no sooner than one refill interval
# after the previous one. No-op until a 429 has taught us the interval.
pace() {
  local kind=$1 r last target nowt
  r=$(refill_of "$kind")
  last=$(last_of "$kind")
  if [ "$r" -gt 0 ] && [ "$last" -gt 0 ]; then
    target=$((last + r))
    nowt=$(now)
    if [ "$target" -gt "$nowt" ]; then
      snooze $((target - nowt)) "pacing inside the ${kind}-crate rate limit"
    fi
  fi
  return 0
}

# Pull the "Please try again after <RFC 2822 date>" instant out of a 429 body and
# print it as epoch seconds. Returns 1 if the message carried no parseable date.
retry_after_epoch() {
  local when epoch
  when=$(printf '%s' "$1" |
    sed -nE 's/.*try again after ([A-Za-z]{3}, [0-9]{1,2} [A-Za-z]{3} [0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2} [A-Za-z]{3,4}).*/\1/p' |
    head -1)
  [ -n "$when" ] || return 1
  # GNU date (CI) first, then BSD date (a local publish from macOS).
  epoch=$(date -u -d "$when" +%s 2>/dev/null) ||
    epoch=$(TZ=UTC date -j -f '%a, %d %b %Y %H:%M:%S %Z' "$when" +%s 2>/dev/null) ||
    return 1
  printf '%s' "$epoch"
}

# Which bucket the last 429 came from, so publish_crate can re-key the crate it
# is working on when crates.io metered it differently than the index suggested.
RATE_BUCKET=""

# Wait out a 429 for one crate, refining the learned refill interval.
handle_rate_limit() {
  local out=$1 kind=$2 name=$3 bucket target last interval known wait
  # crates.io names the bucket it metered against; that beats our guess from the
  # index, and publish_crate adopts it so the eventual success and the pacing
  # that follows are attributed to the same bucket.
  bucket=$kind
  printf '%s' "$out" | grep -q 'too many new crates' && bucket=new
  printf '%s' "$out" | grep -q 'existing crates' && bucket=existing
  RATE_BUCKET=$bucket

  target=$(retry_after_epoch "$out") || target=""
  last=$(last_of "$bucket")

  if [ -n "$target" ]; then
    # The bucket is empty and the next token lands at $target. Once we are past
    # the burst budget every upload consumes the token that just appeared, so
    # $target minus that upload is the refill interval. Keep the largest
    # observation: underestimating costs one more 429, overestimating would idle
    # the rest of the release.
    if [ "$last" -gt 0 ]; then
      interval=$((target - last))
      known=$(refill_of "$bucket")
      if [ "$interval" -gt "$known" ] && [ "$interval" -le "$MAX_REFILL" ]; then
        set_refill "$bucket" "$interval"
        log "learned crates.io ${bucket}-crate limit: ~${interval}s per publish"
        log "${#remaining[@]} crate(s) left at that rate: ~$((${#remaining[@]} * interval))s (~$(((${#remaining[@]} * interval + 59) / 60)) min)"
      fi
    fi
    wait=$(( target - $(now) + 3 )) # +3s slack for clock skew
  else
    wait=$(refill_of "$bucket")
    if [ "$wait" -le 0 ]; then
      case $bucket in
        new) wait=$FALLBACK_WAIT_NEW ;;
        *) wait=$FALLBACK_WAIT_EXISTING ;;
      esac
    fi
    log "429 without a parseable retry instant; falling back to ${wait}s"
  fi
  [ "$wait" -lt 5 ] && wait=5
  snooze "$wait" "crates.io ${bucket}-crate rate limit — next token before retrying $name"
}

publish_crate() {
  local name=$1 kind=$2 attempt out wait transient=0
  for attempt in $(seq 1 "$MAX_ATTEMPTS"); do
    pace "$kind"

    if [ "$DRY_RUN" = 1 ]; then
      log "DRY_RUN: would publish $name ($kind crate)"
      set_last "$kind" "$(now)"
      return 0
    fi

    if out=$(cargo publish -p "$name" 2>&1); then
      set_last "$kind" "$(now)"
      log "published $name"
      return 0
    fi

    if printf '%s' "$out" | grep -qE 'already uploaded|already exists'; then
      log "$name@$VERSION is already on crates.io — continuing"
      return 0
    fi

    if printf '%s' "$out" | grep -qE '429 Too Many Requests|status 429'; then
      handle_rate_limit "$out" "$kind" "$name"
      kind=$RATE_BUCKET
      continue
    fi

    # Before retrying anything, ask the index: cargo can fail after crates.io
    # accepted the upload (e.g. it gives up waiting for the version to appear),
    # and re-uploading would spend a rate-limit token to be told so.
    if [ "$(index_state "$name" "$VERSION")" = published ]; then
      log "$name@$VERSION landed on crates.io despite the error above — continuing"
      return 0
    fi

    # Retry a flaky registry, but only a few times: the pattern below is broad
    # enough to catch a genuine failure whose text happens to mention a
    # connection, and burning 40 backoffs on that would hide it for an hour.
    if [ "$transient" -lt "$MAX_TRANSIENT" ] &&
      printf '%s' "$out" | grep -qEi 'status 5[0-9][0-9]|timed out|timeout|spurious network|connection|reset by peer|gateway|temporarily unavailable|handshake|SSL'; then
      transient=$((transient + 1))
      if [ "$transient" -gt 5 ]; then wait=300; else wait=$((15 * 2 ** (transient - 1))); fi
      log "transient registry error publishing $name (transient $transient/$MAX_TRANSIENT):"
      printf '%s\n' "$out" | tail -15
      snooze "$wait" "retrying $name"
      continue
    fi

    printf '::error::failed to publish %s\n' "$name" >&2
    printf '%s\n' "$out" >&2
    return 1
  done
  printf '::error::gave up on %s after %s attempts\n' "$name" "$MAX_ATTEMPTS" >&2
  return 1
}

# Wait for a freshly published version to show up in the sparse index, so the
# next crate in the order can resolve it as a dependency. cargo already does
# this for its own publishes; this covers the case where it gave up early.
wait_for_index() {
  local name=$1 i
  for i in $(seq 1 60); do
    if [ "$(index_state "$name" "$VERSION")" = published ]; then
      [ "$i" -gt 1 ] && log "$name@$VERSION indexed after $((i * 5))s"
      return 0
    fi
    sleep 5
  done
  log "WARNING: $name@$VERSION still not in the sparse index after 5 min — continuing"
  return 0
}

# Validate the hand-maintained list against the workspace: every publishable
# member listed exactly once, nothing stale, and every crate after the workspace
# crates it depends on (crates.io resolves those from the index at publish time).
check_list() {
  command -v cargo >/dev/null 2>&1 || die "cargo not found"
  command -v python3 >/dev/null 2>&1 || die "python3 not found"
  CRATE_LIST=$(printf '%s\n' "${CRATES[@]}") python3 - <<'PY'
import json
import os
import subprocess
import sys

listed = os.environ["CRATE_LIST"].split()
# --no-deps resolves nothing, so this needs no network.
proc = subprocess.run(
    ["cargo", "metadata", "--no-deps", "--format-version", "1", "--offline"],
    capture_output=True,
    text=True,
)
if proc.returncode != 0:
    print("::error::cargo metadata failed:", file=sys.stderr)
    print(proc.stderr, file=sys.stderr)
    sys.exit(1)
meta = json.loads(proc.stdout)

# `publish = false` in Cargo.toml surfaces as an empty list here.
packages = {p["name"]: p for p in meta["packages"] if p.get("publish") != []}
position = {}
errors = []

for i, name in enumerate(listed):
    if name in position:
        errors.append(f"{name} is listed twice")
    position[name] = i

for name in sorted(set(listed) - set(packages)):
    errors.append(f"{name} is in the publish list but is not a publishable workspace member")
for name in sorted(set(packages) - set(listed)):
    errors.append(
        f"{name} is a publishable workspace member but is missing from the publish list "
        "in scripts/publish-crates.sh — insert it after every crate it depends on"
    )

for name, pkg in packages.items():
    if name not in position:
        continue
    for dep in pkg["dependencies"]:
        # dev-dependencies do not affect publish order; normal and build deps do.
        if dep.get("kind") not in (None, "build"):
            continue
        if not dep.get("path") or dep["name"] not in position:
            continue
        if position[dep["name"]] > position[name]:
            errors.append(
                f"{name} is published before its dependency {dep['name']} — "
                f"move {name} after {dep['name']}"
            )

if errors:
    for e in errors:
        print(f"::error::{e}", file=sys.stderr)
    sys.exit(1)

print(f"publish list OK: {len(listed)} crates, dependency order valid")
PY
}

main() {
  [ $# -ge 1 ] || die "usage: $(basename "$0") <version> | --check"

  case $1 in
    --check)
      check_list
      exit $?
      ;;
    -h | --help)
      sed -n '2,60p' "$SELF"
      exit 0
      ;;
  esac

  VERSION=$1
  case $VERSION in
    v*) die "pass the version without the leading v (got $VERSION)" ;;
  esac
  # Publishing anything other than the checked-out version cannot work, and the
  # usual way to get here is running from main instead of a detached tag.
  local workspace_version
  workspace_version=$(grep -m1 '^version' Cargo.toml | sed 's/.*"\(.*\)".*/\1/')
  if [ "$VERSION" != "$workspace_version" ]; then
    die "asked to publish $VERSION but this checkout is $workspace_version — check out the tag first"
  fi

  check_list || exit 1

  local total=${#CRATES[@]} i=0 published=0 present=0 jumped=0 name state kind reached=0
  if [ -n "$START_AT" ]; then
    log "START_AT=$START_AT — skipping everything before it"
  fi
  log "publishing $total crates at version $VERSION (deadline: ${DEADLINE_MINUTES} min)"

  for name in "${CRATES[@]}"; do
    i=$((i + 1))

    if [ -n "$START_AT" ] && [ "$reached" -eq 0 ]; then
      if [ "$name" = "$START_AT" ]; then
        reached=1
      else
        jumped=$((jumped + 1))
        continue
      fi
    fi

    # Rebuild the not-yet-done tail so a deadline exit can name it.
    remaining=("${CRATES[@]:$((i - 1))}")

    state=$(index_state "$name" "$VERSION")
    case $state in
      published)
        present=$((present + 1))
        log "[$i/$total] $name@$VERSION already published — skipping"
        continue
        ;;
      new) kind=new ;;
      *) kind=existing ;;
    esac

    log "[$i/$total] publishing $name ($kind crate)"
    publish_crate "$name" "$kind" || exit 1
    published=$((published + 1))
    [ "$DRY_RUN" = 1 ] || wait_for_index "$name"
  done

  log "done: $published published, $present already on crates.io, $jumped skipped by START_AT, $total total"
}

main "$@"
