#!/usr/bin/env bash
# Verify every evergreen doc/marketing headline that quotes a service count,
# operation count, Smithy variant count, per-service operation count, Bedrock
# surface count, or performance metric (startup time / idle memory / binary
# size) agrees with the canonical sources:
#
#   - website/content/docs/parity.md   (per-service Ops column + row count + Bedrock 4-part surface)
#   - conformance-baseline.json        (variants_passed + total_variants)
#   - constants in this script         (startup time / idle memory / binary size — no in-repo source of truth)
#
# Run locally with `bash scripts/check-doc-counts.sh` or via the
# `doc-counts` CI job. Blog posts and dated marketing drafts are skipped per
# the project rule "blog posts are point-in-time, don't retroactively update".

set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"

PARITY="website/content/docs/parity.md"
BASELINE="conformance-baseline.json"

if [ ! -f "$PARITY" ]; then
    echo "missing $PARITY" >&2
    exit 2
fi
if [ ! -f "$BASELINE" ]; then
    echo "missing $BASELINE" >&2
    exit 2
fi

# --- Performance metrics ---
# These have no in-repo source of truth. When re-measurement establishes a new
# number, update these constants and audit every page in FILES in the same PR.
# Measured on an Apple M1 Max release build, 2026-11-13, with podman as the
# container runtime: a first SQS request answered in a median 229 ms over 12
# runs, idle RSS 32.1 MiB, 131 MB on disk (125 MiB). The size is rounded to the
# nearest 10 MB. Measure startup with a polling client that does not fork per
# probe, since forking curl and python added about 130 ms. Startup includes the
# container runtime probe and the orphan reap (one `info` call plus three
# concurrent listings), so it depends on how fast the daemon answers; with no
# runtime it is about 60 ms.
STARTUP_MS=300
IDLE_MEM_MIB=30
BINARY_MB=130

# --- Lambda runtime count ---
# Canonical source: `runtime_to_image()` in
# crates/fakecloud-lambda/src/runtime/docker.rs. That match expression is the
# actual list of supported runtimes — anything not in it returns None and
# `CreateFunction` rejects it. Count it with:
#
#   grep -cE '^\s*"[^"]+" => \("' crates/fakecloud-lambda/src/runtime/docker.rs
#
# (= 31 as of 2026-10-02). When fakecloud-lambda gains/drops a runtime, update
# this constant, the runtime list in docs/services/lambda.md AND the runtime
# enumerations in faq.md, and audit every gated page in the same PR.
#
# NOTE (2026-09-27): the path above used to read `src/runtime.rs`, which no
# longer exists — the recipe silently returned nothing, so this constant could
# drift unnoticed. A canonical pointer that 404s is the same bug class as a
# stale count: verify the source resolves before trusting the number it guards.
LAMBDA_RUNTIMES=31

# Canonical service count = row count in parity.md table.
parity_services=$(awk '/^\| \[/ {n++} END{print n+0}' "$PARITY")

# Canonical operation total = sum of the Ops column.
parity_ops=$(awk '
    /^\| \[/ {
        match($0, /\| [0-9]+ \|/)
        if (RSTART > 0) {
            s = substr($0, RSTART + 2, RLENGTH - 4)
            sum += s
        }
    }
    END { print sum + 0 }
' "$PARITY")

# Per-service ops map: "<Service>\t<ops>" lines, one per service in parity.md.
# Used to validate per-service claims in evergreen surfaces.
service_ops_map=$(awk '
    /^\| \[/ {
        # Service name is the bracketed link text in the first cell.
        match($0, /\[[^]]+\]/)
        svc = substr($0, RSTART+1, RLENGTH-2)
        # Ops is the second pipe-delimited field after the leading `| `.
        split($0, parts, "|")
        ops = parts[3]
        gsub(/^ +| +$/, "", ops)
        if (ops ~ /^[0-9]+$/) print svc "\t" ops
    }
' "$PARITY")

# Names other surfaces use for a parity.md row. The per-service pass matches a
# claim by the row's link text, so docs/services/_index.md's "| WAF v2 | 55 |"
# never matched "WAFv2" and kept 55 after the row moved to 59 (2026-10-17).
# Format: "<parity.md link text>=<alias>".
SERVICE_ALIASES=(
    "WAFv2=WAF v2"
    "ELBv2=Elastic Load Balancing v2"
    "ACM=ACM (Certificate Manager)"
    "Amazon Comprehend=Comprehend"
    "Amazon EMR=EMR (Elastic MapReduce)"
    "Amazon Transcribe=Transcribe"
    "IAM Identity Center Identity Store=Identity Store"
    "IAM Identity Center SSO Admin=SSO Admin"
    "SES=SES (v2 + v1 inbound)"
)
for _a in "${SERVICE_ALIASES[@]}"; do
    _ops=$(printf '%s\n' "$service_ops_map" | awk -F'\t' -v s="${_a%%=*}" '$1 == s { print $2; exit }')
    if [ -z "$_ops" ]; then
        echo "SERVICE_ALIASES entry '$_a' names no parity.md row" >&2
        exit 2
    fi
    service_ops_map="$service_ops_map"$'\n'"${_a#*=}"$'\t'"$_ops"
done

# Bedrock 4-part surface: pull each row from the map.
lookup_ops() {
    local svc="$1"
    echo "$service_ops_map" | awk -F'\t' -v svc="$svc" '$1 == svc { print $2; exit }'
}

bedrock_ctrl=$(lookup_ops "Bedrock")
bedrock_runtime=$(lookup_ops "Bedrock Runtime")
bedrock_agent=$(lookup_ops "Bedrock Agent")
bedrock_agent_rt=$(lookup_ops "Bedrock Agent Runtime")
bedrock_family=$(( ${bedrock_ctrl:-0} + ${bedrock_runtime:-0} + ${bedrock_agent:-0} + ${bedrock_agent_rt:-0} ))

# Variant counts straight out of the baseline JSON.
variants_pass=$(jq -r .variants_passed "$BASELINE")
variants_total=$(jq -r .total_variants "$BASELINE")

# Extract the FIRST number from a whole table cell, or nothing. Always exits 0.
#
# Cells need the first number, unlike last_num's grep -o fragments: a cell reads
# "105 (7,509 ops) at true 100% conformance, incl. ECR + ECS + ELBv2" and the
# LAST number there is the 2 in "ELBv2" (and "at true 100%" yields 100). The
# claim is the number the cell leads with.
first_num() {
    printf '%s' "$1" | awk 'match($0, /[0-9][0-9,]*/) { print substr($0, RSTART, RLENGTH); exit }'
}

# Extract the LAST number from a matched fragment, or nothing. Always exits 0.
#
# "Last" rather than "first" because the number we want sits next to the noun the
# grep matched on, while the service NAME can contain digits: "| API Gateway v2 |
# 103 |" must yield 103, not the 2 in "v2". grep -o already truncates the fragment
# at the noun, so the last number is the claim.
#
# NEVER replace this with `grep -oE ... | head -1` in a command substitution —
# see the note above.
last_num() {
    printf '%s' "$1" | awk '{
        n = ""
        while (match($0, /[0-9][0-9,]*/)) {
            n = substr($0, RSTART, RLENGTH)
            $0 = substr($0, RSTART + RLENGTH)
        }
        print n
    }'
}

# Comma-format thousands. Locale-free: `printf %'d` depends on a locale being
# installed (e.g. en_US.UTF-8), which is not guaranteed on minimal CI images
# and silently degrades to "1234" instead of "1,234" — that would cause the
# grouped-thousands regex below to miss matches and produce false negatives.
# Do it ourselves with awk.
fmt() {
    awk -v n="$1" 'BEGIN {
        out = ""
        while (length(n) > 3) {
            out = "," substr(n, length(n) - 2) out
            n = substr(n, 1, length(n) - 3)
        }
        print n out
    }'
}

ops_fmt=$(fmt "$parity_ops")
vp_fmt=$(fmt "$variants_pass")
vt_fmt=$(fmt "$variants_total")

# --- Subset counts (tfacc / real-AWS parity sandbox / E2E suite) ---
# NOTE (2026-10-01): these used to be hardcoded EXCEPTIONS entries
# ("conformance.md:services:27"). A whitelisted constant is a stale count
# waiting to happen: tfacc grew from 27 to 75 services and the page kept saying
# 27 under a green gate, because the exception matched the stale value forever.
# Derive every subset count from the code it describes instead.
TFACC_ALLOWLIST="crates/fakecloud-tfacc/src/allowlist.rs"
PARITY_TESTS="crates/fakecloud-parity/tests"
E2E_TESTS="crates/fakecloud-e2e/tests"
for _p in "$TFACC_ALLOWLIST" "$PARITY_TESTS" "$E2E_TESTS"; do
    if [ ! -e "$_p" ]; then
        echo "missing $_p (canonical source for a subset count moved?)" >&2
        exit 2
    fi
done
# One `Service {` entry per tfacc service in the SERVICES allow-list.
tfacc_services=$(awk '/^pub const SERVICES: &\[Service\] = &\[/ {f=1; next}
                      f && /^\];/ {f=0}
                      f && /^    Service \{/ {n++}
                      END {print n+0}' "$TFACC_ALLOWLIST")
# One test file per service in the real-AWS parity suite.
parity_sandbox=$(find "$PARITY_TESTS" -maxdepth 1 -name '*.rs' -type f | wc -l | tr -d ' ')
# Every #[test] / #[tokio::test(...)] in the E2E suite.
e2e_tests=$(cat "$E2E_TESTS"/*.rs | awk '/#\[(tokio::)?test(\]|\(|$)/ {n++} END {print n+0}')

echo "Canonical truth:"
echo "  services           = $parity_services (parity.md row count)"
echo "  operations         = $parity_ops ($ops_fmt) (sum of parity.md Ops column)"
echo "  variants_passed    = $variants_pass ($vp_fmt)"
echo "  total_variants     = $variants_total ($vt_fmt)"
echo "  startup_ms         = $STARTUP_MS (script constant)"
echo "  idle_mem_mib       = $IDLE_MEM_MIB (script constant)"
echo "  binary_mb          = $BINARY_MB (script constant)"
echo "  bedrock surface    = $bedrock_ctrl + $bedrock_runtime + $bedrock_agent + $bedrock_agent_rt = $bedrock_family (parity.md rows)"
echo "  lambda_runtimes    = $LAMBDA_RUNTIMES (script constant; canonical: runtime_to_image() in crates/fakecloud-lambda/src/runtime/docker.rs)"
echo "  tfacc_services     = $tfacc_services ($TFACC_ALLOWLIST SERVICES entries)"
echo "  parity_sandbox     = $parity_sandbox ($PARITY_TESTS/*.rs)"
echo "  e2e_tests          = $e2e_tests (#[test] in $E2E_TESTS)"
echo

# Files to check. Evergreen-only, derived by EXCLUSION, not by an allowlist.
#
# NOTE (2026-09-27): this used to be a hand-maintained FILES=( ... ) array of 30
# paths while website/content held 68 evergreen pages — every SEO landing page
# (sqs-emulator.md, local-rds.md, ses-emulator.md, vs/aws-sdk-client-mock.md, ...)
# was ungated, because a new page only gets checked if somebody remembers to add
# it here. Inclusions rot; exclusions are stable. Gate everything evergreen by
# default and list only what must NOT be checked. Print the count with
# `echo "${#FILES[@]}"` rather than trusting a number written in a comment.
#
# Excluded, deliberately:
#   */blog/*      point-in-time posts, never retroactively updated
#   marketing/*   dated drafts, same rule
#   docs/operations/  generated by scripts/generate-operations-index.sh
#   node_modules, target, website/public  vendored or build output
FILES=()
while IFS= read -r _f; do
    FILES+=("$_f")
done < <(
    {
        find website/content website/static website/templates \
             README.md AGENTS.md CONTRIBUTING.md conformance-baseline-notes.md \
             -type f \( -name '*.md' -o -name '*.txt' -o -name '*.html' \) 2>/dev/null
        # crates/fakecloud-conformance/README.md claimed "80,074 / 81,489 (98.3%)
        # across 33 services" — a public contradiction of the "true 100%" headline
        # that survived precisely because only website/ + 4 root files were gated.
        find crates -name 'README.md' -type f 2>/dev/null
    } \
    | grep -vE '/blog/|/marketing/|/node_modules/|/target/|/docs/operations/|website/public/' \
    | sort
)

# Known exceptions: file:kind:value
# These are intentional non-headline mentions where the number is correct in
# its local context (subset counts, rhetorical comparisons, etc.).
EXCEPTIONS=(
    # tfacc and the real-AWS parity sandbox cover subsets. DERIVED, never
    # hardcoded — the subset pass below checks each against its own source.
    "website/content/docs/about/conformance.md:services:$tfacc_services"
    "website/content/docs/about/conformance.md:services:$parity_sandbox"
    # rhetorical comparison: "depth-first vs N services at 50%"
    "website/content/docs/about/what-it-is.md:services:100"
    # vs/localstack.md aliases redirect legacy blog slugs that have "500ms" in
    # the URL itself. They're URLs we have to match verbatim, not performance claims.
    "website/content/vs/localstack.md:startup_ms:500"
    # Comparison tables quote the COMPETITOR's performance numbers next to ours.
    # The metric checks extract every number on the line and can't attribute it,
    # so a competitor's figure that collides with one of our metric kinds is
    # whitelisted here. "~150 MiB idle" is LocalStack's idle RSS in the README
    # footprint-comparison row (ours is ~30 MiB, stated in the same row and in
    # "Why fakecloud").
    "README.md:idle_mem_mib:150"
    # "AWS AppConfig: 58 operations" is a DIFFERENT service from AWS Config; the
    # per-service regex matches the "Config" tail of "AppConfig". AppConfig's 58
    # op count is correct in its own context.
    "website/content/supported-services.md:ops_Config:58"
    "website/static/llms.txt:ops_Config:58"
    "website/static/llms-full.txt:ops_Config:58"
    # appconfig.md splits its 58 into the two SDK clients one crate serves:
    # "the 56-operation appconfig API and the 2-operation appconfigdata API".
    "website/content/docs/services/appconfig.md:own_ops:56"
    "website/content/docs/services/appconfig.md:own_ops:2"
    # ec2.md: "Transit Gateway -- the complete 74-op surface", a feature subset.
    "website/content/docs/services/ec2.md:own_ops:74"
)

is_exception() {
    local file="$1" kind="$2" value="$3"
    local needle="$file:$kind:$value"
    for e in "${EXCEPTIONS[@]}"; do
        if [ "$e" = "$needle" ]; then
            return 0
        fi
    done
    return 1
}

fail=0
problems=()

if [ "${#FILES[@]}" -eq 0 ]; then
    echo "no evergreen files matched — check the find roots in this script" >&2
    exit 2
fi

for f in "${FILES[@]}"; do
    if [ ! -f "$f" ]; then
        continue
    fi

    # --- Service count claims ---
    # Catches "39 services", "39 AWS services", and — since 2026-09-27 — forms
    # with a qualifier BETWEEN the number and the noun: "46 other services",
    # "46 other AWS services", "20 more services". The old regex required the
    # number and the noun to be adjacent, so every "N other services" on the
    # site (faq.md, vs/minio, vs/s3mock, vs/elasticmq, vs/dynamodb-local,
    # vs/sam-local, localstack-alternative) sailed past a green gate while
    # claiming 21/22/46. That is what an HN commenter quoted back at us.
    #
    # N-1 is legitimate and expected: "fakecloud does S3 among 104 other
    # services" excludes the subject service, so accept services-1 whenever the
    # phrase carries "other"/"more".
    while read -r phrase; do
        [ -z "$phrase" ] && continue
        hit=$(printf '%s' "$phrase" | grep -oE "^[0-9]+")
        expected="$parity_services"
        case "$phrase" in
            # Word-boundaried: "46 other AWS services" is N-1 (the page's subject
            # service is excluded). Deliberately NOT *more* — "12 more services on
            # the roadmap" means 12 ADDITIONAL, not services-1 — nor a bare
            # *other* substring, which also matches "another"/"mother".
            [0-9]*' other '*) expected=$(( parity_services - 1 )) ;;
        esac
        if [ "$hit" != "$expected" ] && ! is_exception "$f" services "$hit"; then
            problems+=("$f: claims '$phrase', expected $expected")
            fail=1
        fi
    done < <(grep -oE "\b[0-9]+(( [a-z][a-z-]+){0,2}) (AWS )?services\b" "$f" | sort -u)

    # --- Labelled service totals ("**Total Services**: 105") ---
    # The label puts the number AFTER the noun, so the claim regex above never
    # sees it; supported-services.md kept a stale total through a service add.
    while read -r hit; do
        [ -z "$hit" ] && continue
        if [ "$hit" != "$parity_services" ] && ! is_exception "$f" services "$hit"; then
            problems+=("$f: claims 'Total Services: $hit', expected $parity_services")
            fail=1
        fi
    done < <(grep -oiE "total services\**:\** *[0-9]+" "$f" | grep -oE "[0-9]+$" | sort -u)

    # --- Operation total claims ---
    # Comma-formatted thousands only — avoids matching per-service mini-counts
    # like "23 ops" inside feature bullets. "2,592 operations" / "2,592 API operations" etc.
    while read -r hit; do
        [ -z "$hit" ] && continue
        if [ "$hit" != "$ops_fmt" ] && ! is_exception "$f" operations "$hit"; then
            problems+=("$f: claims '$hit operations', expected $ops_fmt")
            fail=1
        fi
    done < <(grep -oE "\b[0-9]{1,3}(,[0-9]{3})+(( [a-z][a-z-]+){0,2}) (API )?(operations|ops|actions)\b" "$f" | grep -oE "^[0-9]{1,3}(,[0-9]{3})+" | sort -u)

    # --- Variant pass-rate claims (X,XXX/Y,YYY) ---
    expected_pair="$vp_fmt/$vt_fmt"
    while read -r hit; do
        [ -z "$hit" ] && continue
        if [ "$hit" != "$expected_pair" ] && ! is_exception "$f" variants "$hit"; then
            problems+=("$f: variants '$hit', expected $expected_pair")
            fail=1
        fi
    done < <(grep -oE "\b[0-9]{1,3}(,[0-9]{3})+/[0-9]{1,3}(,[0-9]{3})+\b" "$f" | sort -u)

    # --- Bare variant total claims ("86,327 variants", "86,327 generated...") ---
    # Catches stale "59,000+ variants" / "54,000+ variants" framing too.
    while read -r hit; do
        [ -z "$hit" ] && continue
        if [ "$hit" != "$vp_fmt" ] && ! is_exception "$f" variants "$hit"; then
            problems+=("$f: claims '$hit variants', expected $vp_fmt")
            fail=1
        fi
    done < <(grep -oE "\b[0-9]{1,3}(,[0-9]{3})+\+?(( [a-z][a-z-]+){0,3}) variants\b" "$f" | grep -oE "^[0-9]{1,3}(,[0-9]{3})+" | sort -u)

    # --- Lambda runtime-count claims ---
    # Catches "23 runtimes", "27 runtimes", "X Lambda runtimes" — anywhere on
    # a page that quotes how many runtimes fakecloud supports. Avoids matching
    # "27 runtimes are supported by real AWS" by looking for the literal
    # token "runtimes" right after the number.
    while read -r hit; do
        [ -z "$hit" ] && continue
        if [ "$hit" != "$LAMBDA_RUNTIMES" ] && ! is_exception "$f" lambda_runtimes "$hit"; then
            problems+=("$f: claims '$hit runtimes', expected $LAMBDA_RUNTIMES")
            fail=1
        fi
    done < <(grep -oE '\b[0-9]+(( [a-z][a-z-]+){0,2}) runtimes\b' "$f" | grep -oE '^[0-9]+' | sort -u)

    # --- Startup time claims ---
    # Pulls every "~?Nms" / "~?N ms" / "<Nms" that appears on a line mentioning
    # "startup" or "starts in". Excludes context lines about other emulators'
    # startup numbers ("LocalStack ~3s") by requiring the number to be on the
    # same line as our own positioning words.
    while read -r hit; do
        [ -z "$hit" ] && continue
        if [ "$hit" != "$STARTUP_MS" ] && ! is_exception "$f" startup_ms "$hit"; then
            problems+=("$f: claims '${hit}ms startup', expected ${STARTUP_MS}ms")
            fail=1
        fi
    done < <(
        grep -E -i 'startup|starts in|start time' "$f" \
            | grep -oE '[<~]?[0-9]+\s*ms\b' \
            | grep -oE '[0-9]+' \
            | sort -u
    )

    # --- Idle memory claims ---
    # "~30 MiB", "30 MiB idle", "30 MiB idle memory". Avoids false positives by
    # only firing on lines that mention "idle" or "memory" alongside the number.
    while read -r hit; do
        [ -z "$hit" ] && continue
        if [ "$hit" != "$IDLE_MEM_MIB" ] && ! is_exception "$f" idle_mem_mib "$hit"; then
            problems+=("$f: claims '${hit} MiB idle memory', expected ${IDLE_MEM_MIB} MiB")
            fail=1
        fi
    done < <(
        grep -E 'idle (memory|RSS)|idle$|MiB idle' "$f" \
            | grep -oE '~?[0-9]+\s*MiB' \
            | grep -oE '[0-9]+' \
            | sort -u
    )

    # --- Binary size claims ---
    # "~120 MB binary", "120MB binary", "120 MB static binary", "binary (~120 MB)".
    # Restricted to lines mentioning "binary" to avoid catching unrelated MB
    # mentions (e.g. install size for competitors).
    while read -r hit; do
        [ -z "$hit" ] && continue
        if [ "$hit" != "$BINARY_MB" ] && ! is_exception "$f" binary_mb "$hit"; then
            problems+=("$f: claims '${hit} MB binary', expected ${BINARY_MB} MB")
            fail=1
        fi
    done < <(
        grep -E -i 'binary' "$f" \
            | grep -oE '~?[0-9]+\s*MB' \
            | grep -oE '[0-9]+' \
            | sort -u
    )

    # --- Bedrock 4-part surface claims ---
    # Catches "111 Bedrock operations" / "214 Bedrock-family operations" mismatches
    # against the parity.md sum, and per-API counts that drift.
    # The pattern fires on any "<N> Bedrock(...) operations" phrase, with
    # optional qualifier words (Runtime, Agent, family). We accept N if it
    # matches the relevant sub-surface or the full family sum.
    while read -r line; do
        [ -z "$line" ] && continue
        n=$(echo "$line" | grep -oE '^[0-9]+')
        qual=$(echo "$line" | sed -E 's/^[0-9]+ //; s/operations?.*$//; s/ +$//')
        expected=""
        case "$qual" in
            "Bedrock"|"Bedrock-family"|"Bedrock family")
                expected="$bedrock_family"
                ;;
            "Bedrock Runtime")
                expected="$bedrock_runtime"
                ;;
            "Bedrock Agent")
                expected="$bedrock_agent"
                ;;
            "Bedrock Agent Runtime")
                expected="$bedrock_agent_rt"
                ;;
        esac
        # Tolerate the bare "Bedrock" form referring to either ctrl-only or family
        # (the site uses both framings; both are accepted as long as N matches one of them).
        if [ "$qual" = "Bedrock" ]; then
            if [ "$n" != "$bedrock_ctrl" ] && [ "$n" != "$bedrock_family" ]; then
                problems+=("$f: claims '$n Bedrock operations', expected $bedrock_ctrl (ctrl) or $bedrock_family (family)")
                fail=1
            fi
        elif [ -n "$expected" ] && [ "$n" != "$expected" ]; then
            problems+=("$f: claims '$n $qual operations', expected $expected")
            fail=1
        fi
    done < <(
        grep -oE '\b[0-9]+ Bedrock(-family| Runtime| Agent Runtime| Agent| family)? operations?\b' "$f" \
            | sort -u
    )

    # The phrasings the "<N> Bedrock operations" regex never saw, all of which
    # kept saying 216 after the family grew to 222 (2026-10-17): "216 ops across
    # 4 APIs", "216 operations across the full Bedrock family", "**216 of 216**
    # Bedrock-family", and per-API lists "(Bedrock 103, Bedrock Runtime 10, ...)"
    # / "### Bedrock (103 actions) + Bedrock Runtime (10 actions)". Only lines
    # that mention Bedrock are read, so other services' "N ops across" is safe.
    while IFS= read -r frag; do
        [ -z "$frag" ] && continue
        # Drop the one boundary character the "N ops across" alternative
        # consumes in front of the number.
        [[ "$frag" =~ ^[0-9B] ]] || frag=${frag:1}
        case "$frag" in
            "Bedrock Agent Runtime"*) want="$bedrock_agent_rt" ;;
            "Bedrock Agent"*)         want="$bedrock_agent" ;;
            "Bedrock Runtime"*)       want="$bedrock_runtime" ;;
            "Bedrock"*)               want="$bedrock_ctrl" ;;
            *)                        want="$bedrock_family" ;;
        esac
        n=$(first_num "$frag")
        n=${n%,}
        [ -z "$n" ] && continue
        if [ "$n" != "$want" ] && ! is_exception "$f" bedrock "$n"; then
            problems+=("$f: claims '$frag', expected $want")
            fail=1
        fi
    done < <(
        grep -E 'Bedrock' "$f" 2>/dev/null \
            | grep -oE '(^|[^0-9,.])[0-9]+ (ops|operations) across|\b[0-9]+ of [0-9]+\**:? Bedrock|Bedrock( Agent Runtime| Agent| Runtime)? \(?[0-9]+( actions\)|,|\))' \
            | sort -u || true
    )

done


# --- Per-service op count claims (hoisted out of the per-file loop) ----------
# Catches "**S3**: 154 operations", "Lambda (82 operations)", etc. Skips
# parity.md (it's the source) and the per-service docs under docs/services/
# (those are the source for their own service).
#
# PERF (2026-09-27): this used to run one grep per (file x service) pair. With
# the allowlist replaced by an exclusion-based file set the gated file count
# went from 30 hand-listed paths to every evergreen file under website/ plus the
# root docs and crate READMEs. At 105 services a grep-per-(file x service) meant
# thousands of processes and pushed a single run into the minutes. Grepping every
# file in ONE invocation per service drops that to a few hundred spawns for the
# same coverage (~40s total). Fix the hot path, don't raise the timeout.
PS_FILES=()
for _f in "${FILES[@]}"; do
    [ -f "$_f" ] || continue
    [[ "$_f" == "$PARITY" ]] && continue
    # Per-service docs are the source for their OWN service, so they are skipped
    # — but _index.md is an aggregate that restates every service's count, and
    # excluding it is why 17 of its rows silently rotted (fixed 2026-09-27).
    if [[ "$_f" == website/content/docs/services/*.md && "$_f" != website/content/docs/services/_index.md ]]; then
        continue
    fi
    PS_FILES+=("$_f")
done

if [ "${#PS_FILES[@]}" -gt 0 ]; then
    while IFS=$'\t' read -r svc canonical_ops; do
        [ -z "$svc" ] && continue
        # Bedrock family is handled by the dedicated Bedrock-surface check above,
        # which accepts both the per-API count and the family sum. Skipping here
        # avoids double-firing on the same phrase.
        case "$svc" in
            "Bedrock"|"Bedrock Runtime"|"Bedrock Agent"|"Bedrock Agent Runtime") continue ;;
        esac
        svc_re=$(printf '%s\n' "$svc" | sed 's/[][\.*^$()+?{}|]/\\&/g')
        while IFS= read -r hit; do
            [ -z "$hit" ] && continue
            hf=${hit%%:*}
            n=$(last_num "${hit#*:}")
            [ -z "$n" ] && continue
            if [ "$n" != "$canonical_ops" ] && ! is_exception "$hf" "ops_${svc}" "$n"; then
                problems+=("$hf: claims '$svc: $n ops', expected $canonical_ops")
                fail=1
            fi
        done < <(
            # One grep per service (see PERF above), one -e per phrasing:
            #   "**S3**: 107 operations" / "Lambda (82 operations)"
            #   "### DynamoDB (58 actions)" / "**Bedrock** (103 ops)"
            #   "| ACM (Certificate Manager) |  40 |" aggregate table rows
            #   "<td>Cognito User Pools</td><td class=\"check\">132 operations"
            #   "**53 KMS operations**" / "**53** KMS operations": number first,
            #     how every SEO landing page phrases it; none of the patterns
            #     above read it (KMS, RDS, DynamoDB, S3, SES and Cognito landing
            #     pages were stale on 2026-10-17). sed cuts the name out of that
            #     shape so last_num never reads the 3 in "S3".
            { grep -oHE \
                -e "(\*\*)?${svc_re}(\*\*)?[: ]\(?[ ]*[0-9]+ (operations?|ops|actions)\b" \
                -e "(\*\*|### )${svc_re}(\*\*)? \([0-9]+ (operations?|ops|actions)\)" \
                -e "^\| *${svc_re} *\| *[0-9]+ (operations?|ops)?" \
                -e "<td>${svc_re}</td><td[^>]*>[0-9]+ (operations?|ops)" \
                -e "(^|[^0-9,.])[0-9]+(\*\*)? (\*\*)?${svc_re}(\*\*)? operations?\b" \
                "${PS_FILES[@]}" 2>/dev/null || true; } \
                | sed -E "s/^([^:]*:[^0-9]*[0-9]+)(\*\*)? (\*\*)?${svc_re}(\*\*)? operations?\$/\1 operations/" \
                | sort -u
        )
    done <<< "$service_ops_map"
fi

# --- Table-cell / row-label claims -------------------------------------------
# The class no <number><noun> regex can reach: the number sits in a value cell
# and the NOUN lives in the row-label cell or a preceding <td>, with the number
# followed by a qualifier instead.
#
#   | Service count | 47 at true 100% conformance (depth-first) |
#   | Services covered today | 47 (3,966 ops) at true 100% conformance |
#   <tr><td>AWS services</td><td class="check">46 at true 100% conformance</td>
#
# All three were live and green on 2026-09-27. Scan any row whose label cell
# talks about service/operation coverage and check the first number in the
# NEXT cell against the canonical totals.
while IFS= read -r hit; do
    [ -z "$hit" ] && continue
    hf=${hit%%:*}
    rest=${hit#*:}
    lineno=${rest%%:*}
    text=${rest#*:}
    # Take the number from the VALUE cell, never the whole line: a digit in the
    # label ("| Service count (2026) |") would otherwise win, and a competitor
    # column ("LocalStack 20 / fakecloud 105") would match the wrong side.
    case "$text" in
        '|'*) value=$(printf '%s' "$text" | awk -F'|' '{print $3}') ;;
        *)    value=$(printf '%s' "$text" | sed -E 's#^.*<td>[^<]*[Aa][Ww][Ss] services[^<]*</td>[[:space:]]*<td[^>]*>##; s#</td>.*##') ;;
    esac
    # `|| true`: grep exits 1 when the cell holds no digit (a legitimate value
    # like "see the parity matrix"). Under `set -euo pipefail` that aborts the
    # WHOLE script from the parent shell — no FAIL header, every problem already
    # collected thrown away, CI showing a bare exit 1 with no diagnostic.
    n=$(first_num "$value")
    [ -z "$n" ] && continue
    # "N other/more services" rows in a label cell are N-1 (the page's subject
    # service is excluded from the count).
    expected_cell="$parity_services"
    case "$text" in
        *[Oo]ther*) expected_cell=$(( parity_services - 1 )) ;;
    esac
    if [ "$n" != "$expected_cell" ] && [ "$n" != "$ops_fmt" ] && [ "$n" != "$parity_ops" ] \
       && ! is_exception "$hf" table_cell "$n"; then
        problems+=("$hf:$lineno: table row claims '$n', expected $expected_cell services / $ops_fmt operations")
        fail=1
    fi
done < <(
    # NOTE: `set -e` is inherited by this subshell, and grep exits 1 when a file
    # has no match — without `|| true` the very first non-matching file would
    # kill the loop and the whole pass would silently scan nothing. That is
    # exactly how this check shipped as dead code on its first draft; it looked
    # correct and caught nothing.
    for _f in "${FILES[@]}"; do
        [ -f "$_f" ] || continue
        grep -nHE '^\| *[^|]*([Ss]ervices? (count|covered)|[Oo]ther AWS services)[^|]*\|' "$_f" 2>/dev/null || true
        grep -nHE '<td>[^<]*AWS services[^<]*</td><td[^>]*>[0-9]' "$_f" 2>/dev/null || true
    done | sort -u
)

# --- parity.md rows vs the Smithy models -------------------------------------
# Every pass above trusts parity.md's Ops column, and nothing checked that column
# against the code. It rotted twice over (found 2026-10-17, gate green):
#
#   - aws-models refreshes added operations that were implemented the same week
#     (S3 object annotations, KMS GetKeyLastUsage, RDS
#     DescribeServerlessV2PlatformVersions, Backup access points, WAFv2 revenue
#     statistics, Bedrock InvokeGuardrailChecks) without the row moving;
#   - rows written at a count below the model and never revisited (Lambda 73
#     of 88, CloudFront 147 of 167), while their service pages said "73 of 73".
#
# 59 dispatched, conformance-gated operations were missing from the headline.
# Every op a model defines is in its crate's supported_actions() (conformance
# probes them all), so a row equals its model's operation count. Rows and models
# are joined on the docs slug, using the model -> slug map that
# scripts/generate-operations-index.sh already owns (a slug may span several
# models, e.g. appconfig + appconfigdata). Names are de-duplicated per model:
# timestream.json carries Write and Query, which share four operation names.
#
# A service page whose slug has no parity row is a shipped service missing from
# the service count, the operation total and the conformance claim at once.
# PENDING_PARITY_ROW names such slugs while the row is being added; it must be
# empty on main once that work lands.
PENDING_PARITY_ROW=()
OPS_INDEX_GEN="scripts/generate-operations-index.sh"
if [ ! -f "$OPS_INDEX_GEN" ]; then
    echo "missing $OPS_INDEX_GEN (model -> slug map moved? update OPS_INDEX_GEN)" >&2
    exit 2
fi
model_slug_map=$(awk '/^SERVICES=\(/ {f=1; next} f && /^\)/ {f=0} f' "$OPS_INDEX_GEN" \
    | sed -nE 's/^ *"([^|"]+)\|[^|"]*\|([^|"]*)".*/\1\t\2/p')
if [ -z "$model_slug_map" ]; then
    echo "could not read the SERVICES map from $OPS_INDEX_GEN" >&2
    exit 2
fi
model_files=()
while IFS=$'\t' read -r _m _s; do
    [ -f "aws-models/$_m.json" ] && model_files+=("aws-models/$_m.json")
done <<< "$model_slug_map"
# With no files jq would read stdin and hang instead of failing.
if [ "${#model_files[@]}" -eq 0 ]; then
    echo "no aws-models/*.json matched $OPS_INDEX_GEN's SERVICES map" >&2
    exit 2
fi
# One jq process for every model: "<model>\t<unique op count>".
model_ops=$(jq -r 'input_filename as $f
    | [(.shapes // {}) | to_entries[] | select(.value.type == "operation") | .key | sub(".*#"; "")]
    | unique | length
    | "\($f | sub("^aws-models/"; "") | sub("\\.json$"; ""))\t\(.)"' "${model_files[@]}")
model_slug_ops=$(awk -F'\t' 'NR == FNR { ops[$1] = $2; next }
    $2 != "" && ($1 in ops) { sum[$2] += ops[$1] }
    END { for (s in sum) print s "\t" sum[s] }' \
    <(printf '%s\n' "$model_ops") <(printf '%s\n' "$model_slug_map") | LC_ALL=C sort -t$'\t' -k1,1)
parity_slug_ops=$(awk -F'|' '/^\| \[/ {
        s = $2; sub(/.*@\/docs\/services\//, "", s); sub(/\.md.*/, "", s)
        o = $3; gsub(/ /, "", o)
        if (o ~ /^[0-9]+$/) sum[s] += o
    }
    END { for (s in sum) print s "\t" sum[s] }' "$PARITY" | LC_ALL=C sort -t$'\t' -k1,1)
while IFS=$'\t' read -r slug want got; do
    [ -z "$slug" ] && continue
    pending=0
    for _p in ${PENDING_PARITY_ROW[@]+"${PENDING_PARITY_ROW[@]}"}; do [ "$_p" = "$slug" ] && pending=1; done
    if [ "$got" = "-" ]; then
        if [ "$pending" -eq 0 ]; then
            problems+=("$PARITY: no row for docs/services/$slug.md ($want modeled operations); the service is missing from every headline count")
            fail=1
        fi
    elif [ "$pending" -eq 1 ]; then
        problems+=("$0: '$slug' has a parity row now; drop it from PENDING_PARITY_ROW")
        fail=1
    elif [ "$want" = "-" ]; then
        problems+=("$PARITY: docs/services/$slug.md has a row but no model in $OPS_INDEX_GEN's SERVICES map")
        fail=1
    elif [ "$want" != "$got" ]; then
        problems+=("$PARITY: docs/services/$slug.md rows sum to $got operations, the Smithy model(s) define $want")
        fail=1
    fi
done < <(LC_ALL=C join -t$'\t' -a1 -a2 -e - -o 0,1.2,2.2 \
    <(printf '%s\n' "$model_slug_ops") <(printf '%s\n' "$parity_slug_ops"))

# --- Per-service pages' own headline counts -----------------------------------
# docs/services/<slug>.md opens with "fakecloud implements **57 of 57** DynamoDB
# operations" or "**109 operations** ship with 100% conformance". The
# per-service pass above skips these pages as "the source for their own
# service" -- they are not, parity.md is -- so on 2026-10-17 eleven of them
# (DynamoDB 57/58, EC2 802/807, Kinesis 44/45, CloudWatch Logs 115/118, Cognito
# 126/132, CloudWatch 49/55, ...) disagreed with their own parity row. A claim
# must equal one of the slug's rows or their sum (Bedrock's page also quotes
# the 4-API family sum).
while IFS= read -r svc_doc; do
    slug=$(basename "$svc_doc" .md)
    [ "$slug" = "_index" ] && continue
    allowed=" $(awk -F'|' -v s="$slug" '/^\| \[/ {
            x = $2; sub(/.*@\/docs\/services\//, "", x); sub(/\.md.*/, "", x)
            if (x == s) { o = $3; gsub(/ /, "", o); print o; t += o }
        }
        END { if (t) print t }' "$PARITY" | tr '\n' ' ')"
    [ "$slug" = "bedrock" ] && allowed="$allowed$bedrock_family "
    [ "$allowed" = " " ] && continue
    while IFS= read -r claim; do
        [ -z "$claim" ] && continue
        lineno=${claim%%:*}
        # "N of M" is a count claim when M is one of the page's counts
        # ("57 of 58" is as stale as "57 of 57"); "1 of 3 shards" is prose.
        if [[ "${claim#*:}" =~ ^([0-9]+)\ of\ ([0-9]+)$ ]]; then
            case "$allowed" in *" ${BASH_REMATCH[2]} "*) ;; *) continue ;; esac
        fi
        for n in $(printf '%s' "${claim#*:}" | grep -oE '[0-9]+' || true); do
            case "$allowed" in
                *" $n "*) ;;
                *)
                    if ! is_exception "$svc_doc" own_ops "$n"; then
                        problems+=("$svc_doc:$lineno: claims '${claim#*:}', parity.md says one of:$allowed")
                        fail=1
                    fi
                    ;;
            esac
        done
    done < <(grep -noE '\b[0-9]+ of [0-9]+\b|\*\*[0-9]+ operations\*\*|\b[Aa]ll [0-9]+ operations\b|\b[0-9]+-operation\b|\b[Cc]omplete [0-9]+-op\b|control plane: [0-9]+ operations|(^|\. )[0-9]+ operations\.' "$svc_doc" || true)
done < <(find website/content/docs/services -maxdepth 1 -name '*.md' -type f | sort)

# --- "(all N ops)" trailers in aggregate table rows -------------------------
# "| AWS Backup | 115 | ... No real backup engine (all 109 ops) |": the count
# cell is gated above, the prose trailer in the same row was not, and on
# 2026-10-17 five rows of docs/services/_index.md (Backup, EKS, OpenSearch,
# Account Management, Identity Store) contradicted themselves. A row's trailer
# must equal its own count cell.
while IFS= read -r bad; do
    [ -z "$bad" ] && continue
    problems+=("$bad")
    fail=1
done < <(awk -F'|' '/^\| / && $3 ~ /^ *[0-9]+ *$/ {
        line = $0; n = $3; gsub(/ /, "", n)
        while (match(line, /\(all [0-9]+ (ops|operations)\)/)) {
            m = substr(line, RSTART + 5); sub(/ .*/, "", m)
            if (m != n) print FILENAME ":" FNR ": row says " n " but its trailer says (all " m " ...)"
            line = substr(line, RSTART + RLENGTH)
        }
    }' "${FILES[@]}" /dev/null)

# --- Single-service landing pages ---------------------------------------------
# "dynamodb-emulator.md" says "57 operations" in its front-matter description,
# with no service name next to the number, so no per-service pattern can read
# it. On a page about one service every "N operations" is that service's count.
# Format: "<page>=<parity.md link text>" (or "=bedrock-family").
LANDING_PAGES=(
    "website/content/bedrock-emulator.md=bedrock-family"
    "website/content/fake-bedrock.md=bedrock-family"
    "website/content/test-bedrock-locally.md=bedrock-family"
    "website/content/cognito-emulator.md=Cognito User Pools"
    "website/content/dynamodb-emulator.md=DynamoDB"
    "website/content/kms-emulator.md=KMS"
    "website/content/local-ecr.md=ECR"
    "website/content/local-ecs.md=ECS"
    "website/content/local-elasticache.md=ElastiCache"
    "website/content/local-elb.md=ELBv2"
    "website/content/local-rds.md=RDS"
    "website/content/local-s3-for-tests.md=S3"
    "website/content/ses-emulator.md=SES"
    "website/content/sns-emulator.md=SNS"
    "website/content/sqs-emulator.md=SQS"
    "website/content/test-lambda-locally.md=Lambda"
)
for _lp in "${LANDING_PAGES[@]}"; do
    _page=${_lp%%=*}
    _svc=${_lp#*=}
    if [ ! -f "$_page" ]; then
        problems+=("$0: LANDING_PAGES names $_page, which does not exist")
        fail=1
        continue
    fi
    if [ "$_svc" = "bedrock-family" ]; then
        _want="$bedrock_family"
    else
        _want=$(printf '%s\n' "$service_ops_map" | awk -F'\t' -v s="$_svc" '$1 == s { print $2; exit }')
        if [ -z "$_want" ]; then
            problems+=("$0: LANDING_PAGES maps $_page to '$_svc', which is not a parity.md row")
            fail=1
            continue
        fi
    fi
    while IFS= read -r _hit; do
        [ -z "$_hit" ] && continue
        _n=$(first_num "${_hit#*:}")
        if [ "$_n" != "$_want" ] && ! is_exception "$_page" landing "$_n"; then
            problems+=("$_page:${_hit%%:*}: claims '${_hit#*:}', expected $_want")
            fail=1
        fi
    done < <(grep -noE '(^|[^0-9,.])[0-9]+(\*\*)? (\*\*)?([A-Z][A-Za-z0-9 ]+ )?(operations|ops)\b' "$_page" \
        | sed -E 's/^([0-9]+):[^0-9]*/\1:/' || true)
done

# --- Subset-count claims -------------------------------------------------------
# conformance.md states "N services today: a, b, c" once per suite. Check N
# against the suite's own source (by section heading, so a tfacc number can't
# satisfy the parity line), and check the enumerated list has exactly N items so
# the list and the number cannot drift apart either.
CONF_DOC="website/content/docs/about/conformance.md"
# Fail loud if the page moves: a silently skipped pass is a green gate over
# stale counts, the exact failure this pass exists to prevent.
if [ ! -f "$CONF_DOC" ]; then
    echo "missing $CONF_DOC (subset-count claims moved? update CONF_DOC)" >&2
    exit 2
fi
{
    while IFS=$'\t' read -r lineno section n items; do
        [ -z "$lineno" ] && continue
        case "$section" in
            *[Tt]erraform*) expected_sub="$tfacc_services" ;;
            *[Pp]arity*)    expected_sub="$parity_sandbox" ;;
            *) problems+=("$CONF_DOC:$lineno: '$n services today' under unrecognised section '$section'"); fail=1; continue ;;
        esac
        if [ "$n" != "$expected_sub" ]; then
            problems+=("$CONF_DOC:$lineno: '$section' claims $n services, expected $expected_sub")
            fail=1
        fi
        if [ "$items" != "$n" ]; then
            problems+=("$CONF_DOC:$lineno: '$section' says $n services but lists $items")
            fail=1
        fi
    done < <(awk '
        /^## / { section = substr($0, 4) }
        /^[0-9]+ services today: / {
            n = $1
            list = $0
            sub(/^[0-9]+ services today: /, "", list)
            sub(/\. The source of truth.*$/, "", list)
            sub(/\.$/, "", list)
            print NR "\t" section "\t" n "\t" split(list, _, ", ")
        }' "$CONF_DOC")

    # "The E2E suite is much bigger (N+ tests, ...)". A floor claim is "true"
    # at any N <= actual, which is exactly how "280+" survived against 2,377
    # real tests: require it within 80% of the real count. Deliberately narrow:
    # only the "E2E suite ... (N+ tests" clause is read, so another suite's
    # count elsewhere on the page or the line is never mistaken for it, and any
    # rewording falls through to the loud "no claim found" failure below.
    e2e_claims=0
    while IFS= read -r claim; do
        [ -z "$claim" ] && continue
        claimed=$(last_num "$claim" | tr -d ',')
        [ -z "$claimed" ] && continue
        e2e_claims=$(( e2e_claims + 1 ))
        if [ "$claimed" -gt "$e2e_tests" ] || [ $(( claimed * 100 )) -lt $(( e2e_tests * 80 )) ]; then
            problems+=("$CONF_DOC: E2E suite claims '$claimed+ tests', actual $e2e_tests tests")
            fail=1
        fi
    done < <(grep -oE 'E2E suite[^(]*\([0-9][0-9,]*\+ tests' "$CONF_DOC" || true)
    # The claim vanishing (sentence reworded) must not turn this pass into a no-op.
    if [ "$e2e_claims" -eq 0 ]; then
        problems+=("$CONF_DOC: no 'E2E suite ... (N+ tests' claim found; update this pass if the sentence was reworded")
        fail=1
    fi
}

# --- IAM enforcement table: "All N supported actions" per service -----------
# reference/security.md's "Enforced services" table states each enforced
# service's action count by hand ("| **S3** | All 74 supported actions"),
# and it drifted (S3 said 74 while `supported_actions()` listed 112). The
# canonical count is the service's `supported_actions()` list: its literal
# `&[...]`, or the `const`/`static` list it returns.
SECURITY_DOC="website/content/docs/reference/security.md"
# Format: "<row name>=<file defining that service's supported_actions()>".
ENFORCED_SERVICE_SOURCES=(
    "IAM=crates/fakecloud-iam/src/iam_service/mod.rs"
    "STS=crates/fakecloud-iam/src/sts_service/mod.rs"
    "SQS=crates/fakecloud-sqs/src/service/mod.rs"
    "SNS=crates/fakecloud-sns/src/service.rs"
    "S3=crates/fakecloud-s3/src/service/mod.rs"
    "KMS=crates/fakecloud-kms/src/service.rs"
    "DynamoDB=crates/fakecloud-dynamodb/src/service/mod.rs"
    "DynamoDB Streams=crates/fakecloud-dynamodb/src/streams_dataplane.rs"
    "Lambda=crates/fakecloud-lambda/src/service/mod.rs"
    "EventBridge Scheduler=crates/fakecloud-scheduler/src/service.rs"
)
# The number of string literals in the list `fn supported_actions` returns.
supported_actions_count() {
    awk '
        function count(line,   n) {
            sub(/\/\/.*/, "", line)
            n = 0
            while (match(line, /"[^"]*"/)) { n++; line = substr(line, RSTART + RLENGTH) }
            return n
        }
        # First pass: find the list fn supported_actions returns.
        FNR == NR {
            if ($0 ~ /fn supported_actions/) { infn = 1; next }
            if (infn && !inlist) {
                if ($0 ~ /&\[/) { inlist = 1; sub(/.*&\[/, "") }
                else if (match($0, /[A-Z][A-Z0-9_]+/)) { name = substr($0, RSTART, RLENGTH); infn = 0; next }
                else next
            }
            if (inlist) {
                if ($0 ~ /^[[:space:]]*\]/) { inlist = 0; infn = 0; done = 1; next }
                total += count($0)
            }
            next
        }
        # Second pass (only for a named list): its `const`/`static` body.
        name != "" && $0 ~ ("(const|static) " name ":") { inconst = 1; sub(/.*= *&\[/, ""); }
        inconst {
            if ($0 ~ /^[[:space:]]*\];/) { inconst = 0; next }
            total += count($0)
        }
        END { print total + 0 }
    ' "$1" "$1"
}
for entry in "${ENFORCED_SERVICE_SOURCES[@]}"; do
    row="${entry%%=*}"
    src="${entry#*=}"
    claim=$(awk -F'|' -v row="$row" '{ n = $2; gsub(/^ *\*\*|\*\* *$/, "", n) } n == row { print $3; exit }' "$SECURITY_DOC")
    claimed=$(printf '%s' "$claim" | grep -oE 'All [0-9]+ supported (actions|operations)' | grep -oE '[0-9]+' || true)
    if [ -z "$claimed" ]; then
        problems+=("$SECURITY_DOC: no '| **$row** | All N supported actions' row found; update ENFORCED_SERVICE_SOURCES if the table changed")
        fail=1
        continue
    fi
    actual=$(supported_actions_count "$src")
    if [ "$actual" -eq 0 ]; then
        problems+=("$src: could not read supported_actions() for $row")
        fail=1
    elif [ "$claimed" != "$actual" ]; then
        problems+=("$SECURITY_DOC: $row row says 'All $claimed supported actions', supported_actions() lists $actual")
        fail=1
    fi
done

if [ "$fail" -eq 0 ]; then
    echo "OK — every evergreen surface agrees with the canonical sources."
    exit 0
fi

echo "FAIL — drift detected:" >&2
for p in "${problems[@]}"; do
    echo "  $p" >&2
done
echo >&2
echo "Reconcile the drifted file(s) against the canonical sources at the top of this output," >&2
echo "or update parity.md / conformance-baseline.json / the script constants if the implementation actually changed." >&2
exit 1
