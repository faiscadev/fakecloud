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
STARTUP_MS=300
IDLE_MEM_MIB=10
BINARY_MB=19

# --- Lambda runtime count ---
# Canonical source: `runtime_to_image()` in crates/fakecloud-lambda/src/runtime.rs.
# That match expression is the actual list of supported runtimes — anything not
# in it returns None and `CreateFunction` rejects it. Count it with:
#
#   grep -cE '^\s*"[^"]+"\s*=>\s*\(' crates/fakecloud-lambda/src/runtime.rs
#
# (= 23 as of 2026-05-20). When fakecloud-lambda gains/drops a runtime, update
# this constant, the runtime list in docs/services/lambda.md, and audit every
# page in FILES in the same PR.
LAMBDA_RUNTIMES=23

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

echo "Canonical truth:"
echo "  services           = $parity_services (parity.md row count)"
echo "  operations         = $parity_ops ($ops_fmt) (sum of parity.md Ops column)"
echo "  variants_passed    = $variants_pass ($vp_fmt)"
echo "  total_variants     = $variants_total ($vt_fmt)"
echo "  startup_ms         = $STARTUP_MS (script constant)"
echo "  idle_mem_mib       = $IDLE_MEM_MIB (script constant)"
echo "  binary_mb          = $BINARY_MB (script constant)"
echo "  bedrock surface    = $bedrock_ctrl + $bedrock_runtime + $bedrock_agent + $bedrock_agent_rt = $bedrock_family (parity.md rows)"
echo "  lambda_runtimes    = $LAMBDA_RUNTIMES (script constant; canonical: docs/services/lambda.md)"
echo

# Files to check. Evergreen-only. Blog posts and dated marketing drafts excluded
# per feedback_no_blogpost_updates.
FILES=(
    README.md
    AGENTS.md
    website/content/_index.md
    website/content/docs/_index.md
    website/content/docs/parity.md
    website/content/docs/services/_index.md
    website/content/docs/about/conformance.md
    website/content/docs/about/what-it-is.md
    website/content/docs/migration-from-localstack.md
    website/content/docs/getting-started/install.md
    website/content/faq.md
    website/content/glossary.md
    website/content/localstack-alternative.md
    website/content/supported-services.md
    website/content/fake-aws-server.md
    website/content/fake-bedrock.md
    website/content/dynamodb-emulator.md
    website/content/vs/dynamodb-local.md
    website/content/vs/elasticmq.md
    website/content/vs/floci.md
    website/content/vs/localstack.md
    website/content/vs/minio.md
    website/content/vs/ministack.md
    website/content/vs/moto.md
    website/content/vs/s3mock.md
    website/content/vs/sam-local.md
    website/content/vs/testcontainers.md
    website/static/llms.txt
    website/static/llms-full.txt
    website/templates/index.html
)

# Known exceptions: file:kind:value
# These are intentional non-headline mentions where the number is correct in
# its local context (subset counts, rhetorical comparisons, etc.).
EXCEPTIONS=(
    # tfacc covers a subset of services
    "website/content/docs/about/conformance.md:services:27"
    # real-AWS parity sandbox covers a subset
    "website/content/docs/about/conformance.md:services:7"
    # rhetorical comparison: "depth-first vs N services at 50%"
    "website/content/docs/about/what-it-is.md:services:100"
    # vs/localstack.md aliases redirect legacy blog slugs that have "500ms" in
    # the URL itself. They're URLs we have to match verbatim, not performance claims.
    "website/content/vs/localstack.md:startup_ms:500"
    # "AWS AppConfig: 58 operations" is a DIFFERENT service from AWS Config; the
    # per-service regex matches the "Config" tail of "AppConfig". AppConfig's 58
    # op count is correct in its own context.
    "website/content/supported-services.md:ops_Config:58"
    "website/static/llms.txt:ops_Config:58"
    "website/static/llms-full.txt:ops_Config:58"
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

for f in "${FILES[@]}"; do
    if [ ! -f "$f" ]; then
        continue
    fi

    # --- Service count claims ---
    # Catches "39 services", "39 AWS services", "39 services covered", etc.
    while read -r hit; do
        [ -z "$hit" ] && continue
        if [ "$hit" != "$parity_services" ] && ! is_exception "$f" services "$hit"; then
            problems+=("$f: claims '$hit services', expected $parity_services")
            fail=1
        fi
    done < <(grep -oE "\b[0-9]+ (AWS )?services\b" "$f" | grep -oE "^[0-9]+" | sort -u)

    # --- Operation total claims ---
    # Comma-formatted thousands only — avoids matching per-service mini-counts
    # like "23 ops" inside feature bullets. "2,592 operations" / "2,592 API operations" etc.
    while read -r hit; do
        [ -z "$hit" ] && continue
        if [ "$hit" != "$ops_fmt" ] && ! is_exception "$f" operations "$hit"; then
            problems+=("$f: claims '$hit operations', expected $ops_fmt")
            fail=1
        fi
    done < <(grep -oE "\b[0-9],[0-9]{3} (API )?operations\b" "$f" | grep -oE "^[0-9],[0-9]{3}" | sort -u)

    # --- Variant pass-rate claims (X,XXX/Y,YYY) ---
    expected_pair="$vp_fmt/$vt_fmt"
    while read -r hit; do
        [ -z "$hit" ] && continue
        if [ "$hit" != "$expected_pair" ] && ! is_exception "$f" variants "$hit"; then
            problems+=("$f: variants '$hit', expected $expected_pair")
            fail=1
        fi
    done < <(grep -oE "\b[0-9]+,[0-9]{3}/[0-9]+,[0-9]{3}\b" "$f" | sort -u)

    # --- Bare variant total claims ("86,327 variants", "86,327 generated...") ---
    # Catches stale "59,000+ variants" / "54,000+ variants" framing too.
    while read -r hit; do
        [ -z "$hit" ] && continue
        if [ "$hit" != "$vp_fmt" ] && ! is_exception "$f" variants "$hit"; then
            problems+=("$f: claims '$hit variants', expected $vp_fmt")
            fail=1
        fi
    done < <(grep -oE "\b[0-9]+,[0-9]{3}\+? (Smithy[-a-z]* )?(generated )?(test )?variants\b" "$f" | grep -oE "^[0-9]+,[0-9]{3}" | sort -u)

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
    done < <(grep -oE '\b[0-9]+\s+runtimes\b' "$f" | grep -oE '^[0-9]+' | sort -u)

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
    # "~10 MiB", "10 MiB idle", "10 MiB idle memory". Avoids false positives by
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
    # "~19 MB binary", "19MB binary", "19 MB static binary", "binary (~19 MB)".
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

    # --- Per-service op count claims ---
    # Catches "**S3**: 154 operations", "Lambda (82 operations)", etc. Walks the
    # per-service map and looks for any service-name followed by a number+ops
    # phrase. Skips parity.md (it's the source) and the per-service service docs
    # under docs/services/ (those are the source for their own service).
    if [[ "$f" == "$PARITY" || "$f" == website/content/docs/services/*.md ]]; then
        continue_per_service=1
    else
        continue_per_service=0
    fi
    if [ "$continue_per_service" -eq 0 ]; then
        while IFS=$'\t' read -r svc canonical_ops; do
            [ -z "$svc" ] && continue
            # Bedrock family is handled by the dedicated Bedrock-surface check
            # above, which accepts both the per-API count and the family sum.
            # Skipping here avoids double-firing on the same phrase.
            case "$svc" in
                "Bedrock"|"Bedrock Runtime"|"Bedrock Agent"|"Bedrock Agent Runtime") continue ;;
            esac
            # Escape regex specials in service name for grep (parentheses, etc.)
            svc_re=$(printf '%s\n' "$svc" | sed 's/[][\.*^$()+?{}|]/\\&/g')
            while read -r hit; do
                [ -z "$hit" ] && continue
                if [ "$hit" != "$canonical_ops" ] && ! is_exception "$f" "ops_${svc}" "$hit"; then
                    problems+=("$f: claims '$svc: $hit ops', expected $canonical_ops")
                    fail=1
                fi
            done < <(
                grep -oE "(\*\*)?${svc_re}(\*\*)?[: ]\(?[ ]*[0-9]+ (operations?|ops)\b" "$f" \
                    | grep -oE '[0-9]+ (operations?|ops)' \
                    | grep -oE '^[0-9]+' \
                    | sort -u
            )
        done <<< "$service_ops_map"
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
