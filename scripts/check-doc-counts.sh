#!/usr/bin/env bash
# Verify every evergreen doc/marketing headline that quotes a service count,
# operation count, or Smithy variant count agrees with the canonical sources:
#
#   - website/content/docs/parity.md   (sum of Ops column + row count)
#   - conformance-baseline.json        (variants_passed + total_variants)
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

# Variant counts straight out of the baseline JSON.
variants_pass=$(jq -r .variants_passed "$BASELINE")
variants_total=$(jq -r .total_variants "$BASELINE")

# Comma-format thousands. `printf %'d` is POSIX-locale-dependent so force C.UTF-8 + en_US.UTF-8 fallback.
fmt() {
    # LC_ALL=en_US.UTF-8 makes %'d insert thousands separators on both macOS and Linux.
    LC_ALL=en_US.UTF-8 printf "%'d" "$1"
}

ops_fmt=$(fmt "$parity_ops")
vp_fmt=$(fmt "$variants_pass")
vt_fmt=$(fmt "$variants_total")

echo "Canonical truth:"
echo "  services           = $parity_services (parity.md row count)"
echo "  operations         = $parity_ops ($ops_fmt) (sum of parity.md Ops column)"
echo "  variants_passed    = $variants_pass ($vp_fmt)"
echo "  total_variants     = $variants_total ($vt_fmt)"
echo

# Files to check. Evergreen-only. Blog posts and dated marketing drafts excluded
# per feedback_no_blogpost_updates.
FILES=(
    README.md
    AGENTS.md
    website/content/docs/parity.md
    website/content/docs/services/_index.md
    website/content/docs/about/conformance.md
    website/content/docs/about/what-it-is.md
    website/content/faq.md
    website/content/localstack-alternative.md
    website/content/fake-aws-server.md
    website/content/fake-bedrock.md
    website/content/dynamodb-emulator.md
    website/content/vs/floci.md
    website/content/vs/ministack.md
    website/content/vs/moto.md
    website/content/vs/elasticmq.md
    website/content/vs/sam-local.md
    website/content/vs/localstack.md
    website/static/llms.txt
    website/static/llms-full.txt
    website/templates/index.html
)

# Known exceptions: file:kind:value
# These are intentional non-headline mentions where the number is correct in
# its local context (subset counts, rhetorical comparisons, etc.).
EXCEPTIONS=(
    # tfacc covers a subset of services
    "website/content/docs/about/conformance.md:services:12"
    # real-AWS parity sandbox covers a subset
    "website/content/docs/about/conformance.md:services:7"
    # rhetorical comparison: "depth-first vs N services at 50%"
    "website/content/docs/about/what-it-is.md:services:100"
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
echo "or update parity.md / conformance-baseline.json if the implementation actually changed." >&2
exit 1
