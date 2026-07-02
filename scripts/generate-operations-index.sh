#!/usr/bin/env bash
# Generate the per-operation index page at
# website/content/docs/operations/_index.md from the AWS Smithy models in
# aws-models/*.json. The output is a high-density crawler index — every AWS
# operation fakecloud might encounter, grouped by service, fully linked.
#
# Run locally with `bash scripts/generate-operations-index.sh`. CI runs the
# script and `git diff --exit-code` to ensure the generated page stays in sync
# with the source models (see `doc-counts` job in .github/workflows/ci.yml).
#
# This page intentionally lists every operation AWS defines, not only the
# subset fakecloud implements. The page header points readers at the parity
# matrix for fakecloud's per-service implementation status — that's the
# authoritative source of "what works today". Listing the full surface keeps
# the page useful for search-engine discovery without hand-rolling lists that
# go stale every release.

set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"

MODELS_DIR="aws-models"
OUT="website/content/docs/operations/_index.md"

if [ ! -d "$MODELS_DIR" ]; then
    echo "missing $MODELS_DIR" >&2
    exit 2
fi

# Service model -> human-readable title + parity-matrix anchor.
# Smithy filenames don't always match the service docs slug, so keep an
# explicit map. Order = display order on the generated page.
#
# Format: <model_basename>|<display_name>|<service_docs_slug_or_empty>
SERVICES=(
    "s3|S3|s3"
    "sqs|SQS|sqs"
    "sns|SNS|sns"
    "eventbridge|EventBridge|eventbridge"
    "pipes|EventBridge Pipes|pipes"
    "scheduler|EventBridge Scheduler|scheduler"
    "lambda|Lambda|lambda"
    "dynamodb|DynamoDB|dynamodb"
    "iam|IAM|iam"
    "sts|STS|sts"
    "ssm|SSM|ssm"
    "secretsmanager|Secrets Manager|secretsmanager"
    "cloudwatch-logs|CloudWatch Logs|logs"
    "kms|KMS|kms"
    "cloudformation|CloudFormation|cloudformation"
    "cloudcontrolapi|Cloud Control API|cloudcontrol"
    "sesv2|SES|ses"
    "cognito-identity-provider|Cognito User Pools|cognito"
    "cognito-identity|Cognito Identity|cognito"
    "kinesis|Kinesis|kinesis"
    "rds|RDS|rds"
    "rds-data|RDS Data|rds-data"
    "dsql|Aurora DSQL|dsql"
    "resource-groups|Resource Groups|resource-groups"
    "tagging|Resource Groups Tagging API|resource-groups-tagging"
    "elasticache|ElastiCache|elasticache"
    "memorydb|MemoryDB|memorydb"
    "eks|EKS|eks"
    "sfn|Step Functions|stepfunctions"
    "apigateway|API Gateway v1|apigateway"
    "apigatewayv2|API Gateway v2|apigatewayv2"
    "bedrock|Bedrock|bedrock"
    "bedrock-runtime|Bedrock Runtime|bedrock"
    "bedrock-agent|Bedrock Agent|bedrock-agent"
    "bedrock-agent-runtime|Bedrock Agent Runtime|bedrock-agent-runtime"
    "ecr|ECR|ecr"
    "ecs|ECS|ecs"
    "elasticloadbalancingv2|Elastic Load Balancing v2|elbv2"
    "cloudfront|CloudFront|cloudfront"
    "route53|Route 53|route53"
    "wafv2|WAF v2|wafv2"
    "application-autoscaling|Application Auto Scaling|application-autoscaling"
    "batch|Batch|batch"
    "athena|Athena|athena"
    "acm|ACM|acm"
    "cloudwatch|CloudWatch (Metrics & Alarms)|cloudwatch"
    "firehose|Firehose|firehose"
    "glue|Glue|glue"
    "organizations|Organizations|organizations"
    "ec2|EC2|ec2"
)

# Services parity.md tracks but aws-models does NOT include. Listed at the
# bottom of the generated page so the page is honest about coverage without
# hand-rolling operation lists from another source. Empty now that every
# tracked service ships a first-party Smithy model.
SERVICES_WITHOUT_MODELS=()

# Extract operation names (sorted) from a Smithy model file.
extract_ops() {
    local model_path="$1"
    jq -r '
        .shapes
        | to_entries
        | map(
            select(.value.type == "operation")
            | .key
            | sub("^com\\.amazonaws\\.[^#]+#"; "")
        )
        | sort
        | .[]
    ' "$model_path"
}

# Stage output in a temp file, then move into place atomically. Avoids leaving
# the repo in a half-generated state if the script fails midway.
tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT

mkdir -p "$(dirname "$OUT")"

# Detect new aws-models/*.json files that aren't mapped in SERVICES above.
# Without this guard, a new model would silently be omitted from the generated
# index and CI would still pass — defeats the point of regenerating from
# source. The `service-map.json` file is metadata, not a service model.
mapped=$(printf '%s\n' "${SERVICES[@]}" | cut -d'|' -f1 | sort -u)
present=$(find "$MODELS_DIR" -maxdepth 1 -name '*.json' -not -name 'service-map.json' -exec basename {} .json \; | sort -u)
unmapped=$(comm -23 <(echo "$present") <(echo "$mapped"))
if [ -n "$unmapped" ]; then
    echo "ERROR: ${MODELS_DIR} contains models not mapped in SERVICES:" >&2
    echo "$unmapped" | sed 's/^/  /' >&2
    echo "Add each one to the SERVICES array with its display name and docs slug." >&2
    exit 2
fi

cat >"$tmp" <<'HEADER'
+++
title = "AWS Operations Index"
description = "Every AWS operation fakecloud might encounter, grouped by service. Generated from AWS Smithy models on every commit, so it stays in sync with what the SDKs actually call."
weight = 2
template = "docs.html"
+++

<!-- generated by scripts/generate-operations-index.sh — do not edit by hand -->

This page lists every AWS API operation defined in the Smithy models for services fakecloud implements. It exists to make the operation surface discoverable by search engines and AI crawlers — `aws s3 get-object`, `lambda:InvokeFunction`, `cognito-idp:AdminInitiateAuth`, every name an SDK might emit appears here verbatim.

This is a surface listing, not an implementation manifest. For fakecloud's per-service implementation status — what's fully wired, what's control-plane only, and the known data-plane gaps — see the [parity matrix](@/docs/parity.md). For the authoritative AWS model files, see [`aws-models/`](https://github.com/faiscadev/fakecloud/tree/main/aws-models) on GitHub.

HEADER

total=0
for entry in "${SERVICES[@]}"; do
    IFS='|' read -r model display slug <<<"$entry"
    model_path="${MODELS_DIR}/${model}.json"
    if [ ! -f "$model_path" ]; then
        echo "missing model: $model_path" >&2
        exit 2
    fi
    ops=$(extract_ops "$model_path")
    if [ -z "$ops" ]; then
        echo "no operations extracted from $model_path" >&2
        exit 2
    fi
    {
        if [ -n "$slug" ]; then
            echo "## [${display}](@/docs/services/${slug}.md)"
        else
            echo "## ${display}"
        fi
        echo
        while IFS= read -r op; do
            echo "- \`${op}\`"
            total=$((total + 1))
        done <<<"$ops"
        echo
    } >>"$tmp"
done

# Append the services without smithy models so the page is honest about
# coverage. Skipped entirely when every tracked service ships a model.
if [ "${#SERVICES_WITHOUT_MODELS[@]}" -gt 0 ]; then
{
    echo "## Services without an AWS Smithy model file"
    echo
    echo "fakecloud implements these services, but no first-party Smithy model ships in [\`aws-models/\`](https://github.com/faiscadev/fakecloud/tree/main/aws-models). Operation lists for these live on their per-service pages and in the [parity matrix](@/docs/parity.md)."
    echo
    for entry in "${SERVICES_WITHOUT_MODELS[@]}"; do
        IFS='|' read -r display slug <<<"$entry"
        if [ -n "$slug" ]; then
            echo "- [${display}](@/docs/services/${slug}.md)"
        else
            echo "- ${display}"
        fi
    done
    echo
} >>"$tmp"
fi

mv "$tmp" "$OUT"
trap - EXIT

echo "Wrote $OUT (${#SERVICES[@]} services with models, ${#SERVICES_WITHOUT_MODELS[@]} listed without models, $total operation entries)."
