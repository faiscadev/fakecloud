#!/usr/bin/env bash
set -euo pipefail

# Update Smithy models from aws/api-models-aws GitHub repo.
# Models are copied into aws-models/ at the repo root.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEST="$REPO_ROOT/aws-models"
TMPDIR=$(mktemp -d)

trap 'rm -rf "$TMPDIR"' EXIT

echo "Cloning aws/api-models-aws (sparse)..."
cd "$TMPDIR"
git clone --depth 1 --filter=blob:none --sparse \
    https://github.com/aws/api-models-aws.git repo 2>&1 | tail -1

cd repo

# Service mapping: our_name:repo_dir
SERVICES=(
    "sqs:sqs"
    "sns:sns"
    "eventbridge:eventbridge"
    "iam:iam"
    "sts:sts"
    "ssm:ssm"
    "s3:s3"
    "dynamodb:dynamodb"
    "lambda:lambda"
    "secretsmanager:secrets-manager"
    "cloudwatch-logs:cloudwatch-logs"
    "kms:kms"
    "kinesis:kinesis"
    "cloudformation:cloudformation"
    "sesv2:sesv2"
    "cognito-identity-provider:cognito-identity-provider"
)

# Sparse checkout only the models we need
SPARSE_DIRS=()
for mapping in "${SERVICES[@]}"; do
    repo_dir="${mapping#*:}"
    SPARSE_DIRS+=("models/$repo_dir")
done
git sparse-checkout set "${SPARSE_DIRS[@]}"

# Copy each model
for mapping in "${SERVICES[@]}"; do
    our_name="${mapping%%:*}"
    repo_dir="${mapping#*:}"
    json_file=$(find "models/$repo_dir" -name "*.json" -type f | head -1)
    if [ -z "$json_file" ]; then
        echo "WARNING: No model found for $our_name (repo dir: $repo_dir)"
        continue
    fi
    cp "$json_file" "$DEST/$our_name.json"
    echo "Updated $our_name.json from $json_file"
done

echo ""
echo "Done. Review changes with: git diff aws-models/"
