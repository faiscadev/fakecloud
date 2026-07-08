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
    "account:account"
    "amplify:amplify"
    "mediaconvert:mediaconvert"
    "config:config-service"
    "identitystore:identitystore"
    "ssoadmin:sso-admin"
    "verifiedpermissions:verifiedpermissions"
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
    "rds:rds"
    "dms:database-migration-service"
    "cloudtrail:cloudtrail"
    "rds-data:rds-data"
    "dsql:dsql"
    "elasticache:elasticache"
    "elasticbeanstalk:elastic-beanstalk"
    "memorydb:memorydb"
    "eks:eks"
    "glacier:glacier"
    "backup:backup"
    "transfer:transfer"
    "appconfig:appconfig"
    "appconfigdata:appconfigdata"
    "es:elasticsearch-service"
    "opensearch:opensearch"
    "servicediscovery:servicediscovery"
    "sfn:sfn"
    "bedrock:bedrock"
    "bedrock-runtime:bedrock-runtime"
    "scheduler:scheduler"
    "apigateway:api-gateway"
    "apigatewayv2:apigatewayv2"
    "ecr:ecr"
    "ecs:ecs"
    "elasticloadbalancingv2:elastic-load-balancing-v2"
    "cloudfront:cloudfront"
    "route53:route-53"
    "route53resolver:route53resolver"
    "acm:acm"
    "acm-pca:acm-pca"
    "application-autoscaling:application-auto-scaling"
    "wafv2:wafv2"
    "athena:athena"
    "cloudwatch:cloudwatch"
    "firehose:firehose"
    "glue:glue"
    "organizations:organizations"
    "ec2:ec2"
    "pipes:pipes"
    "cloudcontrolapi:cloudcontrol"
    "resource-groups:resource-groups"
    "ce:cost-explorer"
    "tagging:resource-groups-tagging-api"
    "lakeformation:lakeformation"
    "codebuild:codebuild"
    "codecommit:codecommit"
    "s3tables:s3tables"
    "ram:ram"
    "codeconnections:codeconnections"
    "codedeploy:codedeploy"
    "codepipeline:codepipeline"
    "codeartifact:codeartifact"
    "efs:efs"
    "mq:mq"
    "kafka:kafka"
    "kinesisanalyticsv2:kinesis-analytics-v2"
    "mwaa:mwaa"
    "emr:emr"
    "textract:textract"
    "transcribe:transcribe"
    "comprehend:comprehend"
    "translate:translate"
    "fis:fis"
    "shield:shield"
    "xray:xray"
    "appsync:appsync"
    "swf:swf"
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
