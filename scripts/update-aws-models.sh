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
    "serverlessrepo:serverlessapplicationrepository"
    "managedblockchain:managedblockchain"
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
    "docdb:docdb"
    "neptune:neptune"
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
    "support:support"
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
# Amazon Timestream ships as two Smithy models (write + query) that share the
# same service family and awsJson1_0 target prefix. fakecloud serves both from
# one crate under one service key, so we merge them into a single combined
# aws-models/timestream.json below.
SPARSE_DIRS+=("models/timestream-write" "models/timestream-query")
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

# Merge Timestream write + query into one combined model keyed as "timestream".
# The two models live in distinct namespaces (no shape-id collisions); we keep
# one service shape whose operations list is the union (dropping the query
# duplicates of the four ops shared with write: DescribeEndpoints,
# ListTagsForResource, TagResource, UntagResource).
TS_WRITE=$(find models/timestream-write -name "*.json" -type f | head -1)
TS_QUERY=$(find models/timestream-query -name "*.json" -type f | head -1)
if [ -n "$TS_WRITE" ] && [ -n "$TS_QUERY" ]; then
    python3 - "$TS_WRITE" "$TS_QUERY" "$DEST/timestream.json" <<'PY'
import json, sys, collections
w = json.load(open(sys.argv[1]), object_pairs_hook=collections.OrderedDict)
q = json.load(open(sys.argv[2]), object_pairs_hook=collections.OrderedDict)
wsvc = [k for k, v in w["shapes"].items() if v.get("type") == "service"][0]
qsvc = [k for k, v in q["shapes"].items() if v.get("type") == "service"][0]
shared = {"DescribeEndpoints", "ListTagsForResource", "TagResource", "UntagResource"}
q_ops = [o for o in q["shapes"][qsvc]["operations"]
         if o["target"].rsplit("#", 1)[1] not in shared]
combined = list(w["shapes"][wsvc]["operations"]) + q_ops
shapes = collections.OrderedDict(w["shapes"])
for k, v in q["shapes"].items():
    if k != qsvc:
        shapes[k] = v
shapes[wsvc]["operations"] = combined
model = collections.OrderedDict([("smithy", "2.0"), ("shapes", shapes)])
with open(sys.argv[3], "w") as f:
    json.dump(model, f, indent=2)
    f.write("\n")
print("Updated timestream.json (merged write + query, %d ops)" % len(combined))
PY
else
    echo "WARNING: could not find both timestream-write and timestream-query models"
fi

echo ""
echo "Done. Review changes with: git diff aws-models/"
