+++
title = "Test Lambda locally"
description = "Run AWS Lambda locally with real runtimes and real event triggers. 23 Lambda runtimes, real S3/SQS/SNS/EventBridge triggers, no account required."
template = "page.html"
+++

Want to run AWS Lambda locally? Use [fakecloud](https://github.com/faiscadev/fakecloud).

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

## What you get

- **All 27 AWS Lambda runtimes** — Node 16/18/20/22/24, Python 3.8/3.9/3.10/3.11/3.12/3.13/3.14, Java 11/17/21/25, .NET 6/8/10, Ruby 3.2/3.3/3.4, Go 1.x, custom (`provided.al2`, `provided.al2023`).
- **Real code execution.** fakecloud pulls the AWS Lambda runtime container and runs your handler. Not a stub, not a simulated response.
- **Real event triggers.** S3 -> Lambda, SQS -> Lambda, SNS -> Lambda, EventBridge -> Lambda, DynamoDB Streams -> Lambda, API Gateway v2 -> Lambda. All wired end-to-end.
- **Real event source mappings.** fakecloud polls SQS/Kinesis, batches events, invokes your Lambda, handles success/failure/DLQ the way AWS does.
- **CloudWatch Logs.** Your function's stdout captured and queryable via `aws logs tail`.
- **CloudWatch metrics.** Every invoke publishes `Invocations`, `Errors`, `Duration`, `Throttles`, and `ConcurrentExecutions` to the `AWS/Lambda` namespace, so dashboards and `GetMetricStatistics` work the same as on AWS.
- **No account, no auth token, no paid tier.**

## Quick deploy + invoke

```sh
# Node.js example
cat > index.js <<'EOF'
exports.handler = async (event) => ({ ok: true, event });
EOF
zip fn.zip index.js

aws --endpoint-url http://localhost:4566 iam create-role \
  --role-name lambda-role \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

aws --endpoint-url http://localhost:4566 lambda create-function \
  --function-name hello \
  --runtime nodejs20.x \
  --role arn:aws:iam::000000000000:role/lambda-role \
  --handler index.handler \
  --zip-file fileb://fn.zip

aws --endpoint-url http://localhost:4566 lambda invoke \
  --function-name hello \
  --payload '{"hi":true}' \
  --cli-binary-format raw-in-base64-out \
  out.json

cat out.json
```

## How it compares

| Tool | Runs function code | Multi-runtime | Cross-service triggers | Language | Free |
|---|---|---|---|---|---|
| fakecloud | Yes (Docker) | 23 runtimes | Yes (S3, SQS, SNS, EventBridge, DynamoDB Streams, API GW v2) | Any | Yes |
| SAM Local | Yes (Docker) | All AWS runtimes | Partial (synthetic events only) | Any | Yes |
| LocalStack Community (post-Mar 2026) | Yes (Docker, auth required) | All | Yes | Any | No (auth token required) |
| serverless-offline | Yes (in-process) | Node only | API Gateway only | Node | Yes |
| Moto | No | N/A | No | Python | Yes |

## Full tutorial

Step-by-step guide with S3, SQS, EventBridge triggers and test-assertion examples: [How to test Lambda locally](/blog/test-lambda-locally/).

## Install

- Binary: `curl -fsSL https://fakecloud.dev/install.sh | bash`
- Docker: `docker run --rm -p 4566:4566 -v /var/run/docker.sock:/var/run/docker.sock ghcr.io/faiscadev/fakecloud`
- Cargo: `cargo install fakecloud`

## Links

- **Lambda service docs:** [fakecloud.dev/docs/services/lambda](/docs/services/lambda/)
- **Repo:** [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- **Full tutorial:** [How to test Lambda locally](/blog/test-lambda-locally/)
- **CI tutorial:** [Integration testing AWS in GitHub Actions](/blog/integration-testing-aws-in-ci/)
