+++
title = "Fake AWS server for tests"
description = "fakecloud is a free, open-source fake AWS server for integration tests. Single binary, port 4566, any AWS SDK in any language. No account, no auth token, no paid tier."
template = "page.html"
+++

Need a fake AWS server to point your tests at? That's what [fakecloud](https://github.com/faiscadev/fakecloud) is.

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

Listens on `http://localhost:4566`. Any AWS SDK in any language points at it and it responds like AWS.

## What "fake AWS server" means here

- **Real HTTP server**, not an in-process mock. Your Go / Java / Kotlin / Node / Rust / PHP / Python code uses the regular AWS SDK with `endpoint_url` set to `http://localhost:4566`.
- **Speaks the AWS wire protocol** at true 100% conformance across every implemented service. 105 services, 7,408 operations, 248,557/248,557 Smithy-model-generated test variants pass on every commit.
- **Real execution** for stateful services: Lambda runs your function code in Docker containers across 23 runtimes, RDS runs real PostgreSQL/MySQL/MariaDB/Oracle/SQL Server/Db2, ElastiCache runs real Redis/Valkey/Memcached.
- **Real cross-service wiring**: S3 -> Lambda, SQS -> Lambda, SNS fan-out, EventBridge -> Step Functions, and 15+ more integrations execute end-to-end, not as stubs.
- **Free, open-source, AGPL-3.0.** No account, no auth token, no paid tier.

## Services covered

S3, SQS, SNS, EventBridge, EventBridge Scheduler, Lambda, DynamoDB, IAM, STS, SSM, Secrets Manager, CloudWatch Logs, CloudWatch (Metrics & Alarms), KMS, CloudFormation, Cloud Control API, SES (v2 + v1 inbound), Cognito User Pools, Cognito Identity, Kinesis, Firehose, RDS, RDS Data API, Aurora DSQL, Resource Groups, ElastiCache, Step Functions, API Gateway v1 (REST), API Gateway v2 (HTTP), Bedrock, Bedrock Agent, Bedrock Agent Runtime, Bedrock Runtime, ECR, ECS, EC2, Elastic Load Balancing v2, CloudFront, Route 53, WAF v2, Application Auto Scaling, Athena, ACM, Glue, Organizations.

Full matrix: [fakecloud.dev/docs/services](/docs/services/).

## Minimal example

### Node.js

```ts
process.env.AWS_ENDPOINT_URL = "http://localhost:4566";
process.env.AWS_ACCESS_KEY_ID = "test";
process.env.AWS_SECRET_ACCESS_KEY = "test";
process.env.AWS_REGION = "us-east-1";

import { S3Client, CreateBucketCommand } from "@aws-sdk/client-s3";
const s3 = new S3Client({});
await s3.send(new CreateBucketCommand({ Bucket: "hello" }));
```

### Python

```python
import boto3
s3 = boto3.client("s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1")
s3.create_bucket(Bucket="hello")
```

### Go

```go
cfg, _ := config.LoadDefaultConfig(ctx,
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
    config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
        func(service, region string, opts ...interface{}) (aws.Endpoint, error) {
            return aws.Endpoint{URL: "http://localhost:4566"}, nil
        },
    )),
)
s3 := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
```

### AWS CLI

```sh
aws --endpoint-url http://localhost:4566 s3 mb s3://hello
```

## How it differs from in-process mocks

Mocks (Moto, aws-sdk-client-mock, aws-sdk-mock) intercept SDK calls inside your test process and return fabricated responses. That's useful for tight unit tests but it does not execute cross-service wiring, does not run Lambda code, and is language-locked to the library's ecosystem.

A fake AWS server on localhost is language-agnostic (any SDK works), runs real cross-service flows, and executes Lambda code in real containers. If your tests ask "does my whole system actually work end-to-end," the server is the right tool.

## Install options

- Homebrew: `brew install fakecloud`
- Binary: `curl -fsSL https://fakecloud.dev/install.sh | bash`
- Docker: `docker run --rm -p 4566:4566 ghcr.io/faiscadev/fakecloud`
- Cargo: `cargo install fakecloud`
- Docker Compose and source builds: [docs](/docs/getting-started/)

## Test-assertion SDKs

fakecloud ships introspection endpoints so your tests can assert on side effects (emails sent, messages published, Lambdas invoked) without writing raw HTTP:

```ts
import { FakeCloud } from "fakecloud";
const fc = new FakeCloud();

const { emails } = await fc.ses.getEmails();
expect(emails).toHaveLength(1);

await fc.reset();
```

SDKs for TypeScript, Python, Go, PHP, Java, Rust. Docs: [fakecloud.dev/docs/sdks](/docs/sdks/).

## Links

- **Repo:** [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- **Docs:** [fakecloud.dev/docs](/docs/)
- **Getting started:** [fakecloud.dev/docs/getting-started](/docs/getting-started/)
- **LocalStack alternative page:** [fakecloud.dev/localstack-alternative](/localstack-alternative/)
- **Migration from LocalStack:** [blog/migrate-from-localstack](/blog/migrate-from-localstack/)
- **Issues:** [github.com/faiscadev/fakecloud/issues](https://github.com/faiscadev/fakecloud/issues)
