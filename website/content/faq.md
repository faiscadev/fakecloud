+++
title = "fakecloud FAQ"
description = "Frequently asked questions about fakecloud: the free, open-source local AWS cloud emulator. Installation, LocalStack migration, supported services, licensing, and more."
template = "page.html"
+++

Common questions about fakecloud. For setup guides and tutorials, see the [docs](/docs/) and [blog](/blog/).

### What is fakecloud?

fakecloud is a free, open-source local AWS cloud emulator for integration testing and local development. It runs on a single port (4566), requires no account or auth token, and aims for 100% behavioral conformance with real AWS on every service it implements. AGPL-3.0 licensed.

### Is fakecloud free?

Yes. AGPL-3.0, free for commercial use. Using fakecloud as a dev/test dependency has zero AGPL implications for your application — the copyleft clause only kicks in if you modify fakecloud itself and redistribute it as a network service.

### Is fakecloud a LocalStack alternative?

Yes. LocalStack replaced its open-source Community Edition with a proprietary image in March 2026 that requires an account and auth token. fakecloud is a free, open-source replacement. See the [LocalStack alternative page](/localstack-alternative/) and the [migration guide](/blog/migrate-from-localstack/).

### How many AWS services does fakecloud support?

41 services and 3,704 API operations. 124,255/124,255 generated Smithy conformance variants pass on every commit — true 100% across every implemented service, with more services on the roadmap. The explicit goal is 100% of AWS services, each at 100% behavioral conformance, with 100% of cross-service integrations. Services land depth-first — a service is added when it passes the full Smithy-model test variants and cross-service wire-ups.

### Which AWS services are supported?

S3, SQS, SNS, DynamoDB, Lambda, IAM, STS, KMS, Secrets Manager, SSM, CloudWatch Logs, CloudWatch (Metrics & Alarms), CloudFormation, EventBridge, EventBridge Scheduler, SES (v2 + v1 inbound), Cognito User Pools, Cognito Identity, Kinesis, Firehose, Glue, Athena, RDS, ElastiCache, ECR, ECS, Elastic Load Balancing v2, CloudFront, Route 53, WAFv2, ACM, Application Auto Scaling, Step Functions, API Gateway v1 (REST), API Gateway v2 (HTTP), Bedrock, Bedrock Agent, Bedrock Agent Runtime, Bedrock Runtime.

### Does fakecloud execute Lambda code for real?

Yes. fakecloud pulls real AWS Lambda runtime containers and executes your handler against them. 27 official runtimes: Node.js 16/18/20/22/24, Python 3.8/3.9/3.10/3.11/3.12/3.13/3.14, Java 11/17/21/25, .NET 6/8/10, Ruby 3.2, Go (`go1.x`), and custom `provided`/`provided.al2`/`provided.al2023`. See [Test Lambda locally](/test-lambda-locally/).

### Does fakecloud run real databases for RDS?

Yes. RDS emulation pulls real PostgreSQL / MySQL / MariaDB / Oracle / SQL Server / Db2 Docker images and runs them as the DB instance. Your SQL schema, indexes, triggers, and extensions work because the engine is real. See [Local RDS for tests](/local-rds/).

### Does fakecloud run real Redis for ElastiCache?

Yes. ElastiCache runs real Redis / Valkey / Memcached Docker images. `LPUSH`, `ZADD`, `XADD`, streams, pub/sub, Lua scripts on Redis/Valkey — and the full memcached text protocol on Memcached — all work. See [Local ElastiCache for tests](/local-elasticache/).

### Do S3 notifications fire Lambda?

Yes, end-to-end. When an object is created in S3, any Lambda subscribed via bucket notification configuration fires for real — the Lambda code runs in a real runtime container. Same for SNS / SQS subscriptions.

### How do I install fakecloud?

One-line install script:

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

Or Docker: `docker run --rm -p 4566:4566 ghcr.io/faiscadev/fakecloud`. Or `cargo install fakecloud`.

### Does fakecloud work with Terraform?

Yes. CI runs the upstream `hashicorp/terraform-provider-aws` `TestAcc*` suites against fakecloud on every commit. See [Terraform local development for AWS](/blog/terraform-local-development-aws/).

### Does fakecloud work with CDK?

Yes. Use the plain `cdk` binary with `AWS_ENDPOINT_URL=http://localhost:4566`, or `cdklocal`. See [CDK local testing](/blog/cdk-local-testing/).

### Can I use fakecloud in CI?

Yes. Fits as a GitHub Actions service container, GitLab CI service, CircleCI service, or install-and-run background step. ~300ms startup means negligible CI overhead. See [Integration testing AWS in CI](/blog/integration-testing-aws-in-ci/).

### Does fakecloud require Docker?

To run fakecloud itself, no. Single binary, ~19 MB. Docker is required for services that run real containers (Lambda runtimes, RDS engines, ElastiCache engines) — same as LocalStack Pro requires.

### Is fakecloud written from scratch?

Yes. Written in Rust, no LocalStack code was used. LocalStack is written in Python; fakecloud is written in Rust and ships as a single static binary.

### How does fakecloud validate correctness?

Every commit runs 54,000+ conformance test variants generated from AWS's own Smithy models, plus end-to-end tests using the official AWS SDKs, plus the upstream `hashicorp/terraform-provider-aws` `TestAcc*` suites. If fakecloud behaves differently from real AWS, that's a bug — [open an issue](https://github.com/faiscadev/fakecloud/issues).

### Can AI coding agents (Claude Code, Cursor, Copilot) use fakecloud?

Yes. Paste-ready snippets for `CLAUDE.md`, `.cursor/rules`, and `.github/copilot-instructions.md` are in the README and [this blog post](/blog/aws-integration-tests-with-claude-code-cursor/). Agents can also fetch `https://fakecloud.dev/llms.txt` for structured API surface.

### Is fakecloud production-ready cloud infrastructure?

No. fakecloud is a testing and local-development tool. It is not production AWS and is not intended to be. For production, use real AWS.

### How does fakecloud compare to Moto?

Moto is a Python library that patches boto3 inside a test process. fakecloud is a real HTTP server on port 4566. Moto is fast and Python-only; fakecloud is language-agnostic and runs real Lambda / RDS / Redis. See [fakecloud vs Moto](/vs/moto/).

### How does fakecloud compare to MinIO?

MinIO is production-grade S3-compatible storage. fakecloud is an AWS testing emulator that happens to do S3 among 36 other services. Different tools for different jobs. See [fakecloud vs MinIO](/vs/minio/).

### How does fakecloud compare to DynamoDB Local?

DynamoDB Local is AWS's official DynamoDB emulator (DynamoDB only). fakecloud emulates DynamoDB plus 36 other AWS services, with Streams fired through real Lambda execution. See [fakecloud vs DynamoDB Local](/vs/dynamodb-local/).

### Where can I ask questions or report bugs?

GitHub issues: [github.com/faiscadev/fakecloud/issues](https://github.com/faiscadev/fakecloud/issues). The roadmap is demand-driven — service requests help prioritize what gets built next.

<script type="application/ld+json">
{
  "@context": "https://schema.org",
  "@type": "FAQPage",
  "mainEntity": [
    {"@type": "Question", "name": "What is fakecloud?", "acceptedAnswer": {"@type": "Answer", "text": "fakecloud is a free, open-source local AWS cloud emulator for integration testing and local development. It runs on a single port (4566), requires no account or auth token, and aims for 100% behavioral conformance with real AWS on every service it implements. AGPL-3.0 licensed."}},
    {"@type": "Question", "name": "Is fakecloud free?", "acceptedAnswer": {"@type": "Answer", "text": "Yes. AGPL-3.0, free for commercial use. Using fakecloud as a dev/test dependency has zero AGPL implications for your application."}},
    {"@type": "Question", "name": "Is fakecloud a LocalStack alternative?", "acceptedAnswer": {"@type": "Answer", "text": "Yes. LocalStack replaced its open-source Community Edition with a proprietary image in March 2026 that requires an account and auth token. fakecloud is a free, open-source replacement."}},
    {"@type": "Question", "name": "How many AWS services does fakecloud support?", "acceptedAnswer": {"@type": "Answer", "text": "41 services and 3,704 API operations. 124,255/124,255 generated Smithy conformance variants pass on every commit, true 100% across every implemented service, with more on the roadmap. The goal is 100% of AWS services, each at 100% behavioral conformance, with 100% of cross-service integrations."}},
    {"@type": "Question", "name": "Which AWS services are supported?", "acceptedAnswer": {"@type": "Answer", "text": "S3, SQS, SNS, DynamoDB, Lambda, IAM, STS, KMS, Secrets Manager, SSM, CloudWatch Logs, CloudWatch (Metrics & Alarms), CloudFormation, EventBridge, EventBridge Scheduler, SES (v2 + v1 inbound), Cognito User Pools, Cognito Identity, Kinesis, Firehose, Glue, Athena, RDS, ElastiCache, ECR, ECS, Elastic Load Balancing v2, CloudFront, Route 53, WAFv2, ACM, Application Auto Scaling, Step Functions, API Gateway v1 (REST), API Gateway v2 (HTTP), Bedrock, Bedrock Agent, Bedrock Agent Runtime, Bedrock Runtime."}},
    {"@type": "Question", "name": "Does fakecloud execute Lambda code for real?", "acceptedAnswer": {"@type": "Answer", "text": "Yes. fakecloud pulls real AWS Lambda runtime containers and executes your handler against them. 27 official runtimes including Node.js 16/18/20/22/24, Python 3.8 through 3.14, Java 11/17/21/25, .NET 6/8/10, Ruby 3.2, Go (go1.x), and custom provided/provided.al2/provided.al2023."}},
    {"@type": "Question", "name": "Does fakecloud run real databases for RDS?", "acceptedAnswer": {"@type": "Answer", "text": "Yes. RDS emulation pulls real PostgreSQL, MySQL, MariaDB, Oracle, SQL Server, and Db2 Docker images and runs them as the DB instance."}},
    {"@type": "Question", "name": "Does fakecloud run real Redis for ElastiCache?", "acceptedAnswer": {"@type": "Answer", "text": "Yes. ElastiCache runs real Redis, Valkey, and Memcached Docker images, so all Redis commands including streams, pub/sub, and Lua scripts work, and the full memcached text protocol works."}},
    {"@type": "Question", "name": "Do S3 notifications fire Lambda?", "acceptedAnswer": {"@type": "Answer", "text": "Yes, end-to-end. When an object is created in S3, any Lambda subscribed via bucket notification fires for real in a runtime container. Same for SNS and SQS subscriptions."}},
    {"@type": "Question", "name": "How do I install fakecloud?", "acceptedAnswer": {"@type": "Answer", "text": "One-line install script: curl -fsSL https://fakecloud.dev/install.sh | bash. Or Docker: docker run --rm -p 4566:4566 ghcr.io/faiscadev/fakecloud. Or cargo install fakecloud."}},
    {"@type": "Question", "name": "Does fakecloud work with Terraform?", "acceptedAnswer": {"@type": "Answer", "text": "Yes. CI runs the upstream hashicorp/terraform-provider-aws TestAcc suites against fakecloud on every commit."}},
    {"@type": "Question", "name": "Does fakecloud work with CDK?", "acceptedAnswer": {"@type": "Answer", "text": "Yes. Use the plain cdk binary with AWS_ENDPOINT_URL=http://localhost:4566, or the cdklocal wrapper."}},
    {"@type": "Question", "name": "Can I use fakecloud in CI?", "acceptedAnswer": {"@type": "Answer", "text": "Yes. Fits as a GitHub Actions service container, GitLab CI service, CircleCI service, or install-and-run background step. ~300ms startup means negligible CI overhead."}},
    {"@type": "Question", "name": "Does fakecloud require Docker?", "acceptedAnswer": {"@type": "Answer", "text": "To run fakecloud itself, no. Single binary, ~19 MB. Docker is required only for services that run real containers such as Lambda runtimes, RDS engines, and ElastiCache engines."}},
    {"@type": "Question", "name": "Is fakecloud written from scratch?", "acceptedAnswer": {"@type": "Answer", "text": "Yes. Written in Rust, no LocalStack code was used. LocalStack is written in Python; fakecloud is written in Rust and ships as a single static binary."}},
    {"@type": "Question", "name": "How does fakecloud validate correctness?", "acceptedAnswer": {"@type": "Answer", "text": "Every commit runs 54,000+ conformance test variants generated from AWS's own Smithy models, plus end-to-end tests using the official AWS SDKs, plus the upstream hashicorp/terraform-provider-aws TestAcc suites."}},
    {"@type": "Question", "name": "Can AI coding agents like Claude Code, Cursor, and GitHub Copilot use fakecloud?", "acceptedAnswer": {"@type": "Answer", "text": "Yes. Paste-ready snippets for CLAUDE.md, .cursor/rules, and .github/copilot-instructions.md are in the README. Agents can also fetch https://fakecloud.dev/llms.txt for a structured API surface."}},
    {"@type": "Question", "name": "Is fakecloud production-ready cloud infrastructure?", "acceptedAnswer": {"@type": "Answer", "text": "No. fakecloud is a testing and local-development tool. It is not production AWS and is not intended to be."}},
    {"@type": "Question", "name": "How does fakecloud compare to Moto?", "acceptedAnswer": {"@type": "Answer", "text": "Moto is a Python library that patches boto3 inside a test process. fakecloud is a real HTTP server on port 4566. Moto is fast and Python-only; fakecloud is language-agnostic and runs real Lambda, RDS, and Redis."}},
    {"@type": "Question", "name": "How does fakecloud compare to MinIO?", "acceptedAnswer": {"@type": "Answer", "text": "MinIO is production-grade S3-compatible storage. fakecloud is an AWS testing emulator that happens to do S3 among 36 other services."}},
    {"@type": "Question", "name": "How does fakecloud compare to DynamoDB Local?", "acceptedAnswer": {"@type": "Answer", "text": "DynamoDB Local is AWS's official DynamoDB emulator, DynamoDB only. fakecloud emulates DynamoDB plus 36 other AWS services, with Streams fired through real Lambda execution."}},
    {"@type": "Question", "name": "Where can I ask questions or report bugs?", "acceptedAnswer": {"@type": "Answer", "text": "GitHub issues: github.com/faiscadev/fakecloud/issues. The roadmap is demand-driven."}}
  ]
}
</script>
