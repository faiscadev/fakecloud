+++
title = "Free, Open-Source LocalStack Alternative for AWS Testing"
date = 2026-04-11
description = "LocalStack went proprietary in March 2026. fakecloud is a free, open-source AWS emulator for integration testing — 20 services, 100% conformance, no auth required."

[extra]
author = "Lucas Vieira"
+++

In March 2026, LocalStack replaced its Community Edition with a proprietary image that requires an account and auth token. For teams that relied on LocalStack for local AWS testing, this broke CI pipelines and forced a choice: pay for a Pro license or find an alternative.

fakecloud is that alternative.

## What is fakecloud?

fakecloud is a free, open-source local AWS emulator for integration testing and local development. It runs on a single port (4566), requires no account or auth token, and aims to faithfully replicate AWS behavior.

Currently supports 20 AWS services with 1,000+ API operations:

- **S3** — objects, multipart uploads, versioning, lifecycle, notifications
- **Lambda** — real code execution via Docker, 13 runtimes, event source mappings
- **SQS** — FIFO queues, dead-letter queues, long polling
- **SNS** — fan-out to SQS/Lambda/HTTP, filter policies
- **DynamoDB** — tables, transactions, PartiQL, streams, global tables
- **EventBridge** — pattern matching, scheduled rules, API destinations
- **RDS** — PostgreSQL/MySQL/MariaDB via Docker, snapshots, read replicas
- **ElastiCache** — Redis/Valkey via Docker, replication groups, failover
- **SES** — v2 API (97 operations), inbound email pipeline, event destinations
- **Cognito User Pools** — authentication, MFA, user management
- **Kinesis** — data streams, shard iterators, stream retention
- **Step Functions** — ASL interpreter, Lambda/SQS/SNS/DynamoDB integrations
- **CloudWatch Logs** — groups, streams, filtering, subscriptions
- **API Gateway v2** — HTTP APIs with Lambda integration
- **IAM, STS, SSM, Secrets Manager, KMS, CloudFormation**

[Full service list with operation counts](https://github.com/faiscadev/fakecloud#supported-services)

## LocalStack vs fakecloud

In March 2026, LocalStack moved many previously-free services behind a paywall (Cognito, SES v2, RDS, ElastiCache, ECR, ECS). The Community Edition now requires authentication and has limited service availability.

| Feature                | fakecloud                                          | LocalStack Community                                                           |
| ---------------------- | -------------------------------------------------- | ------------------------------------------------------------------------------ |
| License                | AGPL-3.0 (free, open-source)                       | Proprietary                                                                    |
| Auth required          | No                                                 | Yes (account + token)                                                          |
| Commercial use         | Free                                               | Paid plans only                                                                |
| Docker required        | No (standalone binary)                             | Yes                                                                            |
| Startup time           | ~500ms                                             | ~3s                                                                            |
| Idle memory            | ~10 MiB                                            | ~150 MiB                                                                       |
| Install size           | ~19 MB binary                                      | ~1 GB Docker image                                                             |
| AWS services           | 20                                                 | 30+                                                                            |
| Test assertion SDKs    | TypeScript, Python, Go, Rust                       | Python, Java                                                                   |
| Cognito User Pools     | 80 operations                                      | [Paid only](https://docs.localstack.cloud/references/licensing/)               |
| SES v2                 | 97 operations                                      | [Paid only](https://docs.localstack.cloud/references/licensing/)               |
| SES inbound email      | Real receipt rule action execution                 | [Stored but never executed](https://docs.localstack.cloud/user-guide/aws/ses/) |
| RDS                    | 22 operations, PostgreSQL/MySQL/MariaDB via Docker | [Paid only](https://docs.localstack.cloud/references/licensing/)               |
| ElastiCache            | 44 operations, Redis/Valkey via Docker             | [Paid only](https://docs.localstack.cloud/references/licensing/)               |

LocalStack has more services overall, but many are behind a paywall. fakecloud focuses on implementing fewer services completely, with 100% conformance to AWS behavior.

## Why we built fakecloud

**LocalStack went proprietary.** In March 2026, `localstack:latest` started requiring authentication. CI pipelines broke. The message was clear: LocalStack Community Edition was no longer community.

**Correctness matters for testing.** If you're building on AWS, your integration tests should talk to something that behaves like AWS — not mocks that return fake success responses. When fakecloud behaves differently from AWS, that's a bug we fix.

**Real infrastructure, not stubs.** fakecloud runs actual software via Docker: real Lambda runtimes (13 languages), real databases (PostgreSQL, MySQL, MariaDB), real Redis/Valkey instances. When your Lambda function reads from RDS, it's talking to an actual Postgres instance, not a mock.

**Testing tools should be free.** fakecloud is a development dependency, not production infrastructure. It should be free and open-source, the same way test frameworks like Jest, Mocha, and pytest are free.

## How we verify correctness

fakecloud doesn't claim to be "AWS-compatible" without backing it up:

- **34,000+ conformance test variants** validated against official AWS Smithy models, covering every operation parameter, boundary condition, and error case
- **280+ end-to-end tests** using the official AWS SDKs (aws-sdk-rust, boto3, aws-sdk-js)
- **Automated testing against real AWS** — our test suite runs against both fakecloud and actual AWS to verify behavioral parity

When tests fail, we fix the behavior. We don't stub responses or return fake success codes.

## Getting started

Install and run fakecloud:

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

Or via Docker:

```sh
docker run --rm -p 4566:4566 ghcr.io/faiscadev/fakecloud
```

Point your AWS SDK at `http://localhost:4566` with dummy credentials:

```sh
aws --endpoint-url http://localhost:4566 sqs create-queue --queue-name test-queue
```

## Test assertions with fakecloud SDKs

fakecloud exposes introspection endpoints that AWS doesn't provide, so you can assert on side effects in your tests:

```typescript
import { FakeCloud } from "fakecloud";

const fc = new FakeCloud("http://localhost:4566");

// After your app sends an email via SES
const { emails } = await fc.ses.getEmails();
expect(emails).toHaveLength(1);
expect(emails[0].destination.toAddresses).toContain("user@example.com");

// After your Lambda processes an event
const { invocations } = await fc.lambda.getInvocations();
expect(invocations[0].statusCode).toBe(200);
```

SDKs available for TypeScript, Python, Go, and Rust.

## What's not implemented (yet)

fakecloud is younger than LocalStack and has fewer services. If you need EC2, ECS, or other services not listed above, LocalStack may still be your best option (if you're willing to pay for Pro).

We're prioritizing services based on real-world demand. [Open an issue](https://github.com/faiscadev/fakecloud/issues) if there's a service you need.

## Roadmap

Coming next:
- **Bedrock** — AI/ML testing (high demand across the ecosystem)
- **Lambda Containers** — practical container support with ECR
- **CloudFront** — CDN configuration testing
- **CloudWatch Metrics** — complete monitoring story alongside Logs
- **Athena** — SQL analytics on S3
- More RDS engines (Oracle, SQL Server)

[Full roadmap](https://github.com/faiscadev/fakecloud/blob/main/ROADMAP.md)

## Other alternatives to LocalStack

**Moto** — Python-based AWS mocking library. More services than fakecloud (100+), but designed for unit tests with mocks rather than integration tests. Doesn't support cross-service integrations or real Lambda execution.

**LocalStack Pro (paid)** — If you need the full service catalog and don't mind paying, LocalStack Pro is mature and feature-complete.

**Real AWS** — For the highest fidelity, test against real AWS. fakecloud is for fast local iteration; real AWS is for pre-production validation.

## Try fakecloud

- **Repo:** [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- **Site:** [fakecloud.dev](https://fakecloud.dev)
- **Install:** `curl -fsSL https://fakecloud.dev/install.sh | bash`

If you find a case where fakecloud behaves differently from AWS, [open an issue](https://github.com/faiscadev/fakecloud/issues) — that's a bug, and we'll fix it.
