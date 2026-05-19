+++
title = "SDK reference"
description = "First-party fakecloud SDKs for TypeScript, Python, Go, PHP, Java, and Rust."
sort_by = "weight"
weight = 6
template = "docs.html"
page_template = "docs-page.html"
+++

fakecloud ships first-party SDKs in six languages for test assertions and simulation control. Each SDK wraps the `/_fakecloud/*` introspection and configuration endpoints into ergonomic helpers that fit the language's testing idioms.

These SDKs are **not** the AWS SDK. Your application code still talks to fakecloud through the normal AWS SDK (boto3, aws-sdk-js, aws-sdk-rust, etc.) — the fakecloud SDK is what your tests use to assert on what happened.

## Which SDK?

| Language   | Install                                         | Page |
| ---------- | ----------------------------------------------- | ---- |
| TypeScript | `npm install fakecloud`                         | [TypeScript SDK](/docs/sdks/typescript/) |
| Python     | `pip install fakecloud`                         | [Python SDK](/docs/sdks/python/) |
| Go         | `go get github.com/faiscadev/fakecloud/sdks/go` | [Go SDK](/docs/sdks/go/) |
| PHP        | `composer require fakecloud/fakecloud`          | [PHP SDK](/docs/sdks/php/) |
| Java       | `dev.fakecloud:fakecloud:0.15.0`                 | [Java SDK](/docs/sdks/java/) |
| Rust       | `cargo add fakecloud-sdk`                       | [Rust SDK](/docs/sdks/rust/) |

## Common surface

All six SDKs cover the same core surface:

- **Reset:** `reset()` and `resetService(service)` to clear state between tests
- **Health:** verify fakecloud is reachable
- **Per-service introspection:** get recorded messages, emails, invocations, events
- **Simulation and processor ticks:** drive time-dependent behavior on demand (TTL expiration, secret rotation, S3 lifecycle)
- **Bedrock test harness:** response configuration, fault injection, call history
- **AWS metadata endpoints:** ECS task metadata (v3, v4), task credentials
- **Admin state mutation:** flip CloudFront distribution status, Route53 health-check status, SSM command status, ACM certificate status, SES sandbox/mail-from status
- **DNSSEC:** Route53 zone DNSSEC material + RRSIG signing
- **Code/layer downloads:** Lambda function code, Lambda layer content (raw bytes)
- **Container/cache introspection:** ECS clusters/tasks/events, ElastiCache clusters/replication-groups/serverless-caches/ACLs, ECR repositories/images/pull-through rules
- **Logs introspection:** anomaly injection, delivery config, field indexes
- **API Gateway v2:** recorded HTTP requests, WebSocket connections, mTLS info, ws:// URL builder
- **RDS bridge:** aws_lambda invoke, aws_s3 import/export

The method names differ across languages to match each language's idiom (camelCase for TS/JS, Java, and PHP, snake_case for Python, PascalCase for Go, snake_case for Rust), but the behavior is the same.
