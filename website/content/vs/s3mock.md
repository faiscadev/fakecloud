+++
title = "fakecloud vs S3Mock"
description = "How fakecloud compares to adobe/S3Mock. Both local S3 emulators; fakecloud adds cross-service wiring and 22 other AWS services."
template = "page.html"
+++

[adobe/S3Mock](https://github.com/adobe/S3Mock) is a lightweight S3-only mock for integration tests, written in Java and distributed as a JAR or Docker image. Simple, focused, well-maintained.

fakecloud does S3 (107 operations) plus 22 other AWS services end-to-end.

## When to pick S3Mock

- Pure S3 tests in JVM-heavy stacks (Java, Kotlin, Scala).
- You want a minimal footprint and simple setup.
- Your app talks to S3 and nothing else from AWS.
- You prefer Adobe's testing ecosystem.

## When to pick fakecloud

- Your tests exercise **S3 + other AWS services** (S3 -> Lambda notifications, S3 -> SNS topic events, SES inbound -> S3).
- Non-JVM language (Go, Python, Node, Rust, PHP) — fakecloud's HTTP server works with any SDK.
- You want real Lambda execution on S3 notifications.
- You want IAM + bucket policies enforced (`--iam strict`), not just stubbed.

## Feature-level comparison

| | fakecloud | S3Mock |
|---|---|---|
| S3 operations | 107 | Core S3 surface |
| Versioning | Yes | Yes |
| Multipart uploads | Yes | Yes |
| Lifecycle | Yes | Partial |
| S3 notifications fire subscribers (real) | **Yes** (SNS/SQS/Lambda) | **No** (no other services) |
| Bucket policy enforcement | Yes (opt-in `--iam strict`) | No |
| Non-JVM SDKs | Any | Any (S3 SDK only) |
| Other AWS services | 22 more | None |
| Startup | ~300ms | ~2s (JVM) |
| Runtime | Rust binary (~19 MB) | JAR / Docker |

## Using both

Not typical — both are S3 emulators. Pick one.

## Links

- [fakecloud GitHub](https://github.com/faiscadev/fakecloud)
- [adobe/S3Mock GitHub](https://github.com/adobe/S3Mock)
- [Local S3 for integration tests](/local-s3-for-tests/)
