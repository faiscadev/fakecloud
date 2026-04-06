+++
title = "Why I'm building a free, open-source AWS emulator"
date = 2026-04-06
description = "LocalStack went proprietary. Our builds broke. So I built a free, open-source replacement in Rust — 13 AWS services, 983 API operations, 98% conformance."

[extra]
author = "Lucas Vieira"
+++

A few days ago, our CI started failing. The culprit: `localstack:latest` now requires an account and an auth token. Broken builds.

I'd seen LocalStack creeping toward proprietary for a while, but this was the moment it actually hit. A colleague noticed it at the same time as me, but I was already working on a fix. I patched the build, but the question stuck: how long can we keep using the old version until we start having real problems?

## I don't trust mocks

I should back up. I care about tests a lot. Enough to become a [maintainer of Chai.js](https://github.com/chaijs/chai). Enough that when I build something, the test infrastructure comes first.

But I have a specific opinion about *what kind* of tests matter. I hate mocks. Not the concept — the way most codebases use them. You end up with tests that don't test anything real. You're asserting that you call functions in the right order. You're verifying the plumbing, not the behavior.

The worst part is when mocked tests give you false confidence. I've seen tests that assert you do the wrong thing successfully — the mock doesn't care, it just returns what you told it to. Your tests pass. Your code has a bug. You ship it.

If you're building on AWS, you need integration tests that actually talk to something that behaves like AWS. Not a mock. Not a stub that returns 200 to everything. Something that implements the real behavior: if you call API A then API B, and AWS would produce side effect C, then your test environment should produce side effect C too.

That's what LocalStack used to give us. And that's what I wanted to keep — but free and open source.

## So I built it

I started fakecloud on April 4th, 2026. Three days later: 12 AWS services, 301 commits, 844 tests.

That pace was possible because of two things. First, I used LLMs heavily throughout — not to generate code I don't understand, but as a force multiplier with strong guardrails. Every feature ships with E2E tests. The tests are the guardrails. If the LLM generates something that doesn't match real AWS behavior, the tests catch it.

Second, Rust. I chose Rust because I love static-typed compiled languages, and Rust's type system is genuinely amazing. You get the performance of no garbage collector without having to manually manage memory. It means fakecloud starts in under 100ms and runs as a single binary — no Docker required, no runtime dependencies.

## Correctness is the whole point

fakecloud doesn't try to be a scalable production cloud. It's not that. It's a testing tool. And the one thing a testing tool needs to get right is correctness.

What does that mean in practice? If you call `CreateQueue`, then `SendMessage`, then `ReceiveMessage` on real AWS and get back your message with specific attributes — fakecloud should do exactly the same thing. If it doesn't, that's a fakecloud bug.

We currently verify this with 280+ E2E tests that use the official `aws-sdk-rust` crate and 34,000+ auto-generated conformance test variants validated against official AWS Smithy models — covering all 983 API operations across 13 services at 98% conformance. The plan for the near future: set up a real AWS account and run our test suite against both fakecloud and real AWS side by side, so we can verify behavioral parity automatically.

## What's here today

13 services, 983 API operations, all open source, all free:

- **S3** (107 actions) — objects, multipart uploads, versioning, lifecycle, notifications, encryption
- **SQS** (23 actions) — FIFO queues, dead-letter queues, long polling, batch operations
- **SNS** (34 actions) — fan-out to SQS, HTTP delivery, filter policies, platform applications
- **EventBridge** (57 actions) — pattern matching, scheduled rules, connections, API destinations, endpoints
- **IAM** (128 actions) — users, roles, policies, groups, instance profiles, OIDC/SAML providers
- **STS** (11 actions) — assume role, session tokens, federation, credential expiration
- **SSM** (146 actions) — parameters, documents, commands, maintenance windows, associations, automation, sessions
- **DynamoDB** (57 actions) — tables, items, transactions, PartiQL, backups, global tables, streaming
- **Lambda** (10 actions) — function CRUD, invoke, event source mappings
- **Secrets Manager** (23 actions) — versioning, soft delete, rotation, replication, resource policies
- **CloudWatch Logs** (113 actions) — groups, streams, filtering, deliveries, transformers, anomaly detection
- **KMS** (53 actions) — encryption, aliases, key management, grants, custom key stores
- **CloudFormation** (8 actions) — template parsing, resource provisioning

Services talk to each other: S3 notifications deliver to SNS/SQS. SNS fans out to SQS. EventBridge rules fire on schedule and deliver to targets. This is the kind of cross-service behavior that matters in integration tests and that most emulators get wrong or skip entirely.

## Try it

```sh
curl -fsSL https://raw.githubusercontent.com/faiscadev/fakecloud/main/install.sh | bash
fakecloud
```

Then point any AWS SDK at `http://localhost:4566` with dummy credentials. That's it.

The code is at [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud). It's AGPL-3.0 — free and open source, including for commercial use.

If you need a local AWS emulator for your integration tests, give it a try. And if something doesn't behave like real AWS — [open an issue](https://github.com/faiscadev/fakecloud/issues). That's a bug, and we'll fix it.
