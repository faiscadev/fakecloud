+++
title = "What fakecloud is (and isn't)"
description = "When to use fakecloud, when not to, and what problem it solves."
weight = 3
+++

## What fakecloud is

A free, open-source local AWS emulator for integration testing and local development. For every service it implements, the goal is 100% behavioral parity with real AWS — measured by a schema-driven [conformance harness](/docs/about/conformance/) that runs 100,210 generated test variants against official AWS Smithy models on every commit, all of which pass.

The point is to let you run your application code against something that behaves like AWS, without burning an AWS account or hitting rate limits. Your tests exercise real SDK code paths end-to-end. Your CI pipeline runs fast and free.

## What fakecloud isn't

**Not a production cloud.** It's not designed for scale, durability, multi-tenancy, or production workloads. State is in-memory by default. Persistence is limited to a subset of services. It's single-binary and single-process. Don't put it in front of real users.

**Not a drop-in for all of AWS.** It implements 40 services — the ones most teams actually test against. If you need EKS, Redshift, or SageMaker, fakecloud isn't the right tool (yet).

**Not a mock.** Mocks return predefined values regardless of whether you call them correctly. fakecloud speaks real AWS wire protocols, validates real SigV4 headers (without signature checking), and returns AWS-shaped responses that the real SDK parses and deserializes. If your code assembles the request wrong, fakecloud fails the same way real AWS would.

## When to use it

- **Integration tests** that exercise your AWS code end-to-end.
- **Local development** when you don't want to pay for dev AWS accounts or deal with IAM setup.
- **CI pipelines** that shouldn't touch real AWS for cost, reliability, or data-leak reasons.
- **Offline work** on planes, trains, bad wifi, air-gapped environments.
- **Testing error paths** you can't easily trigger in real AWS (throttling, timeouts, specific error codes).

## When not to use it

- **Production.** Never. Use real AWS.
- **Load testing** that depends on AWS's real performance characteristics.
- **Testing AWS-side network behavior** like VPC endpoints, cross-region, or specific AZ failures.
- **Billing verification.** fakecloud doesn't charge. Obviously.

## The bet

fakecloud exists because testing AWS code well is harder than it should be. Mocks lie. Staging accounts cost money and leak state between tests. Real AWS is slow, rate-limited, and unsuitable for fast CI feedback. The bet is that a correctness-first local emulator — one that matches AWS's actual wire protocol on every documented operation — is a better tradeoff for most testing workflows than any of the alternatives.

That's why the project prioritizes conformance and cross-service wiring over breadth. 40 services at true 100% (100,210/100,210 Smithy variants pass) is more useful than 100 services at 50%.
