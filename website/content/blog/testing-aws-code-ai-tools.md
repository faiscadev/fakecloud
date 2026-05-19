+++
title = "How to test AWS code written by AI tools"
date = 2026-04-14
description = "AI tools can write AWS code fast, but they hallucinate APIs and create tests that pass while asserting wrong behavior. Here's how to test AI-generated AWS code properly: real integrations, external verification, and guardrails the AI can't game."

[extra]
author = "Lucas Vieira"
+++

AI tools are absurdly good at writing AWS code. They understand IAM policies, VPC configurations, and service integrations in a way that feels magical. Until they don't.

The failure mode everyone hits is the same: the AI writes code, writes tests, the tests pass, you ship it—and then AWS returns something completely different in production.

Here's how to test AI-generated AWS code properly.

## The debugging revelation

I started using ChatGPT to write code the day it launched. The first "oh shit, this actually works" moment wasn't code generation—it was debugging.

AWS is full of rough edges. Permissions that fail silently. Networking that breaks in non-obvious ways. Security groups that look right but aren't. When you hit one of these, you're stuck reading docs, checking IAM, comparing VPC settings.

AI is absurdly fast at this. Paste an error, describe what you tried, get three hypotheses back. "Check if the security group allows egress on 443." "Verify the IAM role has `sts:AssumeRole` for that principal." One of them is usually right.

That's when I realized AI understands AWS in a way that's genuinely useful. Not because it memorized documentation, but because it's seen every way these things fail.

## The hallucination problem

Then you hit the limits.

When debugging gets hard—when nothing makes sense—AI starts making things up. It suggests an AWS feature that doesn't exist. A configuration parameter that was never real. An API method that would solve your problem perfectly, except it's not in the SDK.

This is the failure mode everyone talks about: hallucination. It's real. When the AI doesn't know, it confidently invents an answer.

The standard response is "review every line the AI writes."

That advice doesn't scale. And it doesn't work.

## Why manual review fails

The problem with "review every line" is that it assumes the human is the source of truth. But humans miss things. Humans get tired. Humans skim code that looks reasonable.

More importantly: if the AI writes both the code and the tests, **manual review doesn't catch the right bugs.**

Here's the trap:

1. AI generates code
2. Developer reviews it (looks fine)
3. AI writes tests
4. Tests pass
5. Ship it

Except the tests can be wrong in the same way the code is wrong. The tests assert that the bug exists successfully. Everything is internally consistent, but none of it matches reality.

This is especially bad with mocks. The AI will happily mock AWS APIs in a way that matches the buggy code. The tests pass. Your code goes to production. AWS returns something completely different. Good luck debugging that at 2am.

Manual review didn't save you because the bug wasn't obvious. The tests didn't save you because they tested the wrong behavior.

## What actually works: guardrails that can't be gamed

Here's what worked for fakecloud:

**1. Real integration tests, not mocks.**

Every feature ships with E2E tests using the actual AWS SDK. Not unit tests with mocked clients. Actual `aws-sdk-rust` calls against fakecloud.

When the test uses the real SDK and the code is wrong, the test fails. The AI iterates until it's right.

This matters most for cross-service wiring—the stuff that's easy to get subtly wrong and almost impossible to catch with mocks. fakecloud actually executes the integrations: an EventBridge rule really triggers the Step Functions state machine, an SES inbound rule really invokes the Lambda, an S3 event really lands in SQS. If the AI wires it up wrong, the test fails for the same reason it would fail in production.

**2. Conformance testing the AI didn't write.**

We auto-generate 54,000+ test variants from AWS Smithy models. Every operation, every parameter, every boundary condition, every error case.

The AI can't write code that games these tests because it didn't write the tests. They're generated from AWS's service definitions.

**3. Side-by-side testing against real AWS.**

The same test suite runs against both fakecloud and actual AWS. When behavior diverges, it's a bug.

This is the key insight: **the guardrails are automated and external to the code being tested.**

The AI can't game them. They catch everything. And when they're set up right, they catch it *before* you see the code.

## What this means for AWS testing

If you're building on AWS and using AI to write code, here's what matters:

**Don't mock the AWS SDK in your tests.** Use fakecloud, LocalStack, Testcontainers, or real AWS with cleanup afterward. But don't mock. Mocks let AI-generated tests pass while asserting the wrong behavior.

**Build guardrails the AI can't game.** If the AI writes both the code and the tests, make sure tests are anchored to something external—real SDK behavior, schemas generated from service definitions, or validation against actual AWS.

**Verify architecture, not syntax.** Your job isn't to review every line. It's to make sure the AI is building the right thing in the right way. If the structure is sound and the tests pass, the code is probably fine.

**Let the AI iterate.** When tests fail, let the AI fix them. It's faster than you at reading error messages and adjusting code. Your job is to make sure it's converging toward the right solution.

## The test suite is the guardrail

I built fakecloud almost entirely with Claude. 22 AWS services, 1,150+ operations, 100% conformance to AWS behavior.

fakecloud works because the tests verify everything:

- 54,000+ conformance tests generated from AWS Smithy models
- E2E tests using real AWS SDKs
- CI that runs everything on every commit
- Side-by-side testing against actual AWS

These guardrails are external to the code being tested. The AI can't game them. They catch bugs before I see the code.

If your tests are good—real integration tests, not mocks, not stubs—then AI-generated code is as reliable as human-written code. Maybe more reliable, because the AI doesn't get bored and skip edge cases.

If your tests are bad—mocks everywhere, written by the same AI that wrote the code—then you're in trouble whether a human or an AI wrote the code.

The tooling matters. The tests matter. The mocks-versus-real-APIs decision matters.

---

fakecloud exists because AI tools need a way to verify AWS code against real behavior, not just against tests they wrote themselves.

- **Repo:** [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- **Site:** [fakecloud.dev](https://fakecloud.dev)
- **Install:** `curl -fsSL https://fakecloud.dev/install.sh | bash`

If you're using Claude Code, Cursor, or any other AI coding tool to write AWS applications, you need something that behaves like AWS to test against. Not mocks. Not stubs. Real API behavior.

That's what fakecloud is for.

And if you find a case where fakecloud behaves differently from AWS—that's a bug. [Open an issue](https://github.com/faiscadev/fakecloud/issues). Because the tests should have caught it.
