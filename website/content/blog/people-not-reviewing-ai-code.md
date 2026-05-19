+++
title = "People are not reviewing AI-generated code"
date = 2026-04-15
description = "I'd bet most developers are already shipping AI-generated code without properly reviewing it. Not because they have guardrails, but because they're busy and the code looks fine. Here's what I think we should do about it."

[extra]
author = "Lucas Vieira"
+++

I'd bet good money that most developers using AI tools are already shipping code without properly reviewing every line.

Not because they have some sophisticated testing setup. Because it historically just works. A developer reviews the first ten things the AI writes. They're fine. Reviews the next ten. Also fine. At some point the developer starts skimming. Then stops reading altogether. The review step keeps proving itself to be a waste of time, so it gets skipped. Human nature.

## What validation looks like with guardrails

I built fakecloud almost entirely with Claude. 22 AWS services, 1,650+ operations, 100% conformance to AWS behavior.

Here's my workflow:

1. I define what to build. Architecture, scope, acceptance criteria.
2. AI writes code and tests.
3. Tests run in CI.
4. I look at the results.

Step 4 is where it gets weird.

The tests pass. I look at the implementation. It's correct. I merge.

This happens over and over. I'm doing the validation—I'm looking, I'm checking—but I'm not finding anything wrong. The guardrails already caught it.

**The validation step has become ceremonial.** You still do it because if something's wrong it can be a disaster. But in practice, if you've built the right guardrails, validation finds nothing. The automated systems already caught everything.

## What's actually happening

I still validate before merging because catastrophic failure is real. If something gets through that shouldn't, it's a disaster. So I check. But I'm checking as a safety ritual, not because I expect to find problems.

The tests already found the problems. CI already caught them. By the time I look, there's nothing left to catch.

And here's my gut feeling: **I think most people are already skipping this step.**

I have no proof. But think about it. A developer asks Claude or Copilot to write a function. It looks right. They review it anyway because that's what responsible developers do. It's fine. Next time, same thing. Fine again. After the twentieth time, they start skimming. After the fiftieth, they stop reading altogether.

Why would anyone keep reviewing something that's never wrong?

But I'd bet there are plenty of developers shipping AI-generated code without proper test infrastructure. Without CI catching issues. Without proper monitoring. Just the AI, some tests the AI wrote, and hope.

**I think the industry already moved past "review every line." We're just not talking about it.**

## You can't fix this with process

You can write "review all AI-generated code" in your team's guidelines. You can make it a policy. You can add it to the PR checklist.

But you can't verify that someone actually did it. You can't tell the difference between "I carefully reviewed this" and "I skimmed it for 30 seconds and it looked fine." There's no way to enforce it. And when the AI keeps being right, the incentive to actually do it drops to zero.

So instead of pretending you can control this, **build systems that don't depend on it.**

That's what I did with fakecloud. The code is real. The conformance is real. Most of it was written by Claude, with me defining architecture and making sure the guardrails worked.

And when I validate the code, I find nothing wrong. Not because I'm a great reviewer. Because the guardrails already found everything.

## What good guardrails look like

Most AI-generated code ships with AI-generated tests. The problem is that the tests can be wrong in the same way the code is wrong. Everything is internally consistent, but none of it matches reality. A human reviewer might catch this. Or might not. You can't count on it.

What you can count on:

- **Real integration tests with actual SDKs** — not mocks that mirror the buggy code
- **Conformance tests generated from schemas** — tests the AI didn't write and can't game
- **Side-by-side testing against real systems** — when behavior diverges, it's a bug

The difference isn't whether someone reviewed the code. It's whether you have automated systems that verify correctness independent of the code being tested.

When those systems pass, the code is right regardless of whether anyone read it. When they fail, the AI fixes it until they pass.

Here's a concrete example. If you're writing code that calls Bedrock — the Claude API on AWS — the hard parts are retry logic, fallback models, and circuit breakers. Testing that code against real Bedrock is expensive, non-deterministic, and rate-limited. Mocking it is meaningless because the mocks don't catch the bugs. So you either ship untested error handling or you write unrunnable tests. That's exactly the kind of gap where AI-generated code goes wrong silently. I wrote a whole [guide on how to test Bedrock code locally against fakecloud](/docs/guides/testing-bedrock/) — deterministic responses, injectable faults, call history assertions. That's what "guardrails the AI can't game" looks like in practice.

## The test suite is the guardrail

fakecloud works because the tests verify everything. Not me. Not code review. The tests.

- 54,000+ conformance tests generated from AWS Smithy models
- E2E tests using real AWS SDKs
- CI that runs everything on every commit
- Side-by-side testing against actual AWS

These guardrails are external to the code being tested. The AI can't game them. They catch bugs before I see the code.

So when I validate, I find nothing. Because there's nothing left to find.

If I'm right that people are already shipping AI code without reviewing it, this is the gap. Most codebases don't have external verification. Just AI-generated code, AI-generated tests, and no way to know if they match reality.

The fix isn't telling people to review harder. It's building systems that catch the bugs whether or not anyone reads the code.

## What to do about it

If you're using AI to write code, here's what matters:

**You can't control whether people actually review code.** You can tell them to. You can put it in the process. But you can't get inside their head and verify they actually read every line. So build systems that don't depend on them doing it.

**Build external guardrails.** Tests that the AI didn't write. Conformance checks generated from schemas. Integration tests with real dependencies, not mocks.

**Automate everything.** Don't rely on humans to catch bugs. Humans are slow, inconsistent, lazy, and bad at checking edge cases. Automated systems are fast, consistent, and check everything.

**Accept what's probably already happening.** I think most developers are already shipping AI-generated code without thorough manual review. If that's true, the question isn't whether to do it. It's whether you're doing it safely.

---

For AWS code specifically, I wrote about [how to test AI-generated AWS code properly](/blog/testing-aws-code-ai-tools/). The short version: don't mock AWS APIs. Use something that behaves like real AWS.

That's why fakecloud exists:

- **Repo:** [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- **Site:** [fakecloud.dev](https://fakecloud.dev)
- **Install:** `curl -fsSL https://fakecloud.dev/install.sh | bash`

If you're writing AWS code with Claude, Cursor, or any AI tool, you need automated guardrails that verify correctness without you in the loop. Because if I'm right, people are already skipping manual review—and the only thing standing between them and production bugs is the quality of their automated tests.

Build the guardrails. Then let the AI work.

And if you find a case where fakecloud behaves differently from AWS, [open an issue](https://github.com/faiscadev/fakecloud/issues). Because that means the guardrails missed something. And we need to know.
