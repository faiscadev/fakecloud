+++
title = "How to test Bedrock code locally, for free, deterministically"
date = 2026-04-13
description = "Testing code that calls Bedrock is painful: every run burns tokens, hits rate limits, and returns different text. Here's how to test Bedrock-calling code locally against fakecloud — deterministic responses, configurable per prompt, with fault injection for retry logic."
aliases = [
    "/blog/local-aws-bedrock-prototyping-genai-without-cloud-costs/",
    "/blog/local-generative-ai-implementing-bedrock-with-fakecloud/",
]

[extra]
author = "Lucas Vieira"
+++

If you're writing code that calls Bedrock, you already know the problem. Every test run burns tokens. Every CI build hits rate limits. Every assertion on model output breaks because the model returned something slightly different this time. The only way to make it stop is to mock, and then your tests don't test anything real.

This is the most frustrating corner of AWS to test against, and until a few days ago there was no good answer. LocalStack doesn't emulate Bedrock at any tier — not free, not paid. If you wanted a local Bedrock, you built it yourself.

So I built one.

## The problem with real Bedrock in tests

Four things break, in order of how much they hurt:

1. **Cost.** Every test run that calls a model is a line item on your AWS bill. A CI job that runs on every PR and exercises a Bedrock code path a dozen times becomes real money fast. Teams end up building elaborate VCR-cassette systems to record-once-and-replay, which works until the request shape changes and everything silently goes stale.

2. **Rate limits.** Bedrock has per-account quotas. A noisy test suite gets throttled. Flaky CI. Engineers angry. You start skipping tests in CI to stay under the limit, which defeats the point of having them.

3. **Non-determinism.** Real models return different text every run, even at temperature 0. Snapshot tests are impossible. Assertions on specific strings are impossible. Everyone ends up asserting that *some* output was produced, which is the weakest possible test — it would pass even if the model returned the string "cabbage."

4. **No offline dev.** On a plane, on a train, on bad hotel wifi, in a datacenter without egress. You can't develop Bedrock code at all without a live connection to AWS.

The standard response is "mock the SDK." The problem with mocking the SDK is that it's AI-generated code calling an AI API, and if you let the AI generate both the code and the mocks, your tests will happily assert the wrong behavior and pass. Your code never talks to anything that looks like Bedrock — it talks to a fake object that matches the SDK's interface. If your code assembles the request wrong, the mock doesn't care.

## What fakecloud gives you

fakecloud implements 111 Bedrock operations — the full runtime (InvokeModel, Converse, both streaming variants), plus guardrails, customization jobs, model imports, the whole control plane. You point any AWS SDK at `http://localhost:4566` with dummy credentials and it works.

But the interesting part isn't the operation count. The interesting part is that you can make the "model" return exactly what you want, triggered by exactly what you want, and then assert on exactly what your code sent.

**Configurable responses per prompt.** Set a rule like "when the prompt contains 'summarize', return this JSON; when it contains 'classify', return that one." Your code's branching logic gets tested against deterministic, fixture-like responses you control.

**Fault injection.** Tell fakecloud to throw `ThrottlingException` on the next call. Your retry logic gets exercised. Your fallback-model logic gets exercised. Your circuit breaker gets exercised. All the hard parts of production Bedrock code, which currently go untested on every project I've seen.

**Call history.** After your code runs, query fakecloud for every InvokeModel and Converse call it made — prompts, outputs, timestamps, error field. Assert on what your code *sent*, not just on what it received.

Together, that's a complete test loop.

## A real example

Here's a spam classifier that calls Claude via Bedrock. I want to test both the happy path (spam vs. ham branching) and the retry path (throttling recovery). Using the [TypeScript SDK](https://github.com/faiscadev/fakecloud):

```typescript
import { FakeCloud } from "fakecloud";
import {
  BedrockRuntimeClient,
  InvokeModelCommand,
} from "@aws-sdk/client-bedrock-runtime";

const fc = new FakeCloud();
const modelId = "anthropic.claude-3-haiku-20240307-v1:0";

beforeEach(() => fc.reset());

test("classifier branches on spam vs ham", async () => {
  await fc.bedrock.setResponseRules(modelId, [
    { promptContains: "buy now", response: '{"label":"spam"}' },
    { promptContains: null,      response: '{"label":"ham"}'  }, // catch-all
  ]);

  await classify("hello friend");
  await classify("buy now cheap pills");

  const { invocations } = await fc.bedrock.getInvocations();
  expect(invocations).toHaveLength(2);
  expect(invocations[0].output).toContain("ham");
  expect(invocations[1].output).toContain("spam");
});

test("retries on ThrottlingException", async () => {
  await fc.bedrock.queueFault({
    errorType: "ThrottlingException",
    message: "Rate exceeded",
    httpStatus: 429,
    count: 1, // only the first call faults; the retry succeeds
  });

  await classify("hello");

  const { invocations } = await fc.bedrock.getInvocations();
  expect(invocations).toHaveLength(2);
  expect(invocations[0].error).toContain("ThrottlingException");
  expect(invocations[1].error).toBeNull();
});
```

No tokens burned. No rate limits hit. No test flake. Runs on a plane.

The `classify` function is unchanged from production — it's a real `BedrockRuntimeClient` calling a real `InvokeModelCommand`. The only difference is `endpoint: "http://localhost:4566"` in the SDK config. Your production code never knows it's talking to fakecloud, which is exactly what you want: the tests exercise the same code path that runs in production.

## Isn't a fake model just a mock?

No, and the distinction is important.

A mock replaces the SDK client. A mock knows what method you called and what arguments you passed, and returns whatever you told it to. If your code uses a deprecated field, or sends invalid JSON, or builds the request wrong, the mock doesn't care — it was told to return a value and it returns it.

fakecloud is a real HTTP server implementing the real Bedrock wire protocol. Your AWS SDK sends real SigV4-signed requests over real HTTP. If your code builds the request wrong, fakecloud rejects it the same way real AWS would. The only thing that's "fake" is the inference — instead of calling a real model, fakecloud returns the response you configured.

And that's not a limitation, it's the point. **You don't want a real model in tests. You want a predictable one.** Real models are for production, where you want the best possible output. Tests are for verifying that your code behaves correctly *given* an output. Those are different problems and they need different tools.

When you don't configure anything, fakecloud falls back to a canned response per provider format — Anthropic, Titan, Llama, Cohere, Mistral all get provider-shaped defaults. So even a no-configuration test still gets something realistic back. You only need to configure responses when your code actually branches on content.

## What this unlocks

Things that were hard or impossible before, that are now just tests:

- **Free CI.** Run your full Bedrock-exercising test suite on every PR, every commit, every dependabot bump. No token budget to worry about.
- **Deterministic snapshots.** The model returns exactly what you configured. Your snapshot tests work again.
- **Offline development.** Write Bedrock code on a plane. Debug retry logic on a train. Iterate without thinking about AWS at all.
- **Error-path testing.** Exercise every `catch` block in your code. Retry, fallback, circuit-break, degrade-gracefully — all testable, all deterministic.
- **Cross-service integration tests.** Your Lambda calls Bedrock, writes to DynamoDB, and publishes to EventBridge? fakecloud runs all of those for real, in the same process, wired together. One test can exercise the whole flow.

## Try it

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

Then point any AWS SDK at `http://localhost:4566` with dummy credentials. The TypeScript, Python, Go, and Rust first-party SDKs all have ergonomic helpers for the Bedrock introspection and configuration endpoints shown above.

- **Repo:** [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- **Site:** [fakecloud.dev](https://fakecloud.dev)

If you try it and find a case where fakecloud's Bedrock implementation diverges from real AWS, that's a bug — [open an issue](https://github.com/faiscadev/fakecloud/issues). The tests should have caught it.
