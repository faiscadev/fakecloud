+++
title = "Testing Bedrock"
description = "Complete guide to testing Bedrock-calling code locally against fakecloud: configurable responses, fault injection, call history, and cross-service patterns."
weight = 1
+++

This is the complete manual for testing Bedrock-calling code against fakecloud. It covers configuring deterministic responses, injecting errors to exercise retry logic, asserting on call history, handling each model provider's request format, and wiring Bedrock into cross-service tests.

If you just want the news pitch, read the [blog post](/blog/bedrock-local-testing/). This guide is the reference.

## Why you need this

Four things break when you try to test Bedrock code against the real service:

1. **Cost.** Every test run that calls a model is a line item on your AWS bill.
2. **Rate limits.** Bedrock has per-account quotas. A noisy test suite gets throttled.
3. **Non-determinism.** Real models return different text every run. Assertions on specific strings are impossible.
4. **No offline dev.** Planes, trains, bad wifi, air-gapped environments.

fakecloud solves all four. You get real InvokeModel / Converse / streaming wire behavior, deterministic responses you configure, and a free local loop.

## The core loop

Three primitives, combined however you need:

1. **Configure the response** — what the "model" should return for this test
2. **Run your code** — unchanged from production, using the real AWS SDK
3. **Assert on the call history** — what your code actually sent

```typescript
import { FakeCloud } from "fakecloud";
import { BedrockRuntimeClient, InvokeModelCommand } from "@aws-sdk/client-bedrock-runtime";

const fc = new FakeCloud();
const runtime = new BedrockRuntimeClient({
  endpoint: "http://localhost:4566",
  region: "us-east-1",
  credentials: { accessKeyId: "test", secretAccessKey: "test" },
});

beforeEach(() => fc.reset());

test("example", async () => {
  // 1. Configure
  await fc.bedrock.setModelResponse(
    "anthropic.claude-3-haiku-20240307-v1:0",
    JSON.stringify({
      content: [{ type: "text", text: "hello back" }],
      stop_reason: "end_turn",
      usage: { input_tokens: 5, output_tokens: 3 },
    })
  );

  // 2. Run your code (exactly as in production)
  const response = await runtime.send(
    new InvokeModelCommand({
      modelId: "anthropic.claude-3-haiku-20240307-v1:0",
      body: JSON.stringify({
        anthropic_version: "bedrock-2023-05-31",
        max_tokens: 100,
        messages: [{ role: "user", content: "hi" }],
      }),
    })
  );

  // 3. Assert
  const { invocations } = await fc.bedrock.getInvocations();
  expect(invocations).toHaveLength(1);
  expect(invocations[0].modelId).toBe("anthropic.claude-3-haiku-20240307-v1:0");
});
```

## Response configuration

fakecloud supports three layers of response configuration, evaluated in order:

1. **Prompt-conditional rules** (`setResponseRules`) — first match wins
2. **Single model override** (`setModelResponse`) — one response for the whole model
3. **Canned provider default** — shipped with fakecloud, no configuration needed

### Prompt-conditional rules

Use these when your code's behavior depends on what the model returns. Different prompts get different responses — tests for branching logic, classifier outputs, router decisions.

```typescript
await fc.bedrock.setResponseRules("anthropic.claude-3-haiku-20240307-v1:0", [
  {
    promptContains: "buy now",
    response: JSON.stringify({
      content: [{ type: "text", text: '{"label":"spam","score":0.98}' }],
      stop_reason: "end_turn",
      usage: { input_tokens: 12, output_tokens: 18 },
    }),
  },
  {
    promptContains: "unsubscribe",
    response: JSON.stringify({
      content: [{ type: "text", text: '{"label":"spam","score":0.87}' }],
      stop_reason: "end_turn",
      usage: { input_tokens: 12, output_tokens: 18 },
    }),
  },
  {
    promptContains: null, // catch-all; matches any prompt
    response: JSON.stringify({
      content: [{ type: "text", text: '{"label":"ham","score":0.05}' }],
      stop_reason: "end_turn",
      usage: { input_tokens: 12, output_tokens: 18 },
    }),
  },
]);
```

Rules are evaluated top-to-bottom and the **first match wins**. The null / absent `promptContains` matches anything, so use it as the catch-all at the bottom.

Matching happens on the full user-visible prompt text. For InvokeModel, that's whatever you sent in the model-specific prompt field. For Converse, it's the concatenation of all user-role message text.

To clear rules for a model:

```typescript
await fc.bedrock.clearResponseRules("anthropic.claude-3-haiku-20240307-v1:0");
```

### Single model override

When your test doesn't care about branching and just wants a fixed response:

```typescript
await fc.bedrock.setModelResponse(
  "anthropic.claude-3-haiku-20240307-v1:0",
  JSON.stringify({
    content: [{ type: "text", text: "canned reply" }],
    stop_reason: "end_turn",
    usage: { input_tokens: 10, output_tokens: 2 },
  })
);
```

Every call to that model returns this response, regardless of prompt.

### Canned provider defaults

If you don't configure anything, fakecloud returns a provider-shaped default response. These exist so tests that don't care about model output can still run without setup.

Provider defaults are shipped for:

- Anthropic (Claude 3 Haiku, Sonnet, Opus)
- Amazon Titan
- Meta Llama
- Cohere Command
- Mistral

All follow the same shape as real model responses. They're not useful if your code inspects the output, but they're fine for tests that just verify "did my code call Bedrock at all."

### Echo mode

If you want every model call to reflect the caller's prompt back as the assistant text, set `FAKECLOUD_BEDROCK_ECHO=1` on the fakecloud process. This skips the canned phrase and instead pins the response on whatever the test sent in, which is convenient for assertions that don't care about model behavior — only that the prompt round-tripped through your code.

```bash
FAKECLOUD_BEDROCK_ECHO=1 fakecloud
```

Echo mode applies to InvokeModel, Converse, InvokeModelWithResponseStream, and ConverseStream, and covers all five provider shapes (Anthropic, Amazon, Meta, Cohere, Mistral). It is bypassed by any prompt-conditional rule or single-model override you configure, so explicit responses always win.

Token counts in headers and `usage` fields are derived from the actual prompt and generated text in all modes — they scale with input length rather than returning a fixed placeholder.

## Fault injection

Real Bedrock throws a specific set of errors. Production code has retry, fallback, and circuit-breaker logic for them — and that logic is normally untestable because you can't make real Bedrock fail on command.

fakecloud lets you queue up faults that fire on matching calls.

### Basic throttling

```typescript
await fc.bedrock.queueFault({
  errorType: "ThrottlingException",
  message: "Rate exceeded",
  httpStatus: 429,
  count: 1, // fail the next 1 matching call, then auto-clear
});

// First call throws; retry succeeds (if your code retries)
await classify("hello");

const { invocations } = await fc.bedrock.getInvocations();
expect(invocations).toHaveLength(2);
expect(invocations[0].error).toContain("ThrottlingException");
expect(invocations[1].error).toBeNull();
```

### Supported error types

All return the exact AWS response format — HTTP status, `__type` header, correct JSON body shape. Your AWS SDK will deserialize them into the same error types it does in production.

- `ThrottlingException` (429)
- `ValidationException` (400)
- `ServiceUnavailableException` (503)
- `ModelTimeoutException` (408)
- `ModelStreamErrorException` (500, streaming only)
- `ModelErrorException` (424)
- `ResourceNotFoundException` (404)
- `AccessDeniedException` (403)
- `ModelNotReadyException` (429)

### Filtering which calls fault

Pass `modelId` and/or `operation` to scope a fault to specific calls:

```typescript
// Only fail calls to Claude Opus
await fc.bedrock.queueFault({
  modelId: "anthropic.claude-3-opus-20240229-v1:0",
  errorType: "ThrottlingException",
  count: 5,
});

// Only fail ConverseStream, not regular Converse
await fc.bedrock.queueFault({
  operation: "ConverseStream",
  errorType: "ModelStreamErrorException",
  count: 1,
});
```

Operations you can filter on: `InvokeModel`, `Converse`, `InvokeModelWithResponseStream`, `ConverseStream`.

### After N calls

Fail the third call and beyond while letting the first two through:

```typescript
await fc.bedrock.queueFault({
  errorType: "ThrottlingException",
  // (add `after_n_calls: 2` when that field is supported; today, queue it after
  // you've made the calls that should succeed)
});
```

In the current API, the simplest way to "let the first N succeed and then fault" is to run those calls, then queue the fault, then run the remaining calls.

### Multiple faults

Queue multiple faults with different filters. They evaluate in insertion order; first match wins.

```typescript
await fc.bedrock.queueFault({
  modelId: "anthropic.claude-3-opus-20240229-v1:0",
  errorType: "ThrottlingException",
  count: 1,
});
await fc.bedrock.queueFault({
  modelId: "anthropic.claude-3-haiku-20240307-v1:0",
  errorType: "ModelTimeoutException",
  count: 1,
});
```

### Clearing faults

```typescript
await fc.bedrock.clearFaults();
```

Or between tests via `fc.reset()`.

## Call history

Every InvokeModel / Converse / streaming call is recorded. `getInvocations()` returns the list.

```typescript
const { invocations } = await fc.bedrock.getInvocations();

for (const call of invocations) {
  console.log(call.modelId);   // "anthropic.claude-3-haiku-20240307-v1:0"
  console.log(call.input);     // request body, as parsed JSON
  console.log(call.output);    // response body, as parsed JSON ("" for a faulted call)
  console.log(call.timestamp); // ISO 8601
  console.log(call.error);     // null on success, "ThrottlingException: Rate exceeded" on fault
}
```

Useful assertion patterns:

```typescript
// How many times did we call the model?
expect(invocations).toHaveLength(3);

// Did the retry work?
expect(invocations[0].error).toContain("ThrottlingException");
expect(invocations[1].error).toBeNull();

// Did we pass the right context?
const firstCall = invocations[0].input as { messages: { content: string }[] };
expect(firstCall.messages[0].content).toContain("user-12345");

// Did our cost-tracking see the right token count?
const response = invocations[0].output as { usage: { output_tokens: number } };
expect(response.usage.output_tokens).toBe(42);
```

Streaming calls record the body the equivalent non-streaming call returns: an `InvokeModelWithResponseStream` call reads back as the provider's InvokeModel response, and a `ConverseStream` call as the Converse response, so the same assertions work for both.

## Per-provider request formats

Different model providers use different request body shapes. fakecloud's prompt extraction understands all the ones Bedrock supports, so `promptContains` matching works regardless of provider.

### Anthropic Claude

```json
{
  "anthropic_version": "bedrock-2023-05-31",
  "max_tokens": 1024,
  "messages": [
    { "role": "user", "content": "Your prompt here" }
  ]
}
```

### Amazon Titan

```json
{
  "inputText": "Your prompt here",
  "textGenerationConfig": {
    "maxTokenCount": 1024,
    "temperature": 0.7
  }
}
```

### Meta Llama

```json
{
  "prompt": "Your prompt here",
  "max_gen_len": 1024,
  "temperature": 0.7
}
```

### Cohere Command

```json
{
  "prompt": "Your prompt here",
  "max_tokens": 1024
}
```

### Mistral

```json
{
  "prompt": "Your prompt here",
  "max_tokens": 1024
}
```

For all five, the "prompt" that `promptContains` matches against is the user-visible text content, regardless of which field the provider puts it in.

## Streaming

`InvokeModelWithResponseStream` and `ConverseStream` work with the same response configuration and fault injection as the non-streaming versions. If you configure a single response, fakecloud delivers it as a single chunk. If you inject a streaming-only error (`ModelStreamErrorException`), it surfaces as a top-level HTTP error rather than mid-stream — a reasonable approximation for testing retry logic.

## Cross-service: Lambda -> Bedrock

fakecloud's Bedrock implementation runs in the same process as every other service, so cross-service flows involving Bedrock are testable end-to-end. Example — a Lambda that classifies incoming S3 uploads via Claude and writes results to DynamoDB:

```typescript
test("classifier pipeline end-to-end", async () => {
  // Configure the model response for this test
  await fc.bedrock.setResponseRules(modelId, [
    { promptContains: "buy now", response: spamResponse },
    { promptContains: null, response: hamResponse },
  ]);

  // Upload a file to S3 — this triggers the Lambda via bucket notifications
  await s3.send(new PutObjectCommand({
    Bucket: "uploads",
    Key: "message-1.txt",
    Body: "check out this buy now deal",
  }));

  // Give the Lambda a moment to run
  await new Promise(r => setTimeout(r, 100));

  // Assert: Bedrock was called
  const { invocations } = await fc.bedrock.getInvocations();
  expect(invocations).toHaveLength(1);
  expect(invocations[0].output).toEqual(JSON.parse(spamResponse));

  // Assert: DynamoDB was written
  const result = await ddb.send(new GetItemCommand({
    TableName: "classifications",
    Key: { id: { S: "message-1.txt" } },
  }));
  expect(result.Item?.label.S).toBe("spam");
});
```

Lambda actually invokes the Bedrock runtime. Bedrock really returns the configured response. Lambda really writes to DynamoDB. The test exercises every piece of the wiring, not just each service in isolation.

## Things to watch out for

**Reset between tests.** `fc.reset()` in `beforeEach` clears all Bedrock state — invocations, response rules, queued faults, custom model responses. Tests that forget this will leak state between cases.

**The model doesn't actually run.** fakecloud returns the response you configured (or a canned default). It never calls a real model. This is a feature, not a bug — you don't want a real model in tests, you want a predictable one — but it means fakecloud can't help you test your prompts. Prompt engineering happens against real Bedrock; application logic testing happens against fakecloud.

**Provider format matters for `promptContains`.** If you send a malformed request body, fakecloud's prompt extraction might miss your keyword. When in doubt, use `promptContains: null` as a catch-all and inspect the recorded invocation to confirm what your code actually sent.

**Streaming errors are top-level, not mid-stream.** `ModelStreamErrorException` surfaces as an HTTP error, not as an error event within the stream. Your retry logic sees it the same way either way, but if you have mid-stream error handling specifically, that code path isn't exercised.

## Next

- The [blog post](/blog/bedrock-local-testing/) for the "why this exists" story
- [Introspection endpoints](/docs/reference/introspection/) for the raw HTTP API
- [Cross-service integration tests](/docs/guides/cross-service-integration/) for more multi-service patterns
