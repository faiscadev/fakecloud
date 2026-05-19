+++
title = "Test Bedrock locally"
description = "Test code that calls AWS Bedrock, locally and deterministically. Configurable responses per prompt, fault injection for retry logic, call-history assertions. Free, no account, no GPU."
template = "page.html"
+++

Want to test code that calls Bedrock, without spending real tokens or hitting a real LLM? Use [fakecloud](https://github.com/faiscadev/fakecloud). Real Bedrock-wire-protocol server. Deterministic responses. Millisecond latency. Free.

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

Point any AWS SDK at `http://localhost:4566`.

## Why you don't want a real LLM in tests

Tests exist to prove your code is correct. A real LLM — including Ollama-backed local inference — makes that job harder, not easier:

1. **Non-deterministic.** Same prompt, different output every run. Even at `temperature=0`, attention-kernel non-determinism and tokenizer drift break snapshot tests. You end up asserting `output.length > 0`, which passes even if the model returned "cabbage."
2. **Slow.** Ollama on CPU = seconds per call. 100 tests × 5s = 8 minutes. Your TDD loop dies. CI times out.
3. **Resource-heavy.** GB-scale model weights. CI runners balloon, `docker pull` drags, laptops OOM.
4. **Tests the wrong thing.** A real LLM tests whether the model understood your prompt. Your tests should cover whether *your code* handles Bedrock's response shape, retries correctly on throttling, builds the request body right. Those are different concerns.
5. **Flaky.** Rate limits if real AWS. GPU-less OOM if local. Cold-start delays everywhere.

What your tests actually need:

- Deterministic response per input
- Fault injection (throttle, validation error, timeout)
- Call history ("did my code send the right prompt?")
- Millisecond latency
- Zero external dependencies

fakecloud gives exactly this.

## Configurable responses

```ts
import { FakeCloud } from "fakecloud";
import {
  BedrockRuntimeClient,
  InvokeModelCommand,
} from "@aws-sdk/client-bedrock-runtime";

const fc = new FakeCloud();
const rt = new BedrockRuntimeClient({ endpoint: "http://localhost:4566" });

beforeEach(() => fc.reset());

test("summarizer returns 3-bullet format", async () => {
  await fc.bedrock.setResponseRule({
    whenPromptContains: "summarize",
    respond: {
      completion: "- bullet one\n- bullet two\n- bullet three",
    },
  });

  const out = await summarize(rt, "long text here");

  expect(out.bullets).toHaveLength(3);
});

test("classifier returns spam vs ham", async () => {
  await fc.bedrock.setResponseRule({
    whenPromptContains: "classify",
    respond: { completion: JSON.stringify({ label: "spam" }) },
  });

  expect(await classify(rt, "buy now!!")).toBe("spam");
});
```

Same test file, different fixtures per branch of your code.

## Fault injection for retry logic

```python
import boto3
from fakecloud import FakeCloud

fc = FakeCloud()
rt = boto3.client('bedrock-runtime', endpoint_url='http://localhost:4566',
    aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1')

fc.bedrock.inject_fault(
    operation='InvokeModel',
    error='ThrottlingException',
    count=2,
)

# your code retries with exponential backoff; third call succeeds
result = resilient_classify(rt, "some text")

history = fc.bedrock.get_call_history()
assert len(history) == 3  # two throttles, one success
assert history[0].error == 'ThrottlingException'
assert history[2].error is None
```

Your retry path — the thing that breaks in production if you didn't write it right — now gets exercised in every test run.

## Call-history assertions

```ts
await myAgent.run({ task: "research fakecloud" });

const calls = await fc.bedrock.getCallHistory();
expect(calls).toHaveLength(3);                    // agent made exactly 3 model calls
expect(calls[0].modelId).toBe("anthropic.claude-3-haiku-20240307-v1:0");
expect(calls[0].messages[0].content).toContain("research");
expect(calls[2].messages).toHaveLength(5);        // 3-turn conversation
```

Assert on what your code *sent*, not just what it received.

## Streaming

Bedrock returns binary EventStream frames for streaming endpoints. fakecloud encodes them correctly — your streaming consumer sees real chunks:

```ts
await fc.bedrock.setResponseRule({
  operation: "InvokeModelWithResponseStream",
  chunks: ["Once ", "upon ", "a ", "time"],
});

const stream = await rt.send(new InvokeModelWithResponseStreamCommand({
  modelId: "anthropic.claude-3-haiku-20240307-v1:0",
  body: JSON.stringify({ messages: [{ role: "user", content: "story" }] }),
}));

const chunks = [];
for await (const evt of stream.body!) {
  chunks.push(decodeChunk(evt));
}
expect(chunks).toEqual(["Once ", "upon ", "a ", "time"]);
```

## Comparison

| | Deterministic | Fault injection | Call history | Speed | Cost | AWS bill |
|---|---|---|---|---|---|---|
| **fakecloud** | Yes | Yes | Yes | ms | Free | None |
| Real Bedrock | No | Hard (rate-limit abuse) | No | 100-2000ms | $$$ | Real |
| Ollama / local LLM | No | No | No | 1-30s | CPU + RAM | None |
| LocalStack Ultimate | No (Ollama backed) | Limited | No | 1-30s | Paid Ultimate tier | None |
| Mock library | Yes | Yes | Yes | ms | Free | None, but doesn't test HTTP path |

## Links

- **Install**: `curl -fsSL https://fakecloud.dev/install.sh | bash`
- **Repo**: [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- **Full tutorial**: [How to test Bedrock code locally, for free, deterministically](/blog/bedrock-local-testing/)
- **Related**: [Bedrock emulator](/bedrock-emulator/), [LocalStack Bedrock alternative](/localstack-bedrock-alternative/), [Testing LLM guardrails](/blog/llm-guardrails/)
