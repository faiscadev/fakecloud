+++
title = "Bedrock emulator"
description = "Bedrock emulator for tests: 214 operations across Bedrock, Bedrock Runtime, Bedrock Agent, and Bedrock Agent Runtime. Real wire protocol, fault injection, configurable responses. Deterministic, offline, free. Not a mock library, not a real LLM."
template = "page.html"
aliases = [
    "/features/local-bedrock/",
    "/features/bedrock/",
    "/local-bedrock/",
    "/bedrock-local/",
]
+++

Need a Bedrock emulator? Use [fakecloud](https://github.com/faiscadev/fakecloud). Not a mock library. Not a real LLM. A real server that speaks the Bedrock wire protocol and returns exactly what you tell it to.

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

Point any AWS SDK at `http://localhost:4566`. That's the setup.

## Why emulator, not mock, not Ollama

Three categories of tools, three different jobs:

- **Mock library** (moto, aws-sdk-client-mock) — replaces the AWS SDK internals with stubs. Never hits HTTP. Your code isn't tested against anything that looks like Bedrock — it's tested against a fake object matching the SDK interface. If you assemble the request wrong, the mock doesn't care.
- **Real LLM locally** (Ollama, LM Studio) — real inference on your CPU. Slow. Non-deterministic. GBs of weights. Your tests can't assert on output because every run returns different text. A CI runner takes 8 minutes to boot the model. LocalStack's "Ultimate"-tier Bedrock does this.
- **Emulator** (fakecloud) — real HTTP server on port 4566. Speaks Bedrock's JSON wire protocol. Your AWS SDK sends real requests. You control the response per prompt. Deterministic. Millisecond latency. No GBs of weights.

Tests want the third one. You're testing your code, not whether the model happens to understand the prompt today.

## What fakecloud Bedrock does

**214 operations across the full Bedrock family** (Bedrock + Bedrock Runtime + Bedrock Agent + Bedrock Agent Runtime), not just the runtime:

- **Runtime**: `InvokeModel`, `InvokeModelWithResponseStream`, `Converse`, `ConverseStream` — EventStream binary encoded, same as real AWS.
- **Guardrails**: full CRUD + versioning + apply + content evaluation. Test your guardrail config locally before deploying.
- **Custom models**: `CreateModelCustomizationJob`, `GetCustomModel`, `ListCustomModels`, deletion, versioning.
- **Model import**: `CreateModelImportJob` for bring-your-own weights flow.
- **Inference profiles**, **provisioned throughput**, **model copy jobs**, **async invoke + batch inference via S3**.
- **Prompt management**: prompts + prompt routers with versioning.
- **Foundation model agreements**: the `PutUseCaseForModelAccess` / `CreateFoundationModelAgreement` access approval flow.
- **Automated reasoning policies**, **evaluation jobs**, **marketplace**, **resource policies**, **logging configuration**.
- **Agents**: full Agents control plane — agents, aliases/versions, action groups, knowledge bases, data sources, ingestion jobs, prompts, flows. See [Bedrock Agent](/docs/services/bedrock-agent/).
- **Agent invocation**: `InvokeAgent`, `InvokeInlineAgent`, `InvokeFlow`, `Retrieve`, `RetrieveAndGenerate`, sessions, memory, reranking. Streaming with real eventstream framing. See [Bedrock Agent Runtime](/docs/services/bedrock-agent-runtime/).

What it does NOT do: actual inference. Response content is whatever you configured. That's the point — deterministic tests.

## Configurable responses per prompt

```ts
import { FakeCloud } from "fakecloud";
import {
  BedrockRuntimeClient,
  InvokeModelCommand,
} from "@aws-sdk/client-bedrock-runtime";

const fc = new FakeCloud();

beforeEach(() => fc.reset());

test("spam classifier routes to review on borderline score", async () => {
  await fc.bedrock.setResponseRule({
    whenPromptContains: "classify spam",
    respond: {
      completion: JSON.stringify({ label: "borderline", confidence: 0.62 }),
    },
  });

  const rt = new BedrockRuntimeClient({ endpoint: "http://localhost:4566" });
  const out = await classifyEmail(rt, "buy now!!");

  expect(out.routedTo).toBe("human-review");
});
```

## Fault injection

```ts
await fc.bedrock.injectFault({
  operation: "InvokeModel",
  error: "ThrottlingException",
  count: 2,
});

// your code retries with backoff; third call succeeds
const out = await yourClassifier(rt, "buy now!!");
expect(fc.bedrock.getCallHistory()).toHaveLength(3);
```

Real retry code paths get exercised. No more "we'll test the retry logic later."

## Guardrails, locally

```python
import boto3
bedrock = boto3.client('bedrock', endpoint_url='http://localhost:4566',
    aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1')

g = bedrock.create_guardrail(
    name='no-pii',
    contentPolicyConfig={
        'filtersConfig': [
            {'type': 'HATE', 'inputStrength': 'HIGH', 'outputStrength': 'HIGH'},
        ],
    },
    sensitiveInformationPolicyConfig={
        'piiEntitiesConfig': [{'type': 'EMAIL', 'action': 'BLOCK'}],
    },
    blockedInputMessaging='Blocked.',
    blockedOutputsMessaging='Blocked.',
)

# your app flows
rt = boto3.client('bedrock-runtime', endpoint_url='http://localhost:4566',
    aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1')
rt.apply_guardrail(guardrailIdentifier=g['guardrailId'],
                   guardrailVersion='DRAFT', source='INPUT',
                   content=[{'text': {'text': 'contact me at bob@example.com'}}])
```

`ApplyGuardrail` runs real content evaluation. PII action `BLOCK` returns the blocked message. Test your guardrail policy without AWS.

## Comparison

| Tool | Wire protocol | Deterministic | Guardrails | Fine-tuning jobs | Async batch | Price |
|---|---|---|---|---|---|---|
| **fakecloud** | Real | Yes | Yes | Yes (control plane) | Yes | Free, AGPL-3.0 |
| Real Bedrock | Real | No | Yes | Yes | Yes | $$$ per token |
| LocalStack Ultimate (Ollama) | Real | No (Ollama inference) | No | No | Partial | Paid Ultimate tier |
| Ollama standalone | Different (OpenAI-ish) | No | No | No | No | Free, but doesn't speak Bedrock |
| Mock library (moto, etc.) | N/A (no HTTP) | Yes | Stubbed | Stubbed | Stubbed | Free |

## Links

- **Homebrew**: `brew install fakecloud`
- **Install**: `curl -fsSL https://fakecloud.dev/install.sh | bash`
- **Repo**: [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- **Deep dive**: [How to test Bedrock code locally, for free, deterministically](/blog/bedrock-local-testing/)
- **LLM Guardrails**: [Testing LLM guardrails locally](/blog/llm-guardrails/)
- **Related**: [Fake AWS server for tests](/fake-aws-server/), [Test Lambda locally](/test-lambda-locally/)
