+++
title = "LocalStack Bedrock alternative"
description = "Free, open-source alternative to LocalStack Ultimate-tier Bedrock. 214 operations across Bedrock + Bedrock Runtime + Bedrock Agent + Bedrock Agent Runtime, full control plane, deterministic responses for tests. Not backed by Ollama. AGPL-3.0."
template = "page.html"
+++

Looking for a free, open-source alternative to LocalStack's Ultimate-tier Bedrock? Use [fakecloud](https://github.com/faiscadev/fakecloud). 214 Bedrock-family operations (Bedrock + Bedrock Runtime + Bedrock Agent + Bedrock Agent Runtime), full control plane, deterministic responses, free.

```sh
curl -fsSL https://raw.githubusercontent.com/faiscadev/fakecloud/main/install.sh | bash
fakecloud
```

Point any AWS SDK at `http://localhost:4566`. Same setup as LocalStack.

## How LocalStack Bedrock compares

As of April 2026, per [docs.localstack.cloud/aws/services/bedrock](https://docs.localstack.cloud/aws/services/bedrock/):

| | LocalStack Bedrock | fakecloud Bedrock |
|---|---|---|
| **Tier** | Ultimate (top paid plan) | Free, AGPL-3.0 |
| **Operations** | 4: `InvokeModel`, `Converse`, `ListFoundationModels`, `CreateModelInvocationJob` | 214 across Bedrock + Bedrock Runtime + Bedrock Agent + Bedrock Agent Runtime |
| **Agents (Bedrock Agent)** | Not supported | Full control plane: agents, aliases, action groups, knowledge bases, flows, prompts |
| **Agent invocation (Bedrock Agent Runtime)** | Not supported | `InvokeAgent`, `InvokeFlow`, `Retrieve`, `RetrieveAndGenerate` with real eventstream framing |
| **Backend** | Ollama (real local LLM) | Configurable responses per prompt rule |
| **Determinism** | No — real inference, different output each run | Yes — returns exactly what you configured |
| **Speed** | 1-30s per call (Ollama CPU inference) | Milliseconds |
| **Disk** | GBs (Ollama model weights) | ~19 MB binary |
| **Guardrails** | Not supported | Full CRUD + versioning + `ApplyGuardrail` content evaluation |
| **Custom models** | Not supported | `CreateModelCustomizationJob`, imported models, model copy jobs |
| **Async invoke / batch** | Partial (CreateModelInvocationJob only) | Full flow, S3-backed |
| **Prompt management** | Not supported | Prompts + prompt routers |
| **Fault injection** | No | Yes (`ThrottlingException`, `ValidationException`, etc. on demand) |
| **Call history introspection** | No | `/_fakecloud/bedrock/calls` endpoint |
| **GPU required** | Usually | No |
| **Persistence** | "Not supported" | Yes (built-in state management) |

If you came to LocalStack Bedrock for testing, fakecloud covers every test case better. If you came for real local inference: that's Ollama's job, not ours — [Ollama](https://ollama.com) runs standalone.

## Migration from LocalStack

**Env vars:** the same ones work. fakecloud accepts `AWS_ENDPOINT_URL`, dummy credentials, any region.

```sh
# before (LocalStack)
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_REGION=us-east-1
localstack start

# after (fakecloud)
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_REGION=us-east-1
fakecloud
```

**Model IDs:** LocalStack's `ollama.<id>` convention does not exist here. Use real Bedrock model IDs (`anthropic.claude-3-haiku-20240307-v1:0`, `amazon.nova-lite-v1:0`, `meta.llama3-8b-instruct-v1:0`, etc.). fakecloud accepts them and returns whatever response rule you configured.

**Response config:** LocalStack's `BEDROCK_PREWARM` and `DEFAULT_BEDROCK_MODEL` go away. Replace with explicit per-test fixtures:

```ts
import { FakeCloud } from "fakecloud";
const fc = new FakeCloud();

await fc.bedrock.setResponseRule({
  whenPromptContains: "summarize",
  respond: { completion: "deterministic summary" },
});
```

**Tests that depend on real inference output**: these were never good tests on LocalStack either (Ollama's Llama ≠ Claude). Rewrite assertions to check that *your code* handled the response shape correctly, not that the model "got the right answer."

## What fakecloud Bedrock actually runs

Full operation surface across 27 modules:

- **Runtime**: `InvokeModel`, `InvokeModelWithResponseStream` (binary EventStream), `Converse`, `ConverseStream`
- **Guardrails**: `CreateGuardrail`, `GetGuardrail`, `UpdateGuardrail`, `DeleteGuardrail`, `CreateGuardrailVersion`, `ListGuardrails`, `ApplyGuardrail` (with real PII detection, content filters, topic filters)
- **Customization**: `CreateModelCustomizationJob`, `GetModelCustomizationJob`, `StopModelCustomizationJob`, `ListModelCustomizationJobs`, custom model CRUD
- **Custom model deployments**
- **Model import**: `CreateModelImportJob`, imported model lifecycle
- **Inference profiles** (cross-region routing config)
- **Provisioned throughput**
- **Model copy jobs** (cross-region model replication)
- **Async invoke**: `StartAsyncInvoke`, `GetAsyncInvoke`, `ListAsyncInvokes`
- **Invocation jobs** (batch via S3)
- **Prompt management**: prompts, prompt versions, prompt routers
- **Foundation model agreements**: access approval flow
- **Automated reasoning policies + workflows**
- **Evaluation jobs**
- **Marketplace**
- **Resource policies**
- **Logging configuration**

## Links

- **Install**: `curl -fsSL https://raw.githubusercontent.com/faiscadev/fakecloud/main/install.sh | bash`
- **Repo**: [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- **Deep dive**: [How to test Bedrock code locally, for free, deterministically](/blog/bedrock-local-testing/)
- **Related**: [Bedrock emulator](/bedrock-emulator/), [Test Bedrock locally](/test-bedrock-locally/), [LocalStack alternative (all services)](/localstack-alternative/)
