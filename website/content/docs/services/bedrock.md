+++
title = "Bedrock"
description = "Foundation models, guardrails, custom models, invocation jobs, evaluation jobs, marketplace endpoints."
weight = 21
+++

fakecloud implements **216 of 216** Bedrock-family operations across four APIs:

- **Bedrock** (control plane) — 103 operations *(this page)*
- **Bedrock Runtime** (model invocation) — 11 operations *(this page)*
- [**Bedrock Agent**](/docs/services/bedrock-agent/) (agents control plane) — 72 operations
- [**Bedrock Agent Runtime**](/docs/services/bedrock-agent-runtime/) (agent invocation) — 31 operations

All four at true 100% Smithy conformance. **No other AWS emulator supports Bedrock at any tier.**

For a complete testing guide with code examples, see [Testing Bedrock](/docs/guides/testing-bedrock/). This page is the service reference.

## Control plane (Bedrock)

- **Foundation models** — ListFoundationModels, GetFoundationModel, GetFoundationModelAvailability
- **Guardrails** — CRUD, versioning, content evaluation, enforced configurations
- **Custom models** — CRUD with training jobs
- **Custom model deployments** — deployment lifecycle
- **Model customization jobs** — CreateModelCustomizationJob, GetModelCustomizationJob
- **Model invocation jobs** — batch inference jobs
- **Model import jobs** — CreateModelImportJob, GetModelImportJob
- **Model copy jobs** — CreateModelCopyJob, GetModelCopyJob
- **Evaluation jobs** — CreateEvaluationJob, GetEvaluationJob, StopEvaluationJob
- **Inference profiles** — cross-region routing profiles
- **Prompt routers** — CRUD
- **Provisioned throughput** — CRUD
- **Marketplace model endpoints** — CRUD with foundation model agreements
- **Resource policies** — CRUD
- **Automated reasoning policies** — policies, versions, test cases, build workflows, annotations

## Runtime (Bedrock Runtime)

- **InvokeModel** — with canned or configurable responses
- **InvokeModelWithResponseStream** — streaming, same config surface
- **Converse** — with message history, tool use
- **ConverseStream** — streaming variant
- **ApplyGuardrail** — content evaluation against configured guardrails
- **InvokeGuardrailChecks** — inline guardrail-tier checks (no pre-created guardrail): evaluates messages for `contentFilter` / `promptAttack` categories and `sensitiveInformation` entities, returning per-detection scores, PII match offsets, and per-check text-unit usage
- **CountTokens** — token counting for Anthropic model bodies
- **Async invoke** — StartAsyncInvoke, GetAsyncInvoke, ListAsyncInvokes

## Supported providers

fakecloud understands request bodies for all Bedrock-supported model providers:

- **Anthropic** (Claude 3 Haiku, Sonnet, Opus; Claude 3.5 Sonnet)
- **Amazon Titan** (Express, Lite, Embeddings)
- **Meta Llama** (2, 3)
- **Cohere** (Command, Command R, Command R+)
- **Mistral** (Mistral 7B, Mixtral, Mistral Large)

## Protocol

REST. Path-based routing for runtime operations, JSON bodies per provider.

## Introspection

- `GET /_fakecloud/bedrock/invocations` — list runtime invocations with `modelId`, `input`, `output`, `timestamp`, `error`
- `POST /_fakecloud/bedrock/models/{model_id}/response` — set a single custom response for all calls to a model
- `POST /_fakecloud/bedrock/models/{model_id}/responses` — set prompt-conditional response rules
- `DELETE /_fakecloud/bedrock/models/{model_id}/responses` — clear response rules
- `POST /_fakecloud/bedrock/faults` — queue fault injection rules
- `GET /_fakecloud/bedrock/faults` — list queued faults
- `DELETE /_fakecloud/bedrock/faults` — clear all faults

## Echo mode

Set `FAKECLOUD_BEDROCK_ECHO=1` on the fakecloud process to make every InvokeModel / Converse / streaming call reflect the user's prompt back as the assistant text in the provider-correct shape. Useful for tests that just need the prompt to round-trip through application code without configuring an explicit response per call. Explicit overrides still win.

Token counts in headers and `usage` fields scale with the actual input length in all modes.

## Real model inference (configurable upstream)

Bedrock's product *is* its data plane: LLM inference. Like RDS needing a real Postgres container, Bedrock actually performs real inference when you wire the backing infra — a real LLM endpoint. Point fakecloud at one and `InvokeModel`, `Converse`, `InvokeModelWithResponseStream`, and `ConverseStream` perform genuine HTTP inference: fakecloud translates the incoming Bedrock provider-native request into the upstream protocol, calls it, and translates the response *back* into the exact Bedrock provider-native shape (so an `aws-sdk` BedrockRuntime client sees a normal Bedrock response), with real upstream token counts surfaced through the `usage` fields and the `x-amzn-bedrock-*-token-count` headers.

Configuration is entirely environment-driven (config, not API surface — the SDKs are unchanged):

| Variable | Meaning |
| --- | --- |
| `FAKECLOUD_BEDROCK_UPSTREAM_URL` | Base URL of the upstream endpoint. **Its presence enables real inference.** |
| `FAKECLOUD_BEDROCK_UPSTREAM_KEY` | Optional API key / bearer token for the upstream. |
| `FAKECLOUD_BEDROCK_UPSTREAM_PROTOCOL` | Upstream wire protocol: `anthropic` (`/v1/messages`), `openai` (`/v1/chat/completions`), or `ollama` (`/api/chat`). Default `openai`. |
| `FAKECLOUD_BEDROCK_UPSTREAM_MODEL` | Default upstream model id to forward text generation to. |
| `FAKECLOUD_BEDROCK_UPSTREAM_EMBED_MODEL` | Default upstream model id for embeddings. |
| `FAKECLOUD_BEDROCK_UPSTREAM_MODEL_MAP` | Alias map `bedrock_id_or_prefix=upstream_model,...`, so e.g. `anthropic.claude-3-5-sonnet-*` maps to whatever upstream model you run. |

Model id mapping is faithful to the request: the caller asked for a specific Bedrock model id, so fakecloud forwards a corresponding real model — a configured alias, else the default model, else the Bedrock id with its `provider.` namespace stripped.

**Translation per provider.** The incoming body is parsed per Bedrock provider shape — Anthropic Messages (`messages`/`system`), Amazon Titan/Nova (`inputText` / `messages`), Meta Llama (`prompt`), Cohere (`prompt`/`message`), Mistral (`prompt`) — into a unified chat, sent to the upstream, then the completion is rendered back into that provider's native response shape (Anthropic `content[].text`, Titan `results[].outputText`, Llama `generation`, Cohere `generations[].text`, Mistral `outputs[].text`).

**Embeddings.** Titan and Cohere embed models route to the upstream's embeddings endpoint (OpenAI `/v1/embeddings`, Ollama `/api/embeddings`) when the protocol supports it; the Anthropic protocol has no embeddings endpoint, so those fall back to the deterministic offline vector.

**Streaming.** `InvokeModelWithResponseStream` and `ConverseStream` consume the upstream's SSE stream and re-encode each real incremental token as a Bedrock event-stream frame — genuine per-token deltas, not one canned block — terminating with real usage figures.

**Errors.** An upstream failure maps to a faithful Bedrock error — `ThrottlingException` (upstream 429), `ValidationException` (upstream 4xx), or `ModelErrorException` (network failure, upstream 5xx, or a malformed upstream body) — never a panic and never a silent canned fallback. Explicit per-model response overrides and queued fault-injection rules still take precedence over the upstream call.

When `FAKECLOUD_BEDROCK_UPSTREAM_URL` is unset (the default), the runtime stays fully offline and deterministic — the canned / echo / configurable-response behavior above is unchanged.

## The full test loop

Configure a response, run code, assert on what was called — see [Testing Bedrock](/docs/guides/testing-bedrock/) for complete examples including fault injection for retry testing.

## Why this matters

Bedrock is untestable locally without fakecloud. Real Bedrock burns tokens on every test run, hits per-account rate limits, returns non-deterministic output, and requires network access. Testing error paths (retries, fallbacks, circuit breakers) is nearly impossible because you can't reliably make real Bedrock fail. fakecloud solves all of this — free, deterministic, offline, controllable.

## Limitations

- By default (no upstream configured) the Bedrock runtime (`InvokeModel`, `Converse`, streaming) runs in deterministic offline mode — canned / echo / configurable responses with real token counting and fault injection. This is intentional for deterministic local testing; use the `FAKECLOUD_BEDROCK_ECHO` env var or the per-model override mechanism to control responses. For genuine inference, set `FAKECLOUD_BEDROCK_UPSTREAM_URL` (see [Real model inference](#real-model-inference-configurable-upstream) above) to wire the runtime to a real Anthropic, OpenAI-compatible, or Ollama endpoint.

## Source

- [`crates/fakecloud-bedrock`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-bedrock)
- [AWS Bedrock API reference](https://docs.aws.amazon.com/bedrock/latest/APIReference/welcome.html)
- [AWS Bedrock Runtime API reference](https://docs.aws.amazon.com/bedrock/latest/APIReference/API_Operations_Amazon_Bedrock_Runtime.html)
