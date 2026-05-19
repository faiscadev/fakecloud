+++
title = "LocalStack Ultimate covers 4 Bedrock ops. Here's why that's the wrong bet for tests."
date = 2026-04-22
description = "LocalStack supports Bedrock only on their Ultimate plan, and only four operations, all backed by Ollama. Here's what that gets you, what it doesn't, and why fakecloud's approach is different."

[extra]
author = "Lucas Vieira"
+++

If you've been looking for a local Bedrock emulator and landed on LocalStack's documentation, you've seen this: Bedrock is available, but only on the Ultimate plan, and only four operations are supported. Those four operations are backed by [Ollama](https://ollama.com), the local-LLM runtime.

This is a real product decision, not an accident. It's worth understanding what it gets you, what it doesn't, and why fakecloud took a very different approach.

## What LocalStack Ultimate gives you for Bedrock

Per [docs.localstack.cloud/aws/services/bedrock](https://docs.localstack.cloud/aws/services/bedrock/), as of April 2026:

- **Tier**: Ultimate plan only. This is the top paid tier.
- **Operations**: `InvokeModel`, `Converse`, `ListFoundationModels`, `CreateModelInvocationJob`. Four, total.
- **Backend**: Ollama, called via its HTTP API.
- **Models**: whatever Ollama supports — Llama, Mistral, Phi, etc. You call them via `ollama.<model-id>` strings through the Bedrock SDK.
- **Limitations per the docs**: only text models, no GPU, no persistence, and responsiveness issues on Docker Desktop due to storage/memory constraints.

That's the whole product. The "Bedrock" you're calling in your code is a LocalStack process forwarding your request to Ollama, which runs a model like Llama 3 on your CPU or GPU, and returns the response in a Bedrock-shaped envelope.

## What that actually means at the test bench

Let's be concrete. You're writing tests for code that calls `anthropic.claude-3-haiku-20240307-v1:0` (Haiku). You point your SDK at LocalStack Ultimate's Bedrock. What happens:

1. Haiku doesn't exist locally. You have to set `DEFAULT_BEDROCK_MODEL=ollama.llama3.2` (or whichever Ollama model you have installed).
2. Your code's `modelId: "anthropic.claude-3-haiku-20240307-v1:0"` string gets routed to Llama 3.2.
3. Your prompt, which was tuned for Claude, runs through Llama, which interprets it differently.
4. The response comes back. Llama's output goes through Bedrock's response envelope shape.
5. Your code parses the envelope, extracts the text, and... what?

If your code is doing structured extraction (return JSON matching schema X), Llama and Claude don't produce identical JSON even at temperature 0. If your code is doing classification, they disagree on edge cases. If your code is doing agent work with tool use, they have different tool-use protocols entirely (Claude's tool-use format differs from Llama's function-calling format).

You are not testing your Claude code. You are testing whether your Claude code happens to also work against Llama when shimmed through a Bedrock envelope.

## The slowness problem

Ollama on CPU: 1-30 seconds per call depending on model and prompt length.

Ollama on GPU (if you have one): 100ms-3s per call.

Bedrock on real AWS: 100-2000ms per call.

The "local" Bedrock is often slower than the cloud one. And you get non-determinism either way.

The LocalStack docs explicitly mention `BEDROCK_PREWARM` to reduce startup delays, which is an acknowledgment of the pain — you're essentially waiting for Ollama to load a multi-GB model into memory before your test suite can start. CI runners without `--shm-size` tuning routinely OOM during this.

## The four operations

`InvokeModel` and `Converse` are the data plane — the calls your application makes at runtime. That's the part Ollama can back.

`ListFoundationModels` returns which models exist.

`CreateModelInvocationJob` starts an async batch job against S3 input. This is partial support (based on what the docs describe) — it's the only control-plane operation covered.

What's not covered: 
- Guardrails
- Custom models / customization jobs  
- Model import
- Inference profiles
- Provisioned throughput
- Model copy jobs
- Prompt management
- Foundation model agreements
- Resource policies
- Logging configuration
- Evaluation jobs
- Marketplace
- Automated reasoning

If your test code touches anything in that list — and any serious Bedrock integration touches several of them — LocalStack Ultimate's Bedrock emulation doesn't help.

## What fakecloud does differently

fakecloud covers 111 Bedrock operations. The data plane (InvokeModel, Converse, streaming variants) is not backed by a real LLM — it returns exactly the responses you configure, per-prompt rule.

This is the deliberate opposite of LocalStack Ultimate's choice:

| | LocalStack Ultimate Bedrock | fakecloud Bedrock |
|---|---|---|
| Pricing | Top-tier paid plan | Free, AGPL-3.0 |
| Data plane | Ollama (real local LLM) | Configurable responses per prompt rule |
| Determinism | No | Yes |
| Speed | 1-30s per call | Milliseconds |
| Guardrails | Not supported | Full CRUD + real content evaluation |
| Custom models | Not supported | Full control plane |
| Async batch | Partial | Full flow |
| Prompt management | Not supported | Prompts + prompt routers |
| GPU required | Usually | No |
| Persistence | "Not supported" (from docs) | Yes |

## Why Ollama is the wrong backing for a Bedrock emulator

The word "emulator" and the word "Bedrock" together set an expectation: I can run a simulation of Bedrock locally. People reach for that when they want to test their Bedrock-calling code.

Testing code that calls an LLM API and testing an LLM are different problems. The first wants determinism, speed, fault injection, and call-history introspection. The second wants a real model.

LocalStack's Ultimate Bedrock chose the second. That decision makes sense if your goal is "let me prototype against a Bedrock SDK without burning tokens." It's the wrong decision if your goal is "let me have fast, deterministic integration tests of my production Bedrock code."

fakecloud picked the first job and optimized hard for it. If you want real inference locally, run Ollama directly — it's free, open source, and does that job excellently. If you want deterministic Bedrock behavior in your test suite, you want an emulator with configured responses.

## When to pay for LocalStack Ultimate anyway

There are reasons to pay for LocalStack Ultimate that aren't about Bedrock. Their core strength is deep coverage of the long tail of AWS services — things like ACM, Certificate Manager, Backup, Config, etc. If you rely on those in your infrastructure and you want a local mirror of them, paying for Ultimate may be worth it.

If the Bedrock emulation is the reason you were about to open a procurement ticket for Ultimate, though, stop. Bedrock-on-Ollama isn't going to solve your test problem. What will solve it is an emulator built for tests — which you can get for free.

## What to do next

If you're currently testing against real Bedrock, start with [how to test Bedrock code locally](/blog/bedrock-local-testing/) — walks through the full pattern end to end.

If you're on LocalStack Ultimate and Bedrock was the draw, look at [the migration comparison](/localstack-bedrock-alternative/).

If you want to understand why "just use Ollama" doesn't work as a test strategy, [here's a longer version of the argument](/blog/bedrock-tests-real-llm/).

## Links

- **Install fakecloud**: `curl -fsSL https://fakecloud.dev/install.sh | bash`
- **LocalStack Bedrock docs (cited)**: [docs.localstack.cloud/aws/services/bedrock](https://docs.localstack.cloud/aws/services/bedrock/)
- **Bedrock emulator landing**: [/bedrock-emulator/](/bedrock-emulator/)
- **Migration landing**: [/localstack-bedrock-alternative/](/localstack-bedrock-alternative/)
- **GitHub**: [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
