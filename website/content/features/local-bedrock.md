+++
title = "Local Bedrock Emulation for AI Development"
description = "Develop and test LLM-powered applications locally with fakecloud's high-fidelity Bedrock emulation. Eliminate cloud costs and latency during the 'Inner Loop' of AI agent development."
template = "page.html"
+++

## Performance & Coverage
- **111 Bedrock Operations:** 100% API conformance for model invocation, agent orchestration, and knowledge bases.
- **Zero Latency:** Sub-millisecond response times for local API calls, bypassing internet round-trips.
- **Lightweight & Fast:** ~19MB standalone binary with ~500ms startup time.

## Supported AI Tools
- **Claude Code & Cursor:** Seamlessly integrate local Bedrock endpoints into your AI-assisted coding workflow. [Read the guide](/blog/aws-integration-tests-with-claude-code-cursor/).
- **LangChain & LlamaIndex:** Use standard AWS SDKs to point your RAG pipelines at `http://localhost:4566`.

## Why Local Bedrock?
- **Cost Control:** Zero per-token costs during development and integration testing.
- **Offline Ready:** Develop AI features on planes or in air-gapped environments.
- **Deterministic Testing:** Use fakecloud SDKs to assert on model invocations and agent steps.