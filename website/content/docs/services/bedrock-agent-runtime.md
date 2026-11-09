+++
title = "Bedrock Agent Runtime"
description = "InvokeAgent, Retrieve, RetrieveAndGenerate, flow execution, sessions, memory — Bedrock Agent data plane with real eventstream framing."
weight = 23
+++

fakecloud implements the **Bedrock Agent Runtime** data plane: agent invocation, knowledge-base retrieval, retrieve-and-generate, flow execution, session and memory management, prompt optimization, and reranking. Streaming responses use **real AWS eventstream framing** (prelude, headers, payload, CRCs) so any AWS SDK can parse the response without special-casing the emulator.

For the control plane (CreateAgent, knowledge bases, action groups, flows), see [Bedrock Agent](/docs/services/bedrock-agent/).

## Operations

### Agent invocation

- **InvokeAgent** — `POST /agents/{agentId}/agentAliases/{agentAliasId}/sessions/{sessionId}/text`. Returns `application/vnd.amazon.eventstream` with real framed events: `chunk` (assistant text), `trace` (orchestration / pre-processing / post-processing / knowledge-base lookup / action-group invocation), and optional `returnControl` (when an action group is declared `RETURN_CONTROL`).
- **InvokeInlineAgent** — pass the agent definition in the request, no `agentId` needed. Same eventstream contract as InvokeAgent.
- **InvokeFlow**: execute a prepared flow. Runs the flow by walking its connections, evaluating condition nodes; nodes that would call a model or service reply synthetically. Eventstream of `flowTraceEvent` with `enableTrace`, `flowOutputEvent` from each `Output` node reached, then `flowCompletionEvent`.

### Retrieval

- **Retrieve** — query a knowledge base, returns `retrievalResults[]` with `content`, `location`, `score`, `metadata`.
- **RetrieveAndGenerate** — non-streaming RAG. Combines retrieval with a model call and returns `output.text`, `citations[]`, `sessionId`, `guardrailAction`.
- **RetrieveAndGenerateStream**: streaming variant. Eventstream of `output` and `citation` events, plus `guardrail` when a guardrail is configured: the configured Bedrock guardrail really evaluates the query and the answer.
- **Rerank** — re-score a list of sources against a query using a reranker model.

### Flow execution (long-running flows)

- StartFlowExecution, StopFlowExecution
- GetFlowExecution, ListFlowExecutions, ListFlowExecutionEvents
- GetExecutionFlowSnapshot

### Sessions

- CreateSession, GetSession, UpdateSession, ListSessions, EndSession, DeleteSession
- CreateInvocation, ListInvocations
- PutInvocationStep, GetInvocationStep, ListInvocationSteps

### Memory

- GetAgentMemory — return persisted memory (`SESSION_SUMMARY`) for an agent
- DeleteAgentMemory

### Misc

- OptimizePrompt — rewrite a prompt for a target model (eventstream)
- GenerateQuery — generate a structured query for a knowledge base
- TagResource, UntagResource, ListTagsForResource

## Protocol

REST + JSON for non-streaming ops, REST + `application/vnd.amazon.eventstream` for InvokeAgent / InvokeFlow / InvokeInlineAgent / RetrieveAndGenerateStream / OptimizePrompt.

## Eventstream framing

The eventstream encoder produces AWS-compliant binary frames: 12-byte prelude (total length, headers length, prelude CRC32), header block, payload, and trailing message CRC32. Both `:event-type` and `:content-type` headers are emitted per frame so SDKs decode the same shapes they decode against real AWS. This means streaming tests work end-to-end through `boto3`, `aws-sdk-js`, `aws-sdk-go-v2`, and the Java/Kotlin SDKs without mocking the body decoder.

## State model

- InvokeAgent canned responses default to a single-chunk echo of the input, with traces for `preProcessingTrace`, `orchestrationTrace`, and (when knowledge bases are attached) `knowledgeBaseLookupTrace`. The first call on a fresh session returns a `sessionId` matching the path segment.
- RetrieveAndGenerate canned responses include a citation back to the first knowledge-base document returned by Retrieve, so RAG round-trip tests assert on citation plumbing.
- Sessions persist `sessionMetadata` and `encryptionKeyArn`. Invocation steps persist the verbatim payload. `DeleteSession` also removes the session's invocations, invocation steps and tags.
- State is account-partitioned. In persistent mode (`--storage-mode persistent`) sessions, invocations, invocation steps, flow executions and tags are snapshotted to `<data-path>/bedrock-agent-runtime/snapshot.json` on every mutation and reloaded on startup; the introspection invocation log below resets on restart.

## Limitations

- No real model inference. Responses come from the same configurable-response / echo path documented under [Bedrock](/docs/services/bedrock/).
- No real vector retrieval. Retrieve and RetrieveAndGenerate return synthetic results derived from the data sources attached to the knowledge base.
- Action group Lambda executors are not invoked; the runtime emits a `RETURN_CONTROL` event so test code asserting on the agent's action-group contract receives the same shape it would receive from a real `RETURN_CONTROL` action group.

## Introspection

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/bedrock-agent-runtime/invocations` | GET | List every InvokeAgent / InvokeInlineAgent / InvokeFlow / Retrieve / RetrieveAndGenerate / CreateInvocation call the runtime has seen since the last reset. |

```sh
curl http://localhost:4566/_fakecloud/bedrock-agent-runtime/invocations
```

Each row carries `{invocationId, op, agentId?, flowId?, sessionId?, input, output, outputChunks, trace?, citations[], invokedAt, durationMs}`. `op` is one of `invoke_agent`, `invoke_inline_agent`, `invoke_flow`, `retrieve`, `retrieve_and_generate`, `create_invocation`.

Wrapped by `getInvocations()` on the `bedrockAgentRuntime` sub-client in every first-party SDK.

## Source

- [`crates/fakecloud-bedrock-agent-runtime`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-bedrock-agent-runtime)
- [AWS Bedrock Agent Runtime API reference](https://docs.aws.amazon.com/bedrock/latest/APIReference/API_Operations_Agents_for_Amazon_Bedrock_Runtime.html)
