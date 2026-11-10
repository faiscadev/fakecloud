+++
title = "Bedrock Agent"
description = "Agents, knowledge bases, action groups, flows, prompts, collaborators — full Bedrock Agent control plane."
weight = 22
+++

fakecloud implements the **Bedrock Agent** control plane: agents, agent aliases and versions, knowledge bases, data sources, ingestion jobs, action groups, agent collaborators, flows (with versions and aliases), prompts, and tag management.

Bedrock Agent is a separate AWS service from Bedrock — it has its own endpoint (`bedrock-agent.<region>.amazonaws.com`) and its own SDK client (`boto3.client("bedrock-agent")`). For the model invocation surface, see [Bedrock](/docs/services/bedrock/). For the agent invocation surface, see [Bedrock Agent Runtime](/docs/services/bedrock-agent-runtime/).

## Operations

### Agents

- **CRUD** — CreateAgent, GetAgent, ListAgents, UpdateAgent, DeleteAgent
- **PrepareAgent** — transition `NOT_PREPARED` -> `PREPARING` -> `PREPARED`
- **Versions** — GetAgentVersion, ListAgentVersions, DeleteAgentVersion
- **Aliases** — CreateAgentAlias, GetAgentAlias, ListAgentAliases, UpdateAgentAlias, DeleteAgentAlias

### Knowledge bases

- **CRUD** — CreateKnowledgeBase, GetKnowledgeBase, ListKnowledgeBases, UpdateKnowledgeBase, DeleteKnowledgeBase
- **Data sources** — CreateDataSource, GetDataSource, ListDataSources, UpdateDataSource, DeleteDataSource
- **Ingestion jobs** — StartIngestionJob, GetIngestionJob, ListIngestionJobs, StopIngestionJob
- **Documents** — DeleteKnowledgeBaseDocuments, GetKnowledgeBaseDocuments, ListKnowledgeBaseDocuments

### Action groups

- CreateAgentActionGroup, GetAgentActionGroup, ListAgentActionGroups, UpdateAgentActionGroup, DeleteAgentActionGroup
- Supports Lambda executor, OpenAPI schema, function schema, and `RETURN_CONTROL` action groups
- Parent action group signatures (`AMAZON.UserInput`, `AMAZON.CodeInterpreter`) accepted

### Agent <-> knowledge base wiring

- AssociateAgentKnowledgeBase, DisassociateAgentKnowledgeBase
- GetAgentKnowledgeBase, ListAgentKnowledgeBases, UpdateAgentKnowledgeBase

### Agent collaborators (multi-agent)

- AssociateAgentCollaborator, DisassociateAgentCollaborator
- GetAgentCollaborator, ListAgentCollaborators, UpdateAgentCollaborator

### Flows

- **CRUD** — CreateFlow, GetFlow, ListFlows, UpdateFlow, DeleteFlow, PrepareFlow
- **Versions** — CreateFlowVersion, GetFlowVersion, ListFlowVersions, DeleteFlowVersion
- **Aliases** — CreateFlowAlias, GetFlowAlias, ListFlowAliases, UpdateFlowAlias, DeleteFlowAlias
- **Validation**: ValidateFlowDefinition, with the same findings PrepareFlow records

### Prompts

- CreatePrompt, GetPrompt, ListPrompts, UpdatePrompt, DeletePrompt
- Variants with model overrides, inference configuration, and template variables

### Tags

- TagResource, UntagResource, ListTagsForResource — on agents, agent aliases, knowledge bases, data sources, flows, flow aliases, prompts

## Protocol

REST + JSON. Path-based routing; identifiers are URL-safe strings minted by fakecloud (no AWS-side validation enforced on caller-supplied IDs).

## State model

- Agents start `NOT_PREPARED`. `PrepareAgent` advances to `PREPARED` (and immediately becomes invokable from Bedrock Agent Runtime). `DRAFT` version is implicit; explicit versions are created on first alias attached to `DRAFT`.
- Knowledge bases start `CREATING` and transition to `ACTIVE` on next describe. Ingestion jobs follow `STARTING -> IN_PROGRESS -> COMPLETE`.
- Data sources transition the same way and persist their `vectorIngestionConfiguration` verbatim.
- Flows start `NotPrepared`. `PrepareFlow` answers `Preparing` and validates the definition the way `ValidateFlowDefinition` does (missing Input/Output nodes, missing or unknown node inputs/outputs, unfed or multiply fed inputs, unknown connection ends, missing connection configuration, incompatible data types, duplicate connections, cycles, unreachable nodes, with AWS's messages and severities). Any `Error` finding leaves the flow `Failed`; warnings alone leave it `Prepared`. GetFlow reports the findings as `validations` until the draft is updated, which returns it to `NotPrepared`. Preparing a flow with no definition is a `ValidationException`. Only a `Prepared` draft can be versioned (`ConflictException` otherwise), aliases must route to a numbered version (not `DRAFT`), a definition holds at most one Input node, and node names must be unique.

## Limitations

- Embedding pipelines are not executed; ingestion jobs report success without indexing real content.
- Action group Lambda executors and OpenAPI schemas are stored verbatim and returned by the runtime in `RETURN_CONTROL` traces; the agent does not actually call Lambda for action group execution.
- Prompt evaluation is not performed; prompts are stored and returned for SDK round-trip tests.
- Flow validation does not report `UnsatisfiedConnectionConditions` (conditional paths that can never run), and only checks the required inputs/outputs of Input and Output nodes.

For the data plane (InvokeAgent, Retrieve, RetrieveAndGenerate), see [Bedrock Agent Runtime](/docs/services/bedrock-agent-runtime/).

## Introspection

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/bedrock-agent/agents` | GET | List every agent with aliases, versions, attached knowledge bases, and collaborators flattened into one row each. |

```sh
curl http://localhost:4566/_fakecloud/bedrock-agent/agents
```

The response shape is `{agents: [{agentId, agentName, agentArn, agentStatus, foundationModel, instruction, knowledgeBases[], actionGroups[], collaborators[], aliases[], versions[], promptOverrides, createdAt, updatedAt}]}`. `actionGroups` is currently always `[]` because action-group creation does not yet persist state -- the field is exposed for forward compatibility so callers don't have to migrate their assertion code later.

Wrapped by `getAgents()` on the `bedrockAgent` sub-client in every first-party SDK.

## Source

- [`crates/fakecloud-bedrock-agent`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-bedrock-agent)
- [AWS Bedrock Agent API reference](https://docs.aws.amazon.com/bedrock/latest/APIReference/API_Operations_Agents_for_Amazon_Bedrock.html)
