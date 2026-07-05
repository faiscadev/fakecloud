+++
title = "AWS CodePipeline"
description = "AWS CodePipeline (codepipeline) on fakecloud: a complete 44-operation implementation (100% conformance) — pipelines and their versions, pipeline/action/rule executions, custom action types, webhooks, job polling, stage transitions, approvals, and tagging. awsJson1.1."
weight = 65
+++

fakecloud implements **AWS CodePipeline** as an awsJson1.1 service (sigv4 signing
name `codepipeline`, target prefix `CodePipeline_20150709`). All **44 operations**
ship with **100% conformance** against AWS's own Smithy model, backed by
account-partitioned state that persists across restarts in persistent mode.

CodePipeline models automated release processes as pipelines of stages, actions,
and transitions. fakecloud models the whole control plane as real, persisted
CRUD; there is no release engine, so a `StartPipelineExecution` produces an
execution that settles deterministically from `InProgress` to a terminal
`Succeeded` state across successive reads rather than actually running any
source, build, or deploy actions.

## Pipelines

`CreatePipeline` / `GetPipeline` / `UpdatePipeline` / `DeletePipeline` /
`ListPipelines` manage pipelines. A `CreatePipeline` validates the
`PipelineDeclaration` (a `name`, a `roleArn`, and at least one stage, each with a
named action carrying a well-formed `actionTypeId`), assigns `version` 1, mints
the pipeline ARN, and records `created` / `updated` metadata. `UpdatePipeline`
bumps the version and appends to the pipeline's version history, so
`GetPipeline` can resolve any prior `version` (an unknown version raises
`PipelineVersionNotFoundException`). The `artifactStore` / `artifactStores`,
`stages`, `variables`, `triggers`, `executionMode` (`QUEUED` / `SUPERSEDED` /
`PARALLEL`), and `pipelineType` (`V1` / `V2`) blocks all round-trip verbatim.

## Executions and state

`StartPipelineExecution` mints a `pipelineExecutionId` in exact AWS UUID form and
returns an execution in `InProgress`. Successive `GetPipelineExecution` /
`GetPipelineState` / `ListPipelineExecutions` reads settle it to `Succeeded` —
the same lazy-settle pattern CodeDeploy and CodeBuild use. `GetPipelineState`
projects the stored pipeline's stages onto `stageStates` with per-action
`actionStates` and the latest execution status; `StopPipelineExecution` moves a
running execution to `Stopping` (or `Stopped` when `abandon` is set).
`ListActionExecutions` and `ListRuleExecutions` return the execution history
surfaces, and `ListDeployActionExecutionTargets` covers the deploy-target read.

## Stage operations

`EnableStageTransition` / `DisableStageTransition` gate the inbound/outbound
transitions of a stage, `RetryStageExecution` and `RollbackStage` cover the
recovery actions, `OverrideStageCondition` overrides a `BEFORE_ENTRY` /
`ON_SUCCESS` condition, `PutActionRevision` supplies a source action revision,
and `PutApprovalResult` records an `Approved` / `Rejected` manual-approval
result against a stage's approval token.

## Action types, rule types, and webhooks

`CreateCustomActionType` / `DeleteCustomActionType` / `ListActionTypes` manage
custom action types (a `category` / `provider` / `version` triple with input and
output `ArtifactDetails`), `GetActionType` / `UpdateActionType` cover the newer
action-type registry, and `ListRuleTypes` returns the predefined AWS-owned rule
types. `PutWebhook` / `DeleteWebhook` / `ListWebhooks` manage webhooks (a
`PutWebhook` validates the target pipeline exists and mints the webhook URL and
ARN), and `RegisterWebhookWithThirdParty` / `DeregisterWebhookWithThirdParty`
cover the third-party registration surface.

## Jobs, third-party jobs, and tagging

`PollForJobs` / `PollForThirdPartyJobs` drain empty (no job workers participate),
and `AcknowledgeJob` / `AcknowledgeThirdPartyJob` / `GetJobDetails` /
`GetThirdPartyJobDetails` / `PutJobSuccessResult` / `PutJobFailureResult` /
`PutThirdPartyJobSuccessResult` / `PutThirdPartyJobFailureResult` validate their
inputs and report the job as not found (the honest response when no job is
outstanding). `TagResource` / `UntagResource` / `ListTagsForResource` tag
pipelines and webhooks by ARN.

## Not implemented

There is no release engine — the actual execution of source, build, test,
deploy, approval, and invoke actions (running CodeBuild projects, deploying via
CodeDeploy, invoking Lambda, gating on manual approvals) is out of scope,
matching how LocalStack Community mocks CodePipeline. Pipeline executions settle
to a terminal status synchronously, job polling drains empty, and the
action/rule-execution reads return well-formed empty result sets because no
actions actually run.
