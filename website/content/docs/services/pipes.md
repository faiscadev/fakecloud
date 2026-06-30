+++
title = "EventBridge Pipes"
description = "Amazon EventBridge Pipes — point-to-point source → enrichment → target integrations. restJson1 protocol."
weight = 31
+++

Amazon EventBridge Pipes (the `pipes` service) wires one event source to one
target, with optional filtering and enrichment in between, without writing glue
code: a pipe polls a source (SQS, DynamoDB Streams, Kinesis, …), optionally
filters and enriches each event, then delivers it to a target (Lambda, SQS,
SNS, Step Functions, an EventBridge bus, …).

The wedge: no free local emulator runs Pipes faithfully. LocalStack's Pipes
support arrived late and drifts on event/error semantics; floci delivers every
message to the default region regardless of the configured one; Moto serializes
Pipes timestamps as strings, breaking the SDK. fakecloud already executes the
underlying cross-service paths (SQS/DynamoDB-stream/Kinesis polling, the
EventBridge pattern matcher, Lambda invoke, SNS/SQS/Step Functions delivery), so
Pipes is built to run real source → enrichment → target traffic on that engine.

## Supported today

Batch 1 ships the full Pipes **control plane** with a faithful lifecycle state
machine and persistence:

- **Pipe CRUD** — `CreatePipe`, `DescribePipe`, `ListPipes` (filter by
  `NamePrefix` / `DesiredState` / `CurrentState` / `SourcePrefix` /
  `TargetPrefix`, paginated by `Limit` / `NextToken`), `UpdatePipe`,
  `DeletePipe`. The source, target, role, filtering, enrichment, and target
  parameters are stored and echoed back verbatim on describe.
- **Lifecycle** — a created pipe returns `CREATING` and settles to `RUNNING`
  (or `STOPPED` when `DesiredState=STOPPED`) just like real AWS; `StartPipe`
  (`STARTING → RUNNING`) and `StopPipe` (`STOPPING → STOPPED`) flip the desired
  state, `UpdatePipe` transitions through `UPDATING`, and `DeletePipe`
  transitions through `DELETING` before the pipe disappears. A pipe caught
  mid-transition by a restart is re-driven to its settled state on boot.
- **Tags** — `TagResource`, `UntagResource`, `ListTagsForResource`, plus inline
  `Tags` on `CreatePipe`.
- **Persistence** — pipes survive a restart in persistent mode.

## Execution — real source → enrichment → target

A RUNNING pipe is executed for real by a background runner: it polls the
source, applies the pipe's `FilterCriteria` (the same EventBridge event-pattern
syntax used everywhere else in fakecloud), optionally runs the matched batch
through an enrichment, applies the target `InputTemplate`, and delivers the
result to the target — reusing the exact cross-service delivery paths the rest
of fakecloud uses.

- **Sources** — **SQS queues**, **Kinesis streams**, and **DynamoDB streams**.
  SQS acks by deleting the source message once it is filtered out or
  successfully delivered; SSE-KMS / managed-SSE source bodies are decrypted
  before forwarding, so the target sees plaintext just like a real AWS consumer.
  The stream sources keep a durable per-pipe checkpoint (persisted with the
  Pipes state, so a restart resumes instead of re-replaying the backlog) and
  honor `StartingPosition` (`TRIM_HORIZON` / `LATEST`, plus `AT_TIMESTAMP` for
  Kinesis). A delivery failure leaves the window for redelivery (at-least-once).
- **Filtering** — events matching `SourceParameters.FilterCriteria.Filters[].Pattern`
  are forwarded; non-matching events are dropped (acked), exactly as AWS Pipes
  drops filtered events.
- **Enrichment** — when `Enrichment` is a **Lambda** ARN, the matched batch is
  sent to it as a JSON array and the function's JSON return *replaces* the batch
  (0..N events) before target delivery; an enrichment that returns an empty
  array drops the batch. `EnrichmentParameters.InputTemplate` transforms each
  event before the enrichment call.
- **Input transform** — `TargetParameters.InputTemplate` is applied per event
  before delivery: `<$.json.path>` placeholders resolve against the event and
  `<aws.pipes.event.json>` expands to the whole event; a template that renders
  valid JSON is delivered as that JSON, otherwise as a string.
- **Targets** — **Lambda** (the batch is invoked as a JSON array), **SQS** and
  **SNS** (one message per event), **Step Functions** (`StartExecution` per
  event), **EventBridge bus** (`PutEvents`, with `Source`/`DetailType` from
  `TargetParameters.EventBridgeEventBusParameters`), and **Kinesis** (one record
  per event, partition key from `TargetParameters.KinesisStreamParameters`).

## CloudFormation

`AWS::Pipes::Pipe` is provisioned as a real pipe in the control plane: the
provisioner stores the same JSON shape as the direct `CreatePipe`, so the
background runner picks it up and it reads back identically on `DescribePipe`.
A CFN-created pipe is born settled (`RUNNING`, or `STOPPED` when
`DesiredState=STOPPED`); `Ref` returns the pipe name and `Fn::GetAtt` exposes
`Arn`, `CurrentState`, `StateReason`, `CreationTime`, and `LastModifiedTime`.
The pipe survives a restart in persistent mode and is removed on stack delete.

## Roadmap

- **More enrichment** — Step Functions (sync), API destination, API Gateway.
- **More targets** — ECS, Batch, Redshift Data, HTTP/API destination,
  SageMaker, CloudWatch Logs, Timestream.
- **Terraform** — `aws_pipes_pipe` acceptance coverage.
