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

## Roadmap

- **Execution engine** — real source polling (SQS / DynamoDB Streams / Kinesis)
  with EventBridge-pattern filtering, optional enrichment (Lambda / Step
  Functions / API destination), and target delivery (Lambda / SQS / SNS / Step
  Functions / EventBridge bus / Kinesis) reusing fakecloud's existing invoke and
  delivery paths.
- **CloudFormation** — `AWS::Pipes::Pipe` provisioning.
- **Terraform** — `aws_pipes_pipe` acceptance coverage.
