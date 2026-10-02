+++
title = "Step Functions"
description = "Full ASL interpreter, cross-service task integrations, execution history."
weight = 19
+++

fakecloud implements **37 of 37** Step Functions operations at 100% Smithy conformance, including a **full ASL (Amazon States Language) interpreter**.

## Supported features

- **State machines** — CRUD, versions, aliases, tags. Both `STANDARD` and `EXPRESS` types.
- **Executions** — `StartExecution` (async), `StartSyncExecution` (real synchronous Express runner that blocks until the state machine terminates and returns the final output), `StopExecution`, `DescribeExecution`, `GetExecutionHistory`.
- **ASL states** — Pass, Task, Choice, Wait, Parallel, Map, Succeed, Fail
- **Retry and Catch** — full retry logic with exponential backoff, catch clauses with error filters
- **JSONPath and JSONata** — both query languages for state input/output
- **JSONPath paths**: the full JSONPath syntax AWS accepts (`$['key']`, `[-1]`, `[*]`, slices, `..` deep scan, `[?(@.x > 1)]` filters) in `InputPath` / `OutputPath` / `ItemsPath` / `Parameters` / `ResultSelector` / `ItemSelector` and Choice rules. A path that matches nothing fails the execution with `States.Runtime`, as on AWS (only `IsPresent` may probe a missing field)
- **Choice rules**: every comparator and its `*Path` form (String/Numeric/Timestamp Equals, LessThan, GreaterThan, LessThanEquals, GreaterThanEquals, BooleanEquals, StringMatches), the `Is*` type tests and `And` / `Or` / `Not`
- **Intrinsic functions**: all `States.*` functions, nested up to 10 levels (`States.ArrayGetItem(States.StringSplit($.s, ','), 0)`) with the `\'`, `\{`, `\}`, `\\` string escapes; a failing intrinsic fails the state with `States.IntrinsicFailure`
- **Task integrations** — Lambda (`invoke`, `invoke.waitForTaskToken`), SQS (`sendMessage`), SNS (`publish`), EventBridge (`putEvents`), DynamoDB (`getItem` / `putItem` / `updateItem` / `deleteItem`), and **nested Step Functions** (`states:startExecution` async + `states:startExecution.sync` blocking) — `.sync` waits for the child execution to terminate and propagates its output or failure to the parent.
- **Map state** — parallel item processing with concurrency control. Supports **distributed mode** (`ItemProcessor.ProcessorConfig.Mode=DISTRIBUTED`) with `MaxConcurrency`, `ToleratedFailurePercentage`, and `ItemReader` over S3 (JSON/JSONL/CSV manifests).
- **Parallel state** — concurrent branch execution
- **Activities** — real worker pool. A `Task` state with an activity ARN inserts a pending task; `GetActivityTask` long-polls (up to `FAKECLOUD_SFN_GET_ACTIVITY_TIMEOUT_SECS`, default 5s) for the next pending task; `SendTaskSuccess` / `SendTaskFailure` resolve the workflow. `HeartbeatSeconds` and `TimeoutSeconds` are enforced, and tasks dispatched past their timeout are reaped automatically.
- **Express logging** — `EXPRESS` state machines with `LoggingConfiguration.Level=ALL|ERROR|FATAL` emit `ExecutionStarted` / `ExecutionSucceeded` / `ExecutionFailed` events into the configured CloudWatch Logs destination.
- **State machine execution history** — full event log per execution
- **Error handling** — `States.ALL`, `States.TaskFailed`, `States.Timeout`, `States.HeartbeatTimeout`, custom error names

## Protocol

JSON protocol. `X-Amz-Target` header, JSON body, JSON responses.

## Introspection

- `GET /_fakecloud/stepfunctions/executions` — list all executions with status, input, output, and timestamps
- `POST /_fakecloud/stepfunctions/enqueue-activity-task` — directly insert a pending activity task (skipping a state-machine execution). Body: `{"activityArn": "...", "input": "{}", "heartbeatSeconds": 60, "timeoutSeconds": 300}`. Returns `{"taskToken": "..."}`. Useful for testing worker pool clients without authoring a full ASL workflow.

## Cross-service delivery

- **Step Functions -> Lambda / SQS / SNS / EventBridge / DynamoDB** — Task states invoke functions, send messages, publish events, and read/write items
- **Step Functions -> Step Functions** — Nested executions via `states:startExecution` and `states:startExecution.sync`. The `.sync` variant blocks the parent until the child reaches a terminal state, surfacing the child's output or failure (`States.TaskFailed` with child error/cause) back to the parent task.
- **Step Functions -> CloudWatch Logs** — Express state machine execution events stream to the configured log group.

## Why this matters

Most emulators stub Step Functions or support a tiny subset of the ASL spec. fakecloud runs the full interpreter — your state machine definitions execute with real branching, retries, catches, and task integrations. This lets you test orchestration logic end-to-end without touching real AWS.

## Source

- [`crates/fakecloud-stepfunctions`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-stepfunctions)
- [AWS Step Functions API reference](https://docs.aws.amazon.com/step-functions/latest/apireference/Welcome.html)
- [Amazon States Language spec](https://states-language.net/spec.html)
