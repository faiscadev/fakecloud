+++
title = "Amazon SWF"
description = "Amazon Simple Workflow Service on fakecloud: the full 39-operation control plane -- domains, versioned activity/workflow types, and workflow executions driven by a real decider/worker state machine (decision tasks, activity tasks, and the event history that ties them together) -- with account-partitioned persistence."
weight = 77
+++

fakecloud implements **Amazon SWF** (Simple Workflow Service, `swf`), the task
coordination service. All **39 operations** from the AWS Smithy model ship now,
backed by account-partitioned state that persists across restarts in persistent
mode. The wire protocol is awsJson1_0 (x-amz-target
`SimpleWorkflowService.<Op>`), signing as `swf`.

This is a **working state machine**, not a stubbed control plane. Every domain,
registered type, workflow execution, task token, and history event is real,
validated, persisted state -- no fake success responses. Requests are validated
against the model's `required` / `length` / `range` / `enum` constraints before
any handler runs, and an operation that dereferences a domain, type, execution,
or task token that does not exist returns SWF's `UnknownResourceFault`. A
duplicate domain returns `DomainAlreadyExistsFault`, a duplicate type
`TypeAlreadyExistsFault`, a start whose timeouts/childPolicy/taskList are defined
by neither the request nor the workflow type's registered defaults
`DefaultUndefinedFault`, and a second open execution of the same workflow id
`WorkflowExecutionAlreadyStartedFault`.

## Domains and types

`RegisterDomain` / `DeprecateDomain` / `UndeprecateDomain` / `DescribeDomain` /
`ListDomains` manage domains through their `REGISTERED` / `DEPRECATED` status,
minting the `arn:aws:swf:{region}:{account}:/domain/{name}` ARN that the tagging
operations key off. Activity and workflow types are versioned
(`name` + `version`) and go through the same
`Register` / `Deprecate` / `Undeprecate` / `Delete` / `Describe` / `List`
lifecycle; a type must be deprecated before `Delete*Type` will remove it
(`TypeNotDeprecatedFault` otherwise). A type's registered `default*`
configuration (task list, timeouts, child policy, lambda role) is stored and
echoed back verbatim by `Describe*Type`, and is used to fill in
`StartWorkflowExecution` defaults.

## The decider / worker loop

The workflow/activity/decision state machine is real and in-memory:

1. **`StartWorkflowExecution`** mints an opaque `runId`, opens the execution, and
   seeds the history with a `WorkflowExecutionStarted` event followed by a
   `DecisionTaskScheduled` so a decider's first poll has work.
2. **`PollForDecisionTask`** returns the next pending decision task for the
   requested task list together with the full ordered history, appending a
   `DecisionTaskStarted` event and handing back an opaque `taskToken`. When no
   decision task is pending it returns an empty task (no `taskToken`), exactly as
   SWF's long-poll does on timeout.
3. **`RespondDecisionTaskCompleted`** appends `DecisionTaskCompleted` and applies
   each decision, appending the matching history event(s):
   `ScheduleActivityTask` -> `ActivityTaskScheduled`,
   `CompleteWorkflowExecution` -> `WorkflowExecutionCompleted` (closes the
   execution `COMPLETED`), `FailWorkflowExecution`, `CancelWorkflowExecution`,
   `ContinueAsNewWorkflowExecution`, `RecordMarker`, `StartTimer`, `CancelTimer`,
   `RequestCancelActivityTask`, `SignalExternalWorkflowExecution`,
   `RequestCancelExternalWorkflowExecution`, `StartChildWorkflowExecution`, and
   `ScheduleLambdaFunction`.
4. **`PollForActivityTask`** hands out a scheduled activity task for the
   requested task list (with its `activityId`, `activityType`, and `input`),
   appending `ActivityTaskStarted`.
5. **`RespondActivityTaskCompleted` / `RespondActivityTaskFailed` /
   `RespondActivityTaskCanceled`** record the outcome
   (`ActivityTaskCompleted` / `Failed` / `Canceled`) and schedule a fresh
   decision task so the next `PollForDecisionTask` observes the result.
   `RecordActivityTaskHeartbeat` reports whether cancellation has been requested.

`DescribeWorkflowExecution`, `GetWorkflowExecutionHistory`,
`List`/`CountOpenWorkflowExecutions`, `List`/`CountClosedWorkflowExecutions`,
`CountPendingActivityTasks`, and `CountPendingDecisionTasks` read this state
back; `SignalWorkflowExecution`, `RequestCancelWorkflowExecution`, and
`TerminateWorkflowExecution` drive it from the outside. `TagResource` /
`UntagResource` / `ListTagsForResource` manage domain tags keyed by ARN.

## Honest gap: no autonomous clock

SWF's real service fires timers, activity task-timeouts, and execution
start-to-close timeouts on wall-clock deadlines from a managed scheduler.
fakecloud records `TimerStarted` / `StartTimer` decisions and the
`*StartToCloseTimeout` configuration faithfully, but does **not** run a
background clock that autonomously emits `TimerFired` / `ActivityTaskTimedOut` /
`WorkflowExecutionTimedOut` events -- those transitions are driven by explicit
decider/worker calls (`RespondActivityTask*`, `TerminateWorkflowExecution`,
etc.). Every other part of the state machine -- history sequencing, task
dispatch, decision application, execution close-out, pending-task counts, tags,
and persistence -- is real, account-partitioned state.
