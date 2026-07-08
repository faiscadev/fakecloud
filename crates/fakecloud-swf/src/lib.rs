//! Amazon SWF (`swf`) awsJson1_0 service for fakecloud.
//!
//! The full 39-operation Amazon Simple Workflow Service Smithy model: domains
//! (`RegisterDomain` / `DeprecateDomain` / `UndeprecateDomain` / `DescribeDomain`
//! / `ListDomains`, with `REGISTERED` / `DEPRECATED` status), versioned activity
//! and workflow types (`Register*` / `Deprecate*` / `Undeprecate*` / `Delete*` /
//! `Describe*` / `List*Types`, configuration echoed back verbatim), workflow
//! executions (`StartWorkflowExecution` mints a `runId` and opens the execution;
//! `DescribeWorkflowExecution`, `GetWorkflowExecutionHistory`,
//! `List`/`CountOpen`/`ClosedWorkflowExecutions`, `SignalWorkflowExecution`,
//! `RequestCancelWorkflowExecution`, `TerminateWorkflowExecution`), the pending
//! task counts (`CountPendingActivityTasks` / `CountPendingDecisionTasks`), and
//! ARN-keyed domain tagging (`TagResource` / `UntagResource` /
//! `ListTagsForResource`).
//!
//! Requests carry `X-Amz-Target: SimpleWorkflowService.<Operation>`; dispatch
//! keys off `req.action`. Every operation runs model-driven input validation
//! first (required / length / range / enum), then real, account-partitioned,
//! persisted behavior.
//!
//! The decider/worker state machine is real and in-memory:
//! `StartWorkflowExecution` seeds a `WorkflowExecutionStarted` event followed by
//! a `DecisionTaskScheduled`; `PollForDecisionTask` hands back the next pending
//! decision task with the full history (appending `DecisionTaskStarted`);
//! `RespondDecisionTaskCompleted` applies each decision (`ScheduleActivityTask`,
//! `CompleteWorkflowExecution`, `FailWorkflowExecution`, `StartTimer`,
//! `RecordMarker`, ...) and appends the matching history events;
//! `PollForActivityTask` hands out the scheduled activity task (appending
//! `ActivityTaskStarted`); `RespondActivityTask{Completed,Failed,Canceled}`
//! records the outcome and schedules a fresh decision task so the next
//! `PollForDecisionTask` observes the result; a `CompleteWorkflowExecution`
//! decision closes the execution, which then appears in the closed-execution
//! listings and counts. Task tokens index back to their owning execution so a
//! `Respond*` / `RecordActivityTaskHeartbeat` resolves in O(1).
//!
//! Honest timer gap: SWF's real service fires timers, task-timeouts, and
//! execution-timeouts on wall-clock deadlines from a managed scheduler.
//! fakecloud records `TimerStarted` / `StartTimer` decisions and the
//! `*StartToCloseTimeout` configuration faithfully, but does not run a
//! background clock that autonomously fires `TimerFired` / `*TimedOut` events;
//! those transitions are driven by explicit decider/worker calls. Every other
//! part of the state machine -- history, task dispatch, decision application,
//! execution close-out, counts, tags, and persistence -- is real.

pub mod persistence;
pub mod service;
pub mod shared;
pub mod state;
mod validate;

pub use service::{SwfService, SWF_ACTIONS};
pub use state::{SharedSwfState, SwfData, SwfSnapshot, SWF_SNAPSHOT_SCHEMA_VERSION};
