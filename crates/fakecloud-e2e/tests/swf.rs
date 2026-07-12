//! Amazon SWF (Simple Workflow Service) control-plane E2E.
//!
//! Exercises the real decider/worker loop against a spawned fakecloud server via
//! the AWS Rust SDK, which speaks the real awsJson1_0 wire format (x-amz-target
//! `SimpleWorkflowService.<Op>`):
//!
//!   RegisterDomain -> RegisterWorkflowType -> RegisterActivityType
//!     -> StartWorkflowExecution (mint runId)
//!     -> PollForDecisionTask (history has WorkflowExecutionStarted)
//!     -> RespondDecisionTaskCompleted [ScheduleActivityTask]
//!     -> PollForActivityTask (returns the scheduled activity + input)
//!     -> RespondActivityTaskCompleted
//!     -> PollForDecisionTask (history now shows ActivityTaskCompleted)
//!     -> RespondDecisionTaskCompleted [CompleteWorkflowExecution]
//!     -> DescribeWorkflowExecution (CLOSED / COMPLETED)
//!     -> ListClosedWorkflowExecutions (the execution is listed)
//!
//! The workflow/activity/decision state machine is real, in-memory, and
//! persisted; the history events are appended by the server exactly as SWF
//! sequences them.

mod helpers;

use aws_sdk_swf::primitives::DateTime;
use aws_sdk_swf::types::{
    ActivityType, ChildPolicy, CompleteWorkflowExecutionDecisionAttributes, Decision, DecisionType,
    EventType, ExecutionStatus, ExecutionTimeFilter, ScheduleActivityTaskDecisionAttributes,
    TaskList, WorkflowExecution, WorkflowType,
};
use helpers::TestServer;

async fn swf_client(server: &TestServer) -> aws_sdk_swf::Client {
    aws_sdk_swf::Client::new(&server.aws_config().await)
}

#[tokio::test]
async fn swf_decider_worker_loop() {
    let server = TestServer::start().await;
    let swf = swf_client(&server).await;

    let domain = "e2e-domain";
    let decision_tl = "decisions";
    let activity_tl = "activities";

    // Register the domain, workflow type (with defaults), and activity type.
    swf.register_domain()
        .name(domain)
        .workflow_execution_retention_period_in_days("1")
        .send()
        .await
        .expect("register domain");

    swf.register_workflow_type()
        .domain(domain)
        .name("order")
        .version("1.0")
        .default_task_list(TaskList::builder().name(decision_tl).build().unwrap())
        .default_child_policy(ChildPolicy::Terminate)
        .default_execution_start_to_close_timeout("3600")
        .default_task_start_to_close_timeout("60")
        .send()
        .await
        .expect("register workflow type");

    swf.register_activity_type()
        .domain(domain)
        .name("charge")
        .version("1.0")
        .send()
        .await
        .expect("register activity type");

    // Describe the domain round-trips its configuration.
    let dd = swf
        .describe_domain()
        .name(domain)
        .send()
        .await
        .expect("describe domain");
    assert_eq!(
        dd.configuration()
            .unwrap()
            .workflow_execution_retention_period_in_days(),
        "1"
    );

    // Start an execution -> a runId is minted.
    let start = swf
        .start_workflow_execution()
        .domain(domain)
        .workflow_id("order-42")
        .workflow_type(
            WorkflowType::builder()
                .name("order")
                .version("1.0")
                .build()
                .unwrap(),
        )
        .input("{\"orderId\":42}")
        .send()
        .await
        .expect("start workflow execution");
    let run_id = start.run_id().expect("run id minted").to_string();
    assert!(!run_id.is_empty());

    // Decider polls: the first decision task carries the seeded history.
    let dt = swf
        .poll_for_decision_task()
        .domain(domain)
        .task_list(TaskList::builder().name(decision_tl).build().unwrap())
        .send()
        .await
        .expect("poll decision task");
    let dtoken = dt.task_token().to_string();
    assert!(!dtoken.is_empty());
    let event_types: Vec<&EventType> = dt.events().iter().map(|e| e.event_type()).collect();
    assert!(event_types.contains(&&EventType::WorkflowExecutionStarted));
    assert!(event_types.contains(&&EventType::DecisionTaskStarted));

    // Decision: schedule the activity task.
    swf.respond_decision_task_completed()
        .task_token(dtoken)
        .decisions(
            Decision::builder()
                .decision_type(DecisionType::ScheduleActivityTask)
                .schedule_activity_task_decision_attributes(
                    ScheduleActivityTaskDecisionAttributes::builder()
                        .activity_id("charge-1")
                        .activity_type(
                            ActivityType::builder()
                                .name("charge")
                                .version("1.0")
                                .build()
                                .unwrap(),
                        )
                        .task_list(TaskList::builder().name(activity_tl).build().unwrap())
                        .input("{\"orderId\":42}")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("respond decision (schedule activity)");

    // Worker polls: the scheduled activity task comes back with its input.
    let at = swf
        .poll_for_activity_task()
        .domain(domain)
        .task_list(TaskList::builder().name(activity_tl).build().unwrap())
        .send()
        .await
        .expect("poll activity task");
    let atoken = at.task_token().to_string();
    assert!(!atoken.is_empty());
    assert_eq!(at.activity_id(), "charge-1");
    assert_eq!(at.input(), Some("{\"orderId\":42}"));

    // Worker completes the activity.
    swf.respond_activity_task_completed()
        .task_token(atoken)
        .result("{\"charged\":true}")
        .send()
        .await
        .expect("respond activity completed");

    // Next decision task shows the ActivityTaskCompleted event.
    let dt2 = swf
        .poll_for_decision_task()
        .domain(domain)
        .task_list(TaskList::builder().name(decision_tl).build().unwrap())
        .send()
        .await
        .expect("poll decision task 2");
    let dtoken2 = dt2.task_token().to_string();
    let event_types2: Vec<&EventType> = dt2.events().iter().map(|e| e.event_type()).collect();
    assert!(event_types2.contains(&&EventType::ActivityTaskCompleted));

    // Decision: complete the workflow.
    swf.respond_decision_task_completed()
        .task_token(dtoken2)
        .decisions(
            Decision::builder()
                .decision_type(DecisionType::CompleteWorkflowExecution)
                .complete_workflow_execution_decision_attributes(
                    CompleteWorkflowExecutionDecisionAttributes::builder()
                        .result("{\"charged\":true}")
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("respond decision (complete workflow)");

    // Describe: the execution is CLOSED / COMPLETED.
    let desc = swf
        .describe_workflow_execution()
        .domain(domain)
        .execution(
            WorkflowExecution::builder()
                .workflow_id("order-42")
                .run_id(&run_id)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("describe execution");
    let info = desc.execution_info().expect("execution info");
    assert_eq!(info.execution_status(), &ExecutionStatus::Closed);

    // The full history is retrievable and ends with WorkflowExecutionCompleted.
    let history = swf
        .get_workflow_execution_history()
        .domain(domain)
        .execution(
            WorkflowExecution::builder()
                .workflow_id("order-42")
                .run_id(&run_id)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("get history");
    let last = history.events().last().expect("at least one event");
    assert_eq!(last.event_type(), &EventType::WorkflowExecutionCompleted);

    // ListClosedWorkflowExecutions returns the execution.
    let closed = swf
        .list_closed_workflow_executions()
        .domain(domain)
        .start_time_filter(
            ExecutionTimeFilter::builder()
                .oldest_date(DateTime::from_secs(0))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("list closed executions");
    assert!(closed
        .execution_infos()
        .iter()
        .any(|e| e.execution().map(|x| x.run_id()) == Some(run_id.as_str())));
}
