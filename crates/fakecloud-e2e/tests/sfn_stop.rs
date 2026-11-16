//! StopExecution stops an execution's work, not only its status, as recorded
//! against AWS: nothing after the stop runs, the history ends with
//! `ExecutionAborted`, and what the execution manages (a `.sync` child, a map
//! run's children) is stopped with the cause AWS gives it.

mod helpers;

use helpers::TestServer;
use serde_json::{json, Value};
use tokio::time::{sleep, Duration};

const ROLE: &str = "arn:aws:iam::123456789012:role/sfn-role";

async fn create_machine(client: &aws_sdk_sfn::Client, name: &str, definition: Value) -> String {
    client
        .create_state_machine()
        .name(name)
        .role_arn(ROLE)
        .definition(definition.to_string())
        .send()
        .await
        .unwrap()
        .state_machine_arn()
        .to_string()
}

async fn history_types(client: &aws_sdk_sfn::Client, arn: &str) -> Vec<String> {
    client
        .get_execution_history()
        .execution_arn(arn)
        .send()
        .await
        .unwrap()
        .events()
        .iter()
        .map(|e| e.r#type().as_str().to_string())
        .collect()
}

/// Wait until `arn`'s history has an event of `event_type`.
async fn wait_for_event(client: &aws_sdk_sfn::Client, arn: &str, event_type: &str) {
    for _ in 0..200 {
        if history_types(client, arn)
            .await
            .iter()
            .any(|t| t == event_type)
        {
            return;
        }
        sleep(Duration::from_millis(20)).await;
    }
    panic!("{arn} never recorded {event_type}");
}

/// A queue, and a Task sending to it: the side effect that must not happen
/// after a stop.
async fn queue(server: &TestServer, name: &str) -> (aws_sdk_sqs::Client, String) {
    let sqs = server.sqs_client().await;
    let url = sqs
        .create_queue()
        .queue_name(name)
        .send()
        .await
        .unwrap()
        .queue_url()
        .unwrap()
        .to_string();
    (sqs, url)
}

fn send_task(queue_url: &str) -> Value {
    json!({
        "Type":"Task",
        "Resource":"arn:aws:states:::sqs:sendMessage",
        "Parameters":{"QueueUrl":queue_url,"MessageBody":"ran"},
        "End":true
    })
}

async fn received(sqs: &aws_sdk_sqs::Client, url: &str) -> usize {
    sqs.receive_message()
        .queue_url(url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap()
        .messages()
        .len()
}

// Stopped during a Wait, an execution runs nothing after it, and its history
// ends with ExecutionAborted, whatever its states were doing (recorded for a
// Wait, an inline Map and a Parallel).
#[tokio::test]
async fn a_stopped_execution_runs_nothing_more() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let (sqs, url) = queue(&server, "after-stop").await;
    let wait_then_send = json!({"StartAt":"W","States":{
        "W":{"Type":"Wait","Seconds":1,"Next":"S"},
        "S":send_task(&url)
    }});
    let cases = [
        ("stop-wait", wait_then_send.clone(), "WaitStateEntered"),
        (
            "stop-map",
            json!({"StartAt":"M","States":{"M":{"Type":"Map","ItemProcessor":wait_then_send.clone(),"End":true}}}),
            "WaitStateEntered",
        ),
        (
            "stop-parallel",
            json!({"StartAt":"P","States":{"P":{"Type":"Parallel","Branches":[wait_then_send.clone()],"End":true}}}),
            "WaitStateEntered",
        ),
    ];
    let mut stopped = Vec::new();
    for (name, definition, running_event) in cases {
        let machine = create_machine(&client, name, definition).await;
        let arn = client
            .start_execution()
            .state_machine_arn(&machine)
            .input("[1]")
            .send()
            .await
            .unwrap()
            .execution_arn()
            .to_string();
        wait_for_event(&client, &arn, running_event).await;
        client
            .stop_execution()
            .execution_arn(&arn)
            .error("UserErr")
            .cause("user cause")
            .send()
            .await
            .unwrap();
        stopped.push((name, arn));
    }
    // Long past every Wait.
    sleep(Duration::from_millis(2500)).await;
    assert_eq!(received(&sqs, &url).await, 0, "a stopped execution sent");
    for (name, arn) in stopped {
        let history = client
            .get_execution_history()
            .execution_arn(&arn)
            .send()
            .await
            .unwrap();
        let last = history.events().last().unwrap();
        assert_eq!(last.r#type().as_str(), "ExecutionAborted", "{name}");
        let details = last.execution_aborted_event_details().unwrap();
        assert_eq!(
            (details.error(), details.cause()),
            (Some("UserErr"), Some("user cause")),
            "{name}"
        );
        let types = history_types(&client, &arn).await;
        assert!(
            !types.iter().any(|t| t == "WaitStateExited"),
            "{name}: {types:?}"
        );
    }
}

// A `.sync` child stops with the parent that waits for it, with the cause AWS
// gives it.
#[tokio::test]
async fn stopping_the_parent_stops_its_sync_child() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let child = create_machine(
        &client,
        "sync-child",
        json!({"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":30,"End":true}}}),
    )
    .await;
    let parent = create_machine(
        &client,
        "sync-parent",
        json!({"StartAt":"T","States":{"T":{
            "Type":"Task",
            "Resource":"arn:aws:states:::states:startExecution.sync:2",
            "Parameters":{"StateMachineArn":child},
            "End":true
        }}}),
    )
    .await;
    let parent_arn = client
        .start_execution()
        .state_machine_arn(&parent)
        .send()
        .await
        .unwrap()
        .execution_arn()
        .to_string();
    let mut child_arn = None;
    for _ in 0..200 {
        let listed = client
            .list_executions()
            .state_machine_arn(&child)
            .send()
            .await
            .unwrap();
        if let Some(execution) = listed.executions().first() {
            child_arn = Some(execution.execution_arn().to_string());
            break;
        }
        sleep(Duration::from_millis(20)).await;
    }
    let child_arn = child_arn.expect("the child started");
    client
        .stop_execution()
        .execution_arn(&parent_arn)
        .send()
        .await
        .unwrap();
    let mut described = None;
    for _ in 0..100 {
        let d = client
            .describe_execution()
            .execution_arn(&child_arn)
            .send()
            .await
            .unwrap();
        if d.status().as_str() != "RUNNING" {
            described = Some(d);
            break;
        }
        sleep(Duration::from_millis(20)).await;
    }
    let described = described.expect("the child stopped");
    assert_eq!(described.status().as_str(), "ABORTED");
    assert_eq!(described.error(), None);
    assert_eq!(
        described.cause(),
        Some(
            format!(
                "The Task state in AWS Step Functions execution [{parent_arn}] which was managing this resource was aborted"
            )
            .as_str()
        )
    );
    let types = history_types(&client, &child_arn).await;
    assert_eq!(types.last().map(String::as_str), Some("ExecutionAborted"));
}

// A map run's children stop with their parent, with the cause AWS gives them,
// and count as results written.
#[tokio::test]
async fn stopped_map_run_children_name_their_map_run() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let (sqs, url) = queue(&server, "after-map-stop").await;
    let machine = create_machine(
        &client,
        "stop-dmap",
        json!({"StartAt":"M","States":{"M":{
            "Type":"Map",
            "ItemProcessor":{
                "ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},
                "StartAt":"W",
                "States":{"W":{"Type":"Wait","Seconds":1,"Next":"S"},"S":send_task(&url)}
            },
            "End":true
        }}}),
    )
    .await;
    let parent = client
        .start_execution()
        .state_machine_arn(&machine)
        .input("[1]")
        .send()
        .await
        .unwrap()
        .execution_arn()
        .to_string();
    let mut run = None;
    for _ in 0..200 {
        let runs = client
            .list_map_runs()
            .execution_arn(&parent)
            .send()
            .await
            .unwrap();
        if let Some(r) = runs.map_runs().first() {
            let arn = r.map_run_arn().to_string();
            let children = client
                .list_executions()
                .map_run_arn(&arn)
                .send()
                .await
                .unwrap();
            if !children.executions().is_empty() {
                run = Some((arn, children.executions()[0].execution_arn().to_string()));
                break;
            }
        }
        sleep(Duration::from_millis(20)).await;
    }
    let (run_arn, child_arn) = run.expect("a child started");
    client
        .stop_execution()
        .execution_arn(&parent)
        .send()
        .await
        .unwrap();
    sleep(Duration::from_millis(1500)).await;
    assert_eq!(received(&sqs, &url).await, 0, "a stopped child sent");

    let child = client
        .describe_execution()
        .execution_arn(&child_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(child.status().as_str(), "ABORTED");
    assert_eq!(child.error(), None);
    assert_eq!(
        child.cause(),
        Some(
            format!(
                "The map run execution [{run_arn}] which was managing this resource was aborted"
            )
            .as_str()
        )
    );
    let counts = client
        .describe_map_run()
        .map_run_arn(&run_arn)
        .send()
        .await
        .unwrap();
    let c = counts.execution_counts().unwrap();
    assert_eq!(
        (c.aborted(), c.running(), c.total(), c.results_written()),
        (1, 0, 1, 1)
    );
    // The parent's history records no map run abort of its own.
    let types = history_types(&client, &parent).await;
    assert_eq!(types.last().map(String::as_str), Some("ExecutionAborted"));
    assert!(!types.iter().any(|t| t == "MapRunAborted"), "{types:?}");
}
