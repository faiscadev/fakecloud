mod helpers;

use aws_sdk_sfn::types::Tag;
use helpers::TestServer;
use serde_json::json;
use tokio::time::{sleep, Duration};

fn simple_definition() -> String {
    serde_json::json!({
        "StartAt": "Hello",
        "States": {
            "Hello": {
                "Type": "Pass",
                "Result": "Hello, World!",
                "End": true
            }
        }
    })
    .to_string()
}

fn two_step_definition() -> String {
    serde_json::json!({
        "StartAt": "First",
        "States": {
            "First": {
                "Type": "Pass",
                "Result": "step1",
                "Next": "Second"
            },
            "Second": {
                "Type": "Pass",
                "Result": "step2",
                "End": true
            }
        }
    })
    .to_string()
}

#[tokio::test]
async fn sfn_create_describe_delete_state_machine() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("test-sm")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let arn = create.state_machine_arn();
    assert!(arn.contains("stateMachine:test-sm"));
    let _ = create.creation_date(); // DateTime - just verify it's accessible

    // Describe
    let describe = client
        .describe_state_machine()
        .state_machine_arn(arn)
        .send()
        .await
        .unwrap();
    assert_eq!(describe.name(), "test-sm");
    assert_eq!(describe.state_machine_arn(), arn);
    assert_eq!(describe.status().unwrap().as_str(), "ACTIVE");
    assert_eq!(describe.r#type().as_str(), "STANDARD");
    assert!(describe.definition().contains("Hello"));
    assert_eq!(
        describe.role_arn(),
        "arn:aws:iam::123456789012:role/test-role"
    );

    // Delete
    client
        .delete_state_machine()
        .state_machine_arn(arn)
        .send()
        .await
        .unwrap();

    // Describe after delete should fail
    let err = client
        .describe_state_machine()
        .state_machine_arn(arn)
        .send()
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn sfn_list_state_machines() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    // Create 3 state machines
    for name in &["alpha-sm", "beta-sm", "gamma-sm"] {
        client
            .create_state_machine()
            .name(*name)
            .definition(simple_definition())
            .role_arn("arn:aws:iam::123456789012:role/test-role")
            .send()
            .await
            .unwrap();
    }

    let list = client.list_state_machines().send().await.unwrap();
    let machines = list.state_machines();
    assert_eq!(machines.len(), 3);

    // Should be sorted by name
    let names: Vec<&str> = machines.iter().map(|m| m.name()).collect();
    assert_eq!(names, vec!["alpha-sm", "beta-sm", "gamma-sm"]);

    // Each should have the correct type
    for m in machines {
        assert_eq!(m.r#type().as_str(), "STANDARD");
        let _ = m.creation_date();
    }
}

#[tokio::test]
async fn sfn_list_state_machines_pagination() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    for i in 0..5 {
        client
            .create_state_machine()
            .name(format!("sm-{i:02}"))
            .definition(simple_definition())
            .role_arn("arn:aws:iam::123456789012:role/test-role")
            .send()
            .await
            .unwrap();
    }

    // Page 1: 2 items
    let page1 = client
        .list_state_machines()
        .max_results(2)
        .send()
        .await
        .unwrap();
    assert_eq!(page1.state_machines().len(), 2);
    assert!(page1.next_token().is_some());

    // Page 2
    let page2 = client
        .list_state_machines()
        .max_results(2)
        .next_token(page1.next_token().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(page2.state_machines().len(), 2);
    assert!(page2.next_token().is_some());

    // Page 3 (last)
    let page3 = client
        .list_state_machines()
        .max_results(2)
        .next_token(page2.next_token().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(page3.state_machines().len(), 1);
    assert!(page3.next_token().is_none());
}

#[tokio::test]
async fn sfn_update_state_machine() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("update-test")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();
    let arn = create.state_machine_arn();

    // Update definition
    let update = client
        .update_state_machine()
        .state_machine_arn(arn)
        .definition(two_step_definition())
        .send()
        .await
        .unwrap();
    let _ = update.update_date(); // DateTime - just verify it's accessible

    // Verify update
    let describe = client
        .describe_state_machine()
        .state_machine_arn(arn)
        .send()
        .await
        .unwrap();
    assert!(describe.definition().contains("First"));
    assert!(describe.definition().contains("Second"));
}

#[tokio::test]
async fn sfn_create_express_state_machine() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("express-sm")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .r#type(aws_sdk_sfn::types::StateMachineType::Express)
        .send()
        .await
        .unwrap();

    let describe = client
        .describe_state_machine()
        .state_machine_arn(create.state_machine_arn())
        .send()
        .await
        .unwrap();
    assert_eq!(describe.r#type().as_str(), "EXPRESS");
}

#[tokio::test]
async fn sfn_create_duplicate_name_fails() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    client
        .create_state_machine()
        .name("dup-sm")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let err = client
        .create_state_machine()
        .name("dup-sm")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn sfn_delete_nonexistent_succeeds() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    // AWS returns success for deleting non-existent state machines
    client
        .delete_state_machine()
        .state_machine_arn("arn:aws:states:us-east-1:123456789012:stateMachine:nonexistent")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn sfn_tag_resource() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("tagged-sm")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();
    let arn = create.state_machine_arn();

    // Tag the resource
    client
        .tag_resource()
        .resource_arn(arn)
        .tags(Tag::builder().key("env").value("prod").build())
        .tags(Tag::builder().key("team").value("backend").build())
        .send()
        .await
        .unwrap();

    // List tags
    let tags = client
        .list_tags_for_resource()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
    let tag_list = tags.tags();
    assert_eq!(tag_list.len(), 2);

    let env_tag = tag_list.iter().find(|t| t.key() == Some("env")).unwrap();
    assert_eq!(env_tag.value(), Some("prod"));

    let team_tag = tag_list.iter().find(|t| t.key() == Some("team")).unwrap();
    assert_eq!(team_tag.value(), Some("backend"));
}

#[tokio::test]
async fn sfn_untag_resource() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("untag-sm")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();
    let arn = create.state_machine_arn();

    // Add tags
    client
        .tag_resource()
        .resource_arn(arn)
        .tags(Tag::builder().key("env").value("prod").build())
        .tags(Tag::builder().key("team").value("backend").build())
        .tags(Tag::builder().key("version").value("1").build())
        .send()
        .await
        .unwrap();

    // Remove one tag
    client
        .untag_resource()
        .resource_arn(arn)
        .tag_keys("team")
        .send()
        .await
        .unwrap();

    // Verify remaining
    let tags = client
        .list_tags_for_resource()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
    let tag_list = tags.tags();
    assert_eq!(tag_list.len(), 2);
    assert!(tag_list.iter().any(|t| t.key() == Some("env")));
    assert!(tag_list.iter().any(|t| t.key() == Some("version")));
    assert!(!tag_list.iter().any(|t| t.key() == Some("team")));
}

#[tokio::test]
async fn sfn_create_with_tags() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("create-tagged-sm")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .tags(Tag::builder().key("created-by").value("test").build())
        .send()
        .await
        .unwrap();

    let tags = client
        .list_tags_for_resource()
        .resource_arn(create.state_machine_arn())
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tags().len(), 1);
    assert_eq!(tags.tags()[0].key(), Some("created-by"));
    assert_eq!(tags.tags()[0].value(), Some("test"));
}

#[tokio::test]
async fn sfn_create_with_invalid_definition_fails() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    // Not valid JSON
    let err = client
        .create_state_machine()
        .name("bad-def")
        .definition("not json")
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await;
    assert!(err.is_err());

    // Valid JSON but missing StartAt
    let err = client
        .create_state_machine()
        .name("bad-def2")
        .definition(r#"{"States": {"A": {"Type": "Pass", "End": true}}}"#)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await;
    assert!(err.is_err());

    // Valid JSON but missing States
    let err = client
        .create_state_machine()
        .name("bad-def3")
        .definition(r#"{"StartAt": "A"}"#)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn sfn_health_includes_states() {
    let server = TestServer::start().await;
    let url = format!("{}/_fakecloud/health", server.endpoint());
    let resp: serde_json::Value = reqwest::get(&url).await.unwrap().json().await.unwrap();
    let services = resp["services"].as_array().unwrap();
    assert!(services.iter().any(|s| s.as_str() == Some("states")));
}

#[tokio::test]
async fn sfn_reset_clears_state() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    client
        .create_state_machine()
        .name("reset-sm")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    // Verify it exists
    let list = client.list_state_machines().send().await.unwrap();
    assert_eq!(list.state_machines().len(), 1);

    // Reset
    let http = reqwest::Client::new();
    http.post(format!("{}/_reset", server.endpoint()))
        .send()
        .await
        .unwrap();

    // Verify cleared
    let list = client.list_state_machines().send().await.unwrap();
    assert_eq!(list.state_machines().len(), 0);
}

// ─── Execution Lifecycle Tests ──────────────────────────────────────────

fn pass_with_result_definition() -> String {
    serde_json::json!({
        "StartAt": "PassState",
        "States": {
            "PassState": {
                "Type": "Pass",
                "Result": {"processed": true, "value": 42},
                "End": true
            }
        }
    })
    .to_string()
}

fn pass_chain_with_result_path() -> String {
    serde_json::json!({
        "StartAt": "First",
        "States": {
            "First": {
                "Type": "Pass",
                "Result": "step1-done",
                "ResultPath": "$.firstResult",
                "Next": "Second"
            },
            "Second": {
                "Type": "Pass",
                "Result": "step2-done",
                "ResultPath": "$.secondResult",
                "End": true
            }
        }
    })
    .to_string()
}

fn succeed_definition() -> String {
    serde_json::json!({
        "StartAt": "Done",
        "States": {
            "Done": {
                "Type": "Succeed"
            }
        }
    })
    .to_string()
}

fn fail_definition() -> String {
    serde_json::json!({
        "StartAt": "FailState",
        "States": {
            "FailState": {
                "Type": "Fail",
                "Error": "CustomError",
                "Cause": "Something went wrong"
            }
        }
    })
    .to_string()
}

/// Helper to wait for an execution to finish (not RUNNING).
async fn wait_for_execution(client: &aws_sdk_sfn::Client, arn: &str) -> String {
    for _ in 0..50 {
        sleep(Duration::from_millis(50)).await;
        let desc = client
            .describe_execution()
            .execution_arn(arn)
            .send()
            .await
            .unwrap();
        let status = desc.status().as_str().to_string();
        if status != "RUNNING" {
            return status;
        }
    }
    panic!("Execution did not complete in time: {arn}");
}

#[tokio::test]
async fn sfn_start_and_describe_execution() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("exec-sm")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{"key": "value"}"#)
        .send()
        .await
        .unwrap();

    let exec_arn = start.execution_arn();
    assert!(exec_arn.contains("execution:exec-sm:"));

    let status = wait_for_execution(&client, exec_arn).await;
    assert_eq!(status, "SUCCEEDED");

    // Describe the completed execution
    let desc = client
        .describe_execution()
        .execution_arn(exec_arn)
        .send()
        .await
        .unwrap();

    assert_eq!(desc.status().as_str(), "SUCCEEDED");
    assert!(desc.output().is_some());
    assert!(desc.stop_date().is_some());
}

#[tokio::test]
async fn sfn_execution_with_pass_result() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("pass-result-sm")
        .definition(pass_with_result_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{"original": true}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");

    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();

    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap()).unwrap();
    assert_eq!(output["processed"], true);
    assert_eq!(output["value"], 42);
}

#[tokio::test]
async fn sfn_execution_pass_chain_with_result_path() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("chain-sm")
        .definition(pass_chain_with_result_path())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{"initial": "data"}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");

    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();

    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap()).unwrap();
    assert_eq!(output["initial"], "data");
    assert_eq!(output["firstResult"], "step1-done");
    assert_eq!(output["secondResult"], "step2-done");
}

#[tokio::test]
async fn sfn_execution_succeed_state() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("succeed-sm")
        .definition(succeed_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{"data": "test"}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");
}

#[tokio::test]
async fn sfn_execution_fail_state() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("fail-sm")
        .definition(fail_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "FAILED");

    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();

    assert_eq!(desc.error().unwrap(), "CustomError");
    assert_eq!(desc.cause().unwrap(), "Something went wrong");
}

#[tokio::test]
async fn sfn_list_executions() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("list-exec-sm")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();
    let sm_arn = create.state_machine_arn();

    // Start 3 executions
    for _ in 0..3 {
        client
            .start_execution()
            .state_machine_arn(sm_arn)
            .send()
            .await
            .unwrap();
    }

    // Wait for them to complete
    sleep(Duration::from_millis(200)).await;

    let list = client
        .list_executions()
        .state_machine_arn(sm_arn)
        .send()
        .await
        .unwrap();

    assert_eq!(list.executions().len(), 3);
}

#[tokio::test]
async fn sfn_list_executions_with_status_filter() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    // Create one that succeeds
    let sm1 = client
        .create_state_machine()
        .name("filter-sm")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start1 = client
        .start_execution()
        .state_machine_arn(sm1.state_machine_arn())
        .name("succeed-exec")
        .send()
        .await
        .unwrap();

    wait_for_execution(&client, start1.execution_arn()).await;

    // Start one that fails (using a fail state machine)
    client
        .update_state_machine()
        .state_machine_arn(sm1.state_machine_arn())
        .definition(fail_definition())
        .send()
        .await
        .unwrap();

    let start2 = client
        .start_execution()
        .state_machine_arn(sm1.state_machine_arn())
        .name("fail-exec")
        .send()
        .await
        .unwrap();

    wait_for_execution(&client, start2.execution_arn()).await;

    // Filter by SUCCEEDED
    let succeeded = client
        .list_executions()
        .state_machine_arn(sm1.state_machine_arn())
        .status_filter(aws_sdk_sfn::types::ExecutionStatus::Succeeded)
        .send()
        .await
        .unwrap();
    assert_eq!(succeeded.executions().len(), 1);

    // Filter by FAILED
    let failed = client
        .list_executions()
        .state_machine_arn(sm1.state_machine_arn())
        .status_filter(aws_sdk_sfn::types::ExecutionStatus::Failed)
        .send()
        .await
        .unwrap();
    assert_eq!(failed.executions().len(), 1);
}

#[tokio::test]
async fn sfn_get_execution_history() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("history-sm")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{}"#)
        .send()
        .await
        .unwrap();

    wait_for_execution(&client, start.execution_arn()).await;

    let history = client
        .get_execution_history()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();

    let events = history.events();
    assert!(events.len() >= 4); // ExecutionStarted, PassStateEntered, PassStateExited, ExecutionSucceeded

    // First event should be ExecutionStarted
    assert_eq!(events[0].r#type().as_str(), "ExecutionStarted");
}

#[tokio::test]
async fn sfn_stop_execution() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    // Use a Succeed state which is still fast but gives us a valid execution
    let create = client
        .create_state_machine()
        .name("stop-sm")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .name("stop-exec")
        .send()
        .await
        .unwrap();

    // Try to stop it immediately (it may already be done given fast execution)
    let stop_result = client
        .stop_execution()
        .execution_arn(start.execution_arn())
        .error("UserCancelled")
        .cause("Test cancellation")
        .send()
        .await;

    // Either it stops successfully or it already finished
    if stop_result.is_ok() {
        let desc = client
            .describe_execution()
            .execution_arn(start.execution_arn())
            .send()
            .await
            .unwrap();
        assert_eq!(desc.status().as_str(), "ABORTED");
    }
}

#[tokio::test]
async fn sfn_start_execution_with_name() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("named-exec-sm")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .name("my-execution")
        .send()
        .await
        .unwrap();

    assert!(start.execution_arn().contains("my-execution"));

    wait_for_execution(&client, start.execution_arn()).await;

    // Duplicate execution name should fail
    let err = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .name("my-execution")
        .send()
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn sfn_describe_state_machine_for_execution() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("for-exec-sm")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .send()
        .await
        .unwrap();

    wait_for_execution(&client, start.execution_arn()).await;

    let sm = client
        .describe_state_machine_for_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();

    assert_eq!(sm.name(), "for-exec-sm");
    assert_eq!(sm.state_machine_arn(), create.state_machine_arn());
}

#[tokio::test]
async fn sfn_start_execution_no_input() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("no-input-sm")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");
}

// --- Task state + Error handling tests ---

#[tokio::test]
async fn sfn_task_state_catch_unsupported_resource() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let definition = serde_json::json!({
        "StartAt": "InvokeTask",
        "States": {
            "InvokeTask": {
                "Type": "Task",
                "Resource": "arn:aws:unsupported:us-east-1:123456789012:thing:stuff",
                "Catch": [{
                    "ErrorEquals": ["States.ALL"],
                    "Next": "HandleError",
                    "ResultPath": "$.error"
                }],
                "Next": "Done"
            },
            "HandleError": {
                "Type": "Pass",
                "Result": "error handled",
                "ResultPath": "$.handled",
                "End": true
            },
            "Done": {
                "Type": "Succeed"
            }
        }
    })
    .to_string();

    let create = client
        .create_state_machine()
        .name("task-catch-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{"key": "value"}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");

    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    assert_eq!(output["handled"], "error handled");
    assert_eq!(output["error"]["Error"], "States.TaskFailed");
}

#[tokio::test]
async fn sfn_task_state_catch_with_result_path_null() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let definition = serde_json::json!({
        "StartAt": "InvokeTask",
        "States": {
            "InvokeTask": {
                "Type": "Task",
                "Resource": "arn:aws:unsupported:us-east-1:123456789012:thing:stuff",
                "Catch": [{
                    "ErrorEquals": ["States.TaskFailed"],
                    "Next": "Fallback",
                    "ResultPath": null
                }],
                "End": true
            },
            "Fallback": {
                "Type": "Pass",
                "End": true
            }
        }
    })
    .to_string();

    let create = client
        .create_state_machine()
        .name("task-catch-null-rp-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{"original": "data"}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");

    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    assert_eq!(output["original"], "data");
}

#[tokio::test]
async fn sfn_task_state_no_catch_fails_execution() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let definition = serde_json::json!({
        "StartAt": "InvokeTask",
        "States": {
            "InvokeTask": {
                "Type": "Task",
                "Resource": "arn:aws:unsupported:us-east-1:123456789012:thing:stuff",
                "End": true
            }
        }
    })
    .to_string();

    let create = client
        .create_state_machine()
        .name("task-no-catch-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "FAILED");

    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    assert_eq!(desc.error().unwrap_or(""), "States.TaskFailed");
}

#[tokio::test]
async fn sfn_task_state_catch_specific_error() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let definition = serde_json::json!({
        "StartAt": "InvokeTask",
        "States": {
            "InvokeTask": {
                "Type": "Task",
                "Resource": "arn:aws:unsupported:us-east-1:123456789012:thing:stuff",
                "Catch": [
                    {
                        "ErrorEquals": ["States.Timeout"],
                        "Next": "TimeoutHandler"
                    },
                    {
                        "ErrorEquals": ["States.TaskFailed"],
                        "Next": "TaskFailHandler"
                    }
                ],
                "End": true
            },
            "TimeoutHandler": {
                "Type": "Pass",
                "Result": "timeout",
                "End": true
            },
            "TaskFailHandler": {
                "Type": "Pass",
                "Result": "task-failed",
                "End": true
            }
        }
    })
    .to_string();

    let create = client
        .create_state_machine()
        .name("task-catch-specific-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");

    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    assert_eq!(output, serde_json::json!("task-failed"));
}

#[tokio::test]
async fn sfn_task_state_history_events() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let definition = serde_json::json!({
        "StartAt": "InvokeTask",
        "States": {
            "InvokeTask": {
                "Type": "Task",
                "Resource": "arn:aws:unsupported:us-east-1:123456789012:thing:stuff",
                "Catch": [{
                    "ErrorEquals": ["States.ALL"],
                    "Next": "Done"
                }],
                "Next": "Done"
            },
            "Done": {
                "Type": "Pass",
                "End": true
            }
        }
    })
    .to_string();

    let create = client
        .create_state_machine()
        .name("task-history-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");

    let history = client
        .get_execution_history()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();

    let event_types: Vec<String> = history
        .events()
        .iter()
        .map(|e| e.r#type().as_str().to_string())
        .collect();

    assert!(event_types.contains(&"TaskStateEntered".to_string()));
    assert!(event_types.contains(&"TaskScheduled".to_string()));
    assert!(event_types.contains(&"TaskStarted".to_string()));
    assert!(event_types.contains(&"TaskFailed".to_string()));
    assert!(event_types.contains(&"PassStateEntered".to_string()));
    assert!(event_types.contains(&"ExecutionSucceeded".to_string()));
}

#[tokio::test]
async fn sfn_task_state_lambda_with_catch() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    // Lambda invoke with Catch — works whether Docker is available or not.
    // If no runtime: Lambda returns empty result → succeeds via normal path.
    // If runtime but function missing: error → caught → succeeds via fallback.
    let definition = serde_json::json!({
        "StartAt": "InvokeLambda",
        "States": {
            "InvokeLambda": {
                "Type": "Task",
                "Resource": "arn:aws:lambda:us-east-1:123456789012:function:my-function",
                "ResultPath": "$.lambdaResult",
                "Catch": [{
                    "ErrorEquals": ["States.ALL"],
                    "Next": "Fallback",
                    "ResultPath": "$.lambdaError"
                }],
                "Next": "Done"
            },
            "Fallback": {
                "Type": "Pass",
                "Result": "caught",
                "ResultPath": "$.fallback",
                "End": true
            },
            "Done": {
                "Type": "Succeed"
            }
        }
    })
    .to_string();

    let create = client
        .create_state_machine()
        .name("task-lambda-catch-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{"data": "test"}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");

    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    // Original input preserved
    assert_eq!(output["data"], "test");
}

#[tokio::test]
async fn sfn_task_state_with_parameters() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let definition = serde_json::json!({
        "StartAt": "InvokeTask",
        "States": {
            "InvokeTask": {
                "Type": "Task",
                "Resource": "arn:aws:lambda:us-east-1:123456789012:function:my-function",
                "Parameters": {
                    "action": "process",
                    "payload.$": "$.data"
                },
                "Catch": [{
                    "ErrorEquals": ["States.ALL"],
                    "Next": "Done"
                }],
                "End": true
            },
            "Done": {
                "Type": "Succeed"
            }
        }
    })
    .to_string();

    let create = client
        .create_state_machine()
        .name("task-params-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{"data": {"items": [1,2,3]}}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");
}

#[tokio::test]
async fn sfn_task_chain_with_pass() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    // Task → Pass chain with Catch for Docker-agnostic testing.
    // If Lambda succeeds (no runtime → empty result), goes to Enrich.
    // If Lambda fails (Docker present but function missing), Catch → Enrich.
    let definition = serde_json::json!({
        "StartAt": "InvokeLambda",
        "States": {
            "InvokeLambda": {
                "Type": "Task",
                "Resource": "arn:aws:lambda:us-east-1:123456789012:function:my-function",
                "ResultPath": "$.taskResult",
                "Catch": [{
                    "ErrorEquals": ["States.ALL"],
                    "Next": "Enrich",
                    "ResultPath": "$.taskError"
                }],
                "Next": "Enrich"
            },
            "Enrich": {
                "Type": "Pass",
                "Result": "enriched",
                "ResultPath": "$.enrichment",
                "End": true
            }
        }
    })
    .to_string();

    let create = client
        .create_state_machine()
        .name("task-chain-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{"original": true}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");

    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    assert_eq!(output["original"], true);
    assert_eq!(output["enrichment"], "enriched");
}

// --- Choice state tests ---

#[tokio::test]
async fn sfn_choice_state_string_equals() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let definition = serde_json::json!({
        "StartAt": "Route",
        "States": {
            "Route": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.status",
                        "StringEquals": "active",
                        "Next": "ActivePath"
                    },
                    {
                        "Variable": "$.status",
                        "StringEquals": "inactive",
                        "Next": "InactivePath"
                    }
                ],
                "Default": "DefaultPath"
            },
            "ActivePath": {
                "Type": "Pass",
                "Result": "went-active",
                "End": true
            },
            "InactivePath": {
                "Type": "Pass",
                "Result": "went-inactive",
                "End": true
            },
            "DefaultPath": {
                "Type": "Pass",
                "Result": "went-default",
                "End": true
            }
        }
    })
    .to_string();

    let create = client
        .create_state_machine()
        .name("choice-string-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    // Test active path
    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .name("exec-active")
        .input(r#"{"status": "active"}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");
    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    assert_eq!(output, "went-active");

    // Test inactive path
    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .name("exec-inactive")
        .input(r#"{"status": "inactive"}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");
    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    assert_eq!(output, "went-inactive");

    // Test default path
    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .name("exec-default")
        .input(r#"{"status": "unknown"}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");
    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    assert_eq!(output, "went-default");
}

#[tokio::test]
async fn sfn_choice_state_numeric_comparison() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let definition = serde_json::json!({
        "StartAt": "CheckScore",
        "States": {
            "CheckScore": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.score",
                        "NumericGreaterThanEquals": 90,
                        "Next": "Grade_A"
                    },
                    {
                        "Variable": "$.score",
                        "NumericGreaterThanEquals": 80,
                        "Next": "Grade_B"
                    }
                ],
                "Default": "Grade_C"
            },
            "Grade_A": {
                "Type": "Pass",
                "Result": "A",
                "End": true
            },
            "Grade_B": {
                "Type": "Pass",
                "Result": "B",
                "End": true
            },
            "Grade_C": {
                "Type": "Pass",
                "Result": "C",
                "End": true
            }
        }
    })
    .to_string();

    let create = client
        .create_state_machine()
        .name("choice-numeric-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .name("score-95")
        .input(r#"{"score": 95}"#)
        .send()
        .await
        .unwrap();
    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");
    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    assert_eq!(output, "A");

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .name("score-85")
        .input(r#"{"score": 85}"#)
        .send()
        .await
        .unwrap();
    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");
    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    assert_eq!(output, "B");

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .name("score-50")
        .input(r#"{"score": 50}"#)
        .send()
        .await
        .unwrap();
    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");
    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    assert_eq!(output, "C");
}

#[tokio::test]
async fn sfn_choice_state_boolean_and_compound() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let definition = serde_json::json!({
        "StartAt": "Check",
        "States": {
            "Check": {
                "Type": "Choice",
                "Choices": [
                    {
                        "And": [
                            {"Variable": "$.enabled", "BooleanEquals": true},
                            {"Variable": "$.count", "NumericGreaterThan": 0}
                        ],
                        "Next": "Process"
                    }
                ],
                "Default": "Skip"
            },
            "Process": {
                "Type": "Pass",
                "Result": "processed",
                "End": true
            },
            "Skip": {
                "Type": "Pass",
                "Result": "skipped",
                "End": true
            }
        }
    })
    .to_string();

    let create = client
        .create_state_machine()
        .name("choice-compound-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    // Both conditions true
    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .name("exec-both-true")
        .input(r#"{"enabled": true, "count": 5}"#)
        .send()
        .await
        .unwrap();
    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");
    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    assert_eq!(output, "processed");

    // One condition false — should go to Skip
    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .name("exec-disabled")
        .input(r#"{"enabled": false, "count": 5}"#)
        .send()
        .await
        .unwrap();
    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");
    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    assert_eq!(output, "skipped");
}

#[tokio::test]
async fn sfn_choice_state_not_operator() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let definition = serde_json::json!({
        "StartAt": "Check",
        "States": {
            "Check": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Not": {
                            "Variable": "$.type",
                            "StringEquals": "admin"
                        },
                        "Next": "RegularUser"
                    }
                ],
                "Default": "AdminUser"
            },
            "RegularUser": {
                "Type": "Pass",
                "Result": "regular",
                "End": true
            },
            "AdminUser": {
                "Type": "Pass",
                "Result": "admin",
                "End": true
            }
        }
    })
    .to_string();

    let create = client
        .create_state_machine()
        .name("choice-not-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .name("exec-user")
        .input(r#"{"type": "user"}"#)
        .send()
        .await
        .unwrap();
    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");
    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    assert_eq!(output, "regular");

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .name("exec-admin")
        .input(r#"{"type": "admin"}"#)
        .send()
        .await
        .unwrap();
    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");
    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    assert_eq!(output, "admin");
}

#[tokio::test]
async fn sfn_choice_state_no_match_no_default() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let definition = serde_json::json!({
        "StartAt": "Route",
        "States": {
            "Route": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.status",
                        "StringEquals": "active",
                        "Next": "Active"
                    }
                ]
            },
            "Active": {
                "Type": "Succeed"
            }
        }
    })
    .to_string();

    let create = client
        .create_state_machine()
        .name("choice-no-default-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{"status": "unknown"}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "FAILED");

    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    assert_eq!(desc.error().unwrap_or(""), "States.NoChoiceMatched");
}

#[tokio::test]
async fn sfn_choice_state_is_present() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let definition = serde_json::json!({
        "StartAt": "Check",
        "States": {
            "Check": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.optional",
                        "IsPresent": true,
                        "Next": "HasField"
                    }
                ],
                "Default": "NoField"
            },
            "HasField": {
                "Type": "Pass",
                "Result": "present",
                "End": true
            },
            "NoField": {
                "Type": "Pass",
                "Result": "absent",
                "End": true
            }
        }
    })
    .to_string();

    let create = client
        .create_state_machine()
        .name("choice-ispresent-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .name("exec-with-field")
        .input(r#"{"optional": "value"}"#)
        .send()
        .await
        .unwrap();
    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");
    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    assert_eq!(output, "present");

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .name("exec-without-field")
        .input(r#"{"other": "value"}"#)
        .send()
        .await
        .unwrap();
    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");
    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    assert_eq!(output, "absent");
}

// --- Wait state tests ---

#[tokio::test]
async fn sfn_wait_state_seconds() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let definition = serde_json::json!({
        "StartAt": "WaitBriefly",
        "States": {
            "WaitBriefly": {
                "Type": "Wait",
                "Seconds": 1,
                "Next": "Done"
            },
            "Done": {
                "Type": "Pass",
                "Result": "waited",
                "End": true
            }
        }
    })
    .to_string();

    let create = client
        .create_state_machine()
        .name("wait-seconds-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{}"#)
        .send()
        .await
        .unwrap();

    // Execution should be running during wait
    sleep(Duration::from_millis(200)).await;
    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    assert_eq!(desc.status().as_str(), "RUNNING");

    // Wait for completion
    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");

    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    assert_eq!(output, "waited");
}

#[tokio::test]
async fn sfn_wait_state_seconds_path() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let definition = serde_json::json!({
        "StartAt": "WaitDynamic",
        "States": {
            "WaitDynamic": {
                "Type": "Wait",
                "SecondsPath": "$.delay",
                "Next": "Done"
            },
            "Done": {
                "Type": "Pass",
                "Result": "dynamic-wait-done",
                "End": true
            }
        }
    })
    .to_string();

    let create = client
        .create_state_machine()
        .name("wait-seconds-path-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{"delay": 1}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");

    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    assert_eq!(output, "dynamic-wait-done");
}

#[tokio::test]
async fn sfn_choice_then_wait_workflow() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    // Combined workflow: Choice routes to either a fast or slow path with Wait
    let definition = serde_json::json!({
        "StartAt": "Route",
        "States": {
            "Route": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.priority",
                        "StringEquals": "high",
                        "Next": "FastTrack"
                    }
                ],
                "Default": "SlowTrack"
            },
            "FastTrack": {
                "Type": "Pass",
                "Result": "fast",
                "End": true
            },
            "SlowTrack": {
                "Type": "Wait",
                "Seconds": 1,
                "Next": "Done"
            },
            "Done": {
                "Type": "Pass",
                "Result": "slow",
                "End": true
            }
        }
    })
    .to_string();

    let create = client
        .create_state_machine()
        .name("choice-wait-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .name("exec-high")
        .input(r#"{"priority": "high"}"#)
        .send()
        .await
        .unwrap();
    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");
    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    assert_eq!(output, "fast");

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .name("exec-low")
        .input(r#"{"priority": "low"}"#)
        .send()
        .await
        .unwrap();
    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");
    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    assert_eq!(output, "slow");
}

#[tokio::test]
async fn sfn_choice_state_history_events() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let definition = serde_json::json!({
        "StartAt": "Route",
        "States": {
            "Route": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.go",
                        "BooleanEquals": true,
                        "Next": "Done"
                    }
                ],
                "Default": "Done"
            },
            "Done": {
                "Type": "Succeed"
            }
        }
    })
    .to_string();

    let create = client
        .create_state_machine()
        .name("choice-history-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{"go": true}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");

    let history = client
        .get_execution_history()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();

    let event_types: Vec<String> = history
        .events()
        .iter()
        .map(|e| e.r#type().as_str().to_string())
        .collect();

    assert!(event_types.contains(&"ChoiceStateEntered".to_string()));
    assert!(event_types.contains(&"ChoiceStateExited".to_string()));
    assert!(event_types.contains(&"ExecutionSucceeded".to_string()));
}

// --- Parallel state tests ---

#[tokio::test]
async fn sfn_parallel_state_basic() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    // Three parallel branches, each doing a Pass with different results
    let definition = serde_json::json!({
        "StartAt": "ParallelWork",
        "States": {
            "ParallelWork": {
                "Type": "Parallel",
                "Branches": [
                    {
                        "StartAt": "Branch1",
                        "States": {
                            "Branch1": {
                                "Type": "Pass",
                                "Result": "result-1",
                                "End": true
                            }
                        }
                    },
                    {
                        "StartAt": "Branch2",
                        "States": {
                            "Branch2": {
                                "Type": "Pass",
                                "Result": "result-2",
                                "End": true
                            }
                        }
                    },
                    {
                        "StartAt": "Branch3",
                        "States": {
                            "Branch3": {
                                "Type": "Pass",
                                "Result": "result-3",
                                "End": true
                            }
                        }
                    }
                ],
                "End": true
            }
        }
    })
    .to_string();

    let create = client
        .create_state_machine()
        .name("parallel-basic-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");

    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    // Output should be an array of branch results in order
    assert_eq!(
        output,
        serde_json::json!(["result-1", "result-2", "result-3"])
    );
}

#[tokio::test]
async fn sfn_parallel_state_with_result_path() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let definition = serde_json::json!({
        "StartAt": "ParallelWork",
        "States": {
            "ParallelWork": {
                "Type": "Parallel",
                "ResultPath": "$.results",
                "Branches": [
                    {
                        "StartAt": "A",
                        "States": {
                            "A": {
                                "Type": "Pass",
                                "Result": {"branch": "a"},
                                "End": true
                            }
                        }
                    },
                    {
                        "StartAt": "B",
                        "States": {
                            "B": {
                                "Type": "Pass",
                                "Result": {"branch": "b"},
                                "End": true
                            }
                        }
                    }
                ],
                "End": true
            }
        }
    })
    .to_string();

    let create = client
        .create_state_machine()
        .name("parallel-resultpath-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{"original": true}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");

    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    assert_eq!(output["original"], true);
    assert_eq!(
        output["results"],
        serde_json::json!([{"branch": "a"}, {"branch": "b"}])
    );
}

#[tokio::test]
async fn sfn_parallel_state_branch_failure_with_catch() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    // One branch fails, Catch routes to error handler
    let definition = serde_json::json!({
        "StartAt": "ParallelWork",
        "States": {
            "ParallelWork": {
                "Type": "Parallel",
                "Branches": [
                    {
                        "StartAt": "Good",
                        "States": {
                            "Good": {
                                "Type": "Pass",
                                "Result": "ok",
                                "End": true
                            }
                        }
                    },
                    {
                        "StartAt": "Bad",
                        "States": {
                            "Bad": {
                                "Type": "Fail",
                                "Error": "BranchError",
                                "Cause": "Branch 2 failed"
                            }
                        }
                    }
                ],
                "Catch": [{
                    "ErrorEquals": ["States.ALL"],
                    "Next": "HandleError",
                    "ResultPath": "$.error"
                }],
                "End": true
            },
            "HandleError": {
                "Type": "Pass",
                "Result": "handled",
                "End": true
            }
        }
    })
    .to_string();

    let create = client
        .create_state_machine()
        .name("parallel-catch-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");

    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    assert_eq!(output, "handled");
}

#[tokio::test]
async fn sfn_parallel_state_history_events() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let definition = serde_json::json!({
        "StartAt": "ParallelWork",
        "States": {
            "ParallelWork": {
                "Type": "Parallel",
                "Branches": [
                    {
                        "StartAt": "A",
                        "States": {
                            "A": { "Type": "Pass", "Result": "a", "End": true }
                        }
                    }
                ],
                "End": true
            }
        }
    })
    .to_string();

    let create = client
        .create_state_machine()
        .name("parallel-history-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");

    let history = client
        .get_execution_history()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();

    let event_types: Vec<String> = history
        .events()
        .iter()
        .map(|e| e.r#type().as_str().to_string())
        .collect();

    assert!(event_types.contains(&"ParallelStateEntered".to_string()));
    assert!(event_types.contains(&"ParallelStateExited".to_string()));
}

// --- Map state tests ---

#[tokio::test]
async fn sfn_map_state_basic() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    // Map over an array, each item gets a Pass that adds "processed" field
    let definition = serde_json::json!({
        "StartAt": "ProcessItems",
        "States": {
            "ProcessItems": {
                "Type": "Map",
                "ItemsPath": "$.items",
                "ItemProcessor": {
                    "StartAt": "Process",
                    "States": {
                        "Process": {
                            "Type": "Pass",
                            "Result": "done",
                            "ResultPath": "$.status",
                            "End": true
                        }
                    }
                },
                "End": true
            }
        }
    })
    .to_string();

    let create = client
        .create_state_machine()
        .name("map-basic-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{"items": [{"id": 1}, {"id": 2}, {"id": 3}]}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");

    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    let arr = output.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0]["id"], 1);
    assert_eq!(arr[0]["status"], "done");
    assert_eq!(arr[2]["id"], 3);
}

#[tokio::test]
async fn sfn_map_state_with_result_path() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let definition = serde_json::json!({
        "StartAt": "ProcessItems",
        "States": {
            "ProcessItems": {
                "Type": "Map",
                "ItemsPath": "$.items",
                "ResultPath": "$.processed",
                "ItemProcessor": {
                    "StartAt": "Transform",
                    "States": {
                        "Transform": {
                            "Type": "Pass",
                            "End": true
                        }
                    }
                },
                "End": true
            }
        }
    })
    .to_string();

    let create = client
        .create_state_machine()
        .name("map-resultpath-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{"items": ["a", "b"], "keep": true}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");

    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    assert_eq!(output["keep"], true);
    assert_eq!(output["processed"], serde_json::json!(["a", "b"]));
}

#[tokio::test]
async fn sfn_map_state_empty_array() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let definition = serde_json::json!({
        "StartAt": "ProcessItems",
        "States": {
            "ProcessItems": {
                "Type": "Map",
                "ItemsPath": "$.items",
                "ItemProcessor": {
                    "StartAt": "Process",
                    "States": {
                        "Process": { "Type": "Pass", "End": true }
                    }
                },
                "End": true
            }
        }
    })
    .to_string();

    let create = client
        .create_state_machine()
        .name("map-empty-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{"items": []}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");

    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    assert_eq!(output, serde_json::json!([]));
}

#[tokio::test]
async fn sfn_map_state_history_events() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let definition = serde_json::json!({
        "StartAt": "ProcessItems",
        "States": {
            "ProcessItems": {
                "Type": "Map",
                "ItemsPath": "$.items",
                "ItemProcessor": {
                    "StartAt": "Process",
                    "States": {
                        "Process": { "Type": "Pass", "End": true }
                    }
                },
                "End": true
            }
        }
    })
    .to_string();

    let create = client
        .create_state_machine()
        .name("map-history-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{"items": [1, 2]}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");

    let history = client
        .get_execution_history()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();

    let event_types: Vec<String> = history
        .events()
        .iter()
        .map(|e| e.r#type().as_str().to_string())
        .collect();

    assert!(event_types.contains(&"MapStateEntered".to_string()));
    assert!(event_types.contains(&"MapIterationStarted".to_string()));
    assert!(event_types.contains(&"MapIterationSucceeded".to_string()));
    assert!(event_types.contains(&"MapStateExited".to_string()));
}

#[tokio::test]
async fn sfn_parallel_then_map_workflow() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    // Parallel produces array, then Map processes each item
    let definition = serde_json::json!({
        "StartAt": "Gather",
        "States": {
            "Gather": {
                "Type": "Parallel",
                "ResultPath": "$.gathered",
                "Branches": [
                    {
                        "StartAt": "Source1",
                        "States": {
                            "Source1": { "Type": "Pass", "Result": {"value": 10}, "End": true }
                        }
                    },
                    {
                        "StartAt": "Source2",
                        "States": {
                            "Source2": { "Type": "Pass", "Result": {"value": 20}, "End": true }
                        }
                    }
                ],
                "Next": "Process"
            },
            "Process": {
                "Type": "Map",
                "ItemsPath": "$.gathered",
                "ItemProcessor": {
                    "StartAt": "Transform",
                    "States": {
                        "Transform": {
                            "Type": "Pass",
                            "Result": "transformed",
                            "ResultPath": "$.status",
                            "End": true
                        }
                    }
                },
                "End": true
            }
        }
    })
    .to_string();

    let create = client
        .create_state_machine()
        .name("parallel-map-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&client, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");

    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    let arr = output.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["value"], 10);
    assert_eq!(arr[0]["status"], "transformed");
    assert_eq!(arr[1]["value"], 20);
}

// --- Cross-service integration tests ---

#[tokio::test]
async fn sfn_task_sqs_send_message() {
    let server = TestServer::start().await;
    let sfn = server.sfn_client().await;
    let sqs = server.sqs_client().await;

    // Create an SQS queue
    let queue = sqs
        .create_queue()
        .queue_name("sfn-test-queue")
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap();

    // Create a state machine that sends a message to SQS
    let definition = json!({
        "StartAt": "SendMessage",
        "States": {
            "SendMessage": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sqs:sendMessage",
                "Parameters": {
                    "QueueUrl": queue_url,
                    "MessageBody": "hello from step functions"
                },
                "End": true
            }
        }
    })
    .to_string();

    let create = sfn
        .create_state_machine()
        .name("sqs-send-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = sfn
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&sfn, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");

    // Verify the message was delivered to SQS
    let receive = sqs
        .receive_message()
        .queue_url(queue_url)
        .send()
        .await
        .unwrap();

    let messages = receive.messages();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].body().unwrap(), "hello from step functions");
}

#[tokio::test]
async fn sfn_task_sns_publish() {
    let server = TestServer::start().await;
    let sfn = server.sfn_client().await;
    let sns = server.sns_client().await;

    // Create an SNS topic
    let topic = sns
        .create_topic()
        .name("sfn-test-topic")
        .send()
        .await
        .unwrap();
    let topic_arn = topic.topic_arn().unwrap();

    // Create a state machine that publishes to SNS
    let definition = json!({
        "StartAt": "Publish",
        "States": {
            "Publish": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": topic_arn,
                    "Message": "hello from step functions"
                },
                "End": true
            }
        }
    })
    .to_string();

    let create = sfn
        .create_state_machine()
        .name("sns-publish-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = sfn
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&sfn, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");

    // Verify the message was published via introspection endpoint
    let url = format!("{}/_fakecloud/sns/messages", server.endpoint());
    let resp: serde_json::Value = reqwest::get(&url).await.unwrap().json().await.unwrap();
    let messages = resp["messages"].as_array().unwrap();
    assert!(
        messages
            .iter()
            .any(|m| m["message"].as_str() == Some("hello from step functions")),
        "Expected SNS message not found: {messages:?}"
    );
}

#[tokio::test]
async fn sfn_task_dynamodb_put_and_get_item() {
    let server = TestServer::start().await;
    let sfn = server.sfn_client().await;
    let ddb = server.dynamodb_client().await;

    // Create a DynamoDB table
    ddb.create_table()
        .table_name("sfn-test-table")
        .attribute_definitions(
            aws_sdk_dynamodb::types::AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            aws_sdk_dynamodb::types::KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                .build()
                .unwrap(),
        )
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    // Create a state machine that puts an item, then gets it
    let definition = json!({
        "StartAt": "PutItem",
        "States": {
            "PutItem": {
                "Type": "Task",
                "Resource": "arn:aws:states:::dynamodb:putItem",
                "Parameters": {
                    "TableName": "sfn-test-table",
                    "Item": {
                        "pk": {"S": "item-1"},
                        "data": {"S": "from-step-functions"}
                    }
                },
                "ResultPath": "$.putResult",
                "Next": "GetItem"
            },
            "GetItem": {
                "Type": "Task",
                "Resource": "arn:aws:states:::dynamodb:getItem",
                "Parameters": {
                    "TableName": "sfn-test-table",
                    "Key": {
                        "pk": {"S": "item-1"}
                    }
                },
                "End": true
            }
        }
    })
    .to_string();

    let create = sfn
        .create_state_machine()
        .name("ddb-put-get-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = sfn
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&sfn, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");

    // Verify the output contains the item
    let desc = sfn
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    let output: serde_json::Value = serde_json::from_str(desc.output().unwrap_or("{}")).unwrap();
    assert_eq!(output["Item"]["pk"]["S"], "item-1");
    assert_eq!(output["Item"]["data"]["S"], "from-step-functions");
}

#[tokio::test]
async fn sfn_task_dynamodb_delete_item() {
    let server = TestServer::start().await;
    let sfn = server.sfn_client().await;
    let ddb = server.dynamodb_client().await;

    // Create a DynamoDB table and put an item
    ddb.create_table()
        .table_name("sfn-del-table")
        .attribute_definitions(
            aws_sdk_dynamodb::types::AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            aws_sdk_dynamodb::types::KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                .build()
                .unwrap(),
        )
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    ddb.put_item()
        .table_name("sfn-del-table")
        .item(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S("to-delete".to_string()),
        )
        .item(
            "data",
            aws_sdk_dynamodb::types::AttributeValue::S("will be gone".to_string()),
        )
        .send()
        .await
        .unwrap();

    // Delete the item via Step Functions
    let definition = json!({
        "StartAt": "DeleteItem",
        "States": {
            "DeleteItem": {
                "Type": "Task",
                "Resource": "arn:aws:states:::dynamodb:deleteItem",
                "Parameters": {
                    "TableName": "sfn-del-table",
                    "Key": {
                        "pk": {"S": "to-delete"}
                    }
                },
                "End": true
            }
        }
    })
    .to_string();

    let create = sfn
        .create_state_machine()
        .name("ddb-delete-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = sfn
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&sfn, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");

    // Verify the item was deleted
    let get = ddb
        .get_item()
        .table_name("sfn-del-table")
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S("to-delete".to_string()),
        )
        .send()
        .await
        .unwrap();
    assert!(get.item().is_none());
}

#[tokio::test]
async fn sfn_task_eventbridge_put_events() {
    let server = TestServer::start().await;
    let sfn = server.sfn_client().await;

    // Create a state machine that puts events to EventBridge
    let definition = json!({
        "StartAt": "PutEvents",
        "States": {
            "PutEvents": {
                "Type": "Task",
                "Resource": "arn:aws:states:::events:putEvents",
                "Parameters": {
                    "Entries": [
                        {
                            "Source": "my.app",
                            "DetailType": "OrderCreated",
                            "Detail": "{\"orderId\": \"123\"}",
                            "EventBusName": "default"
                        }
                    ]
                },
                "End": true
            }
        }
    })
    .to_string();

    let create = sfn
        .create_state_machine()
        .name("eb-putevents-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = sfn
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&sfn, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");

    // Verify the event was recorded via introspection
    let url = format!("{}/_fakecloud/events/history", server.endpoint());
    let resp: serde_json::Value = reqwest::get(&url).await.unwrap().json().await.unwrap();
    let events = resp["events"].as_array().unwrap();
    assert!(
        events.iter().any(|e| e["source"].as_str() == Some("my.app")
            && e["detailType"].as_str() == Some("OrderCreated")),
        "Expected EventBridge event not found: {events:?}"
    );
}

#[tokio::test]
async fn sfn_task_sqs_with_dynamic_parameters() {
    let server = TestServer::start().await;
    let sfn = server.sfn_client().await;
    let sqs = server.sqs_client().await;

    // Create an SQS queue
    let queue = sqs
        .create_queue()
        .queue_name("sfn-dynamic-queue")
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap();

    // Create a state machine that uses Parameters.$ to extract from input
    let definition = json!({
        "StartAt": "SendMessage",
        "States": {
            "SendMessage": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sqs:sendMessage",
                "Parameters": {
                    "QueueUrl": queue_url,
                    "MessageBody.$": "$.message"
                },
                "End": true
            }
        }
    })
    .to_string();

    let create = sfn
        .create_state_machine()
        .name("sqs-dynamic-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = sfn
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{"message": "dynamic message content"}"#)
        .send()
        .await
        .unwrap();

    let status = wait_for_execution(&sfn, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");

    // Verify the dynamic message was sent
    let receive = sqs
        .receive_message()
        .queue_url(queue_url)
        .send()
        .await
        .unwrap();

    let messages = receive.messages();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].body().unwrap(), "dynamic message content");
}

#[tokio::test]
async fn sfn_introspection_executions_endpoint() {
    let server = TestServer::start().await;
    let sfn = server.sfn_client().await;

    let create = sfn
        .create_state_machine()
        .name("introspect-sm")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let start = sfn
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .name("introspect-exec")
        .input(r#"{"test": true}"#)
        .send()
        .await
        .unwrap();

    wait_for_execution(&sfn, start.execution_arn()).await;

    // Check the introspection endpoint
    let url = format!("{}/_fakecloud/stepfunctions/executions", server.endpoint());
    let resp: serde_json::Value = reqwest::get(&url).await.unwrap().json().await.unwrap();
    let executions = resp["executions"].as_array().unwrap();
    assert!(!executions.is_empty());

    let exec = executions
        .iter()
        .find(|e| e["name"].as_str() == Some("introspect-exec"))
        .expect("Expected execution not found");
    assert_eq!(exec["status"], "SUCCEEDED");
    assert!(exec["executionArn"]
        .as_str()
        .unwrap()
        .contains("introspect"));
    assert!(exec["stateMachineArn"]
        .as_str()
        .unwrap()
        .contains("introspect-sm"));
    assert!(exec["startDate"].as_str().is_some());
    assert!(exec["stopDate"].as_str().is_some());
}

#[tokio::test]
async fn sfn_cross_service_workflow_sqs_then_choice() {
    let server = TestServer::start().await;
    let sfn = server.sfn_client().await;
    let sqs = server.sqs_client().await;

    // Create two SQS queues for routing
    let high_queue = sqs
        .create_queue()
        .queue_name("high-priority")
        .send()
        .await
        .unwrap();
    let high_url = high_queue.queue_url().unwrap();

    let low_queue = sqs
        .create_queue()
        .queue_name("low-priority")
        .send()
        .await
        .unwrap();
    let low_url = low_queue.queue_url().unwrap();

    // Workflow: Choice routes to different SQS queues based on priority
    let definition = json!({
        "StartAt": "Route",
        "States": {
            "Route": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.priority",
                        "StringEquals": "high",
                        "Next": "SendHigh"
                    }
                ],
                "Default": "SendLow"
            },
            "SendHigh": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sqs:sendMessage",
                "Parameters": {
                    "QueueUrl": high_url,
                    "MessageBody.$": "$.payload"
                },
                "End": true
            },
            "SendLow": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sqs:sendMessage",
                "Parameters": {
                    "QueueUrl": low_url,
                    "MessageBody.$": "$.payload"
                },
                "End": true
            }
        }
    })
    .to_string();

    let create = sfn
        .create_state_machine()
        .name("cross-service-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    // Send a high-priority message
    let start = sfn
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .name("exec-high")
        .input(r#"{"priority": "high", "payload": "urgent order"}"#)
        .send()
        .await
        .unwrap();
    let status = wait_for_execution(&sfn, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");

    // Send a low-priority message
    let start = sfn
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .name("exec-low")
        .input(r#"{"priority": "low", "payload": "regular order"}"#)
        .send()
        .await
        .unwrap();
    let status = wait_for_execution(&sfn, start.execution_arn()).await;
    assert_eq!(status, "SUCCEEDED");

    // Verify high-priority queue got the right message
    let receive = sqs
        .receive_message()
        .queue_url(high_url)
        .send()
        .await
        .unwrap();
    assert_eq!(receive.messages().len(), 1);
    assert_eq!(receive.messages()[0].body().unwrap(), "urgent order");

    // Verify low-priority queue got the right message
    let receive = sqs
        .receive_message()
        .queue_url(low_url)
        .send()
        .await
        .unwrap();
    assert_eq!(receive.messages().len(), 1);
    assert_eq!(receive.messages()[0].body().unwrap(), "regular order");
}
