mod helpers;

use aws_sdk_sfn::types::Tag;
use helpers::TestServer;
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
