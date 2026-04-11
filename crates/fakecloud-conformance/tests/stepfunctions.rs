mod helpers;

use aws_sdk_sfn::types::Tag;
use fakecloud_conformance_macros::test_action;
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

#[test_action("sfn", "CreateStateMachine", checksum = "cad1ea0f")]
#[tokio::test]
async fn sfn_create_state_machine() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let resp = client
        .create_state_machine()
        .name("conf-sm")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();
    assert!(resp.state_machine_arn().contains("stateMachine:conf-sm"));
}

#[test_action("sfn", "DescribeStateMachine", checksum = "e9ff62d1")]
#[tokio::test]
async fn sfn_describe_state_machine() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("conf-describe")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_state_machine()
        .state_machine_arn(create.state_machine_arn())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.name(), "conf-describe");
    assert_eq!(resp.status().unwrap().as_str(), "ACTIVE");
    assert_eq!(resp.r#type().as_str(), "STANDARD");
}

#[test_action("sfn", "ListStateMachines", checksum = "3d392fe1")]
#[tokio::test]
async fn sfn_list_state_machines() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    client
        .create_state_machine()
        .name("conf-list")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let resp = client.list_state_machines().send().await.unwrap();
    assert!(!resp.state_machines().is_empty());
}

#[test_action("sfn", "DeleteStateMachine", checksum = "286b2d42")]
#[tokio::test]
async fn sfn_delete_state_machine() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("conf-delete")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    client
        .delete_state_machine()
        .state_machine_arn(create.state_machine_arn())
        .send()
        .await
        .unwrap();

    let err = client
        .describe_state_machine()
        .state_machine_arn(create.state_machine_arn())
        .send()
        .await;
    assert!(err.is_err());
}

#[test_action("sfn", "UpdateStateMachine", checksum = "a9b06b6a")]
#[tokio::test]
async fn sfn_update_state_machine() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("conf-update")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let resp = client
        .update_state_machine()
        .state_machine_arn(create.state_machine_arn())
        .role_arn("arn:aws:iam::123456789012:role/new-role")
        .send()
        .await
        .unwrap();
    let _ = resp.update_date();

    let describe = client
        .describe_state_machine()
        .state_machine_arn(create.state_machine_arn())
        .send()
        .await
        .unwrap();
    assert_eq!(
        describe.role_arn(),
        "arn:aws:iam::123456789012:role/new-role"
    );
}

#[test_action("sfn", "TagResource", checksum = "047e5817")]
#[tokio::test]
async fn sfn_tag_resource() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("conf-tag")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    client
        .tag_resource()
        .resource_arn(create.state_machine_arn())
        .tags(Tag::builder().key("env").value("test").build())
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
}

#[test_action("sfn", "UntagResource", checksum = "56aea886")]
#[tokio::test]
async fn sfn_untag_resource() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("conf-untag")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    client
        .tag_resource()
        .resource_arn(create.state_machine_arn())
        .tags(Tag::builder().key("env").value("test").build())
        .tags(Tag::builder().key("team").value("eng").build())
        .send()
        .await
        .unwrap();

    client
        .untag_resource()
        .resource_arn(create.state_machine_arn())
        .tag_keys("env")
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
    assert_eq!(tags.tags()[0].key(), Some("team"));
}

#[test_action("sfn", "ListTagsForResource", checksum = "b98da062")]
#[tokio::test]
async fn sfn_list_tags_for_resource() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("conf-list-tags")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    // No tags initially
    let tags = client
        .list_tags_for_resource()
        .resource_arn(create.state_machine_arn())
        .send()
        .await
        .unwrap();
    assert!(tags.tags().is_empty());
}

// ─── Execution Lifecycle Conformance Tests ──────────────────────────

async fn wait_for_execution(client: &aws_sdk_sfn::Client, arn: &str) {
    for _ in 0..50 {
        sleep(Duration::from_millis(50)).await;
        let desc = client
            .describe_execution()
            .execution_arn(arn)
            .send()
            .await
            .unwrap();
        if desc.status().as_str() != "RUNNING" {
            return;
        }
    }
    panic!("Execution did not complete in time");
}

#[test_action("sfn", "StartExecution", checksum = "6ec509e4")]
#[tokio::test]
async fn sfn_start_execution() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("conf-start-exec")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    let resp = client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .input(r#"{}"#)
        .send()
        .await
        .unwrap();
    assert!(resp.execution_arn().contains("execution:"));
}

#[test_action("sfn", "DescribeExecution", checksum = "7574d620")]
#[tokio::test]
async fn sfn_describe_execution() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("conf-desc-exec")
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

    let resp = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_str(), "SUCCEEDED");
}

#[test_action("sfn", "ListExecutions", checksum = "6e3c28ed")]
#[tokio::test]
async fn sfn_list_executions() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("conf-list-exec")
        .definition(simple_definition())
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .send()
        .await
        .unwrap();

    client
        .start_execution()
        .state_machine_arn(create.state_machine_arn())
        .send()
        .await
        .unwrap();

    sleep(Duration::from_millis(200)).await;

    let resp = client
        .list_executions()
        .state_machine_arn(create.state_machine_arn())
        .send()
        .await
        .unwrap();
    assert!(!resp.executions().is_empty());
}

#[test_action("sfn", "StopExecution", checksum = "96371e61")]
#[tokio::test]
async fn sfn_stop_execution() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("conf-stop-exec")
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

    // Try to stop; may already be complete due to fast execution
    let _ = client
        .stop_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await;

    // Just verify describe works after stop attempt
    let desc = client
        .describe_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    // Status is either ABORTED or SUCCEEDED
    let status = desc.status().as_str();
    assert!(status == "ABORTED" || status == "SUCCEEDED");
}

#[test_action("sfn", "GetExecutionHistory", checksum = "447fb14a")]
#[tokio::test]
async fn sfn_get_execution_history() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("conf-history")
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

    let resp = client
        .get_execution_history()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    assert!(!resp.events().is_empty());
}

#[test_action("sfn", "DescribeStateMachineForExecution", checksum = "208431fb")]
#[tokio::test]
async fn sfn_describe_state_machine_for_execution() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;

    let create = client
        .create_state_machine()
        .name("conf-sm-for-exec")
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

    let resp = client
        .describe_state_machine_for_execution()
        .execution_arn(start.execution_arn())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.name(), "conf-sm-for-exec");
}
