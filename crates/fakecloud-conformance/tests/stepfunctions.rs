mod helpers;

use aws_sdk_sfn::types::Tag;
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

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
