mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

const SIMPLE_TEMPLATE: &str = r#"{
    "Resources": {
        "MyQueue": {
            "Type": "AWS::SQS::Queue",
            "Properties": {
                "QueueName": "cf-conf-queue"
            }
        }
    }
}"#;

// ---------------------------------------------------------------------------
// Stack lifecycle
// ---------------------------------------------------------------------------

#[test_action("cloudformation", "CreateStack", checksum = "796b3bcd")]
#[test_action("cloudformation", "DescribeStacks", checksum = "ae6b90a4")]
#[test_action("cloudformation", "DeleteStack", checksum = "de60ab3d")]
#[tokio::test]
async fn cloudformation_create_describe_delete_stack() {
    let server = TestServer::start().await;
    let client = server.cloudformation_client().await;

    let result = client
        .create_stack()
        .stack_name("conf-stack")
        .template_body(SIMPLE_TEMPLATE)
        .send()
        .await
        .unwrap();
    assert!(result.stack_id().is_some());

    let desc = client
        .describe_stacks()
        .stack_name("conf-stack")
        .send()
        .await
        .unwrap();
    let stacks = desc.stacks();
    assert_eq!(stacks.len(), 1);
    assert_eq!(stacks[0].stack_name(), Some("conf-stack"));
    assert_eq!(
        stacks[0].stack_status().map(|s| s.as_str()),
        Some("CREATE_COMPLETE")
    );

    client
        .delete_stack()
        .stack_name("conf-stack")
        .send()
        .await
        .unwrap();
}

#[test_action("cloudformation", "ListStacks", checksum = "0462876a")]
#[tokio::test]
async fn cloudformation_list_stacks() {
    let server = TestServer::start().await;
    let client = server.cloudformation_client().await;

    client
        .create_stack()
        .stack_name("list-stack-a")
        .template_body(SIMPLE_TEMPLATE)
        .send()
        .await
        .unwrap();

    let template2 = r#"{
        "Resources": {
            "Q2": {
                "Type": "AWS::SQS::Queue",
                "Properties": { "QueueName": "cf-conf-queue-2" }
            }
        }
    }"#;

    client
        .create_stack()
        .stack_name("list-stack-b")
        .template_body(template2)
        .send()
        .await
        .unwrap();

    let resp = client.list_stacks().send().await.unwrap();
    assert!(resp.stack_summaries().len() >= 2);
}

// ---------------------------------------------------------------------------
// Stack resources
// ---------------------------------------------------------------------------

#[test_action("cloudformation", "ListStackResources", checksum = "471df8aa")]
#[tokio::test]
async fn cloudformation_list_stack_resources() {
    let server = TestServer::start().await;
    let client = server.cloudformation_client().await;

    let template = r#"{
        "Resources": {
            "Queue1": {
                "Type": "AWS::SQS::Queue",
                "Properties": { "QueueName": "cf-res-q1" }
            },
            "Queue2": {
                "Type": "AWS::SQS::Queue",
                "Properties": { "QueueName": "cf-res-q2" }
            }
        }
    }"#;

    client
        .create_stack()
        .stack_name("resources-stack")
        .template_body(template)
        .send()
        .await
        .unwrap();

    let result = client
        .list_stack_resources()
        .stack_name("resources-stack")
        .send()
        .await
        .unwrap();

    let summaries = result.stack_resource_summaries();
    assert_eq!(summaries.len(), 2);

    let logical_ids: Vec<&str> = summaries
        .iter()
        .filter_map(|r| r.logical_resource_id())
        .collect();
    assert!(logical_ids.contains(&"Queue1"));
    assert!(logical_ids.contains(&"Queue2"));
}

#[test_action("cloudformation", "DescribeStackResources", checksum = "74d268a4")]
#[tokio::test]
async fn cloudformation_describe_stack_resources() {
    let server = TestServer::start().await;
    let client = server.cloudformation_client().await;

    client
        .create_stack()
        .stack_name("dsr-stack")
        .template_body(SIMPLE_TEMPLATE)
        .send()
        .await
        .unwrap();

    let result = client
        .describe_stack_resources()
        .stack_name("dsr-stack")
        .send()
        .await
        .unwrap();

    let resources = result.stack_resources();
    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0].logical_resource_id(), Some("MyQueue"));
    assert_eq!(resources[0].resource_type(), Some("AWS::SQS::Queue"));
}

// ---------------------------------------------------------------------------
// UpdateStack
// ---------------------------------------------------------------------------

#[test_action("cloudformation", "UpdateStack", checksum = "46613ba0")]
#[tokio::test]
async fn cloudformation_update_stack() {
    let server = TestServer::start().await;
    let client = server.cloudformation_client().await;

    client
        .create_stack()
        .stack_name("update-stack")
        .template_body(SIMPLE_TEMPLATE)
        .send()
        .await
        .unwrap();

    let template_v2 = r#"{
        "Resources": {
            "NewQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": { "QueueName": "cf-conf-queue-updated" }
            }
        }
    }"#;

    client
        .update_stack()
        .stack_name("update-stack")
        .template_body(template_v2)
        .send()
        .await
        .unwrap();

    let desc = client
        .describe_stacks()
        .stack_name("update-stack")
        .send()
        .await
        .unwrap();
    assert_eq!(
        desc.stacks()[0].stack_status().map(|s| s.as_str()),
        Some("UPDATE_COMPLETE")
    );
}

// ---------------------------------------------------------------------------
// GetTemplate
// ---------------------------------------------------------------------------

#[test_action("cloudformation", "GetTemplate", checksum = "61885956")]
#[tokio::test]
async fn cloudformation_get_template() {
    let server = TestServer::start().await;
    let client = server.cloudformation_client().await;

    let template = r#"{"Resources":{"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"cf-gt-queue"}}}}"#;

    client
        .create_stack()
        .stack_name("gt-stack")
        .template_body(template)
        .send()
        .await
        .unwrap();

    let result = client
        .get_template()
        .stack_name("gt-stack")
        .send()
        .await
        .unwrap();

    let body = result.template_body().unwrap();
    assert!(body.contains("AWS::SQS::Queue"));
    assert!(body.contains("cf-gt-queue"));
}
