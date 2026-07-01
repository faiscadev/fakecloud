mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

// Cloud Control drives the real CloudFormation resource provisioners, so these
// exercises actually create/read/update/delete a backing AWS::SQS::Queue.

async fn create_queue(client: &aws_sdk_cloudcontrol::Client, name: &str) -> String {
    let resp = client
        .create_resource()
        .type_name("AWS::SQS::Queue")
        .desired_state(format!(r#"{{"QueueName":"{name}"}}"#))
        .send()
        .await
        .unwrap();
    let ev = resp.progress_event().unwrap();
    assert_eq!(ev.operation_status().unwrap().as_str(), "SUCCESS");
    ev.identifier().unwrap().to_string()
}

#[test_action("cloudcontrolapi", "CreateResource", checksum = "f7955b55")]
#[tokio::test]
async fn cloudcontrol_create_resource() {
    let server = TestServer::start().await;
    let client = server.cloudcontrol_client().await;
    let resp = client
        .create_resource()
        .type_name("AWS::SQS::Queue")
        .desired_state(r#"{"QueueName":"cc-create"}"#)
        .send()
        .await
        .unwrap();
    let ev = resp.progress_event().unwrap();
    assert_eq!(ev.operation_status().unwrap().as_str(), "SUCCESS");
    assert!(ev.request_token().is_some());
    assert!(ev.identifier().is_some());
}

#[test_action("cloudcontrolapi", "GetResource", checksum = "452bf6c4")]
#[tokio::test]
async fn cloudcontrol_get_resource() {
    let server = TestServer::start().await;
    let client = server.cloudcontrol_client().await;
    let id = create_queue(&client, "cc-get").await;
    let resp = client
        .get_resource()
        .type_name("AWS::SQS::Queue")
        .identifier(&id)
        .send()
        .await
        .unwrap();
    let desc = resp.resource_description().unwrap();
    assert_eq!(desc.identifier().unwrap(), id);
    assert!(desc.properties().unwrap().contains("cc-get"));
}

#[test_action("cloudcontrolapi", "UpdateResource", checksum = "4e41e422")]
#[tokio::test]
async fn cloudcontrol_update_resource() {
    let server = TestServer::start().await;
    let client = server.cloudcontrol_client().await;
    let id = create_queue(&client, "cc-update").await;
    let resp = client
        .update_resource()
        .type_name("AWS::SQS::Queue")
        .identifier(&id)
        .patch_document(r#"[{"op":"add","path":"/Tags","value":[{"Key":"env","Value":"test"}]}]"#)
        .send()
        .await
        .unwrap();
    let ev = resp.progress_event().unwrap();
    assert_eq!(ev.operation_status().unwrap().as_str(), "SUCCESS");
}

#[test_action("cloudcontrolapi", "DeleteResource", checksum = "e3b5e9a5")]
#[tokio::test]
async fn cloudcontrol_delete_resource() {
    let server = TestServer::start().await;
    let client = server.cloudcontrol_client().await;
    let id = create_queue(&client, "cc-delete").await;
    let resp = client
        .delete_resource()
        .type_name("AWS::SQS::Queue")
        .identifier(&id)
        .send()
        .await
        .unwrap();
    let ev = resp.progress_event().unwrap();
    assert_eq!(ev.operation_status().unwrap().as_str(), "SUCCESS");
    // The resource is gone.
    let err = client
        .get_resource()
        .type_name("AWS::SQS::Queue")
        .identifier(&id)
        .send()
        .await;
    assert!(err.is_err());
}

#[test_action("cloudcontrolapi", "ListResources", checksum = "3ea03a0e")]
#[tokio::test]
async fn cloudcontrol_list_resources() {
    let server = TestServer::start().await;
    let client = server.cloudcontrol_client().await;
    let id = create_queue(&client, "cc-list").await;
    let resp = client
        .list_resources()
        .type_name("AWS::SQS::Queue")
        .send()
        .await
        .unwrap();
    assert!(resp
        .resource_descriptions()
        .iter()
        .any(|d| d.identifier() == Some(id.as_str())));
}

#[test_action("cloudcontrolapi", "GetResourceRequestStatus", checksum = "0874714d")]
#[tokio::test]
async fn cloudcontrol_get_resource_request_status() {
    let server = TestServer::start().await;
    let client = server.cloudcontrol_client().await;
    let resp = client
        .create_resource()
        .type_name("AWS::SQS::Queue")
        .desired_state(r#"{"QueueName":"cc-status"}"#)
        .send()
        .await
        .unwrap();
    let token = resp
        .progress_event()
        .unwrap()
        .request_token()
        .unwrap()
        .to_string();
    let status = client
        .get_resource_request_status()
        .request_token(&token)
        .send()
        .await
        .unwrap();
    let ev = status.progress_event().unwrap();
    assert_eq!(ev.request_token().unwrap(), token);
    assert_eq!(ev.operation_status().unwrap().as_str(), "SUCCESS");
}

#[test_action("cloudcontrolapi", "ListResourceRequests", checksum = "f73d5048")]
#[tokio::test]
async fn cloudcontrol_list_resource_requests() {
    let server = TestServer::start().await;
    let client = server.cloudcontrol_client().await;
    create_queue(&client, "cc-req").await;
    let resp = client.list_resource_requests().send().await.unwrap();
    assert!(!resp.resource_request_status_summaries().is_empty());
}

#[test_action("cloudcontrolapi", "CancelResourceRequest", checksum = "687f7836")]
#[tokio::test]
async fn cloudcontrol_cancel_resource_request() {
    let server = TestServer::start().await;
    let client = server.cloudcontrol_client().await;
    let resp = client
        .create_resource()
        .type_name("AWS::SQS::Queue")
        .desired_state(r#"{"QueueName":"cc-cancel"}"#)
        .send()
        .await
        .unwrap();
    let token = resp
        .progress_event()
        .unwrap()
        .request_token()
        .unwrap()
        .to_string();
    // Request is already terminal (SUCCESS); cancel echoes its final event.
    let resp = client
        .cancel_resource_request()
        .request_token(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.progress_event().unwrap().request_token().unwrap(),
        token
    );
}
