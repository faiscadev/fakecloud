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

// The tests below are behavioral (not conformance-variant) checks for the
// Cubic-flagged semantics: ClientToken idempotency-vs-conflict, rejection of
// unsupported resource types, and List pagination.

#[tokio::test]
async fn cloudcontrol_client_token_idempotency_and_conflict() {
    let server = TestServer::start().await;
    let client = server.cloudcontrol_client().await;

    // Same token + same params -> idempotent replay (same identifier, no dup).
    let first = client
        .create_resource()
        .type_name("AWS::SQS::Queue")
        .desired_state(r#"{"QueueName":"cc-tok"}"#)
        .client_token("tok-1")
        .send()
        .await
        .unwrap();
    let id1 = first
        .progress_event()
        .unwrap()
        .identifier()
        .unwrap()
        .to_string();
    let replay = client
        .create_resource()
        .type_name("AWS::SQS::Queue")
        .desired_state(r#"{"QueueName":"cc-tok"}"#)
        .client_token("tok-1")
        .send()
        .await
        .unwrap();
    assert_eq!(replay.progress_event().unwrap().identifier().unwrap(), id1);

    // Same token + different params -> ClientTokenConflictException.
    let err = client
        .create_resource()
        .type_name("AWS::SQS::Queue")
        .desired_state(r#"{"QueueName":"cc-tok-different"}"#)
        .client_token("tok-1")
        .send()
        .await;
    assert!(
        err.is_err(),
        "reused ClientToken with different params must be rejected"
    );
}

#[tokio::test]
async fn cloudcontrol_rejects_unsupported_type() {
    let server = TestServer::start().await;
    let client = server.cloudcontrol_client().await;
    // Well-formed TypeName that fakecloud has no provisioner for: must NOT be
    // recorded as a phantom resource.
    let resp = client
        .create_resource()
        .type_name("AWS::Fake::Thing")
        .desired_state(r#"{"Name":"x"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.progress_event()
            .unwrap()
            .operation_status()
            .unwrap()
            .as_str(),
        "FAILED"
    );
    // And it is not retrievable.
    let got = client
        .get_resource()
        .type_name("AWS::Fake::Thing")
        .identifier("Resource")
        .send()
        .await;
    assert!(got.is_err());
}

#[tokio::test]
async fn cloudcontrol_list_resources_paginates() {
    let server = TestServer::start().await;
    let client = server.cloudcontrol_client().await;
    create_queue(&client, "cc-page-a").await;
    create_queue(&client, "cc-page-b").await;

    let page1 = client
        .list_resources()
        .type_name("AWS::SQS::Queue")
        .max_results(1)
        .send()
        .await
        .unwrap();
    assert_eq!(page1.resource_descriptions().len(), 1);
    let id1 = page1.resource_descriptions()[0]
        .identifier()
        .unwrap()
        .to_string();
    let next = page1
        .next_token()
        .expect("next token when more remain")
        .to_string();

    let page2 = client
        .list_resources()
        .type_name("AWS::SQS::Queue")
        .max_results(1)
        .next_token(next)
        .send()
        .await
        .unwrap();
    assert_eq!(page2.resource_descriptions().len(), 1);
    let id2 = page2.resource_descriptions()[0]
        .identifier()
        .unwrap()
        .to_string();
    // Pages are distinct and the final page carries no continuation token.
    assert_ne!(id1, id2);
    assert!(page2.next_token().is_none());

    // A NextToken this API never issued yields an empty terminal page (no
    // continuation token), so it can't loop the caller or replay pages.
    let bad = client
        .list_resources()
        .type_name("AWS::SQS::Queue")
        .next_token("not-a-real-token")
        .send()
        .await
        .unwrap();
    assert!(bad.resource_descriptions().is_empty());
    assert!(bad.next_token().is_none());
}
