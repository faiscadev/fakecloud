use super::*;
use parking_lot::RwLock;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

fn expect_err(result: Result<AwsResponse, AwsServiceError>) -> AwsServiceError {
    match result {
        Err(e) => e,
        Ok(_) => panic!("expected error but got Ok"),
    }
}

fn make_service() -> SqsService {
    let state: SharedSqsState = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4566",
        ),
    ));
    SqsService::new(state)
}

fn make_request(action: &str, body: Value) -> AwsRequest {
    AwsRequest {
        service: "sqs".to_string(),
        action: action.to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test-id".to_string(),
        headers: http::HeaderMap::new(),
        query_params: HashMap::new(),
        body: serde_json::to_vec(&body).unwrap().into(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

#[test]
fn iam_condition_keys_for_send_message_populates_attributes() {
    let svc = make_service();
    let req = make_request(
        "SendMessage",
        json!({
            "QueueUrl": "http://localhost:4566/123456789012/q",
            "MessageBody": "hi",
            "MessageAttributes": {
                "Color": {"DataType": "String", "StringValue": "red"},
                "Priority": {"DataType": "Number", "StringValue": "1"}
            }
        }),
    );
    let action = fakecloud_core::auth::IamAction {
        service: "sqs",
        action: "SendMessage",
        resource: "arn:aws:sqs:us-east-1:123456789012:q".to_string(),
    };
    let keys = svc.iam_condition_keys_for(&req, &action);
    assert_eq!(
        keys.get("sqs:messageattribute.Color"),
        Some(&vec!["red".to_string()])
    );
    assert!(keys.contains_key("sqs:messageattribute.Priority"));
}

#[test]
fn iam_condition_keys_for_send_message_without_attrs_is_empty() {
    let svc = make_service();
    let req = make_request(
        "SendMessage",
        json!({"QueueUrl": "http://localhost:4566/123456789012/q", "MessageBody": "hi"}),
    );
    let action = fakecloud_core::auth::IamAction {
        service: "sqs",
        action: "SendMessage",
        resource: "arn:aws:sqs:us-east-1:123456789012:q".to_string(),
    };
    assert!(svc.iam_condition_keys_for(&req, &action).is_empty());
}

#[test]
fn iam_condition_keys_for_non_send_message_is_empty() {
    let svc = make_service();
    let req = make_request("ReceiveMessage", json!({"QueueUrl": "http://x/q"}));
    let action = fakecloud_core::auth::IamAction {
        service: "sqs",
        action: "ReceiveMessage",
        resource: "arn:aws:sqs:us-east-1:123456789012:q".to_string(),
    };
    assert!(svc.iam_condition_keys_for(&req, &action).is_empty());
}

fn create_queue(svc: &SqsService, name: &str) -> String {
    let req = make_request("CreateQueue", json!({ "QueueName": name }));
    let resp = svc.create_queue(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    body["QueueUrl"].as_str().unwrap().to_string()
}

fn send_msg(svc: &SqsService, queue_url: &str, body_text: &str) -> String {
    let req = make_request(
        "SendMessage",
        json!({ "QueueUrl": queue_url, "MessageBody": body_text }),
    );
    let resp = svc.send_message(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    body["MessageId"].as_str().unwrap().to_string()
}

fn receive_msgs(svc: &SqsService, queue_url: &str, max: u32) -> Vec<Value> {
    let req = make_request(
        "ReceiveMessage",
        json!({
            "QueueUrl": queue_url,
            "MaxNumberOfMessages": max,
            "VisibilityTimeout": 0,
        }),
    );
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let resp = rt.block_on(svc.receive_message(&req)).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    body["Messages"].as_array().cloned().unwrap_or_default()
}

// ── CreateQueue / GetQueueUrl / DeleteQueue / ListQueues ─────────

#[test]
fn create_queue_returns_url() {
    let svc = make_service();
    let url = create_queue(&svc, "my-queue");
    assert!(url.contains("my-queue"));
}

#[test]
fn send_message_accepts_queue_arn() {
    let svc = make_service();
    create_queue(&svc, "arn-queue");
    let arn = "arn:aws:sqs:us-east-1:123456789012:arn-queue";
    let id = send_msg(&svc, arn, "hello");
    assert!(!id.is_empty());

    let messages = receive_msgs(&svc, arn, 1);
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["Body"], "hello");
}

#[test]
fn create_queue_idempotent_same_attributes() {
    let svc = make_service();
    let url1 = create_queue(&svc, "my-queue");
    let url2 = create_queue(&svc, "my-queue");
    assert_eq!(url1, url2);
}

#[test]
fn create_queue_conflict_different_attributes() {
    let svc = make_service();
    let req1 = make_request(
        "CreateQueue",
        json!({
            "QueueName": "my-queue",
            "Attributes": { "VisibilityTimeout": "60" }
        }),
    );
    svc.create_queue(&req1).unwrap();

    let req2 = make_request(
        "CreateQueue",
        json!({
            "QueueName": "my-queue",
            "Attributes": { "VisibilityTimeout": "120" }
        }),
    );
    let err = expect_err(svc.create_queue(&req2));
    assert!(err.to_string().contains("QueueAlreadyExists"));
}

#[test]
fn get_queue_url_existing() {
    let svc = make_service();
    let url = create_queue(&svc, "lookup-queue");
    let req = make_request("GetQueueUrl", json!({ "QueueName": "lookup-queue" }));
    let resp = svc.get_queue_url(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["QueueUrl"].as_str().unwrap(), url);
}

#[test]
fn get_queue_url_nonexistent() {
    let svc = make_service();
    let req = make_request("GetQueueUrl", json!({ "QueueName": "nope" }));
    let err = expect_err(svc.get_queue_url(&req));
    assert!(err
        .to_string()
        .contains("AWS.SimpleQueueService.NonExistentQueue"));
}

#[test]
fn delete_queue_removes_it() {
    let svc = make_service();
    let url = create_queue(&svc, "del-queue");
    let req = make_request("DeleteQueue", json!({ "QueueUrl": url }));
    svc.delete_queue(&req).unwrap();

    let req2 = make_request("GetQueueUrl", json!({ "QueueName": "del-queue" }));
    assert!(svc.get_queue_url(&req2).is_err());
}

#[test]
fn list_queues_all() {
    let svc = make_service();
    create_queue(&svc, "alpha");
    create_queue(&svc, "beta");
    create_queue(&svc, "gamma");

    let req = make_request("ListQueues", json!({}));
    let resp = svc.list_queues(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let urls = body["QueueUrls"].as_array().unwrap();
    assert_eq!(urls.len(), 3);
}

#[test]
fn list_queues_with_prefix() {
    let svc = make_service();
    create_queue(&svc, "prod-orders");
    create_queue(&svc, "prod-events");
    create_queue(&svc, "dev-orders");

    let req = make_request("ListQueues", json!({ "QueueNamePrefix": "prod-" }));
    let resp = svc.list_queues(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let urls = body["QueueUrls"].as_array().unwrap();
    assert_eq!(urls.len(), 2);
    for u in urls {
        assert!(u.as_str().unwrap().contains("prod-"));
    }
}

#[test]
fn list_queues_pagination() {
    let svc = make_service();
    for i in 0..5 {
        create_queue(&svc, &format!("page-queue-{i}"));
    }

    let req = make_request("ListQueues", json!({ "MaxResults": 2 }));
    let resp = svc.list_queues(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let urls = body["QueueUrls"].as_array().unwrap();
    assert_eq!(urls.len(), 2);
    assert!(body["NextToken"].as_str().is_some());

    // Second page
    let token = body["NextToken"].as_str().unwrap();
    let req2 = make_request("ListQueues", json!({ "MaxResults": 2, "NextToken": token }));
    let resp2 = svc.list_queues(&req2).unwrap();
    let body2: Value = serde_json::from_slice(resp2.body.expect_bytes()).unwrap();
    let urls2 = body2["QueueUrls"].as_array().unwrap();
    assert_eq!(urls2.len(), 2);

    // Third page (last 1)
    let token2 = body2["NextToken"].as_str().unwrap();
    let req3 = make_request(
        "ListQueues",
        json!({ "MaxResults": 2, "NextToken": token2 }),
    );
    let resp3 = svc.list_queues(&req3).unwrap();
    let body3: Value = serde_json::from_slice(resp3.body.expect_bytes()).unwrap();
    let urls3 = body3["QueueUrls"].as_array().unwrap();
    assert_eq!(urls3.len(), 1);
    assert!(body3["NextToken"].is_null());
}

// ── SendMessage / ReceiveMessage / DeleteMessage ────────────────

#[test]
fn send_and_receive_message() {
    let svc = make_service();
    let url = create_queue(&svc, "msg-queue");
    send_msg(&svc, &url, "hello world");

    let msgs = receive_msgs(&svc, &url, 1);
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0]["Body"].as_str().unwrap(), "hello world");
    assert!(msgs[0]["MessageId"].as_str().is_some());
    assert!(msgs[0]["ReceiptHandle"].as_str().is_some());
}

#[test]
fn send_message_returns_md5() {
    let svc = make_service();
    let url = create_queue(&svc, "md5-queue");
    let req = make_request(
        "SendMessage",
        json!({ "QueueUrl": url, "MessageBody": "test" }),
    );
    let resp = svc.send_message(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["MD5OfMessageBody"].as_str().is_some());
    assert!(body["MessageId"].as_str().is_some());
    // No system attributes set => no MD5OfMessageSystemAttributes field
    assert!(body.get("MD5OfMessageSystemAttributes").is_none());
}

#[test]
fn send_message_returns_md5_of_system_attributes() {
    let svc = make_service();
    let url = create_queue(&svc, "sysattr-queue");
    let req = make_request(
        "SendMessage",
        json!({
            "QueueUrl": url,
            "MessageBody": "test",
            "MessageSystemAttributes": {
                "AWSTraceHeader": {
                    "DataType": "String",
                    "StringValue": "Root=1-abc-def"
                }
            }
        }),
    );
    let resp = svc.send_message(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let md5 = body["MD5OfMessageSystemAttributes"]
        .as_str()
        .expect("MD5OfMessageSystemAttributes missing");
    assert_eq!(md5.len(), 32);
    assert!(md5.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn receive_empty_queue() {
    let svc = make_service();
    let url = create_queue(&svc, "empty-queue");
    let msgs = receive_msgs(&svc, &url, 10);
    assert!(msgs.is_empty());
}

#[test]
fn receive_respects_max_messages() {
    let svc = make_service();
    let url = create_queue(&svc, "multi-queue");
    for i in 0..5 {
        send_msg(&svc, &url, &format!("msg-{i}"));
    }
    let msgs = receive_msgs(&svc, &url, 3);
    assert_eq!(msgs.len(), 3);
}

#[test]
fn delete_message_removes_from_inflight() {
    let svc = make_service();
    let url = create_queue(&svc, "del-msg-queue");
    send_msg(&svc, &url, "to-delete");

    // Receive with a high visibility timeout so it stays inflight
    let req = make_request(
        "ReceiveMessage",
        json!({
            "QueueUrl": url,
            "MaxNumberOfMessages": 1,
            "VisibilityTimeout": 300,
        }),
    );
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let resp = rt.block_on(svc.receive_message(&req)).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let receipt = body["Messages"][0]["ReceiptHandle"]
        .as_str()
        .unwrap()
        .to_string();

    // Delete it
    let del_req = make_request(
        "DeleteMessage",
        json!({ "QueueUrl": url, "ReceiptHandle": receipt }),
    );
    svc.delete_message(&del_req).unwrap();

    // Receive again with visibility 0 - should be empty
    let msgs = receive_msgs(&svc, &url, 10);
    assert!(msgs.is_empty());
}

#[test]
fn delete_message_invalid_receipt_handle() {
    let svc = make_service();
    let url = create_queue(&svc, "bad-receipt-queue");
    let req = make_request(
        "DeleteMessage",
        json!({ "QueueUrl": url, "ReceiptHandle": "bogus" }),
    );
    let err = expect_err(svc.delete_message(&req));
    assert!(err.to_string().contains("ReceiptHandleIsInvalid"));
}

// ── SendMessageBatch ────────────────────────────────────────────

#[test]
fn send_message_batch_success() {
    let svc = make_service();
    let url = create_queue(&svc, "batch-queue");
    let req = make_request(
        "SendMessageBatch",
        json!({
            "QueueUrl": url,
            "Entries": [
                { "Id": "a", "MessageBody": "first" },
                { "Id": "b", "MessageBody": "second" },
                { "Id": "c", "MessageBody": "third" },
            ]
        }),
    );
    let resp = svc.send_message_batch(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let successful = body["Successful"].as_array().unwrap();
    assert_eq!(successful.len(), 3);

    let ids: Vec<&str> = successful
        .iter()
        .map(|e| e["Id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&"a"));
    assert!(ids.contains(&"b"));
    assert!(ids.contains(&"c"));

    // Verify messages are receivable
    let msgs = receive_msgs(&svc, &url, 10);
    assert_eq!(msgs.len(), 3);
}

#[test]
fn send_message_batch_empty_fails() {
    let svc = make_service();
    let url = create_queue(&svc, "batch-empty-queue");
    let req = make_request(
        "SendMessageBatch",
        json!({ "QueueUrl": url, "Entries": [] }),
    );
    let err = expect_err(svc.send_message_batch(&req));
    assert!(err.to_string().contains("EmptyBatchRequest"));
}

#[test]
fn send_message_batch_duplicate_ids_fails() {
    let svc = make_service();
    let url = create_queue(&svc, "batch-dup-queue");
    let req = make_request(
        "SendMessageBatch",
        json!({
            "QueueUrl": url,
            "Entries": [
                { "Id": "a", "MessageBody": "first" },
                { "Id": "a", "MessageBody": "second" },
            ]
        }),
    );
    let err = expect_err(svc.send_message_batch(&req));
    assert!(err.to_string().contains("BatchEntryIdsNotDistinct"));
}

// ── ChangeMessageVisibility ─────────────────────────────────────

#[test]
fn change_message_visibility() {
    let svc = make_service();
    let url = create_queue(&svc, "vis-queue");
    send_msg(&svc, &url, "visibility-test");

    // Receive with high visibility timeout
    let req = make_request(
        "ReceiveMessage",
        json!({
            "QueueUrl": url,
            "MaxNumberOfMessages": 1,
            "VisibilityTimeout": 300,
        }),
    );
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let resp = rt.block_on(svc.receive_message(&req)).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let receipt = body["Messages"][0]["ReceiptHandle"]
        .as_str()
        .unwrap()
        .to_string();

    // Change visibility to 0 (make immediately visible)
    let cmv_req = make_request(
        "ChangeMessageVisibility",
        json!({
            "QueueUrl": url,
            "ReceiptHandle": receipt,
            "VisibilityTimeout": 0,
        }),
    );
    svc.change_message_visibility(&cmv_req).unwrap();

    // Message should be receivable again immediately
    let msgs = receive_msgs(&svc, &url, 1);
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0]["Body"].as_str().unwrap(), "visibility-test");
}

#[test]
fn change_message_visibility_invalid_receipt() {
    let svc = make_service();
    let url = create_queue(&svc, "vis-bad-queue");
    let req = make_request(
        "ChangeMessageVisibility",
        json!({
            "QueueUrl": url,
            "ReceiptHandle": "invalid-handle",
            "VisibilityTimeout": 0,
        }),
    );
    let err = expect_err(svc.change_message_visibility(&req));
    assert!(err.to_string().contains("ReceiptHandleIsInvalid"));
}

// ── GetQueueAttributes / SetQueueAttributes ─────────────────────

#[test]
fn get_queue_attributes_all() {
    let svc = make_service();
    let url = create_queue(&svc, "attrs-queue");
    let req = make_request(
        "GetQueueAttributes",
        json!({
            "QueueUrl": url,
            "AttributeNames": ["All"],
        }),
    );
    let resp = svc.get_queue_attributes(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let attrs = body["Attributes"].as_object().unwrap();

    assert_eq!(attrs["VisibilityTimeout"].as_str().unwrap(), "30");
    assert_eq!(attrs["DelaySeconds"].as_str().unwrap(), "0");
    assert!(attrs.contains_key("QueueArn"));
    assert!(attrs.contains_key("ApproximateNumberOfMessages"));
}

#[test]
fn get_queue_attributes_specific() {
    let svc = make_service();
    let url = create_queue(&svc, "specific-attrs-queue");
    let req = make_request(
        "GetQueueAttributes",
        json!({
            "QueueUrl": url,
            "AttributeNames": ["VisibilityTimeout", "DelaySeconds"],
        }),
    );
    let resp = svc.get_queue_attributes(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let attrs = body["Attributes"].as_object().unwrap();

    assert!(attrs.contains_key("VisibilityTimeout"));
    assert!(attrs.contains_key("DelaySeconds"));
    assert!(!attrs.contains_key("QueueArn"));
}

#[test]
fn set_queue_attributes_updates_values() {
    let svc = make_service();
    let url = create_queue(&svc, "set-attrs-queue");

    let set_req = make_request(
        "SetQueueAttributes",
        json!({
            "QueueUrl": url,
            "Attributes": { "VisibilityTimeout": "60", "DelaySeconds": "10" },
        }),
    );
    svc.set_queue_attributes(&set_req).unwrap();

    let get_req = make_request(
        "GetQueueAttributes",
        json!({
            "QueueUrl": url,
            "AttributeNames": ["VisibilityTimeout", "DelaySeconds"],
        }),
    );
    let resp = svc.get_queue_attributes(&get_req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let attrs = body["Attributes"].as_object().unwrap();
    assert_eq!(attrs["VisibilityTimeout"].as_str().unwrap(), "60");
    assert_eq!(attrs["DelaySeconds"].as_str().unwrap(), "10");
}

#[test]
fn get_queue_attributes_message_counts() {
    let svc = make_service();
    let url = create_queue(&svc, "count-queue");
    send_msg(&svc, &url, "msg-1");
    send_msg(&svc, &url, "msg-2");

    let req = make_request(
        "GetQueueAttributes",
        json!({
            "QueueUrl": url,
            "AttributeNames": ["ApproximateNumberOfMessages"],
        }),
    );
    let resp = svc.get_queue_attributes(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["Attributes"]["ApproximateNumberOfMessages"]
            .as_str()
            .unwrap(),
        "2"
    );
}

#[test]
fn set_queue_attributes_removes_policy() {
    let svc = make_service();
    let url = create_queue(&svc, "policy-queue");

    let set_req = make_request(
        "SetQueueAttributes",
        json!({
            "QueueUrl": url,
            "Attributes": { "Policy": "{\"Version\":\"2012-10-17\"}" },
        }),
    );
    svc.set_queue_attributes(&set_req).unwrap();

    let remove_req = make_request(
        "SetQueueAttributes",
        json!({
            "QueueUrl": url,
            "Attributes": { "Policy": "" },
        }),
    );
    svc.set_queue_attributes(&remove_req).unwrap();

    let get_req = make_request(
        "GetQueueAttributes",
        json!({
            "QueueUrl": url,
            "AttributeNames": ["All"],
        }),
    );
    let resp = svc.get_queue_attributes(&get_req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let attrs = body["Attributes"].as_object().unwrap();
    assert!(!attrs.contains_key("Policy"));
}

// ── PurgeQueue ──────────────────────────────────────────────────

#[test]
fn purge_queue_removes_all_messages() {
    let svc = make_service();
    let url = create_queue(&svc, "purge-queue");
    send_msg(&svc, &url, "msg-1");
    send_msg(&svc, &url, "msg-2");
    send_msg(&svc, &url, "msg-3");

    let req = make_request("PurgeQueue", json!({ "QueueUrl": url }));
    svc.purge_queue(&req).unwrap();

    let msgs = receive_msgs(&svc, &url, 10);
    assert!(msgs.is_empty());
}

#[test]
fn purge_queue_nonexistent_fails() {
    let svc = make_service();
    let req = make_request(
        "PurgeQueue",
        json!({ "QueueUrl": "http://localhost:4566/123456789012/nope" }),
    );
    assert!(svc.purge_queue(&req).is_err());
}

// ── TagQueue / UntagQueue / ListQueueTags ───────────────────────

#[test]
fn tag_and_list_queue_tags() {
    let svc = make_service();
    let url = create_queue(&svc, "tag-queue");

    let tag_req = make_request(
        "TagQueue",
        json!({
            "QueueUrl": url,
            "Tags": { "env": "prod", "team": "backend" },
        }),
    );
    svc.tag_queue(&tag_req).unwrap();

    let list_req = make_request("ListQueueTags", json!({ "QueueUrl": url }));
    let resp = svc.list_queue_tags(&list_req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let tags = body["Tags"].as_object().unwrap();
    assert_eq!(tags["env"].as_str().unwrap(), "prod");
    assert_eq!(tags["team"].as_str().unwrap(), "backend");
}

#[test]
fn untag_queue_removes_tags() {
    let svc = make_service();
    let url = create_queue(&svc, "untag-queue");

    let tag_req = make_request(
        "TagQueue",
        json!({
            "QueueUrl": url,
            "Tags": { "env": "prod", "team": "backend", "version": "1" },
        }),
    );
    svc.tag_queue(&tag_req).unwrap();

    let untag_req = make_request(
        "UntagQueue",
        json!({
            "QueueUrl": url,
            "TagKeys": ["env", "version"],
        }),
    );
    svc.untag_queue(&untag_req).unwrap();

    let list_req = make_request("ListQueueTags", json!({ "QueueUrl": url }));
    let resp = svc.list_queue_tags(&list_req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let tags = body["Tags"].as_object().unwrap();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags["team"].as_str().unwrap(), "backend");
}

#[test]
fn tag_queue_merges_with_existing() {
    let svc = make_service();
    let url = create_queue(&svc, "merge-tag-queue");

    let tag1 = make_request("TagQueue", json!({ "QueueUrl": url, "Tags": { "a": "1" } }));
    svc.tag_queue(&tag1).unwrap();

    let tag2 = make_request("TagQueue", json!({ "QueueUrl": url, "Tags": { "b": "2" } }));
    svc.tag_queue(&tag2).unwrap();

    let list_req = make_request("ListQueueTags", json!({ "QueueUrl": url }));
    let resp = svc.list_queue_tags(&list_req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let tags = body["Tags"].as_object().unwrap();
    assert_eq!(tags.len(), 2);
    assert_eq!(tags["a"].as_str().unwrap(), "1");
    assert_eq!(tags["b"].as_str().unwrap(), "2");
}

#[test]
fn tag_queue_empty_tags_is_noop() {
    // Real SQS accepts `TagQueue` with an empty `Tags` map as a no-op.
    // The op's Smithy `errors` list (`InvalidAddress`, `InvalidSecurity`,
    // `QueueDoesNotExist`, `RequestThrottled`, `UnsupportedOperation`)
    // doesn't include any "missing parameter" code, and we previously
    // emitted an undeclared `MissingParameter` here.
    let svc = make_service();
    let url = create_queue(&svc, "empty-tag-queue");
    let req = make_request("TagQueue", json!({ "QueueUrl": url, "Tags": {} }));
    let resp = svc.tag_queue(&req).expect("empty Tags should be a no-op");
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body.as_object().map(|o| o.is_empty()).unwrap_or(false));
}

#[test]
fn list_queue_tags_empty_by_default() {
    let svc = make_service();
    let url = create_queue(&svc, "no-tags-queue");
    let req = make_request("ListQueueTags", json!({ "QueueUrl": url }));
    let resp = svc.list_queue_tags(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let tags = body["Tags"].as_object().unwrap();
    assert!(tags.is_empty());
}

// ── CreateQueue with tags and custom attributes ─────────────────

#[test]
fn create_queue_with_tags() {
    let svc = make_service();
    let req = make_request(
        "CreateQueue",
        json!({
            "QueueName": "tagged-at-create",
            "Tags": { "env": "test" },
        }),
    );
    let resp = svc.create_queue(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let url = body["QueueUrl"].as_str().unwrap();

    let list_req = make_request("ListQueueTags", json!({ "QueueUrl": url }));
    let resp = svc.list_queue_tags(&list_req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Tags"]["env"].as_str().unwrap(), "test");
}

#[test]
fn create_queue_with_custom_visibility_timeout() {
    let svc = make_service();
    let req = make_request(
        "CreateQueue",
        json!({
            "QueueName": "custom-vt",
            "Attributes": { "VisibilityTimeout": "45" },
        }),
    );
    let resp = svc.create_queue(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let url = body["QueueUrl"].as_str().unwrap();

    let get_req = make_request(
        "GetQueueAttributes",
        json!({
            "QueueUrl": url,
            "AttributeNames": ["VisibilityTimeout"],
        }),
    );
    let resp = svc.get_queue_attributes(&get_req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["Attributes"]["VisibilityTimeout"].as_str().unwrap(),
        "45"
    );
}

// ── FIFO queue ──────────────────────────────────────────────────

#[test]
fn create_fifo_queue() {
    let svc = make_service();
    let url = create_queue(&svc, "my-queue.fifo");
    assert!(url.contains(".fifo"));

    let req = make_request(
        "GetQueueAttributes",
        json!({
            "QueueUrl": url,
            "AttributeNames": ["FifoQueue"],
        }),
    );
    let resp = svc.get_queue_attributes(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Attributes"]["FifoQueue"].as_str().unwrap(), "true");
}

#[test]
fn fifo_queue_requires_message_group_id() {
    let svc = make_service();
    let url = create_queue(&svc, "strict.fifo");
    let req = make_request(
        "SendMessage",
        json!({
            "QueueUrl": url,
            "MessageBody": "hello",
            "MessageDeduplicationId": "dedup-1",
        }),
    );
    let err = expect_err(svc.send_message(&req));
    assert!(err.to_string().contains("MessageGroupId"));
}

// ── Queue name validation ───────────────────────────────────────

#[test]
fn create_queue_invalid_name() {
    let svc = make_service();
    let req = make_request(
        "CreateQueue",
        json!({ "QueueName": "bad name with spaces" }),
    );
    let err = expect_err(svc.create_queue(&req));
    assert!(err.to_string().contains("InvalidParameterValue"));
}

// ── Message attributes ──────────────────────────────────────────

#[test]
fn send_message_with_attributes() {
    let svc = make_service();
    let url = create_queue(&svc, "msg-attrs-queue");
    let req = make_request(
        "SendMessage",
        json!({
            "QueueUrl": url,
            "MessageBody": "with-attrs",
            "MessageAttributes": {
                "Color": {
                    "DataType": "String",
                    "StringValue": "blue"
                }
            }
        }),
    );
    let resp = svc.send_message(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["MD5OfMessageAttributes"].as_str().is_some());
}

// ── Inflight tracking ───────────────────────────────────────────

#[test]
fn receive_increments_inflight_count() {
    let svc = make_service();
    let url = create_queue(&svc, "inflight-queue");
    send_msg(&svc, &url, "tracked");

    // Receive with high visibility timeout
    let req = make_request(
        "ReceiveMessage",
        json!({
            "QueueUrl": url,
            "MaxNumberOfMessages": 1,
            "VisibilityTimeout": 300,
        }),
    );
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(svc.receive_message(&req)).unwrap();

    let attr_req = make_request(
        "GetQueueAttributes",
        json!({
            "QueueUrl": url,
            "AttributeNames": [
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible"
            ],
        }),
    );
    let resp = svc.get_queue_attributes(&attr_req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let attrs = body["Attributes"].as_object().unwrap();
    assert_eq!(attrs["ApproximateNumberOfMessages"].as_str().unwrap(), "0");
    assert_eq!(
        attrs["ApproximateNumberOfMessagesNotVisible"]
            .as_str()
            .unwrap(),
        "1"
    );
}

// ── Batch additions: permission, batch ops, dead letter ─────────

fn body_json(resp: AwsResponse) -> Value {
    serde_json::from_slice(resp.body.expect_bytes()).unwrap()
}

fn create_queue_url(svc: &SqsService, name: &str) -> String {
    let resp = svc
        .create_queue(&make_request("CreateQueue", json!({ "QueueName": name })))
        .unwrap();
    body_json(resp)["QueueUrl"].as_str().unwrap().to_string()
}

fn send_msg_simple(svc: &SqsService, url: &str, body: &str) {
    svc.send_message(&make_request(
        "SendMessage",
        json!({ "QueueUrl": url, "MessageBody": body }),
    ))
    .unwrap();
}

// ── AddPermission / RemovePermission ─────────────────────────────

#[test]
fn add_permission_builds_policy_and_remove_strips_it() {
    let svc = make_service();
    let url = create_queue_url(&svc, "perm-q");

    let add = make_request(
        "AddPermission",
        json!({
            "QueueUrl": url,
            "Label": "AllowSend",
            "Actions": ["SendMessage"],
            "AWSAccountIds": ["111111111111"]
        }),
    );
    svc.add_permission(&add).unwrap();

    let attrs = svc
        .get_queue_attributes(&make_request(
            "GetQueueAttributes",
            json!({ "QueueUrl": url, "AttributeNames": ["Policy"] }),
        ))
        .unwrap();
    let body = body_json(attrs);
    let policy_str = body["Attributes"]["Policy"].as_str().unwrap();
    let policy: Value = serde_json::from_str(policy_str).unwrap();
    let stmts = policy["Statement"].as_array().unwrap();
    assert_eq!(stmts.len(), 1);
    assert_eq!(stmts[0]["Sid"], json!("AllowSend"));

    let rm = make_request(
        "RemovePermission",
        json!({ "QueueUrl": url, "Label": "AllowSend" }),
    );
    svc.remove_permission(&rm).unwrap();
    let attrs2 = svc
        .get_queue_attributes(&make_request(
            "GetQueueAttributes",
            json!({ "QueueUrl": url, "AttributeNames": ["Policy"] }),
        ))
        .unwrap();
    let body2 = body_json(attrs2);
    let policy2: Value =
        serde_json::from_str(body2["Attributes"]["Policy"].as_str().unwrap()).unwrap();
    assert!(policy2["Statement"].as_array().unwrap().is_empty());
}

#[test]
fn add_permission_empty_actions_rejected() {
    let svc = make_service();
    let url = create_queue_url(&svc, "perm-q2");
    let req = make_request(
        "AddPermission",
        json!({
            "QueueUrl": url,
            "Label": "L",
            "Actions": [],
            "AWSAccountIds": ["111111111111"]
        }),
    );
    assert_eq!(
        expect_err(svc.add_permission(&req)).code(),
        "MissingParameter"
    );
}

#[test]
fn add_permission_rejects_owner_only_actions() {
    let svc = make_service();
    let url = create_queue_url(&svc, "perm-q3");
    let req = make_request(
        "AddPermission",
        json!({
            "QueueUrl": url,
            "Label": "L",
            "Actions": ["DeleteQueue"],
            "AWSAccountIds": ["111111111111"]
        }),
    );
    assert_eq!(
        expect_err(svc.add_permission(&req)).code(),
        "InvalidParameterValue"
    );
}

#[test]
fn add_permission_rejects_duplicate_label() {
    let svc = make_service();
    let url = create_queue_url(&svc, "perm-q4");
    let add = make_request(
        "AddPermission",
        json!({
            "QueueUrl": url,
            "Label": "L",
            "Actions": ["SendMessage"],
            "AWSAccountIds": ["111111111111"]
        }),
    );
    svc.add_permission(&add).unwrap();
    assert_eq!(
        expect_err(svc.add_permission(&add)).code(),
        "InvalidParameterValue"
    );
}

#[test]
fn remove_permission_unknown_label_errors() {
    let svc = make_service();
    let url = create_queue_url(&svc, "perm-q5");
    let req = make_request(
        "RemovePermission",
        json!({ "QueueUrl": url, "Label": "ghost" }),
    );
    assert_eq!(
        expect_err(svc.remove_permission(&req)).code(),
        "InvalidParameterValue"
    );
}

// ── DeleteMessageBatch ───────────────────────────────────────────

#[test]
fn delete_message_batch_removes_listed_messages() {
    let svc = make_service();
    let url = create_queue_url(&svc, "batch-del");
    send_msg_simple(&svc, &url, "a");
    send_msg_simple(&svc, &url, "b");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let recv = rt
        .block_on(svc.receive_message(&make_request(
            "ReceiveMessage",
            json!({ "QueueUrl": url, "MaxNumberOfMessages": 2 }),
        )))
        .unwrap();
    let messages = body_json(recv)["Messages"].as_array().unwrap().clone();
    assert_eq!(messages.len(), 2);

    let entries: Vec<Value> = messages
        .iter()
        .enumerate()
        .map(|(i, m)| {
            json!({
                "Id": format!("e{i}"),
                "ReceiptHandle": m["ReceiptHandle"].as_str().unwrap(),
            })
        })
        .collect();

    let del = svc
        .delete_message_batch(&make_request(
            "DeleteMessageBatch",
            json!({ "QueueUrl": url, "Entries": entries }),
        ))
        .unwrap();
    let body = body_json(del);
    assert_eq!(body["Successful"].as_array().unwrap().len(), 2);
}

// ── ChangeMessageVisibilityBatch ─────────────────────────────────

#[test]
fn change_message_visibility_batch_updates_multiple() {
    let svc = make_service();
    let url = create_queue_url(&svc, "batch-vis");
    send_msg_simple(&svc, &url, "a");
    send_msg_simple(&svc, &url, "b");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let recv = rt
        .block_on(svc.receive_message(&make_request(
            "ReceiveMessage",
            json!({ "QueueUrl": url, "MaxNumberOfMessages": 2 }),
        )))
        .unwrap();
    let messages = body_json(recv)["Messages"].as_array().unwrap().clone();

    let entries: Vec<Value> = messages
        .iter()
        .enumerate()
        .map(|(i, m)| {
            json!({
                "Id": format!("e{i}"),
                "ReceiptHandle": m["ReceiptHandle"].as_str().unwrap(),
                "VisibilityTimeout": 300,
            })
        })
        .collect();

    let resp = svc
        .change_message_visibility_batch(&make_request(
            "ChangeMessageVisibilityBatch",
            json!({ "QueueUrl": url, "Entries": entries }),
        ))
        .unwrap();
    let body = body_json(resp);
    assert_eq!(body["Successful"].as_array().unwrap().len(), 2);
}

/// 1.20: DeleteMessageBatch must reject >10 entries with
/// TooManyEntriesInBatchRequest, like SendMessageBatch.
#[test]
fn delete_message_batch_rejects_over_10_entries() {
    let svc = make_service();
    let url = create_queue_url(&svc, "del-too-many");
    let entries: Vec<Value> = (0..11)
        .map(|i| json!({"Id": format!("e{i}"), "ReceiptHandle": format!("rh{i}")}))
        .collect();
    let err = expect_err(svc.delete_message_batch(&make_request(
        "DeleteMessageBatch",
        json!({ "QueueUrl": url, "Entries": entries }),
    )));
    assert!(
        err.to_string().contains("TooManyEntriesInBatchRequest"),
        "got {err}"
    );
}

/// 1.20: ChangeMessageVisibilityBatch must reject >10 entries.
#[test]
fn change_message_visibility_batch_rejects_over_10_entries() {
    let svc = make_service();
    let url = create_queue_url(&svc, "vis-too-many");
    let entries: Vec<Value> = (0..11)
        .map(|i| {
            json!({"Id": format!("e{i}"), "ReceiptHandle": format!("rh{i}"), "VisibilityTimeout": 30})
        })
        .collect();
    let err = expect_err(svc.change_message_visibility_batch(&make_request(
        "ChangeMessageVisibilityBatch",
        json!({ "QueueUrl": url, "Entries": entries }),
    )));
    assert!(
        err.to_string().contains("TooManyEntriesInBatchRequest"),
        "got {err}"
    );
}

/// 1.20: ChangeMessageVisibilityBatch must reject duplicate batch entry
/// IDs (matches the other batch ops).
#[test]
fn change_message_visibility_batch_rejects_duplicate_ids() {
    let svc = make_service();
    let url = create_queue_url(&svc, "vis-dup");
    let entries = vec![
        json!({"Id": "dup", "ReceiptHandle": "rh1", "VisibilityTimeout": 30}),
        json!({"Id": "dup", "ReceiptHandle": "rh2", "VisibilityTimeout": 30}),
    ];
    let err = expect_err(svc.change_message_visibility_batch(&make_request(
        "ChangeMessageVisibilityBatch",
        json!({ "QueueUrl": url, "Entries": entries }),
    )));
    assert!(
        err.to_string().contains("BatchEntryIdsNotDistinct"),
        "got {err}"
    );
}

/// 1.28: FIFO ReceiveMessage replays the same batch when retried with the
/// same ReceiveRequestAttemptId within the visibility window, instead of
/// returning the next messages.
#[test]
fn fifo_receive_request_attempt_id_replays_same_batch() {
    let svc = make_service();
    let url = create_queue_url(&svc, "rraid.fifo");
    for i in 0..3 {
        svc.send_message(&make_request(
            "SendMessage",
            json!({
                "QueueUrl": url,
                "MessageBody": format!("m{i}"),
                "MessageGroupId": "g1",
                "MessageDeduplicationId": format!("d{i}"),
            }),
        ))
        .unwrap();
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let recv = |attempt: &str| -> Vec<Value> {
        let resp = rt
            .block_on(svc.receive_message(&make_request(
                "ReceiveMessage",
                json!({
                    "QueueUrl": url,
                    "MaxNumberOfMessages": 1,
                    "VisibilityTimeout": 300,
                    "ReceiveRequestAttemptId": attempt,
                }),
            )))
            .unwrap();
        body_json(resp)["Messages"]
            .as_array()
            .cloned()
            .unwrap_or_default()
    };

    let first = recv("attempt-A");
    assert_eq!(first.len(), 1);
    let first_id = first[0]["MessageId"].as_str().unwrap().to_string();

    // Same attempt id -> same message replayed (not the next one).
    let replay = recv("attempt-A");
    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0]["MessageId"].as_str().unwrap(), first_id);

    // A different attempt id is blocked by FIFO group locking (the group
    // still has an inflight message), so it returns nothing - which proves
    // the replay above was the cache, not just a fresh receive.
    let other = recv("attempt-B");
    assert!(
        other.is_empty(),
        "FIFO group is locked by the inflight message"
    );
}

// ── ListDeadLetterSourceQueues ───────────────────────────────────

#[test]
fn list_dead_letter_source_queues_finds_sources() {
    let svc = make_service();
    let dlq_url = create_queue_url(&svc, "dlq");
    let dlq_arn = {
        let resp = svc
            .get_queue_attributes(&make_request(
                "GetQueueAttributes",
                json!({ "QueueUrl": dlq_url, "AttributeNames": ["QueueArn"] }),
            ))
            .unwrap();
        body_json(resp)["Attributes"]["QueueArn"]
            .as_str()
            .unwrap()
            .to_string()
    };

    // Create a source queue that points to the DLQ.
    let src_url = create_queue_url(&svc, "src-q");
    let redrive = json!({ "deadLetterTargetArn": dlq_arn, "maxReceiveCount": "3" }).to_string();
    svc.set_queue_attributes(&make_request(
        "SetQueueAttributes",
        json!({
            "QueueUrl": src_url,
            "Attributes": { "RedrivePolicy": redrive }
        }),
    ))
    .unwrap();

    let resp = svc
        .list_dead_letter_source_queues(&make_request(
            "ListDeadLetterSourceQueues",
            json!({ "QueueUrl": dlq_url }),
        ))
        .unwrap();
    let body = body_json(resp);
    let urls = body["queueUrls"].as_array().unwrap();
    assert_eq!(urls.len(), 1);
    assert!(urls[0].as_str().unwrap().contains("src-q"));
}

#[test]
fn redrive_with_unresolvable_dlq_arn_keeps_message_on_source_queue() {
    // A RedrivePolicy whose deadLetterTargetArn no longer resolves at runtime
    // (the DLQ was deleted after being configured) must not destroy over-limit
    // messages: real SQS keeps them on the source queue. Previously the redrive
    // move silently dropped them. (The DLQ is created + configured first, since
    // SetQueueAttributes now validates the target at config time, then deleted
    // to recreate the unresolvable-at-runtime condition.)
    let svc = make_service();
    let src_url = create_queue_url(&svc, "bad-dlq-src");
    let dlq_url = create_queue_url(&svc, "transient-dlq");
    let dlq_arn = "arn:aws:sqs:us-east-1:123456789012:transient-dlq";
    let redrive = json!({
        "deadLetterTargetArn": dlq_arn,
        "maxReceiveCount": "1"
    })
    .to_string();
    svc.set_queue_attributes(&make_request(
        "SetQueueAttributes",
        json!({ "QueueUrl": src_url, "Attributes": { "RedrivePolicy": redrive } }),
    ))
    .unwrap();
    // Delete the DLQ so the redrive target no longer resolves at runtime.
    svc.delete_queue(&make_request("DeleteQueue", json!({ "QueueUrl": dlq_url })))
        .unwrap();
    send_msg(&svc, &src_url, "survivor");

    // First receive delivers (receive_count 1 == max); the second crosses
    // maxReceiveCount and routes the message through the redrive path.
    assert_eq!(receive_msgs(&svc, &src_url, 1).len(), 1);
    assert_eq!(receive_msgs(&svc, &src_url, 1).len(), 0);

    // The message survives on the source queue instead of vanishing.
    let attrs = body_json(
        svc.get_queue_attributes(&make_request(
            "GetQueueAttributes",
            json!({ "QueueUrl": src_url, "AttributeNames": ["ApproximateNumberOfMessages"] }),
        ))
        .unwrap(),
    );
    assert_eq!(attrs["Attributes"]["ApproximateNumberOfMessages"], "1");

    // Loop-prevention (4.3): the message was returned to source with its
    // receive history reset, so a fresh receive delivers it again for normal
    // reprocessing. Before the fix its receive_count stayed past
    // maxReceiveCount with visible_at=None, so the next receive re-redrove it
    // into the same unresolvable target and spun in a hot loop (0 delivered
    // forever).
    assert_eq!(
        receive_msgs(&svc, &src_url, 1).len(),
        1,
        "returned message must be re-receivable, not hot-looping through redrive"
    );
}

// ── Error branch tests ──

#[test]
fn get_queue_url_not_found() {
    let svc = make_service();
    let req = make_request("GetQueueUrl", json!({"QueueName": "ghost"}));
    let err = expect_err(svc.get_queue_url(&req));
    assert_eq!(err.code(), "AWS.SimpleQueueService.NonExistentQueue");
}

#[test]
fn delete_queue_not_found() {
    let svc = make_service();
    let req = make_request(
        "DeleteQueue",
        json!({"QueueUrl": "http://localhost/123/ghost"}),
    );
    let err = expect_err(svc.delete_queue(&req));
    assert_eq!(err.code(), "AWS.SimpleQueueService.NonExistentQueue");
}

#[test]
fn send_message_queue_not_found() {
    let svc = make_service();
    let req = make_request(
        "SendMessage",
        json!({"QueueUrl": "http://localhost/123/ghost", "MessageBody": "hi"}),
    );
    let err = expect_err(svc.send_message(&req));
    assert_eq!(err.code(), "AWS.SimpleQueueService.NonExistentQueue");
}

#[tokio::test]
async fn receive_message_queue_not_found() {
    let svc = make_service();
    let req = make_request(
        "ReceiveMessage",
        json!({"QueueUrl": "http://localhost/123/ghost"}),
    );
    let err = expect_err(svc.receive_message(&req).await);
    assert_eq!(err.code(), "AWS.SimpleQueueService.NonExistentQueue");
}

#[test]
fn get_queue_attributes_not_found() {
    let svc = make_service();
    let req = make_request(
        "GetQueueAttributes",
        json!({"QueueUrl": "http://localhost/123/ghost", "AttributeNames": ["All"]}),
    );
    let err = expect_err(svc.get_queue_attributes(&req));
    assert_eq!(err.code(), "AWS.SimpleQueueService.NonExistentQueue");
}

#[test]
fn set_queue_attributes_not_found() {
    let svc = make_service();
    let req = make_request(
        "SetQueueAttributes",
        json!({"QueueUrl": "http://localhost/123/ghost", "Attributes": {"VisibilityTimeout": "30"}}),
    );
    let err = expect_err(svc.set_queue_attributes(&req));
    assert_eq!(err.code(), "AWS.SimpleQueueService.NonExistentQueue");
}

#[test]
fn purge_queue_not_found() {
    let svc = make_service();
    let req = make_request(
        "PurgeQueue",
        json!({"QueueUrl": "http://localhost/123/ghost"}),
    );
    let err = expect_err(svc.purge_queue(&req));
    assert_eq!(err.code(), "AWS.SimpleQueueService.NonExistentQueue");
}

#[test]
fn tag_queue_not_found() {
    let svc = make_service();
    let req = make_request(
        "TagQueue",
        json!({"QueueUrl": "http://localhost/123/ghost", "Tags": {"k": "v"}}),
    );
    let err = expect_err(svc.tag_queue(&req));
    assert_eq!(err.code(), "AWS.SimpleQueueService.NonExistentQueue");
}

#[test]
fn untag_queue_not_found() {
    let svc = make_service();
    let req = make_request(
        "UntagQueue",
        json!({"QueueUrl": "http://localhost/123/ghost", "TagKeys": ["k"]}),
    );
    let err = expect_err(svc.untag_queue(&req));
    assert_eq!(err.code(), "AWS.SimpleQueueService.NonExistentQueue");
}

#[test]
fn list_queue_tags_not_found() {
    let svc = make_service();
    let req = make_request(
        "ListQueueTags",
        json!({"QueueUrl": "http://localhost/123/ghost"}),
    );
    let err = expect_err(svc.list_queue_tags(&req));
    assert_eq!(err.code(), "AWS.SimpleQueueService.NonExistentQueue");
}

#[test]
fn change_message_visibility_not_found() {
    let svc = make_service();
    let req = make_request(
        "ChangeMessageVisibility",
        json!({"QueueUrl": "http://localhost/123/ghost", "ReceiptHandle": "rh", "VisibilityTimeout": 30}),
    );
    let err = expect_err(svc.change_message_visibility(&req));
    assert_eq!(err.code(), "AWS.SimpleQueueService.NonExistentQueue");
}

#[test]
fn delete_message_queue_not_found() {
    let svc = make_service();
    let req = make_request(
        "DeleteMessage",
        json!({"QueueUrl": "http://localhost/123/ghost", "ReceiptHandle": "rh"}),
    );
    let err = expect_err(svc.delete_message(&req));
    assert_eq!(err.code(), "AWS.SimpleQueueService.NonExistentQueue");
}

#[test]
fn send_message_missing_body() {
    let svc = make_service();
    let url = create_queue(&svc, "mb-q");
    let req = make_request("SendMessage", json!({"QueueUrl": url}));
    let err = expect_err(svc.send_message(&req));
    assert_eq!(err.code(), "MissingParameter");
}

// ── FIFO queue lifecycle ──

#[test]
fn fifo_queue_create_and_send_with_group_id() {
    let svc = make_service();
    let req = make_request(
        "CreateQueue",
        json!({"QueueName": "test.fifo", "Attributes": {"FifoQueue": "true"}}),
    );
    let resp = svc.create_queue(&req).unwrap();
    let b = body_json(resp);
    let url = b["QueueUrl"].as_str().unwrap().to_string();

    let req = make_request(
        "SendMessage",
        json!({
            "QueueUrl": url,
            "MessageBody": "hi",
            "MessageGroupId": "g1",
            "MessageDeduplicationId": "d1",
        }),
    );
    svc.send_message(&req).unwrap();
}

#[test]
fn fifo_queue_send_without_group_id_fails() {
    let svc = make_service();
    let req = make_request(
        "CreateQueue",
        json!({"QueueName": "fifo2.fifo", "Attributes": {"FifoQueue": "true"}}),
    );
    let resp = svc.create_queue(&req).unwrap();
    let b = body_json(resp);
    let url = b["QueueUrl"].as_str().unwrap().to_string();

    let req = make_request("SendMessage", json!({"QueueUrl": url, "MessageBody": "hi"}));
    assert!(svc.send_message(&req).is_err());
}

// ── Send message batch ──

#[test]
fn send_message_batch_happy_path() {
    let svc = make_service();
    let url = create_queue(&svc, "batch-q");

    let req = make_request(
        "SendMessageBatch",
        json!({
            "QueueUrl": url,
            "Entries": [
                {"Id": "1", "MessageBody": "msg1"},
                {"Id": "2", "MessageBody": "msg2"},
                {"Id": "3", "MessageBody": "msg3"},
            ]
        }),
    );
    let resp = svc.send_message_batch(&req).unwrap();
    let b = body_json(resp);
    assert_eq!(b["Successful"].as_array().unwrap().len(), 3);
}

#[test]
fn send_message_batch_queue_not_found() {
    let svc = make_service();
    let req = make_request(
        "SendMessageBatch",
        json!({
            "QueueUrl": "http://localhost/123/ghost",
            "Entries": [{"Id": "1", "MessageBody": "msg"}]
        }),
    );
    assert!(svc.send_message_batch(&req).is_err());
}

// ── Delete message batch ──

#[tokio::test]
async fn receive_honors_queue_receive_wait_time_default() {
    // ReceiveMessageWaitTimeSeconds on the queue must drive long polling when
    // the request omits WaitTimeSeconds (bug-audit 2026-06-20, 1.24). Empty
    // queue + 1s queue default => the receive blocks ~1s, then returns empty.
    let svc = make_service();
    let url = create_queue(&svc, "longpoll-q");
    svc.set_queue_attributes(&make_request(
        "SetQueueAttributes",
        json!({"QueueUrl": url, "Attributes": {"ReceiveMessageWaitTimeSeconds": "1"}}),
    ))
    .unwrap();

    let start = std::time::Instant::now();
    let resp = svc
        .receive_message(&make_request("ReceiveMessage", json!({"QueueUrl": url})))
        .await
        .unwrap();
    let elapsed = start.elapsed();
    let b = body_json(resp);
    assert!(
        b.get("Messages")
            .and_then(|m| m.as_array())
            .is_none_or(|a| a.is_empty()),
        "empty queue should return no messages"
    );
    assert!(
        elapsed >= std::time::Duration::from_millis(900),
        "receive should have long-polled ~1s from the queue default, took {elapsed:?}"
    );
}

#[tokio::test]
async fn receive_short_polls_when_no_wait_configured() {
    // Control: no queue default and no request WaitTimeSeconds => short poll,
    // returns immediately even when empty.
    let svc = make_service();
    let url = create_queue(&svc, "shortpoll-q");
    let start = std::time::Instant::now();
    svc.receive_message(&make_request("ReceiveMessage", json!({"QueueUrl": url})))
        .await
        .unwrap();
    assert!(
        start.elapsed() < std::time::Duration::from_millis(500),
        "short poll should return promptly"
    );
}

#[tokio::test]
async fn delete_message_batch_removes_messages() {
    let svc = make_service();
    let url = create_queue(&svc, "del-batch-q");

    send_msg_simple(&svc, &url, "msg1");
    send_msg_simple(&svc, &url, "msg2");

    let req = make_request(
        "ReceiveMessage",
        json!({"QueueUrl": url, "MaxNumberOfMessages": 10}),
    );
    let resp = svc.receive_message(&req).await.unwrap();
    let b = body_json(resp);
    let messages = b["Messages"].as_array().unwrap();

    let entries: Vec<Value> = messages
        .iter()
        .enumerate()
        .map(|(i, m)| json!({"Id": format!("d{i}"), "ReceiptHandle": m["ReceiptHandle"].clone()}))
        .collect();

    let req = make_request(
        "DeleteMessageBatch",
        json!({"QueueUrl": url, "Entries": entries}),
    );
    let resp = svc.delete_message_batch(&req).unwrap();
    let b = body_json(resp);
    assert_eq!(b["Successful"].as_array().unwrap().len(), 2);
}

// ── Change message visibility batch ──

#[tokio::test]
async fn change_message_visibility_batch_happy() {
    let svc = make_service();
    let url = create_queue(&svc, "cmvb-q");
    send_msg_simple(&svc, &url, "msg1");

    let req = make_request(
        "ReceiveMessage",
        json!({"QueueUrl": url, "MaxNumberOfMessages": 1}),
    );
    let resp = svc.receive_message(&req).await.unwrap();
    let b = body_json(resp);
    let rh = b["Messages"][0]["ReceiptHandle"]
        .as_str()
        .unwrap()
        .to_string();

    let req = make_request(
        "ChangeMessageVisibilityBatch",
        json!({
            "QueueUrl": url,
            "Entries": [{"Id": "1", "ReceiptHandle": rh, "VisibilityTimeout": 60}]
        }),
    );
    let resp = svc.change_message_visibility_batch(&req).unwrap();
    let b = body_json(resp);
    assert_eq!(b["Successful"].as_array().unwrap().len(), 1);
}

// ── Permissions ──

#[test]
fn add_and_remove_permission() {
    let svc = make_service();
    let url = create_queue(&svc, "perm-q");

    let req = make_request(
        "AddPermission",
        json!({
            "QueueUrl": url,
            "Label": "AllowAll",
            "AWSAccountIds": ["123456789012"],
            "Actions": ["ReceiveMessage"]
        }),
    );
    svc.add_permission(&req).unwrap();

    let req = make_request(
        "RemovePermission",
        json!({"QueueUrl": url, "Label": "AllowAll"}),
    );
    svc.remove_permission(&req).unwrap();
}

#[test]
fn add_permission_empty_actions_list_errors() {
    let svc = make_service();
    let url = create_queue(&svc, "perm-empty-q");

    let req = make_request(
        "AddPermission",
        json!({
            "QueueUrl": url,
            "Label": "L",
            "AWSAccountIds": ["123"],
            "Actions": []
        }),
    );
    assert!(svc.add_permission(&req).is_err());
}

// ── Visibility timeout change ──

#[tokio::test]
async fn change_message_visibility_updates() {
    let svc = make_service();
    let url = create_queue(&svc, "cmv-q");
    send_msg_simple(&svc, &url, "msg1");

    let req = make_request(
        "ReceiveMessage",
        json!({"QueueUrl": url, "MaxNumberOfMessages": 1}),
    );
    let resp = svc.receive_message(&req).await.unwrap();
    let b = body_json(resp);
    let rh = b["Messages"][0]["ReceiptHandle"]
        .as_str()
        .unwrap()
        .to_string();

    let req = make_request(
        "ChangeMessageVisibility",
        json!({"QueueUrl": url, "ReceiptHandle": rh, "VisibilityTimeout": 120}),
    );
    svc.change_message_visibility(&req).unwrap();
}

// ── Message attributes ──

#[tokio::test]
async fn send_and_receive_message_with_attributes() {
    let svc = make_service();
    let url = create_queue(&svc, "attr-q");

    let req = make_request(
        "SendMessage",
        json!({
            "QueueUrl": url,
            "MessageBody": "hello",
            "MessageAttributes": {
                "color": {"DataType": "String", "StringValue": "blue"},
                "count": {"DataType": "Number", "StringValue": "42"}
            }
        }),
    );
    svc.send_message(&req).unwrap();

    let req = make_request(
        "ReceiveMessage",
        json!({
            "QueueUrl": url,
            "MessageAttributeNames": ["All"],
            "MaxNumberOfMessages": 1
        }),
    );
    let resp = svc.receive_message(&req).await.unwrap();
    let b = body_json(resp);
    let msg = &b["Messages"][0];
    assert!(msg["MessageAttributes"].is_object());
}

// ── Delay ──

#[test]
fn send_message_with_delay() {
    let svc = make_service();
    let url = create_queue(&svc, "delay-q");

    let req = make_request(
        "SendMessage",
        json!({"QueueUrl": url, "MessageBody": "delayed", "DelaySeconds": 10}),
    );
    svc.send_message(&req).unwrap();
}

// ── Redrive policy ──

#[test]
fn set_redrive_policy_attribute() {
    let svc = make_service();
    let dlq_url = create_queue(&svc, "dlq");
    let url = create_queue(&svc, "main-q");

    let req = make_request(
        "GetQueueAttributes",
        json!({"QueueUrl": dlq_url, "AttributeNames": ["QueueArn"]}),
    );
    let resp = svc.get_queue_attributes(&req).unwrap();
    let b = body_json(resp);
    let dlq_arn = b["Attributes"]["QueueArn"].as_str().unwrap().to_string();

    let redrive =
        serde_json::json!({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": 3}).to_string();
    let req = make_request(
        "SetQueueAttributes",
        json!({
            "QueueUrl": url,
            "Attributes": {"RedrivePolicy": redrive}
        }),
    );
    svc.set_queue_attributes(&req).unwrap();
}

// ── create queue validation branches ──

#[test]
fn create_queue_name_too_long() {
    let svc = make_service();
    let name = "x".repeat(81);
    let req = make_request("CreateQueue", json!({"QueueName": name}));
    expect_err(svc.create_queue(&req));
}

#[test]
fn create_queue_name_empty() {
    let svc = make_service();
    let req = make_request("CreateQueue", json!({"QueueName": ""}));
    expect_err(svc.create_queue(&req));
}

#[test]
fn create_queue_invalid_chars() {
    let svc = make_service();
    let req = make_request("CreateQueue", json!({"QueueName": "bad name"}));
    expect_err(svc.create_queue(&req));
}

#[test]
fn create_queue_fifo_without_suffix() {
    let svc = make_service();
    let req = make_request(
        "CreateQueue",
        json!({
            "QueueName": "plain",
            "Attributes": {"FifoQueue": "true"}
        }),
    );
    expect_err(svc.create_queue(&req));
}

#[test]
fn create_queue_invalid_max_message_size() {
    let svc = make_service();
    let req = make_request(
        "CreateQueue",
        json!({
            "QueueName": "mms",
            "Attributes": {"MaximumMessageSize": "100"}
        }),
    );
    expect_err(svc.create_queue(&req));
}

#[test]
fn create_queue_invalid_delay_seconds() {
    let svc = make_service();
    let req = make_request(
        "CreateQueue",
        json!({
            "QueueName": "ds",
            "Attributes": {"DelaySeconds": "10000"}
        }),
    );
    expect_err(svc.create_queue(&req));
}

// ── send_message error branches ──

#[test]
fn send_message_missing_queue_url() {
    let svc = make_service();
    let req = make_request("SendMessage", json!({"MessageBody": "hi"}));
    expect_err(svc.send_message(&req));
}

#[test]
fn send_message_queue_not_found_detailed() {
    let svc = make_service();
    let req = make_request(
        "SendMessage",
        json!({
            "QueueUrl": "http://localhost:4566/123456789012/ghost",
            "MessageBody": "hi"
        }),
    );
    expect_err(svc.send_message(&req));
}

#[test]
fn send_message_invalid_delay_seconds() {
    let svc = make_service();
    let req = make_request("CreateQueue", json!({"QueueName": "d"}));
    let resp = svc.create_queue(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let url = body["QueueUrl"].as_str().unwrap().to_string();
    let req = make_request(
        "SendMessage",
        json!({
            "QueueUrl": url,
            "MessageBody": "hi",
            "DelaySeconds": 9999
        }),
    );
    expect_err(svc.send_message(&req));
}

// ── change_message_visibility error branches ──

#[test]
fn change_message_visibility_over_max_errors() {
    let svc = make_service();
    let req = make_request("CreateQueue", json!({"QueueName": "cmv"}));
    let resp = svc.create_queue(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let url = body["QueueUrl"].as_str().unwrap().to_string();
    let req = make_request(
        "ChangeMessageVisibility",
        json!({
            "QueueUrl": url,
            "ReceiptHandle": "bogus",
            "VisibilityTimeout": 99999
        }),
    );
    expect_err(svc.change_message_visibility(&req));
}

// ── delete_message ──

#[test]
fn delete_message_missing_receipt_errors() {
    let svc = make_service();
    let req = make_request("CreateQueue", json!({"QueueName": "dm"}));
    let resp = svc.create_queue(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let url = body["QueueUrl"].as_str().unwrap().to_string();
    let req = make_request("DeleteMessage", json!({"QueueUrl": url}));
    expect_err(svc.delete_message(&req));
}

// ── get_queue_attributes ──

#[test]
fn get_queue_attributes_filtered_by_names() {
    let svc = make_service();
    svc.create_queue(&make_request("CreateQueue", json!({"QueueName": "filt"})))
        .unwrap();
    let req = make_request(
        "GetQueueAttributes",
        json!({
            "QueueUrl": "http://localhost:4566/123456789012/filt",
            "AttributeNames": ["VisibilityTimeout"]
        }),
    );
    let resp = svc.get_queue_attributes(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["Attributes"]["VisibilityTimeout"].is_string());
    assert!(body["Attributes"]["MessageRetentionPeriod"].is_null());
}

// ── set_queue_attributes error branches ──

// ── list_queues pagination/prefix ──

#[test]
fn list_queues_by_prefix_pagination() {
    let svc = make_service();
    for i in 0..10 {
        svc.create_queue(&make_request(
            "CreateQueue",
            json!({"QueueName": format!("pfx-{i}")}),
        ))
        .unwrap();
    }
    svc.create_queue(&make_request("CreateQueue", json!({"QueueName": "other"})))
        .unwrap();
    let req = make_request(
        "ListQueues",
        json!({"QueueNamePrefix": "pfx-", "MaxResults": 3}),
    );
    let resp = svc.list_queues(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["QueueUrls"].as_array().unwrap().len(), 3);
    assert!(body["NextToken"].is_string());
}

// ── tag_queue errors ──

#[test]
fn tag_queue_missing_url_errors() {
    let svc = make_service();
    let req = make_request("TagQueue", json!({"Tags": {"env": "prod"}}));
    expect_err(svc.tag_queue(&req));
}

// ── fifo queue specific errors ──

#[test]
fn send_to_fifo_without_dedup_id_and_no_content_dedup_errors() {
    let svc = make_service();
    svc.create_queue(&make_request(
        "CreateQueue",
        json!({
            "QueueName": "nd.fifo",
            "Attributes": {"FifoQueue": "true"}
        }),
    ))
    .unwrap();
    let req = make_request(
        "SendMessage",
        json!({
            "QueueUrl": "http://localhost:4566/123456789012/nd.fifo",
            "MessageBody": "m",
            "MessageGroupId": "g1"
        }),
    );
    expect_err(svc.send_message(&req));
}

// ── purge queue ──

#[test]
fn purge_queue_missing_url_errors() {
    let svc = make_service();
    let req = make_request("PurgeQueue", json!({}));
    expect_err(svc.purge_queue(&req));
}

// ── delete_message_batch ──

#[test]
fn delete_message_batch_empty_errors() {
    let svc = make_service();
    svc.create_queue(&make_request("CreateQueue", json!({"QueueName": "dmb"})))
        .unwrap();
    let req = make_request(
        "DeleteMessageBatch",
        json!({
            "QueueUrl": "http://localhost:4566/123456789012/dmb",
            "Entries": []
        }),
    );
    expect_err(svc.delete_message_batch(&req));
}

// ── list_dead_letter_source_queues nonexistent ──

#[test]
fn list_dead_letter_source_queues_nonexistent_ok() {
    let svc = make_service();
    svc.create_queue(&make_request("CreateQueue", json!({"QueueName": "dlq"})))
        .unwrap();
    let req = make_request(
        "ListDeadLetterSourceQueues",
        json!({"QueueUrl": "http://localhost:4566/123456789012/dlq"}),
    );
    let resp = svc.list_dead_letter_source_queues(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["queueUrls"].as_array().unwrap().is_empty());
}

#[test]
fn purge_queue_unknown_queue_errors() {
    let svc = make_service();
    let req = make_request(
        "PurgeQueue",
        json!({"QueueUrl": "http://localhost:4566/123456789012/ghost"}),
    );
    assert!(svc.purge_queue(&req).is_err());
}

#[test]
fn get_queue_url_missing_name_errors() {
    let svc = make_service();
    let req = make_request("GetQueueUrl", json!({}));
    assert!(svc.get_queue_url(&req).is_err());
}

#[test]
fn get_queue_attributes_missing_url_errors() {
    let svc = make_service();
    let req = make_request("GetQueueAttributes", json!({}));
    assert!(svc.get_queue_attributes(&req).is_err());
}

#[test]
fn set_queue_attributes_missing_url_errors() {
    let svc = make_service();
    let req = make_request(
        "SetQueueAttributes",
        json!({"Attributes": {"VisibilityTimeout": "60"}}),
    );
    assert!(svc.set_queue_attributes(&req).is_err());
}

#[test]
fn remove_permission_unknown_queue_errors() {
    let svc = make_service();
    let req = make_request(
        "RemovePermission",
        json!({
            "QueueUrl": "http://localhost:4566/123456789012/ghost",
            "Label": "l"
        }),
    );
    assert!(svc.remove_permission(&req).is_err());
}

#[test]
fn untag_queue_missing_url_errors() {
    let svc = make_service();
    let req = make_request("UntagQueue", json!({"TagKeys": ["k"]}));
    assert!(svc.untag_queue(&req).is_err());
}

fn make_query_request(action: &str, params: &[(&str, &str)]) -> AwsRequest {
    let mut qp = HashMap::new();
    for (k, v) in params {
        qp.insert(k.to_string(), v.to_string());
    }
    AwsRequest {
        service: "sqs".to_string(),
        action: action.to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test-id".to_string(),
        headers: http::HeaderMap::new(),
        query_params: qp,
        body: Vec::<u8>::new().into(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: true,
        access_key_id: None,
        principal: None,
    }
}

fn create_dlq_with_source(svc: &SqsService, dlq_name: &str, src_name: &str) -> String {
    let dlq_url = create_queue_url(svc, dlq_name);
    let dlq_arn = body_json(
        svc.get_queue_attributes(&make_request(
            "GetQueueAttributes",
            json!({ "QueueUrl": dlq_url, "AttributeNames": ["QueueArn"] }),
        ))
        .unwrap(),
    )["Attributes"]["QueueArn"]
        .as_str()
        .unwrap()
        .to_string();
    let src_url = create_queue_url(svc, src_name);
    let redrive = json!({ "deadLetterTargetArn": dlq_arn, "maxReceiveCount": "1" }).to_string();
    svc.set_queue_attributes(&make_request(
        "SetQueueAttributes",
        json!({
            "QueueUrl": src_url,
            "Attributes": { "RedrivePolicy": redrive }
        }),
    ))
    .unwrap();
    dlq_arn
}

#[tokio::test]
async fn start_message_move_task_query_protocol_parses_string_rate() {
    let svc = make_service();
    let dlq_arn = create_dlq_with_source(&svc, "qp-dlq", "qp-src");
    // Query protocol passes integers as strings; this should parse fine.
    // The rate-bound path spawns a background mover, so this test runs
    // on the tokio runtime.
    let req = make_query_request(
        "StartMessageMoveTask",
        &[
            ("SourceArn", dlq_arn.as_str()),
            ("MaxNumberOfMessagesPerSecond", "100"),
        ],
    );
    let resp = svc.start_message_move_task(&req).unwrap();
    assert!(resp.status.is_success());
}

#[tokio::test]
async fn start_message_move_task_clamps_out_of_range_rate() {
    // `StartMessageMoveTask`'s declared Smithy errors don't include any
    // "value out of range" code (`InvalidParameterValue` is not in
    // `InvalidAddress` / `InvalidSecurity` / `RequestThrottled` /
    // `ResourceNotFoundException` / `UnsupportedOperation`). Clamp
    // out-of-range rates into the legal `1..=500` window and let the
    // task run.
    let svc = make_service();
    let dlq_arn = create_dlq_with_source(&svc, "oor-dlq", "oor-src");

    let req = make_query_request(
        "StartMessageMoveTask",
        &[
            ("SourceArn", dlq_arn.as_str()),
            ("MaxNumberOfMessagesPerSecond", "0"),
        ],
    );
    let resp = svc.start_message_move_task(&req).unwrap();
    assert!(resp.status.is_success());

    let req = make_query_request(
        "StartMessageMoveTask",
        &[
            ("SourceArn", dlq_arn.as_str()),
            ("MaxNumberOfMessagesPerSecond", "501"),
        ],
    );
    // The second call should fail because the source already has an
    // active task (UnsupportedOperation), not because of the rate.
    let err = expect_err(svc.start_message_move_task(&req));
    assert_eq!(err.code(), "AWS.SimpleQueueService.UnsupportedOperation");
}

#[test]
fn start_message_move_task_rejects_non_dlq_source() {
    let svc = make_service();
    let q_url = create_queue_url(&svc, "lonely");
    let q_arn = body_json(
        svc.get_queue_attributes(&make_request(
            "GetQueueAttributes",
            json!({ "QueueUrl": q_url, "AttributeNames": ["QueueArn"] }),
        ))
        .unwrap(),
    )["Attributes"]["QueueArn"]
        .as_str()
        .unwrap()
        .to_string();
    let req = make_request("StartMessageMoveTask", json!({ "SourceArn": q_arn }));
    let err = expect_err(svc.start_message_move_task(&req));
    assert_eq!(err.code(), "AWS.SimpleQueueService.UnsupportedOperation");
}

#[test]
fn start_message_move_task_rejects_unknown_source() {
    let svc = make_service();
    let req = make_request(
        "StartMessageMoveTask",
        json!({ "SourceArn": "arn:aws:sqs:us-east-1:123456789012:ghost" }),
    );
    let err = expect_err(svc.start_message_move_task(&req));
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn list_message_move_tasks_query_protocol_caps_max_results() {
    let svc = make_service();
    let dlq_arn = create_dlq_with_source(&svc, "lmmt-dlq2", "lmmt-src2");
    // Start a task so there's something to list.
    svc.start_message_move_task(&make_request(
        "StartMessageMoveTask",
        json!({ "SourceArn": dlq_arn }),
    ))
    .unwrap();
    // Query protocol: MaxResults arrives as string. Helper must parse it.
    let req = make_query_request(
        "ListMessageMoveTasks",
        &[("SourceArn", dlq_arn.as_str()), ("MaxResults", "5")],
    );
    let resp = svc.list_message_move_tasks(&req).unwrap();
    assert!(resp.status.is_success());
}

#[test]
fn list_message_move_tasks_rejects_unknown_source() {
    let svc = make_service();
    let req = make_request(
        "ListMessageMoveTasks",
        json!({ "SourceArn": "arn:aws:sqs:us-east-1:123456789012:nope" }),
    );
    let err = expect_err(svc.list_message_move_tasks(&req));
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn cancel_message_move_task_unknown_handle_errors() {
    let svc = make_service();
    let req = make_request(
        "CancelMessageMoveTask",
        json!({ "TaskHandle": "no-such-task" }),
    );
    let err = expect_err(svc.cancel_message_move_task(&req));
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn cancel_message_move_task_completed_task_errors() {
    let svc = make_service();
    let dlq_arn = create_dlq_with_source(&svc, "cmmt-dlq", "cmmt-src");
    let resp = svc
        .start_message_move_task(&make_request(
            "StartMessageMoveTask",
            json!({ "SourceArn": dlq_arn }),
        ))
        .unwrap();
    let handle = body_json(resp)["TaskHandle"].as_str().unwrap().to_string();
    let req = make_request("CancelMessageMoveTask", json!({ "TaskHandle": handle }));
    let err = expect_err(svc.cancel_message_move_task(&req));
    assert_eq!(err.code(), "AWS.SimpleQueueService.UnsupportedOperation");
}

#[test]
fn val_as_i64_handles_number_and_string() {
    assert_eq!(val_as_i64(&json!(42)), Some(42));
    assert_eq!(val_as_i64(&json!("42")), Some(42));
    assert_eq!(val_as_i64(&json!("not-int")), None);
    assert_eq!(val_as_i64(&json!(null)), None);
}

#[test]
fn start_message_move_task_drains_to_explicit_destination() {
    let svc = make_service();
    let dlq_arn = create_dlq_with_source(&svc, "drain-dlq", "drain-src");
    // Pre-stage a couple of messages on the DLQ.
    let dlq_url = body_json(
        svc.get_queue_url(&make_request(
            "GetQueueUrl",
            json!({ "QueueName": "drain-dlq" }),
        ))
        .unwrap(),
    )["QueueUrl"]
        .as_str()
        .unwrap()
        .to_string();
    for body in ["m1", "m2"] {
        svc.send_message(&make_request(
            "SendMessage",
            json!({ "QueueUrl": &dlq_url, "MessageBody": body }),
        ))
        .unwrap();
    }
    // Make a custom destination queue and grab its ARN.
    let dest_url = create_queue_url(&svc, "drain-dest");
    let dest_arn = body_json(
        svc.get_queue_attributes(&make_request(
            "GetQueueAttributes",
            json!({ "QueueUrl": dest_url, "AttributeNames": ["QueueArn"] }),
        ))
        .unwrap(),
    )["Attributes"]["QueueArn"]
        .as_str()
        .unwrap()
        .to_string();
    let resp = svc
        .start_message_move_task(&make_request(
            "StartMessageMoveTask",
            json!({ "SourceArn": dlq_arn, "DestinationArn": dest_arn }),
        ))
        .unwrap();
    assert!(body_json(resp)["TaskHandle"].as_str().is_some());
    // Verify destination queue received both messages.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let recv = runtime
        .block_on(svc.receive_message(&make_request(
            "ReceiveMessage",
            json!({ "QueueUrl": dest_url, "MaxNumberOfMessages": 10 }),
        )))
        .unwrap();
    let body = body_json(recv);
    assert_eq!(body["Messages"].as_array().map(|a| a.len()).unwrap_or(0), 2);
}

#[test]
fn start_message_move_task_unknown_destination_errors() {
    let svc = make_service();
    let dlq_arn = create_dlq_with_source(&svc, "ud-dlq", "ud-src");
    let req = make_request(
        "StartMessageMoveTask",
        json!({
            "SourceArn": dlq_arn,
            "DestinationArn": "arn:aws:sqs:us-east-1:123456789012:no-such-dest"
        }),
    );
    let err = expect_err(svc.start_message_move_task(&req));
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn start_message_move_task_blocks_concurrent_running() {
    let svc = make_service();
    let dlq_arn = create_dlq_with_source(&svc, "conc-dlq", "conc-src");
    // First call completes synchronously (Completed). To exercise the
    // "already running" guard, force a Running entry directly.
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create("123456789012");
        state.message_move_tasks.push(MessageMoveTask {
            task_handle: "FakeCloudMessageMoveTask-running".to_string(),
            source_arn: dlq_arn.clone(),
            destination_arn: None,
            max_messages_per_second: None,
            status: MessageMoveTaskStatus::Running,
            messages_moved: 0,
            messages_to_move: 0,
            started_timestamp: 0,
            failure_reason: None,
            driver_pid: std::process::id(),
            cancel_flag: Arc::new(AtomicBool::new(false)),
        });
    }
    let req = make_request("StartMessageMoveTask", json!({ "SourceArn": dlq_arn }));
    let err = expect_err(svc.start_message_move_task(&req));
    assert_eq!(err.code(), "AWS.SimpleQueueService.UnsupportedOperation");
}

#[test]
fn cancel_message_move_task_cancels_running() {
    let svc = make_service();
    // Insert a Running task; cancellation should succeed and report
    // ApproximateNumberOfMessagesMoved.
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create("123456789012");
        state.message_move_tasks.push(MessageMoveTask {
            task_handle: "running-handle".to_string(),
            source_arn: "arn:aws:sqs:us-east-1:123456789012:src".to_string(),
            destination_arn: None,
            max_messages_per_second: None,
            status: MessageMoveTaskStatus::Running,
            messages_moved: 7,
            messages_to_move: 10,
            started_timestamp: 0,
            failure_reason: None,
            driver_pid: std::process::id(),
            cancel_flag: Arc::new(AtomicBool::new(false)),
        });
    }
    let resp = svc
        .cancel_message_move_task(&make_request(
            "CancelMessageMoveTask",
            json!({ "TaskHandle": "running-handle" }),
        ))
        .unwrap();
    let body = body_json(resp);
    assert_eq!(
        body["ApproximateNumberOfMessagesMoved"].as_u64().unwrap(),
        7
    );
}

#[test]
fn list_message_move_tasks_caps_at_max_and_excludes_running_handle() {
    let svc = make_service();
    let dlq_arn = create_dlq_with_source(&svc, "cap-dlq", "cap-src");
    // Insert 12 tasks; ListMessageMoveTasks should cap MaxResults at 10.
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create("123456789012");
        for i in 0..12 {
            state.message_move_tasks.push(MessageMoveTask {
                task_handle: format!("h{i}"),
                source_arn: dlq_arn.clone(),
                destination_arn: None,
                max_messages_per_second: None,
                status: MessageMoveTaskStatus::Completed,
                messages_moved: 0,
                messages_to_move: 0,
                started_timestamp: i,
                failure_reason: None,
                driver_pid: std::process::id(),
                cancel_flag: Arc::new(AtomicBool::new(false)),
            });
        }
    }
    let req = make_request(
        "ListMessageMoveTasks",
        json!({ "SourceArn": dlq_arn, "MaxResults": 50 }),
    );
    let body = body_json(svc.list_message_move_tasks(&req).unwrap());
    assert_eq!(body["Results"].as_array().unwrap().len(), 10);
    // Completed tasks must not include TaskHandle (per AWS docs).
    for r in body["Results"].as_array().unwrap() {
        assert!(r.get("TaskHandle").is_none());
    }
}

#[test]
fn send_message_batch_over_max_entries_errors() {
    let svc = make_service();
    svc.create_queue(&make_request("CreateQueue", json!({"QueueName": "smbo"})))
        .unwrap();
    let mut entries = Vec::new();
    for i in 0..15 {
        entries.push(json!({"Id": format!("e{i}"), "MessageBody": "x"}));
    }
    let req = make_request(
        "SendMessageBatch",
        json!({
            "QueueUrl": "http://localhost:4566/123456789012/smbo",
            "Entries": entries
        }),
    );
    assert!(svc.send_message_batch(&req).is_err());
}

// ── MaximumMessageSize default + attribute-inclusive sizing (1.3 / 1.13) ──

fn queue_attribute(svc: &SqsService, queue_url: &str, name: &str) -> String {
    let req = make_request(
        "GetQueueAttributes",
        json!({ "QueueUrl": queue_url, "AttributeNames": ["All"] }),
    );
    let resp = svc.get_queue_attributes(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    body["Attributes"][name].as_str().unwrap_or("").to_string()
}

#[test]
fn default_queue_max_message_size_is_256kib() {
    let svc = make_service();
    let url = create_queue(&svc, "default-size");
    // AWS default is 262144 (256 KiB), not 1 MiB (1.3).
    assert_eq!(queue_attribute(&svc, &url, "MaximumMessageSize"), "262144");
}

#[test]
fn default_queue_rejects_300kb_body() {
    let svc = make_service();
    let url = create_queue(&svc, "reject-300kb");
    let big = "x".repeat(300 * 1024); // 307200 bytes > 262144
    let req = make_request(
        "SendMessage",
        json!({ "QueueUrl": url, "MessageBody": big }),
    );
    let err = expect_err(svc.send_message(&req));
    assert_eq!(err.code(), "InvalidParameterValue");
}

#[test]
fn explicit_max_message_size_is_preserved() {
    let svc = make_service();
    let req = make_request(
        "CreateQueue",
        json!({
            "QueueName": "explicit-size",
            "Attributes": { "MaximumMessageSize": "1024" }
        }),
    );
    let resp = svc.create_queue(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let url = body["QueueUrl"].as_str().unwrap().to_string();
    assert_eq!(queue_attribute(&svc, &url, "MaximumMessageSize"), "1024");
}

#[test]
fn message_size_includes_attributes() {
    let svc = make_service();
    // AWS enforces MaximumMessageSize in [1024, 1 MiB], so use the
    // minimum and craft a body just under it.
    let req = make_request(
        "CreateQueue",
        json!({
            "QueueName": "attr-size",
            "Attributes": { "MaximumMessageSize": "1024" }
        }),
    );
    let resp = svc.create_queue(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let url = body["QueueUrl"].as_str().unwrap().to_string();

    // A 1020-byte body alone is under 1024 and accepted.
    let body_text = "x".repeat(1020);
    let ok_req = make_request(
        "SendMessage",
        json!({ "QueueUrl": url, "MessageBody": body_text }),
    );
    assert!(svc.send_message(&ok_req).is_ok());

    // Same 1020-byte body + attr name "k" (1) + type "String" (6) +
    // value "abcdefghij" (10) = 1037 > 1024 -> rejected. Without
    // attribute accounting this would have been accepted (1.13).
    let body_text = "x".repeat(1020);
    let big_req = make_request(
        "SendMessage",
        json!({
            "QueueUrl": url,
            "MessageBody": body_text,
            "MessageAttributes": {
                "k": { "DataType": "String", "StringValue": "abcdefghij" }
            }
        }),
    );
    let err = expect_err(svc.send_message(&big_req));
    assert_eq!(err.code(), "InvalidParameterValue");
}

/// No snapshot store (memory mode) -> no persist hook for the CFN provisioner.
#[test]
fn snapshot_hook_is_none_without_store() {
    let svc = make_service();
    assert!(svc.snapshot_hook().is_none());
}

/// With a store, the hook is present and invoking it runs the whole-state
/// persist path the CloudFormation provisioner uses after mutating SQS
/// state directly.
#[tokio::test]
async fn snapshot_hook_fires_with_store() {
    let store: Arc<dyn fakecloud_persistence::SnapshotStore> =
        Arc::new(fakecloud_persistence::MemorySnapshotStore::new());
    let svc = make_service().with_snapshot_store(store);
    let hook = svc
        .snapshot_hook()
        .expect("hook present when a store is set");
    hook().await;
}

/// A request-level VisibilityTimeout outside 0..=43200 must be rejected with
/// InvalidParameterValue instead of panicking the worker thread (a near-i64::MAX
/// value overflows `now + Duration::seconds(v)`).
#[tokio::test]
async fn receive_message_rejects_out_of_range_visibility_timeout() {
    let svc = make_service();
    let url = create_queue(&svc, "vt-q");
    send_msg(&svc, &url, "hello");

    let huge = make_request(
        "ReceiveMessage",
        json!({ "QueueUrl": url, "VisibilityTimeout": i64::MAX }),
    );
    let err = expect_err(svc.receive_message(&huge).await);
    assert_eq!(err.code(), "InvalidParameterValue");

    let negative = make_request(
        "ReceiveMessage",
        json!({ "QueueUrl": url, "VisibilityTimeout": -1 }),
    );
    let err = expect_err(svc.receive_message(&negative).await);
    assert_eq!(err.code(), "InvalidParameterValue");
}

/// SetQueueAttributes must validate the redrive DLQ target the same way
/// CreateQueue does — Terraform's `aws_sqs_redrive_policy` configures the DLQ
/// through SetQueueAttributes, and an unchecked bad ARN churns messages forever.
#[test]
fn set_queue_attributes_rejects_nonexistent_redrive_target() {
    let svc = make_service();
    let url = create_queue(&svc, "src-q");

    let bad = make_request(
        "SetQueueAttributes",
        json!({
            "QueueUrl": url,
            "Attributes": {
                "RedrivePolicy": "{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:123456789012:ghost-dlq\",\"maxReceiveCount\":3}"
            }
        }),
    );
    let err = expect_err(svc.set_queue_attributes(&bad));
    assert_eq!(err.code(), "AWS.SimpleQueueService.NonExistentQueue");

    // A real DLQ is accepted.
    let dlq_url = create_queue(&svc, "real-dlq");
    let dlq_arn = "arn:aws:sqs:us-east-1:123456789012:real-dlq";
    assert!(dlq_url.ends_with("real-dlq"));
    let good = make_request(
        "SetQueueAttributes",
        json!({
            "QueueUrl": url,
            "Attributes": {
                "RedrivePolicy": format!("{{\"deadLetterTargetArn\":\"{dlq_arn}\",\"maxReceiveCount\":3}}")
            }
        }),
    );
    svc.set_queue_attributes(&good).expect("valid DLQ accepted");
}

/// A message-move task left RUNNING by a previous process (stale driver pid)
/// must be resumed at startup so it doesn't hang forever and the DLQ drain
/// completes.
#[tokio::test]
async fn resume_message_move_tasks_drains_orphaned_running_task() {
    let svc = make_service();
    let dlq_url = create_queue(&svc, "drain-dlq");
    let main_url = create_queue(&svc, "drain-main");
    let dlq_arn = "arn:aws:sqs:us-east-1:123456789012:drain-dlq".to_string();

    // main references dlq as its DLQ, so dlq is a valid move source.
    let set_redrive = make_request(
        "SetQueueAttributes",
        json!({
            "QueueUrl": main_url,
            "Attributes": {
                "RedrivePolicy": format!("{{\"deadLetterTargetArn\":\"{dlq_arn}\",\"maxReceiveCount\":1}}")
            }
        }),
    );
    svc.set_queue_attributes(&set_redrive).unwrap();

    send_msg(&svc, &dlq_url, "m1");
    send_msg(&svc, &dlq_url, "m2");

    // Inject an orphaned RUNNING task (stale pid => left by a prior process).
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create("123456789012");
        state.message_move_tasks.push(MessageMoveTask {
            task_handle: "FakeCloudMessageMoveTask-orphan".to_string(),
            source_arn: dlq_arn.clone(),
            destination_arn: None,
            max_messages_per_second: Some(500),
            status: MessageMoveTaskStatus::Running,
            messages_moved: 0,
            messages_to_move: 2,
            started_timestamp: Utc::now().timestamp_millis(),
            failure_reason: None,
            driver_pid: std::process::id().wrapping_add(1), // not us
            cancel_flag: Arc::new(AtomicBool::new(false)),
        });
    }

    svc.resume_message_move_tasks();

    // Wait for the resumed mover to drain the DLQ into main.
    let mut completed = false;
    for _ in 0..200 {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let accounts = svc.state.read();
        let state = accounts.get("123456789012").unwrap();
        let task = state
            .message_move_tasks
            .iter()
            .find(|t| t.task_handle == "FakeCloudMessageMoveTask-orphan")
            .unwrap();
        if task.status == MessageMoveTaskStatus::Completed {
            completed = true;
            assert_eq!(task.messages_moved, 2);
            assert_eq!(
                state
                    .queues
                    .values()
                    .find(|q| q.arn == dlq_arn)
                    .unwrap()
                    .messages
                    .len(),
                0,
                "source DLQ drained"
            );
            break;
        }
    }
    assert!(completed, "orphaned move task should resume and complete");
}

// ── FIFO DeduplicationScope + SequenceNumber shape ──

fn create_fifo_queue_with_attrs(svc: &SqsService, name: &str, attrs: Value) -> String {
    let mut merged = serde_json::Map::new();
    merged.insert("FifoQueue".to_string(), json!("true"));
    if let Some(obj) = attrs.as_object() {
        for (k, v) in obj {
            merged.insert(k.clone(), v.clone());
        }
    }
    let resp = svc
        .create_queue(&make_request(
            "CreateQueue",
            json!({ "QueueName": name, "Attributes": Value::Object(merged) }),
        ))
        .unwrap();
    body_json(resp)["QueueUrl"].as_str().unwrap().to_string()
}

fn send_fifo(svc: &SqsService, url: &str, body: &str, group: &str, dedup: &str) -> Value {
    let resp = svc
        .send_message(&make_request(
            "SendMessage",
            json!({
                "QueueUrl": url,
                "MessageBody": body,
                "MessageGroupId": group,
                "MessageDeduplicationId": dedup,
            }),
        ))
        .unwrap();
    body_json(resp)
}

/// DeduplicationScope=messageGroup scopes dedup per group: the SAME
/// MessageDeduplicationId in two DIFFERENT groups is not a duplicate, so
/// both messages must be delivered.
#[test]
fn fifo_dedup_scope_message_group_delivers_same_id_across_groups() {
    let svc = make_service();
    let url = create_fifo_queue_with_attrs(
        &svc,
        "scope-mg.fifo",
        json!({ "DeduplicationScope": "messageGroup" }),
    );

    let first = send_fifo(&svc, &url, "from-g1", "g1", "shared-dedup");
    let second = send_fifo(&svc, &url, "from-g2", "g2", "shared-dedup");
    // Distinct groups -> distinct messages (no dedup replay).
    assert_ne!(
        first["MessageId"].as_str().unwrap(),
        second["MessageId"].as_str().unwrap(),
        "same dedup id in different groups must not be deduped under messageGroup scope"
    );

    let msgs = receive_msgs(&svc, &url, 10);
    let bodies: std::collections::HashSet<&str> =
        msgs.iter().filter_map(|m| m["Body"].as_str()).collect();
    assert!(bodies.contains("from-g1"));
    assert!(bodies.contains("from-g2"));
    assert_eq!(bodies.len(), 2, "both group messages delivered");
}

/// Default (queue) DeduplicationScope still dedupes queue-wide: the same
/// dedup id in different groups collapses to the first message.
#[test]
fn fifo_dedup_scope_queue_dedupes_across_groups() {
    let svc = make_service();
    // No DeduplicationScope attr -> AWS default "queue".
    let url = create_fifo_queue_with_attrs(&svc, "scope-q.fifo", json!({}));

    let first = send_fifo(&svc, &url, "from-g1", "g1", "shared-dedup");
    let second = send_fifo(&svc, &url, "from-g2", "g2", "shared-dedup");
    // Queue-wide dedup: the second send replays the first message id.
    assert_eq!(
        first["MessageId"].as_str().unwrap(),
        second["MessageId"].as_str().unwrap(),
        "same dedup id queue-wide replays the original message id"
    );

    let msgs = receive_msgs(&svc, &url, 10);
    assert_eq!(msgs.len(), 1, "queue-scope dedup keeps only one message");
    assert_eq!(msgs[0]["Body"].as_str().unwrap(), "from-g1");
}

/// FIFO SequenceNumber is a large ~20-digit value (like AWS), and strictly
/// increases per queue with send order.
#[test]
fn fifo_sequence_number_is_20_digits_and_monotonic() {
    let svc = make_service();
    let url = create_fifo_queue_with_attrs(&svc, "seq.fifo", json!({}));

    let a = send_fifo(&svc, &url, "a", "g1", "d1");
    let b = send_fifo(&svc, &url, "b", "g1", "d2");

    let seq_a = a["SequenceNumber"].as_str().unwrap();
    let seq_b = b["SequenceNumber"].as_str().unwrap();

    assert_eq!(
        seq_a.len(),
        20,
        "SequenceNumber should be ~20 digits: {seq_a}"
    );
    assert_eq!(
        seq_b.len(),
        20,
        "SequenceNumber should be ~20 digits: {seq_b}"
    );
    assert!(seq_a.chars().all(|c| c.is_ascii_digit()));
    // Monotonic: parse as u128 and compare (lexical also works at equal len).
    let na: u128 = seq_a.parse().unwrap();
    let nb: u128 = seq_b.parse().unwrap();
    assert!(nb > na, "SequenceNumber must increase: {na} -> {nb}");
}

/// ListQueues pagination cursor is stable across a concurrent delete
/// between page fetches: it carries the last queue URL, not a positional
/// offset, so the next page neither skips nor duplicates queues.
#[test]
fn list_queues_token_survives_interleaved_delete() {
    let svc = make_service();
    // Names sort as q0..q4 (URLs share a common prefix, so sort by suffix).
    for i in 0..5 {
        create_queue(&svc, &format!("q{i}"));
    }

    // Page 1: first two (q0, q1).
    let resp = svc
        .list_queues(&make_request("ListQueues", json!({ "MaxResults": 2 })))
        .unwrap();
    let page1 = body_json(resp);
    let urls1: Vec<String> = page1["QueueUrls"]
        .as_array()
        .unwrap()
        .iter()
        .map(|u| u.as_str().unwrap().to_string())
        .collect();
    assert_eq!(urls1.len(), 2);
    assert!(urls1[0].ends_with("/q0"));
    assert!(urls1[1].ends_with("/q1"));
    let token = page1["NextToken"].as_str().unwrap().to_string();

    // Interleave: delete a queue BEFORE the marker (q0). A positional-offset
    // token would now skip q2; the URL marker resumes correctly past q1.
    svc.delete_queue(&make_request(
        "DeleteQueue",
        json!({ "QueueUrl": urls1[0].clone() }),
    ))
    .unwrap();

    // Page 2 with the marker: must return q2, q3 (nothing skipped).
    let resp = svc
        .list_queues(&make_request(
            "ListQueues",
            json!({ "MaxResults": 2, "NextToken": token }),
        ))
        .unwrap();
    let page2 = body_json(resp);
    let urls2: Vec<String> = page2["QueueUrls"]
        .as_array()
        .unwrap()
        .iter()
        .map(|u| u.as_str().unwrap().to_string())
        .collect();
    assert_eq!(urls2.len(), 2, "no queues skipped after interleaved delete");
    assert!(urls2[0].ends_with("/q2"), "resumed at q2, got {}", urls2[0]);
    assert!(urls2[1].ends_with("/q3"));
}

// ── VisibilityTimeout queue-attribute range validation (DoS guard) ──

/// CreateQueue must reject an out-of-range VisibilityTimeout with
/// InvalidAttributeValue rather than storing a value that later overflows the
/// receive-path arithmetic and panics the worker thread.
#[test]
fn create_queue_rejects_out_of_range_visibility_timeout() {
    let svc = make_service();
    let req = make_request(
        "CreateQueue",
        json!({
            "QueueName": "vt-create",
            "Attributes": { "VisibilityTimeout": "9000000000000" }
        }),
    );
    let err = expect_err(svc.create_queue(&req));
    assert_eq!(err.code(), "InvalidAttributeValue");

    // 43201 (one past the 12 h max) is rejected too.
    let req = make_request(
        "CreateQueue",
        json!({ "QueueName": "vt-create2", "Attributes": { "VisibilityTimeout": "43201" } }),
    );
    assert_eq!(
        expect_err(svc.create_queue(&req)).code(),
        "InvalidAttributeValue"
    );
}

/// SetQueueAttributes must reject an out-of-range VisibilityTimeout with
/// InvalidAttributeValue, and a subsequent ReceiveMessage must still succeed
/// (i.e. the bad value was never stored, so nothing panics).
#[tokio::test]
async fn set_queue_attributes_rejects_out_of_range_visibility_timeout_no_panic() {
    let svc = make_service();
    let url = create_queue(&svc, "vt-set");

    // The value that overflowed the receive-path arithmetic before this guard.
    let req = make_request(
        "SetQueueAttributes",
        json!({ "QueueUrl": url, "Attributes": { "VisibilityTimeout": "9000000000000" } }),
    );
    assert_eq!(
        expect_err(svc.set_queue_attributes(&req)).code(),
        "InvalidAttributeValue"
    );

    // 43201 is rejected; the 43200 boundary is accepted.
    let req = make_request(
        "SetQueueAttributes",
        json!({ "QueueUrl": url, "Attributes": { "VisibilityTimeout": "43201" } }),
    );
    assert_eq!(
        expect_err(svc.set_queue_attributes(&req)).code(),
        "InvalidAttributeValue"
    );
    let req = make_request(
        "SetQueueAttributes",
        json!({ "QueueUrl": url, "Attributes": { "VisibilityTimeout": "43200" } }),
    );
    svc.set_queue_attributes(&req).expect("43200 is in range");

    // ReceiveMessage still works — no panic from a stored overflow value.
    send_msg(&svc, &url, "hi");
    let resp = svc
        .receive_message(&make_request("ReceiveMessage", json!({ "QueueUrl": url })))
        .await
        .expect("receive must not panic");
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Messages"].as_array().unwrap().len(), 1);
}

/// After a message's visibility window lapses and it is redelivered under a
/// fresh receipt handle, a DeleteMessage carrying the STALE handle must fail
/// instead of deleting the redelivered copy (AWS at-least-once semantics).
#[tokio::test]
async fn stale_receipt_handle_cannot_delete_redelivered_message() {
    let svc = make_service();
    let url = create_queue(&svc, "stale-rh");
    send_msg(&svc, &url, "m");

    // First receive with VisibilityTimeout=0 -> immediately eligible for
    // redelivery; capture the first handle (rh1).
    let first = svc
        .receive_message(&make_request(
            "ReceiveMessage",
            json!({ "QueueUrl": url, "VisibilityTimeout": 0 }),
        ))
        .await
        .unwrap();
    let b: Value = serde_json::from_slice(first.body.expect_bytes()).unwrap();
    let rh1 = b["Messages"][0]["ReceiptHandle"]
        .as_str()
        .unwrap()
        .to_string();

    // Second receive returns the now-visible message under a NEW handle (rh2);
    // rh1 must be invalidated at this point.
    let second = svc
        .receive_message(&make_request(
            "ReceiveMessage",
            json!({ "QueueUrl": url, "VisibilityTimeout": 30 }),
        ))
        .await
        .unwrap();
    let b2: Value = serde_json::from_slice(second.body.expect_bytes()).unwrap();
    let rh2 = b2["Messages"][0]["ReceiptHandle"]
        .as_str()
        .unwrap()
        .to_string();
    assert_ne!(rh1, rh2, "redelivery must issue a fresh receipt handle");

    // Deleting with the stale handle must fail, not remove the live message.
    let stale = svc.delete_message(&make_request(
        "DeleteMessage",
        json!({ "QueueUrl": url, "ReceiptHandle": rh1 }),
    ));
    assert_eq!(expect_err(stale).code(), "ReceiptHandleIsInvalid");

    // The current handle still deletes the message.
    svc.delete_message(&make_request(
        "DeleteMessage",
        json!({ "QueueUrl": url, "ReceiptHandle": rh2 }),
    ))
    .expect("current handle deletes");
}

/// A SendMessageBatch to a FIFO queue where one entry omits MessageGroupId
/// must report that entry as a per-entry BatchResultErrorEntry (partial
/// success), not abort the whole batch with a request-level 400.
#[test]
fn send_message_batch_fifo_missing_group_id_is_per_entry_failure() {
    let svc = make_service();
    let url = create_queue(&svc, "batch.fifo");

    let req = make_request(
        "SendMessageBatch",
        json!({
            "QueueUrl": url,
            "Entries": [
                { "Id": "a", "MessageBody": "x", "MessageGroupId": "g", "MessageDeduplicationId": "d1" },
                { "Id": "b", "MessageBody": "y" }
            ]
        }),
    );
    let resp = svc.send_message_batch(&req).expect("batch must not 400");
    let body = body_json(resp);
    let successful = body["Successful"].as_array().unwrap();
    let failed = body["Failed"].as_array().unwrap();
    assert_eq!(successful.len(), 1);
    assert_eq!(successful[0]["Id"], "a");
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["Id"], "b");
    assert_eq!(failed[0]["Code"], "MissingParameter");
    assert_eq!(failed[0]["SenderFault"], true);
}

/// A message redriven to a DLQ starts a fresh receive history: its
/// ApproximateReceiveCount resets so a DLQ with its own redrive policy does
/// not immediately re-redrive the message on first receive.
#[tokio::test]
async fn redriven_message_resets_receive_count_in_dlq() {
    let svc = make_service();
    let dlq_url = create_queue(&svc, "reset-dlq");
    let src_url = create_queue(&svc, "reset-src");
    let dlq_arn = "arn:aws:sqs:us-east-1:123456789012:reset-dlq";
    let redrive = json!({ "deadLetterTargetArn": dlq_arn, "maxReceiveCount": "1" }).to_string();
    svc.set_queue_attributes(&make_request(
        "SetQueueAttributes",
        json!({ "QueueUrl": src_url, "Attributes": { "RedrivePolicy": redrive } }),
    ))
    .unwrap();

    send_msg(&svc, &src_url, "over-limit");
    // First receive (count 1 == max) delivers; second crosses max and redrives.
    // Use VisibilityTimeout=0 so the message is immediately eligible again.
    let req1 = make_request(
        "ReceiveMessage",
        json!({ "QueueUrl": src_url, "VisibilityTimeout": 0 }),
    );
    let first = body_json(svc.receive_message(&req1).await.unwrap());
    assert_eq!(first["Messages"].as_array().unwrap().len(), 1);
    let req2 = make_request(
        "ReceiveMessage",
        json!({ "QueueUrl": src_url, "VisibilityTimeout": 0 }),
    );
    let second = body_json(svc.receive_message(&req2).await.unwrap());
    assert_eq!(
        second["Messages"].as_array().map(|m| m.len()).unwrap_or(0),
        0
    );

    // First receive from the DLQ: ApproximateReceiveCount must be "1" (fresh),
    // not carried over from the source queue.
    let resp = svc
        .receive_message(&make_request(
            "ReceiveMessage",
            json!({ "QueueUrl": dlq_url, "AttributeNames": ["ApproximateReceiveCount"] }),
        ))
        .await
        .unwrap();
    let body = body_json(resp);
    let msgs = body["Messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0]["Attributes"]["ApproximateReceiveCount"], "1");
}

/// In a non-`aws` partition (cn / gov-cloud) the queue ARN must carry the
/// region's partition, and that partition-correct ARN must resolve back to the
/// queue on send/receive.
#[test]
fn queue_arn_uses_region_partition_and_resolves() {
    let state: SharedSqsState = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "cn-north-1",
            "http://localhost:4566",
        ),
    ));
    let svc = SqsService::new(state);
    let url = create_queue(&svc, "cn-queue");
    let arn = "arn:aws-cn:sqs:cn-north-1:123456789012:cn-queue";

    // GetQueueAttributes reports the aws-cn ARN.
    let resp = svc
        .get_queue_attributes(&make_request(
            "GetQueueAttributes",
            json!({ "QueueUrl": url, "AttributeNames": ["QueueArn"] }),
        ))
        .unwrap();
    assert_eq!(body_json(resp)["Attributes"]["QueueArn"], arn);

    // Sending via that partition-correct ARN resolves the queue.
    let id = send_msg(&svc, arn, "ni-hao");
    assert!(!id.is_empty());
    let msgs = receive_msgs(&svc, arn, 1);
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0]["Body"], "ni-hao");
}

// ── Host-aware QueueUrl rendering (bug-hunt 1.10) ───────────────

fn make_request_with_host(action: &str, body: Value, host: &str) -> AwsRequest {
    let mut req = make_request(action, body);
    req.headers
        .insert("host", http::HeaderValue::from_str(host).unwrap());
    req
}

#[test]
fn create_queue_url_defaults_to_localhost_without_host_header() {
    // No Host header, no override -> falls back to the boot-time endpoint,
    // preserving the historical `http://localhost:<port>` shape that existing
    // conformance/e2e tests depend on.
    let svc = make_service();
    let resp = svc
        .create_queue(&make_request(
            "CreateQueue",
            json!({ "QueueName": "loc-q" }),
        ))
        .unwrap();
    let url = body_json(resp)["QueueUrl"].as_str().unwrap().to_string();
    assert_eq!(url, "http://localhost:4566/123456789012/loc-q");
}

#[test]
fn create_queue_url_reflects_host_header() {
    // docker-compose / remote setups reach fakecloud under a service name; the
    // returned QueueUrl must be routable from the caller's network.
    let svc = make_service();
    let resp = svc
        .create_queue(&make_request_with_host(
            "CreateQueue",
            json!({ "QueueName": "svc-q" }),
            "fakecloud:4566",
        ))
        .unwrap();
    let url = body_json(resp)["QueueUrl"].as_str().unwrap().to_string();
    assert_eq!(url, "http://fakecloud:4566/123456789012/svc-q");
}

#[test]
fn get_and_list_queue_url_reflect_host_header() {
    let svc = make_service();
    // Create under the default endpoint (stored identity stays localhost).
    create_queue_url(&svc, "host-q");

    // GetQueueUrl reflects the caller's Host.
    let resp = svc
        .get_queue_url(&make_request_with_host(
            "GetQueueUrl",
            json!({ "QueueName": "host-q" }),
            "fakecloud:4566",
        ))
        .unwrap();
    assert_eq!(
        body_json(resp)["QueueUrl"].as_str().unwrap(),
        "http://fakecloud:4566/123456789012/host-q"
    );

    // ListQueues reflects it too.
    let resp = svc
        .list_queues(&make_request_with_host(
            "ListQueues",
            json!({}),
            "fakecloud:4566",
        ))
        .unwrap();
    let urls = body_json(resp)["QueueUrls"].clone();
    assert_eq!(urls, json!(["http://fakecloud:4566/123456789012/host-q"]));
}

#[test]
fn external_url_override_wins_over_host_header() {
    // The explicit external-URL override (FAKECLOUD_EXTERNAL_URL in production,
    // read once at startup) wins over the request Host header. Exercised via the
    // pure resolver with an injected override rather than the process-global env
    // var — under `cargo test` all lib tests share one process, so mutating that
    // env var mid-run would race sibling tests (which is exactly what regressed
    // the host-header/localhost cases in CI).
    let mut headers = http::HeaderMap::new();
    headers.insert("host", http::HeaderValue::from_static("fakecloud:4566"));
    let base = crate::resolve_endpoint_base_with(
        &headers,
        "http://localhost:4566",
        Some("https://sqs.example.test/"),
    );
    assert_eq!(base, "https://sqs.example.test");

    // With no override, the Host header wins over the fallback.
    let base = crate::resolve_endpoint_base_with(&headers, "http://localhost:4566", None);
    assert_eq!(base, "http://fakecloud:4566");
}

#[test]
fn queue_url_host_independent_lookup_still_resolves() {
    // A queue created under one host must still be found when the caller sends
    // its SendMessage against a differently-hosted QueueUrl.
    let svc = make_service();
    create_queue_url(&svc, "lookup-q");
    let id = send_msg(
        &svc,
        "http://some-other-host:9999/123456789012/lookup-q",
        "hi",
    );
    assert!(!id.is_empty());
}

// ── Query-protocol attribute enumeration is contiguous, not capped (1.13) ──

#[test]
fn get_queue_attributes_reads_more_than_twenty_individual_names() {
    // A client enumerating >20 individual AttributeName.N entries must get the
    // whole list back, not a 20-entry prefix.
    let names: Vec<String> = (1..=22).map(|i| format!("Attr{i}")).collect();
    let mut params: Vec<(String, String)> = Vec::new();
    for (idx, name) in names.iter().enumerate() {
        params.push((format!("AttributeName.{}", idx + 1), name.clone()));
    }
    let param_refs: Vec<(&str, &str)> = params
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let req = make_query_request("GetQueueAttributes", &param_refs);
    let parsed = parse_body(&req);
    let out = parsed["AttributeNames"].as_array().unwrap();
    assert_eq!(out.len(), 22);
    assert_eq!(out.last().unwrap(), "Attr22");
}

#[test]
fn set_queue_attributes_reads_more_than_twenty_individual_pairs() {
    let mut params: Vec<(String, String)> = Vec::new();
    for i in 1..=22 {
        params.push((format!("Attribute.{i}.Name"), format!("Name{i}")));
        params.push((format!("Attribute.{i}.Value"), format!("Val{i}")));
    }
    let param_refs: Vec<(&str, &str)> = params
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let req = make_query_request("SetQueueAttributes", &param_refs);
    let parsed = parse_body(&req);
    let attrs = parsed["Attributes"].as_object().unwrap();
    assert_eq!(attrs.len(), 22);
    assert_eq!(attrs.get("Name22").unwrap(), "Val22");
}

#[test]
fn receive_message_parses_query_protocol_message_attribute_names() {
    // Query-protocol ReceiveMessage selects which message attributes to return
    // via MessageAttributeName.N (e.g. MessageAttributeName.1=All). parse_body
    // must surface these into the `MessageAttributeNames` list so the receive
    // handler returns them — otherwise AWS Java SDK v1 / legacy botocore clients
    // get ZERO message attributes back.
    let req = make_query_request(
        "ReceiveMessage",
        &[
            ("QueueUrl", "http://localhost/123456789012/q"),
            ("MessageAttributeName.1", "All"),
        ],
    );
    let parsed = parse_body(&req);
    let names = parsed["MessageAttributeNames"].as_array().unwrap();
    assert_eq!(names.len(), 1);
    assert_eq!(names[0], "All");
}

#[test]
fn receive_message_message_attribute_names_enumeration_is_contiguous() {
    // Multiple named selectors enumerate contiguously and stop at the first gap,
    // matching how SDKs serialize the list.
    let req = make_query_request(
        "ReceiveMessage",
        &[
            ("QueueUrl", "http://localhost/123456789012/q"),
            ("MessageAttributeName.1", "alpha"),
            ("MessageAttributeName.2", "beta"),
            ("MessageAttributeName.4", "delta"),
        ],
    );
    let parsed = parse_body(&req);
    let names = parsed["MessageAttributeNames"].as_array().unwrap();
    assert_eq!(names.len(), 2);
    assert_eq!(names[1], "beta");
}

#[test]
fn attribute_name_enumeration_stops_at_first_gap() {
    // Contiguous enumeration: a hole at index 3 stops the scan (matches how
    // AWS SDKs serialize the list — always 1..N contiguous).
    let req = make_query_request(
        "GetQueueAttributes",
        &[
            ("AttributeName.1", "A"),
            ("AttributeName.2", "B"),
            ("AttributeName.4", "D"),
        ],
    );
    let parsed = parse_body(&req);
    let out = parsed["AttributeNames"].as_array().unwrap();
    assert_eq!(out.len(), 2);
}
