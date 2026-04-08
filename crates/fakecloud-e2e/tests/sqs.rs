mod helpers;

use aws_sdk_sqs::types::{
    ChangeMessageVisibilityBatchRequestEntry, DeleteMessageBatchRequestEntry,
    MessageAttributeValue, QueueAttributeName, SendMessageBatchRequestEntry,
};
use helpers::TestServer;
use std::time::Duration;

#[tokio::test]
async fn sqs_create_list_delete_queue() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    // Create
    let resp = client
        .create_queue()
        .queue_name("test-queue")
        .send()
        .await
        .unwrap();
    let queue_url = resp.queue_url().unwrap().to_string();
    assert!(queue_url.contains("test-queue"));

    // List
    let resp = client.list_queues().send().await.unwrap();
    let urls = resp.queue_urls();
    assert_eq!(urls.len(), 1);
    assert_eq!(urls[0], queue_url);

    // Delete
    client
        .delete_queue()
        .queue_url(&queue_url)
        .send()
        .await
        .unwrap();

    let resp = client.list_queues().send().await.unwrap();
    assert_eq!(resp.queue_urls().len(), 0);
}

#[tokio::test]
async fn sqs_send_receive_delete_message() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    let resp = client
        .create_queue()
        .queue_name("msg-queue")
        .send()
        .await
        .unwrap();
    let queue_url = resp.queue_url().unwrap().to_string();

    // Send
    client
        .send_message()
        .queue_url(&queue_url)
        .message_body("hello world")
        .send()
        .await
        .unwrap();

    // Receive
    let resp = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();

    let messages = resp.messages();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].body().unwrap(), "hello world");

    let receipt_handle = messages[0].receipt_handle().unwrap().to_string();

    // Delete message
    client
        .delete_message()
        .queue_url(&queue_url)
        .receipt_handle(&receipt_handle)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn sqs_multiple_messages() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    let resp = client
        .create_queue()
        .queue_name("multi-queue")
        .send()
        .await
        .unwrap();
    let queue_url = resp.queue_url().unwrap().to_string();

    for i in 0..5 {
        client
            .send_message()
            .queue_url(&queue_url)
            .message_body(format!("message {i}"))
            .send()
            .await
            .unwrap();
    }

    let resp = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.messages().len(), 5);
}

#[tokio::test]
async fn sqs_get_queue_url() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    let create_resp = client
        .create_queue()
        .queue_name("url-queue")
        .send()
        .await
        .unwrap();
    let expected_url = create_resp.queue_url().unwrap();

    let resp = client
        .get_queue_url()
        .queue_name("url-queue")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.queue_url().unwrap(), expected_url);
}

#[tokio::test]
async fn sqs_purge_queue() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    let resp = client
        .create_queue()
        .queue_name("purge-queue")
        .send()
        .await
        .unwrap();
    let queue_url = resp.queue_url().unwrap().to_string();

    client
        .send_message()
        .queue_url(&queue_url)
        .message_body("to be purged")
        .send()
        .await
        .unwrap();

    client
        .purge_queue()
        .queue_url(&queue_url)
        .send()
        .await
        .unwrap();

    let resp = client
        .receive_message()
        .queue_url(&queue_url)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.messages().len(), 0);
}

#[tokio::test]
async fn sqs_create_queue_idempotent() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    let resp1 = client
        .create_queue()
        .queue_name("idempotent-queue")
        .send()
        .await
        .unwrap();
    let resp2 = client
        .create_queue()
        .queue_name("idempotent-queue")
        .send()
        .await
        .unwrap();

    assert_eq!(resp1.queue_url().unwrap(), resp2.queue_url().unwrap());
}

#[tokio::test]
async fn sqs_cli_create_and_list() {
    let server = TestServer::start().await;

    let output = server
        .aws_cli(&["sqs", "create-queue", "--queue-name", "cli-queue"])
        .await;
    assert!(output.success(), "create failed: {}", output.stderr_text());

    let output = server.aws_cli(&["sqs", "list-queues"]).await;
    assert!(output.success(), "list failed: {}", output.stderr_text());
    let json = output.stdout_json();
    let urls = json["QueueUrls"].as_array().unwrap();
    assert_eq!(urls.len(), 1);
    assert!(urls[0].as_str().unwrap().contains("cli-queue"));
}

#[tokio::test]
async fn sqs_send_message_batch() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    let resp = client
        .create_queue()
        .queue_name("batch-send-queue")
        .send()
        .await
        .unwrap();
    let queue_url = resp.queue_url().unwrap().to_string();

    let entries: Vec<SendMessageBatchRequestEntry> = (0..3)
        .map(|i| {
            SendMessageBatchRequestEntry::builder()
                .id(format!("msg-{i}"))
                .message_body(format!("batch message {i}"))
                .build()
                .unwrap()
        })
        .collect();

    let batch_resp = client
        .send_message_batch()
        .queue_url(&queue_url)
        .set_entries(Some(entries))
        .send()
        .await
        .unwrap();

    assert_eq!(batch_resp.successful().len(), 3);
    assert!(batch_resp.failed().is_empty());

    // Verify all messages are receivable
    let recv_resp = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();
    assert_eq!(recv_resp.messages().len(), 3);
}

#[tokio::test]
async fn sqs_delete_message_batch() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    let resp = client
        .create_queue()
        .queue_name("batch-delete-queue")
        .send()
        .await
        .unwrap();
    let queue_url = resp.queue_url().unwrap().to_string();

    // Send 3 messages
    for i in 0..3 {
        client
            .send_message()
            .queue_url(&queue_url)
            .message_body(format!("delete me {i}"))
            .send()
            .await
            .unwrap();
    }

    // Receive all messages
    let recv_resp = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();
    let messages = recv_resp.messages();
    assert_eq!(messages.len(), 3);

    // Delete them in a batch
    let entries: Vec<DeleteMessageBatchRequestEntry> = messages
        .iter()
        .enumerate()
        .map(|(i, m)| {
            DeleteMessageBatchRequestEntry::builder()
                .id(format!("del-{i}"))
                .receipt_handle(m.receipt_handle().unwrap())
                .build()
                .unwrap()
        })
        .collect();

    let del_resp = client
        .delete_message_batch()
        .queue_url(&queue_url)
        .set_entries(Some(entries))
        .send()
        .await
        .unwrap();

    assert_eq!(del_resp.successful().len(), 3);
    assert!(del_resp.failed().is_empty());
}

#[tokio::test]
async fn sqs_set_queue_attributes() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    let resp = client
        .create_queue()
        .queue_name("attrs-queue")
        .send()
        .await
        .unwrap();
    let queue_url = resp.queue_url().unwrap().to_string();

    // Set attributes
    client
        .set_queue_attributes()
        .queue_url(&queue_url)
        .attributes(QueueAttributeName::VisibilityTimeout, "60")
        .attributes(QueueAttributeName::DelaySeconds, "5")
        .send()
        .await
        .unwrap();

    // Get and verify
    let attrs_resp = client
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(QueueAttributeName::All)
        .send()
        .await
        .unwrap();

    let attrs = attrs_resp.attributes().unwrap();
    assert_eq!(
        attrs.get(&QueueAttributeName::VisibilityTimeout).unwrap(),
        "60"
    );
    assert_eq!(attrs.get(&QueueAttributeName::DelaySeconds).unwrap(), "5");
}

#[tokio::test]
async fn sqs_message_attributes() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    let resp = client
        .create_queue()
        .queue_name("msg-attrs-queue")
        .send()
        .await
        .unwrap();
    let queue_url = resp.queue_url().unwrap().to_string();

    // Send message with attributes
    let attr_value = MessageAttributeValue::builder()
        .data_type("String")
        .string_value("test-value")
        .build()
        .unwrap();

    client
        .send_message()
        .queue_url(&queue_url)
        .message_body("hello with attrs")
        .message_attributes("my-attr", attr_value)
        .send()
        .await
        .unwrap();

    // Receive and verify attributes
    let recv_resp = client
        .receive_message()
        .queue_url(&queue_url)
        .message_attribute_names("All")
        .send()
        .await
        .unwrap();

    let messages = recv_resp.messages();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].body().unwrap(), "hello with attrs");

    let msg_attrs = messages[0].message_attributes().unwrap();
    let attr = msg_attrs.get("my-attr").unwrap();
    assert_eq!(attr.data_type(), "String");
    assert_eq!(attr.string_value().unwrap(), "test-value");
}

#[tokio::test]
async fn sqs_fifo_queue_ordering() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    let resp = client
        .create_queue()
        .queue_name("ordering.fifo")
        .attributes(QueueAttributeName::FifoQueue, "true")
        .attributes(QueueAttributeName::ContentBasedDeduplication, "true")
        .send()
        .await
        .unwrap();
    let queue_url = resp.queue_url().unwrap().to_string();

    // Send messages with MessageGroupId
    for i in 0..3 {
        client
            .send_message()
            .queue_url(&queue_url)
            .message_body(format!("fifo-msg-{i}"))
            .message_group_id("group-1")
            .send()
            .await
            .unwrap();
    }

    let recv = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();

    let messages = recv.messages();
    assert_eq!(messages.len(), 3);
    assert_eq!(messages[0].body().unwrap(), "fifo-msg-0");
    assert_eq!(messages[1].body().unwrap(), "fifo-msg-1");
    assert_eq!(messages[2].body().unwrap(), "fifo-msg-2");
}

#[tokio::test]
async fn sqs_fifo_queue_missing_group_id() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    let resp = client
        .create_queue()
        .queue_name("no-group.fifo")
        .attributes(QueueAttributeName::FifoQueue, "true")
        .attributes(QueueAttributeName::ContentBasedDeduplication, "true")
        .send()
        .await
        .unwrap();
    let queue_url = resp.queue_url().unwrap().to_string();

    // Send without MessageGroupId should fail
    let result = client
        .send_message()
        .queue_url(&queue_url)
        .message_body("should fail")
        .send()
        .await;

    assert!(
        result.is_err(),
        "Expected error when missing MessageGroupId on FIFO queue"
    );
}

#[tokio::test]
async fn sqs_long_polling_wait_time_seconds() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    let resp = client
        .create_queue()
        .queue_name("longpoll-queue")
        .send()
        .await
        .unwrap();
    let queue_url = resp.queue_url().unwrap().to_string();

    // Spawn a task that sends a message after a short delay
    let send_client = server.sqs_client().await;
    let send_url = queue_url.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        send_client
            .send_message()
            .queue_url(&send_url)
            .message_body("delayed hello")
            .send()
            .await
            .unwrap();
    });

    // Use WaitTimeSeconds to long poll - should pick up the message
    let recv = client
        .receive_message()
        .queue_url(&queue_url)
        .wait_time_seconds(5)
        .send()
        .await
        .unwrap();

    let messages = recv.messages();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].body().unwrap(), "delayed hello");
}

#[tokio::test]
async fn sqs_change_message_visibility_batch() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    let resp = client
        .create_queue()
        .queue_name("cmvb-queue")
        .attributes(QueueAttributeName::VisibilityTimeout, "30")
        .send()
        .await
        .unwrap();
    let queue_url = resp.queue_url().unwrap().to_string();

    // Send 3 messages
    for i in 0..3 {
        client
            .send_message()
            .queue_url(&queue_url)
            .message_body(format!("vis-msg-{i}"))
            .send()
            .await
            .unwrap();
    }

    // Receive all messages (they become inflight)
    let recv_resp = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();
    let messages = recv_resp.messages();
    assert_eq!(messages.len(), 3);

    // Change visibility of all messages in a batch (set to 0 to make them immediately visible)
    let entries: Vec<ChangeMessageVisibilityBatchRequestEntry> = messages
        .iter()
        .enumerate()
        .map(|(i, m)| {
            ChangeMessageVisibilityBatchRequestEntry::builder()
                .id(format!("chg-{i}"))
                .receipt_handle(m.receipt_handle().unwrap())
                .visibility_timeout(0)
                .build()
                .unwrap()
        })
        .collect();

    let batch_resp = client
        .change_message_visibility_batch()
        .queue_url(&queue_url)
        .set_entries(Some(entries))
        .send()
        .await
        .unwrap();

    assert_eq!(batch_resp.successful().len(), 3);
    assert!(batch_resp.failed().is_empty());

    // Messages should now be visible again since visibility was set to 0
    let recv_resp2 = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();
    assert_eq!(recv_resp2.messages().len(), 3);
}

/// Fair Queues: when multiple message groups exist on a standard queue,
/// messages from groups with fewer in-flight messages are prioritized.
#[tokio::test]
async fn sqs_fair_queues_prioritize_quiet_groups() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    let resp = client
        .create_queue()
        .queue_name("fair-queue")
        .send()
        .await
        .unwrap();
    let queue_url = resp.queue_url().unwrap().to_string();

    // Send 5 messages from "noisy-tenant" and 1 from "quiet-tenant"
    for i in 0..5 {
        client
            .send_message()
            .queue_url(&queue_url)
            .message_body(format!("noisy-{i}"))
            .message_group_id("noisy-tenant")
            .send()
            .await
            .unwrap();
    }
    client
        .send_message()
        .queue_url(&queue_url)
        .message_body("quiet-0")
        .message_group_id("quiet-tenant")
        .send()
        .await
        .unwrap();

    // Receive 3 messages (simulating first consumer batch) — puts them in-flight
    let batch1 = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(3)
        .send()
        .await
        .unwrap();
    assert_eq!(batch1.messages().len(), 3);

    // Now noisy-tenant has 3 in-flight, quiet-tenant has 0.
    // Next receive should prioritize quiet-tenant's message.
    let batch2 = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(1)
        .send()
        .await
        .unwrap();
    assert_eq!(batch2.messages().len(), 1);
    assert_eq!(
        batch2.messages()[0].body().unwrap(),
        "quiet-0",
        "fair queues should prioritize the quiet tenant's message"
    );
}

#[tokio::test]
async fn sqs_delete_nonexistent_queue_fails() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    let result = client
        .delete_queue()
        .queue_url("http://localhost:4566/000000000000/no-such-queue")
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn sqs_get_queue_url_nonexistent_fails() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    let result = client
        .get_queue_url()
        .queue_name("nonexistent-queue")
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn sqs_tag_untag_list_queue_tags() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    let resp = client
        .create_queue()
        .queue_name("tag-queue")
        .send()
        .await
        .unwrap();
    let queue_url = resp.queue_url().unwrap().to_string();

    // Tag the queue
    client
        .tag_queue()
        .queue_url(&queue_url)
        .tags("env", "prod")
        .tags("team", "backend")
        .send()
        .await
        .unwrap();

    // List tags
    let tags_resp = client
        .list_queue_tags()
        .queue_url(&queue_url)
        .send()
        .await
        .unwrap();
    let tags = tags_resp.tags().unwrap();
    assert_eq!(tags.get("env").unwrap(), "prod");
    assert_eq!(tags.get("team").unwrap(), "backend");

    // Untag
    client
        .untag_queue()
        .queue_url(&queue_url)
        .tag_keys("team")
        .send()
        .await
        .unwrap();

    let tags_resp = client
        .list_queue_tags()
        .queue_url(&queue_url)
        .send()
        .await
        .unwrap();
    let tags = tags_resp.tags().unwrap();
    assert_eq!(tags.len(), 1);
    assert!(tags.get("team").is_none());
}

#[tokio::test]
async fn sqs_fifo_deduplication() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    let resp = client
        .create_queue()
        .queue_name("dedup.fifo")
        .attributes(QueueAttributeName::FifoQueue, "true")
        .send()
        .await
        .unwrap();
    let queue_url = resp.queue_url().unwrap().to_string();

    // Send two messages with the same deduplication ID
    for _ in 0..2 {
        client
            .send_message()
            .queue_url(&queue_url)
            .message_body("dedup-msg")
            .message_group_id("group-1")
            .message_deduplication_id("same-dedup-id")
            .send()
            .await
            .unwrap();
    }

    let recv = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();

    // Only one message should be received due to deduplication
    assert_eq!(recv.messages().len(), 1);
}

#[tokio::test]
async fn sqs_change_message_visibility() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    let resp = client
        .create_queue()
        .queue_name("vis-queue")
        .attributes(QueueAttributeName::VisibilityTimeout, "30")
        .send()
        .await
        .unwrap();
    let queue_url = resp.queue_url().unwrap().to_string();

    client
        .send_message()
        .queue_url(&queue_url)
        .message_body("vis-test")
        .send()
        .await
        .unwrap();

    let recv = client
        .receive_message()
        .queue_url(&queue_url)
        .send()
        .await
        .unwrap();
    let receipt = recv.messages()[0].receipt_handle().unwrap().to_string();

    // Make message immediately visible again
    client
        .change_message_visibility()
        .queue_url(&queue_url)
        .receipt_handle(&receipt)
        .visibility_timeout(0)
        .send()
        .await
        .unwrap();

    // Should be receivable again
    let recv2 = client
        .receive_message()
        .queue_url(&queue_url)
        .send()
        .await
        .unwrap();
    assert_eq!(recv2.messages().len(), 1);
}

#[tokio::test]
async fn sqs_dead_letter_queue() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    // Create DLQ
    let dlq = client
        .create_queue()
        .queue_name("my-dlq")
        .send()
        .await
        .unwrap();
    let dlq_url = dlq.queue_url().unwrap().to_string();
    let dlq_attrs = client
        .get_queue_attributes()
        .queue_url(&dlq_url)
        .attribute_names(QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap();
    let dlq_arn = dlq_attrs
        .attributes()
        .unwrap()
        .get(&QueueAttributeName::QueueArn)
        .unwrap()
        .to_string();

    // Create source queue with redrive policy pointing to DLQ
    let redrive_policy = format!(
        r#"{{"deadLetterTargetArn":"{}","maxReceiveCount":"1"}}"#,
        dlq_arn
    );
    let src = client
        .create_queue()
        .queue_name("src-queue")
        .attributes(QueueAttributeName::RedrivePolicy, &redrive_policy)
        .send()
        .await
        .unwrap();
    let src_url = src.queue_url().unwrap().to_string();

    // List dead letter source queues for the DLQ
    let sources = client
        .list_dead_letter_source_queues()
        .queue_url(&dlq_url)
        .send()
        .await
        .unwrap();
    assert!(
        sources.queue_urls().iter().any(|u| u.contains("src-queue")),
        "DLQ should list the source queue"
    );

    // Verify redrive policy is set
    let attrs = client
        .get_queue_attributes()
        .queue_url(&src_url)
        .attribute_names(QueueAttributeName::RedrivePolicy)
        .send()
        .await
        .unwrap();
    let rp = attrs
        .attributes()
        .unwrap()
        .get(&QueueAttributeName::RedrivePolicy)
        .unwrap();
    assert!(rp.contains(&dlq_arn));
}

#[tokio::test]
async fn sqs_list_queues_prefix() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    client
        .create_queue()
        .queue_name("prefix-alpha")
        .send()
        .await
        .unwrap();
    client
        .create_queue()
        .queue_name("prefix-beta")
        .send()
        .await
        .unwrap();
    client
        .create_queue()
        .queue_name("other-queue")
        .send()
        .await
        .unwrap();

    let resp = client
        .list_queues()
        .queue_name_prefix("prefix-")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.queue_urls().len(), 2);
}

#[tokio::test]
async fn sqs_add_remove_permission() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    let resp = client
        .create_queue()
        .queue_name("perm-queue")
        .send()
        .await
        .unwrap();
    let queue_url = resp.queue_url().unwrap().to_string();

    client
        .add_permission()
        .queue_url(&queue_url)
        .label("my-permission")
        .aws_account_ids("123456789012")
        .actions("SendMessage")
        .send()
        .await
        .unwrap();

    client
        .remove_permission()
        .queue_url(&queue_url)
        .label("my-permission")
        .send()
        .await
        .unwrap();
}

/// Regression: GetQueueAttributes via query protocol (AWS CLI) should return attributes.
#[tokio::test]
async fn sqs_get_queue_attributes_via_query_protocol() {
    let server = TestServer::start().await;

    // Create queue via CLI
    let output = server
        .aws_cli(&["sqs", "create-queue", "--queue-name", "query-attrs-queue"])
        .await;
    assert!(output.success(), "create failed: {}", output.stderr_text());
    let json = output.stdout_json();
    let queue_url = json["QueueUrl"].as_str().unwrap();

    // GetQueueAttributes via CLI (uses query protocol)
    let output = server
        .aws_cli(&[
            "sqs",
            "get-queue-attributes",
            "--queue-url",
            queue_url,
            "--attribute-names",
            "All",
        ])
        .await;
    assert!(
        output.success(),
        "get-queue-attributes failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    let attrs = json["Attributes"].as_object().unwrap();
    assert!(
        attrs.contains_key("QueueArn"),
        "expected QueueArn in attributes, got: {attrs:?}"
    );
    assert!(
        attrs.contains_key("VisibilityTimeout"),
        "expected VisibilityTimeout in attributes"
    );
}

#[tokio::test]
async fn sqs_list_queues_pagination() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    // Create 5 queues
    for i in 0..5 {
        client
            .create_queue()
            .queue_name(format!("page-queue-{i}"))
            .send()
            .await
            .unwrap();
    }

    // List with MaxResults=2: should return 2 queues and a NextToken
    let resp = client.list_queues().max_results(2).send().await.unwrap();
    assert_eq!(resp.queue_urls().len(), 2);
    let token = resp
        .next_token()
        .expect("expected NextToken when more results exist");

    // Use NextToken to get next page
    let resp2 = client
        .list_queues()
        .max_results(2)
        .next_token(token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp2.queue_urls().len(), 2);
    let token2 = resp2
        .next_token()
        .expect("expected NextToken for third page");

    // Third page: 1 remaining queue, no NextToken
    let resp3 = client
        .list_queues()
        .max_results(2)
        .next_token(token2)
        .send()
        .await
        .unwrap();
    assert_eq!(resp3.queue_urls().len(), 1);
    assert!(
        resp3.next_token().is_none(),
        "expected no NextToken on last page"
    );

    // Collect all URLs across pages and verify all 5 queues are returned
    let mut all_urls: Vec<String> = Vec::new();
    all_urls.extend(resp.queue_urls().iter().cloned());
    all_urls.extend(resp2.queue_urls().iter().cloned());
    all_urls.extend(resp3.queue_urls().iter().cloned());
    assert_eq!(all_urls.len(), 5);
    for i in 0..5 {
        assert!(
            all_urls
                .iter()
                .any(|u| u.contains(&format!("page-queue-{i}"))),
            "missing page-queue-{i} in paginated results"
        );
    }
}

#[tokio::test]
async fn sqs_list_queues_pagination_all_fit_in_one_page() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    // Create 2 queues
    for i in 0..2 {
        client
            .create_queue()
            .queue_name(format!("small-page-{i}"))
            .send()
            .await
            .unwrap();
    }

    // List with MaxResults=10: all fit, no NextToken
    let resp = client.list_queues().max_results(10).send().await.unwrap();
    assert_eq!(resp.queue_urls().len(), 2);
    assert!(
        resp.next_token().is_none(),
        "expected no NextToken when all queues fit in one page"
    );
}

/// Regression: CreateQueue with invalid DelaySeconds should return an error.
#[tokio::test]
async fn sqs_create_queue_invalid_delay_seconds() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    // DelaySeconds must be 0..=900; 9999 is invalid
    let result = client
        .create_queue()
        .queue_name("bad-delay-queue")
        .attributes(QueueAttributeName::DelaySeconds, "9999")
        .send()
        .await;
    assert!(
        result.is_err(),
        "Expected error for invalid DelaySeconds value"
    );
}

/// Regression: ListDeadLetterSourceQueues returns queues that use this queue as DLQ.
#[tokio::test]
async fn sqs_list_dead_letter_source_queues() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    // Create DLQ
    let dlq = client
        .create_queue()
        .queue_name("regression-dlq")
        .send()
        .await
        .unwrap();
    let dlq_url = dlq.queue_url().unwrap().to_string();
    let dlq_attrs = client
        .get_queue_attributes()
        .queue_url(&dlq_url)
        .attribute_names(QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap();
    let dlq_arn = dlq_attrs
        .attributes()
        .unwrap()
        .get(&QueueAttributeName::QueueArn)
        .unwrap()
        .to_string();

    // Create source queue with redrive policy
    let redrive = format!(
        r#"{{"deadLetterTargetArn":"{}","maxReceiveCount":"3"}}"#,
        dlq_arn
    );
    client
        .create_queue()
        .queue_name("regression-src")
        .attributes(QueueAttributeName::RedrivePolicy, &redrive)
        .send()
        .await
        .unwrap();

    // ListDeadLetterSourceQueues should list the source queue
    let sources = client
        .list_dead_letter_source_queues()
        .queue_url(&dlq_url)
        .send()
        .await
        .unwrap();
    let urls = sources.queue_urls();
    assert_eq!(urls.len(), 1, "expected 1 source queue, got {}", urls.len());
    assert!(
        urls[0].contains("regression-src"),
        "expected source queue URL to contain 'regression-src', got: {}",
        urls[0]
    );
}

#[tokio::test]
async fn sqs_introspection_messages() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    let queue = client
        .create_queue()
        .queue_name("intro-queue")
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap().to_string();

    client
        .send_message()
        .queue_url(&queue_url)
        .message_body("introspection test body")
        .send()
        .await
        .unwrap();

    let url = format!("{}/_fakecloud/sqs/messages", server.endpoint());
    let resp: serde_json::Value = reqwest::get(&url).await.unwrap().json().await.unwrap();
    let queues = resp["queues"].as_array().unwrap();
    assert!(!queues.is_empty(), "expected at least one queue");

    let q = queues
        .iter()
        .find(|q| q["queueName"] == "intro-queue")
        .expect("expected intro-queue in response");
    assert_eq!(q["queueUrl"], queue_url);

    let messages = q["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["body"], "introspection test body");
    assert_eq!(messages[0]["receiveCount"], 0);
    assert_eq!(messages[0]["inFlight"], false);
    assert!(!messages[0]["messageId"].as_str().unwrap().is_empty());
    assert!(!messages[0]["createdAt"].as_str().unwrap().is_empty());
}

#[tokio::test]
async fn sqs_simulation_expiration_tick() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    // Create queue with very short retention period (1 second)
    let resp = client
        .create_queue()
        .queue_name("expire-queue")
        .attributes(QueueAttributeName::MessageRetentionPeriod, "1")
        .send()
        .await
        .unwrap();
    let queue_url = resp.queue_url().unwrap().to_string();

    // Send a message
    client
        .send_message()
        .queue_url(&queue_url)
        .message_body("will expire")
        .send()
        .await
        .unwrap();

    // Wait for message to expire
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Call the expiration tick endpoint
    let url = format!(
        "{}/_fakecloud/sqs/expiration-processor/tick",
        server.endpoint()
    );
    let resp: serde_json::Value = reqwest::Client::new()
        .post(&url)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(resp["expiredMessages"].as_u64().unwrap() >= 1);

    // Verify message is gone
    let recv = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();
    assert_eq!(recv.messages().len(), 0);
}

#[tokio::test]
async fn sqs_simulation_force_dlq() {
    let server = TestServer::start().await;
    let client = server.sqs_client().await;

    // Create DLQ
    let dlq_resp = client
        .create_queue()
        .queue_name("my-dlq")
        .send()
        .await
        .unwrap();
    let dlq_url = dlq_resp.queue_url().unwrap().to_string();

    // Get DLQ ARN
    let dlq_attrs = client
        .get_queue_attributes()
        .queue_url(&dlq_url)
        .attribute_names(QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap();
    let dlq_arn = dlq_attrs
        .attributes()
        .unwrap()
        .get(&QueueAttributeName::QueueArn)
        .unwrap()
        .clone();

    // Create source queue with redrive policy (maxReceiveCount=1)
    let redrive_policy =
        serde_json::json!({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": 1}).to_string();
    let src_resp = client
        .create_queue()
        .queue_name("src-queue")
        .attributes(QueueAttributeName::RedrivePolicy, &redrive_policy)
        .send()
        .await
        .unwrap();
    let src_url = src_resp.queue_url().unwrap().to_string();

    // Send a message
    client
        .send_message()
        .queue_url(&src_url)
        .message_body("hello dlq")
        .send()
        .await
        .unwrap();

    // Receive the message once (increments receive count to 1)
    let recv = client
        .receive_message()
        .queue_url(&src_url)
        .max_number_of_messages(1)
        .send()
        .await
        .unwrap();
    assert_eq!(recv.messages().len(), 1);

    // Wait for visibility timeout to expire so message returns to queue
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Call force-dlq endpoint
    let url = format!("{}/_fakecloud/sqs/src-queue/force-dlq", server.endpoint());
    let resp: serde_json::Value = reqwest::Client::new()
        .post(&url)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["movedMessages"].as_u64().unwrap(), 1);

    // Verify message appeared in DLQ
    let dlq_recv = client
        .receive_message()
        .queue_url(&dlq_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();
    assert_eq!(dlq_recv.messages().len(), 1);
    assert_eq!(dlq_recv.messages()[0].body().unwrap(), "hello dlq");
}
