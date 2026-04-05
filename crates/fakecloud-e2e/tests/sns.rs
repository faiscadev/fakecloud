mod helpers;

use aws_sdk_sns::primitives::Blob;
use aws_sdk_sqs::types::QueueAttributeName;
use helpers::TestServer;

#[tokio::test]
async fn sns_create_list_delete_topic() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    // Create
    let resp = client
        .create_topic()
        .name("test-topic")
        .send()
        .await
        .unwrap();
    let topic_arn = resp.topic_arn().unwrap().to_string();
    assert!(topic_arn.contains("test-topic"));

    // List
    let resp = client.list_topics().send().await.unwrap();
    assert_eq!(resp.topics().len(), 1);
    assert_eq!(resp.topics()[0].topic_arn().unwrap(), topic_arn);

    // Delete
    client
        .delete_topic()
        .topic_arn(&topic_arn)
        .send()
        .await
        .unwrap();

    let resp = client.list_topics().send().await.unwrap();
    assert_eq!(resp.topics().len(), 0);
}

#[tokio::test]
async fn sns_create_topic_idempotent() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let r1 = client.create_topic().name("same").send().await.unwrap();
    let r2 = client.create_topic().name("same").send().await.unwrap();
    assert_eq!(r1.topic_arn().unwrap(), r2.topic_arn().unwrap());
}

#[tokio::test]
async fn sns_subscribe_and_list() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let resp = client
        .create_topic()
        .name("sub-topic")
        .send()
        .await
        .unwrap();
    let topic_arn = resp.topic_arn().unwrap().to_string();

    // Subscribe
    let resp = client
        .subscribe()
        .topic_arn(&topic_arn)
        .protocol("sqs")
        .endpoint("arn:aws:sqs:us-east-1:123456789012:my-queue")
        .send()
        .await
        .unwrap();
    let sub_arn = resp.subscription_arn().unwrap().to_string();
    assert!(sub_arn.contains("sub-topic"));

    // List all subscriptions
    let resp = client.list_subscriptions().send().await.unwrap();
    assert_eq!(resp.subscriptions().len(), 1);

    // List by topic
    let resp = client
        .list_subscriptions_by_topic()
        .topic_arn(&topic_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.subscriptions().len(), 1);

    // Unsubscribe
    client
        .unsubscribe()
        .subscription_arn(&sub_arn)
        .send()
        .await
        .unwrap();

    let resp = client.list_subscriptions().send().await.unwrap();
    assert_eq!(resp.subscriptions().len(), 0);
}

#[tokio::test]
async fn sns_publish() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let resp = client
        .create_topic()
        .name("pub-topic")
        .send()
        .await
        .unwrap();
    let topic_arn = resp.topic_arn().unwrap().to_string();

    let resp = client
        .publish()
        .topic_arn(&topic_arn)
        .message("hello from sns")
        .send()
        .await
        .unwrap();
    assert!(resp.message_id().is_some());
}

#[tokio::test]
async fn sns_cli_create_topic() {
    let server = TestServer::start().await;
    let output = server
        .aws_cli(&["sns", "create-topic", "--name", "cli-topic"])
        .await;
    assert!(output.success(), "failed: {}", output.stderr_text());
    let json = output.stdout_json();
    assert!(json["TopicArn"].as_str().unwrap().contains("cli-topic"));
}

#[tokio::test]
async fn sns_publish_with_message_attributes() {
    use aws_sdk_sns::types::MessageAttributeValue;

    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let resp = client
        .create_topic()
        .name("attr-topic")
        .send()
        .await
        .unwrap();
    let topic_arn = resp.topic_arn().unwrap().to_string();

    let attr = MessageAttributeValue::builder()
        .data_type("String")
        .string_value("test-value")
        .build()
        .unwrap();

    let resp = client
        .publish()
        .topic_arn(&topic_arn)
        .message("hello with attrs")
        .message_attributes("myAttr", attr)
        .send()
        .await
        .unwrap();
    assert!(resp.message_id().is_some());
}

#[tokio::test]
async fn sns_fifo_topic_creation() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    // Create a FIFO topic (must set FifoTopic=true attribute)
    let resp = client
        .create_topic()
        .name("my-topic.fifo")
        .attributes("FifoTopic", "true")
        .send()
        .await
        .unwrap();
    let topic_arn = resp.topic_arn().unwrap().to_string();
    assert!(topic_arn.ends_with(".fifo"));

    // Verify FifoTopic attribute is set
    let attrs = client
        .get_topic_attributes()
        .topic_arn(&topic_arn)
        .send()
        .await
        .unwrap();
    let attributes = attrs.attributes().unwrap();
    assert_eq!(
        attributes.get("FifoTopic").map(|s| s.as_str()),
        Some("true")
    );
}

/// Publish to SNS topic with SQS subscriber and verify delivery.
#[tokio::test]
async fn sns_publish_delivers_to_sqs_subscriber() {
    let server = TestServer::start().await;
    let sns = server.sns_client().await;
    let sqs = server.sqs_client().await;

    // Create SQS queue
    let queue = sqs
        .create_queue()
        .queue_name("sns-delivery-queue")
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap().to_string();
    let attrs = sqs
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap();
    let queue_arn = attrs
        .attributes()
        .unwrap()
        .get(&QueueAttributeName::QueueArn)
        .unwrap()
        .to_string();

    // Create topic and subscribe SQS
    let topic = sns
        .create_topic()
        .name("delivery-topic")
        .send()
        .await
        .unwrap();
    let topic_arn = topic.topic_arn().unwrap().to_string();

    sns.subscribe()
        .topic_arn(&topic_arn)
        .protocol("sqs")
        .endpoint(&queue_arn)
        .send()
        .await
        .unwrap();

    // Publish
    sns.publish()
        .topic_arn(&topic_arn)
        .message("delivery test message")
        .subject("Test Subject")
        .send()
        .await
        .unwrap();

    // Verify delivery
    let msgs = sqs
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();

    assert_eq!(msgs.messages().len(), 1, "expected 1 message in SQS");
    let body = msgs.messages()[0].body().unwrap();
    let envelope: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(envelope["Type"], "Notification");
    assert_eq!(envelope["Message"], "delivery test message");
    assert_eq!(envelope["TopicArn"], topic_arn);
}

/// Subscribing Lambda/email/sms protocols should succeed (stub delivery).
#[tokio::test]
async fn sns_subscribe_lambda_email_sms() {
    let server = TestServer::start().await;
    let sns = server.sns_client().await;

    let topic = sns.create_topic().name("stub-topic").send().await.unwrap();
    let topic_arn = topic.topic_arn().unwrap().to_string();

    // Subscribe Lambda
    let resp = sns
        .subscribe()
        .topic_arn(&topic_arn)
        .protocol("lambda")
        .endpoint("arn:aws:lambda:us-east-1:123456789012:function:my-func")
        .send()
        .await
        .unwrap();
    assert!(resp.subscription_arn().is_some());

    // Subscribe email
    let resp = sns
        .subscribe()
        .topic_arn(&topic_arn)
        .protocol("email")
        .endpoint("user@example.com")
        .send()
        .await
        .unwrap();
    assert!(resp.subscription_arn().is_some());

    // Subscribe SMS
    let resp = sns
        .subscribe()
        .topic_arn(&topic_arn)
        .protocol("sms")
        .endpoint("+15551234567")
        .send()
        .await
        .unwrap();
    assert!(resp.subscription_arn().is_some());

    // Publish to the topic — should not error despite stub targets
    let resp = sns
        .publish()
        .topic_arn(&topic_arn)
        .message("message to all subscribers")
        .send()
        .await
        .unwrap();
    assert!(resp.message_id().is_some());

    // Verify all subscriptions are listed
    let subs = sns
        .list_subscriptions_by_topic()
        .topic_arn(&topic_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(subs.subscriptions().len(), 3);
}

#[tokio::test]
async fn sns_get_set_topic_attributes() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let topic = client
        .create_topic()
        .name("attrs-topic")
        .send()
        .await
        .unwrap();
    let topic_arn = topic.topic_arn().unwrap().to_string();

    // Get attributes
    let attrs = client
        .get_topic_attributes()
        .topic_arn(&topic_arn)
        .send()
        .await
        .unwrap();
    let map = attrs.attributes().unwrap();
    assert_eq!(map.get("TopicArn").unwrap(), &topic_arn);

    // Set display name attribute
    client
        .set_topic_attributes()
        .topic_arn(&topic_arn)
        .attribute_name("DisplayName")
        .attribute_value("My Display Name")
        .send()
        .await
        .unwrap();

    let attrs = client
        .get_topic_attributes()
        .topic_arn(&topic_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(
        attrs.attributes().unwrap().get("DisplayName").unwrap(),
        "My Display Name"
    );
}

#[tokio::test]
async fn sns_delete_nonexistent_topic_succeeds() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    // Deleting a non-existent topic should not error (AWS behavior)
    client
        .delete_topic()
        .topic_arn("arn:aws:sns:us-east-1:123456789012:no-such-topic")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn sns_tag_untag_topic() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let topic = client
        .create_topic()
        .name("tag-topic")
        .send()
        .await
        .unwrap();
    let topic_arn = topic.topic_arn().unwrap().to_string();

    use aws_sdk_sns::types::Tag;
    client
        .tag_resource()
        .resource_arn(&topic_arn)
        .tags(Tag::builder().key("env").value("staging").build().unwrap())
        .send()
        .await
        .unwrap();

    let tags = client
        .list_tags_for_resource()
        .resource_arn(&topic_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tags().len(), 1);
    assert_eq!(tags.tags()[0].key(), "env");

    client
        .untag_resource()
        .resource_arn(&topic_arn)
        .tag_keys("env")
        .send()
        .await
        .unwrap();

    let tags = client
        .list_tags_for_resource()
        .resource_arn(&topic_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tags().len(), 0);
}

#[tokio::test]
async fn sns_get_set_subscription_attributes() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let topic = client
        .create_topic()
        .name("sub-attrs-topic")
        .send()
        .await
        .unwrap();
    let topic_arn = topic.topic_arn().unwrap().to_string();

    let sub = client
        .subscribe()
        .topic_arn(&topic_arn)
        .protocol("sqs")
        .endpoint("arn:aws:sqs:us-east-1:123456789012:my-queue")
        .send()
        .await
        .unwrap();
    let sub_arn = sub.subscription_arn().unwrap().to_string();

    // Get subscription attributes
    let attrs = client
        .get_subscription_attributes()
        .subscription_arn(&sub_arn)
        .send()
        .await
        .unwrap();
    let map = attrs.attributes().unwrap();
    assert_eq!(map.get("Protocol").unwrap(), "sqs");
    assert_eq!(map.get("TopicArn").unwrap(), &topic_arn);

    // Set RawMessageDelivery
    client
        .set_subscription_attributes()
        .subscription_arn(&sub_arn)
        .attribute_name("RawMessageDelivery")
        .attribute_value("true")
        .send()
        .await
        .unwrap();

    let attrs = client
        .get_subscription_attributes()
        .subscription_arn(&sub_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(
        attrs
            .attributes()
            .unwrap()
            .get("RawMessageDelivery")
            .unwrap(),
        "true"
    );
}

#[tokio::test]
async fn sns_publish_batch() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let topic = client
        .create_topic()
        .name("batch-topic")
        .send()
        .await
        .unwrap();
    let topic_arn = topic.topic_arn().unwrap().to_string();

    use aws_sdk_sns::types::PublishBatchRequestEntry;
    let entries = vec![
        PublishBatchRequestEntry::builder()
            .id("msg-1")
            .message("batch message 1")
            .build()
            .unwrap(),
        PublishBatchRequestEntry::builder()
            .id("msg-2")
            .message("batch message 2")
            .build()
            .unwrap(),
        PublishBatchRequestEntry::builder()
            .id("msg-3")
            .message("batch message 3")
            .build()
            .unwrap(),
    ];

    let resp = client
        .publish_batch()
        .topic_arn(&topic_arn)
        .set_publish_batch_request_entries(Some(entries))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.successful().len(), 3);
    assert!(resp.failed().is_empty());
}

#[tokio::test]
async fn sns_platform_application_lifecycle() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    // Create platform application
    let app = client
        .create_platform_application()
        .name("my-app")
        .platform("GCM")
        .attributes("PlatformCredential", "fake-api-key")
        .send()
        .await
        .unwrap();
    let app_arn = app.platform_application_arn().unwrap().to_string();

    // List platform applications
    let list = client.list_platform_applications().send().await.unwrap();
    assert_eq!(list.platform_applications().len(), 1);

    // Create platform endpoint
    let endpoint = client
        .create_platform_endpoint()
        .platform_application_arn(&app_arn)
        .token("device-token-123")
        .send()
        .await
        .unwrap();
    let endpoint_arn = endpoint.endpoint_arn().unwrap().to_string();

    // List endpoints
    let endpoints = client
        .list_endpoints_by_platform_application()
        .platform_application_arn(&app_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(endpoints.endpoints().len(), 1);

    // Get endpoint attributes
    let attrs = client
        .get_endpoint_attributes()
        .endpoint_arn(&endpoint_arn)
        .send()
        .await
        .unwrap();
    assert!(attrs.attributes().is_some());

    // Delete endpoint
    client
        .delete_endpoint()
        .endpoint_arn(&endpoint_arn)
        .send()
        .await
        .unwrap();

    // Delete platform application
    client
        .delete_platform_application()
        .platform_application_arn(&app_arn)
        .send()
        .await
        .unwrap();

    let list = client.list_platform_applications().send().await.unwrap();
    assert!(list.platform_applications().is_empty());
}

#[tokio::test]
async fn sns_confirm_subscription() {
    let server = TestServer::start().await;
    let client = server.sns_client().await;

    let topic = client
        .create_topic()
        .name("confirm-topic")
        .send()
        .await
        .unwrap();
    let topic_arn = topic.topic_arn().unwrap().to_string();

    // ConfirmSubscription with a fake token should succeed (stub)
    let resp = client
        .confirm_subscription()
        .topic_arn(&topic_arn)
        .token("fake-confirmation-token")
        .send()
        .await
        .unwrap();
    assert!(resp.subscription_arn().is_some());
}

/// Regression: binary message attributes should be preserved when delivered to SQS
/// via raw message delivery.
#[tokio::test]
async fn sns_binary_message_attribute_delivery() {
    let server = TestServer::start().await;
    let sns = server.sns_client().await;
    let sqs = server.sqs_client().await;

    // Create SQS queue
    let queue = sqs
        .create_queue()
        .queue_name("binary-attr-queue")
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap().to_string();
    let attrs = sqs
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap();
    let queue_arn = attrs
        .attributes()
        .unwrap()
        .get(&QueueAttributeName::QueueArn)
        .unwrap()
        .to_string();

    // Create topic, subscribe SQS with raw message delivery
    let topic = sns
        .create_topic()
        .name("binary-attr-topic")
        .send()
        .await
        .unwrap();
    let topic_arn = topic.topic_arn().unwrap().to_string();

    let sub = sns
        .subscribe()
        .topic_arn(&topic_arn)
        .protocol("sqs")
        .endpoint(&queue_arn)
        .send()
        .await
        .unwrap();
    let sub_arn = sub.subscription_arn().unwrap().to_string();

    // Enable raw message delivery
    sns.set_subscription_attributes()
        .subscription_arn(&sub_arn)
        .attribute_name("RawMessageDelivery")
        .attribute_value("true")
        .send()
        .await
        .unwrap();

    // Publish with a binary attribute
    let binary_data = vec![0xDE, 0xAD, 0xBE, 0xEF];
    let bin_attr = aws_sdk_sns::types::MessageAttributeValue::builder()
        .data_type("Binary")
        .binary_value(Blob::new(binary_data.clone()))
        .build()
        .unwrap();

    sns.publish()
        .topic_arn(&topic_arn)
        .message("binary attr test")
        .message_attributes("binData", bin_attr)
        .send()
        .await
        .unwrap();

    // Receive from SQS and verify the binary attribute is present
    let msgs = sqs
        .receive_message()
        .queue_url(&queue_url)
        .message_attribute_names("All")
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();

    assert_eq!(msgs.messages().len(), 1, "expected 1 message in SQS");
    let msg = &msgs.messages()[0];
    assert_eq!(msg.body().unwrap(), "binary attr test");

    let msg_attrs = msg.message_attributes().unwrap();
    assert!(
        msg_attrs.contains_key("binData"),
        "expected 'binData' attribute in SQS message"
    );
    let attr = msg_attrs.get("binData").unwrap();
    assert_eq!(attr.data_type(), "Binary");
}
