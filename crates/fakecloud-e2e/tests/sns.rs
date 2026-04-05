mod helpers;

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
