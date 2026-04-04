mod helpers;

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
