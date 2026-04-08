mod helpers;

use aws_sdk_eventbridge::types::{PutEventsRequestEntry, Target};
use helpers::TestServer;

/// SNS → SQS fan-out: publish to a topic, verify the message arrives in a subscribed SQS queue.
#[tokio::test]
async fn sns_to_sqs_fanout() {
    let server = TestServer::start().await;
    let sqs = server.sqs_client().await;
    let sns = server.sns_client().await;

    // Create SQS queue
    let queue = sqs
        .create_queue()
        .queue_name("fanout-queue")
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap().to_string();

    // Get queue ARN
    let attrs = sqs
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap();
    let queue_arn = attrs
        .attributes()
        .unwrap()
        .get(&aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .unwrap()
        .to_string();

    // Create SNS topic and subscribe the SQS queue
    let topic = sns
        .create_topic()
        .name("fanout-topic")
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

    // Publish to SNS
    sns.publish()
        .topic_arn(&topic_arn)
        .message("hello from SNS fan-out")
        .send()
        .await
        .unwrap();

    // Receive from SQS — the message should have been delivered!
    let msgs = sqs
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();

    assert_eq!(msgs.messages().len(), 1, "expected 1 message in SQS queue");
    let body = msgs.messages()[0].body().unwrap();
    // The body is an SNS notification envelope
    let envelope: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(envelope["Type"], "Notification");
    assert_eq!(envelope["Message"], "hello from SNS fan-out");
    assert_eq!(envelope["TopicArn"], topic_arn);
}

/// SNS → multiple SQS queues: fan-out to 2 queues simultaneously.
#[tokio::test]
async fn sns_fanout_to_multiple_queues() {
    let server = TestServer::start().await;
    let sqs = server.sqs_client().await;
    let sns = server.sns_client().await;

    // Create 2 SQS queues
    let q1 = sqs
        .create_queue()
        .queue_name("multi-q1")
        .send()
        .await
        .unwrap();
    let q1_url = q1.queue_url().unwrap().to_string();
    let q1_arn = get_queue_arn(&sqs, &q1_url).await;

    let q2 = sqs
        .create_queue()
        .queue_name("multi-q2")
        .send()
        .await
        .unwrap();
    let q2_url = q2.queue_url().unwrap().to_string();
    let q2_arn = get_queue_arn(&sqs, &q2_url).await;

    // Create topic and subscribe both queues
    let topic = sns.create_topic().name("multi-topic").send().await.unwrap();
    let topic_arn = topic.topic_arn().unwrap().to_string();

    sns.subscribe()
        .topic_arn(&topic_arn)
        .protocol("sqs")
        .endpoint(&q1_arn)
        .send()
        .await
        .unwrap();
    sns.subscribe()
        .topic_arn(&topic_arn)
        .protocol("sqs")
        .endpoint(&q2_arn)
        .send()
        .await
        .unwrap();

    // Publish
    sns.publish()
        .topic_arn(&topic_arn)
        .message("broadcast message")
        .send()
        .await
        .unwrap();

    // Both queues should have the message
    let msgs1 = sqs
        .receive_message()
        .queue_url(&q1_url)
        .send()
        .await
        .unwrap();
    assert_eq!(msgs1.messages().len(), 1);

    let msgs2 = sqs
        .receive_message()
        .queue_url(&q2_url)
        .send()
        .await
        .unwrap();
    assert_eq!(msgs2.messages().len(), 1);
}

/// EventBridge → SQS: put an event that matches a rule with an SQS target.
#[tokio::test]
async fn eventbridge_to_sqs() {
    let server = TestServer::start().await;
    let sqs = server.sqs_client().await;
    let eb = server.eventbridge_client().await;

    // Create SQS queue
    let queue = sqs
        .create_queue()
        .queue_name("eb-sqs-queue")
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap().to_string();
    let queue_arn = get_queue_arn(&sqs, &queue_url).await;

    // Create rule matching "orders" source
    eb.put_rule()
        .name("order-rule")
        .event_pattern(r#"{"source": ["orders"]}"#)
        .send()
        .await
        .unwrap();

    // Add SQS target
    eb.put_targets()
        .rule("order-rule")
        .targets(
            Target::builder()
                .id("sqs-1")
                .arn(&queue_arn)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Put matching event
    eb.put_events()
        .entries(
            PutEventsRequestEntry::builder()
                .source("orders")
                .detail_type("OrderPlaced")
                .detail(r#"{"orderId": "42", "amount": 99.99}"#)
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Message should arrive in SQS
    let msgs = sqs
        .receive_message()
        .queue_url(&queue_url)
        .send()
        .await
        .unwrap();

    assert_eq!(msgs.messages().len(), 1, "expected event in SQS queue");
    let body: serde_json::Value = serde_json::from_str(msgs.messages()[0].body().unwrap()).unwrap();
    assert_eq!(body["source"], "orders");
    assert_eq!(body["detail-type"], "OrderPlaced");
    assert_eq!(body["detail"]["orderId"], "42");
}

/// EventBridge → SQS: non-matching event should NOT be delivered.
#[tokio::test]
async fn eventbridge_non_matching_event_not_delivered() {
    let server = TestServer::start().await;
    let sqs = server.sqs_client().await;
    let eb = server.eventbridge_client().await;

    let queue = sqs
        .create_queue()
        .queue_name("eb-no-match-queue")
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap().to_string();
    let queue_arn = get_queue_arn(&sqs, &queue_url).await;

    eb.put_rule()
        .name("specific-rule")
        .event_pattern(r#"{"source": ["payments"]}"#)
        .send()
        .await
        .unwrap();

    eb.put_targets()
        .rule("specific-rule")
        .targets(
            Target::builder()
                .id("sqs-1")
                .arn(&queue_arn)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Put event with DIFFERENT source — should NOT match
    eb.put_events()
        .entries(
            PutEventsRequestEntry::builder()
                .source("orders")
                .detail_type("OrderPlaced")
                .detail("{}")
                .build(),
        )
        .send()
        .await
        .unwrap();

    let msgs = sqs
        .receive_message()
        .queue_url(&queue_url)
        .send()
        .await
        .unwrap();
    assert_eq!(
        msgs.messages().len(),
        0,
        "non-matching event should not be delivered"
    );
}

/// The full chain: EventBridge → SNS → SQS.
/// An event matches a rule targeting an SNS topic, which fans out to a subscribed SQS queue.
#[tokio::test]
async fn eventbridge_to_sns_to_sqs_chain() {
    let server = TestServer::start().await;
    let sqs = server.sqs_client().await;
    let sns = server.sns_client().await;
    let eb = server.eventbridge_client().await;

    // Create SQS queue (final destination)
    let queue = sqs
        .create_queue()
        .queue_name("chain-queue")
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap().to_string();
    let queue_arn = get_queue_arn(&sqs, &queue_url).await;

    // Create SNS topic (middle)
    let topic = sns.create_topic().name("chain-topic").send().await.unwrap();
    let topic_arn = topic.topic_arn().unwrap().to_string();

    // Subscribe SQS queue to SNS topic
    sns.subscribe()
        .topic_arn(&topic_arn)
        .protocol("sqs")
        .endpoint(&queue_arn)
        .send()
        .await
        .unwrap();

    // Create EventBridge rule targeting SNS topic
    eb.put_rule()
        .name("chain-rule")
        .event_pattern(r#"{"source": ["inventory"]}"#)
        .send()
        .await
        .unwrap();

    eb.put_targets()
        .rule("chain-rule")
        .targets(
            Target::builder()
                .id("sns-target")
                .arn(&topic_arn)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Put event into EventBridge
    eb.put_events()
        .entries(
            PutEventsRequestEntry::builder()
                .source("inventory")
                .detail_type("StockUpdated")
                .detail(r#"{"sku": "WIDGET-42", "quantity": 100}"#)
                .build(),
        )
        .send()
        .await
        .unwrap();

    // The event should flow: EventBridge → SNS → SQS
    let msgs = sqs
        .receive_message()
        .queue_url(&queue_url)
        .send()
        .await
        .unwrap();

    assert_eq!(
        msgs.messages().len(),
        1,
        "expected chained event in SQS queue"
    );

    // The SQS message body is an SNS envelope wrapping the EventBridge event
    let sns_envelope: serde_json::Value =
        serde_json::from_str(msgs.messages()[0].body().unwrap()).unwrap();
    assert_eq!(sns_envelope["Type"], "Notification");

    // The SNS Message field contains the EventBridge event JSON
    let eb_event: serde_json::Value =
        serde_json::from_str(sns_envelope["Message"].as_str().unwrap()).unwrap();
    assert_eq!(eb_event["source"], "inventory");
    assert_eq!(eb_event["detail-type"], "StockUpdated");
    assert_eq!(eb_event["detail"]["sku"], "WIDGET-42");
}

/// Full workflow: IAM + STS + SSM + SQS together.
#[tokio::test]
async fn iam_ssm_sqs_workflow() {
    let server = TestServer::start().await;
    let iam = server.iam_client().await;
    let sts = server.sts_client().await;
    let ssm = server.ssm_client().await;
    let sqs = server.sqs_client().await;

    // Verify identity
    let identity = sts.get_caller_identity().send().await.unwrap();
    assert_eq!(identity.account().unwrap(), "123456789012");

    // Create IAM user
    iam.create_user()
        .user_name("app-user")
        .send()
        .await
        .unwrap();

    // Store config in SSM
    ssm.put_parameter()
        .name("/app/queue-name")
        .value("app-queue")
        .r#type(aws_sdk_ssm::types::ParameterType::String)
        .send()
        .await
        .unwrap();

    // Read config from SSM
    let param = ssm
        .get_parameter()
        .name("/app/queue-name")
        .send()
        .await
        .unwrap();
    let queue_name = param.parameter().unwrap().value().unwrap();

    // Create SQS queue using config
    let queue = sqs
        .create_queue()
        .queue_name(queue_name)
        .send()
        .await
        .unwrap();
    assert!(queue.queue_url().unwrap().contains("app-queue"));

    // Send and receive
    sqs.send_message()
        .queue_url(queue.queue_url().unwrap())
        .message_body("config-driven message")
        .send()
        .await
        .unwrap();

    let msgs = sqs
        .receive_message()
        .queue_url(queue.queue_url().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(msgs.messages()[0].body().unwrap(), "config-driven message");
}

async fn get_queue_arn(sqs: &aws_sdk_sqs::Client, queue_url: &str) -> String {
    let attrs = sqs
        .get_queue_attributes()
        .queue_url(queue_url)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap();
    attrs
        .attributes()
        .unwrap()
        .get(&aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .unwrap()
        .to_string()
}

/// Per-service reset: resetting SQS should leave SNS state intact.
#[tokio::test]
async fn per_service_reset_sqs_leaves_sns() {
    let server = TestServer::start().await;
    let sqs = server.sqs_client().await;
    let sns = server.sns_client().await;
    let http_client = reqwest::Client::new();

    // Create an SQS queue
    sqs.create_queue()
        .queue_name("reset-test-queue")
        .send()
        .await
        .unwrap();

    // Create an SNS topic
    let topic_resp = sns
        .create_topic()
        .name("reset-test-topic")
        .send()
        .await
        .unwrap();
    let topic_arn = topic_resp.topic_arn().unwrap().to_string();

    // Verify both exist
    assert_eq!(
        sqs.list_queues().send().await.unwrap().queue_urls().len(),
        1
    );
    assert_eq!(sns.list_topics().send().await.unwrap().topics().len(), 1);

    // Reset only SQS
    let url = format!("{}/_fakecloud/reset/sqs", server.endpoint());
    let resp: serde_json::Value = http_client
        .post(&url)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["reset"], "sqs");

    // Verify SQS queue is gone
    assert_eq!(
        sqs.list_queues().send().await.unwrap().queue_urls().len(),
        0
    );

    // Verify SNS topic still exists
    let topics = sns.list_topics().send().await.unwrap();
    assert_eq!(topics.topics().len(), 1);
    assert_eq!(topics.topics()[0].topic_arn().unwrap(), topic_arn);
}

/// Per-service reset: unknown service returns 404.
#[tokio::test]
async fn per_service_reset_unknown_service_returns_404() {
    let server = TestServer::start().await;
    let http_client = reqwest::Client::new();

    let url = format!("{}/_fakecloud/reset/nonexistent", server.endpoint());
    let resp = http_client.post(&url).send().await.unwrap();
    assert_eq!(resp.status(), 404);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["error"].as_str().unwrap().contains("Unknown service"));
}
