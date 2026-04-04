mod helpers;

use aws_sdk_eventbridge::types::{PutEventsRequestEntry, Target};
use helpers::TestServer;

/// Test that SQS and SNS can be used together — create a queue, subscribe it to a topic.
#[tokio::test]
async fn sns_subscribe_sqs_queue() {
    let server = TestServer::start().await;
    let sqs = server.sqs_client().await;
    let sns = server.sns_client().await;

    // Create SQS queue
    let queue = sqs
        .create_queue()
        .queue_name("fan-out-queue")
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

    // Create SNS topic
    let topic = sns
        .create_topic()
        .name("fan-out-topic")
        .send()
        .await
        .unwrap();
    let topic_arn = topic.topic_arn().unwrap().to_string();

    // Subscribe SQS queue to SNS topic
    let sub = sns
        .subscribe()
        .topic_arn(&topic_arn)
        .protocol("sqs")
        .endpoint(&queue_arn)
        .send()
        .await
        .unwrap();
    assert!(sub.subscription_arn().is_some());

    // Verify subscription exists
    let subs = sns
        .list_subscriptions_by_topic()
        .topic_arn(&topic_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(subs.subscriptions().len(), 1);
    assert_eq!(subs.subscriptions()[0].protocol().unwrap(), "sqs");

    // Publish a message
    let pub_resp = sns
        .publish()
        .topic_arn(&topic_arn)
        .message("hello from sns")
        .send()
        .await
        .unwrap();
    assert!(pub_resp.message_id().is_some());
}

/// Test EventBridge rules with SQS targets — create rule, add SQS target, put events.
#[tokio::test]
async fn eventbridge_rule_with_sqs_target() {
    let server = TestServer::start().await;
    let sqs = server.sqs_client().await;
    let eb = server.eventbridge_client().await;

    // Create SQS queue for target
    let queue = sqs
        .create_queue()
        .queue_name("eb-target-queue")
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap().to_string();

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

    // Create EventBridge rule
    eb.put_rule()
        .name("order-events")
        .event_pattern(r#"{"source": ["orders"]}"#)
        .send()
        .await
        .unwrap();

    // Add SQS target
    eb.put_targets()
        .rule("order-events")
        .targets(
            Target::builder()
                .id("sqs-target")
                .arn(&queue_arn)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Verify target
    let targets = eb
        .list_targets_by_rule()
        .rule("order-events")
        .send()
        .await
        .unwrap();
    assert_eq!(targets.targets().len(), 1);
    assert_eq!(targets.targets()[0].arn(), &queue_arn);

    // Put events
    let resp = eb
        .put_events()
        .entries(
            PutEventsRequestEntry::builder()
                .source("orders")
                .detail_type("OrderPlaced")
                .detail(r#"{"orderId": "42"}"#)
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.failed_entry_count(), 0);
}

/// Full workflow: IAM user + STS identity + SSM parameter + SQS queue.
#[tokio::test]
async fn full_workflow_iam_ssm_sqs() {
    let server = TestServer::start().await;
    let iam = server.iam_client().await;
    let sts = server.sts_client().await;
    let ssm = server.ssm_client().await;
    let sqs = server.sqs_client().await;

    // Verify identity
    let identity = sts.get_caller_identity().send().await.unwrap();
    assert_eq!(identity.account().unwrap(), "000000000000");

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
