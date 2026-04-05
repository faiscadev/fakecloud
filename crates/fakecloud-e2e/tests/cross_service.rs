mod helpers;

use aws_sdk_eventbridge::types::{PutEventsRequestEntry, Target};
use aws_sdk_s3::primitives::ByteStream;
use helpers::TestServer;

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

/// S3 PUT notification -> SQS: verify that uploading an object triggers a notification.
#[tokio::test]
async fn s3_put_notification_to_sqs() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    let sqs = server.sqs_client().await;

    // Create SQS queue
    let queue = sqs
        .create_queue()
        .queue_name("s3-notif-queue")
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap().to_string();
    let queue_arn = get_queue_arn(&sqs, &queue_url).await;

    // Create S3 bucket
    s3.create_bucket()
        .bucket("cross-notif")
        .send()
        .await
        .unwrap();

    // Set notification config
    let notif_config = format!(
        r#"{{"QueueConfigurations":[{{"QueueArn":"{}","Events":["s3:ObjectCreated:*"]}}]}}"#,
        queue_arn
    );
    let output = server
        .aws_cli(&[
            "s3api",
            "put-bucket-notification-configuration",
            "--bucket",
            "cross-notif",
            "--notification-configuration",
            &notif_config,
        ])
        .await;
    assert!(output.success());

    // Upload object
    s3.put_object()
        .bucket("cross-notif")
        .key("hello.txt")
        .body(ByteStream::from_static(b"hello cross-service"))
        .send()
        .await
        .unwrap();

    // Receive from SQS
    let msgs = sqs
        .receive_message()
        .queue_url(&queue_url)
        .wait_time_seconds(2)
        .send()
        .await
        .unwrap();
    assert!(
        !msgs.messages().is_empty(),
        "expected S3 notification in SQS"
    );

    let event: serde_json::Value =
        serde_json::from_str(msgs.messages()[0].body().unwrap()).unwrap();
    assert_eq!(event["Records"][0]["eventSource"], "aws:s3");
}

/// S3 DELETE notification -> SQS
#[tokio::test]
async fn s3_delete_notification_to_sqs() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    let sqs = server.sqs_client().await;

    let queue = sqs
        .create_queue()
        .queue_name("s3-del-notif")
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap().to_string();
    let queue_arn = get_queue_arn(&sqs, &queue_url).await;

    s3.create_bucket()
        .bucket("del-notif-bucket")
        .send()
        .await
        .unwrap();

    // Put an object first
    s3.put_object()
        .bucket("del-notif-bucket")
        .key("to-delete.txt")
        .body(ByteStream::from_static(b"data"))
        .send()
        .await
        .unwrap();

    // Set notification for deletes
    let notif_config = format!(
        r#"{{"QueueConfigurations":[{{"QueueArn":"{}","Events":["s3:ObjectRemoved:*"]}}]}}"#,
        queue_arn
    );
    let output = server
        .aws_cli(&[
            "s3api",
            "put-bucket-notification-configuration",
            "--bucket",
            "del-notif-bucket",
            "--notification-configuration",
            &notif_config,
        ])
        .await;
    assert!(output.success());

    // Delete the object
    s3.delete_object()
        .bucket("del-notif-bucket")
        .key("to-delete.txt")
        .send()
        .await
        .unwrap();

    // Receive notification
    let msgs = sqs
        .receive_message()
        .queue_url(&queue_url)
        .wait_time_seconds(2)
        .send()
        .await
        .unwrap();
    assert!(
        !msgs.messages().is_empty(),
        "expected S3 delete notification in SQS"
    );
}

/// SNS fan-out to multiple SQS queues with different messages.
#[tokio::test]
async fn sns_fanout_multiple_messages() {
    let server = TestServer::start().await;
    let sns = server.sns_client().await;
    let sqs = server.sqs_client().await;

    let q1 = sqs
        .create_queue()
        .queue_name("fanout-a")
        .send()
        .await
        .unwrap();
    let q1_url = q1.queue_url().unwrap().to_string();
    let q1_arn = get_queue_arn(&sqs, &q1_url).await;

    let q2 = sqs
        .create_queue()
        .queue_name("fanout-b")
        .send()
        .await
        .unwrap();
    let q2_url = q2.queue_url().unwrap().to_string();
    let q2_arn = get_queue_arn(&sqs, &q2_url).await;

    let topic = sns
        .create_topic()
        .name("fanout-multi")
        .send()
        .await
        .unwrap();
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

    // Publish 3 messages
    for i in 0..3 {
        sns.publish()
            .topic_arn(&topic_arn)
            .message(format!("msg-{i}"))
            .send()
            .await
            .unwrap();
    }

    // Both queues should have 3 messages each
    let msgs1 = sqs
        .receive_message()
        .queue_url(&q1_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();
    assert_eq!(msgs1.messages().len(), 3);

    let msgs2 = sqs
        .receive_message()
        .queue_url(&q2_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();
    assert_eq!(msgs2.messages().len(), 3);
}

/// EventBridge -> SQS with detail-type matching.
#[tokio::test]
async fn eb_detail_type_matching_to_sqs() {
    let server = TestServer::start().await;
    let eb = server.eventbridge_client().await;
    let sqs = server.sqs_client().await;

    let queue = sqs
        .create_queue()
        .queue_name("eb-detail-queue")
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap().to_string();
    let queue_arn = get_queue_arn(&sqs, &queue_url).await;

    // Rule matching both source and detail-type
    eb.put_rule()
        .name("detail-rule")
        .event_pattern(r#"{"source": ["payments"], "detail-type": ["PaymentProcessed"]}"#)
        .send()
        .await
        .unwrap();

    eb.put_targets()
        .rule("detail-rule")
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

    // Send matching event
    eb.put_events()
        .entries(
            PutEventsRequestEntry::builder()
                .source("payments")
                .detail_type("PaymentProcessed")
                .detail(r#"{"amount": 100}"#)
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Send non-matching event (wrong detail-type)
    eb.put_events()
        .entries(
            PutEventsRequestEntry::builder()
                .source("payments")
                .detail_type("PaymentFailed")
                .detail(r#"{"reason": "insufficient funds"}"#)
                .build(),
        )
        .send()
        .await
        .unwrap();

    let msgs = sqs
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();

    // Only the matching event should be delivered
    assert_eq!(msgs.messages().len(), 1);
    let body: serde_json::Value = serde_json::from_str(msgs.messages()[0].body().unwrap()).unwrap();
    assert_eq!(body["detail-type"], "PaymentProcessed");
}

/// Full chain: SSM config -> SQS queue name -> SNS -> SQS delivery.
#[tokio::test]
async fn ssm_config_driven_sns_sqs_workflow() {
    let server = TestServer::start().await;
    let ssm = server.ssm_client().await;
    let sns = server.sns_client().await;
    let sqs = server.sqs_client().await;

    // Store topic and queue names in SSM
    ssm.put_parameter()
        .name("/workflow/topic-name")
        .value("workflow-topic")
        .r#type(aws_sdk_ssm::types::ParameterType::String)
        .send()
        .await
        .unwrap();
    ssm.put_parameter()
        .name("/workflow/queue-name")
        .value("workflow-queue")
        .r#type(aws_sdk_ssm::types::ParameterType::String)
        .send()
        .await
        .unwrap();

    // Read config from SSM
    let topic_name = ssm
        .get_parameter()
        .name("/workflow/topic-name")
        .send()
        .await
        .unwrap()
        .parameter()
        .unwrap()
        .value()
        .unwrap()
        .to_string();
    let queue_name = ssm
        .get_parameter()
        .name("/workflow/queue-name")
        .send()
        .await
        .unwrap()
        .parameter()
        .unwrap()
        .value()
        .unwrap()
        .to_string();

    // Create SQS queue
    let queue = sqs
        .create_queue()
        .queue_name(&queue_name)
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap().to_string();
    let queue_arn = get_queue_arn(&sqs, &queue_url).await;

    // Create SNS topic and subscribe SQS
    let topic = sns.create_topic().name(&topic_name).send().await.unwrap();
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
        .message("config-driven workflow")
        .send()
        .await
        .unwrap();

    // Verify delivery
    let msgs = sqs
        .receive_message()
        .queue_url(&queue_url)
        .send()
        .await
        .unwrap();
    assert_eq!(msgs.messages().len(), 1);
    let envelope: serde_json::Value =
        serde_json::from_str(msgs.messages()[0].body().unwrap()).unwrap();
    assert_eq!(envelope["Message"], "config-driven workflow");
}
