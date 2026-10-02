mod helpers;

use std::io::Write;

fn docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn require_docker_or_skip(test: &str) -> bool {
    if docker_available() {
        return true;
    }
    if std::env::var("CI").is_ok() {
        panic!("docker is required for {test} in CI");
    }
    eprintln!("skipping {test}: docker is not available");
    false
}

use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType,
};
use aws_sdk_eventbridge::types::{PutEventsRequestEntry, Target};
use aws_sdk_s3::primitives::ByteStream;
use helpers::TestServer;

/// Create a ZIP file in memory containing a single file.
fn make_zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let buf = Vec::new();
    let cursor = std::io::Cursor::new(buf);
    let mut writer = zip::ZipWriter::new(cursor);
    for (name, content) in entries {
        let options = zip::write::SimpleFileOptions::default().unix_permissions(0o755);
        writer.start_file(*name, options).unwrap();
        writer.write_all(content).unwrap();
    }
    let cursor = writer.finish().unwrap();
    cursor.into_inner()
}

/// Query recorded Lambda invocations via internal API.
async fn get_lambda_invocations(endpoint: &str) -> serde_json::Value {
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{endpoint}/_fakecloud/lambda/invocations"))
        .send()
        .await
        .unwrap();
    resp.json::<serde_json::Value>().await.unwrap()
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

/// SSM -> SecretsManager: resolve a secret via the SSM parameter path.
#[tokio::test]
async fn ssm_secretsmanager_parameter_resolution() {
    let server = TestServer::start().await;
    let ssm = server.ssm_client().await;
    let sm = server.secretsmanager_client().await;

    // Create a secret in SecretsManager
    sm.create_secret()
        .name("my/test-secret")
        .secret_string("super-secret-value-42")
        .send()
        .await
        .unwrap();

    // Retrieve it via SSM parameter path with WithDecryption=true
    let param = ssm
        .get_parameter()
        .name("/aws/reference/secretsmanager/my/test-secret")
        .with_decryption(true)
        .send()
        .await
        .unwrap();

    let p = param.parameter().unwrap();
    assert_eq!(p.value().unwrap(), "super-secret-value-42");
    assert_eq!(
        p.name().unwrap(),
        "/aws/reference/secretsmanager/my/test-secret"
    );

    // Without WithDecryption should fail
    let err = ssm
        .get_parameter()
        .name("/aws/reference/secretsmanager/my/test-secret")
        .with_decryption(false)
        .send()
        .await;
    assert!(err.is_err(), "expected error without WithDecryption");

    // Non-existent secret should fail
    let err = ssm
        .get_parameter()
        .name("/aws/reference/secretsmanager/no-such-secret")
        .with_decryption(true)
        .send()
        .await;
    assert!(err.is_err(), "expected error for non-existent secret");
}

/// SQS -> Lambda: event source mapping triggers Lambda invocation.
#[tokio::test]
async fn sqs_lambda_event_source_mapping() {
    let server = TestServer::start().await;
    let sqs = server.sqs_client().await;
    let lambda = server.lambda_client().await;

    // Create SQS queue
    let queue = sqs
        .create_queue()
        .queue_name("lambda-trigger-queue")
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap().to_string();
    let queue_arn = get_queue_arn(&sqs, &queue_url).await;

    // Create Lambda function
    // Use a real Python handler so the container runtime returns Ok and
    // the poller acks the batch. With the post-Cubic correctness fix we
    // only delete SQS messages on a successful Lambda invocation; an
    // un-runnable "fake-code" blob would cause the poller to leave the
    // message visible for retry, which doesn't reflect what the test is
    // checking.
    lambda
        .create_function()
        .function_name("sqs-processor")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/lambda-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(aws_sdk_lambda::primitives::Blob::new(make_zip(&[(
                    "index.py",
                    br#"def handler(event, context):
    return {"statusCode": 200}
"#,
                )])))
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Create event source mapping
    lambda
        .create_event_source_mapping()
        .event_source_arn(&queue_arn)
        .function_name("sqs-processor")
        .batch_size(10)
        .enabled(true)
        .send()
        .await
        .unwrap();

    // Send a message to the queue
    sqs.send_message()
        .queue_url(&queue_url)
        .message_body(r#"{"order_id": "12345"}"#)
        .send()
        .await
        .unwrap();

    // Poll for the post-invoke `aws:sqs` record rather than a fixed
    // sleep — Docker container startup can take >2s in CI and would
    // race the assertion below. Bound the wait so a stuck poller fails
    // the test instead of hanging the whole CI job.
    let invocations = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let invs = get_lambda_invocations(server.endpoint()).await;
            let saw_sqs = invs["invocations"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .any(|inv| inv["source"].as_str() == Some("aws:sqs"))
                })
                .unwrap_or(false);
            if saw_sqs {
                break invs;
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("timed out waiting for Lambda invocation");
    let inv_list = invocations["invocations"].as_array().unwrap();
    assert!(
        !inv_list.is_empty(),
        "expected at least one Lambda invocation"
    );

    // Verify the invocation payload contains the SQS message. Find the
    // `aws:sqs`-shaped invocation explicitly: the LambdaDelivery adapter
    // also records an `aws:lambda:delivery` entry at the start of every
    // invoke, so under slow Docker startup the last entry can be the
    // delivery record rather than the post-invoke poller record.
    let inv = inv_list
        .iter()
        .rev()
        .find(|inv| inv["source"].as_str() == Some("aws:sqs"))
        .expect("expected an aws:sqs-shaped Lambda invocation payload");
    assert!(inv["functionArn"]
        .as_str()
        .unwrap()
        .contains("sqs-processor"));
    let payload: serde_json::Value =
        serde_json::from_str(inv["payload"].as_str().unwrap()).unwrap();
    assert_eq!(payload["Records"][0]["body"], r#"{"order_id": "12345"}"#);
    assert_eq!(payload["Records"][0]["eventSource"], "aws:sqs");

    // The message should be consumed (not available in SQS anymore)
    let msgs = sqs
        .receive_message()
        .queue_url(&queue_url)
        .wait_time_seconds(1)
        .send()
        .await
        .unwrap();
    assert!(
        msgs.messages().is_empty(),
        "message should have been consumed by Lambda poller"
    );
}

/// Kinesis -> Lambda: event source mapping triggers Lambda invocation.
#[tokio::test]
async fn kinesis_lambda_event_source_mapping() {
    let server = TestServer::start().await;
    let kinesis = server.kinesis_client().await;
    let lambda = server.lambda_client().await;

    kinesis
        .create_stream()
        .stream_name("lambda-trigger-stream")
        .shard_count(1)
        .send()
        .await
        .unwrap();

    let stream = kinesis
        .describe_stream_summary()
        .stream_name("lambda-trigger-stream")
        .send()
        .await
        .unwrap();
    let stream_arn = stream
        .stream_description_summary()
        .unwrap()
        .stream_arn()
        .to_string();

    lambda
        .create_function()
        .function_name("kinesis-processor")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/lambda-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(aws_sdk_lambda::primitives::Blob::new(make_zip(&[(
                    "index.py",
                    br#"def handler(event, context):
    return {"statusCode": 200}
"#,
                )])))
                .build(),
        )
        .send()
        .await
        .unwrap();

    lambda
        .create_event_source_mapping()
        .event_source_arn(&stream_arn)
        .function_name("kinesis-processor")
        .batch_size(10)
        .enabled(true)
        .send()
        .await
        .unwrap();

    kinesis
        .put_record()
        .stream_name("lambda-trigger-stream")
        .partition_key("key-1")
        .data(aws_sdk_kinesis::primitives::Blob::new(
            br#"{"order_id":"kinesis-123"}"#,
        ))
        .send()
        .await
        .unwrap();

    // Poll for the Kinesis-shaped Lambda invocation rather than sleeping
    // a fixed duration. Bounded by a 10s deadline.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let (invocations, inv) = loop {
        let invocations = get_lambda_invocations(server.endpoint()).await;
        let inv_list = invocations["invocations"].as_array().unwrap().clone();
        let kinesis_inv = inv_list.iter().rev().find(|inv| {
            inv["payload"]
                .as_str()
                .unwrap_or("")
                .contains("\"eventSource\":\"aws:kinesis\"")
        });
        if let Some(inv) = kinesis_inv {
            break (invocations, inv.clone());
        }
        if std::time::Instant::now() >= deadline {
            panic!("expected a Kinesis-shaped Lambda invocation within 10s");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    };
    let inv_list = invocations["invocations"].as_array().unwrap();
    assert!(
        !inv_list.is_empty(),
        "expected at least one Lambda invocation"
    );
    assert!(inv["functionArn"]
        .as_str()
        .unwrap()
        .contains("kinesis-processor"));
    let payload: serde_json::Value =
        serde_json::from_str(inv["payload"].as_str().unwrap()).unwrap();
    assert_eq!(payload["Records"][0]["eventSource"], "aws:kinesis");
    assert_eq!(payload["Records"][0]["kinesis"]["partitionKey"], "key-1");

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let invocations_again = get_lambda_invocations(server.endpoint()).await;
    let inv_list_again = invocations_again["invocations"].as_array().unwrap();
    let kinesis_invocation_count = inv_list
        .iter()
        .filter(|inv| {
            inv["payload"]
                .as_str()
                .unwrap_or("")
                .contains("\"eventSource\":\"aws:kinesis\"")
        })
        .count();
    let kinesis_invocation_count_again = inv_list_again
        .iter()
        .filter(|inv| {
            inv["payload"]
                .as_str()
                .unwrap_or("")
                .contains("\"eventSource\":\"aws:kinesis\"")
        })
        .count();
    assert_eq!(
        kinesis_invocation_count, 1,
        "one Kinesis record should produce exactly one Lambda invocation"
    );
    assert_eq!(
        kinesis_invocation_count_again, kinesis_invocation_count,
        "checkpointed Kinesis records should not be redelivered"
    );
}

/// EventBridge -> Lambda: put_events with a Lambda target records invocation.
#[tokio::test]
async fn eventbridge_lambda_delivery() {
    let server = TestServer::start().await;
    let eb = server.eventbridge_client().await;
    let lambda = server.lambda_client().await;

    // Create Lambda function
    lambda
        .create_function()
        .function_name("eb-handler")
        .runtime(aws_sdk_lambda::types::Runtime::Nodejs18x)
        .role("arn:aws:iam::123456789012:role/lambda-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(aws_sdk_lambda::primitives::Blob::new(b"fake-code"))
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Create EventBridge rule with Lambda target
    eb.put_rule()
        .name("lambda-rule")
        .event_pattern(r#"{"source": ["myapp"]}"#)
        .send()
        .await
        .unwrap();

    let lambda_arn = "arn:aws:lambda:us-east-1:123456789012:function:eb-handler";
    eb.put_targets()
        .rule("lambda-rule")
        .targets(
            Target::builder()
                .id("lambda-1")
                .arn(lambda_arn)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Send event
    eb.put_events()
        .entries(
            PutEventsRequestEntry::builder()
                .source("myapp")
                .detail_type("OrderCreated")
                .detail(r#"{"order_id": "99"}"#)
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Check invocations via internal API
    let invocations = get_lambda_invocations(server.endpoint()).await;
    let inv_list = invocations["invocations"].as_array().unwrap();
    let eb_invocations: Vec<_> = inv_list
        .iter()
        .filter(|i| i["source"] == "aws:events")
        .collect();
    assert!(
        !eb_invocations.is_empty(),
        "expected EventBridge->Lambda invocation"
    );
    assert!(eb_invocations[0]["functionArn"]
        .as_str()
        .unwrap()
        .contains("eb-handler"));
}

/// EventBridge -> CloudWatch Logs: put_events with a Logs target writes to log group.
#[tokio::test]
async fn eventbridge_logs_delivery() {
    let server = TestServer::start().await;
    let eb = server.eventbridge_client().await;
    let logs = server.logs_client().await;

    // Create a log group (EventBridge will auto-create if needed, but let's be explicit)
    logs.create_log_group()
        .log_group_name("/aws/events/my-rule")
        .send()
        .await
        .unwrap();

    let log_group_arn = "arn:aws:logs:us-east-1:123456789012:log-group:/aws/events/my-rule";

    // Create rule targeting CloudWatch Logs
    eb.put_rule()
        .name("logs-rule")
        .event_pattern(r#"{"source": ["audit"]}"#)
        .send()
        .await
        .unwrap();

    eb.put_targets()
        .rule("logs-rule")
        .targets(
            Target::builder()
                .id("logs-1")
                .arn(log_group_arn)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Send event
    eb.put_events()
        .entries(
            PutEventsRequestEntry::builder()
                .source("audit")
                .detail_type("UserLogin")
                .detail(r#"{"user": "alice"}"#)
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Check CloudWatch Logs for the event
    let streams = logs
        .describe_log_streams()
        .log_group_name("/aws/events/my-rule")
        .send()
        .await
        .unwrap();
    assert!(!streams.log_streams().is_empty(), "expected log stream");

    let events = logs
        .get_log_events()
        .log_group_name("/aws/events/my-rule")
        .log_stream_name("events")
        .send()
        .await
        .unwrap();
    let log_events = events.events();
    assert!(!log_events.is_empty(), "expected log events");

    let msg = log_events[0].message().unwrap();
    let parsed: serde_json::Value = serde_json::from_str(msg).unwrap();
    assert_eq!(parsed["source"], "audit");
    assert_eq!(parsed["detail-type"], "UserLogin");
}

/// Turn on EventBridge notifications for `bucket`.
async fn enable_s3_eventbridge_notifications(s3: &aws_sdk_s3::Client, bucket: &str) {
    s3.put_bucket_notification_configuration()
        .bucket(bucket)
        .notification_configuration(
            aws_sdk_s3::types::NotificationConfiguration::builder()
                .event_bridge_configuration(
                    aws_sdk_s3::types::EventBridgeConfiguration::builder().build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
}

/// S3 -> EventBridge -> Lambda (#2628): an `Object Created` event S3 publishes
/// to the default bus reaches a rule's Lambda target exactly like PutEvents,
/// so the invocation is recorded (and executed when a container runtime is
/// available) instead of being logged and dropped.
#[tokio::test]
async fn s3_eventbridge_notification_invokes_lambda_target() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    let eb = server.eventbridge_client().await;
    let lambda = server.lambda_client().await;

    lambda
        .create_function()
        .function_name("s3-eb-fn")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/lambda-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(aws_sdk_lambda::primitives::Blob::new(make_zip(&[(
                    "index.py",
                    b"def handler(event, context):\n    return {}\n",
                )])))
                .build(),
        )
        .send()
        .await
        .unwrap();

    eb.put_rule()
        .name("s3-to-fn")
        .event_pattern(r#"{"source":["aws.s3"],"detail-type":["Object Created"]}"#)
        .send()
        .await
        .unwrap();
    let fn_arn = "arn:aws:lambda:us-east-1:123456789012:function:s3-eb-fn";
    eb.put_targets()
        .rule("s3-to-fn")
        .targets(Target::builder().id("fn").arn(fn_arn).build().unwrap())
        .send()
        .await
        .unwrap();

    s3.create_bucket().bucket("eb-bucket").send().await.unwrap();
    enable_s3_eventbridge_notifications(&s3, "eb-bucket").await;
    s3.put_object()
        .bucket("eb-bucket")
        .key("anything")
        .body(ByteStream::from_static(b"payload"))
        .send()
        .await
        .unwrap();

    // Poll until the invocation is recorded, so the test doesn't depend on the
    // delivery being synchronous with the PutObject response.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let matched = loop {
        let invocations = get_lambda_invocations(server.endpoint()).await;
        let matched: Vec<serde_json::Value> = invocations["invocations"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|i| i["source"] == "aws:events" && i["functionArn"] == fn_arn)
            .cloned()
            .collect();
        if !matched.is_empty() {
            break matched;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "expected an S3->EventBridge->Lambda invocation, got {invocations}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    };
    assert_eq!(matched.len(), 1, "expected exactly one invocation");
    let payload: serde_json::Value =
        serde_json::from_str(matched[0]["payload"].as_str().unwrap()).unwrap();
    assert_eq!(payload["source"], "aws.s3");
    assert_eq!(payload["detail-type"], "Object Created");
    assert_eq!(payload["detail"]["bucket"]["name"], "eb-bucket");
    assert_eq!(payload["detail"]["object"]["key"], "anything");
}

/// S3 -> EventBridge -> Lambda (#2628), proving the function really *runs* in
/// its container rather than only being recorded: the handler writes the
/// object key it received into DynamoDB, which the test then reads back.
#[tokio::test]
async fn s3_eventbridge_notification_executes_lambda_target() {
    if !require_docker_or_skip("s3_eventbridge_notification_executes_lambda_target") {
        return;
    }
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    let eb = server.eventbridge_client().await;
    let lambda = server.lambda_client().await;
    let ddb = server.dynamodb_client().await;

    ddb.create_table()
        .table_name("s3-eb-seen")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    let handler = br#"import boto3

def handler(event, context):
    detail = event["detail"]
    boto3.client("dynamodb").put_item(
        TableName="s3-eb-seen",
        Item={
            "pk": {"S": detail["object"]["key"]},
            "bucket": {"S": detail["bucket"]["name"]},
            "source": {"S": event["source"]},
        },
    )
    return {"ok": True}
"#;
    lambda
        .create_function()
        .function_name("s3-eb-exec-fn")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/lambda-role")
        .handler("index.handler")
        .timeout(30)
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(aws_sdk_lambda::primitives::Blob::new(make_zip(&[(
                    "index.py", handler,
                )])))
                .build(),
        )
        .send()
        .await
        .unwrap();

    eb.put_rule()
        .name("s3-to-exec-fn")
        .event_pattern(r#"{"source":["aws.s3"],"detail-type":["Object Created"]}"#)
        .send()
        .await
        .unwrap();
    eb.put_targets()
        .rule("s3-to-exec-fn")
        .targets(
            Target::builder()
                .id("fn")
                .arn("arn:aws:lambda:us-east-1:123456789012:function:s3-eb-exec-fn")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    s3.create_bucket()
        .bucket("eb-exec-bucket")
        .send()
        .await
        .unwrap();
    enable_s3_eventbridge_notifications(&s3, "eb-exec-bucket").await;
    s3.put_object()
        .bucket("eb-exec-bucket")
        .key("ran.txt")
        .body(ByteStream::from_static(b"payload"))
        .send()
        .await
        .unwrap();

    // Delivery is asynchronous and the first invoke pays the container cold
    // start (image pull on a fresh runner), so poll generously.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(180);
    let item = loop {
        let got = ddb
            .get_item()
            .table_name("s3-eb-seen")
            .key("pk", AttributeValue::S("ran.txt".to_string()))
            .send()
            .await
            .unwrap();
        if let Some(item) = got.item().cloned() {
            break item;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "S3->EventBridge->Lambda target never executed (no DynamoDB write from the handler)"
        );
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    };
    assert_eq!(
        item.get("bucket"),
        Some(&AttributeValue::S("eb-exec-bucket".to_string()))
    );
    assert_eq!(
        item.get("source"),
        Some(&AttributeValue::S("aws.s3".to_string()))
    );
}

/// S3 -> EventBridge -> CloudWatch Logs: the cross-service delivery writes a
/// Logs target's log group, matching PutEvents.
#[tokio::test]
async fn s3_eventbridge_notification_delivers_to_logs_target() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    let eb = server.eventbridge_client().await;
    let logs = server.logs_client().await;

    logs.create_log_group()
        .log_group_name("/aws/events/s3")
        .send()
        .await
        .unwrap();
    eb.put_rule()
        .name("s3-to-logs")
        .event_pattern(r#"{"source":["aws.s3"]}"#)
        .send()
        .await
        .unwrap();
    eb.put_targets()
        .rule("s3-to-logs")
        .targets(
            Target::builder()
                .id("logs")
                .arn("arn:aws:logs:us-east-1:123456789012:log-group:/aws/events/s3")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    s3.create_bucket()
        .bucket("eb-logs-bucket")
        .send()
        .await
        .unwrap();
    enable_s3_eventbridge_notifications(&s3, "eb-logs-bucket").await;
    s3.put_object()
        .bucket("eb-logs-bucket")
        .key("k.txt")
        .body(ByteStream::from_static(b"payload"))
        .send()
        .await
        .unwrap();

    // Poll until the event lands (the "events" stream only exists once the
    // first delivery creates it), so the test doesn't depend on delivery being
    // synchronous with the PutObject response.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let log_events = loop {
        let got = logs
            .get_log_events()
            .log_group_name("/aws/events/s3")
            .log_stream_name("events")
            .send()
            .await;
        let last_error = match got {
            Ok(out) if !out.events().is_empty() => break out.events().to_vec(),
            Ok(_) => None,
            // The stream doesn't exist until the first delivery creates it;
            // any other error is permanent and fails immediately.
            Err(err) => {
                let svc = err.into_service_error();
                assert!(
                    svc.is_resource_not_found_exception(),
                    "GetLogEvents failed: {svc:?}"
                );
                Some(svc)
            }
        };
        assert!(
            std::time::Instant::now() < deadline,
            "S3 event never reached the CloudWatch Logs target (last error: {last_error:?})"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    };
    assert_eq!(
        log_events.len(),
        1,
        "expected one S3 event in the log group"
    );
    let parsed: serde_json::Value = serde_json::from_str(log_events[0].message().unwrap()).unwrap();
    assert_eq!(parsed["source"], "aws.s3");
    assert_eq!(parsed["detail"]["bucket"]["name"], "eb-logs-bucket");
    assert_eq!(parsed["detail"]["object"]["key"], "k.txt");
}

/// S3 -> KMS: PutObject with aws:kms encryption stores KMS key ID,
/// bucket default encryption applies KMS to all objects.
#[tokio::test]
async fn s3_kms_encryption() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    let kms = server.kms_client().await;

    // Create a KMS key
    let key_resp = kms
        .create_key()
        .description("S3 encryption key")
        .send()
        .await
        .unwrap();
    let key_id = key_resp.key_metadata().unwrap().key_id().to_string();
    let key_arn = key_resp.key_metadata().unwrap().arn().unwrap().to_string();

    // Create S3 bucket
    s3.create_bucket()
        .bucket("kms-test-bucket")
        .send()
        .await
        .unwrap();

    // Put object with explicit KMS encryption
    s3.put_object()
        .bucket("kms-test-bucket")
        .key("encrypted.txt")
        .body(ByteStream::from_static(b"secret data"))
        .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::AwsKms)
        .ssekms_key_id(&key_id)
        .send()
        .await
        .unwrap();

    // Get the object and verify SSE headers
    let get = s3
        .get_object()
        .bucket("kms-test-bucket")
        .key("encrypted.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(get.server_side_encryption().unwrap().as_str(), "aws:kms");
    assert!(
        get.ssekms_key_id().unwrap().contains(&key_id) || get.ssekms_key_id().unwrap() == key_arn
    );

    // Set bucket default encryption to KMS via CLI (JSON format)
    let encryption_json = format!(
        r#"{{"Rules":[{{"ApplyServerSideEncryptionByDefault":{{"SSEAlgorithm":"aws:kms","KMSMasterKeyID":"{key_id}"}}}}]}}"#
    );
    let output = server
        .aws_cli(&[
            "s3api",
            "put-bucket-encryption",
            "--bucket",
            "kms-test-bucket",
            "--server-side-encryption-configuration",
            &encryption_json,
        ])
        .await;
    assert!(
        output.success(),
        "put-bucket-encryption failed: {}",
        output.stderr_text()
    );

    // Put object without explicit SSE - should inherit bucket default KMS
    s3.put_object()
        .bucket("kms-test-bucket")
        .key("auto-encrypted.txt")
        .body(ByteStream::from_static(b"auto encrypted data"))
        .send()
        .await
        .unwrap();

    // Get the auto-encrypted object
    let get2 = s3
        .get_object()
        .bucket("kms-test-bucket")
        .key("auto-encrypted.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(get2.server_side_encryption().unwrap().as_str(), "aws:kms");
    // Key should be resolved to the full ARN
    assert!(
        get2.ssekms_key_id().is_some(),
        "expected KMS key ID on auto-encrypted object"
    );
}

/// CloudWatch Logs subscription filter -> SQS: verify that log events
/// matching a subscription filter are delivered to the SQS queue.
#[tokio::test]
async fn logs_subscription_filter_delivers_to_sqs() {
    use aws_sdk_cloudwatchlogs::types::InputLogEvent;
    use base64::Engine;
    use std::io::Read;

    let server = TestServer::start().await;
    let logs = server.logs_client().await;
    let sqs = server.sqs_client().await;

    // Create SQS queue
    let queue = sqs
        .create_queue()
        .queue_name("logs-sub-queue")
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap().to_string();
    let queue_arn = get_queue_arn(&sqs, &queue_url).await;

    // Create log group and stream
    let group_name = "/test/subscription";
    let stream_name = "app-stream";

    logs.create_log_group()
        .log_group_name(group_name)
        .send()
        .await
        .unwrap();

    logs.create_log_stream()
        .log_group_name(group_name)
        .log_stream_name(stream_name)
        .send()
        .await
        .unwrap();

    // Put subscription filter targeting the SQS queue
    logs.put_subscription_filter()
        .log_group_name(group_name)
        .filter_name("all-events")
        .filter_pattern("")
        .destination_arn(&queue_arn)
        .send()
        .await
        .unwrap();

    // Put log events
    let now = chrono::Utc::now().timestamp_millis();
    logs.put_log_events()
        .log_group_name(group_name)
        .log_stream_name(stream_name)
        .log_events(
            InputLogEvent::builder()
                .timestamp(now)
                .message("hello from subscription test")
                .build()
                .unwrap(),
        )
        .log_events(
            InputLogEvent::builder()
                .timestamp(now + 1)
                .message("second event")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Receive message from SQS. Poll up to a deadline rather than a single
    // 1s receive so delivery latency under parallel CI load can't flake the
    // assertion.
    let messages =
        helpers::sqs_receive_at_least(&sqs, &queue_url, 1, std::time::Duration::from_secs(5)).await;
    assert_eq!(messages.len(), 1, "expected exactly one SQS message");

    // Decode the payload: base64 -> gzip -> JSON
    let body = messages[0].body().unwrap();
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(body)
        .unwrap();
    let mut decoder = flate2::read::GzDecoder::new(&decoded[..]);
    let mut json_str = String::new();
    decoder.read_to_string(&mut json_str).unwrap();
    let payload: serde_json::Value = serde_json::from_str(&json_str).unwrap();

    assert_eq!(payload["messageType"], "DATA_MESSAGE");
    assert_eq!(payload["logGroup"], group_name);
    assert_eq!(payload["logStream"], stream_name);
    assert_eq!(payload["subscriptionFilters"][0], "all-events");

    let log_events = payload["logEvents"].as_array().unwrap();
    assert_eq!(log_events.len(), 2);
    assert_eq!(log_events[0]["message"], "hello from subscription test");
    assert_eq!(log_events[1]["message"], "second event");
}

// make_zip is defined at the top of this file

/// Poll an AWS CLI describe call until the job status at `status_pointer`
/// leaves IN_PROGRESS, returning the final JSON.
async fn wait_for_cli_job(
    server: &TestServer,
    args: &[&str],
    status_pointer: &str,
) -> serde_json::Value {
    for _ in 0..200 {
        let out = server.aws_cli(args).await;
        assert!(out.success(), "{args:?} failed: {}", out.stderr_text());
        let json = out.stdout_json();
        if json.pointer(status_pointer).and_then(|v| v.as_str()) != Some("IN_PROGRESS") {
            return json;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("{args:?} never left IN_PROGRESS");
}

/// DynamoDB export to S3 and import from S3 roundtrip test.
#[tokio::test]
async fn dynamodb_export_import_roundtrip() {
    let server = TestServer::start().await;
    let ddb = server.dynamodb_client().await;
    let s3 = server.s3_client().await;

    // Create source table
    ddb.create_table()
        .table_name("ExportSource")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    // Put items
    for i in 1..=3 {
        ddb.put_item()
            .table_name("ExportSource")
            .item("pk", AttributeValue::S(format!("item-{i}")))
            .item("data", AttributeValue::S(format!("value-{i}")))
            .item("count", AttributeValue::N(i.to_string()))
            .send()
            .await
            .unwrap();
    }

    // Create S3 bucket for export
    s3.create_bucket()
        .bucket("export-bucket")
        .send()
        .await
        .unwrap();

    // Get table ARN
    let table_desc = ddb
        .describe_table()
        .table_name("ExportSource")
        .send()
        .await
        .unwrap();
    let table_arn = table_desc.table().unwrap().table_arn().unwrap().to_string();

    // Export via CLI (SDK ExportTableToPointInTime is complex)
    let export_output = server
        .aws_cli(&[
            "dynamodb",
            "export-table-to-point-in-time",
            "--table-arn",
            &table_arn,
            "--s3-bucket",
            "export-bucket",
            "--s3-prefix",
            "exports/source",
            "--export-format",
            "DYNAMODB_JSON",
        ])
        .await;
    assert!(
        export_output.success(),
        "export failed: {}",
        export_output.stderr_text()
    );
    let export_json = export_output.stdout_json();
    // The start call reports the export as accepted; its counts come from
    // DescribeExport once it has finished.
    assert_eq!(
        export_json["ExportDescription"]["ExportStatus"],
        "IN_PROGRESS"
    );
    let export_arn = export_json["ExportDescription"]["ExportArn"]
        .as_str()
        .unwrap()
        .to_string();
    // The export runs in the background; poll DescribeExport as real clients
    // do until it settles.
    let describe_json = wait_for_cli_job(
        &server,
        &["dynamodb", "describe-export", "--export-arn", &export_arn],
        "/ExportDescription/ExportStatus",
    )
    .await;
    assert_eq!(
        describe_json["ExportDescription"]["ExportStatus"],
        "COMPLETED"
    );
    let item_count = describe_json["ExportDescription"]["ItemCount"]
        .as_i64()
        .unwrap_or(0);
    assert_eq!(item_count, 3, "Expected 3 items exported");

    // The export lands in the AWS layout:
    // <prefix>/AWSDynamoDB/<export-id>/{manifest-summary.json,manifest-files.json,data/*.json.gz}
    let export_id = export_arn.rsplit('/').next().unwrap();
    let base = format!("exports/source/AWSDynamoDB/{export_id}");
    assert_eq!(
        describe_json["ExportDescription"]["ExportManifest"],
        format!("{base}/manifest-summary.json")
    );
    let get_text = |key: String| {
        let s3 = s3.clone();
        async move {
            let obj = s3
                .get_object()
                .bucket("export-bucket")
                .key(&key)
                .send()
                .await
                .unwrap_or_else(|e| panic!("get {key}: {e:?}"));
            obj.body.collect().await.unwrap().into_bytes().to_vec()
        }
    };
    let summary: serde_json::Value =
        serde_json::from_slice(&get_text(format!("{base}/manifest-summary.json")).await).unwrap();
    assert_eq!(summary["itemCount"], 3);
    assert_eq!(summary["outputFormat"], "DYNAMODB_JSON");
    let files = get_text(format!("{base}/manifest-files.json")).await;
    let entry: serde_json::Value =
        serde_json::from_str(std::str::from_utf8(&files).unwrap().lines().next().unwrap()).unwrap();
    let data_key = entry["dataFileS3Key"].as_str().unwrap().to_string();
    assert!(data_key.starts_with(&format!("{base}/data/")) && data_key.ends_with(".json.gz"));
    let gz = get_text(data_key).await;
    let mut s3_text = String::new();
    std::io::Read::read_to_string(&mut flate2::read::GzDecoder::new(&gz[..]), &mut s3_text)
        .unwrap();
    // Should have 3 lines (one per item)
    let lines: Vec<&str> = s3_text.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(lines.len(), 3, "Expected 3 JSON Lines in export");

    // Now import the export's data files (gzip DynamoDB JSON) into a new table
    let source = format!(r#"{{"S3Bucket":"export-bucket","S3KeyPrefix":"{base}/data/"}}"#);
    let import_output = server
        .aws_cli(&[
            "dynamodb",
            "import-table",
            "--input-format",
            "DYNAMODB_JSON",
            "--input-compression-type",
            "GZIP",
            "--s3-bucket-source",
            &source,
            "--table-creation-parameters",
            r#"{"TableName":"ImportDest","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"BillingMode":"PAY_PER_REQUEST"}"#,
        ])
        .await;
    assert!(
        import_output.success(),
        "import failed: {}",
        import_output.stderr_text()
    );
    let import_json = import_output.stdout_json();
    assert_eq!(
        import_json["ImportTableDescription"]["ImportStatus"],
        "IN_PROGRESS"
    );
    let import_arn = import_json["ImportTableDescription"]["ImportArn"]
        .as_str()
        .unwrap()
        .to_string();
    let describe_json = wait_for_cli_job(
        &server,
        &["dynamodb", "describe-import", "--import-arn", &import_arn],
        "/ImportTableDescription/ImportStatus",
    )
    .await;
    assert_eq!(
        describe_json["ImportTableDescription"]["ImportStatus"], "COMPLETED",
        "{describe_json}"
    );
    let processed = describe_json["ImportTableDescription"]["ProcessedItemCount"]
        .as_i64()
        .unwrap_or(0);
    assert_eq!(processed, 3, "Expected 3 items imported");
    let table = ddb
        .describe_table()
        .table_name("ImportDest")
        .send()
        .await
        .unwrap();
    assert_eq!(
        table.table().unwrap().table_status().unwrap().as_str(),
        "ACTIVE"
    );

    // Verify items in the imported table
    let scan = ddb.scan().table_name("ImportDest").send().await.unwrap();
    assert_eq!(scan.count(), 3, "Imported table should have 3 items");

    // Verify item data matches
    let items = scan.items();
    for item in items {
        let pk = item.get("pk").unwrap().as_s().unwrap();
        assert!(
            pk.starts_with("item-"),
            "Item pk should start with 'item-': {pk}"
        );
        assert!(
            item.contains_key("data"),
            "Item should have 'data' attribute"
        );
        assert!(
            item.contains_key("count"),
            "Item should have 'count' attribute"
        );
    }
}

/// SecretsManager rotation invokes the configured Lambda function with the correct payload.
#[tokio::test]
async fn secretsmanager_rotation_invokes_lambda() {
    if !require_docker_or_skip("secretsmanager_rotation_invokes_lambda") {
        return;
    }
    let server = TestServer::start().await;
    let sm = server.secretsmanager_client().await;
    let lambda = server.lambda_client().await;

    // Create a Lambda function (no real code needed -- invocation is recorded regardless)
    lambda
        .create_function()
        .function_name("rotation-handler")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/lambda-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(aws_sdk_lambda::primitives::Blob::new(b"fake-code"))
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Create a secret
    sm.create_secret()
        .name("rotation-test-secret")
        .secret_string("old-password")
        .send()
        .await
        .unwrap();

    let lambda_arn = "arn:aws:lambda:us-east-1:123456789012:function:rotation-handler";
    let token = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

    // Rotate the secret with a Lambda ARN
    let resp = sm
        .rotate_secret()
        .secret_id("rotation-test-secret")
        .rotation_lambda_arn(lambda_arn)
        .client_request_token(token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.version_id().unwrap(), token);

    // Poll for all 4 rotation Lambda invocations (background task is async)
    let expected_steps = ["createSecret", "setSecret", "testSecret", "finishSecret"];
    let mut rotation_invocations = Vec::new();
    for attempt in 0..10 {
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        let invocations = get_lambda_invocations(server.endpoint()).await;
        let inv_list = invocations["invocations"].as_array().unwrap().clone();
        rotation_invocations = inv_list
            .into_iter()
            .filter(|i| {
                i["functionArn"]
                    .as_str()
                    .unwrap_or("")
                    .contains("rotation-handler")
            })
            .collect::<Vec<_>>();
        if rotation_invocations.len() >= expected_steps.len() {
            break;
        }
    }

    assert_eq!(
        rotation_invocations.len(),
        expected_steps.len(),
        "expected {} rotation Lambda invocations, got {}: {rotation_invocations:?}",
        expected_steps.len(),
        rotation_invocations.len(),
    );

    // Verify each rotation step was invoked in order
    for (inv, expected_step) in rotation_invocations.iter().zip(expected_steps.iter()) {
        let payload: serde_json::Value =
            serde_json::from_str(inv["payload"].as_str().unwrap()).unwrap();
        assert!(
            payload["SecretId"]
                .as_str()
                .unwrap()
                .contains("rotation-test-secret"),
            "SecretId should contain the secret name/ARN, got: {}",
            payload["SecretId"]
        );
        assert_eq!(payload["ClientRequestToken"], token);
        assert_eq!(
            payload["Step"], *expected_step,
            "expected step {expected_step}, got {}",
            payload["Step"]
        );
    }

    // Verify rotation metadata was set on the secret. (We do NOT assert that
    // AWSPENDING exists, because real AWS Secrets Manager does not pre-create
    // the AWSPENDING version — the rotation Lambda is responsible for putting
    // it via PutSecretValue. This test uses a no-op Lambda that records
    // invocations but doesn't put any value, so no AWSPENDING version exists.)
    let describe = sm
        .describe_secret()
        .secret_id("rotation-test-secret")
        .send()
        .await
        .unwrap();
    assert_eq!(describe.rotation_enabled(), Some(true));
    assert_eq!(
        describe.rotation_lambda_arn().unwrap(),
        lambda_arn,
        "rotation Lambda ARN should be recorded on the secret"
    );
}

/// EventBridge -> SNS -> SQS with FilterPolicyScope=MessageBody. The
/// cross-service publish path must apply the subscription's filter
/// policy the same way the direct `Publish` op does, so a non-matching
/// EB event must not deliver to the SQS subscriber.
#[tokio::test]
async fn eventbridge_sns_filter_policy_drops_non_matching() {
    let server = TestServer::start().await;
    let eb = server.eventbridge_client().await;
    let sns = server.sns_client().await;
    let sqs = server.sqs_client().await;

    let topic = sns
        .create_topic()
        .name("eb-filtered-topic")
        .send()
        .await
        .unwrap();
    let topic_arn = topic.topic_arn().unwrap().to_string();

    let queue = sqs
        .create_queue()
        .queue_name("eb-filtered-queue")
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap().to_string();
    let queue_arn = get_queue_arn(&sqs, &queue_url).await;

    let sub = sns
        .subscribe()
        .topic_arn(&topic_arn)
        .protocol("sqs")
        .endpoint(&queue_arn)
        .send()
        .await
        .unwrap();
    let sub_arn = sub.subscription_arn().unwrap().to_string();

    // FilterPolicyScope=MessageBody so the policy matches against the
    // EventBridge event JSON delivered as the SNS Message body.
    sns.set_subscription_attributes()
        .subscription_arn(&sub_arn)
        .attribute_name("FilterPolicyScope")
        .attribute_value("MessageBody")
        .send()
        .await
        .unwrap();
    sns.set_subscription_attributes()
        .subscription_arn(&sub_arn)
        .attribute_name("FilterPolicy")
        .attribute_value(r#"{"source":["payments"]}"#)
        .send()
        .await
        .unwrap();

    // Rule routes everything from sources "payments" and "auth" to the SNS topic
    eb.put_rule()
        .name("filter-rule")
        .event_pattern(r#"{"source": ["payments", "auth"]}"#)
        .send()
        .await
        .unwrap();
    eb.put_targets()
        .rule("filter-rule")
        .targets(
            Target::builder()
                .id("sns-1")
                .arn(&topic_arn)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

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
    eb.put_events()
        .entries(
            PutEventsRequestEntry::builder()
                .source("auth")
                .detail_type("LoginAttempt")
                .detail(r#"{"user": "alice"}"#)
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Wait briefly for async fan-out
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let msgs = sqs
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();
    let received = msgs.messages();
    assert_eq!(
        received.len(),
        1,
        "filter policy should drop the auth event; got {} msgs",
        received.len()
    );
    // The delivered message is wrapped in an SNS notification envelope
    // around the original EventBridge event JSON.
    let envelope: serde_json::Value = serde_json::from_str(received[0].body().unwrap()).unwrap();
    assert_eq!(envelope["Type"], "Notification");
    let inner: serde_json::Value =
        serde_json::from_str(envelope["Message"].as_str().unwrap()).unwrap();
    assert_eq!(inner["source"], "payments");
}

/// Poll DescribeImport until the background import job settles.
async fn wait_for_import(
    ddb: &aws_sdk_dynamodb::Client,
    import_arn: &str,
) -> aws_sdk_dynamodb::types::ImportTableDescription {
    for _ in 0..200 {
        let desc = ddb
            .describe_import()
            .import_arn(import_arn)
            .send()
            .await
            .unwrap()
            .import_table_description()
            .unwrap()
            .clone();
        if desc.import_status().map(|s| s.as_str()) != Some("IN_PROGRESS") {
            return desc;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("import {import_arn} never settled");
}

/// ImportTable is a job: the call returns IN_PROGRESS at once and the
/// gzip-compressed CSV is read, decompressed and parsed afterwards. CSV key
/// columns take their AttributeDefinitions type; other columns are strings.
/// A missing source bucket fails the job (not the call) and leaves no table.
#[tokio::test]
async fn dynamodb_import_table_gzip_csv_job() {
    let server = helpers::TestServer::start().await;
    let s3 = server.s3_client().await;
    let ddb = server.dynamodb_client().await;

    s3.create_bucket()
        .bucket("csv-import")
        .send()
        .await
        .unwrap();
    let csv = "id,name,score\n1,alice,10\n2,\"bob, jr\",\n";
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(csv.as_bytes()).unwrap();
    s3.put_object()
        .bucket("csv-import")
        .key("in/part-0.csv.gz")
        .body(enc.finish().unwrap().into())
        .send()
        .await
        .unwrap();

    let params = |table: &str| {
        aws_sdk_dynamodb::types::TableCreationParameters::builder()
            .table_name(table)
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("id")
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("id")
                    .attribute_type(ScalarAttributeType::N)
                    .build()
                    .unwrap(),
            )
            .billing_mode(BillingMode::PayPerRequest)
            .build()
            .unwrap()
    };
    let resp = ddb
        .import_table()
        .input_format(aws_sdk_dynamodb::types::InputFormat::Csv)
        .input_compression_type(aws_sdk_dynamodb::types::InputCompressionType::Gzip)
        .s3_bucket_source(
            aws_sdk_dynamodb::types::S3BucketSource::builder()
                .s3_bucket("csv-import")
                .s3_key_prefix("in/")
                .build()
                .unwrap(),
        )
        .table_creation_parameters(params("CsvImported"))
        .send()
        .await
        .unwrap();
    let started = resp.import_table_description().unwrap();
    assert_eq!(started.import_status().unwrap().as_str(), "IN_PROGRESS");
    // The table exists from the moment the import is accepted.
    ddb.describe_table()
        .table_name("CsvImported")
        .send()
        .await
        .unwrap();

    let desc = wait_for_import(&ddb, started.import_arn().unwrap()).await;
    assert_eq!(desc.import_status().unwrap().as_str(), "COMPLETED");
    assert_eq!(desc.imported_item_count(), 2);
    assert_eq!(desc.input_compression_type().unwrap().as_str(), "GZIP");
    let table = ddb
        .describe_table()
        .table_name("CsvImported")
        .send()
        .await
        .unwrap();
    assert_eq!(
        table.table().unwrap().table_status().unwrap().as_str(),
        "ACTIVE"
    );
    let bob = ddb
        .get_item()
        .table_name("CsvImported")
        .key("id", AttributeValue::N("2".into()))
        .send()
        .await
        .unwrap();
    let bob = bob.item().unwrap();
    assert_eq!(bob["name"].as_s().unwrap(), "bob, jr");
    assert!(!bob.contains_key("score"), "empty CSV column is omitted");
    let alice = ddb
        .get_item()
        .table_name("CsvImported")
        .key("id", AttributeValue::N("1".into()))
        .send()
        .await
        .unwrap();
    assert_eq!(alice.item().unwrap()["score"].as_s().unwrap(), "10");

    let resp = ddb
        .import_table()
        .input_format(aws_sdk_dynamodb::types::InputFormat::Csv)
        .s3_bucket_source(
            aws_sdk_dynamodb::types::S3BucketSource::builder()
                .s3_bucket("no-such-import-bucket")
                .build()
                .unwrap(),
        )
        .table_creation_parameters(params("NeverCreated"))
        .send()
        .await
        .unwrap();
    let desc = wait_for_import(
        &ddb,
        resp.import_table_description()
            .unwrap()
            .import_arn()
            .unwrap(),
    )
    .await;
    assert_eq!(desc.import_status().unwrap().as_str(), "FAILED");
    assert_eq!(desc.failure_code(), Some("S3NoSuchBucket"));
    let err = ddb
        .describe_table()
        .table_name("NeverCreated")
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("ResourceNotFoundException"),
        "{err:?}"
    );
}

/// ImportTable writes each row the way PutItem would: a row without a valid
/// primary key (missing, wrong-typed or empty) or with a malformed attribute
/// value is an import error, counted and skipped, and a row repeating an
/// earlier row's key replaces it. Such rows used to be stored as-is, leaving
/// rows no key lookup could address and no Scan cursor could page past.
#[tokio::test]
async fn dynamodb_import_table_skips_invalid_keys_and_dedupes() {
    let server = helpers::TestServer::start().await;
    let s3 = server.s3_client().await;
    let ddb = server.dynamodb_client().await;

    s3.create_bucket()
        .bucket("import-validation")
        .send()
        .await
        .unwrap();
    let lines = [
        r#"{"Item":{"pk":{"S":"a"},"v":{"S":"first"}}}"#,
        r#"{"Item":{"pk":{"S":"b"}}}"#,
        r#"{"Item":{"other":{"S":"no key"}}}"#,
        r#"{"Item":{"pk":{"N":"5"}}}"#,
        r#"{"Item":{"pk":{"S":""}}}"#,
        r#"{"Item":{"pk":{"S":"c"},"n":{"N":"not-a-number"}}}"#,
        r#"{"Item":{"pk":{"S":"a"},"v":{"S":"second"}}}"#,
    ];
    s3.put_object()
        .bucket("import-validation")
        .key("data/part-0.json")
        .body(lines.join("\n").into_bytes().into())
        .send()
        .await
        .unwrap();

    let resp = ddb
        .import_table()
        .input_format(aws_sdk_dynamodb::types::InputFormat::DynamodbJson)
        .s3_bucket_source(
            aws_sdk_dynamodb::types::S3BucketSource::builder()
                .s3_bucket("import-validation")
                .s3_key_prefix("data/")
                .build()
                .unwrap(),
        )
        .table_creation_parameters(
            aws_sdk_dynamodb::types::TableCreationParameters::builder()
                .table_name("ImportValidated")
                .key_schema(
                    aws_sdk_dynamodb::types::KeySchemaElement::builder()
                        .attribute_name("pk")
                        .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                        .build()
                        .unwrap(),
                )
                .attribute_definitions(
                    aws_sdk_dynamodb::types::AttributeDefinition::builder()
                        .attribute_name("pk")
                        .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                        .build()
                        .unwrap(),
                )
                .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let import_arn = resp
        .import_table_description()
        .unwrap()
        .import_arn()
        .unwrap()
        .to_string();
    let desc = wait_for_import(&ddb, &import_arn).await;
    assert_eq!(desc.processed_item_count(), 7);
    assert_eq!(desc.imported_item_count(), 2);
    assert_eq!(desc.error_count(), 4);
    // Item validation errors fail the import (the valid rows stay imported).
    assert_eq!(desc.import_status().unwrap().as_str(), "FAILED");
    assert_eq!(desc.failure_code(), Some("ItemValidationError"));

    let scan = ddb
        .scan()
        .table_name("ImportValidated")
        .send()
        .await
        .unwrap();
    assert_eq!(scan.count(), 2, "{:?}", scan.items());
    let a = scan
        .items()
        .iter()
        .find(|item| item["pk"].as_s().unwrap() == "a")
        .expect("row a imported");
    assert_eq!(
        a["v"].as_s().unwrap(),
        "second",
        "a later row replaces an earlier one"
    );
}
