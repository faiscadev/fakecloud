mod helpers;

use std::io::Write;

use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType,
};
use aws_sdk_eventbridge::types::{PutEventsRequestEntry, Target};
use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::{Environment, FunctionCode, Runtime};
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
    lambda
        .create_function()
        .function_name("sqs-processor")
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

    // Wait for the poller to pick it up
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Check that Lambda was invoked
    let invocations = get_lambda_invocations(server.endpoint()).await;
    let inv_list = invocations["invocations"].as_array().unwrap();
    assert!(
        !inv_list.is_empty(),
        "expected at least one Lambda invocation"
    );

    // Verify the invocation payload contains the SQS message
    let inv = &inv_list[inv_list.len() - 1];
    assert!(inv["functionArn"]
        .as_str()
        .unwrap()
        .contains("sqs-processor"));
    assert_eq!(inv["source"], "aws:sqs");
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

    // Receive message from SQS
    let recv = sqs
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(10)
        .wait_time_seconds(1)
        .send()
        .await
        .unwrap();

    let messages = recv.messages().to_vec();
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
    let item_count = export_json["ExportDescription"]["ItemCount"]
        .as_i64()
        .unwrap_or(0);
    assert_eq!(item_count, 3, "Expected 3 items exported");

    // Verify that data was written to S3
    let s3_obj = s3
        .get_object()
        .bucket("export-bucket")
        .key("exports/source/data/manifest-files.json")
        .send()
        .await
        .unwrap();
    let s3_body = s3_obj.body.collect().await.unwrap().into_bytes();
    let s3_text = std::str::from_utf8(&s3_body).unwrap();
    assert!(!s3_text.is_empty(), "Export data should be non-empty in S3");
    // Should have 3 lines (one per item)
    let lines: Vec<&str> = s3_text.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(lines.len(), 3, "Expected 3 JSON Lines in export");

    // Now import into a new table
    let import_output = server
        .aws_cli(&[
            "dynamodb",
            "import-table",
            "--input-format",
            "DYNAMODB_JSON",
            "--s3-bucket-source",
            r#"{"S3Bucket":"export-bucket","S3KeyPrefix":"exports/source/"}"#,
            "--table-creation-parameters",
            r#"{"TableName":"ImportDest","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}]}"#,
        ])
        .await;
    assert!(
        import_output.success(),
        "import failed: {}",
        import_output.stderr_text()
    );
    let import_json = import_output.stdout_json();
    let processed = import_json["ImportTableDescription"]["ProcessedItemCount"]
        .as_i64()
        .unwrap_or(0);
    assert_eq!(processed, 3, "Expected 3 items imported");

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

/// SNS -> Lambda real execution: publishing to an SNS topic with a Lambda subscriber
/// actually invokes the Lambda function via a container. The Lambda function writes
/// proof to an SQS queue to confirm it ran and received the SNS event.
#[tokio::test]
#[ignore] // Requires Docker with host.docker.internal networking — run locally, not in CI
async fn sns_to_lambda_actually_executes() {
    let server = TestServer::start().await;
    let sqs = server.sqs_client().await;
    let sns = server.sns_client().await;
    let lambda = server.lambda_client().await;

    // 1. Create an SQS queue where Lambda will write proof that it ran
    let queue = sqs
        .create_queue()
        .queue_name("lambda-proof-queue")
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap().to_string();
    let _queue_arn = get_queue_arn(&sqs, &queue_url).await;

    // 2. Create a Lambda function with Python code that sends a message to SQS
    //    containing the SNS event it received.
    // Inside Docker, host.docker.internal resolves to the host machine.
    let docker_endpoint = format!("http://host.docker.internal:{}", server.port());
    // The queue URL returned by SQS uses localhost:4566, but from inside Docker
    // we need to use host.docker.internal:{port}. Construct it directly.
    let docker_queue_url = format!(
        "http://host.docker.internal:{}/123456789012/lambda-proof-queue",
        server.port()
    );
    let python_code = format!(
        r#"
import json
import urllib.request
import urllib.parse

def handler(event, context):
    endpoint = "{docker_endpoint}"
    queue_url = "{docker_queue_url}"
    params = urllib.parse.urlencode({{
        "Action": "SendMessage",
        "QueueUrl": queue_url,
        "MessageBody": json.dumps(event),
        "Version": "2012-11-05",
    }})
    req = urllib.request.Request(
        endpoint + "/",
        data=params.encode("utf-8"),
        headers={{
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/sqs/aws4_request, SignedHeaders=host, Signature=fake",
        }},
    )
    urllib.request.urlopen(req, timeout=5)
    return {{"statusCode": 200, "body": "ok"}}
"#
    );

    let zip = make_zip(&[("index.py", python_code.as_bytes())]);

    lambda
        .create_function()
        .function_name("sns-proof-func")
        .runtime(Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/lambda-role")
        .handler("index.handler")
        .environment(
            Environment::builder()
                .variables("FAKECLOUD_ENDPOINT", server.endpoint())
                .build(),
        )
        .code(FunctionCode::builder().zip_file(Blob::new(zip)).build())
        .send()
        .await
        .unwrap();

    // 3. Create an SNS topic
    let topic = sns
        .create_topic()
        .name("lambda-trigger-topic")
        .send()
        .await
        .unwrap();
    let topic_arn = topic.topic_arn().unwrap().to_string();

    // 4. Subscribe the Lambda function to the topic
    let lambda_arn = "arn:aws:lambda:us-east-1:123456789012:function:sns-proof-func";
    sns.subscribe()
        .topic_arn(&topic_arn)
        .protocol("lambda")
        .endpoint(lambda_arn)
        .send()
        .await
        .unwrap();

    // 5. Publish a message to the topic
    sns.publish()
        .topic_arn(&topic_arn)
        .message("hello from SNS")
        .subject("Test Subject")
        .send()
        .await
        .unwrap();

    // 6. Wait for Lambda to execute and write to SQS
    let mut proof_message = None;
    for _ in 0..30 {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let msgs = sqs
            .receive_message()
            .queue_url(&queue_url)
            .max_number_of_messages(1)
            .send()
            .await
            .unwrap();
        if !msgs.messages().is_empty() {
            proof_message = Some(msgs.messages()[0].body().unwrap().to_string());
            break;
        }
    }

    // 7. Assert the queue has a message proving Lambda ran and received the SNS event
    let proof = proof_message
        .expect("Lambda did not write proof to SQS — SNS->Lambda execution did not happen");
    let event: serde_json::Value = serde_json::from_str(&proof).unwrap();

    // The Lambda receives an SNS event with Records array
    assert!(
        event["Records"].is_array(),
        "Expected SNS event with Records array, got: {event}"
    );
    let record = &event["Records"][0];
    assert_eq!(record["EventSource"], "aws:sns");
    assert_eq!(record["Sns"]["Message"], "hello from SNS");
    assert_eq!(record["Sns"]["Subject"], "Test Subject");
}
