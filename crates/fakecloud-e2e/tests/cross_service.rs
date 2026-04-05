mod helpers;

use aws_sdk_eventbridge::types::{PutEventsRequestEntry, Target};
use aws_sdk_s3::primitives::ByteStream;
use helpers::TestServer;

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
