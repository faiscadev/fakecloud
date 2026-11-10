//! Lambda async invocations route their result to the configured
//! OnSuccess/OnFailure destination (SQS / SNS / EventBridge / Lambda).
//! Test gates on Docker since real Lambda execution is required.

mod helpers;

use std::time::Duration;

use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::{
    DestinationConfig as LambdaDestinationConfig, FunctionCode, InvocationType,
    OnFailure as LambdaOnFailure, OnSuccess as LambdaOnSuccess, Runtime,
};
use aws_sdk_sqs::types::QueueAttributeName;
use helpers::TestServer;

fn docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn build_python_handler_zip(body: &str) -> Vec<u8> {
    use std::io::Write;
    let mut buf = Vec::new();
    {
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
        let opts: zip::write::SimpleFileOptions =
            zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Stored);
        zip.start_file("index.py", opts).unwrap();
        zip.write_all(body.as_bytes()).unwrap();
        zip.finish().unwrap();
    }
    buf
}

#[tokio::test]
async fn lambda_async_invoke_routes_on_success_to_sqs() {
    if !docker_available() {
        eprintln!("docker required for Lambda execution; skipping");
        return;
    }
    let server = TestServer::start().await;
    let lambda = server.lambda_client().await;
    let iam = server.iam_client().await;
    let sqs = server.sqs_client().await;

    let q = sqs
        .create_queue()
        .queue_name("destinations-success")
        .send()
        .await
        .unwrap();
    let queue_url = q.queue_url().unwrap().to_string();
    let queue_arn = sqs
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap()
        .attributes()
        .unwrap()
        .get(&QueueAttributeName::QueueArn)
        .unwrap()
        .to_string();

    iam.create_role()
        .role_name("dest-success-role")
        .assume_role_policy_document(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#,
        )
        .send()
        .await
        .unwrap();
    let zip_bytes = build_python_handler_zip(
        "def handler(event, context):\n    return {'ok': True, 'echo': event}\n",
    );
    lambda
        .create_function()
        .function_name("dest-success-fn")
        .runtime(Runtime::Python311)
        .role("arn:aws:iam::123456789012:role/dest-success-role")
        .handler("index.handler")
        .code(FunctionCode::builder().zip_file(zip_bytes.into()).build())
        .send()
        .await
        .unwrap();

    lambda
        .put_function_event_invoke_config()
        .function_name("dest-success-fn")
        .destination_config(
            LambdaDestinationConfig::builder()
                .on_success(LambdaOnSuccess::builder().destination(&queue_arn).build())
                .build(),
        )
        .send()
        .await
        .unwrap();

    lambda
        .invoke()
        .function_name("dest-success-fn")
        .invocation_type(InvocationType::Event)
        .payload(Blob::new(br#"{"hello":"world"}"#.as_slice()))
        .send()
        .await
        .unwrap();

    // SQS receive — destination record should arrive shortly.
    let mut saw = false;
    for _ in 0..30 {
        let recv = sqs
            .receive_message()
            .queue_url(&queue_url)
            .max_number_of_messages(10)
            .wait_time_seconds(1)
            .send()
            .await
            .unwrap();
        for m in recv.messages() {
            let body = m.body().unwrap_or("");
            let v: serde_json::Value = match serde_json::from_str(body) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if v["requestContext"]["condition"].as_str() == Some("Success")
                && v["responsePayload"]["ok"].as_bool() == Some(true)
            {
                saw = true;
            }
        }
        if saw {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        saw,
        "expected OnSuccess destination record on SQS for async invoke"
    );
}

#[tokio::test]
async fn lambda_async_invoke_routes_on_failure_to_sns() {
    if !docker_available() {
        eprintln!("docker required for Lambda execution; skipping");
        return;
    }
    let server = TestServer::start().await;
    let lambda = server.lambda_client().await;
    let iam = server.iam_client().await;
    let sqs = server.sqs_client().await;
    let sns = server.sns_client().await;

    // SNS topic + SQS subscription so we can read what landed.
    let topic = sns
        .create_topic()
        .name("dest-failure-topic")
        .send()
        .await
        .unwrap();
    let topic_arn = topic.topic_arn().unwrap().to_string();
    let q = sqs
        .create_queue()
        .queue_name("dest-failure-q")
        .send()
        .await
        .unwrap();
    let queue_url = q.queue_url().unwrap().to_string();
    let queue_arn = sqs
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap()
        .attributes()
        .unwrap()
        .get(&QueueAttributeName::QueueArn)
        .unwrap()
        .to_string();
    sns.subscribe()
        .topic_arn(&topic_arn)
        .protocol("sqs")
        .endpoint(&queue_arn)
        .send()
        .await
        .unwrap();

    iam.create_role()
        .role_name("dest-failure-role")
        .assume_role_policy_document(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#,
        )
        .send()
        .await
        .unwrap();
    let zip_bytes =
        build_python_handler_zip("def handler(event, context):\n    raise Exception('boom')\n");
    lambda
        .create_function()
        .function_name("dest-failure-fn")
        .runtime(Runtime::Python311)
        .role("arn:aws:iam::123456789012:role/dest-failure-role")
        .handler("index.handler")
        .code(FunctionCode::builder().zip_file(zip_bytes.into()).build())
        .send()
        .await
        .unwrap();
    // No retries, so the failure record arrives without Lambda's one- and
    // two-minute retry waits.
    lambda
        .put_function_event_invoke_config()
        .function_name("dest-failure-fn")
        .maximum_retry_attempts(0)
        .destination_config(
            LambdaDestinationConfig::builder()
                .on_failure(LambdaOnFailure::builder().destination(&topic_arn).build())
                .build(),
        )
        .send()
        .await
        .unwrap();
    lambda
        .invoke()
        .function_name("dest-failure-fn")
        .invocation_type(InvocationType::Event)
        .payload(Blob::new(br#"{}"#.as_slice()))
        .send()
        .await
        .unwrap();

    let mut saw = false;
    for _ in 0..30 {
        let recv = sqs
            .receive_message()
            .queue_url(&queue_url)
            .max_number_of_messages(10)
            .wait_time_seconds(1)
            .send()
            .await
            .unwrap();
        for m in recv.messages() {
            let body = m.body().unwrap_or("");
            let env: serde_json::Value = match serde_json::from_str(body) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let inner_str = env.get("Message").and_then(|v| v.as_str()).unwrap_or("");
            let inner: serde_json::Value =
                serde_json::from_str(inner_str).unwrap_or(serde_json::Value::Null);
            if inner["requestContext"]["condition"].as_str() == Some("RetriesExhausted") {
                assert_eq!(inner["requestContext"]["approximateInvokeCount"], 1);
                assert_eq!(inner["responseContext"]["statusCode"], 200);
                assert_eq!(inner["responseContext"]["functionError"], "Unhandled");
                assert_eq!(inner["responsePayload"]["errorType"], "Exception");
                assert_eq!(inner["responsePayload"]["errorMessage"], "boom");
                saw = true;
            }
        }
        if saw {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        saw,
        "expected OnFailure destination record on SNS for async invoke that errored"
    );
}

async fn queue_arn(sqs: &aws_sdk_sqs::Client, queue_url: &str) -> String {
    sqs.get_queue_attributes()
        .queue_url(queue_url)
        .attribute_names(QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap()
        .attributes()
        .unwrap()
        .get(&QueueAttributeName::QueueArn)
        .unwrap()
        .to_string()
}

/// Receive up to `attempts` times until a message arrives.
async fn receive_one(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    attempts: usize,
) -> Option<aws_sdk_sqs::types::Message> {
    for _ in 0..attempts {
        let recv = sqs
            .receive_message()
            .queue_url(queue_url)
            .max_number_of_messages(1)
            .message_attribute_names("All")
            .wait_time_seconds(1)
            .send()
            .await
            .unwrap();
        if let Some(m) = recv.messages().first() {
            return Some(m.clone());
        }
    }
    None
}

/// A failed async invocation is retried `MaximumRetryAttempts` times and the
/// discarded event lands in the function's dead-letter queue as-is, with
/// Lambda's `RequestID` / `ErrorCode` / `ErrorMessage` attributes.
#[tokio::test]
async fn lambda_async_failure_goes_to_the_dead_letter_queue() {
    if !docker_available() {
        eprintln!("docker required for Lambda execution; skipping");
        return;
    }
    let server = TestServer::start().await;
    let lambda = server.lambda_client().await;
    let sqs = server.sqs_client().await;
    let dlq_url = sqs
        .create_queue()
        .queue_name("fn-dlq")
        .send()
        .await
        .unwrap()
        .queue_url()
        .unwrap()
        .to_string();
    let dlq_arn = queue_arn(&sqs, &dlq_url).await;
    let zip_bytes = build_python_handler_zip(
        "def handler(event, context):\n    raise ValueError('bad order')\n",
    );
    lambda
        .create_function()
        .function_name("dlq-fn")
        .runtime(Runtime::Python311)
        .role("arn:aws:iam::123456789012:role/dlq-role")
        .handler("index.handler")
        .dead_letter_config(
            aws_sdk_lambda::types::DeadLetterConfig::builder()
                .target_arn(&dlq_arn)
                .build(),
        )
        .code(FunctionCode::builder().zip_file(zip_bytes.into()).build())
        .send()
        .await
        .unwrap();
    lambda
        .put_function_event_invoke_config()
        .function_name("dlq-fn")
        .maximum_retry_attempts(0)
        .send()
        .await
        .unwrap();
    lambda
        .invoke()
        .function_name("dlq-fn")
        .invocation_type(InvocationType::Event)
        .payload(Blob::new(br#"{"order":42}"#.as_slice()))
        .send()
        .await
        .unwrap();

    let msg = receive_one(&sqs, &dlq_url, 60)
        .await
        .expect("discarded event in the DLQ");
    let body: serde_json::Value = serde_json::from_str(msg.body().unwrap()).unwrap();
    assert_eq!(body, serde_json::json!({"order": 42}), "the event as-is");
    let attrs = msg.message_attributes().unwrap();
    assert_eq!(attrs["ErrorCode"].data_type(), "Number");
    assert_eq!(attrs["ErrorCode"].string_value(), Some("200"));
    assert_eq!(attrs["ErrorMessage"].string_value(), Some("bad order"));
    assert!(!attrs["RequestID"].string_value().unwrap_or("").is_empty());
}

/// An event source's asynchronous invocation (here SNS) gets the same
/// Lambda async error handling: the failed event's record reaches the
/// function's OnFailure destination.
#[tokio::test]
async fn sns_triggered_function_error_reaches_on_failure_destination() {
    if !docker_available() {
        eprintln!("docker required for Lambda execution; skipping");
        return;
    }
    let server = TestServer::start().await;
    let lambda = server.lambda_client().await;
    let sqs = server.sqs_client().await;
    let sns = server.sns_client().await;
    let fail_url = sqs
        .create_queue()
        .queue_name("sns-fn-failures")
        .send()
        .await
        .unwrap()
        .queue_url()
        .unwrap()
        .to_string();
    let fail_arn = queue_arn(&sqs, &fail_url).await;
    let zip_bytes = build_python_handler_zip(
        "def handler(event, context):\n    raise RuntimeError('cannot process')\n",
    );
    let function_arn = lambda
        .create_function()
        .function_name("sns-target-fn")
        .runtime(Runtime::Python311)
        .role("arn:aws:iam::123456789012:role/sns-target-role")
        .handler("index.handler")
        .code(FunctionCode::builder().zip_file(zip_bytes.into()).build())
        .send()
        .await
        .unwrap()
        .function_arn()
        .unwrap()
        .to_string();
    lambda
        .put_function_event_invoke_config()
        .function_name("sns-target-fn")
        .maximum_retry_attempts(0)
        .destination_config(
            LambdaDestinationConfig::builder()
                .on_failure(LambdaOnFailure::builder().destination(&fail_arn).build())
                .build(),
        )
        .send()
        .await
        .unwrap();
    let topic_arn = sns
        .create_topic()
        .name("fn-topic")
        .send()
        .await
        .unwrap()
        .topic_arn()
        .unwrap()
        .to_string();
    sns.subscribe()
        .topic_arn(&topic_arn)
        .protocol("lambda")
        .endpoint(&function_arn)
        .send()
        .await
        .unwrap();
    sns.publish()
        .topic_arn(&topic_arn)
        .message("hello")
        .send()
        .await
        .unwrap();

    let msg = receive_one(&sqs, &fail_url, 60)
        .await
        .expect("OnFailure record for the SNS-triggered invocation");
    let record: serde_json::Value = serde_json::from_str(msg.body().unwrap()).unwrap();
    assert_eq!(record["requestContext"]["condition"], "RetriesExhausted");
    assert_eq!(record["responseContext"]["functionError"], "Unhandled");
    assert_eq!(record["responsePayload"]["errorType"], "RuntimeError");
    assert_eq!(
        record["requestPayload"]["Records"][0]["Sns"]["Message"],
        "hello"
    );
}

/// An SQS event source mapping treats a function error as a failed batch:
/// the message returns to the queue after its visibility timeout and, once
/// it reaches the queue's `maxReceiveCount`, SQS redrives it to the DLQ.
#[tokio::test]
async fn sqs_mapping_function_error_redrives_to_the_queue_dlq() {
    if !docker_available() {
        eprintln!("docker required for Lambda execution; skipping");
        return;
    }
    let server = TestServer::start().await;
    let lambda = server.lambda_client().await;
    let sqs = server.sqs_client().await;
    let dlq_url = sqs
        .create_queue()
        .queue_name("esm-src-dlq")
        .send()
        .await
        .unwrap()
        .queue_url()
        .unwrap()
        .to_string();
    let dlq_arn = queue_arn(&sqs, &dlq_url).await;
    let src_url = sqs
        .create_queue()
        .queue_name("esm-src")
        .attributes(QueueAttributeName::VisibilityTimeout, "1")
        .attributes(
            QueueAttributeName::RedrivePolicy,
            serde_json::json!({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": 2}).to_string(),
        )
        .send()
        .await
        .unwrap()
        .queue_url()
        .unwrap()
        .to_string();
    let src_arn = queue_arn(&sqs, &src_url).await;
    let zip_bytes =
        build_python_handler_zip("def handler(event, context):\n    raise ValueError('poison')\n");
    lambda
        .create_function()
        .function_name("esm-poison-fn")
        .runtime(Runtime::Python311)
        .role("arn:aws:iam::123456789012:role/esm-role")
        .handler("index.handler")
        .code(FunctionCode::builder().zip_file(zip_bytes.into()).build())
        .send()
        .await
        .unwrap();
    lambda
        .create_event_source_mapping()
        .event_source_arn(&src_arn)
        .function_name("esm-poison-fn")
        .batch_size(1)
        .enabled(true)
        .send()
        .await
        .unwrap();
    sqs.send_message()
        .queue_url(&src_url)
        .message_body("poison-pill")
        .send()
        .await
        .unwrap();

    let msg = receive_one(&sqs, &dlq_url, 90)
        .await
        .expect("the failing message is redriven to the DLQ");
    assert_eq!(msg.body(), Some("poison-pill"));
}
