mod helpers;

use std::io::Write;

use aws_sdk_eventbridge::types::{PutEventsRequestEntry, Target};
use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::{Environment, FunctionCode, Runtime};
use helpers::TestServer;

fn dockerized_endpoint(server: &TestServer) -> String {
    format!("http://host.docker.internal:{}", server.port())
}

fn dockerized_queue_url(server: &TestServer, queue_name: &str) -> String {
    format!(
        "http://host.docker.internal:{}/123456789012/{}",
        server.port(), queue_name
    )
}

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

/// Helper to get SQS queue ARN from queue URL.
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

/// Build Python Lambda code that sends a marker message to an SQS queue.
///
/// The Lambda receives the fakecloud endpoint and result queue URL via
/// environment variables, then uses urllib to call the SQS SendMessage API
/// directly (no boto3 needed).
fn python_sqs_writer_code() -> &'static str {
    r#"
import json
import os
import urllib.request
import urllib.parse

def lambda_handler(event, context):
    endpoint = os.environ["FAKECLOUD_ENDPOINT"]
    queue_url = os.environ["RESULT_QUEUE_URL"]

    # Use the SQS Query API directly via HTTP POST
    params = urllib.parse.urlencode({
        "Action": "SendMessage",
        "QueueUrl": queue_url,
        "MessageBody": json.dumps({
            "marker": "lambda-executed",
            "source_event": event,
        }),
    }).encode()

    req = urllib.request.Request(endpoint, data=params, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    # SQS needs the Authorization header for routing
    req.add_header("Authorization", (
        "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20200101/us-east-1/sqs/aws4_request, "
        "SignedHeaders=host, Signature=fake"
    ))
    urllib.request.urlopen(req)

    return {"statusCode": 200, "body": "marker sent"}
"#
}


/// Helper: create the result SQS queue and a Lambda function that writes to it.
/// Returns (result_queue_url, lambda_function_name).
async fn create_marker_lambda(
    server: &TestServer,
    sqs: &aws_sdk_sqs::Client,
    lambda: &aws_sdk_lambda::Client,
    queue_name: &str,
    function_name: &str,
) -> String {
    // Create result queue where the Lambda will write its marker
    let queue = sqs
        .create_queue()
        .queue_name(queue_name)
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap().to_string();
    let docker_endpoint = dockerized_endpoint(server);
    let docker_queue_url = dockerized_queue_url(server, queue_name);

    // Create Lambda with Python code that writes to the result queue
    let zip = make_zip(&[("lambda_function.py", python_sqs_writer_code().as_bytes())]);

    lambda
        .create_function()
        .function_name(function_name)
        .runtime(Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/lambda-role")
        .handler("lambda_function.lambda_handler")
        .environment(
            Environment::builder()
                .variables("FAKECLOUD_ENDPOINT", &docker_endpoint)
                .variables("RESULT_QUEUE_URL", &docker_queue_url)
                .build(),
        )
        .code(FunctionCode::builder().zip_file(Blob::new(zip)).build())
        .send()
        .await
        .unwrap();

    queue_url
}

/// Check the result queue for the marker message written by the Lambda.
/// Returns true if the Lambda actually executed and wrote its marker.
async fn check_marker(sqs: &aws_sdk_sqs::Client, queue_url: &str) -> bool {
    for _ in 0..10 {
        let msgs = sqs
            .receive_message()
            .queue_url(queue_url)
            .max_number_of_messages(10)
            .wait_time_seconds(2)
            .send()
            .await
            .unwrap();

        for msg in msgs.messages() {
            if let Some(body) = msg.body() {
                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(body) {
                    if parsed["marker"] == "lambda-executed" {
                        return true;
                    }
                }
            }
        }

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    false
}

#[tokio::test]
async fn sns_to_lambda_executes_code() {
    let server = TestServer::start().await;
    let sns = server.sns_client().await;
    let sqs = server.sqs_client().await;
    let lambda = server.lambda_client().await;

    let result_queue_url =
        create_marker_lambda(&server, &sqs, &lambda, "sns-lambda-result", "sns-handler").await;

    // Create SNS topic
    let topic = sns
        .create_topic()
        .name("lambda-trigger-topic")
        .send()
        .await
        .unwrap();
    let topic_arn = topic.topic_arn().unwrap().to_string();

    // Subscribe the Lambda function to the topic
    let lambda_arn = "arn:aws:lambda:us-east-1:123456789012:function:sns-handler";
    sns.subscribe()
        .topic_arn(&topic_arn)
        .protocol("lambda")
        .endpoint(lambda_arn)
        .send()
        .await
        .unwrap();

    // Publish a message to trigger the Lambda
    sns.publish()
        .topic_arn(&topic_arn)
        .message(r#"{"order_id": "sns-test-123"}"#)
        .send()
        .await
        .unwrap();

    // Verify the Lambda ACTUALLY EXECUTED by checking for its marker in SQS
    assert!(
        check_marker(&sqs, &result_queue_url).await,
        "Lambda did not actually execute: no marker found in result queue. \
         SNS->Lambda cross-service invocation does not call the Docker runtime."
    );
}

#[tokio::test]
async fn eventbridge_to_lambda_executes_code() {
    let server = TestServer::start().await;
    let eb = server.eventbridge_client().await;
    let sqs = server.sqs_client().await;
    let lambda = server.lambda_client().await;

    let result_queue_url =
        create_marker_lambda(&server, &sqs, &lambda, "eb-lambda-result", "eb-handler").await;

    // Create EventBridge rule
    eb.put_rule()
        .name("lambda-exec-rule")
        .event_pattern(r#"{"source": ["integration-test"]}"#)
        .send()
        .await
        .unwrap();

    // Target the Lambda function
    let lambda_arn = "arn:aws:lambda:us-east-1:123456789012:function:eb-handler";
    eb.put_targets()
        .rule("lambda-exec-rule")
        .targets(
            Target::builder()
                .id("lambda-target")
                .arn(lambda_arn)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Send an event that matches the rule
    eb.put_events()
        .entries(
            PutEventsRequestEntry::builder()
                .source("integration-test")
                .detail_type("TestEvent")
                .detail(r#"{"test_id": "eb-exec-123"}"#)
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Verify the Lambda ACTUALLY EXECUTED
    assert!(
        check_marker(&sqs, &result_queue_url).await,
        "Lambda did not actually execute: no marker found in result queue. \
         EventBridge->Lambda cross-service invocation does not call the Docker runtime."
    );
}

#[tokio::test]
async fn sqs_to_lambda_event_source_mapping_executes_code() {
    let server = TestServer::start().await;
    let sqs = server.sqs_client().await;
    let lambda = server.lambda_client().await;

    let result_queue_url =
        create_marker_lambda(&server, &sqs, &lambda, "esm-lambda-result", "esm-handler").await;

    // Create source queue (the one that triggers the Lambda)
    let source_queue = sqs
        .create_queue()
        .queue_name("esm-source-queue")
        .send()
        .await
        .unwrap();
    let source_queue_url = source_queue.queue_url().unwrap().to_string();
    let source_queue_arn = get_queue_arn(&sqs, &source_queue_url).await;

    // Create event source mapping: source queue -> Lambda
    lambda
        .create_event_source_mapping()
        .event_source_arn(&source_queue_arn)
        .function_name("esm-handler")
        .batch_size(1)
        .enabled(true)
        .send()
        .await
        .unwrap();

    // Send a message to the source queue to trigger the Lambda
    sqs.send_message()
        .queue_url(&source_queue_url)
        .message_body(r#"{"order_id": "esm-test-456"}"#)
        .send()
        .await
        .unwrap();

    // Verify the Lambda ACTUALLY EXECUTED by checking the result queue
    assert!(
        check_marker(&sqs, &result_queue_url).await,
        "Lambda did not actually execute: no marker found in result queue. \
         SQS event source mapping does not invoke the Docker runtime."
    );
}

#[tokio::test]
async fn s3_to_lambda_notification_executes_code() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    let sqs = server.sqs_client().await;
    let lambda = server.lambda_client().await;

    let result_queue_url =
        create_marker_lambda(&server, &sqs, &lambda, "s3-lambda-result", "s3-handler").await;

    // Create S3 bucket
    s3.create_bucket()
        .bucket("lambda-notif-bucket")
        .send()
        .await
        .unwrap();

    // Configure bucket notification with LambdaFunctionConfiguration
    let lambda_arn = "arn:aws:lambda:us-east-1:123456789012:function:s3-handler";
    let notif_config = format!(
        r#"{{"LambdaFunctionConfigurations":[{{"LambdaFunctionArn":"{}","Events":["s3:ObjectCreated:*"]}}]}}"#,
        lambda_arn
    );
    let output = server
        .aws_cli(&[
            "s3api",
            "put-bucket-notification-configuration",
            "--bucket",
            "lambda-notif-bucket",
            "--notification-configuration",
            &notif_config,
        ])
        .await;
    // This may fail at parse time if LambdaFunctionConfiguration isn't supported
    assert!(
        output.success(),
        "put-bucket-notification-configuration with Lambda target failed: {}",
        output.stderr_text()
    );

    // Upload an object to trigger the Lambda
    s3.put_object()
        .bucket("lambda-notif-bucket")
        .key("trigger.txt")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(
            b"trigger content",
        ))
        .send()
        .await
        .unwrap();

    // Verify the Lambda ACTUALLY EXECUTED
    assert!(
        check_marker(&sqs, &result_queue_url).await,
        "Lambda did not actually execute: no marker found in result queue. \
         S3->Lambda notification does not invoke the Docker runtime."
    );
}

#[tokio::test]
#[ignore] // Requires Docker with host.docker.internal networking
async fn dynamodb_streams_to_lambda_executes_code() {
    let server = TestServer::start().await;
    let dynamodb = server.dynamodb_client().await;
    let sqs = server.sqs_client().await;
    let lambda = server.lambda_client().await;

    let result_queue_url = create_marker_lambda(
        &server,
        &sqs,
        &lambda,
        "streams-lambda-result",
        "streams-handler",
    )
    .await;

    // Create DynamoDB table with streams enabled
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
        StreamSpecification, StreamViewType,
    };

    dynamodb
        .create_table()
        .table_name("StreamsTable")
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
        .stream_specification(
            StreamSpecification::builder()
                .stream_enabled(true)
                .stream_view_type(StreamViewType::NewAndOldImages)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Get the stream ARN
    let table_desc = dynamodb
        .describe_table()
        .table_name("StreamsTable")
        .send()
        .await
        .unwrap();
    let stream_arn = table_desc
        .table()
        .unwrap()
        .latest_stream_arn()
        .unwrap()
        .to_string();

    // Create event source mapping: DynamoDB stream -> Lambda
    lambda
        .create_event_source_mapping()
        .event_source_arn(&stream_arn)
        .function_name("streams-handler")
        .batch_size(10)
        .enabled(true)
        .send()
        .await
        .unwrap();

    // Put an item to trigger the stream
    use aws_sdk_dynamodb::types::AttributeValue;
    dynamodb
        .put_item()
        .table_name("StreamsTable")
        .item("pk", AttributeValue::S("test-key".to_string()))
        .item("data", AttributeValue::S("test-value".to_string()))
        .send()
        .await
        .unwrap();

    // Verify the Lambda ACTUALLY EXECUTED by checking the result queue
    assert!(
        check_marker(&sqs, &result_queue_url).await,
        "Lambda did not actually execute: no marker found in result queue. \
         DynamoDB Streams->Lambda integration does not invoke the Docker runtime."
    );
}

// EXPECTED TO FAIL: Lambda cross-service execution not yet wired up
//
// SecretsManager RotateSecret with a rotation Lambda ARN should invoke the
// Lambda with rotation steps (createSecret, setSecret, testSecret,
// finishSecret). The Lambda should actually execute, create a new secret
// version, and promote it from AWSPENDING to AWSCURRENT. Currently
// RotateSecret does not invoke the Lambda runtime.
#[tokio::test]
#[ignore] // Requires Docker with host.docker.internal networking
async fn secretsmanager_rotation_lambda_executes() {
    let server = TestServer::start().await;
    let sm = server.secretsmanager_client().await;
    let sqs = server.sqs_client().await;
    let lambda = server.lambda_client().await;

    // Create result queue for the marker
    let result_queue = sqs
        .create_queue()
        .queue_name("rotation-result")
        .send()
        .await
        .unwrap();
    let result_queue_url = result_queue.queue_url().unwrap().to_string();

    // Create a rotation Lambda that:
    // 1. Handles the rotation steps (createSecret puts the new value)
    // 2. Writes a marker to SQS to prove it ran
    let rotation_code = r#"
import json
import os
import urllib.request
import urllib.parse

def lambda_handler(event, context):
    endpoint = os.environ["FAKECLOUD_ENDPOINT"]
    queue_url = os.environ["RESULT_QUEUE_URL"]
    step = event.get("Step", "unknown")
    secret_id = event.get("SecretId", "unknown")
    token = event.get("ClientRequestToken", "unknown")

    # For the createSecret step, put a new pending secret value
    if step == "createSecret":
        # Call SecretsManager PutSecretValue with AWSPENDING
        body = json.dumps({
            "SecretId": secret_id,
            "ClientRequestToken": token,
            "SecretString": "rotated-secret-value",
            "VersionStages": ["AWSPENDING"],
        }).encode()
        req = urllib.request.Request(
            endpoint,
            data=body,
            method="POST",
        )
        req.add_header("Content-Type", "application/x-amz-json-1.1")
        req.add_header("X-Amz-Target", "secretsmanager.PutSecretValue")
        req.add_header("Authorization", (
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20200101/us-east-1/secretsmanager/aws4_request, "
            "SignedHeaders=host, Signature=fake"
        ))
        urllib.request.urlopen(req)

    # For finishSecret, promote AWSPENDING to AWSCURRENT
    if step == "finishSecret":
        body = json.dumps({
            "SecretId": secret_id,
            "VersionStage": "AWSCURRENT",
            "MoveToVersionId": token,
        }).encode()
        req = urllib.request.Request(
            endpoint,
            data=body,
            method="POST",
        )
        req.add_header("Content-Type", "application/x-amz-json-1.1")
        req.add_header("X-Amz-Target", "secretsmanager.UpdateSecretVersionStage")
        req.add_header("Authorization", (
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20200101/us-east-1/secretsmanager/aws4_request, "
            "SignedHeaders=host, Signature=fake"
        ))
        urllib.request.urlopen(req)

    # Write marker to SQS to prove we executed
    params = urllib.parse.urlencode({
        "Action": "SendMessage",
        "QueueUrl": queue_url,
        "MessageBody": json.dumps({
            "marker": "lambda-executed",
            "step": step,
            "secret_id": secret_id,
        }),
    }).encode()
    req = urllib.request.Request(endpoint, data=params, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    req.add_header("Authorization", (
        "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20200101/us-east-1/sqs/aws4_request, "
        "SignedHeaders=host, Signature=fake"
    ))
    urllib.request.urlopen(req)

    return {"statusCode": 200}
"#;

    let zip = make_zip(&[("lambda_function.py", rotation_code.as_bytes())]);

    lambda
        .create_function()
        .function_name("rotation-handler")
        .runtime(Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/lambda-role")
        .handler("lambda_function.lambda_handler")
        .environment(
            Environment::builder()
                .variables("FAKECLOUD_ENDPOINT", server.endpoint())
                .variables("RESULT_QUEUE_URL", &result_queue_url)
                .build(),
        )
        .code(FunctionCode::builder().zip_file(Blob::new(zip)).build())
        .send()
        .await
        .unwrap();

    // Create a secret to rotate
    sm.create_secret()
        .name("rotation-test-secret")
        .secret_string("original-secret-value")
        .send()
        .await
        .unwrap();

    // Trigger rotation with the Lambda ARN
    let lambda_arn = "arn:aws:lambda:us-east-1:123456789012:function:rotation-handler";
    sm.rotate_secret()
        .secret_id("rotation-test-secret")
        .rotation_lambda_arn(lambda_arn)
        .send()
        .await
        .unwrap();

    // Verify the Lambda ACTUALLY EXECUTED
    assert!(
        check_marker(&sqs, &result_queue_url).await,
        "Rotation Lambda did not actually execute: no marker found in result queue. \
         SecretsManager RotateSecret does not invoke the Docker runtime."
    );

    // Additionally verify the secret value was actually rotated
    let secret = sm
        .get_secret_value()
        .secret_id("rotation-test-secret")
        .send()
        .await
        .unwrap();
    assert_eq!(
        secret.secret_string().unwrap(),
        "rotated-secret-value",
        "Secret value was not rotated. The rotation Lambda did not execute the \
         createSecret/finishSecret steps via the Docker runtime."
    );
}
