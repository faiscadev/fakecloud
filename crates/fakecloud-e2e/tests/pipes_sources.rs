//! EventBridge Pipes additional sources (batch 4): pipes read from Kinesis
//! streams and DynamoDB streams (not just SQS), and the target `InputTemplate`
//! transforms each event before delivery. An SQS target keeps the whole path
//! observable through the SDK without a container runtime.

mod helpers;

use helpers::TestServer;

const ROLE: &str = "arn:aws:iam::000000000000:role/pipe-role";

async fn make_queue(sqs: &aws_sdk_sqs::Client, name: &str) -> (String, String) {
    let url = sqs
        .create_queue()
        .queue_name(name)
        .send()
        .await
        .unwrap()
        .queue_url()
        .unwrap()
        .to_string();
    let arn = sqs
        .get_queue_attributes()
        .queue_url(&url)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap()
        .attributes()
        .unwrap()
        .get(&aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .unwrap()
        .to_string();
    (url, arn)
}

async fn wait_running(pipes: &aws_sdk_pipes::Client, name: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if let Ok(r) = pipes.describe_pipe().name(name).send().await {
            if r.current_state() == Some(&aws_sdk_pipes::types::PipeState::Running) {
                return;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("pipe {name} never reached RUNNING");
}

/// Poll a queue until one message arrives (up to `secs`), returning its body.
async fn recv_one(sqs: &aws_sdk_sqs::Client, url: &str, secs: u64) -> Option<String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(secs);
    while std::time::Instant::now() < deadline {
        let r = sqs
            .receive_message()
            .queue_url(url)
            .max_number_of_messages(10)
            .wait_time_seconds(0)
            .send()
            .await
            .unwrap();
        if let Some(m) = r.messages().first() {
            return m.body().map(str::to_string);
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    None
}

#[tokio::test]
async fn pipe_kinesis_source_to_sqs_target() {
    let s = TestServer::start().await;
    let cfg = s.aws_config().await;
    let sqs = aws_sdk_sqs::Client::new(&cfg);
    let kinesis = aws_sdk_kinesis::Client::new(&cfg);
    let pipes = aws_sdk_pipes::Client::new(&cfg);

    kinesis
        .create_stream()
        .stream_name("pipe-kinesis-src")
        .shard_count(1)
        .send()
        .await
        .expect("create stream");
    let stream_arn = kinesis
        .describe_stream_summary()
        .stream_name("pipe-kinesis-src")
        .send()
        .await
        .unwrap()
        .stream_description_summary()
        .unwrap()
        .stream_arn()
        .to_string();

    let (tgt_url, tgt_arn) = make_queue(&sqs, "pipe-tgt-from-kinesis").await;

    let source_params = aws_sdk_pipes::types::PipeSourceParameters::builder()
        .kinesis_stream_parameters(
            aws_sdk_pipes::types::PipeSourceKinesisStreamParameters::builder()
                .starting_position(aws_sdk_pipes::types::KinesisStreamStartPosition::TrimHorizon)
                .batch_size(10)
                .build()
                .unwrap(),
        )
        .build();

    pipes
        .create_pipe()
        .name("kinesis-src-pipe")
        .source(&stream_arn)
        .target(&tgt_arn)
        .role_arn(ROLE)
        .source_parameters(source_params)
        .send()
        .await
        .expect("create pipe");
    wait_running(&pipes, "kinesis-src-pipe").await;

    kinesis
        .put_record()
        .stream_name("pipe-kinesis-src")
        .partition_key("pk-1")
        .data(aws_sdk_kinesis::primitives::Blob::new(
            b"from-kinesis".to_vec(),
        ))
        .send()
        .await
        .expect("put record");

    let body = recv_one(&sqs, &tgt_url, 10)
        .await
        .expect("kinesis record delivered to target queue");
    let event: serde_json::Value = serde_json::from_str(&body).expect("event is JSON");
    assert_eq!(event["eventSource"], "aws:kinesis");
    assert_eq!(event["eventSourceARN"], stream_arn);
    // The record data rides through base64-encoded in the Pipes envelope.
    let data_b64 = event["kinesis"]["data"].as_str().unwrap();
    let decoded = base64_decode(data_b64);
    assert_eq!(decoded, b"from-kinesis");
}

#[tokio::test]
async fn pipe_dynamodb_stream_source_to_sqs_target() {
    let s = TestServer::start().await;
    let cfg = s.aws_config().await;
    let sqs = aws_sdk_sqs::Client::new(&cfg);
    let ddb = aws_sdk_dynamodb::Client::new(&cfg);
    let pipes = aws_sdk_pipes::Client::new(&cfg);

    use aws_sdk_dynamodb::types as dt;
    ddb.create_table()
        .table_name("pipe-ddb-src")
        .attribute_definitions(
            dt::AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(dt::ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            dt::KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(dt::KeyType::Hash)
                .build()
                .unwrap(),
        )
        .billing_mode(dt::BillingMode::PayPerRequest)
        .stream_specification(
            dt::StreamSpecification::builder()
                .stream_enabled(true)
                .stream_view_type(dt::StreamViewType::NewAndOldImages)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create table");

    let stream_arn = ddb
        .describe_table()
        .table_name("pipe-ddb-src")
        .send()
        .await
        .unwrap()
        .table()
        .unwrap()
        .latest_stream_arn()
        .expect("stream arn")
        .to_string();

    let (tgt_url, tgt_arn) = make_queue(&sqs, "pipe-tgt-from-ddb").await;

    let source_params = aws_sdk_pipes::types::PipeSourceParameters::builder()
        .dynamo_db_stream_parameters(
            aws_sdk_pipes::types::PipeSourceDynamoDbStreamParameters::builder()
                .starting_position(aws_sdk_pipes::types::DynamoDbStreamStartPosition::TrimHorizon)
                .batch_size(10)
                .build()
                .unwrap(),
        )
        .build();

    pipes
        .create_pipe()
        .name("ddb-src-pipe")
        .source(&stream_arn)
        .target(&tgt_arn)
        .role_arn(ROLE)
        .source_parameters(source_params)
        .send()
        .await
        .expect("create pipe");
    wait_running(&pipes, "ddb-src-pipe").await;

    ddb.put_item()
        .table_name("pipe-ddb-src")
        .item("pk", dt::AttributeValue::S("row-1".into()))
        .send()
        .await
        .expect("put item");

    let body = recv_one(&sqs, &tgt_url, 10)
        .await
        .expect("ddb stream record delivered to target queue");
    let event: serde_json::Value = serde_json::from_str(&body).expect("event is JSON");
    assert_eq!(event["eventSource"], "aws:dynamodb");
    assert_eq!(event["eventName"], "INSERT");
    assert_eq!(event["dynamodb"]["NewImage"]["pk"]["S"], "row-1");
}

#[tokio::test]
async fn pipe_input_template_transforms_event() {
    let s = TestServer::start().await;
    let cfg = s.aws_config().await;
    let sqs = aws_sdk_sqs::Client::new(&cfg);
    let pipes = aws_sdk_pipes::Client::new(&cfg);

    let (src_url, src_arn) = make_queue(&sqs, "pipe-src-tmpl").await;
    let (tgt_url, tgt_arn) = make_queue(&sqs, "pipe-tgt-tmpl").await;

    let target_params = aws_sdk_pipes::types::PipeTargetParameters::builder()
        .input_template(r#"{"transformed": "<$.body>", "src": "<$.eventSource>"}"#)
        .build();

    pipes
        .create_pipe()
        .name("tmpl-pipe")
        .source(&src_arn)
        .target(&tgt_arn)
        .role_arn(ROLE)
        .target_parameters(target_params)
        .send()
        .await
        .expect("create pipe");
    wait_running(&pipes, "tmpl-pipe").await;

    sqs.send_message()
        .queue_url(&src_url)
        .message_body("payload")
        .send()
        .await
        .unwrap();

    let body = recv_one(&sqs, &tgt_url, 8)
        .await
        .expect("transformed event delivered");
    let event: serde_json::Value = serde_json::from_str(&body).expect("template produced JSON");
    // <$.body> resolves to the source message body, <$.eventSource> to "aws:sqs".
    assert_eq!(event["transformed"], "payload");
    assert_eq!(event["src"], "aws:sqs");
}

fn base64_decode(s: &str) -> Vec<u8> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.decode(s).unwrap()
}
