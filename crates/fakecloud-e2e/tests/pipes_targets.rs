//! EventBridge Pipes additional targets (batch 3): an SQS-source pipe delivers
//! matching events to Step Functions and Kinesis targets, observed through the
//! respective SDKs.

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

#[tokio::test]
async fn pipe_sqs_source_to_kinesis_target() {
    let s = TestServer::start().await;
    let cfg = s.aws_config().await;
    let sqs = aws_sdk_sqs::Client::new(&cfg);
    let kinesis = aws_sdk_kinesis::Client::new(&cfg);
    let pipes = aws_sdk_pipes::Client::new(&cfg);

    let (src_url, src_arn) = make_queue(&sqs, "pipe-src-kinesis").await;

    kinesis
        .create_stream()
        .stream_name("pipe-target-stream")
        .shard_count(1)
        .send()
        .await
        .expect("create stream");
    let stream_arn = kinesis
        .describe_stream_summary()
        .stream_name("pipe-target-stream")
        .send()
        .await
        .unwrap()
        .stream_description_summary()
        .unwrap()
        .stream_arn()
        .to_string();

    pipes
        .create_pipe()
        .name("kinesis-pipe")
        .source(&src_arn)
        .target(&stream_arn)
        .role_arn(ROLE)
        .send()
        .await
        .expect("create pipe");
    wait_running(&pipes, "kinesis-pipe").await;

    sqs.send_message()
        .queue_url(&src_url)
        .message_body("to-kinesis")
        .send()
        .await
        .unwrap();

    // Read the stream until the forwarded event record shows up.
    let shard_id = kinesis
        .describe_stream()
        .stream_name("pipe-target-stream")
        .send()
        .await
        .unwrap()
        .stream_description()
        .unwrap()
        .shards()[0]
        .shard_id()
        .to_string();
    let mut iter = kinesis
        .get_shard_iterator()
        .stream_name("pipe-target-stream")
        .shard_id(&shard_id)
        .shard_iterator_type(aws_sdk_kinesis::types::ShardIteratorType::TrimHorizon)
        .send()
        .await
        .unwrap()
        .shard_iterator()
        .unwrap()
        .to_string();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);
    let mut found = None;
    while std::time::Instant::now() < deadline {
        let resp = kinesis
            .get_records()
            .shard_iterator(&iter)
            .send()
            .await
            .unwrap();
        for rec in resp.records() {
            let data = rec.data().as_ref();
            let event: serde_json::Value = serde_json::from_slice(data).unwrap();
            if event["body"] == "to-kinesis" {
                found = Some(event);
                break;
            }
        }
        if found.is_some() {
            break;
        }
        if let Some(next) = resp.next_shard_iterator() {
            iter = next.to_string();
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    let event = found.expect("event delivered to Kinesis target");
    assert_eq!(event["eventSource"], "aws:sqs");
}

#[tokio::test]
async fn pipe_sqs_source_to_stepfunctions_target() {
    let s = TestServer::start().await;
    let cfg = s.aws_config().await;
    let sqs = aws_sdk_sqs::Client::new(&cfg);
    let sfn = aws_sdk_sfn::Client::new(&cfg);
    let pipes = aws_sdk_pipes::Client::new(&cfg);

    let (src_url, src_arn) = make_queue(&sqs, "pipe-src-sfn").await;

    let definition = r#"{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}"#;
    let sm_arn = sfn
        .create_state_machine()
        .name("pipe-target-sm")
        .definition(definition)
        .role_arn("arn:aws:iam::000000000000:role/sfn-role")
        .send()
        .await
        .expect("create state machine")
        .state_machine_arn()
        .to_string();

    pipes
        .create_pipe()
        .name("sfn-pipe")
        .source(&src_arn)
        .target(&sm_arn)
        .role_arn(ROLE)
        .send()
        .await
        .expect("create pipe");
    wait_running(&pipes, "sfn-pipe").await;

    sqs.send_message()
        .queue_url(&src_url)
        .message_body("to-sfn")
        .send()
        .await
        .unwrap();

    // The pipe starts a Step Functions execution per event.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);
    let mut started = false;
    while std::time::Instant::now() < deadline {
        let execs = sfn
            .list_executions()
            .state_machine_arn(&sm_arn)
            .send()
            .await
            .unwrap();
        if !execs.executions().is_empty() {
            started = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    assert!(started, "pipe did not start a Step Functions execution");
}
