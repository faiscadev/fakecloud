//! EventBridge Pipes execution engine (batch 2): an SQS source pipe polls its
//! source queue, applies the EventBridge-pattern filter, and delivers matching
//! events to the target. Uses an SQS target so the whole path is observable
//! through the SDK without a container runtime.

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
async fn pipe_sqs_source_to_sqs_target_delivers() {
    let s = TestServer::start().await;
    let cfg = s.aws_config().await;
    let sqs = aws_sdk_sqs::Client::new(&cfg);
    let pipes = aws_sdk_pipes::Client::new(&cfg);

    let (src_url, src_arn) = make_queue(&sqs, "pipe-src-deliver").await;
    let (tgt_url, tgt_arn) = make_queue(&sqs, "pipe-tgt-deliver").await;

    pipes
        .create_pipe()
        .name("deliver-pipe")
        .source(&src_arn)
        .target(&tgt_arn)
        .role_arn(ROLE)
        .send()
        .await
        .expect("create pipe");
    wait_running(&pipes, "deliver-pipe").await;

    sqs.send_message()
        .queue_url(&src_url)
        .message_body("hello-pipes")
        .send()
        .await
        .unwrap();

    // The runner forwards the SQS-source event JSON to the target queue.
    let body = recv_one(&sqs, &tgt_url, 8)
        .await
        .expect("event delivered to target queue");
    let event: serde_json::Value = serde_json::from_str(&body).expect("event is JSON");
    assert_eq!(event["body"], "hello-pipes");
    assert_eq!(event["eventSource"], "aws:sqs");
    assert_eq!(event["eventSourceArn"], src_arn);

    // Source queue is drained (the message was acked after delivery).
    let leftover = sqs
        .receive_message()
        .queue_url(&src_url)
        .visibility_timeout(0)
        .send()
        .await
        .unwrap();
    assert!(leftover.messages().is_empty(), "source not drained");
}

#[tokio::test]
async fn pipe_filter_drops_non_matching_events() {
    let s = TestServer::start().await;
    let cfg = s.aws_config().await;
    let sqs = aws_sdk_sqs::Client::new(&cfg);
    let pipes = aws_sdk_pipes::Client::new(&cfg);

    let (src_url, src_arn) = make_queue(&sqs, "pipe-src-filter").await;
    let (tgt_url, tgt_arn) = make_queue(&sqs, "pipe-tgt-filter").await;

    // Only events whose body equals "keep" pass the filter.
    let filter = aws_sdk_pipes::types::FilterCriteria::builder()
        .filters(
            aws_sdk_pipes::types::Filter::builder()
                .pattern(r#"{"body":["keep"]}"#)
                .build(),
        )
        .build();
    let source_params = aws_sdk_pipes::types::PipeSourceParameters::builder()
        .filter_criteria(filter)
        .build();

    pipes
        .create_pipe()
        .name("filter-pipe")
        .source(&src_arn)
        .target(&tgt_arn)
        .role_arn(ROLE)
        .source_parameters(source_params)
        .send()
        .await
        .expect("create pipe");
    wait_running(&pipes, "filter-pipe").await;

    sqs.send_message()
        .queue_url(&src_url)
        .message_body("drop")
        .send()
        .await
        .unwrap();
    sqs.send_message()
        .queue_url(&src_url)
        .message_body("keep")
        .send()
        .await
        .unwrap();

    // The matching event arrives at the target.
    let body = recv_one(&sqs, &tgt_url, 8)
        .await
        .expect("matching event delivered");
    let event: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(event["body"], "keep");

    // The non-matching event was dropped (acked), not delivered. Give the
    // runner time to also process the dropped message, then confirm the target
    // never receives a second message and the source is fully drained.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let more = sqs
        .receive_message()
        .queue_url(&tgt_url)
        .visibility_timeout(0)
        .send()
        .await
        .unwrap();
    assert!(
        more.messages().is_empty(),
        "filtered event should not have been delivered"
    );
    let src_left = sqs
        .receive_message()
        .queue_url(&src_url)
        .visibility_timeout(0)
        .send()
        .await
        .unwrap();
    assert!(
        src_left.messages().is_empty(),
        "both messages should be acked from source (drop-as-ack)"
    );
}
