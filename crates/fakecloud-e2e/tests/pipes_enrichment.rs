//! EventBridge Pipes enrichment (batch 4): an SQS-source pipe runs each matched
//! batch through a Lambda *enrichment* whose JSON return replaces the batch,
//! then delivers the enriched events to the target. Uses a real Python Lambda
//! (container runtime) for the enrichment round-trip and an SQS target so the
//! result is observable through the SDK.

mod helpers;

use std::io::Write;

use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::{FunctionCode, Runtime};
use helpers::TestServer;

const ROLE: &str = "arn:aws:iam::000000000000:role/pipe-role";

fn make_zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let cursor = std::io::Cursor::new(Vec::new());
    let mut writer = zip::ZipWriter::new(cursor);
    for (name, content) in entries {
        let options = zip::write::SimpleFileOptions::default().unix_permissions(0o755);
        writer.start_file(*name, options).unwrap();
        writer.write_all(content).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

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

// The enrichment receives the matched batch as a JSON array and returns a new
// array; Pipes forwards exactly what it returns to the target.
const ENRICH_HANDLER: &str = r#"
def handler(event, context):
    # event is the array of source events
    return [{"enriched": True, "count": len(event), "first_body": event[0]["body"]}]
"#;

#[tokio::test]
async fn pipe_lambda_enrichment_rewrites_batch() {
    let s = TestServer::start().await;
    let cfg = s.aws_config().await;
    let sqs = aws_sdk_sqs::Client::new(&cfg);
    let lambda = aws_sdk_lambda::Client::new(&cfg);
    let pipes = aws_sdk_pipes::Client::new(&cfg);

    let (src_url, src_arn) = make_queue(&sqs, "pipe-src-enrich").await;
    let (tgt_url, tgt_arn) = make_queue(&sqs, "pipe-tgt-enrich").await;

    let zip = make_zip(&[("index.py", ENRICH_HANDLER.as_bytes())]);
    lambda
        .create_function()
        .function_name("pipe-enricher")
        .runtime(Runtime::Python313)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(FunctionCode::builder().zip_file(Blob::new(zip)).build())
        .send()
        .await
        .expect("create enrichment function");
    let enrich_arn = lambda
        .get_function()
        .function_name("pipe-enricher")
        .send()
        .await
        .unwrap()
        .configuration()
        .unwrap()
        .function_arn()
        .unwrap()
        .to_string();

    pipes
        .create_pipe()
        .name("enrich-pipe")
        .source(&src_arn)
        .target(&tgt_arn)
        .enrichment(&enrich_arn)
        .role_arn(ROLE)
        .send()
        .await
        .expect("create pipe");
    wait_running(&pipes, "enrich-pipe").await;

    sqs.send_message()
        .queue_url(&src_url)
        .message_body("raw-event")
        .send()
        .await
        .unwrap();

    let body = recv_one(&sqs, &tgt_url, 30)
        .await
        .expect("enriched event delivered to target queue");
    let event: serde_json::Value = serde_json::from_str(&body).expect("event is JSON");
    assert_eq!(event["enriched"], true);
    assert_eq!(event["count"], 1);
    assert_eq!(event["first_body"], "raw-event");
}
