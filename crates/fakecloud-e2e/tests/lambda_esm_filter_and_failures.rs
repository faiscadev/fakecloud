//! Lambda event source mapping FilterCriteria + batchItemFailures
//! semantics. These tests exercise the SQS poller end-to-end without
//! requiring a real Docker-backed Lambda — the poller records routed
//! invocations into Lambda state, and FilterCriteria-dropped messages
//! still get acked off the queue.

mod helpers;

use aws_sdk_lambda::types::{Filter, FilterCriteria as LambdaFilterCriteria};
use aws_sdk_sqs::types::QueueAttributeName;
use helpers::TestServer;
use std::time::Duration;

const MINIMAL_HANDLER: &str = "index.handler";

async fn create_function(lambda: &aws_sdk_lambda::Client, name: &str) -> String {
    let zip = aws_sdk_lambda::primitives::Blob::new(minimal_zip());
    lambda
        .create_function()
        .function_name(name)
        .runtime(aws_sdk_lambda::types::Runtime::Provided)
        .role("arn:aws:iam::000000000000:role/lambda-test-role")
        .handler(MINIMAL_HANDLER)
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(zip)
                .build(),
        )
        .send()
        .await
        .unwrap()
        .function_arn()
        .unwrap()
        .to_string()
}

fn minimal_zip() -> Vec<u8> {
    use std::io::Write;
    let mut buf = Vec::new();
    {
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
        let opts: zip::write::SimpleFileOptions = zip::write::SimpleFileOptions::default();
        zip.start_file("index.sh", opts).unwrap();
        zip.write_all(b"#!/bin/sh\necho hi\n").unwrap();
        zip.finish().unwrap();
    }
    buf
}

#[tokio::test]
async fn sqs_filter_criteria_drops_non_matching_messages() {
    let server = TestServer::start().await;
    let lambda = server.lambda_client().await;
    let sqs = server.sqs_client().await;
    let http = reqwest::Client::new();

    let function_arn = create_function(&lambda, "filter-fn").await;
    let queue = sqs
        .create_queue()
        .queue_name("filter-q")
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap().to_string();
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

    // FilterCriteria: only deliver records whose body decodes to a
    // JSON object with `action == "process"`.
    let filter = LambdaFilterCriteria::builder()
        .filters(
            Filter::builder()
                .pattern(r#"{"body": {"action": ["process"]}}"#)
                .build(),
        )
        .build();
    lambda
        .create_event_source_mapping()
        .function_name(&function_arn)
        .event_source_arn(&queue_arn)
        .batch_size(10)
        .filter_criteria(filter)
        .send()
        .await
        .unwrap();

    // Two messages: one matches, one doesn't.
    sqs.send_message()
        .queue_url(&queue_url)
        .message_body(r#"{"action":"process","id":1}"#)
        .send()
        .await
        .unwrap();
    sqs.send_message()
        .queue_url(&queue_url)
        .message_body(r#"{"action":"skip","id":2}"#)
        .send()
        .await
        .unwrap();

    // Give the poller a few cycles to evaluate FilterCriteria.
    // Recorded invocations are appended every time the poller hands a
    // batch to Lambda — even if the actual Lambda invocation fails for
    // env-specific reasons (no Docker, missing handler, etc.), the
    // recorded payload reflects the filter outcome.
    let mut filter_arr: Vec<serde_json::Value> = Vec::new();
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let resp: serde_json::Value = http
            .get(format!(
                "{}/_fakecloud/lambda/invocations",
                server.endpoint()
            ))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let arr = resp["invocations"].as_array().cloned().unwrap_or_default();
        filter_arr = arr
            .into_iter()
            .filter(|inv| {
                inv["functionArn"]
                    .as_str()
                    .or_else(|| inv["function_arn"].as_str())
                    .map(|a| a == function_arn)
                    .unwrap_or(false)
            })
            .collect();
        if !filter_arr.is_empty() {
            break;
        }
    }
    assert!(
        !filter_arr.is_empty(),
        "expected the matching message to be routed to filter-fn at least once"
    );
    let saw_skipped = filter_arr.iter().any(|inv| {
        let payload = inv["payload"].as_str().unwrap_or("");
        payload.contains("\"action\":\"skip\"") || payload.contains("\"action\": \"skip\"")
    });
    assert!(
        !saw_skipped,
        "non-matching message must not reach the function via the SQS poller"
    );

    // The non-matching message must be acked off the queue regardless
    // of whether the Lambda invoke succeeds. Allow the matching
    // message to remain in flight — without a real Lambda runtime the
    // failed invoke causes a retry, which is correct AWS behavior.
    let mut remaining_bodies: Vec<String> = Vec::new();
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let recv = sqs
            .receive_message()
            .queue_url(&queue_url)
            .max_number_of_messages(10)
            .visibility_timeout(0)
            .send()
            .await
            .unwrap();
        remaining_bodies = recv
            .messages()
            .iter()
            .filter_map(|m| m.body().map(String::from))
            .collect();
        if remaining_bodies
            .iter()
            .any(|b| b.contains("\"action\":\"skip\""))
        {
            break;
        }
    }
    assert!(
        !remaining_bodies
            .iter()
            .any(|b| b.contains("\"action\":\"skip\"")),
        "non-matching message should have been acked off the queue, but it's still there: {remaining_bodies:?}"
    );

    let _ = QueueAttributeName::ApproximateNumberOfMessages;
}

/// FilterCriteria use the EventBridge pattern language: a scalar pattern
/// matches any element of an array value, and an object pattern matches an
/// array of objects when any element matches.
#[tokio::test]
async fn sqs_filter_criteria_matches_array_values() {
    let server = TestServer::start().await;
    let lambda = server.lambda_client().await;
    let sqs = server.sqs_client().await;
    let http = reqwest::Client::new();

    let function_arn = create_function(&lambda, "filter-array-fn").await;
    let queue_url = sqs
        .create_queue()
        .queue_name("filter-array-q")
        .send()
        .await
        .unwrap()
        .queue_url()
        .unwrap()
        .to_string();
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

    // A scalar leaf is not a valid pattern.
    let bad = lambda
        .create_event_source_mapping()
        .function_name(&function_arn)
        .event_source_arn(&queue_arn)
        .filter_criteria(
            LambdaFilterCriteria::builder()
                .filters(Filter::builder().pattern(r#"{"body": "x"}"#).build())
                .build(),
        )
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        bad.into_service_error().meta().code(),
        Some("InvalidParameterValueException")
    );

    let filter = LambdaFilterCriteria::builder()
        .filters(
            Filter::builder()
                .pattern(r#"{"body": {"tags": ["urgent"], "lines": {"sku": ["gold"]}}}"#)
                .build(),
        )
        .build();
    lambda
        .create_event_source_mapping()
        .function_name(&function_arn)
        .event_source_arn(&queue_arn)
        .batch_size(10)
        .filter_criteria(filter)
        .send()
        .await
        .unwrap();

    for body in [
        r#"{"id":"match","tags":["low","urgent"],"lines":[{"sku":"tin"},{"sku":"gold"}]}"#,
        r#"{"id":"no-tag","tags":["low"],"lines":[{"sku":"gold"}]}"#,
        r#"{"id":"no-sku","tags":["urgent"],"lines":[{"sku":"tin"}]}"#,
    ] {
        sqs.send_message()
            .queue_url(&queue_url)
            .message_body(body)
            .send()
            .await
            .unwrap();
    }

    let mut payloads: Vec<String> = Vec::new();
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let resp: serde_json::Value = http
            .get(format!(
                "{}/_fakecloud/lambda/invocations",
                server.endpoint()
            ))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        payloads = resp["invocations"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|inv| {
                inv["functionArn"]
                    .as_str()
                    .or_else(|| inv["function_arn"].as_str())
                    == Some(function_arn.as_str())
            })
            .filter_map(|inv| inv["payload"].as_str().map(String::from))
            .collect();
        if !payloads.is_empty() {
            break;
        }
    }
    assert!(
        payloads.iter().any(|p| p.contains("match")),
        "the matching message must be delivered: {payloads:?}"
    );
    assert!(
        !payloads
            .iter()
            .any(|p| p.contains("no-tag") || p.contains("no-sku")),
        "non-matching messages must be filtered out: {payloads:?}"
    );
}
