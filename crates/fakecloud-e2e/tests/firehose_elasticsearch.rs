use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_firehose::types::{
    DeliveryStreamType, ElasticsearchBufferingHints, ElasticsearchDestinationConfiguration,
    KinesisStreamSourceConfiguration, S3DestinationConfiguration,
};
use aws_sdk_kinesis::primitives::Blob;
use aws_sdk_kinesis::types::PutRecordsRequestEntry;
use base64::Engine;
use fakecloud_testkit::TestServer;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

const ROLE: &str = "arn:aws:iam::123456789012:role/firehose";

/// Minimal `_bulk` endpoint: records each request (path, body) and rejects
/// any document containing `"bad"` with a 400 item, like a mapping error.
/// While `down` is set it answers every request with 503.
async fn bulk_endpoint(down: Arc<AtomicBool>) -> (String, Arc<Mutex<Vec<(String, String)>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let captured = seen.clone();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let captured = captured.clone();
            let down = down.clone();
            tokio::spawn(async move {
                let mut acc = Vec::new();
                let mut chunk = [0u8; 4096];
                let (head_end, length) = loop {
                    let n = sock.read(&mut chunk).await.unwrap();
                    assert!(n > 0, "connection closed before headers");
                    acc.extend_from_slice(&chunk[..n]);
                    if let Some(pos) = acc.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&acc[..pos]).to_lowercase();
                        let length = head
                            .lines()
                            .find_map(|l| l.strip_prefix("content-length:"))
                            .map(|v| v.trim().parse::<usize>().unwrap())
                            .unwrap_or(0);
                        break (pos + 4, length);
                    }
                };
                while acc.len() < head_end + length {
                    let n = sock.read(&mut chunk).await.unwrap();
                    acc.extend_from_slice(&chunk[..n]);
                }
                let head = String::from_utf8_lossy(&acc[..head_end]).to_string();
                let path = head.split_whitespace().nth(1).unwrap().to_owned();
                let body = String::from_utf8(acc[head_end..].to_vec()).unwrap();
                let items: Vec<Value> = body
                    .lines()
                    .skip(1)
                    .step_by(2)
                    .map(|doc| {
                        let status = if doc.contains("\"bad\"") { 400 } else { 201 };
                        json!({"index": {"status": status, "error": {"type": "mapper_parsing_exception"}}})
                    })
                    .collect();
                let errors = items.iter().any(|i| i["index"]["status"] == 400);
                captured.lock().await.push((path, body));
                let (status, reply) = if down.load(Ordering::SeqCst) {
                    ("503 Service Unavailable", String::new())
                } else {
                    (
                        "200 OK",
                        json!({"took": 1, "errors": errors, "items": items}).to_string(),
                    )
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{reply}",
                    reply.len()
                );
                sock.write_all(response.as_bytes()).await.unwrap();
            });
        }
    });
    (url, seen)
}

#[tokio::test]
async fn kinesis_source_indexes_documents_and_backs_up_failures() {
    let server = TestServer::start().await;
    let kinesis = server.kinesis_client().await;
    let firehose = server.firehose_client().await;
    let s3 = server.s3_client().await;
    let (endpoint, seen) = bulk_endpoint(Arc::default()).await;

    kinesis
        .create_stream()
        .stream_name("activity-dev")
        .shard_count(1)
        .send()
        .await
        .unwrap();
    let stream_arn = kinesis
        .describe_stream_summary()
        .stream_name("activity-dev")
        .send()
        .await
        .unwrap()
        .stream_description_summary()
        .unwrap()
        .stream_arn()
        .to_owned();
    s3.create_bucket()
        .bucket("firehose-bucket")
        .send()
        .await
        .unwrap();

    firehose
        .create_delivery_stream()
        .delivery_stream_name("activity-dev-elasticsearch")
        .delivery_stream_type(DeliveryStreamType::KinesisStreamAsSource)
        .kinesis_stream_source_configuration(
            KinesisStreamSourceConfiguration::builder()
                .kinesis_stream_arn(&stream_arn)
                .role_arn(ROLE)
                .build()
                .unwrap(),
        )
        .elasticsearch_destination_configuration(
            ElasticsearchDestinationConfiguration::builder()
                .role_arn(ROLE)
                .index_name("activity")
                .cluster_endpoint(&endpoint)
                .buffering_hints(
                    ElasticsearchBufferingHints::builder()
                        .size_in_mbs(1)
                        .interval_in_seconds(0)
                        .build(),
                )
                .s3_configuration(
                    S3DestinationConfiguration::builder()
                        .role_arn(ROLE)
                        .bucket_arn("arn:aws:s3:::firehose-bucket")
                        .prefix("backup/")
                        .error_output_prefix("errors/")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let entry = |data: &str| {
        PutRecordsRequestEntry::builder()
            .partition_key("pk")
            .data(Blob::new(data))
            .build()
            .unwrap()
    };
    let sent = chrono::Utc::now();
    kinesis
        .put_records()
        .stream_name("activity-dev")
        .records(entry("{\"n\":1}\n"))
        .records(entry("{\"bad\":true}"))
        .send()
        .await
        .unwrap();

    let failed = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let listed = s3
                .list_objects_v2()
                .bucket("firehose-bucket")
                .send()
                .await
                .unwrap();
            if let Some(key) = listed.contents().first().and_then(|o| o.key()) {
                return key.to_owned();
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("failed document backed up within 15s");

    let requests = seen.lock().await.clone();
    assert_eq!(requests.len(), 1, "{requests:?}");
    let (path, body) = &requests[0];
    assert_eq!(path, "/_bulk");
    let lines: Vec<Value> = body
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines.len(), 4);
    let index = lines[0]["index"]["_index"].as_str().unwrap().to_owned();
    let day = |at: chrono::DateTime<chrono::Utc>| format!("activity-{}", at.format("%Y-%m-%d"));
    assert!(
        [day(sent), day(chrono::Utc::now())].contains(&index),
        "{index}"
    );
    assert!(lines[0]["index"]["_id"].is_string());
    assert_eq!(lines[1], json!({"n": 1}));
    assert_eq!(lines[3], json!({"bad": true}));

    assert!(failed.starts_with("errors/"), "{failed}");
    let object = s3
        .get_object()
        .bucket("firehose-bucket")
        .key(&failed)
        .send()
        .await
        .unwrap()
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes();
    let record: Value = serde_json::from_slice(object.trim_ascii_end()).unwrap();
    assert_eq!(record["errorCode"], "400");
    assert_eq!(record["esIndexName"], index.as_str());
    assert_eq!(record["esDocumentId"], lines[2]["index"]["_id"]);
    let raw = base64::engine::general_purpose::STANDARD
        .decode(record["rawData"].as_str().unwrap())
        .unwrap();
    assert_eq!(raw, b"{\"bad\":true}");
}

/// Records read while the cluster is down are not lost by a restart: the
/// Kinesis checkpoint stays behind them until delivery commits, so the
/// restarted server reads them again and indexes them under the same IDs.
#[tokio::test]
async fn kinesis_source_redelivers_after_restart_while_cluster_down() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let down = Arc::new(AtomicBool::new(true));
    let (endpoint, seen) = bulk_endpoint(down.clone()).await;

    let kinesis = server.kinesis_client().await;
    kinesis
        .create_stream()
        .stream_name("activity-dev")
        .shard_count(1)
        .send()
        .await
        .unwrap();
    let stream_arn = kinesis
        .describe_stream_summary()
        .stream_name("activity-dev")
        .send()
        .await
        .unwrap()
        .stream_description_summary()
        .unwrap()
        .stream_arn()
        .to_owned();
    server
        .s3_client()
        .await
        .create_bucket()
        .bucket("firehose-bucket")
        .send()
        .await
        .unwrap();
    server
        .firehose_client()
        .await
        .create_delivery_stream()
        .delivery_stream_name("activity-dev-elasticsearch")
        .delivery_stream_type(DeliveryStreamType::KinesisStreamAsSource)
        .kinesis_stream_source_configuration(
            KinesisStreamSourceConfiguration::builder()
                .kinesis_stream_arn(&stream_arn)
                .role_arn(ROLE)
                .build()
                .unwrap(),
        )
        .elasticsearch_destination_configuration(
            ElasticsearchDestinationConfiguration::builder()
                .role_arn(ROLE)
                .index_name("activity")
                .cluster_endpoint(&endpoint)
                .buffering_hints(
                    ElasticsearchBufferingHints::builder()
                        .size_in_mbs(1)
                        .interval_in_seconds(0)
                        .build(),
                )
                .s3_configuration(
                    S3DestinationConfiguration::builder()
                        .role_arn(ROLE)
                        .bucket_arn("arn:aws:s3:::firehose-bucket")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    for n in 0..3 {
        kinesis
            .put_record()
            .stream_name("activity-dev")
            .partition_key("pk")
            .data(Blob::new(format!("{{\"n\":{n}}}")))
            .send()
            .await
            .unwrap();
    }

    let documents = |body: &str| -> Vec<(Value, Value)> {
        let lines: Vec<Value> = body
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        lines
            .chunks(2)
            .map(|pair| (pair[0]["index"]["_id"].clone(), pair[1].clone()))
            .collect()
    };
    let attempted = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let ids: Vec<_> = seen
                .lock()
                .await
                .iter()
                .flat_map(|(_, body)| documents(body))
                .collect();
            if ids.len() >= 3 {
                return ids;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("records read and attempted while the cluster is down");

    server.restart().await;
    down.store(false, Ordering::SeqCst);
    seen.lock().await.clear();

    let mut delivered = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let docs: Vec<_> = seen
                .lock()
                .await
                .iter()
                .flat_map(|(_, body)| documents(body))
                .collect();
            if docs.len() >= 3 {
                return docs;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("records re-read and delivered after restart");
    delivered.sort_by_key(|(_, doc)| doc["n"].as_i64());
    assert_eq!(
        delivered
            .iter()
            .map(|(_, doc)| doc.clone())
            .collect::<Vec<_>>(),
        vec![json!({"n": 0}), json!({"n": 1}), json!({"n": 2})]
    );
    for (id, doc) in &delivered {
        assert!(
            attempted.iter().any(|(old, d)| old == id && d == doc),
            "{doc} redelivered under a new ID {id}"
        );
    }
}
