use std::sync::Arc;
use std::time::Duration;

use aws_sdk_firehose::types::{
    DeliveryStreamType, ElasticsearchBufferingHints, ElasticsearchDestinationConfiguration,
    ElasticsearchRetryOptions, KinesisStreamSourceConfiguration, Record,
    S3DestinationConfiguration,
};
use aws_sdk_kinesis::primitives::Blob;
use fakecloud_testkit::TestServer;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

const ROLE: &str = "arn:aws:iam::123456789012:role/firehose";

/// What a real cluster cannot be made to do on demand: overload, internal
/// errors and hanging. Delivery to real clusters is tested in
/// `firehose_search_containers.rs`.
#[derive(Clone, Copy)]
enum Answer {
    /// Index everything.
    Indexed,
    /// Answer the request as a whole with this status.
    Status(u16, &'static str),
    /// Fail every item with this status and error.
    Items(u16, &'static str),
    /// Never answer.
    Hang,
}

/// Minimal `_bulk` endpoint recording each request (path, body). Each answer is
/// what the current state of `answer` says.
async fn bulk_endpoint(
    answer: Arc<std::sync::Mutex<Answer>>,
) -> (String, Arc<Mutex<Vec<(String, String)>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let captured = seen.clone();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let captured = captured.clone();
            let answer = answer.clone();
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
                let documents = body.lines().count() / 2;
                captured.lock().await.push((path, body));
                let answer = *answer.lock().unwrap();
                let (status, reply) = match answer {
                    Answer::Hang => {
                        std::future::pending::<()>().await;
                        unreachable!()
                    }
                    Answer::Status(status, body) => (status, body.to_owned()),
                    Answer::Indexed => (
                        200,
                        json!({"took": 1, "errors": false, "items":
                            vec![json!({"index": {"status": 201}}); documents]})
                        .to_string(),
                    ),
                    Answer::Items(status, error) => {
                        let error: Value = serde_json::from_str(error).unwrap();
                        (
                            200,
                            json!({"took": 1, "errors": true, "items":
                                vec![json!({"index": {"status": status, "error": error}}); documents]})
                            .to_string(),
                        )
                    }
                };
                let response = format!(
                    "HTTP/1.1 {status} Answer\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{reply}",
                    reply.len()
                );
                sock.write_all(response.as_bytes()).await.unwrap();
            });
        }
    });
    (url, seen)
}

/// A Direct PUT stream to `endpoint` with one record put, retrying for
/// `retry_seconds`, its failures under `backup/`.
async fn direct_put_stream(server: &TestServer, endpoint: &str, retry_seconds: i32) {
    create_direct_stream(server, endpoint, retry_seconds).await;
    put_direct_record(server).await;
}

async fn create_direct_stream(server: &TestServer, endpoint: &str, retry_seconds: i32) {
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
        .delivery_stream_name("direct")
        .elasticsearch_destination_configuration(
            ElasticsearchDestinationConfiguration::builder()
                .role_arn(ROLE)
                .index_name("activity")
                .cluster_endpoint(endpoint)
                .buffering_hints(
                    ElasticsearchBufferingHints::builder()
                        .size_in_mbs(1)
                        .interval_in_seconds(0)
                        .build(),
                )
                .retry_options(
                    ElasticsearchRetryOptions::builder()
                        .duration_in_seconds(retry_seconds)
                        .build(),
                )
                .s3_configuration(
                    S3DestinationConfiguration::builder()
                        .role_arn(ROLE)
                        .bucket_arn("arn:aws:s3:::firehose-bucket")
                        .prefix("backup/")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
}

async fn put_direct_record(server: &TestServer) {
    server
        .firehose_client()
        .await
        .put_record()
        .delivery_stream_name("direct")
        .record(
            Record::builder()
                .data(aws_sdk_firehose::primitives::Blob::new("{\"n\":1}"))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
}

async fn failure_line(server: &TestServer, within: Duration) -> Value {
    let s3 = server.s3_client().await;
    let key = tokio::time::timeout(within, async {
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
    .expect("failure record written");
    assert!(key.starts_with("backup/elasticsearch-failed/"), "{key}");
    let body = s3
        .get_object()
        .bucket("firehose-bucket")
        .key(&key)
        .send()
        .await
        .unwrap()
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes();
    serde_json::from_slice(&body).unwrap()
}

/// The status of an item that overloads keeps the form recorded for 403, and the
/// item is retried for the duration. No 429 has been recorded from real Firehose.
#[tokio::test]
async fn overloaded_items_retry_and_keep_their_status() {
    let server = TestServer::start().await;
    let answer = Arc::new(std::sync::Mutex::new(Answer::Items(
        429,
        r#"{"type":"es_rejected_execution_exception","reason":"queue full"}"#,
    )));
    let (endpoint, seen) = bulk_endpoint(answer).await;
    direct_put_stream(&server, &endpoint, 4).await;
    let line = failure_line(&server, Duration::from_secs(30)).await;
    assert_eq!(line["errorCode"], "429");
    assert_eq!(line["attemptsMade"], 2, "at 0 and at the 4 s deadline");
    assert_eq!(seen.lock().await.len(), 2);
    assert!(line["errorMessage"]
        .as_str()
        .unwrap()
        .contains("es_rejected_execution_exception"));
}

/// Retried requests stop failing as soon as the cluster recovers.
#[tokio::test]
async fn a_recovered_cluster_gets_the_retried_documents() {
    let server = TestServer::start().await;
    let answer = Arc::new(std::sync::Mutex::new(Answer::Status(503, "")));
    let (endpoint, seen) = bulk_endpoint(answer.clone()).await;
    direct_put_stream(&server, &endpoint, 60).await;
    tokio::time::timeout(Duration::from_secs(30), async {
        while seen.lock().await.is_empty() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("first attempt");
    *answer.lock().unwrap() = Answer::Indexed;
    tokio::time::timeout(Duration::from_secs(30), async {
        while seen.lock().await.len() < 2 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("retried");
    tokio::time::sleep(Duration::from_secs(1)).await;
    let listed = server
        .s3_client()
        .await
        .list_objects_v2()
        .bucket("firehose-bucket")
        .send()
        .await
        .unwrap();
    assert!(listed.contents().is_empty(), "nothing failed");
}

/// A request that fails as a whole with a server error, once retries are spent.
#[tokio::test]
async fn server_errors_fail_with_a_cluster_error_code() {
    let server = TestServer::start().await;
    let answer = Arc::new(std::sync::Mutex::new(Answer::Status(500, "boom")));
    let (endpoint, _) = bulk_endpoint(answer).await;
    let sent = chrono::Utc::now();
    direct_put_stream(&server, &endpoint, 0).await;
    let line = failure_line(&server, Duration::from_secs(15)).await;
    assert_eq!(line["errorCode"], "ES.ClusterError");
    assert_eq!(line["attemptsMade"], 1);
    // The default rotation is OneDay. Either day, if the test straddles midnight.
    let day = |at: chrono::DateTime<chrono::Utc>| format!("activity-{}", at.format("%Y-%m-%d"));
    let index = line["esIndexName"].as_str().unwrap().to_owned();
    assert!(
        [day(sent), day(chrono::Utc::now())].contains(&index),
        "{index}"
    );
}

/// A cluster that accepts a request and never answers times out at 60 s.
#[tokio::test]
async fn a_hanging_cluster_fails_with_a_timeout_code() {
    let server = TestServer::start().await;
    let answer = Arc::new(std::sync::Mutex::new(Answer::Hang));
    let (endpoint, _) = bulk_endpoint(answer).await;
    direct_put_stream(&server, &endpoint, 0).await;
    let line = failure_line(&server, Duration::from_secs(90)).await;
    assert_eq!(line["errorCode"], "ES.RequestTimeout");
}

/// The cluster went away after the stream was created, which requires it.
#[tokio::test]
async fn a_cluster_gone_since_creation_fails_with_a_connection_code() {
    let server = TestServer::start().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    create_direct_stream(&server, &endpoint, 0).await;
    drop(listener);
    put_direct_record(&server).await;
    let line = failure_line(&server, Duration::from_secs(15)).await;
    assert_eq!(line["errorCode"], "ES.ConnectionReset");
    assert_eq!(line["attemptsMade"], 1);
}

/// A cluster whose access policy refuses the role, as a whole request.
#[tokio::test]
async fn a_refused_role_fails_with_access_denied() {
    let server = TestServer::start().await;
    let answer = Arc::new(std::sync::Mutex::new(Answer::Status(403, "{}")));
    let (endpoint, _) = bulk_endpoint(answer).await;
    direct_put_stream(&server, &endpoint, 0).await;
    let line = failure_line(&server, Duration::from_secs(15)).await;
    assert_eq!(line["errorCode"], "ES.AccessDenied");
    assert_eq!(line["attemptsMade"], 1);
    assert!(line["esDocumentId"].is_string(), "{line}");
    assert_eq!(line["esTypeName"], Value::Null);
}

/// A `ClusterEndpoint` is checked when it is named.
#[tokio::test]
async fn create_connects_to_the_cluster_endpoint() {
    let server = TestServer::start().await;
    server
        .s3_client()
        .await
        .create_bucket()
        .bucket("firehose-bucket")
        .send()
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gone = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    for (endpoint, message) in [
        (
            gone.as_str(),
            "Error connecting to the provided ElasticSearch cluster endpoint.  Ensure that the \
             ElasticSearch cluster endpoint is valid.",
        ),
        (
            "https://no-such-host.invalid",
            "Invalid Elasticsearch Cluster Endpoint https://no-such-host.invalid. If this \
             Elasticsearch domain is in VPC, please specify VpcConfiguration.",
        ),
    ] {
        let error = server
            .firehose_client()
            .await
            .create_delivery_stream()
            .delivery_stream_name("refused")
            .elasticsearch_destination_configuration(
                ElasticsearchDestinationConfiguration::builder()
                    .role_arn(ROLE)
                    .index_name("activity")
                    .cluster_endpoint(endpoint)
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
            .unwrap_err();
        let service = error.as_service_error().unwrap();
        assert!(service.is_invalid_argument_exception(), "{service:?}");
        assert_eq!(service.meta().message(), Some(message));
    }
}

/// Records read while the cluster is down are not lost by a restart: the
/// Kinesis checkpoint stays behind them until delivery commits, so the
/// restarted server reads them again and indexes them under the same IDs.
#[tokio::test]
async fn kinesis_source_redelivers_after_restart_while_cluster_down() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let answer = Arc::new(std::sync::Mutex::new(Answer::Status(503, "")));
    let (endpoint, seen) = bulk_endpoint(answer.clone()).await;

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
    *answer.lock().unwrap() = Answer::Indexed;
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
