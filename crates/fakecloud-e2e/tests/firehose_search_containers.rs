//! Firehose delivering to REAL search clusters by `ClusterEndpoint`: OpenSearch
//! 2.11, the engine the AWS recordings were made against, and Elasticsearch 7.10.
//! A missing container runtime fails these tests rather than skipping them.

use std::process::Command;
use std::time::Duration;

use aws_sdk_firehose::primitives::Blob;
use aws_sdk_firehose::types::{
    AmazonopensearchserviceBufferingHints, AmazonopensearchserviceDestinationConfiguration,
    AmazonopensearchserviceIndexRotationPeriod, AmazonopensearchserviceRetryOptions,
    AmazonopensearchserviceS3BackupMode, DeliveryStreamType, ElasticsearchBufferingHints,
    ElasticsearchDestinationConfiguration, ElasticsearchIndexRotationPeriod,
    ElasticsearchRetryOptions, ElasticsearchS3BackupMode, KinesisStreamSourceConfiguration, Record,
    S3DestinationConfiguration,
};
use base64::Engine;
use fakecloud_testkit::{detect_container_cli, TestServer};
use serde_json::{json, Value};

const ROLE: &str = "arn:aws:iam::123456789012:role/firehose";
const BUCKET: &str = "search-failures";

#[derive(Clone, Copy, PartialEq)]
enum Family {
    OpenSearch,
    Elasticsearch,
}

impl Family {
    fn folder(self) -> &'static str {
        match self {
            Family::OpenSearch => "AmazonOpenSearchService-failed",
            Family::Elasticsearch => "elasticsearch-failed",
        }
    }

    fn service_exception(self) -> &'static str {
        match self {
            Family::OpenSearch => "OS.ServiceException",
            Family::Elasticsearch => "ES.ServiceException",
        }
    }

    fn message_prefix(self) -> &'static str {
        match self {
            Family::OpenSearch => {
                "Error received from the Amazon OpenSearch Service cluster or OpenSearch \
                 Serverless collection. If the cluster or collection is behind a VPC, ensure \
                 network configuration allows connectivity. "
            }
            Family::Elasticsearch => "Error received from Elasticsearch cluster. ",
        }
    }

    fn member(self, name: &str) -> String {
        match self {
            Family::OpenSearch => format!("os{name}"),
            Family::Elasticsearch => format!("es{name}"),
        }
    }
}

/// One search container, removed when the test ends however it ends.
struct Cluster {
    cli: String,
    id: String,
    url: String,
    http: reqwest::Client,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        let _ = Command::new(&self.cli)
            .args(["rm", "-f", &self.id])
            .output();
    }
}

impl Cluster {
    async fn start(image: &str, env: &[&str]) -> Self {
        let cli = detect_container_cli();
        let info = Command::new(&cli).arg("info").output();
        assert!(
            info.is_ok_and(|output| output.status.success()),
            "{cli} is required: these tests deliver to real search containers"
        );
        let mut args = vec![
            "run".to_owned(),
            "-d".to_owned(),
            "-p".to_owned(),
            "127.0.0.1::9200".to_owned(),
            "-e".to_owned(),
            "discovery.type=single-node".to_owned(),
        ];
        for var in env {
            args.push("-e".to_owned());
            args.push((*var).to_owned());
        }
        args.push(image.to_owned());
        let run = Command::new(&cli).args(&args).output().unwrap();
        assert!(
            run.status.success(),
            "{image} did not start: {}",
            String::from_utf8_lossy(&run.stderr)
        );
        let id = String::from_utf8(run.stdout).unwrap().trim().to_owned();
        let port = Command::new(&cli)
            .args(["port", &id, "9200/tcp"])
            .output()
            .unwrap();
        let mapped = String::from_utf8(port.stdout).unwrap();
        let address = mapped.lines().next().expect("9200 is published").trim();
        let cluster = Self {
            cli,
            id,
            url: format!("http://{address}"),
            http: reqwest::Client::new(),
        };
        let health = format!("{}/_cluster/health", cluster.url);
        tokio::time::timeout(Duration::from_secs(300), async {
            loop {
                if let Ok(response) = cluster.http.get(&health).send().await {
                    if response.status().is_success() {
                        return;
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
        .await
        .expect("the cluster answers within 5 minutes");
        cluster
    }

    async fn opensearch() -> Self {
        Self::start(
            "public.ecr.aws/opensearchproject/opensearch:2.11.1",
            &[
                "DISABLE_SECURITY_PLUGIN=true",
                "DISABLE_INSTALL_DEMO_CONFIG=true",
                "OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m",
            ],
        )
        .await
    }

    async fn elasticsearch() -> Self {
        Self::start(
            "public.ecr.aws/docker/library/elasticsearch:7.10.1",
            &[
                "ES_JAVA_OPTS=-Xms512m -Xmx512m",
                "xpack.security.enabled=false",
            ],
        )
        .await
    }

    /// `(id, source)` of everything in an index pattern; empty when there is none.
    async fn documents(&self, index: &str) -> Vec<(String, String, Value)> {
        let _ = self
            .http
            .post(format!("{}/{index}/_refresh", self.url))
            .send()
            .await
            .unwrap();
        let response = self
            .http
            .get(format!("{}/{index}/_search?size=50", self.url))
            .send()
            .await
            .unwrap();
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Vec::new();
        }
        let body: Value = response.json().await.unwrap();
        body["hits"]["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|hit| {
                (
                    hit["_index"].as_str().unwrap().to_owned(),
                    hit["_id"].as_str().unwrap().to_owned(),
                    hit["_source"].clone(),
                )
            })
            .collect()
    }

    async fn block_writes(&self, index: &str) {
        let response = self
            .http
            .put(format!("{}/{index}", self.url))
            .json(&json!({"settings": {"index": {"blocks": {"write": true}}}}))
            .send()
            .await
            .unwrap();
        assert!(
            response.status().is_success(),
            "{}",
            response.text().await.unwrap()
        );
    }
}

struct Stream<'a> {
    family: Family,
    cluster: &'a Cluster,
    name: &'a str,
    index: &'a str,
    prefix: &'a str,
    retry_seconds: i32,
    rotation: &'a str,
    backup_all: bool,
    source: Option<&'a str>,
}

async fn create_stream(firehose: &aws_sdk_firehose::Client, stream: Stream<'_>) {
    let s3 = S3DestinationConfiguration::builder()
        .role_arn(ROLE)
        .bucket_arn(format!("arn:aws:s3:::{BUCKET}"))
        .prefix(stream.prefix)
        .build()
        .unwrap();
    let mut request = firehose
        .create_delivery_stream()
        .delivery_stream_name(stream.name);
    if let Some(source) = stream.source {
        request = request
            .delivery_stream_type(DeliveryStreamType::KinesisStreamAsSource)
            .kinesis_stream_source_configuration(
                KinesisStreamSourceConfiguration::builder()
                    .kinesis_stream_arn(source)
                    .role_arn(ROLE)
                    .build()
                    .unwrap(),
            );
    }
    let request = match stream.family {
        Family::OpenSearch => request.amazonopensearchservice_destination_configuration(
            AmazonopensearchserviceDestinationConfiguration::builder()
                .role_arn(ROLE)
                .index_name(stream.index)
                .cluster_endpoint(&stream.cluster.url)
                .index_rotation_period(AmazonopensearchserviceIndexRotationPeriod::from(
                    stream.rotation,
                ))
                .buffering_hints(
                    AmazonopensearchserviceBufferingHints::builder()
                        .size_in_mbs(1)
                        .interval_in_seconds(0)
                        .build(),
                )
                .retry_options(
                    AmazonopensearchserviceRetryOptions::builder()
                        .duration_in_seconds(stream.retry_seconds)
                        .build(),
                )
                .s3_backup_mode(if stream.backup_all {
                    AmazonopensearchserviceS3BackupMode::AllDocuments
                } else {
                    AmazonopensearchserviceS3BackupMode::FailedDocumentsOnly
                })
                .s3_configuration(s3)
                .build()
                .unwrap(),
        ),
        Family::Elasticsearch => request.elasticsearch_destination_configuration(
            ElasticsearchDestinationConfiguration::builder()
                .role_arn(ROLE)
                .index_name(stream.index)
                .cluster_endpoint(&stream.cluster.url)
                .index_rotation_period(ElasticsearchIndexRotationPeriod::from(stream.rotation))
                .buffering_hints(
                    ElasticsearchBufferingHints::builder()
                        .size_in_mbs(1)
                        .interval_in_seconds(0)
                        .build(),
                )
                .retry_options(
                    ElasticsearchRetryOptions::builder()
                        .duration_in_seconds(stream.retry_seconds)
                        .build(),
                )
                .s3_backup_mode(if stream.backup_all {
                    ElasticsearchS3BackupMode::AllDocuments
                } else {
                    ElasticsearchS3BackupMode::FailedDocumentsOnly
                })
                .s3_configuration(s3)
                .build()
                .unwrap(),
        ),
    };
    request.send().await.unwrap();
}

async fn put_batch(firehose: &aws_sdk_firehose::Client, stream: &str, records: &[&[u8]]) {
    let mut request = firehose.put_record_batch().delivery_stream_name(stream);
    for record in records {
        request = request.records(Record::builder().data(Blob::new(*record)).build().unwrap());
    }
    let response = request.send().await.unwrap();
    assert_eq!(response.failed_put_count(), 0);
}

/// The objects under `prefix` once `expected` of them exist, as `(key, bytes)`.
async fn objects(s3: &aws_sdk_s3::Client, prefix: &str, expected: usize) -> Vec<(String, Vec<u8>)> {
    let keys = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let listed = s3
                .list_objects_v2()
                .bucket(BUCKET)
                .prefix(prefix)
                .send()
                .await
                .unwrap();
            let keys: Vec<String> = listed
                .contents()
                .iter()
                .map(|object| object.key().unwrap().to_owned())
                .collect();
            if keys.len() >= expected {
                return keys;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{expected} objects under {prefix} within 60s"));
    let mut found = Vec::new();
    for key in keys {
        let body = s3
            .get_object()
            .bucket(BUCKET)
            .key(&key)
            .send()
            .await
            .unwrap()
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes()
            .to_vec();
        found.push((key, body));
    }
    found
}

fn lines(body: &[u8]) -> Vec<Value> {
    assert_ne!(body.last(), Some(&b'\n'), "no newline after the last line");
    std::str::from_utf8(body)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn raw_data(line: &Value) -> Vec<u8> {
    base64::engine::general_purpose::STANDARD
        .decode(line["rawData"].as_str().unwrap())
        .unwrap()
}

/// `<prefix><folder>/yyyy/MM/dd/HH/<stream>-1-yyyy-MM-dd-HH-mm-ss-<uuid>`.
fn assert_failure_key(key: &str, prefix: &str, family: Family, stream: &str) {
    let rest = key
        .strip_prefix(&format!("{prefix}{}/", family.folder()))
        .unwrap_or_else(|| panic!("{key}"));
    let parts: Vec<&str> = rest.split('/').collect();
    assert_eq!(parts.len(), 5, "{key}");
    assert_eq!(parts[0].len(), 4, "{key}");
    let name = parts[4]
        .strip_prefix(&format!("{stream}-1-"))
        .unwrap_or_else(|| panic!("{key}"));
    assert_eq!(name.len(), 19 + 1 + 36, "{key}");
}

/// A batch of everything the recordings tried: what is indexed, what the cluster
/// refuses item by item, and what never reaches it.
async fn mixed_batch(
    family: Family,
    cluster: &Cluster,
    firehose: &aws_sdk_firehose::Client,
    s3: &aws_sdk_s3::Client,
) {
    create_stream(
        firehose,
        Stream {
            family,
            cluster,
            name: "mix",
            index: "fcmix",
            prefix: "mix/",
            retry_seconds: 0,
            rotation: "NoRotation",
            backup_all: false,
            source: None,
        },
    )
    .await;
    let records: [&[u8]; 13] = [
        b"{\"n\":1,\"k\":\"good1\"}",
        b"not json",
        b"{\"n\":\"text\",\"k\":\"conflict\"}",
        b"[1,2]",
        b"   ",
        b"{\"k\":\"trailing newline\"}\n",
        b"\"scalar\"",
        b"42",
        b"null",
        b"{\"k\":\"nested\",\"o\":{\"x\":1}}",
        b"",
        "{\"k\":\"utf8 \u{e9}\"}".as_bytes(),
        b"\xff\xfe binary",
    ];
    put_batch(firehose, "mix", &records).await;

    let found = objects(s3, "mix/", 2).await;
    assert_eq!(
        found.len(),
        2,
        "refused and sent records have separate objects"
    );
    let mut refused = Vec::new();
    let mut sent = Vec::new();
    for (key, body) in &found {
        assert_failure_key(key, "mix/", family, "mix");
        let parsed = lines(body);
        if parsed[0]["attemptsMade"] == 0 {
            refused = parsed;
        } else {
            sent = parsed;
        }
    }

    assert_eq!(refused.len(), 2);
    for line in &refused {
        assert_eq!(line["subsequenceNumber"], 0);
        assert!(line.get(family.member("DocumentId")).is_none(), "{line}");
        assert_eq!(line["arrivalTimestamp"].as_i64().unwrap() % 1000, 0);
    }
    let empty = refused.iter().find(|l| l["rawData"] == "").unwrap();
    assert_eq!(empty["errorCode"], "EmptyRecordException");
    assert_eq!(
        empty["errorMessage"],
        "com.amazonaws.firehose.v2.delivery.opensearch.processor.\
         OpenSearchRecordsInMemoryRenderer$EmptyRecordException: The record did not contain any \
         JSON documents"
    );
    let binary = refused.iter().find(|l| l["rawData"] != "").unwrap();
    assert_eq!(binary["errorCode"], "InvalidEncodingException");
    assert_eq!(raw_data(binary), b"\xff\xfe binary");

    let mut failed: Vec<Vec<u8>> = sent.iter().map(raw_data).collect();
    failed.sort();
    let mut expected: Vec<Vec<u8>> = [
        &b"not json"[..],
        b"{\"n\":\"text\",\"k\":\"conflict\"}",
        b"[1,2]",
        b"   ",
        b"\"scalar\"",
        b"42",
        b"null",
    ]
    .iter()
    .map(|raw| raw.to_vec())
    .collect();
    expected.sort();
    assert_eq!(failed, expected);

    let first = sent[0]["errorMessage"].as_str().unwrap();
    assert!(first.starts_with(family.message_prefix()), "{first}");
    assert!(first.contains("mapper_parsing_exception"), "{first}");
    let prefix_of = |id: &str| id.rsplit_once('.').unwrap().0.to_owned();
    let id_prefix = prefix_of(sent[0][family.member("DocumentId")].as_str().unwrap());
    assert_eq!(id_prefix.len(), 56, "{id_prefix}");
    for line in &sent {
        assert_eq!(line["errorCode"], family.service_exception());
        assert_eq!(line["errorMessage"], first, "the first failure's message");
        assert_eq!(line["attemptsMade"], 1);
        assert_eq!(line[family.member("IndexName")], "fcmix");
        assert_eq!(line[family.member("TypeName")], Value::Null);
        let id = line[family.member("DocumentId")].as_str().unwrap();
        assert_eq!(prefix_of(id), id_prefix, "one put, one prefix");
        assert_eq!(
            id.rsplit_once('.').unwrap().1,
            line["subsequenceNumber"].to_string()
        );
    }

    let documents = cluster.documents("fcmix").await;
    let mut indexed: Vec<&str> = documents
        .iter()
        .map(|(_, _, source)| source["k"].as_str().unwrap())
        .collect();
    indexed.sort_unstable();
    assert_eq!(
        indexed,
        ["good1", "nested", "trailing newline", "utf8 \u{e9}"]
    );
    for (_, id, _) in &documents {
        assert_eq!(prefix_of(id), id_prefix, "{id}");
    }
}

/// A newline inside a record breaks the request's framing, so the whole request
/// fails and nothing of it is indexed. Firehose does not split or sanitise.
async fn embedded_newline_fails_the_whole_bulk(
    family: Family,
    cluster: &Cluster,
    firehose: &aws_sdk_firehose::Client,
    s3: &aws_sdk_s3::Client,
) {
    create_stream(
        firehose,
        Stream {
            family,
            cluster,
            name: "newline",
            index: "fcnewline",
            prefix: "newline/",
            retry_seconds: 0,
            rotation: "NoRotation",
            backup_all: false,
            source: None,
        },
    )
    .await;
    put_batch(
        firehose,
        "newline",
        &[b"{\"k\":\"a\"}", b"{\"a\":1}\n{\"a\":2}", b"{\"k\":\"b\"}"],
    )
    .await;
    let found = objects(s3, "newline/", 1).await;
    assert_eq!(found.len(), 1);
    let parsed = lines(&found[0].1);
    assert_eq!(parsed.len(), 3);
    for line in &parsed {
        assert_eq!(line["errorCode"], family.service_exception());
        assert_eq!(line["attemptsMade"], 1);
        let message = line["errorMessage"].as_str().unwrap();
        assert!(message.starts_with(family.message_prefix()), "{message}");
        assert!(
            message.contains("Malformed action/metadata line [5]")
                && message.contains("\"status\":400"),
            "{message}"
        );
    }
    assert!(cluster.documents("fcnewline").await.is_empty());
}

/// A write-blocked index fails item by item with the status as a string and the
/// cluster's error as it came. `RetryOptions` decides how often that is tried:
/// once at 0, and at 0, 4 and 12 s with a 12 s duration.
async fn blocked_index_keeps_the_status_and_retries(
    family: Family,
    cluster: &Cluster,
    firehose: &aws_sdk_firehose::Client,
    s3: &aws_sdk_s3::Client,
) {
    cluster.block_writes("fcblocked").await;
    for (name, prefix, retry_seconds, attempts) in [
        ("noretry", "noretry/", 0, 1),
        ("blocked", "blocked/", 12, 3),
    ] {
        create_stream(
            firehose,
            Stream {
                family,
                cluster,
                name,
                index: "fcblocked",
                prefix,
                retry_seconds,
                rotation: "NoRotation",
                backup_all: false,
                source: None,
            },
        )
        .await;
        put_batch(firehose, name, &[b"{\"k\":\"blocked\"}"]).await;
        let found = objects(s3, prefix, 1).await;
        let parsed = lines(&found[0].1);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0]["errorCode"], "403");
        assert_eq!(parsed[0]["attemptsMade"], attempts, "{name}");
        let message = parsed[0]["errorMessage"].as_str().unwrap();
        assert!(
            message.starts_with("{\"type\":\"cluster_block_exception\""),
            "{message}"
        );
        // Delivery failures do not touch the stream's status.
        let description = firehose
            .describe_delivery_stream()
            .delivery_stream_name(name)
            .send()
            .await
            .unwrap();
        let description = description.delivery_stream_description().unwrap();
        assert_eq!(description.delivery_stream_status().as_str(), "ACTIVE");
        assert!(description.failure_description().is_none());
    }
}

/// A Kinesis source's IDs are `<sequence number>.0`.
async fn kinesis_source_ids_are_sequence_numbers(
    family: Family,
    cluster: &Cluster,
    server: &TestServer,
    firehose: &aws_sdk_firehose::Client,
    s3: &aws_sdk_s3::Client,
) {
    let kinesis = server.kinesis_client().await;
    kinesis
        .create_stream()
        .stream_name("source")
        .shard_count(1)
        .send()
        .await
        .unwrap();
    let arn = kinesis
        .describe_stream_summary()
        .stream_name("source")
        .send()
        .await
        .unwrap()
        .stream_description_summary()
        .unwrap()
        .stream_arn()
        .to_owned();
    create_stream(
        firehose,
        Stream {
            family,
            cluster,
            name: "from-kinesis",
            index: "fckin",
            prefix: "kin/",
            retry_seconds: 0,
            rotation: "NoRotation",
            backup_all: false,
            source: Some(&arn),
        },
    )
    .await;
    let mut sequences = Vec::new();
    for data in [&b"{\"k\":\"kgood\"}"[..], b"not json", b""] {
        let put = kinesis
            .put_record()
            .stream_name("source")
            .partition_key("pk")
            .data(aws_sdk_kinesis::primitives::Blob::new(data))
            .send()
            .await
            .unwrap();
        sequences.push(put.sequence_number().to_owned());
    }
    let found = objects(s3, "kin/", 2).await;
    let every: Vec<Value> = found.iter().flat_map(|(_, body)| lines(body)).collect();
    let sent = every
        .iter()
        .find(|line| line["rawData"] == "bm90IGpzb24=")
        .unwrap_or_else(|| panic!("{every:?}"));
    assert_eq!(
        sent[family.member("DocumentId")],
        format!("{}.0", sequences[1])
    );
    assert_eq!(sent["subsequenceNumber"], 0);
    let documents = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let documents = cluster.documents("fckin").await;
            if !documents.is_empty() {
                return documents;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("the good record is indexed");
    assert_eq!(documents[0].1, format!("{}.0", sequences[0]));
}

/// `AllDocuments` keeps the raw records under the backup prefix, and a rotated
/// index carries its week.
async fn all_documents_backup_and_week_rotation(
    family: Family,
    cluster: &Cluster,
    firehose: &aws_sdk_firehose::Client,
    s3: &aws_sdk_s3::Client,
) {
    create_stream(
        firehose,
        Stream {
            family,
            cluster,
            name: "rot",
            index: "fcrotw",
            prefix: "rot/",
            retry_seconds: 0,
            rotation: "OneWeek",
            backup_all: true,
            source: None,
        },
    )
    .await;
    put_batch(firehose, "rot", &[b"{\"k\":\"rot\"}"]).await;
    let found = objects(s3, "rot/", 1).await;
    assert_eq!(found.len(), 1);
    assert!(!found[0].0.contains("-failed/"), "{}", found[0].0);
    assert_eq!(found[0].1, b"{\"k\":\"rot\"}");
    let documents = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let documents = cluster.documents("fcrotw*").await;
            if !documents.is_empty() {
                return documents;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("the record is indexed");
    let index = &documents[0].0;
    let week = index.strip_prefix("fcrotw-").unwrap();
    let (year, number) = week.split_once("-w").unwrap_or_else(|| panic!("{index}"));
    assert_eq!(year.len(), 4, "{index}");
    assert_eq!(number.len(), 2, "{index}");
}

/// Which code a 400 item gets depends on the rest of its batch, as recorded on
/// OpenSearch Service. Elasticsearch has not shown the parse exception.
async fn part_of_a_batch_indexed(
    family: Family,
    cluster: &Cluster,
    firehose: &aws_sdk_firehose::Client,
    s3: &aws_sdk_s3::Client,
) {
    create_stream(
        firehose,
        Stream {
            family,
            cluster,
            name: "one",
            index: "fcone",
            prefix: "one/",
            retry_seconds: 60,
            rotation: "NoRotation",
            backup_all: false,
            source: None,
        },
    )
    .await;
    put_batch(firehose, "one", &[b"{\"k\":\"good\"}", b"not json"]).await;
    let found = objects(s3, "one/", 1).await;
    let parsed = lines(&found[0].1);
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0]["attemptsMade"], 1, "a 400 item is not retried");
    if family == Family::OpenSearch {
        assert_eq!(
            parsed[0]["errorCode"],
            "OS.AmazonOpenSearchServiceParseException"
        );
        assert_eq!(
            parsed[0]["errorMessage"],
            "The Amazon OpenSearch Service cluster returned an \
             AmazonOpenSearchServiceParseException. Ensure that the data being put is valid."
        );
    } else {
        assert_eq!(parsed[0]["errorCode"], family.service_exception());
    }
    assert_eq!(cluster.documents("fcone").await.len(), 1);
}

/// Each sent alone, as recorded: nothing is trimmed or refused early, so what
/// happens to these is what the cluster makes of them.
async fn newlines_and_spaces_reach_the_cluster(
    family: Family,
    cluster: &Cluster,
    firehose: &aws_sdk_firehose::Client,
    s3: &aws_sdk_s3::Client,
) {
    enum Outcome {
        Indexed(&'static str),
        ItemFails(&'static str),
        RequestFails,
    }
    let cases: [(&str, &[u8], Outcome); 6] = [
        ("nl0", b"\n", Outcome::ItemFails("document is empty")),
        ("nl1", b"{\"k\":\"dbl\"}\n\n", Outcome::Indexed("dbl")),
        ("nl2", b"\n{\"k\":\"lead\"}", Outcome::RequestFails),
        ("nl3", b"\r\n", Outcome::ItemFails("document is empty")),
        ("nl4", b"{\"k\":\"crlf\"}\r\n", Outcome::Indexed("crlf")),
        ("nl5", b"  {\"k\":\"space\"}  ", Outcome::Indexed("space")),
    ];
    for (name, record, outcome) in cases {
        let prefix = format!("{name}/");
        create_stream(
            firehose,
            Stream {
                family,
                cluster,
                name,
                index: name,
                prefix: &prefix,
                retry_seconds: 0,
                rotation: "NoRotation",
                backup_all: false,
                source: None,
            },
        )
        .await;
        put_batch(firehose, name, &[record]).await;
        match outcome {
            Outcome::Indexed(k) => {
                let documents = tokio::time::timeout(Duration::from_secs(30), async {
                    loop {
                        let documents = cluster.documents(name).await;
                        if !documents.is_empty() {
                            return documents;
                        }
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                })
                .await
                .unwrap_or_else(|_| panic!("{name} indexed"));
                assert_eq!(documents.len(), 1, "{name}: {documents:?}");
                assert_eq!(documents[0].2["k"], k, "{name}");
            }
            Outcome::ItemFails(reason) => {
                let found = objects(s3, &prefix, 1).await;
                let parsed = lines(&found[0].1);
                assert_eq!(parsed[0]["errorCode"], family.service_exception(), "{name}");
                let message = parsed[0]["errorMessage"].as_str().unwrap();
                assert!(
                    message.contains("mapper_parsing_exception"),
                    "{name}: {message}"
                );
                assert!(message.contains(reason), "{name}: {message}");
            }
            Outcome::RequestFails => {
                let found = objects(s3, &prefix, 1).await;
                let parsed = lines(&found[0].1);
                assert_eq!(parsed[0]["errorCode"], family.service_exception(), "{name}");
                let message = parsed[0]["errorMessage"].as_str().unwrap();
                assert!(
                    message.contains("Malformed action/metadata line [3]"),
                    "{message}"
                );
            }
        }
    }
}

async fn deliver_to(family: Family, cluster: Cluster) {
    let server = TestServer::start().await;
    let firehose = server.firehose_client().await;
    let s3 = server.s3_client().await;
    s3.create_bucket().bucket(BUCKET).send().await.unwrap();
    mixed_batch(family, &cluster, &firehose, &s3).await;
    embedded_newline_fails_the_whole_bulk(family, &cluster, &firehose, &s3).await;
    blocked_index_keeps_the_status_and_retries(family, &cluster, &firehose, &s3).await;
    kinesis_source_ids_are_sequence_numbers(family, &cluster, &server, &firehose, &s3).await;
    all_documents_backup_and_week_rotation(family, &cluster, &firehose, &s3).await;
    part_of_a_batch_indexed(family, &cluster, &firehose, &s3).await;
    newlines_and_spaces_reach_the_cluster(family, &cluster, &firehose, &s3).await;
}

#[tokio::test]
async fn opensearch_destination_delivers_to_a_real_opensearch_cluster() {
    deliver_to(Family::OpenSearch, Cluster::opensearch().await).await;
}

#[tokio::test]
async fn elasticsearch_destination_delivers_to_a_real_elasticsearch_cluster() {
    deliver_to(Family::Elasticsearch, Cluster::elasticsearch().await).await;
}
