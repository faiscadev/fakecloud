//! OpenSearch Service / Elasticsearch Service domains backed by REAL search
//! clusters: a domain reports `Processing` until its container answers health,
//! then indexes and searches through its AWS-format endpoint (served by
//! fakecloud's `Host`-routed proxy) and its published port, takes Firehose
//! `DomainARN` deliveries, keeps its indices across a configuration change and
//! a persistent-mode restart, and is torn down with its data volume on delete.
//!
//! The clusters run `public.ecr.aws/opensearchproject/opensearch:2.11.1` and
//! `docker.elastic.co/elasticsearch/elasticsearch-oss:7.10.2`, the images the
//! runtime maps `OpenSearch_2.11` and `Elasticsearch_7.10` to. A missing
//! container runtime fails these tests rather than skipping them.

use std::process::Command;
use std::time::Duration;

use aws_sdk_firehose::primitives::Blob;
use aws_sdk_firehose::types::{
    AmazonopensearchserviceBufferingHints, AmazonopensearchserviceDestinationConfiguration,
    AmazonopensearchserviceIndexRotationPeriod, AmazonopensearchserviceRetryOptions, Record,
    S3DestinationConfiguration,
};
use fakecloud_testkit::{detect_container_cli, TestServer};
use serde_json::{json, Value};

const ROLE: &str = "arn:aws:iam::123456789012:role/firehose";

fn require_container_runtime() -> String {
    let cli = detect_container_cli();
    let info = Command::new(&cli).arg("info").output();
    assert!(
        info.is_ok_and(|o| o.status.success()),
        "{cli} is required: these tests run real search clusters"
    );
    cli
}

/// Container ids (or volume names) carrying `label`.
fn labelled(cli: &str, what: &[&str], label: &str) -> Vec<String> {
    let mut args: Vec<&str> = what.to_vec();
    args.extend(["--filter", label]);
    let out = Command::new(cli).args(&args).output().unwrap();
    String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        .map(str::to_string)
        .collect()
}

/// `DomainStatus` from DescribeDomain (or DescribeElasticsearchDomain).
async fn describe(server: &TestServer, es_api: bool, name: &str) -> Value {
    let out = if es_api {
        server
            .aws_cli(&["es", "describe-elasticsearch-domain", "--domain-name", name])
            .await
    } else {
        server
            .aws_cli(&["opensearch", "describe-domain", "--domain-name", name])
            .await
    };
    assert!(out.success(), "{}", out.stderr_text());
    out.stdout_json()["DomainStatus"].clone()
}

/// Poll until the domain stops processing; returns its final status.
async fn wait_settled(server: &TestServer, es_api: bool, name: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(420), async {
        loop {
            let status = describe(server, es_api, name).await;
            if status["Processing"] == json!(false) {
                return status;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("domain {name} still processing after 7 minutes"))
}

/// A client for one domain through fakecloud's listener, addressed by the
/// domain's own endpoint name in `Host`.
struct DomainClient {
    http: reqwest::Client,
    base: String,
    host: String,
}

impl DomainClient {
    fn new(server: &TestServer, endpoint: &str) -> Self {
        Self {
            http: reqwest::Client::builder().no_proxy().build().unwrap(),
            base: server.endpoint().to_string(),
            host: endpoint.to_string(),
        }
    }

    async fn send(&self, method: reqwest::Method, path: &str, body: Option<Value>) -> (u16, Value) {
        let mut req = self
            .http
            .request(method, format!("{}{path}", self.base))
            .header("Host", &self.host);
        if let Some(b) = body {
            req = req.json(&b);
        }
        let resp = req.send().await.unwrap();
        let status = resp.status().as_u16();
        let text = resp.text().await.unwrap();
        (
            status,
            serde_json::from_str(&text).unwrap_or(Value::String(text)),
        )
    }

    async fn search_hits(&self, index: &str, query: &str) -> Vec<Value> {
        let (_, _) = self
            .send(reqwest::Method::POST, &format!("/{index}/_refresh"), None)
            .await;
        let (status, body) = self
            .send(
                reqwest::Method::GET,
                &format!("/{index}/_search?q={query}&size=50"),
                None,
            )
            .await;
        if status == 404 {
            // Not created yet (Firehose creates it with its first bulk).
            return Vec::new();
        }
        assert_eq!(status, 200, "{body}");
        body["hits"]["hits"].as_array().cloned().unwrap_or_default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_opensearch_domain_runs_a_real_cluster_that_firehose_delivers_to() {
    let cli = require_container_runtime();
    let server = TestServer::start().await;

    let created = server
        .aws_cli(&[
            "opensearch",
            "create-domain",
            "--domain-name",
            "books",
            "--engine-version",
            "OpenSearch_2.11",
        ])
        .await;
    assert!(created.success(), "{}", created.stderr_text());
    let status = &created.stdout_json()["DomainStatus"];
    assert_eq!(status["Processing"], json!(true));
    assert_eq!(status["DomainProcessingStatus"], "Creating");
    let arn = status["ARN"].as_str().unwrap().to_string();
    let endpoint = status["Endpoint"].as_str().unwrap().to_string();
    assert!(
        endpoint.starts_with("search-books-") && endpoint.ends_with(".us-east-1.es.amazonaws.com"),
        "{endpoint}"
    );

    let settled = wait_settled(&server, false, "books").await;
    assert_eq!(settled["DomainProcessingStatus"], "Active");
    assert_eq!(settled["Created"], json!(true));
    // The AWS-format endpoint is still what the domain reports.
    assert_eq!(settled["Endpoint"], endpoint.as_str());

    // One real container backs it, on its published port too.
    let label = format!("label=fakecloud-opensearch={arn}");
    let containers = labelled(&cli, &["ps", "-q"], &label);
    assert_eq!(containers.len(), 1, "{containers:?}");
    let port = Command::new(&cli)
        .args(["port", &containers[0], "9200"])
        .output()
        .unwrap();
    let mapped = String::from_utf8_lossy(&port.stdout);
    let host_port = mapped
        .lines()
        .find_map(|l| l.trim().rsplit(':').next()?.parse::<u16>().ok())
        .expect("9200 is published");
    let direct: Value = reqwest::get(format!("http://127.0.0.1:{host_port}/"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(direct["version"]["distribution"], "opensearch");
    assert!(
        direct["version"]["number"]
            .as_str()
            .unwrap()
            .starts_with("2.11."),
        "{direct}"
    );

    // Index and search through the domain's endpoint name.
    let domain = DomainClient::new(&server, &endpoint);
    let (code, body) = domain
        .send(
            reqwest::Method::PUT,
            "/books/_doc/1?refresh=true",
            Some(json!({"title": "dune", "year": 1965})),
        )
        .await;
    assert_eq!(code, 201, "{body}");
    let hits = domain.search_hits("books", "title:dune").await;
    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(hits[0]["_source"]["year"], 1965);

    // Firehose delivers to the domain by ARN, not by a cluster endpoint.
    let s3 = server.s3_client().await;
    s3.create_bucket()
        .bucket("domain-failures")
        .send()
        .await
        .unwrap();
    let firehose = server.firehose_client().await;
    firehose
        .create_delivery_stream()
        .delivery_stream_name("to-domain")
        .amazonopensearchservice_destination_configuration(
            AmazonopensearchserviceDestinationConfiguration::builder()
                .role_arn(ROLE)
                .domain_arn(&arn)
                .index_name("events")
                .index_rotation_period(AmazonopensearchserviceIndexRotationPeriod::NoRotation)
                .buffering_hints(
                    AmazonopensearchserviceBufferingHints::builder()
                        .size_in_mbs(1)
                        .interval_in_seconds(0)
                        .build(),
                )
                .retry_options(
                    AmazonopensearchserviceRetryOptions::builder()
                        .duration_in_seconds(60)
                        .build(),
                )
                .s3_configuration(
                    S3DestinationConfiguration::builder()
                        .role_arn(ROLE)
                        .bucket_arn("arn:aws:s3:::domain-failures")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let sent = firehose
        .put_record_batch()
        .delivery_stream_name("to-domain")
        .records(
            Record::builder()
                .data(Blob::new(br#"{"kind":"click","n":1}"#.to_vec()))
                .build()
                .unwrap(),
        )
        .records(
            Record::builder()
                .data(Blob::new(br#"{"kind":"click","n":2}"#.to_vec()))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(sent.failed_put_count(), 0);
    let delivered = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let hits = domain.search_hits("events", "kind:click").await;
            if hits.len() >= 2 {
                return hits;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("Firehose delivered both records to the domain within 60s");
    let mut ns: Vec<i64> = delivered
        .iter()
        .map(|h| h["_source"]["n"].as_i64().unwrap())
        .collect();
    ns.sort();
    assert_eq!(ns, vec![1, 2]);
    let failures = s3
        .list_objects_v2()
        .bucket("domain-failures")
        .send()
        .await
        .unwrap();
    assert_eq!(failures.key_count(), Some(0), "nothing failed into S3");

    // A node-setting change replaces the cluster on the same data volume.
    let updated = server
        .aws_cli(&[
            "opensearch",
            "update-domain-config",
            "--domain-name",
            "books",
            "--advanced-options",
            r#"{"indices.query.bool.max_clause_count":"2048"}"#,
        ])
        .await;
    assert!(updated.success(), "{}", updated.stderr_text());
    let modifying = describe(&server, false, "books").await;
    assert_eq!(modifying["Processing"], json!(true), "{modifying}");
    assert_eq!(modifying["DomainProcessingStatus"], "Modifying");
    wait_settled(&server, false, "books").await;
    let (code, settings) = domain
        .send(
            reqwest::Method::GET,
            "/_nodes/settings?flat_settings=true",
            None,
        )
        .await;
    assert_eq!(code, 200, "{settings}");
    let node = settings["nodes"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap();
    assert_eq!(
        node["settings"]["indices.query.bool.max_clause_count"], "2048",
        "{node}"
    );
    assert_eq!(
        domain.search_hits("books", "title:dune").await.len(),
        1,
        "the index survived the replacement"
    );
    let replaced = labelled(&cli, &["ps", "-q"], &label);
    assert_eq!(replaced.len(), 1, "{replaced:?}");
    assert_ne!(
        replaced[0], containers[0],
        "a new container serves the domain"
    );

    // Delete tears the cluster and its data volume down.
    let deleted = server
        .aws_cli(&["opensearch", "delete-domain", "--domain-name", "books"])
        .await;
    assert!(deleted.success(), "{}", deleted.stderr_text());
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if labelled(&cli, &["ps", "-aq"], &label).is_empty()
                && labelled(&cli, &["volume", "ls", "-q"], &label).is_empty()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("the domain's container and data volume are removed");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_elasticsearch_domain_from_the_2015_api_runs_a_real_cluster() {
    let cli = require_container_runtime();
    let server = TestServer::start().await;
    let created = server
        .aws_cli(&[
            "es",
            "create-elasticsearch-domain",
            "--domain-name",
            "legacy",
            "--elasticsearch-version",
            "7.10",
        ])
        .await;
    assert!(created.success(), "{}", created.stderr_text());
    assert_eq!(
        created.stdout_json()["DomainStatus"]["Processing"],
        json!(true)
    );
    let settled = wait_settled(&server, true, "legacy").await;
    assert_eq!(settled["ElasticsearchVersion"], "7.10");
    let endpoint = settled["Endpoint"].as_str().unwrap().to_string();

    let domain = DomainClient::new(&server, &endpoint);
    let (code, root) = domain.send(reqwest::Method::GET, "/", None).await;
    assert_eq!(code, 200, "{root}");
    assert_eq!(root["version"]["number"], "7.10.2");
    let (code, body) = domain
        .send(
            reqwest::Method::POST,
            "/logs/_doc?refresh=true",
            Some(json!({"level": "warn", "msg": "disk"})),
        )
        .await;
    assert_eq!(code, 201, "{body}");
    assert_eq!(domain.search_hits("logs", "level:warn").await.len(), 1);

    // Visible through the 2021 API as the same domain.
    let os_view = describe(&server, false, "legacy").await;
    assert_eq!(os_view["EngineVersion"], "Elasticsearch_7.10");
    assert_eq!(os_view["Processing"], json!(false));

    let arn = settled["ARN"].as_str().unwrap();
    assert_eq!(
        labelled(
            &cli,
            &["ps", "-q"],
            &format!("label=fakecloud-opensearch={arn}")
        )
        .len(),
        1
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_persisted_domain_comes_back_with_its_indices_after_a_restart() {
    let cli = require_container_runtime();
    let dir = tempfile::tempdir().unwrap();
    let data_path = fakecloud_testkit::data_path_for(&dir);
    let data_path = data_path.to_str().unwrap();
    let mut server = TestServer::start_full(
        &[],
        &["--storage-mode", "persistent", "--data-path", data_path],
    )
    .await;
    let created = server
        .aws_cli(&[
            "opensearch",
            "create-domain",
            "--domain-name",
            "kept",
            "--engine-version",
            "OpenSearch_2.11",
        ])
        .await;
    assert!(created.success(), "{}", created.stderr_text());
    let arn = created.stdout_json()["DomainStatus"]["ARN"]
        .as_str()
        .unwrap()
        .to_string();
    let endpoint = wait_settled(&server, false, "kept").await["Endpoint"]
        .as_str()
        .unwrap()
        .to_string();
    let (code, body) = DomainClient::new(&server, &endpoint)
        .send(
            reqwest::Method::PUT,
            "/notes/_doc/a?refresh=true",
            Some(json!({"text": "remember me"})),
        )
        .await;
    assert_eq!(code, 201, "{body}");

    // A graceful restart removes the container but keeps the data volume.
    server.restart().await;
    let label = format!("label=fakecloud-opensearch={arn}");
    let after = wait_settled(&server, false, "kept").await;
    assert_eq!(after["Endpoint"], endpoint.as_str());
    let hits = DomainClient::new(&server, &endpoint)
        .search_hits("notes", "text:remember")
        .await;
    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(labelled(&cli, &["ps", "-q"], &label).len(), 1);

    // Clean up: the data dir's volumes outlive the process by design.
    let deleted = server
        .aws_cli(&["opensearch", "delete-domain", "--domain-name", "kept"])
        .await;
    assert!(deleted.success(), "{}", deleted.stderr_text());
    tokio::time::timeout(Duration::from_secs(60), async {
        while !labelled(&cli, &["volume", "ls", "-q"], &label).is_empty() {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("the domain's data volume is removed");
}

#[tokio::test]
async fn without_a_backend_a_domain_settles_control_plane_only() {
    let server = TestServer::start_with_env(&[("FAKECLOUD_OPENSEARCH_DISABLE_BACKEND", "1")]).await;
    let created = server
        .aws_cli(&[
            "opensearch",
            "create-domain",
            "--domain-name",
            "meta",
            "--engine-version",
            "OpenSearch_2.11",
        ])
        .await;
    assert!(created.success(), "{}", created.stderr_text());
    let status = &created.stdout_json()["DomainStatus"];
    assert_eq!(status["Processing"], json!(true));
    assert_eq!(status["DomainProcessingStatus"], "Active");
    let settled = describe(&server, false, "meta").await;
    assert_eq!(settled["Processing"], json!(false));
    let endpoint = settled["Endpoint"].as_str().unwrap();
    // Nothing serves the endpoint name.
    let (code, _) = DomainClient::new(&server, endpoint)
        .send(reqwest::Method::GET, "/", None)
        .await;
    assert_eq!(code, 503);
}
