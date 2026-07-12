//! End-to-end tests for AWS X-Ray, driven through the real `aws-sdk-xray`
//! client against a live fakecloud server. Exercises the trace data plane
//! (ingest segment documents forming a small service graph, then read them back
//! via `BatchGetTraces` / `GetTraceSummaries` and derive the service graph via
//! `GetServiceGraph`) plus the control plane (sampling rules with the built-in
//! `Default`, groups, encryption config, and ARN-keyed tagging).

use aws_sdk_xray::primitives::DateTime;
use aws_sdk_xray::types::{EncryptionType, SamplingRule, Tag};
use fakecloud_testkit::TestServer;

async fn xray_client(server: &TestServer) -> aws_sdk_xray::Client {
    aws_sdk_xray::Client::new(&server.aws_config().await)
}

/// A base epoch (seconds) the ingested segments sit around, so a time-range
/// query brackets them deterministically.
const BASE: f64 = 1_700_000_000.0;

/// Two segment documents forming a `web -> backend` service graph: the `web`
/// segment carries a `namespace: "remote"` subsegment calling `backend`, and a
/// standalone `backend` segment records the downstream service.
fn segment_docs(trace_id: &str) -> Vec<String> {
    let web = format!(
        r#"{{"trace_id":"{trace_id}","id":"1111111111111111","name":"web","start_time":{start},"end_time":{end},
            "http":{{"request":{{"method":"GET","url":"http://web/"}},"response":{{"status":200}}}},
            "subsegments":[{{"id":"2222222222222222","name":"backend","namespace":"remote","start_time":{s2},"end_time":{e2}}}]}}"#,
        start = BASE + 1.0,
        end = BASE + 3.0,
        s2 = BASE + 1.5,
        e2 = BASE + 2.5,
    );
    let backend = format!(
        r#"{{"trace_id":"{trace_id}","id":"2222222222222222","name":"backend","start_time":{start},"end_time":{end},
            "parent_id":"1111111111111111","fault":true,
            "http":{{"request":{{"method":"GET","url":"http://backend/"}},"response":{{"status":500}}}}}}"#,
        start = BASE + 1.5,
        end = BASE + 2.5,
    );
    vec![web, backend]
}

#[tokio::test]
async fn xray_trace_data_plane_and_control_plane() {
    let server = TestServer::start().await;
    let xray = xray_client(&server).await;

    let trace_id = "1-58406520-a006649127e371903a2de979";

    // --- Ingest trace segments ---
    let put = xray
        .put_trace_segments()
        .set_trace_segment_documents(Some(segment_docs(trace_id)))
        .send()
        .await
        .expect("put_trace_segments");
    assert!(
        put.unprocessed_trace_segments().is_empty(),
        "all segments should ingest cleanly, got {:?}",
        put.unprocessed_trace_segments()
    );

    // --- BatchGetTraces returns the assembled trace ---
    let batch = xray
        .batch_get_traces()
        .trace_ids(trace_id)
        .send()
        .await
        .expect("batch_get_traces");
    let traces = batch.traces();
    assert_eq!(traces.len(), 1, "one trace assembled");
    assert_eq!(traces[0].id(), Some(trace_id));
    assert_eq!(traces[0].segments().len(), 2, "both segments echoed");

    // --- GetTraceSummaries finds it by time range ---
    let summaries = xray
        .get_trace_summaries()
        .start_time(DateTime::from_secs((BASE - 100.0) as i64))
        .end_time(DateTime::from_secs((BASE + 100.0) as i64))
        .send()
        .await
        .expect("get_trace_summaries");
    let ts = summaries.trace_summaries();
    assert!(
        ts.iter().any(|s| s.id() == Some(trace_id)),
        "trace should be summarized in range"
    );
    let summary = ts.iter().find(|s| s.id() == Some(trace_id)).unwrap();
    assert_eq!(summary.has_fault(), Some(true), "backend fault surfaces");

    // --- GetServiceGraph derives the web -> backend nodes + edge ---
    let graph = xray
        .get_service_graph()
        .start_time(DateTime::from_secs((BASE - 100.0) as i64))
        .end_time(DateTime::from_secs((BASE + 100.0) as i64))
        .send()
        .await
        .expect("get_service_graph");
    let services = graph.services();
    assert_eq!(services.len(), 2, "web + backend nodes: {services:?}");
    let web = services
        .iter()
        .find(|s| s.name() == Some("web"))
        .expect("web node");
    assert_eq!(web.root(), Some(true), "web is the entry point");
    assert_eq!(web.edges().len(), 1, "web -> backend edge");
    let backend = services
        .iter()
        .find(|s| s.name() == Some("backend"))
        .expect("backend node");
    assert_eq!(backend.root(), Some(false));

    // --- Sampling rules: Default is present, and a created rule shows up ---
    let rule = SamplingRule::builder()
        .rule_name("e2e-rule")
        .resource_arn("*")
        .priority(100)
        .fixed_rate(0.1)
        .reservoir_size(2)
        .service_name("*")
        .service_type("*")
        .host("*")
        .http_method("*")
        .url_path("*")
        .version(1)
        .build()
        .expect("sampling rule");
    xray.create_sampling_rule()
        .sampling_rule(rule)
        .send()
        .await
        .expect("create_sampling_rule");
    let rules = xray
        .get_sampling_rules()
        .send()
        .await
        .expect("get_sampling_rules");
    let names: Vec<&str> = rules
        .sampling_rule_records()
        .iter()
        .filter_map(|r| r.sampling_rule().and_then(|s| s.rule_name()))
        .collect();
    assert!(names.contains(&"Default"), "built-in Default rule present");
    assert!(names.contains(&"e2e-rule"), "created rule present");

    // --- Groups ---
    let created = xray
        .create_group()
        .group_name("e2e-group")
        .filter_expression("fault")
        .send()
        .await
        .expect("create_group");
    let group_arn = created
        .group()
        .and_then(|g| g.group_arn())
        .expect("group arn")
        .to_string();
    let groups = xray.get_groups().send().await.expect("get_groups");
    assert!(groups
        .groups()
        .iter()
        .any(|g| g.group_name() == Some("e2e-group")));

    // --- Encryption config ---
    let default_cfg = xray
        .get_encryption_config()
        .send()
        .await
        .expect("get_encryption_config");
    assert_eq!(
        default_cfg.encryption_config().and_then(|c| c.r#type()),
        Some(&EncryptionType::None)
    );
    xray.put_encryption_config()
        .r#type(EncryptionType::None)
        .send()
        .await
        .expect("put_encryption_config");

    // --- Tagging the group by ARN ---
    xray.tag_resource()
        .resource_arn(&group_arn)
        .tags(
            Tag::builder()
                .key("team")
                .value("observability")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("tag_resource");
    let tags = xray
        .list_tags_for_resource()
        .resource_arn(&group_arn)
        .send()
        .await
        .expect("list_tags_for_resource");
    assert!(tags
        .tags()
        .iter()
        .any(|t| t.key() == "team" && t.value() == "observability"));
}
