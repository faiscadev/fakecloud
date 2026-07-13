use super::*;

#[test]
fn s3_condition_keys_emits_list_params() {
    let mut q = std::collections::HashMap::new();
    q.insert("prefix".to_string(), "logs/".to_string());
    q.insert("delimiter".to_string(), "/".to_string());
    q.insert("max-keys".to_string(), "100".to_string());
    let keys = s3_condition_keys("ListObjectsV2", &q);
    assert_eq!(keys.get("s3:prefix"), Some(&vec!["logs/".to_string()]));
    assert_eq!(keys.get("s3:delimiter"), Some(&vec!["/".to_string()]));
    assert_eq!(keys.get("s3:max-keys"), Some(&vec!["100".to_string()]));
}

#[test]
fn s3_condition_keys_omits_absent_params() {
    let q = std::collections::HashMap::new();
    let keys = s3_condition_keys("ListObjectsV2", &q);
    assert!(keys.is_empty());
}

#[test]
fn s3_condition_keys_partial_params() {
    let mut q = std::collections::HashMap::new();
    q.insert("prefix".to_string(), "archive/".to_string());
    let keys = s3_condition_keys("ListObjects", &q);
    assert_eq!(keys.len(), 1);
    assert_eq!(keys.get("s3:prefix"), Some(&vec!["archive/".to_string()]));
}

#[test]
fn s3_condition_keys_empty_for_non_list_actions() {
    let mut q = std::collections::HashMap::new();
    q.insert("prefix".to_string(), "logs/".to_string());
    assert!(s3_condition_keys("GetObject", &q).is_empty());
    assert!(s3_condition_keys("PutObject", &q).is_empty());
    assert!(s3_condition_keys("ListBuckets", &q).is_empty());
}

#[test]
fn valid_bucket_names() {
    assert!(is_valid_bucket_name("my-bucket"));
    assert!(is_valid_bucket_name("my.bucket.name"));
    assert!(is_valid_bucket_name("abc"));
    assert!(!is_valid_bucket_name("ab"));
    assert!(!is_valid_bucket_name("-bucket"));
    assert!(!is_valid_bucket_name("Bucket"));
    assert!(!is_valid_bucket_name("bucket-"));
}

#[test]
fn parse_delete_xml() {
    let xml =
        r#"<Delete><Object><Key>a.txt</Key></Object><Object><Key>b/c.txt</Key></Object></Delete>"#;
    let entries = parse_delete_objects_xml(xml);
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].key, "a.txt");
    assert!(entries[0].version_id.is_none());
    assert_eq!(entries[1].key, "b/c.txt");
}

#[test]
fn parse_delete_xml_with_version() {
    let xml = r#"<Delete><Object><Key>a.txt</Key><VersionId>v1</VersionId></Object></Delete>"#;
    let entries = parse_delete_objects_xml(xml);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key, "a.txt");
    assert_eq!(entries[0].version_id.as_deref(), Some("v1"));
}

#[test]
fn parse_tags_xml() {
    let xml = r#"<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>"#;
    let tags = parse_tagging_xml(xml);
    assert_eq!(tags, vec![("env".to_string(), "prod".to_string())]);
}

#[test]
fn md5_hash() {
    let hash = compute_md5(b"hello");
    assert_eq!(hash, "5d41402abc4b2a76b9719d911017c592");
}

#[test]
fn test_etag_matches() {
    assert!(etag_matches("\"abc\"", "\"abc\""));
    assert!(etag_matches("abc", "\"abc\""));
    assert!(etag_matches("*", "\"abc\""));
    assert!(!etag_matches("\"xyz\"", "\"abc\""));
}

#[test]
fn test_event_matches() {
    assert!(event_matches("s3:ObjectCreated:Put", "s3:ObjectCreated:*"));
    assert!(event_matches("s3:ObjectCreated:Copy", "s3:ObjectCreated:*"));
    assert!(event_matches(
        "s3:ObjectRemoved:Delete",
        "s3:ObjectRemoved:*"
    ));
    assert!(!event_matches(
        "s3:ObjectRemoved:Delete",
        "s3:ObjectCreated:*"
    ));
    assert!(event_matches(
        "s3:ObjectCreated:Put",
        "s3:ObjectCreated:Put"
    ));
    assert!(event_matches("s3:ObjectCreated:Put", "s3:*"));
}

#[test]
fn test_parse_notification_config() {
    let xml = r#"<NotificationConfiguration>
        <QueueConfiguration>
            <Queue>arn:aws:sqs:us-east-1:123456789012:my-queue</Queue>
            <Event>s3:ObjectCreated:*</Event>
        </QueueConfiguration>
        <TopicConfiguration>
            <Topic>arn:aws:sns:us-east-1:123456789012:my-topic</Topic>
            <Event>s3:ObjectRemoved:*</Event>
        </TopicConfiguration>
    </NotificationConfiguration>"#;
    let targets = parse_notification_config(xml);
    assert_eq!(targets.len(), 2);
    assert_eq!(
        targets[0].arn,
        "arn:aws:sqs:us-east-1:123456789012:my-queue"
    );
    assert_eq!(targets[0].events, vec!["s3:ObjectCreated:*"]);
    assert_eq!(
        targets[1].arn,
        "arn:aws:sns:us-east-1:123456789012:my-topic"
    );
    assert_eq!(targets[1].events, vec!["s3:ObjectRemoved:*"]);
}

#[test]
fn test_parse_notification_config_lambda() {
    // Test CloudFunctionConfiguration (older format)
    let xml = r#"<NotificationConfiguration>
        <CloudFunctionConfiguration>
            <CloudFunction>arn:aws:lambda:us-east-1:123456789012:function:my-func</CloudFunction>
            <Event>s3:ObjectCreated:*</Event>
        </CloudFunctionConfiguration>
    </NotificationConfiguration>"#;
    let targets = parse_notification_config(xml);
    assert_eq!(targets.len(), 1);
    assert!(matches!(
        targets[0].target_type,
        NotificationTargetType::Lambda
    ));
    assert_eq!(
        targets[0].arn,
        "arn:aws:lambda:us-east-1:123456789012:function:my-func"
    );
    assert_eq!(targets[0].events, vec!["s3:ObjectCreated:*"]);
}

#[test]
fn test_parse_notification_config_lambda_new_format() {
    // Test LambdaFunctionConfiguration (newer format used by AWS SDK)
    let xml = r#"<NotificationConfiguration>
        <LambdaFunctionConfiguration>
            <Function>arn:aws:lambda:us-east-1:123456789012:function:my-func</Function>
            <Event>s3:ObjectCreated:Put</Event>
            <Event>s3:ObjectRemoved:*</Event>
        </LambdaFunctionConfiguration>
    </NotificationConfiguration>"#;
    let targets = parse_notification_config(xml);
    assert_eq!(targets.len(), 1);
    assert!(matches!(
        targets[0].target_type,
        NotificationTargetType::Lambda
    ));
    assert_eq!(
        targets[0].arn,
        "arn:aws:lambda:us-east-1:123456789012:function:my-func"
    );
    assert_eq!(
        targets[0].events,
        vec!["s3:ObjectCreated:Put", "s3:ObjectRemoved:*"]
    );
}

#[test]
fn test_parse_notification_config_all_types() {
    let xml = r#"<NotificationConfiguration>
        <QueueConfiguration>
            <Queue>arn:aws:sqs:us-east-1:123456789012:q</Queue>
            <Event>s3:ObjectCreated:*</Event>
        </QueueConfiguration>
        <TopicConfiguration>
            <Topic>arn:aws:sns:us-east-1:123456789012:t</Topic>
            <Event>s3:ObjectRemoved:*</Event>
        </TopicConfiguration>
        <LambdaFunctionConfiguration>
            <Function>arn:aws:lambda:us-east-1:123456789012:function:f</Function>
            <Event>s3:ObjectCreated:Put</Event>
        </LambdaFunctionConfiguration>
    </NotificationConfiguration>"#;
    let targets = parse_notification_config(xml);
    assert_eq!(targets.len(), 3);
    assert!(matches!(
        targets[0].target_type,
        NotificationTargetType::Sqs
    ));
    assert!(matches!(
        targets[1].target_type,
        NotificationTargetType::Sns
    ));
    assert!(matches!(
        targets[2].target_type,
        NotificationTargetType::Lambda
    ));
}

#[test]
fn test_parse_notification_config_with_filters() {
    let xml = r#"<NotificationConfiguration>
        <LambdaFunctionConfiguration>
            <Function>arn:aws:lambda:us-east-1:123456789012:function:my-func</Function>
            <Event>s3:ObjectCreated:*</Event>
            <Filter>
                <S3Key>
                    <FilterRule>
                        <Name>prefix</Name>
                        <Value>images/</Value>
                    </FilterRule>
                    <FilterRule>
                        <Name>suffix</Name>
                        <Value>.jpg</Value>
                    </FilterRule>
                </S3Key>
            </Filter>
        </LambdaFunctionConfiguration>
    </NotificationConfiguration>"#;
    let targets = parse_notification_config(xml);
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].prefix_filter, Some("images/".to_string()));
    assert_eq!(targets[0].suffix_filter, Some(".jpg".to_string()));
}

#[test]
fn test_parse_notification_config_no_filters() {
    let xml = r#"<NotificationConfiguration>
        <LambdaFunctionConfiguration>
            <Function>arn:aws:lambda:us-east-1:123456789012:function:my-func</Function>
            <Event>s3:ObjectCreated:*</Event>
        </LambdaFunctionConfiguration>
    </NotificationConfiguration>"#;
    let targets = parse_notification_config(xml);
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].prefix_filter, None);
    assert_eq!(targets[0].suffix_filter, None);
}

#[test]
fn test_key_matches_filters() {
    // No filters — everything matches
    assert!(key_matches_filters("anything", &None, &None));

    // Prefix only
    assert!(key_matches_filters(
        "images/photo.jpg",
        &Some("images/".to_string()),
        &None
    ));
    assert!(!key_matches_filters(
        "docs/file.txt",
        &Some("images/".to_string()),
        &None
    ));

    // Suffix only
    assert!(key_matches_filters(
        "images/photo.jpg",
        &None,
        &Some(".jpg".to_string())
    ));
    assert!(!key_matches_filters(
        "images/photo.png",
        &None,
        &Some(".jpg".to_string())
    ));

    // Both prefix and suffix
    assert!(key_matches_filters(
        "images/photo.jpg",
        &Some("images/".to_string()),
        &Some(".jpg".to_string())
    ));
    assert!(!key_matches_filters(
        "images/photo.png",
        &Some("images/".to_string()),
        &Some(".jpg".to_string())
    ));
    assert!(!key_matches_filters(
        "docs/photo.jpg",
        &Some("images/".to_string()),
        &Some(".jpg".to_string())
    ));
}

#[test]
fn test_parse_cors_config() {
    let xml = r#"<CORSConfiguration>
        <CORSRule>
            <AllowedOrigin>https://example.com</AllowedOrigin>
            <AllowedMethod>GET</AllowedMethod>
            <AllowedMethod>PUT</AllowedMethod>
            <AllowedHeader>*</AllowedHeader>
            <ExposeHeader>x-amz-request-id</ExposeHeader>
            <MaxAgeSeconds>3600</MaxAgeSeconds>
        </CORSRule>
    </CORSConfiguration>"#;
    let rules = parse_cors_config(xml);
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].allowed_origins, vec!["https://example.com"]);
    assert_eq!(rules[0].allowed_methods, vec!["GET", "PUT"]);
    assert_eq!(rules[0].allowed_headers, vec!["*"]);
    assert_eq!(rules[0].expose_headers, vec!["x-amz-request-id"]);
    assert_eq!(rules[0].max_age_seconds, Some(3600));
}

#[test]
fn test_origin_matches() {
    assert!(origin_matches("https://example.com", "https://example.com"));
    assert!(origin_matches("https://example.com", "*"));
    assert!(origin_matches("https://foo.example.com", "*.example.com"));
    assert!(!origin_matches("https://evil.com", "https://example.com"));
}

/// Regression: resolve_object with versionId="null" must match objects
/// whose version_id is either None or Some("null").
#[test]
fn resolve_null_version_matches_both_none_and_null_string() {
    use crate::state::S3Bucket;
    use bytes::Bytes;
    use chrono::Utc;

    let mut b = S3Bucket::new("test", "us-east-1", "owner");

    // Helper to create a minimal S3Object
    let make_obj = |key: &str, vid: Option<&str>| crate::state::S3Object {
        key: key.to_string(),
        body: crate::state::memory_body(Bytes::from_static(b"x")),
        content_type: "text/plain".to_string(),
        etag: "\"abc\"".to_string(),
        size: 1,
        last_modified: Utc::now(),
        storage_class: "STANDARD".to_string(),
        version_id: vid.map(|s| s.to_string()),
        ..Default::default()
    };

    // Object with version_id = Some("null") (pre-versioning migrated)
    let obj = make_obj("file.txt", Some("null"));
    b.objects.insert("file.txt".to_string(), obj.clone());
    b.object_versions.insert("file.txt".to_string(), vec![obj]);

    let null_str = "null".to_string();
    let result = resolve_object(&b, "file.txt", Some(&null_str));
    assert!(
        result.is_ok(),
        "versionId=null should match version_id=Some(\"null\")"
    );

    // Object with version_id = None (true pre-versioning)
    let obj2 = make_obj("file2.txt", None);
    b.objects.insert("file2.txt".to_string(), obj2.clone());
    b.object_versions
        .insert("file2.txt".to_string(), vec![obj2]);

    let result2 = resolve_object(&b, "file2.txt", Some(&null_str));
    assert!(
        result2.is_ok(),
        "versionId=null should match version_id=None"
    );
}

#[test]
fn test_parse_replication_rules() {
    let xml = r#"<ReplicationConfiguration>
        <Role>arn:aws:iam::role/replication</Role>
        <Rule>
            <Status>Enabled</Status>
            <Filter><Prefix>logs/</Prefix></Filter>
            <Destination><Bucket>arn:aws:s3:::dest-bucket</Bucket></Destination>
        </Rule>
        <Rule>
            <Status>Disabled</Status>
            <Filter><Prefix></Prefix></Filter>
            <Destination><Bucket>arn:aws:s3:::other-bucket</Bucket></Destination>
        </Rule>
    </ReplicationConfiguration>"#;

    let rules = parse_replication_rules(xml);
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].status, "Enabled");
    assert_eq!(rules[0].prefix, "logs/");
    assert_eq!(rules[0].dest_bucket, "dest-bucket");
    assert_eq!(rules[1].status, "Disabled");
    assert_eq!(rules[1].prefix, "");
    assert_eq!(rules[1].dest_bucket, "other-bucket");
}

#[test]
fn test_parse_normalized_replication_rules() {
    // First, normalize the XML like the server does
    let input_xml = r#"<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Role>arn:aws:iam::123456789012:role/replication-role</Role><Rule><ID>replicate-all</ID><Status>Enabled</Status><Filter><Prefix></Prefix></Filter><Destination><Bucket>arn:aws:s3:::repl-dest</Bucket></Destination></Rule></ReplicationConfiguration>"#;
    let normalized = normalize_replication_xml(input_xml);
    eprintln!("Normalized XML: {normalized}");
    let rules = parse_replication_rules(&normalized);
    assert_eq!(rules.len(), 1, "Expected 1 rule, got {}", rules.len());
    assert_eq!(rules[0].status, "Enabled");
    assert_eq!(rules[0].dest_bucket, "repl-dest");
}

#[test]
fn test_replicate_object() {
    use crate::state::{S3Bucket, S3State};

    let mut state = S3State::new("123456789012", "us-east-1");

    // Create source and destination buckets
    let mut src = S3Bucket::new("source", "us-east-1", "owner");
    src.versioning = Some("Enabled".to_string());
    src.replication_config = Some(
        "<ReplicationConfiguration>\
         <Rule><Status>Enabled</Status>\
         <Filter><Prefix></Prefix></Filter>\
         <Destination><Bucket>arn:aws:s3:::destination</Bucket></Destination>\
         </Rule></ReplicationConfiguration>"
            .to_string(),
    );
    let obj = S3Object {
        key: "test-key".to_string(),
        body: crate::state::memory_body(Bytes::from_static(b"hello")),
        content_type: "text/plain".to_string(),
        etag: "abc".to_string(),
        size: 5,
        last_modified: Utc::now(),
        storage_class: "STANDARD".to_string(),
        version_id: Some("v1".to_string()),
        ..Default::default()
    };
    src.objects.insert("test-key".to_string(), obj);
    state.buckets.insert("source".to_string(), src);

    let dest = S3Bucket::new("destination", "us-east-1", "owner");
    state.buckets.insert("destination".to_string(), dest);

    replicate_object(&mut state, "source", "test-key");

    // Object should now exist in destination
    let dest_obj = state
        .buckets
        .get("destination")
        .unwrap()
        .objects
        .get("test-key");
    assert!(dest_obj.is_some());
    assert_eq!(
        state.read_body(&dest_obj.unwrap().body).unwrap(),
        Bytes::from_static(b"hello")
    );
}

#[test]
fn cors_header_value_does_not_panic_on_unusual_input() {
    // Verify that CORS header value parsing doesn't panic even with unusual strings.
    // HeaderValue::from_str rejects non-visible-ASCII, so our unwrap_or_else fallback
    // must produce a valid (empty) header value instead of panicking.
    let valid_origin = "https://example.com";
    let result: Result<http::HeaderValue, _> = valid_origin.parse();
    assert!(result.is_ok());

    // Non-ASCII would fail .parse() for HeaderValue; verify fallback works
    let bad_origin = "https://ex\x01ample.com";
    let result: Result<http::HeaderValue, _> = bad_origin.parse();
    assert!(result.is_err());
    // Our production code uses unwrap_or_else to return empty HeaderValue
    let fallback = bad_origin
        .parse()
        .unwrap_or_else(|_| http::HeaderValue::from_static(""));
    assert_eq!(fallback, "");
}

// ────────────────────────────────────────────────────────────────
// Service-level tests for tags / multipart / config submodules.
//
// Each helper below builds an isolated S3Service with the in-memory
// store so the submodule handlers can be driven directly without a
// running Axum router.
// ────────────────────────────────────────────────────────────────

use crate::state::{S3Bucket, S3Object};
use bytes::Bytes;
use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::service::{AwsRequest, AwsServiceError};
use http::{HeaderMap, Method, StatusCode};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

fn make_service() -> S3Service {
    let state: SharedS3State = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    S3Service::new(state, Arc::new(DeliveryBus::new()))
}

fn seed_bucket(svc: &S3Service, name: &str) {
    let mut mas = svc.state.write();
    let state = mas.default_mut();
    state
        .buckets
        .insert(name.to_string(), S3Bucket::new(name, "us-east-1", "owner"));
}

fn seed_object(svc: &S3Service, bucket: &str, key: &str, body: &[u8]) {
    let mut mas = svc.state.write();
    let state = mas.default_mut();
    let b = state.buckets.get_mut(bucket).expect("bucket seeded");
    let mut obj = S3Object {
        key: key.to_string(),
        body: fakecloud_persistence::BodyRef::Memory(Bytes::copy_from_slice(body)),
        content_type: "application/octet-stream".to_string(),
        etag: format!("\"{}\"", compute_md5(body)),
        size: body.len() as u64,
        last_modified: chrono::Utc::now(),
        ..Default::default()
    };
    obj.metadata.insert("version".to_string(), "1".to_string());
    b.objects.insert(key.to_string(), obj);
}

fn make_request(method: Method, path: &str, query: &[(&str, &str)], body: &[u8]) -> AwsRequest {
    let segments: Vec<String> = path
        .trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    let query_params: HashMap<String, String> = query
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
    let raw_query = query
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    // Wire body_stream from the same bytes so streaming-only handlers
    // (put_object, upload_part) can consume it. Buffered handlers
    // (put_object_tagging, put_object_acl, …) read `body` directly
    // and ignore the stream.
    let stream_body =
        fakecloud_core::service::RequestBodyStream::from(Bytes::copy_from_slice(body));
    AwsRequest {
        service: "s3".to_string(),
        action: String::new(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test-req".to_string(),
        headers: HeaderMap::new(),
        query_params,
        body: Bytes::copy_from_slice(body),
        body_stream: parking_lot::Mutex::new(Some(stream_body)),
        path_segments: segments,
        raw_path: path.to_string(),
        raw_query,
        method,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

fn assert_aws_err(
    result: Result<AwsResponse, AwsServiceError>,
    expect_code: &str,
) -> AwsServiceError {
    let err = match result {
        Ok(_) => panic!("expected error, got Ok response"),
        Err(e) => e,
    };
    match &err {
        AwsServiceError::AwsError { code, .. } => {
            assert_eq!(code, expect_code, "wrong error code");
        }
        other => panic!("expected AwsError, got {other:?}"),
    }
    err
}

// ── Tags (service/tags.rs) ───────────────────────────────────────

#[test]
fn get_object_tagging_on_object_returns_xml_tagset() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    seed_object(&svc, "b", "k", b"hello");
    {
        let mut mas = svc.state.write();
        let obj = mas
            .default_mut()
            .buckets
            .get_mut("b")
            .unwrap()
            .objects
            .get_mut("k")
            .unwrap();
        obj.tags.insert("env".to_string(), "prod".to_string());
        obj.tags.insert("team".to_string(), "plat".to_string());
    }

    let req = make_request(Method::GET, "/b/k", &[("tagging", "")], b"");
    let resp = svc
        .get_object_tagging("123456789012", &req, "b", "k")
        .unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<Tag><Key>env</Key><Value>prod</Value></Tag>"));
    assert!(body.contains("<Tag><Key>team</Key><Value>plat</Value></Tag>"));
}

#[test]
fn get_object_tagging_missing_bucket_errors() {
    let svc = make_service();
    let req = make_request(Method::GET, "/nope/k", &[("tagging", "")], b"");
    assert_aws_err(
        svc.get_object_tagging("123456789012", &req, "nope", "k"),
        "NoSuchBucket",
    );
}

#[test]
fn put_object_tagging_rejects_aws_prefixed_key() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    seed_object(&svc, "b", "k", b"x");

    let xml =
        r#"<Tagging><TagSet><Tag><Key>aws:internal</Key><Value>v</Value></Tag></TagSet></Tagging>"#;
    let req = make_request(Method::PUT, "/b/k", &[("tagging", "")], xml.as_bytes());
    assert_aws_err(
        svc.put_object_tagging("123456789012", &req, "b", "k"),
        "InvalidTag",
    );
}

#[test]
fn put_object_tagging_rejects_too_many_tags() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    seed_object(&svc, "b", "k", b"x");

    let mut xml = String::from("<Tagging><TagSet>");
    for i in 0..11 {
        xml.push_str(&format!("<Tag><Key>k{i}</Key><Value>v</Value></Tag>"));
    }
    xml.push_str("</TagSet></Tagging>");
    let req = make_request(Method::PUT, "/b/k", &[("tagging", "")], xml.as_bytes());
    assert_aws_err(
        svc.put_object_tagging("123456789012", &req, "b", "k"),
        "BadRequest",
    );
}

#[test]
fn put_object_tagging_on_missing_object_errors() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let xml = r#"<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>"#;
    let req = make_request(
        Method::PUT,
        "/b/missing",
        &[("tagging", "")],
        xml.as_bytes(),
    );
    assert_aws_err(
        svc.put_object_tagging("123456789012", &req, "b", "missing"),
        "NoSuchKey",
    );
}

#[test]
fn put_object_tagging_replaces_existing_tags() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    seed_object(&svc, "b", "k", b"x");
    {
        let mut mas = svc.state.write();
        let obj = mas
            .default_mut()
            .buckets
            .get_mut("b")
            .unwrap()
            .objects
            .get_mut("k")
            .unwrap();
        obj.tags.insert("old".to_string(), "gone".to_string());
    }

    let xml = r#"<Tagging><TagSet><Tag><Key>new</Key><Value>here</Value></Tag></TagSet></Tagging>"#;
    let req = make_request(Method::PUT, "/b/k", &[("tagging", "")], xml.as_bytes());
    let resp = svc
        .put_object_tagging("123456789012", &req, "b", "k")
        .unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let __mas = svc.state.read();
    let state = __mas.default_ref();
    let tags = &state
        .buckets
        .get("b")
        .unwrap()
        .objects
        .get("k")
        .unwrap()
        .tags;
    assert_eq!(tags.get("new").map(String::as_str), Some("here"));
    assert!(!tags.contains_key("old"));
}

#[test]
fn delete_object_tagging_clears_tags() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    seed_object(&svc, "b", "k", b"x");
    {
        let mut mas = svc.state.write();
        let obj = mas
            .default_mut()
            .buckets
            .get_mut("b")
            .unwrap()
            .objects
            .get_mut("k")
            .unwrap();
        obj.tags.insert("env".to_string(), "prod".to_string());
    }

    let resp = svc.delete_object_tagging("123456789012", "b", "k").unwrap();
    assert_eq!(resp.status, StatusCode::NO_CONTENT);
    let __mas = svc.state.read();
    let state = __mas.default_ref();
    assert!(state
        .buckets
        .get("b")
        .unwrap()
        .objects
        .get("k")
        .unwrap()
        .tags
        .is_empty());
}

#[test]
fn delete_object_tagging_missing_key_errors() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    assert_aws_err(
        svc.delete_object_tagging("123456789012", "b", "gone"),
        "NoSuchKey",
    );
}

// ── Multipart (service/multipart.rs) ─────────────────────────────

fn initiate_mpu(svc: &S3Service, bucket: &str, key: &str) -> String {
    let req = make_request(
        Method::POST,
        &format!("/{bucket}/{key}"),
        &[("uploads", "")],
        b"",
    );
    let resp = svc
        .create_multipart_upload("123456789012", &req, bucket, key)
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    let start = body.find("<UploadId>").unwrap() + "<UploadId>".len();
    let end = body.find("</UploadId>").unwrap();
    body[start..end].to_string()
}

#[test]
fn create_multipart_upload_records_upload_in_state() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let upload_id = initiate_mpu(&svc, "b", "big.bin");
    let __mas = svc.state.read();
    let state = __mas.default_ref();
    assert!(state
        .buckets
        .get("b")
        .unwrap()
        .multipart_uploads
        .contains_key(&upload_id));
}

#[test]
fn create_multipart_upload_rejects_acl_and_grants_combo() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let mut req = make_request(Method::POST, "/b/k", &[("uploads", "")], b"");
    req.headers.insert("x-amz-acl", "private".parse().unwrap());
    req.headers
        .insert("x-amz-grant-read", "id=owner".parse().unwrap());
    assert_aws_err(
        svc.create_multipart_upload("123456789012", &req, "b", "k"),
        "InvalidRequest",
    );
}

#[test]
fn create_multipart_upload_missing_bucket_errors() {
    let svc = make_service();
    let req = make_request(Method::POST, "/ghost/k", &[("uploads", "")], b"");
    assert_aws_err(
        svc.create_multipart_upload("123456789012", &req, "ghost", "k"),
        "NoSuchBucket",
    );
}

#[tokio::test]
async fn upload_part_rejects_invalid_part_number() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let upload_id = initiate_mpu(&svc, "b", "k");

    // part_number < 1 is masked as NoSuchUpload (matching AWS behavior).
    let req = make_request(Method::PUT, "/b/k", &[("partNumber", "0")], b"body");
    assert_aws_err(
        svc.upload_part("123456789012", &req, "b", "k", &upload_id, 0)
            .await,
        "NoSuchUpload",
    );

    // part_number > 10000 returns InvalidArgument.
    let req2 = make_request(Method::PUT, "/b/k", &[("partNumber", "10001")], b"body");
    assert_aws_err(
        svc.upload_part("123456789012", &req2, "b", "k", &upload_id, 10_001)
            .await,
        "InvalidArgument",
    );
}

#[tokio::test]
async fn upload_part_missing_upload_errors() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(Method::PUT, "/b/k", &[("partNumber", "1")], b"body");
    assert_aws_err(
        svc.upload_part("123456789012", &req, "b", "k", "not-an-upload", 1)
            .await,
        "NoSuchUpload",
    );
}

// PutObject's disk write is wrapped in run_blocking_io (bug-audit
// 2026-06-20, 3.2). On a multi-threaded runtime that path uses
// block_in_place, which panics if mis-guarded; this exercises it end-to-end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_object_runs_on_multi_thread_runtime() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let body = b"streamed-under-block-in-place";
    let req = make_request(Method::PUT, "/b/k", &[], body);
    let resp = svc
        .put_object("123456789012", &req, "b", "k")
        .await
        .expect("put_object should succeed on a multi-thread runtime");
    assert_eq!(resp.status, StatusCode::OK);

    let get_req = make_request(Method::GET, "/b/k", &[], b"");
    let got = svc.get_object("123456789012", &get_req, "b", "k").unwrap();
    assert_eq!(got.body.expect_bytes(), body);
}

#[tokio::test]
async fn mpu_full_lifecycle_creates_object() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let upload_id = initiate_mpu(&svc, "b", "k");

    // Single-part upload — CompleteMultipartUpload's MIN_PART_SIZE check
    // only applies to non-last parts, so a single part of any size works.
    let part_body = b"hello";
    let req = make_request(Method::PUT, "/b/k", &[("partNumber", "1")], part_body);
    let resp = svc
        .upload_part("123456789012", &req, "b", "k", &upload_id, 1)
        .await
        .unwrap();
    let etag = resp
        .headers
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let complete_xml = format!(
        r#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"#,
    );
    let complete_req = make_request(
        Method::POST,
        "/b/k",
        &[("uploadId", &upload_id)],
        complete_xml.as_bytes(),
    );
    let resp = svc
        .complete_multipart_upload("123456789012", &complete_req, "b", "k", &upload_id)
        .unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let __mas = svc.state.read();
    let state = __mas.default_ref();
    let bucket = state.buckets.get("b").unwrap();
    let obj = bucket.objects.get("k").expect("object materialized");
    assert_eq!(obj.size, part_body.len() as u64);
    assert!(!bucket.multipart_uploads.contains_key(&upload_id));
}

/// 1.18: a multipart object with an additional checksum must report a
/// COMPOSITE checksum (`base64(HASH(concat(part raw digests)))-N`), not
/// a checksum over the whole reassembled object.
#[tokio::test]
async fn mpu_complete_uses_composite_additional_checksum() {
    let svc = make_service();
    seed_bucket(&svc, "comp");

    // Initiate with a SHA256 additional checksum.
    let init_req = make_request(Method::POST, "/comp/k", &[("uploads", "")], b"");
    let mut init_req = init_req;
    init_req.headers.insert(
        "x-amz-checksum-algorithm",
        http::HeaderValue::from_static("SHA256"),
    );
    let resp = svc
        .create_multipart_upload("123456789012", &init_req, "comp", "k")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    let start = body.find("<UploadId>").unwrap() + "<UploadId>".len();
    let end = body.find("</UploadId>").unwrap();
    let upload_id = body[start..end].to_string();

    let part1 = vec![b'a'; 5 * 1024 * 1024];
    let part2 = b"second-part".to_vec();
    let mut etags = Vec::new();
    for (n, data) in [(1u32, &part1), (2u32, &part2)] {
        let req = make_request(
            Method::PUT,
            "/comp/k",
            &[("partNumber", &n.to_string())],
            data,
        );
        let r = svc
            .upload_part("123456789012", &req, "comp", "k", &upload_id, n as i64)
            .await
            .unwrap();
        etags.push(r.headers.get("etag").unwrap().to_str().unwrap().to_string());
    }

    let complete_xml = format!(
        r#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part><Part><PartNumber>2</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>"#,
        etags[0], etags[1],
    );
    let complete_req = make_request(
        Method::POST,
        "/comp/k",
        &[("uploadId", &upload_id)],
        complete_xml.as_bytes(),
    );
    svc.complete_multipart_upload("123456789012", &complete_req, "comp", "k", &upload_id)
        .unwrap();

    let expected = super::compute_composite_checksum(
        "SHA256",
        &[
            super::compute_checksum_raw("SHA256", &part1),
            super::compute_checksum_raw("SHA256", &part2),
        ],
    )
    .unwrap();
    assert!(
        expected.ends_with("-2"),
        "composite checksum must carry -N suffix"
    );

    let __mas = svc.state.read();
    let state = __mas.default_ref();
    let obj = state.buckets.get("comp").unwrap().objects.get("k").unwrap();
    assert_eq!(
        obj.checksum_value.as_deref(),
        Some(expected.as_str()),
        "stored checksum must be the COMPOSITE checksum-of-checksums, not a whole-object checksum"
    );

    // Sanity: it must NOT equal a single checksum over the whole object.
    let mut whole = part1.clone();
    whole.extend_from_slice(&part2);
    let whole_ck = super::compute_checksum("SHA256", &whole);
    assert_ne!(obj.checksum_value.as_deref(), Some(whole_ck.as_str()));
}

/// 1.18: composite checksum helper round-trips for every supported
/// additional-checksum algorithm.
#[test]
fn compute_composite_checksum_supports_all_algorithms() {
    for algo in ["CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256"] {
        let p1 = super::compute_checksum_raw(algo, b"part-one");
        let p2 = super::compute_checksum_raw(algo, b"part-two");
        assert!(!p1.is_empty(), "{algo} raw digest");
        let composite = super::compute_composite_checksum(algo, &[p1, p2]).unwrap();
        assert!(composite.ends_with("-2"), "{algo} composite suffix");
    }
    assert!(super::compute_composite_checksum("SHA256", &[]).is_none());
    assert!(super::compute_composite_checksum("BOGUS", &[vec![1]]).is_none());
}

/// 1.19: DeleteObjects must reject >1000 keys with a 400 MalformedXML.
#[tokio::test]
async fn delete_objects_rejects_over_1000_keys() {
    let svc = make_service();
    seed_bucket(&svc, "bdel-limit");

    let mut xml = String::from("<Delete>");
    for i in 0..1001 {
        xml.push_str(&format!("<Object><Key>k{i}.txt</Key></Object>"));
    }
    xml.push_str("</Delete>");
    let req = make_request(
        Method::POST,
        "/bdel-limit",
        &[("delete", "")],
        xml.as_bytes(),
    );
    assert_aws_err(
        svc.delete_objects("123456789012", &req, "bdel-limit"),
        "MalformedXML",
    );
}

/// bug-hunt 2026-06-13, finding 4.1: CompleteMultipartUpload assembles
/// parts off-lock from a cloned snapshot. In disk mode a concurrent
/// UploadPart of the same part number rewrites the fixed `part-NNNNN.bin`
/// file *after* the snapshot, so the bytes read no longer match the
/// snapshot's recorded etag. Complete must fail closed (InvalidPart)
/// rather than assemble bytes that don't match the parts the client
/// committed. We reproduce the divergence directly by mutating the stored
/// part body so it no longer hashes to the recorded etag, then completing
/// with that (now-stale) etag — which passes the submitted-etag check but
/// must be caught by the body re-verification.
#[tokio::test]
async fn mpu_complete_fails_closed_when_part_bytes_diverge_from_etag() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let upload_id = initiate_mpu(&svc, "b", "k");

    let req = make_request(
        Method::PUT,
        "/b/k",
        &[("partNumber", "1")],
        b"original-bytes",
    );
    let resp = svc
        .upload_part("123456789012", &req, "b", "k", &upload_id, 1)
        .await
        .unwrap();
    let etag = resp
        .headers
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Simulate a concurrent same-part overwrite that lands after the upload
    // recorded its etag: the stored bytes change, the etag stays stale.
    {
        let mut mas = svc.state.write();
        let part = mas
            .default_mut()
            .buckets
            .get_mut("b")
            .unwrap()
            .multipart_uploads
            .get_mut(&upload_id)
            .unwrap()
            .parts
            .get_mut(&1u32)
            .unwrap();
        part.body =
            fakecloud_persistence::BodyRef::Memory(Bytes::from_static(b"DIFFERENT-tampered-bytes"));
    }

    let complete_xml = format!(
        r#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"#,
    );
    let complete_req = make_request(
        Method::POST,
        "/b/k",
        &[("uploadId", &upload_id)],
        complete_xml.as_bytes(),
    );
    assert_aws_err(
        svc.complete_multipart_upload("123456789012", &complete_req, "b", "k", &upload_id),
        "InvalidPart",
    );
}

#[tokio::test]
async fn mpu_complete_rejects_small_non_last_part() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let upload_id = initiate_mpu(&svc, "b", "k");

    for n in 1..=2 {
        let body = format!("part{n}");
        let req = make_request(
            Method::PUT,
            "/b/k",
            &[("partNumber", &n.to_string())],
            body.as_bytes(),
        );
        svc.upload_part("123456789012", &req, "b", "k", &upload_id, n)
            .await
            .unwrap();
    }

    // Grab the etags from state.
    let (etag1, etag2) = {
        let __mas = svc.state.read();
        let state = __mas.default_ref();
        let parts = &state
            .buckets
            .get("b")
            .unwrap()
            .multipart_uploads
            .get(&upload_id)
            .unwrap()
            .parts;
        (
            parts.get(&1).unwrap().etag.clone(),
            parts.get(&2).unwrap().etag.clone(),
        )
    };

    let complete_xml = format!(
        r#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part><Part><PartNumber>2</PartNumber><ETag>{etag2}</ETag></Part></CompleteMultipartUpload>"#,
    );
    let complete_req = make_request(
        Method::POST,
        "/b/k",
        &[("uploadId", &upload_id)],
        complete_xml.as_bytes(),
    );
    assert_aws_err(
        svc.complete_multipart_upload("123456789012", &complete_req, "b", "k", &upload_id),
        "EntityTooSmall",
    );
}

#[test]
fn abort_multipart_upload_removes_upload() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let upload_id = initiate_mpu(&svc, "b", "k");
    let resp = svc
        .abort_multipart_upload("123456789012", "b", "k", &upload_id)
        .unwrap();
    assert_eq!(resp.status, StatusCode::NO_CONTENT);
    let __mas = svc.state.read();
    let state = __mas.default_ref();
    assert!(!state
        .buckets
        .get("b")
        .unwrap()
        .multipart_uploads
        .contains_key(&upload_id));
}

#[test]
fn abort_multipart_upload_unknown_id_errors() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    assert_aws_err(
        svc.abort_multipart_upload("123456789012", "b", "k", "no-such"),
        "NoSuchUpload",
    );
}

#[test]
fn list_multipart_uploads_includes_all_in_flight() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let u1 = initiate_mpu(&svc, "b", "a");
    let u2 = initiate_mpu(&svc, "b", "b");
    let resp = svc
        .list_multipart_uploads("123456789012", "b", &std::collections::HashMap::new())
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains(&u1));
    assert!(body.contains(&u2));
}

#[tokio::test]
async fn list_parts_after_upload_returns_parts() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let upload_id = initiate_mpu(&svc, "b", "k");
    let req = make_request(Method::PUT, "/b/k", &[("partNumber", "1")], b"data");
    svc.upload_part("123456789012", &req, "b", "k", &upload_id, 1)
        .await
        .unwrap();

    let list_req = make_request(Method::GET, "/b/k", &[("uploadId", &upload_id)], b"");
    let resp = svc
        .list_parts("123456789012", &list_req, "b", "k", &upload_id)
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<PartNumber>1</PartNumber>"));
}

// ── Config (service/config.rs) ───────────────────────────────────

#[test]
fn bucket_encryption_put_get_delete_round_trip() {
    let svc = make_service();
    seed_bucket(&svc, "b");

    let xml = r#"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"#;
    let req = make_request(Method::PUT, "/b", &[("encryption", "")], xml.as_bytes());
    let resp = svc
        .put_bucket_encryption("123456789012", &req, "b")
        .unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Normalized body should include BucketKeyEnabled=false.
    let get = svc.get_bucket_encryption("123456789012", "b").unwrap();
    let body = std::str::from_utf8(get.body.expect_bytes()).unwrap();
    assert!(body.contains("AES256"));
    assert!(body.contains("<BucketKeyEnabled>false</BucketKeyEnabled>"));

    let del = svc.delete_bucket_encryption("123456789012", "b").unwrap();
    assert_eq!(del.status, StatusCode::NO_CONTENT);
    assert_aws_err(
        svc.get_bucket_encryption("123456789012", "b"),
        "ServerSideEncryptionConfigurationNotFoundError",
    );
}

#[test]
fn bucket_policy_rejects_malformed_json() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(Method::PUT, "/b", &[("policy", "")], b"not-json");
    assert_aws_err(
        svc.put_bucket_policy("123456789012", &req, "b"),
        "MalformedPolicy",
    );
}

#[test]
fn bucket_policy_put_get_delete_round_trip() {
    let svc = make_service();
    seed_bucket(&svc, "b");

    let body = br#"{"Version":"2012-10-17","Statement":[]}"#;
    let put_req = make_request(Method::PUT, "/b", &[("policy", "")], body);
    let resp = svc
        .put_bucket_policy("123456789012", &put_req, "b")
        .unwrap();
    assert_eq!(resp.status, StatusCode::NO_CONTENT);

    let get = svc.get_bucket_policy("123456789012", "b").unwrap();
    assert_eq!(get.body.expect_bytes(), body);

    let del = svc.delete_bucket_policy("123456789012", "b").unwrap();
    assert_eq!(del.status, StatusCode::NO_CONTENT);
    assert_aws_err(
        svc.get_bucket_policy("123456789012", "b"),
        "NoSuchBucketPolicy",
    );
}

#[test]
fn bucket_lifecycle_empty_rules_clears_config() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    {
        let mut __mas = svc.state.write();
        let state = __mas.default_mut();
        state.buckets.get_mut("b").unwrap().lifecycle_config = Some("placeholder".to_string());
    }
    let req = make_request(
        Method::PUT,
        "/b",
        &[("lifecycle", "")],
        b"<LifecycleConfiguration></LifecycleConfiguration>",
    );
    svc.put_bucket_lifecycle("123456789012", &req, "b").unwrap();
    let __mas = svc.state.read();
    let state = __mas.default_ref();
    assert!(state.buckets.get("b").unwrap().lifecycle_config.is_none());
}

#[test]
fn bucket_cors_put_get_delete_round_trip() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let xml = br#"<CORSConfiguration><CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>*</AllowedOrigin></CORSRule></CORSConfiguration>"#;
    let req = make_request(Method::PUT, "/b", &[("cors", "")], xml);
    svc.put_bucket_cors("123456789012", &req, "b").unwrap();
    let got = svc.get_bucket_cors("123456789012", "b").unwrap();
    assert!(std::str::from_utf8(got.body.expect_bytes())
        .unwrap()
        .contains("CORSConfiguration"));
    svc.delete_bucket_cors("123456789012", "b").unwrap();
    assert_aws_err(
        svc.get_bucket_cors("123456789012", "b"),
        "NoSuchCORSConfiguration",
    );
}

#[test]
fn bucket_versioning_put_and_get() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(
        Method::PUT,
        "/b",
        &[("versioning", "")],
        b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
    );
    svc.put_bucket_versioning("123456789012", &req, "b")
        .unwrap();

    let __mas = svc.state.read();
    let state = __mas.default_ref();
    assert_eq!(
        state.buckets.get("b").unwrap().versioning.as_deref(),
        Some("Enabled")
    );
    drop(__mas);

    let resp = svc.get_bucket_versioning("123456789012", "b").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<Status>Enabled</Status>"));
}

#[test]
fn bucket_tagging_put_get_delete_round_trip() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(
        Method::PUT,
        "/b",
        &[("tagging", "")],
        br#"<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>"#,
    );
    svc.put_bucket_tagging("123456789012", &req, "b").unwrap();
    let get_req = make_request(Method::GET, "/b", &[("tagging", "")], b"");
    let got = svc
        .get_bucket_tagging("123456789012", &get_req, "b")
        .unwrap();
    assert!(std::str::from_utf8(got.body.expect_bytes())
        .unwrap()
        .contains("<Key>env</Key>"));
    let del_req = make_request(Method::DELETE, "/b", &[("tagging", "")], b"");
    svc.delete_bucket_tagging("123456789012", &del_req, "b")
        .unwrap();
    assert_aws_err(
        svc.get_bucket_tagging("123456789012", &get_req, "b"),
        "NoSuchTagSet",
    );
}

#[test]
fn bucket_accelerate_rejects_invalid_status() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(
        Method::PUT,
        "/b",
        &[("accelerate", "")],
        b"<AccelerateConfiguration><Status>Bogus</Status></AccelerateConfiguration>",
    );
    assert_aws_err(
        svc.put_bucket_accelerate("123456789012", &req, "b"),
        "MalformedXML",
    );
}

#[test]
fn bucket_accelerate_enabled_is_persisted() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(
        Method::PUT,
        "/b",
        &[("accelerate", "")],
        b"<AccelerateConfiguration><Status>Enabled</Status></AccelerateConfiguration>",
    );
    svc.put_bucket_accelerate("123456789012", &req, "b")
        .unwrap();
    let got = svc.get_bucket_accelerate("123456789012", "b").unwrap();
    assert!(std::str::from_utf8(got.body.expect_bytes())
        .unwrap()
        .contains("<Status>Enabled</Status>"));
}

#[test]
fn public_access_block_put_get_delete_round_trip() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let body = br#"<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>true</IgnorePublicAcls><BlockPublicPolicy>true</BlockPublicPolicy><RestrictPublicBuckets>true</RestrictPublicBuckets></PublicAccessBlockConfiguration>"#;
    let req = make_request(Method::PUT, "/b", &[("publicAccessBlock", "")], body);
    svc.put_public_access_block("123456789012", &req, "b")
        .unwrap();
    let got = svc.get_public_access_block("123456789012", "b").unwrap();
    assert!(std::str::from_utf8(got.body.expect_bytes())
        .unwrap()
        .contains("<BlockPublicAcls>true</BlockPublicAcls>"));
    svc.delete_public_access_block("123456789012", "b").unwrap();
    assert_aws_err(
        svc.get_public_access_block("123456789012", "b"),
        "NoSuchPublicAccessBlockConfiguration",
    );
}

#[test]
fn block_public_policy_rejects_wildcard_principal_policy() {
    // PAB.BlockPublicPolicy=true must reject PutBucketPolicy that
    // grants Allow to Principal "*" — the canonical "public read"
    // shape — with AccessDenied.
    let svc = make_service();
    seed_bucket(&svc, "locked");

    let pab_body = br#"<PublicAccessBlockConfiguration><BlockPublicPolicy>true</BlockPublicPolicy></PublicAccessBlockConfiguration>"#;
    let req = make_request(
        Method::PUT,
        "/locked",
        &[("publicAccessBlock", "")],
        pab_body,
    );
    svc.put_public_access_block("123456789012", &req, "locked")
        .unwrap();

    let policy = br#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::locked/*"}]}"#;
    let req = make_request(Method::PUT, "/locked", &[("policy", "")], policy);
    let err = match svc.put_bucket_policy("123456789012", &req, "locked") {
        Err(e) => e,
        Ok(_) => panic!("expected BlockPublicPolicy to reject public policy"),
    };
    assert_eq!(err.status(), StatusCode::FORBIDDEN);
}

#[test]
fn block_public_policy_allows_non_public_policy() {
    // A policy that names a specific AWS principal must not be
    // blocked even when BlockPublicPolicy is set.
    let svc = make_service();
    seed_bucket(&svc, "locked");

    let pab_body = br#"<PublicAccessBlockConfiguration><BlockPublicPolicy>true</BlockPublicPolicy></PublicAccessBlockConfiguration>"#;
    let req = make_request(
        Method::PUT,
        "/locked",
        &[("publicAccessBlock", "")],
        pab_body,
    );
    svc.put_public_access_block("123456789012", &req, "locked")
        .unwrap();

    let policy = br#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789012:role/r"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::locked/*"}]}"#;
    let req = make_request(Method::PUT, "/locked", &[("policy", "")], policy);
    svc.put_bucket_policy("123456789012", &req, "locked")
        .expect("named-principal policy should be allowed");
}

#[test]
fn block_public_acls_rejects_canned_public_read() {
    // PAB.BlockPublicAcls=true must reject `x-amz-acl: public-read`
    // on PutBucketAcl, which adds an AllUsers READ grant.
    let svc = make_service();
    seed_bucket(&svc, "locked");

    let pab_body = br#"<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls></PublicAccessBlockConfiguration>"#;
    let req = make_request(
        Method::PUT,
        "/locked",
        &[("publicAccessBlock", "")],
        pab_body,
    );
    svc.put_public_access_block("123456789012", &req, "locked")
        .unwrap();

    let mut req = make_request(Method::PUT, "/locked", &[("acl", "")], &[]);
    req.headers
        .insert("x-amz-acl", "public-read".parse().unwrap());
    let err = match svc.put_bucket_acl("123456789012", &req, "locked") {
        Err(e) => e,
        Ok(_) => panic!("expected BlockPublicAcls to reject public-read canned ACL"),
    };
    assert_eq!(err.status(), StatusCode::FORBIDDEN);
}

#[test]
fn bucket_website_put_get_delete_round_trip() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(
        Method::PUT,
        "/b",
        &[("website", "")],
        b"<WebsiteConfiguration><IndexDocument><Suffix>index.html</Suffix></IndexDocument></WebsiteConfiguration>",
    );
    svc.put_bucket_website("123456789012", &req, "b").unwrap();
    let got = svc.get_bucket_website("123456789012", "b").unwrap();
    assert!(std::str::from_utf8(got.body.expect_bytes())
        .unwrap()
        .contains("<Suffix>index.html</Suffix>"));
    svc.delete_bucket_website("123456789012", "b").unwrap();
    assert_aws_err(
        svc.get_bucket_website("123456789012", "b"),
        "NoSuchWebsiteConfiguration",
    );
}

#[test]
fn bucket_replication_requires_existing_bucket() {
    let svc = make_service();
    let req = make_request(Method::PUT, "/nope", &[("replication", "")], b"<x/>");
    assert_aws_err(
        svc.put_bucket_replication("123456789012", &req, "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn bucket_ownership_controls_put_get_delete_round_trip() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(
        Method::PUT,
        "/b",
        &[("ownershipControls", "")],
        b"<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
    );
    svc.put_bucket_ownership_controls("123456789012", &req, "b")
        .unwrap();
    let got = svc
        .get_bucket_ownership_controls("123456789012", "b")
        .unwrap();
    assert!(std::str::from_utf8(got.body.expect_bytes())
        .unwrap()
        .contains("BucketOwnerEnforced"));
    svc.delete_bucket_ownership_controls("123456789012", "b")
        .unwrap();
    assert_aws_err(
        svc.get_bucket_ownership_controls("123456789012", "b"),
        "OwnershipControlsNotFoundError",
    );
}

// ── Error branch tests: object operations ──

#[test]
fn get_object_nonexistent_bucket() {
    // GetObject's Smithy `errors` declares NoSuchKey + InvalidObjectState;
    // missing-bucket collapses into NoSuchKey for strict conformance.
    let svc = make_service();
    let req = make_request(Method::GET, "/no-bucket/key", &[], b"");
    assert_aws_err(
        svc.get_object("123456789012", &req, "no-bucket", "key"),
        "NoSuchKey",
    );
}

#[tokio::test]
async fn get_object_blocked_by_pab_ignore_public_acls() {
    let svc = make_service();
    seed_bucket(&svc, "b");

    let mut put_req = make_request(Method::PUT, "/b/k", &[], b"hello");
    put_req
        .headers
        .insert("x-amz-acl", "public-read".parse().unwrap());
    svc.put_object("123456789012", &put_req, "b", "k")
        .await
        .unwrap();

    // Anonymous GET works before PAB is enabled.
    let get_req = make_request(Method::GET, "/b/k", &[], b"");
    svc.get_object("123456789012", &get_req, "b", "k").unwrap();

    // Enable PAB with IgnorePublicAcls=true.
    let pab_xml = br#"<PublicAccessBlockConfiguration><IgnorePublicAcls>true</IgnorePublicAcls></PublicAccessBlockConfiguration>"#;
    let pab_req = make_request(Method::PUT, "/b", &[("publicAccessBlock", "")], pab_xml);
    svc.put_public_access_block("123456789012", &pab_req, "b")
        .unwrap();

    // Anonymous GET now rejected.
    let get_req = make_request(Method::GET, "/b/k", &[], b"");
    assert_aws_err(
        svc.get_object("123456789012", &get_req, "b", "k"),
        "AccessDenied",
    );

    // Authenticated GET still allowed.
    let mut authed = make_request(Method::GET, "/b/k", &[], b"");
    authed.access_key_id = Some("AKIA-TEST".to_string());
    svc.get_object("123456789012", &authed, "b", "k").unwrap();
}

#[test]
fn get_object_nonexistent_key() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(Method::GET, "/b/missing", &[], b"");
    assert_aws_err(
        svc.get_object("123456789012", &req, "b", "missing"),
        "NoSuchKey",
    );
}

#[tokio::test]
async fn put_object_key_too_long() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let long_key = "x".repeat(1025);
    let req = make_request(Method::PUT, &format!("/b/{long_key}"), &[], b"data");
    assert_aws_err(
        svc.put_object("123456789012", &req, "b", &long_key).await,
        "KeyTooLongError",
    );
}

#[tokio::test]
async fn put_object_with_aws_tag_prefix() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let mut req = make_request(Method::PUT, "/b/tagged", &[], b"data");
    req.headers
        .insert("x-amz-tagging", "aws:reserved=nope".parse().unwrap());
    assert_aws_err(
        svc.put_object("123456789012", &req, "b", "tagged").await,
        "InvalidTag",
    );
}

#[tokio::test]
async fn put_object_acl_and_grant_conflict() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let mut req = make_request(Method::PUT, "/b/conflict", &[], b"data");
    req.headers
        .insert("x-amz-acl", "public-read".parse().unwrap());
    req.headers
        .insert("x-amz-grant-read", "id=abc123".parse().unwrap());
    assert_aws_err(
        svc.put_object("123456789012", &req, "b", "conflict").await,
        "InvalidRequest",
    );
}

#[test]
fn head_object_nonexistent_key() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(Method::HEAD, "/b/missing", &[], b"");
    // HeadObject's Smithy model declares only `NotFound` as the error
    // shape (com.amazonaws.s3#HeadObject -> errors: [NotFound]); HEAD
    // responses carry no body so SDKs read the code from
    // `x-amz-error-code`, which must match the declared shape.
    assert_aws_err(
        svc.head_object("123456789012", &req, "b", "missing"),
        "NotFound",
    );
}

#[test]
fn head_object_nonexistent_bucket() {
    let svc = make_service();
    let req = make_request(Method::HEAD, "/nope/key", &[], b"");
    assert_aws_err(
        svc.head_object("123456789012", &req, "nope", "key"),
        "NotFound",
    );
}

#[test]
fn delete_object_nonexistent_bucket() {
    let svc = make_service();
    let req = make_request(Method::DELETE, "/nope/key", &[], b"");
    assert_aws_err(
        svc.delete_object("123456789012", &req, "nope", "key"),
        "NoSuchBucket",
    );
}

#[test]
fn copy_object_source_not_found() {
    let svc = make_service();
    seed_bucket(&svc, "src-b");
    seed_bucket(&svc, "dst-b");
    let mut req = make_request(Method::PUT, "/dst-b/copied", &[], b"");
    req.headers
        .insert("x-amz-copy-source", "src-b/nonexistent".parse().unwrap());
    assert_aws_err(
        svc.copy_object("123456789012", &req, "dst-b", "copied"),
        "NoSuchKey",
    );
}

#[test]
fn copy_object_source_bucket_not_found() {
    let svc = make_service();
    seed_bucket(&svc, "dst-b2");
    let mut req = make_request(Method::PUT, "/dst-b2/copied", &[], b"");
    req.headers
        .insert("x-amz-copy-source", "nope-bucket/key".parse().unwrap());
    assert_aws_err(
        svc.copy_object("123456789012", &req, "dst-b2", "copied"),
        "NoSuchBucket",
    );
}

#[test]
fn list_objects_v2_nonexistent_bucket() {
    let svc = make_service();
    let req = make_request(Method::GET, "/nope", &[("list-type", "2")], b"");
    assert_aws_err(
        svc.list_objects_v2("123456789012", &req, "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn list_objects_v2_empty_continuation_token() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(
        Method::GET,
        "/b",
        &[("list-type", "2"), ("continuation-token", "")],
        b"",
    );
    assert_aws_err(
        svc.list_objects_v2("123456789012", &req, "b"),
        "InvalidArgument",
    );
}

#[test]
fn list_objects_v1_nonexistent_bucket() {
    let svc = make_service();
    let req = make_request(Method::GET, "/nope", &[], b"");
    assert_aws_err(
        svc.list_objects_v1("123456789012", &req, "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn list_object_versions_nonexistent_bucket() {
    let svc = make_service();
    let req = make_request(Method::GET, "/nope", &[("versions", "")], b"");
    assert_aws_err(
        svc.list_object_versions("123456789012", &req, "nope"),
        "NoSuchBucket",
    );
}

// ── Error branch tests: multipart operations ──

#[test]
fn create_multipart_nonexistent_bucket() {
    let svc = make_service();
    let req = make_request(Method::POST, "/nope/key", &[("uploads", "")], b"");
    assert_aws_err(
        svc.create_multipart_upload("123456789012", &req, "nope", "key"),
        "NoSuchBucket",
    );
}

#[tokio::test]
async fn upload_part_nonexistent_upload() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(
        Method::PUT,
        "/b/key",
        &[("uploadId", "bogus"), ("partNumber", "1")],
        b"data",
    );
    assert_aws_err(
        svc.upload_part("123456789012", &req, "b", "key", "bogus", 1)
            .await,
        "NoSuchUpload",
    );
}

#[test]
fn complete_multipart_nonexistent_upload() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(
        Method::POST,
        "/b/key",
        &[("uploadId", "bogus")],
        b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"abc\"</ETag></Part></CompleteMultipartUpload>",
    );
    assert_aws_err(
        svc.complete_multipart_upload("123456789012", &req, "b", "key", "bogus"),
        "NoSuchUpload",
    );
}

#[test]
fn abort_multipart_nonexistent_upload() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    assert_aws_err(
        svc.abort_multipart_upload("123456789012", "b", "key", "bogus"),
        "NoSuchUpload",
    );
}

#[test]
fn list_parts_nonexistent_upload() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(Method::GET, "/b/key", &[("uploadId", "bogus")], b"");
    assert_aws_err(
        svc.list_parts("123456789012", &req, "b", "key", "bogus"),
        "NoSuchUpload",
    );
}

// ── Error branch tests: config operations ──

#[test]
fn get_bucket_acl_nonexistent() {
    let svc = make_service();
    let req = make_request(Method::GET, "/nope", &[("acl", "")], b"");
    assert_aws_err(
        svc.get_bucket_acl("123456789012", &req, "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn get_bucket_versioning_nonexistent() {
    let svc = make_service();
    assert_aws_err(
        svc.get_bucket_versioning("123456789012", "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn put_bucket_versioning_nonexistent() {
    let svc = make_service();
    let req = make_request(
        Method::PUT,
        "/nope",
        &[("versioning", "")],
        b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
    );
    assert_aws_err(
        svc.put_bucket_versioning("123456789012", &req, "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn get_bucket_location_nonexistent() {
    let svc = make_service();
    assert_aws_err(
        svc.get_bucket_location("123456789012", "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn get_bucket_lifecycle_nonexistent() {
    let svc = make_service();
    assert_aws_err(
        svc.get_bucket_lifecycle("123456789012", "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn get_bucket_notification_nonexistent() {
    let svc = make_service();
    assert_aws_err(
        svc.get_bucket_notification("123456789012", "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn get_bucket_encryption_nonexistent() {
    let svc = make_service();
    assert_aws_err(
        svc.get_bucket_encryption("123456789012", "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn get_bucket_logging_nonexistent() {
    let svc = make_service();
    assert_aws_err(
        svc.get_bucket_logging("123456789012", "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn get_object_lock_nonexistent() {
    let svc = make_service();
    assert_aws_err(
        svc.get_object_lock_configuration("123456789012", "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn get_object_attributes_nonexistent_key() {
    let svc = make_service();
    seed_bucket(&svc, "b");
    let req = make_request(Method::GET, "/b/missing", &[("attributes", "")], b"");
    assert_aws_err(
        svc.get_object_attributes("123456789012", &req, "b", "missing"),
        "NoSuchKey",
    );
}

#[test]
fn get_object_attributes_nonexistent_bucket() {
    // GetObjectAttributes only declares NoSuchKey in its Smithy `errors`.
    let svc = make_service();
    let req = make_request(Method::GET, "/nope/key", &[("attributes", "")], b"");
    assert_aws_err(
        svc.get_object_attributes("123456789012", &req, "nope", "key"),
        "NoSuchKey",
    );
}

#[test]
fn restore_object_nonexistent_bucket() {
    let svc = make_service();
    let req = make_request(
        Method::POST,
        "/nope/key",
        &[("restore", "")],
        b"<RestoreRequest><Days>1</Days></RestoreRequest>",
    );
    assert_aws_err(
        svc.restore_object("123456789012", &req, "nope", "key"),
        "NoSuchBucket",
    );
}

#[test]
fn get_public_access_block_nonexistent() {
    let svc = make_service();
    assert_aws_err(
        svc.get_public_access_block("123456789012", "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn get_bucket_policy_nonexistent() {
    let svc = make_service();
    assert_aws_err(
        svc.get_bucket_policy("123456789012", "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn get_bucket_cors_nonexistent() {
    let svc = make_service();
    assert_aws_err(svc.get_bucket_cors("123456789012", "nope"), "NoSuchBucket");
}

#[test]
fn get_bucket_tagging_nonexistent() {
    let svc = make_service();
    let req = make_request(Method::GET, "/nope", &[("tagging", "")], b"");
    assert_aws_err(
        svc.get_bucket_tagging("123456789012", &req, "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn get_bucket_website_nonexistent() {
    let svc = make_service();
    assert_aws_err(
        svc.get_bucket_website("123456789012", "nope"),
        "NoSuchBucket",
    );
}

// ── Object lock (lock.rs - 0% coverage) ──

#[test]
fn put_and_get_object_retention() {
    let svc = make_service();
    seed_bucket(&svc, "lock-b");
    seed_object(&svc, "lock-b", "retained.txt", b"data");

    let body = b"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2030-01-01T00:00:00Z</RetainUntilDate></Retention>";
    let req = make_request(
        Method::PUT,
        "/lock-b/retained.txt",
        &[("retention", "")],
        body,
    );
    svc.put_object_retention("123456789012", &req, "lock-b", "retained.txt")
        .unwrap();

    let req = make_request(
        Method::GET,
        "/lock-b/retained.txt",
        &[("retention", "")],
        b"",
    );
    let resp = svc
        .get_object_retention("123456789012", &req, "lock-b", "retained.txt")
        .unwrap();
    let body_str = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body_str.contains("GOVERNANCE"));
}

#[test]
fn get_object_retention_nonexistent_bucket() {
    let svc = make_service();
    let req = make_request(Method::GET, "/nope/key", &[("retention", "")], b"");
    assert_aws_err(
        svc.get_object_retention("123456789012", &req, "nope", "key"),
        "NoSuchBucket",
    );
}

#[test]
fn get_object_retention_nonexistent_key() {
    let svc = make_service();
    seed_bucket(&svc, "lock-b2");
    let req = make_request(Method::GET, "/lock-b2/missing", &[("retention", "")], b"");
    assert_aws_err(
        svc.get_object_retention("123456789012", &req, "lock-b2", "missing"),
        "NoSuchKey",
    );
}

#[test]
fn put_and_get_object_legal_hold() {
    let svc = make_service();
    seed_bucket(&svc, "hold-b");
    seed_object(&svc, "hold-b", "held.txt", b"data");

    let body = b"<LegalHold><Status>ON</Status></LegalHold>";
    let req = make_request(Method::PUT, "/hold-b/held.txt", &[("legal-hold", "")], body);
    svc.put_object_legal_hold("123456789012", &req, "hold-b", "held.txt")
        .unwrap();

    let req = make_request(Method::GET, "/hold-b/held.txt", &[("legal-hold", "")], b"");
    let resp = svc
        .get_object_legal_hold("123456789012", &req, "hold-b", "held.txt")
        .unwrap();
    let body_str = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body_str.contains("ON"));
}

#[test]
fn get_object_legal_hold_nonexistent() {
    let svc = make_service();
    let req = make_request(Method::GET, "/nope/key", &[("legal-hold", "")], b"");
    assert_aws_err(
        svc.get_object_legal_hold("123456789012", &req, "nope", "key"),
        "NoSuchBucket",
    );
}

// ── Object ACL (acl.rs - 0% coverage) ──

#[test]
fn get_object_acl_default() {
    let svc = make_service();
    seed_bucket(&svc, "acl-b");
    seed_object(&svc, "acl-b", "file.txt", b"data");

    let req = make_request(Method::GET, "/acl-b/file.txt", &[("acl", "")], b"");
    let resp = svc
        .get_object_acl("123456789012", &req, "acl-b", "file.txt")
        .unwrap();
    let body_str = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body_str.contains("AccessControlPolicy"));
}

#[test]
fn get_object_acl_nonexistent_bucket() {
    // GetObjectAcl only declares NoSuchKey in its Smithy `errors`.
    let svc = make_service();
    let req = make_request(Method::GET, "/nope/key", &[("acl", "")], b"");
    assert_aws_err(
        svc.get_object_acl("123456789012", &req, "nope", "key"),
        "NoSuchKey",
    );
}

#[test]
fn get_object_acl_nonexistent_key() {
    let svc = make_service();
    seed_bucket(&svc, "acl-b2");
    let req = make_request(Method::GET, "/acl-b2/missing", &[("acl", "")], b"");
    assert_aws_err(
        svc.get_object_acl("123456789012", &req, "acl-b2", "missing"),
        "NoSuchKey",
    );
}

#[test]
fn put_object_acl() {
    let svc = make_service();
    seed_bucket(&svc, "acl-put-b");
    seed_object(&svc, "acl-put-b", "file.txt", b"data");

    let acl_xml = b"<AccessControlPolicy><Owner><ID>owner</ID></Owner><AccessControlList><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>owner</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>";
    let req = make_request(Method::PUT, "/acl-put-b/file.txt", &[("acl", "")], acl_xml);
    svc.put_object_acl("123456789012", &req, "acl-put-b", "file.txt")
        .unwrap();
}

// ── Happy-path handler tests (objects.rs coverage) ──

#[tokio::test]
async fn put_object_via_handler_and_get_back() {
    let svc = make_service();
    seed_bucket(&svc, "hp");

    // PUT through handler (not seed_object)
    let req = make_request(Method::PUT, "/hp/test.txt", &[], b"hello world");
    let resp = svc
        .put_object("123456789012", &req, "hp", "test.txt")
        .await
        .unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // GET back
    let req = make_request(Method::GET, "/hp/test.txt", &[], b"");
    let resp = svc
        .get_object("123456789012", &req, "hp", "test.txt")
        .unwrap();
    assert_eq!(resp.body.expect_bytes(), b"hello world");
}

#[tokio::test]
async fn put_object_with_content_type() {
    let svc = make_service();
    seed_bucket(&svc, "ct");

    let mut req = make_request(Method::PUT, "/ct/doc.json", &[], b"{\"key\":\"val\"}");
    req.headers
        .insert("content-type", "application/json".parse().unwrap());
    svc.put_object("123456789012", &req, "ct", "doc.json")
        .await
        .unwrap();

    let req = make_request(Method::GET, "/ct/doc.json", &[], b"");
    let resp = svc
        .get_object("123456789012", &req, "ct", "doc.json")
        .unwrap();
    assert_eq!(resp.content_type, "application/json");
}

#[tokio::test]
async fn put_object_with_metadata() {
    let svc = make_service();
    seed_bucket(&svc, "meta");

    let mut req = make_request(Method::PUT, "/meta/obj", &[], b"data");
    req.headers
        .insert("x-amz-meta-color", "blue".parse().unwrap());
    req.headers
        .insert("x-amz-meta-size", "large".parse().unwrap());
    svc.put_object("123456789012", &req, "meta", "obj")
        .await
        .unwrap();

    let req = make_request(Method::HEAD, "/meta/obj", &[], b"");
    let resp = svc
        .head_object("123456789012", &req, "meta", "obj")
        .unwrap();
    assert!(resp
        .headers
        .get("x-amz-meta-color")
        .is_some_and(|v| v == "blue"));
}

#[tokio::test]
async fn put_object_returns_etag() {
    let svc = make_service();
    seed_bucket(&svc, "etag");

    let req = make_request(Method::PUT, "/etag/f.txt", &[], b"content");
    let resp = svc
        .put_object("123456789012", &req, "etag", "f.txt")
        .await
        .unwrap();
    assert!(resp.headers.get("etag").is_some());
}

#[tokio::test]
async fn head_object_returns_headers() {
    let svc = make_service();
    seed_bucket(&svc, "head");

    let req = make_request(Method::PUT, "/head/f.txt", &[], b"12345");
    svc.put_object("123456789012", &req, "head", "f.txt")
        .await
        .unwrap();

    let req = make_request(Method::HEAD, "/head/f.txt", &[], b"");
    let resp = svc
        .head_object("123456789012", &req, "head", "f.txt")
        .unwrap();
    assert_eq!(
        resp.headers
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap(),
        "5"
    );
}

#[tokio::test]
async fn head_object_defaults_sse_and_gates_checksum_on_mode() {
    let svc = make_service();
    seed_bucket(&svc, "sse");

    // PUT with a CRC32 checksum requested (the Go SDK does this automatically).
    let mut put = make_request(Method::PUT, "/sse/f.txt", &[], b"12345");
    put.headers.insert(
        "x-amz-checksum-algorithm",
        http::HeaderValue::from_static("CRC32"),
    );
    svc.put_object("123456789012", &put, "sse", "f.txt")
        .await
        .unwrap();

    // HEAD without checksum mode: SSE defaults to AES256, no checksum header.
    let req = make_request(Method::HEAD, "/sse/f.txt", &[], b"");
    let resp = svc
        .head_object("123456789012", &req, "sse", "f.txt")
        .unwrap();
    assert_eq!(
        resp.headers.get("x-amz-server-side-encryption").unwrap(),
        "AES256"
    );
    assert!(
        resp.headers.get("x-amz-checksum-crc32").is_none(),
        "checksum must not be returned without x-amz-checksum-mode: ENABLED"
    );

    // HEAD with checksum mode enabled: the stored checksum is returned.
    let mut req = make_request(Method::HEAD, "/sse/f.txt", &[], b"");
    req.headers.insert(
        "x-amz-checksum-mode",
        http::HeaderValue::from_static("ENABLED"),
    );
    let resp = svc
        .head_object("123456789012", &req, "sse", "f.txt")
        .unwrap();
    assert!(resp.headers.get("x-amz-checksum-crc32").is_some());
}

#[tokio::test]
async fn delete_object_via_handler() {
    let svc = make_service();
    seed_bucket(&svc, "del");

    let req = make_request(Method::PUT, "/del/rm.txt", &[], b"bye");
    svc.put_object("123456789012", &req, "del", "rm.txt")
        .await
        .unwrap();

    let req = make_request(Method::DELETE, "/del/rm.txt", &[], b"");
    svc.delete_object("123456789012", &req, "del", "rm.txt")
        .unwrap();

    let req = make_request(Method::GET, "/del/rm.txt", &[], b"");
    assert_aws_err(
        svc.get_object("123456789012", &req, "del", "rm.txt"),
        "NoSuchKey",
    );
}

#[tokio::test]
async fn copy_object_via_handler() {
    let svc = make_service();
    seed_bucket(&svc, "cpsrc");
    seed_bucket(&svc, "cpdst");

    let req = make_request(Method::PUT, "/cpsrc/orig.txt", &[], b"original");
    svc.put_object("123456789012", &req, "cpsrc", "orig.txt")
        .await
        .unwrap();

    let mut req = make_request(Method::PUT, "/cpdst/copy.txt", &[], b"");
    req.headers
        .insert("x-amz-copy-source", "cpsrc/orig.txt".parse().unwrap());
    svc.copy_object("123456789012", &req, "cpdst", "copy.txt")
        .unwrap();

    let req = make_request(Method::GET, "/cpdst/copy.txt", &[], b"");
    let resp = svc
        .get_object("123456789012", &req, "cpdst", "copy.txt")
        .unwrap();
    assert_eq!(resp.body.expect_bytes(), b"original");
}

#[tokio::test]
async fn copy_object_within_same_bucket() {
    let svc = make_service();
    seed_bucket(&svc, "same");

    let req = make_request(Method::PUT, "/same/a.txt", &[], b"aaa");
    svc.put_object("123456789012", &req, "same", "a.txt")
        .await
        .unwrap();

    let mut req = make_request(Method::PUT, "/same/b.txt", &[], b"");
    req.headers
        .insert("x-amz-copy-source", "same/a.txt".parse().unwrap());
    svc.copy_object("123456789012", &req, "same", "b.txt")
        .unwrap();

    let req = make_request(Method::GET, "/same/b.txt", &[], b"");
    let resp = svc
        .get_object("123456789012", &req, "same", "b.txt")
        .unwrap();
    assert_eq!(resp.body.expect_bytes(), b"aaa");
}

#[tokio::test]
async fn list_objects_v2_via_handler() {
    let svc = make_service();
    seed_bucket(&svc, "lsv2");

    for i in 0..3 {
        let key = format!("file{i}.txt");
        let req = make_request(Method::PUT, &format!("/lsv2/{key}"), &[], b"data");
        svc.put_object("123456789012", &req, "lsv2", &key)
            .await
            .unwrap();
    }

    let req = make_request(Method::GET, "/lsv2", &[("list-type", "2")], b"");
    let resp = svc.list_objects_v2("123456789012", &req, "lsv2").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<KeyCount>3</KeyCount>"));
}

#[tokio::test]
async fn list_objects_v2_with_prefix() {
    let svc = make_service();
    seed_bucket(&svc, "pfx");

    for key in &["docs/a.txt", "docs/b.txt", "images/c.png"] {
        let req = make_request(Method::PUT, &format!("/pfx/{key}"), &[], b"x");
        svc.put_object("123456789012", &req, "pfx", key)
            .await
            .unwrap();
    }

    let req = make_request(
        Method::GET,
        "/pfx",
        &[("list-type", "2"), ("prefix", "docs/")],
        b"",
    );
    let resp = svc.list_objects_v2("123456789012", &req, "pfx").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<KeyCount>2</KeyCount>"));
}

#[tokio::test]
async fn list_objects_v2_with_delimiter() {
    let svc = make_service();
    seed_bucket(&svc, "dlm");

    for key in &["a/1.txt", "a/2.txt", "b/3.txt", "root.txt"] {
        let req = make_request(Method::PUT, &format!("/dlm/{key}"), &[], b"x");
        svc.put_object("123456789012", &req, "dlm", key)
            .await
            .unwrap();
    }

    let req = make_request(
        Method::GET,
        "/dlm",
        &[("list-type", "2"), ("delimiter", "/")],
        b"",
    );
    let resp = svc.list_objects_v2("123456789012", &req, "dlm").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    // Should have common prefixes a/ and b/
    assert!(body.contains("<CommonPrefixes>"));
    // root.txt should be in contents
    assert!(body.contains("root.txt"));
}

#[tokio::test]
async fn list_objects_v1_via_handler() {
    let svc = make_service();
    seed_bucket(&svc, "lsv1");

    let req = make_request(Method::PUT, "/lsv1/test.txt", &[], b"data");
    svc.put_object("123456789012", &req, "lsv1", "test.txt")
        .await
        .unwrap();

    let req = make_request(Method::GET, "/lsv1", &[], b"");
    let resp = svc.list_objects_v1("123456789012", &req, "lsv1").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<Key>test.txt</Key>"));
}

#[tokio::test]
async fn delete_objects_batch() {
    let svc = make_service();
    seed_bucket(&svc, "bdel");

    for i in 0..3 {
        let key = format!("d{i}.txt");
        let req = make_request(Method::PUT, &format!("/bdel/{key}"), &[], b"x");
        svc.put_object("123456789012", &req, "bdel", &key)
            .await
            .unwrap();
    }

    let delete_xml =
        b"<Delete><Object><Key>d0.txt</Key></Object><Object><Key>d1.txt</Key></Object></Delete>";
    let req = make_request(Method::POST, "/bdel", &[("delete", "")], delete_xml);
    let resp = svc.delete_objects("123456789012", &req, "bdel").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<Deleted>"));

    // d2.txt should still exist
    let req = make_request(Method::GET, "/bdel/d2.txt", &[], b"");
    svc.get_object("123456789012", &req, "bdel", "d2.txt")
        .unwrap();
}

#[tokio::test]
async fn put_object_overwrites_existing() {
    let svc = make_service();
    seed_bucket(&svc, "ow");

    let req = make_request(Method::PUT, "/ow/f.txt", &[], b"version1");
    svc.put_object("123456789012", &req, "ow", "f.txt")
        .await
        .unwrap();

    let req = make_request(Method::PUT, "/ow/f.txt", &[], b"version2");
    svc.put_object("123456789012", &req, "ow", "f.txt")
        .await
        .unwrap();

    let req = make_request(Method::GET, "/ow/f.txt", &[], b"");
    let resp = svc.get_object("123456789012", &req, "ow", "f.txt").unwrap();
    assert_eq!(resp.body.expect_bytes(), b"version2");
}

#[tokio::test]
async fn get_object_attributes_via_handler() {
    let svc = make_service();
    seed_bucket(&svc, "attr");

    let req = make_request(Method::PUT, "/attr/f.txt", &[], b"content");
    svc.put_object("123456789012", &req, "attr", "f.txt")
        .await
        .unwrap();

    let mut req = make_request(Method::GET, "/attr/f.txt", &[], b"");
    req.headers.insert(
        "x-amz-object-attributes",
        "ETag,ObjectSize".parse().unwrap(),
    );
    let resp = svc
        .get_object_attributes("123456789012", &req, "attr", "f.txt")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<ObjectSize>"));
}

// ── Multipart upload happy path ──

#[tokio::test]
async fn multipart_upload_lifecycle() {
    let svc = make_service();
    seed_bucket(&svc, "mp");

    // Create
    let req = make_request(Method::POST, "/mp/big.bin", &[("uploads", "")], b"");
    let resp = svc
        .create_multipart_upload("123456789012", &req, "mp", "big.bin")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    let uid_start = body.find("<UploadId>").unwrap() + 10;
    let uid_end = body.find("</UploadId>").unwrap();
    let upload_id = &body[uid_start..uid_end];

    // Upload part 1 (>5MB to pass minimum size check)
    let big_data = vec![b'A'; 5 * 1024 * 1024 + 1];
    let req = make_request(Method::PUT, "/mp/big.bin", &[], &big_data);
    let resp = svc
        .upload_part("123456789012", &req, "mp", "big.bin", upload_id, 1)
        .await
        .unwrap();
    let etag1 = resp
        .headers
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Upload part 2 (last part can be any size)
    let req = make_request(Method::PUT, "/mp/big.bin", &[], b"part2-data");
    let resp = svc
        .upload_part("123456789012", &req, "mp", "big.bin", upload_id, 2)
        .await
        .unwrap();
    let etag2 = resp
        .headers
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // List parts
    let req = make_request(Method::GET, "/mp/big.bin", &[], b"");
    let resp = svc
        .list_parts("123456789012", &req, "mp", "big.bin", upload_id)
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<Part>"));

    // Complete
    let complete_xml = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part><Part><PartNumber>2</PartNumber><ETag>{etag2}</ETag></Part></CompleteMultipartUpload>"
    );
    let req = make_request(Method::POST, "/mp/big.bin", &[], complete_xml.as_bytes());
    svc.complete_multipart_upload("123456789012", &req, "mp", "big.bin", upload_id)
        .unwrap();

    // Verify object exists
    let req = make_request(Method::GET, "/mp/big.bin", &[], b"");
    let resp = svc
        .get_object("123456789012", &req, "mp", "big.bin")
        .unwrap();
    let body = resp.body.expect_bytes();
    // First part is 5MB+1 of 'A', second is "part2-data"
    assert!(body.len() > 5 * 1024 * 1024);
}

#[test]
fn multipart_upload_abort() {
    let svc = make_service();
    seed_bucket(&svc, "mpa");

    let req = make_request(Method::POST, "/mpa/abort.bin", &[("uploads", "")], b"");
    let resp = svc
        .create_multipart_upload("123456789012", &req, "mpa", "abort.bin")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    let uid_start = body.find("<UploadId>").unwrap() + 10;
    let uid_end = body.find("</UploadId>").unwrap();
    let upload_id = body[uid_start..uid_end].to_string();

    svc.abort_multipart_upload("123456789012", "mpa", "abort.bin", &upload_id)
        .unwrap();

    // Upload should be gone
    let req = make_request(Method::GET, "/mpa/abort.bin", &[], b"");
    assert_aws_err(
        svc.list_parts("123456789012", &req, "mpa", "abort.bin", &upload_id),
        "NoSuchUpload",
    );
}

#[test]
fn list_multipart_uploads() {
    let svc = make_service();
    seed_bucket(&svc, "mpl");

    let req = make_request(Method::POST, "/mpl/f1.bin", &[("uploads", "")], b"");
    svc.create_multipart_upload("123456789012", &req, "mpl", "f1.bin")
        .unwrap();

    let resp = svc
        .list_multipart_uploads("123456789012", "mpl", &std::collections::HashMap::new())
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<Upload>"));
    assert!(body.contains("f1.bin"));
}

// ── Config handler happy paths ──

#[test]
fn put_and_get_bucket_versioning() {
    let svc = make_service();
    seed_bucket(&svc, "ver");

    let req = make_request(
        Method::PUT,
        "/ver",
        &[("versioning", "")],
        b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
    );
    svc.put_bucket_versioning("123456789012", &req, "ver")
        .unwrap();

    let resp = svc.get_bucket_versioning("123456789012", "ver").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("Enabled"));
}

#[test]
fn put_and_get_bucket_lifecycle() {
    let svc = make_service();
    seed_bucket(&svc, "lc");

    let xml = b"<LifecycleConfiguration><Rule><ID>expire</ID><Filter><Prefix></Prefix></Filter><Status>Enabled</Status><Expiration><Days>30</Days></Expiration></Rule></LifecycleConfiguration>";
    let req = make_request(Method::PUT, "/lc", &[("lifecycle", "")], xml);
    svc.put_bucket_lifecycle("123456789012", &req, "lc")
        .unwrap();

    let resp = svc.get_bucket_lifecycle("123456789012", "lc").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<Rule>"));
}

#[test]
fn put_and_get_bucket_notification() {
    let svc = make_service();
    seed_bucket(&svc, "notif");

    let xml = b"<NotificationConfiguration></NotificationConfiguration>";
    let req = make_request(Method::PUT, "/notif", &[("notification", "")], xml);
    svc.put_bucket_notification("123456789012", &req, "notif")
        .unwrap();

    let resp = svc
        .get_bucket_notification("123456789012", "notif")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("NotificationConfiguration"));
}

#[test]
fn put_and_get_and_delete_bucket_encryption() {
    let svc = make_service();
    seed_bucket(&svc, "enc");

    let xml = b"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>";
    let req = make_request(Method::PUT, "/enc", &[("encryption", "")], xml);
    svc.put_bucket_encryption("123456789012", &req, "enc")
        .unwrap();

    let resp = svc.get_bucket_encryption("123456789012", "enc").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("AES256"));

    svc.delete_bucket_encryption("123456789012", "enc").unwrap();
}

#[test]
fn bucket_logging_put_and_get() {
    let svc = make_service();
    seed_bucket(&svc, "logging-b");

    let xml = b"<BucketLoggingStatus><LoggingEnabled><TargetBucket>logging-b</TargetBucket><TargetPrefix>logs/</TargetPrefix></LoggingEnabled></BucketLoggingStatus>";
    let req = make_request(Method::PUT, "/logging-b", &[("logging", "")], xml);
    svc.put_bucket_logging("123456789012", &req, "logging-b")
        .unwrap();

    let resp = svc.get_bucket_logging("123456789012", "logging-b").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("LoggingEnabled"));
}

// ── Versioned object operations ──

#[tokio::test]
async fn versioned_put_and_get() {
    let svc = make_service();
    seed_bucket(&svc, "vb");

    // Enable versioning
    let req = make_request(
        Method::PUT,
        "/vb",
        &[("versioning", "")],
        b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
    );
    svc.put_bucket_versioning("123456789012", &req, "vb")
        .unwrap();

    // Put v1
    let req = make_request(Method::PUT, "/vb/key", &[], b"version1");
    let resp = svc
        .put_object("123456789012", &req, "vb", "key")
        .await
        .unwrap();
    let v1 = resp
        .headers
        .get("x-amz-version-id")
        .map(|h| h.to_str().unwrap().to_string());
    assert!(v1.is_some());

    // Put v2
    let req = make_request(Method::PUT, "/vb/key", &[], b"version2");
    let resp = svc
        .put_object("123456789012", &req, "vb", "key")
        .await
        .unwrap();
    let _v2 = resp
        .headers
        .get("x-amz-version-id")
        .map(|h| h.to_str().unwrap().to_string());

    // Get latest
    let req = make_request(Method::GET, "/vb/key", &[], b"");
    let resp = svc.get_object("123456789012", &req, "vb", "key").unwrap();
    assert_eq!(resp.body.expect_bytes(), b"version2");

    // List versions
    let req = make_request(Method::GET, "/vb", &[("versions", "")], b"");
    let resp = svc
        .list_object_versions("123456789012", &req, "vb")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<Version>"));
}

// ── Conditional GET ──

#[tokio::test]
async fn get_object_if_match_succeeds() {
    let svc = make_service();
    seed_bucket(&svc, "cond");

    let req = make_request(Method::PUT, "/cond/f.txt", &[], b"data");
    let resp = svc
        .put_object("123456789012", &req, "cond", "f.txt")
        .await
        .unwrap();
    let etag = resp
        .headers
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let mut req = make_request(Method::GET, "/cond/f.txt", &[], b"");
    req.headers.insert("if-match", etag.parse().unwrap());
    let resp = svc
        .get_object("123456789012", &req, "cond", "f.txt")
        .unwrap();
    assert_eq!(resp.body.expect_bytes(), b"data");
}

#[tokio::test]
async fn get_object_if_match_fails() {
    let svc = make_service();
    seed_bucket(&svc, "cond2");

    let req = make_request(Method::PUT, "/cond2/f.txt", &[], b"data");
    svc.put_object("123456789012", &req, "cond2", "f.txt")
        .await
        .unwrap();

    let mut req = make_request(Method::GET, "/cond2/f.txt", &[], b"");
    req.headers
        .insert("if-match", "\"wrong-etag\"".parse().unwrap());
    assert_aws_err(
        svc.get_object("123456789012", &req, "cond2", "f.txt"),
        "PreconditionFailed",
    );
}

#[tokio::test]
async fn get_object_if_none_match_returns_304() {
    let svc = make_service();
    seed_bucket(&svc, "cond3");

    let req = make_request(Method::PUT, "/cond3/f.txt", &[], b"data");
    let resp = svc
        .put_object("123456789012", &req, "cond3", "f.txt")
        .await
        .unwrap();
    let etag = resp
        .headers
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let mut req = make_request(Method::GET, "/cond3/f.txt", &[], b"");
    req.headers.insert("if-none-match", etag.parse().unwrap());
    let err = svc.get_object("123456789012", &req, "cond3", "f.txt");
    // Should return PreconditionFailed or 304 Not Modified
    assert!(err.is_err());
}

// ── Put with if-none-match (conditional put) ──

#[tokio::test]
async fn put_object_if_none_match_prevents_overwrite() {
    let svc = make_service();
    seed_bucket(&svc, "cnm");

    let req = make_request(Method::PUT, "/cnm/f.txt", &[], b"first");
    svc.put_object("123456789012", &req, "cnm", "f.txt")
        .await
        .unwrap();

    // Try to put again with if-none-match: *
    let mut req = make_request(Method::PUT, "/cnm/f.txt", &[], b"second");
    req.headers.insert("if-none-match", "*".parse().unwrap());
    assert_aws_err(
        svc.put_object("123456789012", &req, "cnm", "f.txt").await,
        "PreconditionFailed",
    );
}

// ── Storage class ──

#[tokio::test]
async fn put_object_with_storage_class() {
    let svc = make_service();
    seed_bucket(&svc, "sc");

    let mut req = make_request(Method::PUT, "/sc/f.txt", &[], b"data");
    req.headers
        .insert("x-amz-storage-class", "GLACIER".parse().unwrap());
    svc.put_object("123456789012", &req, "sc", "f.txt")
        .await
        .unwrap();

    let req = make_request(Method::HEAD, "/sc/f.txt", &[], b"");
    let resp = svc
        .head_object("123456789012", &req, "sc", "f.txt")
        .unwrap();
    assert_eq!(
        resp.headers
            .get("x-amz-storage-class")
            .unwrap()
            .to_str()
            .unwrap(),
        "GLACIER"
    );
}

// ── Delete versioned object creates delete marker ──

#[tokio::test]
async fn delete_versioned_object_creates_marker() {
    let svc = make_service();
    seed_bucket(&svc, "dv");

    let req = make_request(
        Method::PUT,
        "/dv",
        &[("versioning", "")],
        b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
    );
    svc.put_bucket_versioning("123456789012", &req, "dv")
        .unwrap();

    let req = make_request(Method::PUT, "/dv/key", &[], b"data");
    svc.put_object("123456789012", &req, "dv", "key")
        .await
        .unwrap();

    let req = make_request(Method::DELETE, "/dv/key", &[], b"");
    let resp = svc
        .delete_object("123456789012", &req, "dv", "key")
        .unwrap();
    assert!(resp.headers.get("x-amz-delete-marker").is_some());

    // GET should fail (object is "deleted")
    let req = make_request(Method::GET, "/dv/key", &[], b"");
    assert_aws_err(
        svc.get_object("123456789012", &req, "dv", "key"),
        "NoSuchKey",
    );
}

// ── Copy with metadata replacement ──

#[tokio::test]
async fn copy_object_with_metadata_replace() {
    let svc = make_service();
    seed_bucket(&svc, "cpm");

    let mut req = make_request(Method::PUT, "/cpm/src", &[], b"data");
    req.headers
        .insert("x-amz-meta-original", "yes".parse().unwrap());
    svc.put_object("123456789012", &req, "cpm", "src")
        .await
        .unwrap();

    let mut req = make_request(Method::PUT, "/cpm/dst", &[], b"");
    req.headers
        .insert("x-amz-copy-source", "cpm/src".parse().unwrap());
    req.headers
        .insert("x-amz-metadata-directive", "REPLACE".parse().unwrap());
    req.headers
        .insert("x-amz-meta-new-key", "new-val".parse().unwrap());
    svc.copy_object("123456789012", &req, "cpm", "dst").unwrap();

    let req = make_request(Method::HEAD, "/cpm/dst", &[], b"");
    let resp = svc.head_object("123456789012", &req, "cpm", "dst").unwrap();
    assert!(resp
        .headers
        .get("x-amz-meta-new-key")
        .is_some_and(|v| v == "new-val"));
    // Original metadata should NOT be present
    assert!(resp.headers.get("x-amz-meta-original").is_none());
}

// ── Large list with pagination (max-keys) ──

#[tokio::test]
async fn list_objects_v2_with_max_keys() {
    let svc = make_service();
    seed_bucket(&svc, "pg");

    for i in 0..5 {
        let key = format!("k{i}");
        let req = make_request(Method::PUT, &format!("/pg/{key}"), &[], b"x");
        svc.put_object("123456789012", &req, "pg", &key)
            .await
            .unwrap();
    }

    let req = make_request(
        Method::GET,
        "/pg",
        &[("list-type", "2"), ("max-keys", "2")],
        b"",
    );
    let resp = svc.list_objects_v2("123456789012", &req, "pg").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<IsTruncated>true</IsTruncated>"));
    assert!(body.contains("<MaxKeys>2</MaxKeys>"));
}

// ── buckets.rs coverage (list/create/delete/head/location) ──

#[test]
fn list_buckets_empty_account() {
    let svc = make_service();
    let req = make_request(Method::GET, "/", &[], b"");
    let resp = svc.list_buckets("123456789012", &req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<ListAllMyBucketsResult"));
    assert!(body.contains("<Owner><ID>123456789012</ID>"));
    assert!(body.contains("<Buckets></Buckets>"));
}

#[test]
fn list_buckets_sorted_by_name() {
    let svc = make_service();
    seed_bucket(&svc, "zeta");
    seed_bucket(&svc, "alpha");
    seed_bucket(&svc, "middle");

    let req = make_request(Method::GET, "/", &[], b"");
    let resp = svc.list_buckets("123456789012", &req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    let a = body.find("alpha").unwrap();
    let m = body.find("middle").unwrap();
    let z = body.find("zeta").unwrap();
    assert!(a < m && m < z, "buckets must be sorted");
}

fn seed_bucket_in_region(svc: &S3Service, name: &str, region: &str) {
    let mut mas = svc.state.write();
    let state = mas.default_mut();
    state
        .buckets
        .insert(name.to_string(), S3Bucket::new(name, region, "owner"));
}

#[test]
fn list_buckets_includes_bucket_region() {
    let svc = make_service();
    seed_bucket(&svc, "alpha");

    let req = make_request(Method::GET, "/", &[], b"");
    let resp = svc.list_buckets("123456789012", &req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(
        body.contains("<BucketRegion>us-east-1</BucketRegion>"),
        "response should include BucketRegion per-bucket: {body}"
    );
}

#[test]
fn list_buckets_filter_by_bucket_region() {
    let svc = make_service();
    seed_bucket_in_region(&svc, "east-bucket", "us-east-1");
    seed_bucket_in_region(&svc, "west-bucket", "us-west-2");

    let req = make_request(Method::GET, "/", &[("bucket-region", "us-west-2")], b"");
    let resp = svc.list_buckets("123456789012", &req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("west-bucket"));
    assert!(!body.contains("east-bucket"));
    assert!(body.contains("<BucketRegion>us-west-2</BucketRegion>"));
}

#[test]
fn list_buckets_filter_by_prefix() {
    let svc = make_service();
    seed_bucket(&svc, "foo-1");
    seed_bucket(&svc, "foo-2");
    seed_bucket(&svc, "bar");

    let req = make_request(Method::GET, "/", &[("prefix", "foo-")], b"");
    let resp = svc.list_buckets("123456789012", &req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("foo-1"));
    assert!(body.contains("foo-2"));
    assert!(!body.contains("<Name>bar</Name>"));
    assert!(body.contains("<Prefix>foo-</Prefix>"));
}

#[test]
fn list_buckets_max_buckets_paginates() {
    let svc = make_service();
    for n in &["a", "b", "c", "d", "e"] {
        seed_bucket(&svc, n);
    }

    let req = make_request(Method::GET, "/", &[("max-buckets", "2")], b"");
    let resp = svc.list_buckets("123456789012", &req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<Name>a</Name>"));
    assert!(body.contains("<Name>b</Name>"));
    assert!(!body.contains("<Name>c</Name>"));
    assert!(body.contains("<ContinuationToken>"));
}

#[test]
fn list_buckets_continuation_token_resumes() {
    let svc = make_service();
    for n in &["a", "b", "c", "d", "e"] {
        seed_bucket(&svc, n);
    }

    let req = make_request(Method::GET, "/", &[("max-buckets", "2")], b"");
    let resp = svc.list_buckets("123456789012", &req).unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    let start = body.find("<ContinuationToken>").unwrap() + "<ContinuationToken>".len();
    let end = body.find("</ContinuationToken>").unwrap();
    let token = body[start..end].to_string();

    let req2 = make_request(
        Method::GET,
        "/",
        &[("max-buckets", "2"), ("continuation-token", &token)],
        b"",
    );
    let resp2 = svc.list_buckets("123456789012", &req2).unwrap();
    let body2 = std::str::from_utf8(resp2.body.expect_bytes()).unwrap();
    assert!(body2.contains("<Name>c</Name>"));
    assert!(body2.contains("<Name>d</Name>"));
    assert!(!body2.contains("<Name>a</Name>"));
    assert!(!body2.contains("<Name>b</Name>"));
    // page 2 has more (e remains) so still emits a token
    assert!(body2.contains("<ContinuationToken>"));

    // page 3: should be e + no continuation
    let start = body2.find("<ContinuationToken>").unwrap() + "<ContinuationToken>".len();
    let end = body2.find("</ContinuationToken>").unwrap();
    let token2 = body2[start..end].to_string();
    let req3 = make_request(
        Method::GET,
        "/",
        &[("max-buckets", "2"), ("continuation-token", &token2)],
        b"",
    );
    let resp3 = svc.list_buckets("123456789012", &req3).unwrap();
    let body3 = std::str::from_utf8(resp3.body.expect_bytes()).unwrap();
    assert!(body3.contains("<Name>e</Name>"));
    assert!(!body3.contains("<ContinuationToken>"));
}

#[test]
fn list_buckets_invalid_max_buckets_errors() {
    let svc = make_service();
    let req = make_request(Method::GET, "/", &[("max-buckets", "0")], b"");
    assert_aws_err(svc.list_buckets("123456789012", &req), "InvalidArgument");

    let req2 = make_request(Method::GET, "/", &[("max-buckets", "20000")], b"");
    assert_aws_err(svc.list_buckets("123456789012", &req2), "InvalidArgument");

    let req3 = make_request(Method::GET, "/", &[("max-buckets", "abc")], b"");
    assert_aws_err(svc.list_buckets("123456789012", &req3), "InvalidArgument");
}

#[test]
fn list_buckets_invalid_continuation_token_errors() {
    let svc = make_service();
    let req = make_request(
        Method::GET,
        "/",
        &[("continuation-token", "!!!notb64!!!")],
        b"",
    );
    assert_aws_err(svc.list_buckets("123456789012", &req), "InvalidArgument");

    let req2 = make_request(Method::GET, "/", &[("continuation-token", "")], b"");
    assert_aws_err(svc.list_buckets("123456789012", &req2), "InvalidArgument");
}

#[test]
fn create_bucket_invalid_name_errors() {
    let svc = make_service();
    let req = make_request(Method::PUT, "/AB", &[], b"");
    assert_aws_err(
        svc.create_bucket("123456789012", &req, "AB"),
        "InvalidBucketName",
    );
}

#[test]
fn create_bucket_idempotent_same_region_us_east_1() {
    let svc = make_service();
    let req = make_request(Method::PUT, "/idem", &[], b"");
    svc.create_bucket("123456789012", &req, "idem").unwrap();
    let resp = svc.create_bucket("123456789012", &req, "idem").unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

#[test]
fn create_bucket_already_owned_other_region() {
    let svc = make_service();
    let mut req = make_request(
        Method::PUT,
        "/bk1",
        &[],
        b"<CreateBucketConfiguration><LocationConstraint>eu-west-1</LocationConstraint></CreateBucketConfiguration>",
    );
    req.region = "eu-west-1".to_string();
    svc.create_bucket("123456789012", &req, "bk1").unwrap();
    assert_aws_err(
        svc.create_bucket("123456789012", &req, "bk1"),
        "BucketAlreadyOwnedByYou",
    );
}

#[test]
fn create_bucket_us_east_1_with_explicit_constraint_invalid() {
    let svc = make_service();
    let req = make_request(
        Method::PUT,
        "/bk2",
        &[],
        b"<CreateBucketConfiguration><LocationConstraint>us-east-1</LocationConstraint></CreateBucketConfiguration>",
    );
    assert_aws_err(
        svc.create_bucket("123456789012", &req, "bk2"),
        "InvalidLocationConstraint",
    );
}

#[test]
fn create_bucket_constraint_mismatch_region_errors() {
    let svc = make_service();
    let mut req = make_request(
        Method::PUT,
        "/bk3",
        &[],
        b"<CreateBucketConfiguration><LocationConstraint>us-west-2</LocationConstraint></CreateBucketConfiguration>",
    );
    req.region = "eu-west-1".to_string();
    assert_aws_err(
        svc.create_bucket("123456789012", &req, "bk3"),
        "IllegalLocationConstraintException",
    );
}

#[test]
fn create_bucket_missing_constraint_in_non_default_region_errors() {
    let svc = make_service();
    let mut req = make_request(Method::PUT, "/bk4", &[], b"");
    req.region = "eu-west-1".to_string();
    assert_aws_err(
        svc.create_bucket("123456789012", &req, "bk4"),
        "IllegalLocationConstraintException",
    );
}

#[test]
fn create_bucket_invalid_region_constraint() {
    let svc = make_service();
    let req = make_request(
        Method::PUT,
        "/bk5",
        &[],
        b"<CreateBucketConfiguration><LocationConstraint>not-a-region</LocationConstraint></CreateBucketConfiguration>",
    );
    assert_aws_err(
        svc.create_bucket("123456789012", &req, "bk5"),
        "InvalidLocationConstraint",
    );
}

#[test]
fn create_bucket_with_us_east_1_constraint_when_region_not_matching() {
    let svc = make_service();
    let mut req = make_request(
        Method::PUT,
        "/bk6",
        &[],
        b"<CreateBucketConfiguration><LocationConstraint>us-east-1</LocationConstraint></CreateBucketConfiguration>",
    );
    req.region = "eu-west-1".to_string();
    assert_aws_err(
        svc.create_bucket("123456789012", &req, "bk6"),
        "IllegalLocationConstraintException",
    );
}

#[test]
fn create_bucket_with_object_lock_enables_versioning() {
    let svc = make_service();
    let mut req = make_request(Method::PUT, "/olb", &[], b"");
    req.headers
        .insert("x-amz-bucket-object-lock-enabled", "true".parse().unwrap());
    svc.create_bucket("123456789012", &req, "olb").unwrap();
    let accts = svc.state.read();
    let state = accts.get("123456789012").unwrap();
    let b = state.buckets.get("olb").unwrap();
    assert_eq!(b.versioning.as_deref(), Some("Enabled"));
    assert!(b
        .object_lock_config
        .as_deref()
        .unwrap_or("")
        .contains("ObjectLockEnabled"));
}

#[test]
fn create_bucket_with_object_ownership_header() {
    let svc = make_service();
    let mut req = make_request(Method::PUT, "/own", &[], b"");
    req.headers.insert(
        "x-amz-object-ownership",
        "BucketOwnerEnforced".parse().unwrap(),
    );
    svc.create_bucket("123456789012", &req, "own").unwrap();
    let accts = svc.state.read();
    let state = accts.get("123456789012").unwrap();
    let b = state.buckets.get("own").unwrap();
    assert!(b
        .ownership_controls
        .as_deref()
        .unwrap_or("")
        .contains("BucketOwnerEnforced"));
}

#[test]
fn create_bucket_with_canned_acl_public_read() {
    let svc = make_service();
    let mut req = make_request(Method::PUT, "/prb", &[], b"");
    req.headers
        .insert("x-amz-acl", "public-read".parse().unwrap());
    svc.create_bucket("123456789012", &req, "prb").unwrap();
    let accts = svc.state.read();
    let state = accts.get("123456789012").unwrap();
    let b = state.buckets.get("prb").unwrap();
    assert!(!b.acl_grants.is_empty());
}

#[test]
fn delete_bucket_nonexistent_errors() {
    let svc = make_service();
    let req = make_request(Method::DELETE, "/nope", &[], b"");
    assert_aws_err(
        svc.delete_bucket("123456789012", &req, "nope"),
        "NoSuchBucket",
    );
}

#[test]
fn delete_bucket_not_empty_errors() {
    let svc = make_service();
    seed_bucket(&svc, "full");
    seed_object(&svc, "full", "k", b"x");
    let req = make_request(Method::DELETE, "/full", &[], b"");
    assert_aws_err(
        svc.delete_bucket("123456789012", &req, "full"),
        "BucketNotEmpty",
    );
}

#[test]
fn delete_bucket_with_versions_not_empty_errors() {
    let svc = make_service();
    seed_bucket(&svc, "ver");
    {
        let mut mas = svc.state.write();
        let state = mas.default_mut();
        let b = state.buckets.get_mut("ver").unwrap();
        b.object_versions.insert(
            "k".to_string(),
            vec![S3Object {
                key: "k".to_string(),
                body: fakecloud_persistence::BodyRef::Memory(Bytes::from_static(b"v")),
                content_type: "text/plain".to_string(),
                etag: "\"abc\"".to_string(),
                size: 1,
                last_modified: chrono::Utc::now(),
                ..Default::default()
            }],
        );
    }
    let req = make_request(Method::DELETE, "/ver", &[], b"");
    assert_aws_err(
        svc.delete_bucket("123456789012", &req, "ver"),
        "BucketNotEmpty",
    );
}

#[test]
fn delete_bucket_empty_succeeds() {
    let svc = make_service();
    seed_bucket(&svc, "empty");
    let req = make_request(Method::DELETE, "/empty", &[], b"");
    let resp = svc.delete_bucket("123456789012", &req, "empty").unwrap();
    assert_eq!(resp.status, StatusCode::NO_CONTENT);
}

#[test]
fn head_bucket_missing_errors() {
    // HeadBucket's Smithy `errors:` list is just [NotFound]; the wire-
    // level code lives in `x-amz-error-code` because HEAD has no body.
    // Use the model-declared code instead of `NoSuchBucket`.
    let svc = make_service();
    assert_aws_err(svc.head_bucket("123456789012", "nope"), "NotFound");
}

#[tokio::test]
async fn bucket_subresource_without_bucket_errors() {
    // Regression: requests like `GET /?tagging` (bucket name omitted)
    // used to fall through to `list_buckets` and return 200, masking the
    // missing required bucket. Real S3 rejects these with a validation
    // error; mirror that so conformance probes targeting required-field
    // omission see the expected 4xx.
    use fakecloud_core::service::AwsService;
    let svc = make_service();
    let cases: &[(Method, &str)] = &[
        (Method::GET, "tagging"),
        (Method::GET, "acl"),
        (Method::GET, "lifecycle"),
        (Method::GET, "encryption"),
        (Method::PUT, "tagging"),
        (Method::PUT, "encryption"),
        (Method::DELETE, "tagging"),
        (Method::DELETE, "lifecycle"),
    ];
    for (method, q) in cases {
        let req = make_request(method.clone(), "/", &[(q, "")], b"");
        let resp = svc.handle(req).await;
        let err = match resp {
            Ok(_) => panic!("{method} /?{q} should error, got Ok"),
            Err(e) => e,
        };
        assert_eq!(
            err.code(),
            "InvalidBucketName",
            "method={method} q={q} got {:?}",
            err.code()
        );
    }
}

#[test]
fn head_bucket_exists_returns_ok() {
    let svc = make_service();
    seed_bucket(&svc, "hb");
    let resp = svc.head_bucket("123456789012", "hb").unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

#[test]
fn head_bucket_returns_x_amz_bucket_region_header() {
    // Regression for issue #816 / #817 class: AWS Toolkit reads
    // x-amz-bucket-region from HeadBucket to do region routing.
    let svc = make_service();
    seed_bucket(&svc, "hbr");
    let resp = svc.head_bucket("123456789012", "hbr").unwrap();
    assert_eq!(
        resp.headers
            .get("x-amz-bucket-region")
            .and_then(|v| v.to_str().ok()),
        Some("us-east-1"),
    );
    assert_eq!(
        resp.headers
            .get("x-amz-bucket-location-type")
            .and_then(|v| v.to_str().ok()),
        Some("Region"),
    );
}

#[test]
fn get_bucket_location_us_east_1_returns_empty() {
    let svc = make_service();
    seed_bucket(&svc, "loc");
    let resp = svc.get_bucket_location("123456789012", "loc").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<LocationConstraint"));
    assert!(body.contains("></LocationConstraint>"));
}

#[test]
fn get_bucket_location_other_region_returns_region() {
    let svc = make_service();
    {
        let mut mas = svc.state.write();
        let state = mas.default_mut();
        state
            .buckets
            .insert("eu".to_string(), S3Bucket::new("eu", "eu-west-1", "owner"));
    }
    let resp = svc.get_bucket_location("123456789012", "eu").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains(">eu-west-1<"));
}

// ── objects.rs additional coverage ──

#[tokio::test]
async fn get_object_range_request() {
    let svc = make_service();
    seed_bucket(&svc, "range");
    let req = make_request(Method::PUT, "/range/k", &[], b"0123456789ABCDEF");
    svc.put_object("123456789012", &req, "range", "k")
        .await
        .unwrap();

    let mut req = make_request(Method::GET, "/range/k", &[], b"");
    req.headers.insert("range", "bytes=2-5".parse().unwrap());
    let resp = svc.get_object("123456789012", &req, "range", "k").unwrap();
    assert_eq!(resp.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(resp.body.expect_bytes(), b"2345");
}

#[tokio::test]
async fn get_object_range_suffix() {
    let svc = make_service();
    seed_bucket(&svc, "rsx");
    let req = make_request(Method::PUT, "/rsx/k", &[], b"0123456789");
    svc.put_object("123456789012", &req, "rsx", "k")
        .await
        .unwrap();

    let mut req = make_request(Method::GET, "/rsx/k", &[], b"");
    req.headers.insert("range", "bytes=-3".parse().unwrap());
    let resp = svc.get_object("123456789012", &req, "rsx", "k").unwrap();
    assert_eq!(resp.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(resp.body.expect_bytes(), b"789");
}

#[tokio::test]
async fn get_object_range_open_ended() {
    let svc = make_service();
    seed_bucket(&svc, "roe");
    let req = make_request(Method::PUT, "/roe/k", &[], b"0123456789");
    svc.put_object("123456789012", &req, "roe", "k")
        .await
        .unwrap();

    let mut req = make_request(Method::GET, "/roe/k", &[], b"");
    req.headers.insert("range", "bytes=7-".parse().unwrap());
    let resp = svc.get_object("123456789012", &req, "roe", "k").unwrap();
    assert_eq!(resp.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(resp.body.expect_bytes(), b"789");
}

#[tokio::test]
async fn get_object_range_invalid_format() {
    let svc = make_service();
    seed_bucket(&svc, "rinv");
    let req = make_request(Method::PUT, "/rinv/k", &[], b"hello");
    svc.put_object("123456789012", &req, "rinv", "k")
        .await
        .unwrap();

    let mut req = make_request(Method::GET, "/rinv/k", &[], b"");
    req.headers.insert("range", "bogus=2-5".parse().unwrap());
    // Non-standard prefix -> full content expected
    let resp = svc.get_object("123456789012", &req, "rinv", "k").unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(resp.body.expect_bytes(), b"hello");
}

#[tokio::test]
async fn get_object_if_match_mismatch_errors() {
    let svc = make_service();
    seed_bucket(&svc, "ifm");
    let req = make_request(Method::PUT, "/ifm/k", &[], b"abc");
    svc.put_object("123456789012", &req, "ifm", "k")
        .await
        .unwrap();

    let mut req = make_request(Method::GET, "/ifm/k", &[], b"");
    req.headers
        .insert("if-match", "\"nomatch\"".parse().unwrap());
    let err = svc.get_object("123456789012", &req, "ifm", "k");
    assert_aws_err(err, "PreconditionFailed");
}

#[tokio::test]
async fn get_object_if_none_match_star_not_modified() {
    let svc = make_service();
    seed_bucket(&svc, "inm");
    let req = make_request(Method::PUT, "/inm/k", &[], b"abc");
    svc.put_object("123456789012", &req, "inm", "k")
        .await
        .unwrap();

    let mut req = make_request(Method::GET, "/inm/k", &[], b"");
    req.headers.insert("if-none-match", "*".parse().unwrap());
    let err = svc.get_object("123456789012", &req, "inm", "k");
    assert!(err.is_err());
}

#[tokio::test]
async fn head_object_range_request() {
    let svc = make_service();
    seed_bucket(&svc, "hrng");
    let req = make_request(Method::PUT, "/hrng/k", &[], b"0123456789");
    svc.put_object("123456789012", &req, "hrng", "k")
        .await
        .unwrap();

    let mut req = make_request(Method::HEAD, "/hrng/k", &[], b"");
    req.headers.insert("range", "bytes=2-5".parse().unwrap());
    let resp = svc.head_object("123456789012", &req, "hrng", "k").unwrap();
    assert_eq!(resp.status, StatusCode::PARTIAL_CONTENT);
}

#[tokio::test]
async fn put_object_with_metadata_headers() {
    let svc = make_service();
    seed_bucket(&svc, "meta");
    let mut req = make_request(Method::PUT, "/meta/k", &[], b"x");
    req.headers
        .insert("x-amz-meta-user", "alice".parse().unwrap());
    req.headers
        .insert("x-amz-meta-env", "prod".parse().unwrap());
    svc.put_object("123456789012", &req, "meta", "k")
        .await
        .unwrap();

    let req = make_request(Method::HEAD, "/meta/k", &[], b"");
    let resp = svc.head_object("123456789012", &req, "meta", "k").unwrap();
    assert_eq!(resp.headers.get("x-amz-meta-user").unwrap(), "alice");
    assert_eq!(resp.headers.get("x-amz-meta-env").unwrap(), "prod");
}

#[tokio::test]
async fn put_object_with_storage_class_header() {
    let svc = make_service();
    seed_bucket(&svc, "stor");
    let mut req = make_request(Method::PUT, "/stor/k", &[], b"x");
    req.headers
        .insert("x-amz-storage-class", "STANDARD_IA".parse().unwrap());
    svc.put_object("123456789012", &req, "stor", "k")
        .await
        .unwrap();

    let req = make_request(Method::HEAD, "/stor/k", &[], b"");
    let resp = svc.head_object("123456789012", &req, "stor", "k").unwrap();
    assert_eq!(
        resp.headers.get("x-amz-storage-class").unwrap(),
        "STANDARD_IA"
    );
}

#[tokio::test]
async fn put_object_with_website_redirect() {
    let svc = make_service();
    seed_bucket(&svc, "wr");
    let mut req = make_request(Method::PUT, "/wr/k", &[], b"x");
    req.headers.insert(
        "x-amz-website-redirect-location",
        "/elsewhere".parse().unwrap(),
    );
    svc.put_object("123456789012", &req, "wr", "k")
        .await
        .unwrap();
    let req = make_request(Method::GET, "/wr/k", &[], b"");
    let resp = svc.get_object("123456789012", &req, "wr", "k").unwrap();
    assert_eq!(
        resp.headers.get("x-amz-website-redirect-location").unwrap(),
        "/elsewhere"
    );
}

#[test]
fn delete_object_nonexistent_is_ok() {
    let svc = make_service();
    seed_bucket(&svc, "dne");
    let req = make_request(Method::DELETE, "/dne/missing", &[], b"");
    let resp = svc
        .delete_object("123456789012", &req, "dne", "missing")
        .unwrap();
    assert_eq!(resp.status, StatusCode::NO_CONTENT);
}

#[test]
fn delete_object_bucket_not_found() {
    let svc = make_service();
    let req = make_request(Method::DELETE, "/nope/k", &[], b"");
    assert_aws_err(
        svc.delete_object("123456789012", &req, "nope", "k"),
        "NoSuchBucket",
    );
}

#[tokio::test]
async fn list_objects_v2_with_prefix_and_delimiter() {
    let svc = make_service();
    seed_bucket(&svc, "pfxd");
    for k in &["a/1", "a/2", "b/1"] {
        let req = make_request(Method::PUT, &format!("/pfxd/{k}"), &[], b"x");
        svc.put_object("123456789012", &req, "pfxd", k)
            .await
            .unwrap();
    }
    let req = make_request(
        Method::GET,
        "/pfxd",
        &[("list-type", "2"), ("prefix", "a/"), ("delimiter", "/")],
        b"",
    );
    let resp = svc.list_objects_v2("123456789012", &req, "pfxd").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<Contents>"));
}

#[tokio::test]
async fn list_objects_v1_basic() {
    let svc = make_service();
    seed_bucket(&svc, "v1");
    for k in &["a", "b"] {
        let req = make_request(Method::PUT, &format!("/v1/{k}"), &[], b"x");
        svc.put_object("123456789012", &req, "v1", k).await.unwrap();
    }
    let req = make_request(Method::GET, "/v1", &[], b"");
    let resp = svc.list_objects_v1("123456789012", &req, "v1").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<ListBucketResult"));
    assert!(body.contains("<Key>a</Key>"));
    assert!(body.contains("<Key>b</Key>"));
}

/// ListObjects v1: a truncated listing WITHOUT a delimiter must NOT emit
/// NextMarker (the client resumes from the last key itself). AWS only
/// returns NextMarker when a delimiter is supplied.
#[tokio::test]
async fn list_objects_v1_truncated_no_delimiter_omits_next_marker() {
    let svc = make_service();
    seed_bucket(&svc, "nm1");
    for k in &["k0", "k1", "k2"] {
        let req = make_request(Method::PUT, &format!("/nm1/{k}"), &[], b"x");
        svc.put_object("123456789012", &req, "nm1", k)
            .await
            .unwrap();
    }
    let req = make_request(Method::GET, "/nm1", &[("max-keys", "1")], b"");
    let resp = svc.list_objects_v1("123456789012", &req, "nm1").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<IsTruncated>true</IsTruncated>"));
    assert!(
        !body.contains("<NextMarker>"),
        "NextMarker must be omitted without a delimiter: {body}"
    );
}

/// ListObjects v1: a truncated listing WITH a delimiter includes NextMarker
/// so the client can resume past the whole common-prefix group.
#[tokio::test]
async fn list_objects_v1_truncated_with_delimiter_includes_next_marker() {
    let svc = make_service();
    seed_bucket(&svc, "nm2");
    for k in &["p1/a", "p2/b", "p3/c"] {
        let req = make_request(Method::PUT, &format!("/nm2/{k}"), &[], b"x");
        svc.put_object("123456789012", &req, "nm2", k)
            .await
            .unwrap();
    }
    let req = make_request(
        Method::GET,
        "/nm2",
        &[("max-keys", "1"), ("delimiter", "/")],
        b"",
    );
    let resp = svc.list_objects_v1("123456789012", &req, "nm2").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<IsTruncated>true</IsTruncated>"));
    assert!(
        body.contains("<NextMarker>p1/</NextMarker>"),
        "NextMarker should carry the common-prefix cursor: {body}"
    );
}

#[test]
fn get_object_key_not_found() {
    let svc = make_service();
    seed_bucket(&svc, "gkn");
    let req = make_request(Method::GET, "/gkn/missing", &[], b"");
    assert_aws_err(
        svc.get_object("123456789012", &req, "gkn", "missing"),
        "NoSuchKey",
    );
}

#[test]
fn get_object_bucket_not_found() {
    // Strict conformance: GetObject's Smithy `errors` excludes NoSuchBucket,
    // so missing-bucket collapses to NoSuchKey.
    let svc = make_service();
    let req = make_request(Method::GET, "/ghost/k", &[], b"");
    assert_aws_err(
        svc.get_object("123456789012", &req, "ghost", "k"),
        "NoSuchKey",
    );
}

// ── restore_object ──

#[tokio::test]
async fn restore_object_non_archival_errors() {
    let svc = make_service();
    seed_bucket(&svc, "roc");
    let req = make_request(Method::PUT, "/roc/k", &[], b"x");
    svc.put_object("123456789012", &req, "roc", "k")
        .await
        .unwrap();

    let req = make_request(Method::POST, "/roc/k", &[("restore", "")], b"");
    assert_aws_err(
        svc.restore_object("123456789012", &req, "roc", "k"),
        "InvalidObjectState",
    );
}

#[tokio::test]
async fn restore_object_glacier_accepted() {
    let svc = make_service();
    seed_bucket(&svc, "rog");
    let mut req = make_request(Method::PUT, "/rog/k", &[], b"x");
    req.headers
        .insert("x-amz-storage-class", "GLACIER".parse().unwrap());
    svc.put_object("123456789012", &req, "rog", "k")
        .await
        .unwrap();

    let req = make_request(Method::POST, "/rog/k", &[("restore", "")], b"");
    let resp = svc
        .restore_object("123456789012", &req, "rog", "k")
        .unwrap();
    assert_eq!(resp.status, StatusCode::ACCEPTED);
}

#[test]
fn restore_object_nonexistent_key() {
    let svc = make_service();
    seed_bucket(&svc, "rnk");
    let req = make_request(Method::POST, "/rnk/ghost", &[("restore", "")], b"");
    assert_aws_err(
        svc.restore_object("123456789012", &req, "rnk", "ghost"),
        "NoSuchKey",
    );
}

// ── list_object_versions ──

#[tokio::test]
async fn list_object_versions_basic() {
    let svc = make_service();
    seed_bucket(&svc, "lov");
    {
        let mut mas = svc.state.write();
        let state = mas.default_mut();
        let b = state.buckets.get_mut("lov").unwrap();
        b.versioning = Some("Enabled".to_string());
    }
    let req = make_request(Method::PUT, "/lov/k", &[], b"v1");
    svc.put_object("123456789012", &req, "lov", "k")
        .await
        .unwrap();
    let req = make_request(Method::PUT, "/lov/k", &[], b"v2");
    svc.put_object("123456789012", &req, "lov", "k")
        .await
        .unwrap();

    let req = make_request(Method::GET, "/lov", &[("versions", "")], b"");
    let resp = svc
        .list_object_versions("123456789012", &req, "lov")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<ListVersionsResult"));
}

#[test]
fn delete_objects_nonexistent_bucket() {
    let svc = make_service();
    let xml = b"<Delete><Object><Key>k</Key></Object></Delete>";
    let req = make_request(Method::POST, "/ghost", &[("delete", "")], xml);
    assert_aws_err(
        svc.delete_objects("123456789012", &req, "ghost"),
        "NoSuchBucket",
    );
}

#[tokio::test]
async fn put_object_custom_content_type() {
    let svc = make_service();
    seed_bucket(&svc, "ct");
    let mut req = make_request(Method::PUT, "/ct/k", &[], b"hi");
    req.headers
        .insert("content-type", "text/plain".parse().unwrap());
    svc.put_object("123456789012", &req, "ct", "k")
        .await
        .unwrap();
    let req = make_request(Method::GET, "/ct/k", &[], b"");
    let resp = svc.get_object("123456789012", &req, "ct", "k").unwrap();
    assert_eq!(resp.content_type, "text/plain");
}

#[test]
fn head_object_bucket_not_found() {
    let svc = make_service();
    let req = make_request(Method::HEAD, "/ghost/k", &[], b"");
    let result = svc.head_object("123456789012", &req, "ghost", "k");
    assert!(result.is_err());
}

#[tokio::test]
async fn get_object_attributes_basic() {
    let svc = make_service();
    seed_bucket(&svc, "goa");
    let req = make_request(Method::PUT, "/goa/k", &[], b"hi");
    svc.put_object("123456789012", &req, "goa", "k")
        .await
        .unwrap();

    let mut req = make_request(Method::GET, "/goa/k", &[("attributes", "")], b"");
    req.headers.insert(
        "x-amz-object-attributes",
        "ETag,ObjectSize".parse().unwrap(),
    );
    let resp = svc
        .get_object_attributes("123456789012", &req, "goa", "k")
        .unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

#[test]
fn get_object_attributes_bucket_not_found() {
    let svc = make_service();
    let req = make_request(Method::GET, "/ghost/k", &[("attributes", "")], b"");
    let result = svc.get_object_attributes("123456789012", &req, "ghost", "k");
    assert!(result.is_err());
}

#[test]
fn get_object_attributes_key_not_found() {
    let svc = make_service();
    seed_bucket(&svc, "gak");
    let req = make_request(Method::GET, "/gak/ghost", &[("attributes", "")], b"");
    let result = svc.get_object_attributes("123456789012", &req, "gak", "ghost");
    assert!(result.is_err());
}

// ── ACL ──

#[test]
fn get_object_acl_bucket_not_found() {
    let svc = make_service();
    let req = make_request(Method::GET, "/ghost/k", &[("acl", "")], b"");
    assert!(svc
        .get_object_acl("123456789012", &req, "ghost", "k")
        .is_err());
}

#[test]
fn put_object_acl_bucket_not_found() {
    let svc = make_service();
    let mut req = make_request(Method::PUT, "/ghost/k", &[("acl", "")], b"");
    req.headers.insert("x-amz-acl", "private".parse().unwrap());
    assert!(svc
        .put_object_acl("123456789012", &req, "ghost", "k")
        .is_err());
}

#[tokio::test]
async fn get_object_acl_returns_acl_xml() {
    let svc = make_service();
    seed_bucket(&svc, "acl");
    let req = make_request(Method::PUT, "/acl/k", &[], b"x");
    svc.put_object("123456789012", &req, "acl", "k")
        .await
        .unwrap();
    let req = make_request(Method::GET, "/acl/k", &[("acl", "")], b"");
    let resp = svc
        .get_object_acl("123456789012", &req, "acl", "k")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("AccessControlPolicy"));
}

// ── object lock ──

#[test]
fn put_object_retention_bucket_not_found() {
    let svc = make_service();
    let xml = b"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2030-01-01T00:00:00Z</RetainUntilDate></Retention>";
    let req = make_request(Method::PUT, "/ghost/k", &[("retention", "")], xml);
    assert!(svc
        .put_object_retention("123456789012", &req, "ghost", "k")
        .is_err());
}

#[test]
fn get_object_legal_hold_bucket_not_found() {
    let svc = make_service();
    let req = make_request(Method::GET, "/ghost/k", &[("legal-hold", "")], b"");
    assert!(svc
        .get_object_legal_hold("123456789012", &req, "ghost", "k")
        .is_err());
}

#[test]
fn get_object_retention_bucket_not_found() {
    let svc = make_service();
    let req = make_request(Method::GET, "/ghost/k", &[("retention", "")], b"");
    assert!(svc
        .get_object_retention("123456789012", &req, "ghost", "k")
        .is_err());
}

// ── Multipart variations ──

#[test]
fn list_multipart_uploads_nonexistent_bucket() {
    let svc = make_service();
    assert!(svc
        .list_multipart_uploads("123456789012", "ghost", &std::collections::HashMap::new())
        .is_err());
}

#[test]
fn list_multipart_uploads_empty() {
    let svc = make_service();
    seed_bucket(&svc, "empmp");
    let resp = svc
        .list_multipart_uploads("123456789012", "empmp", &std::collections::HashMap::new())
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<ListMultipartUploadsResult"));
}

#[test]
fn put_public_access_block_bucket_not_found() {
    let svc = make_service();
    let xml = b"<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls></PublicAccessBlockConfiguration>";
    let req = make_request(Method::PUT, "/ghost", &[("publicAccessBlock", "")], xml);
    assert!(svc
        .put_public_access_block("123456789012", &req, "ghost")
        .is_err());
}

#[test]
fn public_access_block_lifecycle() {
    let svc = make_service();
    seed_bucket(&svc, "pab");
    let xml = b"<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>true</IgnorePublicAcls></PublicAccessBlockConfiguration>";
    let req = make_request(Method::PUT, "/pab", &[("publicAccessBlock", "")], xml);
    svc.put_public_access_block("123456789012", &req, "pab")
        .unwrap();

    let resp = svc.get_public_access_block("123456789012", "pab").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("BlockPublicAcls"));

    svc.delete_public_access_block("123456789012", "pab")
        .unwrap();
}

#[test]
fn put_bucket_replication_bucket_not_found() {
    let svc = make_service();
    let xml = b"<ReplicationConfiguration><Role>arn</Role></ReplicationConfiguration>";
    let req = make_request(Method::PUT, "/ghost", &[("replication", "")], xml);
    assert!(svc
        .put_bucket_replication("123456789012", &req, "ghost")
        .is_err());
}

#[test]
fn put_ownership_controls_bucket_not_found() {
    let svc = make_service();
    let xml = b"<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>";
    let req = make_request(Method::PUT, "/ghost", &[("ownershipControls", "")], xml);
    assert!(svc
        .put_bucket_ownership_controls("123456789012", &req, "ghost")
        .is_err());
}

#[test]
fn put_bucket_accelerate_bucket_not_found() {
    let svc = make_service();
    let xml = b"<AccelerateConfiguration><Status>Enabled</Status></AccelerateConfiguration>";
    let req = make_request(Method::PUT, "/ghost", &[("accelerate", "")], xml);
    assert!(svc
        .put_bucket_accelerate("123456789012", &req, "ghost")
        .is_err());
}

#[test]
fn get_bucket_accelerate_lifecycle() {
    let svc = make_service();
    seed_bucket(&svc, "acc");
    let xml = b"<AccelerateConfiguration><Status>Enabled</Status></AccelerateConfiguration>";
    let req = make_request(Method::PUT, "/acc", &[("accelerate", "")], xml);
    svc.put_bucket_accelerate("123456789012", &req, "acc")
        .unwrap();
    let resp = svc.get_bucket_accelerate("123456789012", "acc").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("AccelerateConfiguration"));
}

#[test]
fn put_bucket_website_bucket_not_found() {
    let svc = make_service();
    let xml = b"<WebsiteConfiguration><IndexDocument><Suffix>index.html</Suffix></IndexDocument></WebsiteConfiguration>";
    let req = make_request(Method::PUT, "/ghost", &[("website", "")], xml);
    assert!(svc
        .put_bucket_website("123456789012", &req, "ghost")
        .is_err());
}

#[test]
fn put_object_tagging_bucket_not_found() {
    let svc = make_service();
    let xml = b"<Tagging><TagSet></TagSet></Tagging>";
    let req = make_request(Method::PUT, "/ghost/k", &[("tagging", "")], xml);
    assert!(svc
        .put_object_tagging("123456789012", &req, "ghost", "k")
        .is_err());
}

#[test]
fn put_object_tagging_key_not_found() {
    let svc = make_service();
    seed_bucket(&svc, "pot");
    let xml = b"<Tagging><TagSet></TagSet></Tagging>";
    let req = make_request(Method::PUT, "/pot/ghost", &[("tagging", "")], xml);
    assert!(svc
        .put_object_tagging("123456789012", &req, "pot", "ghost")
        .is_err());
}

#[tokio::test]
async fn put_object_tagging_lifecycle() {
    let svc = make_service();
    seed_bucket(&svc, "pota");
    let req = make_request(Method::PUT, "/pota/k", &[], b"x");
    svc.put_object("123456789012", &req, "pota", "k")
        .await
        .unwrap();

    let xml = b"<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>";
    let req = make_request(Method::PUT, "/pota/k", &[("tagging", "")], xml);
    svc.put_object_tagging("123456789012", &req, "pota", "k")
        .unwrap();

    let req = make_request(Method::GET, "/pota/k", &[("tagging", "")], b"");
    let resp = svc
        .get_object_tagging("123456789012", &req, "pota", "k")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<Key>env</Key>"));

    svc.delete_object_tagging("123456789012", "pota", "k")
        .unwrap();
}

#[test]
fn delete_object_tagging_bucket_not_found() {
    let svc = make_service();
    assert!(svc
        .delete_object_tagging("123456789012", "ghost", "k")
        .is_err());
}

#[test]
fn put_bucket_tagging_bucket_not_found() {
    let svc = make_service();
    let xml = b"<Tagging><TagSet></TagSet></Tagging>";
    let req = make_request(Method::PUT, "/ghost", &[("tagging", "")], xml);
    assert!(svc
        .put_bucket_tagging("123456789012", &req, "ghost")
        .is_err());
}

#[test]
fn bucket_tagging_lifecycle() {
    let svc = make_service();
    seed_bucket(&svc, "bt");
    let xml = b"<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>";
    let req = make_request(Method::PUT, "/bt", &[("tagging", "")], xml);
    svc.put_bucket_tagging("123456789012", &req, "bt").unwrap();

    let req = make_request(Method::GET, "/bt", &[("tagging", "")], b"");
    let resp = svc.get_bucket_tagging("123456789012", &req, "bt").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<Key>env</Key>"));

    let req = make_request(Method::DELETE, "/bt", &[("tagging", "")], b"");
    svc.delete_bucket_tagging("123456789012", &req, "bt")
        .unwrap();
}

#[tokio::test]
async fn delete_objects_batch_respects_compliance_retention() {
    // Single DeleteObject already gates COMPLIANCE retention; the
    // batch DeleteObjects unversioned arm previously skipped the
    // gate, letting a caller sidestep compliance via the batch
    // endpoint. This test pins the gate in place.
    let svc = make_service();
    seed_bucket(&svc, "comp-batch");
    seed_object(&svc, "comp-batch", "locked.txt", b"x");
    seed_object(&svc, "comp-batch", "free.txt", b"y");

    // Apply COMPLIANCE retention to one of the two objects.
    let body = b"<Retention><Mode>COMPLIANCE</Mode><RetainUntilDate>2099-01-01T00:00:00Z</RetainUntilDate></Retention>";
    let req = make_request(
        Method::PUT,
        "/comp-batch/locked.txt",
        &[("retention", "")],
        body,
    );
    svc.put_object_retention("123456789012", &req, "comp-batch", "locked.txt")
        .unwrap();

    // Batch delete both — compliance object must come back as Error.
    let xml = r#"<Delete><Object><Key>locked.txt</Key></Object><Object><Key>free.txt</Key></Object></Delete>"#;
    let req = make_request(
        Method::POST,
        "/comp-batch",
        &[("delete", "")],
        xml.as_bytes(),
    );
    let resp = svc
        .delete_objects("123456789012", &req, "comp-batch")
        .unwrap();
    let body_str = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(
        body_str.contains("<Key>locked.txt</Key>") && body_str.contains("InvalidRequest")
            || body_str.contains("<Key>locked.txt</Key>") && body_str.contains("AccessDenied"),
        "expected locked.txt to surface as Error in DeleteResult, got: {body_str}"
    );
    assert!(
        body_str.contains("<Deleted><Key>free.txt</Key>"),
        "expected free.txt to be deleted, got: {body_str}"
    );
}

#[tokio::test]
async fn bucket_owner_enforced_rejects_put_bucket_acl_and_object_acl() {
    let svc = make_service();
    seed_bucket(&svc, "owner-enforced");

    let body = br#"<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>"#;
    let req = make_request(
        Method::PUT,
        "/owner-enforced",
        &[("ownershipControls", "")],
        body,
    );
    svc.put_bucket_ownership_controls("123456789012", &req, "owner-enforced")
        .unwrap();

    let mut req = make_request(Method::PUT, "/owner-enforced", &[("acl", "")], b"");
    req.headers.insert("x-amz-acl", "private".parse().unwrap());
    let err = match svc.put_bucket_acl("123456789012", &req, "owner-enforced") {
        Err(e) => e,
        Ok(_) => panic!("PutBucketAcl should be rejected under BucketOwnerEnforced"),
    };
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);

    // Seed an object first (PutObject without ACL header is fine).
    let req = make_request(Method::PUT, "/owner-enforced/k", &[], b"x");
    svc.put_object("123456789012", &req, "owner-enforced", "k")
        .await
        .unwrap();

    let mut req = make_request(Method::PUT, "/owner-enforced/k", &[("acl", "")], b"");
    req.headers
        .insert("x-amz-acl", "public-read".parse().unwrap());
    let err = match svc.put_object_acl("123456789012", &req, "owner-enforced", "k") {
        Err(e) => e,
        Ok(_) => panic!("PutObjectAcl should be rejected under BucketOwnerEnforced"),
    };
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn bucket_owner_enforced_rejects_put_object_with_acl_header() {
    let svc = make_service();
    seed_bucket(&svc, "noaclonput");

    let body = br#"<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>"#;
    let req = make_request(
        Method::PUT,
        "/noaclonput",
        &[("ownershipControls", "")],
        body,
    );
    svc.put_bucket_ownership_controls("123456789012", &req, "noaclonput")
        .unwrap();

    // Plain PutObject still works.
    let req = make_request(Method::PUT, "/noaclonput/plain", &[], b"x");
    svc.put_object("123456789012", &req, "noaclonput", "plain")
        .await
        .unwrap();

    // PutObject with x-amz-acl rejects.
    let mut req = make_request(Method::PUT, "/noaclonput/withacl", &[], b"x");
    req.headers
        .insert("x-amz-acl", "public-read".parse().unwrap());
    let err = match svc
        .put_object("123456789012", &req, "noaclonput", "withacl")
        .await
    {
        Err(e) => e,
        Ok(_) => panic!("PutObject with x-amz-acl should be rejected under BucketOwnerEnforced"),
    };
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn put_bucket_encryption_aws_kms_requires_kms_master_key_id() {
    // AWS rejects PutBucketEncryption with aws:kms but no
    // KMSMasterKeyID — there's no key for the bucket to default
    // encrypt against. fakecloud was silently accepting it.
    let svc = make_service();
    seed_bucket(&svc, "kmsenc");

    let body = b"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>";
    let req = make_request(Method::PUT, "/kmsenc", &[("encryption", "")], body);
    let err = match svc.put_bucket_encryption("123456789012", &req, "kmsenc") {
        Err(e) => e,
        Ok(_) => panic!("expected InvalidArgument when KMSMasterKeyID is missing"),
    };
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);

    let body = b"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>alias/aws/s3</KMSMasterKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>";
    let req = make_request(Method::PUT, "/kmsenc", &[("encryption", "")], body);
    svc.put_bucket_encryption("123456789012", &req, "kmsenc")
        .unwrap();
}

#[test]
fn put_bucket_encryption_rejects_control_char_in_kms_key_id() {
    // Regression (bug-audit 2026-06-13 2.1): a control byte (newline) in the
    // bucket-default KMSMasterKeyID used to be persisted verbatim, applied as
    // an object default on PutObject, then panic the GET/HEAD response path
    // when round-tripped into x-amz-server-side-encryption-aws-kms-key-id.
    // It must now be rejected up front with 400, not a panic.
    let svc = make_service();
    seed_bucket(&svc, "kmsfuzz");

    let body = b"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>bad\nkey</KMSMasterKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>";
    let req = make_request(Method::PUT, "/kmsfuzz", &[("encryption", "")], body);
    let err = match svc.put_bucket_encryption("123456789012", &req, "kmsfuzz") {
        Err(e) => e,
        Ok(_) => panic!("control char in KMSMasterKeyID should be rejected"),
    };
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);

    // A NUL byte is likewise rejected.
    let body = b"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>k\x00id</KMSMasterKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>";
    let req = make_request(Method::PUT, "/kmsfuzz", &[("encryption", "")], body);
    assert!(
        svc.put_bucket_encryption("123456789012", &req, "kmsfuzz")
            .is_err(),
        "NUL byte in KMSMasterKeyID should be rejected"
    );

    // A legitimate KMS key ARN still round-trips and persists.
    let arn = "arn:aws:kms:us-east-1:123456789012:key/abcd-1234";
    let body = format!(
        "<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>{arn}</KMSMasterKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"
    );
    let req = make_request(
        Method::PUT,
        "/kmsfuzz",
        &[("encryption", "")],
        body.as_bytes(),
    );
    svc.put_bucket_encryption("123456789012", &req, "kmsfuzz")
        .expect("a valid KMS key ARN must be accepted");
}

#[test]
fn put_bucket_encryption_rejects_invalid_sse_algorithm() {
    // Regression (bug-audit 2026-06-13 2.2): SSEAlgorithm is a closed enum.
    // A garbage value (including one carrying a control char) used to be
    // stored verbatim and later panic the GET response header. AWS returns
    // MalformedXML for an unknown algorithm.
    let svc = make_service();
    seed_bucket(&svc, "algofuzz");

    let body = b"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>x\ny</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>";
    let req = make_request(Method::PUT, "/algofuzz", &[("encryption", "")], body);
    let err = match svc.put_bucket_encryption("123456789012", &req, "algofuzz") {
        Err(e) => e,
        Ok(_) => panic!("invalid SSEAlgorithm should be rejected"),
    };
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);

    // AES256 still accepted.
    let body = b"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>";
    let req = make_request(Method::PUT, "/algofuzz", &[("encryption", "")], body);
    svc.put_bucket_encryption("123456789012", &req, "algofuzz")
        .expect("AES256 is a valid SSEAlgorithm");
}

#[tokio::test]
async fn bucket_default_kms_key_round_trips_on_get_without_panic() {
    // End-to-end of the fixed path: a valid bucket-default KMS config is
    // applied to an object PUT with no SSE headers and surfaces cleanly on
    // GET — no dropped connection, and the key id is returned verbatim.
    let svc = make_service();
    seed_bucket(&svc, "kmsdef");

    let arn = "arn:aws:kms:us-east-1:123456789012:key/abcd-1234";
    let enc = format!(
        "<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>{arn}</KMSMasterKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"
    );
    let enc_req = make_request(
        Method::PUT,
        "/kmsdef",
        &[("encryption", "")],
        enc.as_bytes(),
    );
    svc.put_bucket_encryption("123456789012", &enc_req, "kmsdef")
        .unwrap();

    let put_req = make_request(Method::PUT, "/kmsdef/obj", &[], b"data");
    svc.put_object("123456789012", &put_req, "kmsdef", "obj")
        .await
        .unwrap();

    let get_req = make_request(Method::GET, "/kmsdef/obj", &[], b"");
    let resp = svc
        .get_object("123456789012", &get_req, "kmsdef", "obj")
        .unwrap();
    assert_eq!(
        resp.headers
            .get("x-amz-server-side-encryption-aws-kms-key-id")
            .and_then(|v| v.to_str().ok()),
        Some(arn),
        "valid bucket-default KMS key id must round-trip on GET"
    );
    assert_eq!(
        resp.headers
            .get("x-amz-server-side-encryption")
            .and_then(|v| v.to_str().ok()),
        Some("aws:kms")
    );
}

#[test]
fn get_bucket_policy_status_uses_real_json_parse() {
    // Substring scan would falsely flag a Description string as
    // public. Real JSON parse only flags actual Principal=*.
    let svc = make_service();
    seed_bucket(&svc, "polstat");

    let policy = br#"{"Version":"2012-10-17","Id":"some \"Principal\":\"*\" string","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789012:role/r"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::polstat/*"}]}"#;
    let req = make_request(Method::PUT, "/polstat", &[("policy", "")], policy);
    svc.put_bucket_policy("123456789012", &req, "polstat")
        .unwrap();
    let resp = svc
        .get_bucket_policy_status("123456789012", "polstat")
        .unwrap();
    assert!(std::str::from_utf8(resp.body.expect_bytes())
        .unwrap()
        .contains("<IsPublic>false</IsPublic>"));

    let policy = br#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::polstat/*"}]}"#;
    let req = make_request(Method::PUT, "/polstat", &[("policy", "")], policy);
    svc.put_bucket_policy("123456789012", &req, "polstat")
        .unwrap();
    let resp = svc
        .get_bucket_policy_status("123456789012", "polstat")
        .unwrap();
    assert!(std::str::from_utf8(resp.body.expect_bytes())
        .unwrap()
        .contains("<IsPublic>true</IsPublic>"));
}

#[tokio::test]
async fn compute_checksum_streaming_supports_crc32c() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data");
    let mut file = std::fs::File::create(&path).unwrap();
    file.write_all(b"hello world").unwrap();
    let result = super::compute_checksum_streaming("CRC32C", &path)
        .await
        .unwrap();
    // Reference CRC32C("hello world") = 0xC99465AA, big-endian -> base64
    let expected = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        0xC99465AAu32.to_be_bytes(),
    );
    assert_eq!(result, expected);
}

#[tokio::test]
async fn compute_checksum_streaming_supports_crc64nvme() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data");
    let mut file = std::fs::File::create(&path).unwrap();
    file.write_all(b"hello world").unwrap();
    let result = super::compute_checksum_streaming("CRC64NVME", &path)
        .await
        .unwrap();
    // 8-byte digest, base64 = 12 chars
    let decoded = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        result.as_bytes(),
    )
    .unwrap();
    assert_eq!(decoded.len(), 8);
}

#[tokio::test]
async fn access_point_control_plane_crud() {
    let state: SharedS3State = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("000000000000", "us-east-1", ""),
    ));
    let delivery = Arc::new(fakecloud_core::delivery::DeliveryBus::new());
    let service = crate::S3Service::new(state, delivery);

    // Pre-create bucket.
    let req = AwsRequest {
        service: "s3".to_string(),
        action: String::new(),
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        request_id: "req-1".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::new(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec!["ap-bucket".to_string()],
        raw_path: "/ap-bucket".to_string(),
        raw_query: String::new(),
        method: http::Method::PUT,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    service.handle(req).await.unwrap();

    // Create access point.
    let req = AwsRequest {
        service: "s3".to_string(),
        action: String::new(),
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        request_id: "req-2".to_string(),
        headers: {
            let mut h = http::HeaderMap::new();
            h.insert("host", "000000000000.s3-control.us-east-1.localhost.localstack.cloud:4566".parse().unwrap());
            h
        },
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::from_static(b"<CreateAccessPointConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Bucket>ap-bucket</Bucket></CreateAccessPointConfiguration>"),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec!["v20180820".to_string(), "accesspoint".to_string(), "my-ap".to_string()],
        raw_path: "/v20180820/accesspoint/my-ap".to_string(),
        raw_query: String::new(),
        method: http::Method::PUT,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    let resp = service.handle(req).await.unwrap();
    assert_eq!(resp.status, http::StatusCode::OK);
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("AccessPointArn"));
    assert!(body.contains("my-ap-000000000000"));

    // Get access point.
    let req = AwsRequest {
        service: "s3".to_string(),
        action: String::new(),
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        request_id: "req-3".to_string(),
        headers: {
            let mut h = http::HeaderMap::new();
            h.insert(
                "host",
                "000000000000.s3-control.us-east-1.localhost.localstack.cloud:4566"
                    .parse()
                    .unwrap(),
            );
            h
        },
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::new(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![
            "v20180820".to_string(),
            "accesspoint".to_string(),
            "my-ap".to_string(),
        ],
        raw_path: "/v20180820/accesspoint/my-ap".to_string(),
        raw_query: String::new(),
        method: http::Method::GET,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    let resp = service.handle(req).await.unwrap();
    assert_eq!(resp.status, http::StatusCode::OK);
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("my-ap"));
    assert!(body.contains("ap-bucket"));

    // List access points.
    let req = AwsRequest {
        service: "s3".to_string(),
        action: String::new(),
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        request_id: "req-4".to_string(),
        headers: {
            let mut h = http::HeaderMap::new();
            h.insert(
                "host",
                "000000000000.s3-control.us-east-1.localhost.localstack.cloud:4566"
                    .parse()
                    .unwrap(),
            );
            h
        },
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::new(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec!["v20180820".to_string(), "accesspoint".to_string()],
        raw_path: "/v20180820/accesspoint".to_string(),
        raw_query: String::new(),
        method: http::Method::GET,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    let resp = service.handle(req).await.unwrap();
    assert_eq!(resp.status, http::StatusCode::OK);
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("my-ap"));

    // Delete access point.
    let req = AwsRequest {
        service: "s3".to_string(),
        action: String::new(),
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        request_id: "req-5".to_string(),
        headers: {
            let mut h = http::HeaderMap::new();
            h.insert(
                "host",
                "000000000000.s3-control.us-east-1.localhost.localstack.cloud:4566"
                    .parse()
                    .unwrap(),
            );
            h
        },
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::new(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![
            "v20180820".to_string(),
            "accesspoint".to_string(),
            "my-ap".to_string(),
        ],
        raw_path: "/v20180820/accesspoint/my-ap".to_string(),
        raw_query: String::new(),
        method: http::Method::DELETE,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };
    let resp = service.handle(req).await.unwrap();
    assert_eq!(resp.status, http::StatusCode::NO_CONTENT);
}

#[test]
fn access_point_data_plane_routes_to_bucket() {
    let state: SharedS3State = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("000000000000", "us-east-1", ""),
    ));
    let delivery = Arc::new(fakecloud_core::delivery::DeliveryBus::new());
    let service = crate::S3Service::new(state.clone(), delivery);

    // Create bucket and access point directly in state.
    {
        let mut st = state.write();
        let s3_st = st.get_or_create("000000000000");
        s3_st.buckets.insert(
            "ap-data-bucket".to_string(),
            crate::state::S3Bucket::new("ap-data-bucket", "us-east-1", "000000000000"),
        );
        s3_st.access_points.insert(
            "data-ap".to_string(),
            crate::state::S3AccessPoint {
                name: "data-ap".to_string(),
                bucket: "ap-data-bucket".to_string(),
                account_id: "000000000000".to_string(),
                network_origin: "Internet".to_string(),
                vpc_configuration: None,
                creation_date: chrono::Utc::now(),
                public_access_block: None,
                bucket_account_id: Some("000000000000".to_string()),
            },
        );
    }

    let mut req = AwsRequest {
        service: "s3".to_string(),
        action: String::new(),
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        request_id: "req-1".to_string(),
        headers: {
            let mut h = http::HeaderMap::new();
            h.insert(
                "host",
                "data-ap-000000000000.s3-accesspoint.us-east-1.localhost.localstack.cloud:4566"
                    .parse()
                    .unwrap(),
            );
            h
        },
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::new(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec!["data-ap-000000000000".to_string(), "key.txt".to_string()],
        raw_path: "/data-ap-000000000000/key.txt".to_string(),
        raw_query: String::new(),
        method: http::Method::GET,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };

    crate::service::access_points::resolve_access_point(&service, &mut req).unwrap();

    assert_eq!(req.path_segments[0], "ap-data-bucket");
    assert_eq!(req.raw_path, "/ap-data-bucket/key.txt");
}

#[tokio::test]
async fn mpu_complete_on_versioned_bucket_pushes_new_version() {
    // On a versioned bucket, CompleteMultipartUpload must add a NEW version
    // (not mutate/corrupt the prior version's body in place) -- bug-audit
    // 2026-06-20, 4.1.
    let svc = make_service();
    seed_bucket(&svc, "ver");
    {
        let mut mas = svc.state.write();
        let b = mas.default_mut().buckets.get_mut("ver").unwrap();
        b.versioning = Some("Enabled".to_string());
    }

    // v1 via a normal PutObject.
    let put_req = make_request(Method::PUT, "/ver/k", &[], b"original-v1");
    svc.put_object("123456789012", &put_req, "ver", "k")
        .await
        .unwrap();

    // v2 via a single-part multipart upload.
    let init_req = make_request(Method::POST, "/ver/k", &[("uploads", "")], b"");
    let resp = svc
        .create_multipart_upload("123456789012", &init_req, "ver", "k")
        .unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    let start = body.find("<UploadId>").unwrap() + "<UploadId>".len();
    let end = body.find("</UploadId>").unwrap();
    let upload_id = body[start..end].to_string();

    let part = b"multipart-v2-body".to_vec();
    let part_req = make_request(Method::PUT, "/ver/k", &[("partNumber", "1")], &part);
    let r = svc
        .upload_part("123456789012", &part_req, "ver", "k", &upload_id, 1)
        .await
        .unwrap();
    let etag = r.headers.get("etag").unwrap().to_str().unwrap().to_string();

    let complete_xml = format!(
        r#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"#,
    );
    let complete_req = make_request(
        Method::POST,
        "/ver/k",
        &[("uploadId", &upload_id)],
        complete_xml.as_bytes(),
    );
    svc.complete_multipart_upload("123456789012", &complete_req, "ver", "k", &upload_id)
        .unwrap();

    let mas = svc.state.read();
    let b = mas.default_ref().buckets.get("ver").unwrap();
    let versions = b
        .object_versions
        .get("k")
        .expect("k must be tracked in object_versions");
    assert_eq!(
        versions.len(),
        2,
        "PutObject + completed MPU must yield 2 versions, got {}",
        versions.len()
    );
}

// ────────────────────────────────────────────────────────────────
// Persistence regression tests (bug-hunt 2026-07-13).
//
// RenameObject, UpdateObjectEncryption, and the metadata
// inventory/journal-table updates used to mutate RAM only and never
// call the store, so they silently reverted on restart in disk mode.
// A recording store proves the handler now drives the store write
// path (the harness can't restart the server in-sandbox).
// ────────────────────────────────────────────────────────────────

struct RecordingStore {
    inner: fakecloud_persistence::MemoryS3Store,
    calls: parking_lot::Mutex<Vec<String>>,
}

impl RecordingStore {
    fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            inner: fakecloud_persistence::MemoryS3Store::new(),
            calls: parking_lot::Mutex::new(Vec::new()),
        })
    }
    fn record(&self, call: String) {
        self.calls.lock().push(call);
    }
    fn calls(&self) -> Vec<String> {
        self.calls.lock().clone()
    }
}

impl fakecloud_persistence::S3Store for RecordingStore {
    fn load(&self) -> fakecloud_persistence::StoreResult<fakecloud_persistence::S3StateSnapshot> {
        self.inner.load()
    }
    fn put_bucket_meta(
        &self,
        bucket: &str,
        meta: &fakecloud_persistence::BucketMeta,
    ) -> fakecloud_persistence::StoreResult<()> {
        self.record(format!("put_bucket_meta:{bucket}"));
        self.inner.put_bucket_meta(bucket, meta)
    }
    fn put_bucket_subresource(
        &self,
        bucket: &str,
        kind: fakecloud_persistence::BucketSubresource,
        payload: &str,
    ) -> fakecloud_persistence::StoreResult<()> {
        self.record(format!("put_bucket_subresource:{bucket}:{kind:?}"));
        self.inner.put_bucket_subresource(bucket, kind, payload)
    }
    fn delete_bucket_subresource(
        &self,
        bucket: &str,
        kind: fakecloud_persistence::BucketSubresource,
    ) -> fakecloud_persistence::StoreResult<()> {
        self.record(format!("delete_bucket_subresource:{bucket}:{kind:?}"));
        self.inner.delete_bucket_subresource(bucket, kind)
    }
    fn delete_bucket(&self, bucket: &str) -> fakecloud_persistence::StoreResult<()> {
        self.record(format!("delete_bucket:{bucket}"));
        self.inner.delete_bucket(bucket)
    }
    fn put_object(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&str>,
        body: fakecloud_persistence::BodySource,
        meta: &fakecloud_persistence::ObjectMeta,
    ) -> fakecloud_persistence::StoreResult<fakecloud_persistence::BodyRef> {
        self.record(format!("put_object:{bucket}/{key}"));
        self.inner.put_object(bucket, key, version, body, meta)
    }
    fn put_object_meta(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&str>,
        meta: &fakecloud_persistence::ObjectMeta,
    ) -> fakecloud_persistence::StoreResult<()> {
        self.record(format!("put_object_meta:{bucket}/{key}"));
        self.inner.put_object_meta(bucket, key, version, meta)
    }
    fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&str>,
    ) -> fakecloud_persistence::StoreResult<()> {
        self.record(format!("delete_object:{bucket}/{key}"));
        self.inner.delete_object(bucket, key, version)
    }
    fn open_object_body(
        &self,
        body: &fakecloud_persistence::BodyRef,
    ) -> fakecloud_persistence::StoreResult<Bytes> {
        self.inner.open_object_body(body)
    }
    fn mpu_create(
        &self,
        bucket: &str,
        upload_id: &str,
        init: &fakecloud_persistence::MpuInit,
    ) -> fakecloud_persistence::StoreResult<()> {
        self.inner.mpu_create(bucket, upload_id, init)
    }
    fn mpu_put_part(
        &self,
        bucket: &str,
        upload_id: &str,
        part_number: u32,
        body: fakecloud_persistence::BodySource,
        etag: &str,
    ) -> fakecloud_persistence::StoreResult<fakecloud_persistence::BodyRef> {
        self.inner
            .mpu_put_part(bucket, upload_id, part_number, body, etag)
    }
    fn mpu_abort(&self, bucket: &str, upload_id: &str) -> fakecloud_persistence::StoreResult<()> {
        self.inner.mpu_abort(bucket, upload_id)
    }
    fn mpu_complete(
        &self,
        bucket: &str,
        upload_id: &str,
        final_key: &str,
        version: Option<&str>,
        meta: &fakecloud_persistence::ObjectMeta,
    ) -> fakecloud_persistence::StoreResult<fakecloud_persistence::BodyRef> {
        self.inner
            .mpu_complete(bucket, upload_id, final_key, version, meta)
    }
}

fn make_recording_service() -> (S3Service, std::sync::Arc<RecordingStore>) {
    let state: SharedS3State = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let store = RecordingStore::new();
    let svc = S3Service::with_store(state, Arc::new(DeliveryBus::new()), store.clone());
    (svc, store)
}

#[test]
fn rename_object_persists_move_through_store() {
    let (svc, store) = make_recording_service();
    seed_bucket(&svc, "b");
    seed_object(&svc, "b", "old", b"payload");

    let mut req = make_request(Method::PUT, "/b/new", &[("renameObject", "")], b"");
    req.headers
        .insert("x-amz-rename-source", "/old".parse().unwrap());
    svc.rename_object("123456789012", &req, "b", "new").unwrap();

    // In-memory move happened.
    {
        let mas = svc.state.read();
        let b = mas.default_ref().buckets.get("b").unwrap();
        assert!(b.objects.contains_key("new"));
        assert!(!b.objects.contains_key("old"));
    }
    // …and it was persisted: the new key written, the old key deleted.
    let calls = store.calls();
    assert!(
        calls.iter().any(|c| c == "put_object:b/new"),
        "expected put_object for the new key, got {calls:?}"
    );
    assert!(
        calls.iter().any(|c| c == "delete_object:b/old"),
        "expected delete_object for the old key, got {calls:?}"
    );
}

#[test]
fn update_object_encryption_persists_reencrypted_body() {
    let (svc, store) = make_recording_service();
    seed_bucket(&svc, "b");
    seed_object(&svc, "b", "k", b"payload");

    let mut req = make_request(Method::PUT, "/b/k", &[("encryption", "")], b"");
    req.headers
        .insert("x-amz-server-side-encryption", "AES256".parse().unwrap());
    svc.update_object_encryption("123456789012", &req, "b", "k")
        .unwrap();

    // Algorithm changed -> the body path must be re-persisted, not RAM-only.
    let calls = store.calls();
    assert!(
        calls.iter().any(|c| c == "put_object:b/k"),
        "expected put_object to persist the re-encrypted body, got {calls:?}"
    );
    // Metadata reflects the new algorithm in memory too.
    let mas = svc.state.read();
    let obj = mas
        .default_ref()
        .buckets
        .get("b")
        .unwrap()
        .objects
        .get("k")
        .unwrap();
    assert_eq!(obj.sse_algorithm.as_deref(), Some("AES256"));
}

#[test]
fn update_metadata_inventory_table_persists_config() {
    let (svc, store) = make_recording_service();
    seed_bucket(&svc, "b");

    let body = b"<InventoryTableConfiguration><ConfigurationState>ENABLED</ConfigurationState></InventoryTableConfiguration>";
    let req = make_request(Method::PUT, "/b", &[("metadataInventoryTable", "")], body);
    svc.update_bucket_metadata_inventory_table("123456789012", &req, "b")
        .unwrap();

    let calls = store.calls();
    assert!(
        calls
            .iter()
            .any(|c| c == "put_bucket_subresource:b:MetadataConfiguration"),
        "inventory-table update must persist the composite metadata config, got {calls:?}"
    );
    // Journal-table sibling persists the same way.
    let req2 = make_request(Method::PUT, "/b", &[("metadataJournalTable", "")], body);
    svc.update_bucket_metadata_journal_table("123456789012", &req2, "b")
        .unwrap();
    let calls = store.calls();
    assert!(
        calls
            .iter()
            .filter(|c| *c == "put_bucket_subresource:b:MetadataConfiguration")
            .count()
            >= 2,
        "journal-table update must persist too, got {calls:?}"
    );
}

#[test]
fn put_object_retention_mode_without_date_is_malformed_xml() {
    // A retention carrying a Mode but no valid RetainUntilDate is rejected by
    // AWS with 400 MalformedXML, not silently stored (bug-hunt 2026-07-13).
    let svc = make_service();
    seed_bucket(&svc, "b");
    seed_object(&svc, "b", "k", b"payload");

    let body = b"<Retention><Mode>GOVERNANCE</Mode></Retention>";
    let req = make_request(Method::PUT, "/b/k", &[("retention", "")], body);
    let err = match svc.put_object_retention("123456789012", &req, "b", "k") {
        Ok(_) => panic!("mode without retain-until must be rejected"),
        Err(e) => e,
    };
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    assert_eq!(err.code(), "MalformedXML");

    // A well-formed retention (mode + date) still succeeds.
    let ok_body =
        b"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2099-01-01T00:00:00Z</RetainUntilDate></Retention>";
    let ok_req = make_request(Method::PUT, "/b/k", &[("retention", "")], ok_body);
    svc.put_object_retention("123456789012", &ok_req, "b", "k")
        .expect("valid retention must succeed");
}

#[test]
fn list_objects_v2_rejects_non_integer_max_keys() {
    // A non-integer / out-of-range max-keys is a 400 InvalidArgument, not a
    // silent coercion to 1000 (bug-hunt 2026-07-13).
    let svc = make_service();
    seed_bucket(&svc, "b");

    for bad in ["abc", "-1", "99999999999999999999"] {
        let req = make_request(
            Method::GET,
            "/b",
            &[("list-type", "2"), ("max-keys", bad)],
            b"",
        );
        let err = match svc.list_objects_v2("123456789012", &req, "b") {
            Ok(_) => panic!("max-keys={bad} must be rejected"),
            Err(e) => e,
        };
        assert_eq!(err.status(), StatusCode::BAD_REQUEST, "max-keys={bad}");
        assert_eq!(err.code(), "InvalidArgument", "max-keys={bad}");
    }

    // A valid value still works and caps at 1000.
    let ok = make_request(
        Method::GET,
        "/b",
        &[("list-type", "2"), ("max-keys", "5000")],
        b"",
    );
    let resp = svc.list_objects_v2("123456789012", &ok, "b").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(body.contains("<MaxKeys>1000</MaxKeys>"), "body: {body}");
}

#[test]
fn list_object_versions_keeps_low_sorting_common_prefix_on_first_page() {
    // When plain keys fill max-keys before a lower-sorting delimiter-rolled
    // prefix, the prefix must still appear on the page it belongs to instead
    // of being dropped as the marker advances past it (bug-hunt 2026-07-13).
    let svc = make_service();
    seed_bucket(&svc, "b");
    seed_object(&svc, "b", "a/x", b"1");
    seed_object(&svc, "b", "bb", b"2");
    seed_object(&svc, "b", "cc", b"3");

    let req = make_request(
        Method::GET,
        "/b",
        &[("versions", ""), ("delimiter", "/"), ("max-keys", "2")],
        b"",
    );
    let resp = svc.list_object_versions("123456789012", &req, "b").unwrap();
    let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();

    // "a/" sorts before "bb"/"cc" so it must be emitted on page 1, not lost.
    assert!(
        body.contains("<CommonPrefixes><Prefix>a/</Prefix></CommonPrefixes>"),
        "page 1 must keep the a/ CommonPrefix, body: {body}"
    );
    assert!(
        body.contains("<IsTruncated>true</IsTruncated>"),
        "body: {body}"
    );
    assert!(body.contains("<Key>bb</Key>"), "body: {body}");
    // The page is capped at max-keys: "cc" spills to the next page.
    assert!(!body.contains("<Key>cc</Key>"), "body: {body}");
}
