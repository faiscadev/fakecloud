use std::sync::Arc;

use bytes::Bytes;
use chrono::Utc;
use fakecloud_persistence::{BodySource, S3Store};
use md5::{Digest, Md5};
use uuid::Uuid;

use crate::persistence::object_meta_snapshot;
use crate::state::{S3Object, SharedS3State};
use crate::xml_util::extract_tag;

/// Whether eager, synchronous delivery of best-effort S3 reports (server
/// access logs and inventory reports) is enabled.
///
/// Real S3 delivers both asynchronously: access logs are best-effort with
/// minutes-to-hours latency, and inventory reports run on a daily/weekly
/// schedule. A bucket created and destroyed within a single test therefore
/// never accumulates these objects, so it deletes cleanly. fakecloud can
/// deliver them synchronously for users who want to exercise the features,
/// but that breaks the realistic create/destroy lifecycle (the destination
/// bucket is never empty), so it is opt-in via
/// `FAKECLOUD_S3_EAGER_DELIVERY=1`.
pub fn eager_delivery_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FAKECLOUD_S3_EAGER_DELIVERY")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    })
}

/// Parsed logging configuration extracted from the XML stored on the bucket.
pub struct LoggingConfig {
    pub target_bucket: String,
    pub target_prefix: String,
}

/// Parse a `<BucketLoggingStatus>` XML body into a `LoggingConfig`, if logging
/// is enabled (i.e. the `<LoggingEnabled>` element is present).
pub(crate) fn parse_logging_config(xml: &str) -> Option<LoggingConfig> {
    let le_start = xml.find("<LoggingEnabled>")?;
    let le_end = xml.find("</LoggingEnabled>")?;
    let le_body = &xml[le_start + 16..le_end];

    let target_bucket = extract_tag(le_body, "TargetBucket")?;
    let target_prefix = extract_tag(le_body, "TargetPrefix").unwrap_or_default();

    Some(LoggingConfig {
        target_bucket,
        target_prefix,
    })
}

/// Everything needed to describe a single S3 request for access logging.
pub struct AccessLogRequest<'a> {
    pub operation: &'a str,
    pub key: Option<&'a str>,
    pub status: u16,
    pub request_id: &'a str,
    pub method: &'a str,
    pub path: &'a str,
}

/// Generate an S3 access log line in a format similar to AWS.
///
/// See <https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html>
pub fn format_access_log_entry(
    bucket_owner: &str,
    bucket: &str,
    request: &AccessLogRequest<'_>,
) -> String {
    let now = Utc::now();
    let time = now.format("[%d/%b/%Y:%H:%M:%S %z]");
    let key_str = request.key.unwrap_or("-");
    let AccessLogRequest {
        operation,
        status,
        request_id,
        method,
        path,
        ..
    } = request;
    // Simplified log line matching the AWS format fields
    format!(
        "{bucket_owner} {bucket} {time} 127.0.0.1 arn:aws:iam::000000000000:user/testuser {request_id} REST.{operation} {key_str} \"{method} {path} HTTP/1.1\" {status} - - - - - \"-\" \"FakeCloud/1.0\" - - - - -\n"
    )
}

/// After a request has been processed, check whether the source bucket has
/// logging enabled and, if so, write a log entry to the target bucket.
///
/// This should be called at the end of the `handle` method so that every S3
/// operation on a logging-enabled bucket produces a record.
pub fn maybe_write_access_log(
    state: &SharedS3State,
    store: &Arc<dyn S3Store>,
    source_bucket: &str,
    request: &AccessLogRequest<'_>,
) {
    // Real S3 delivers access logs asynchronously and best-effort, so a bucket
    // is empty during a quick create/destroy test. Only deliver synchronously
    // when explicitly opted in.
    if !eager_delivery_enabled() {
        return;
    }

    // Read logging config from the source bucket
    let (logging_config_xml, bucket_owner) = {
        let mas = state.read();
        let acct = match mas.find_account(|s| s.buckets.contains_key(source_bucket)) {
            Some(a) => a,
            None => return,
        };
        let st = match mas.get(acct) {
            Some(s) => s,
            None => return,
        };
        let config_xml = st
            .buckets
            .get(source_bucket)
            .and_then(|b| b.logging_config.clone());
        let owner = st
            .buckets
            .get(source_bucket)
            .map(|b| b.acl_owner_id.clone())
            .unwrap_or_else(|| "unknown".to_string());
        (config_xml, owner)
    };

    let config = match logging_config_xml.and_then(|xml| parse_logging_config(&xml)) {
        Some(c) => c,
        None => return,
    };

    let entry = format_access_log_entry(&bucket_owner, source_bucket, request);

    let now = Utc::now();
    let log_key = format!(
        "{}{}",
        config.target_prefix,
        now.format("%Y-%m-%d-%H-%M-%S-")
    ) + &Uuid::new_v4().to_string()[..8];

    let data = Bytes::from(entry);
    let size = data.len() as u64;
    let etag = format!("{:x}", Md5::digest(&data));

    let log_object = S3Object {
        key: log_key.clone(),
        body: crate::state::memory_body(data.clone()),
        content_type: "text/plain".to_string(),
        etag,
        size,
        last_modified: now,
        storage_class: "STANDARD".to_string(),
        ..Default::default()
    };

    let meta = object_meta_snapshot(&log_object);
    {
        let mut mas = state.write();
        let target_acct = mas
            .find_account(|s| s.buckets.contains_key(&config.target_bucket))
            .map(|a| a.to_string());
        let inserted = if let Some(acct) = target_acct {
            if let Some(st) = mas.get_mut(&acct) {
                if let Some(target) = st.buckets.get_mut(&config.target_bucket) {
                    target.objects.insert(log_key.clone(), log_object);
                    true
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };
        if !inserted {
            return;
        }
    }
    if let Err(err) = store.put_object(
        &config.target_bucket,
        &log_key,
        None,
        BodySource::Bytes(data),
        &meta,
    ) {
        tracing::error!(
            bucket = %config.target_bucket,
            key = %log_key,
            error = %err,
            "failed to persist S3 access log object via store"
        );
    }
}

/// Determine the S3 operation name from the HTTP method and key presence.
pub fn operation_name(method: &http::Method, key: Option<&str>) -> &'static str {
    match (method.as_str(), key) {
        ("GET", None) => "GET.BUCKET",
        ("GET", Some(_)) => "GET.OBJECT",
        ("PUT", None) => "PUT.BUCKET",
        ("PUT", Some(_)) => "PUT.OBJECT",
        ("DELETE", None) => "DELETE.BUCKET",
        ("DELETE", Some(_)) => "DELETE.OBJECT",
        ("HEAD", None) => "HEAD.BUCKET",
        ("HEAD", Some(_)) => "HEAD.OBJECT",
        ("POST", _) => "POST",
        _ => "UNKNOWN",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_logging_config_enabled() {
        let xml = r#"<BucketLoggingStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <LoggingEnabled>
                <TargetBucket>log-bucket</TargetBucket>
                <TargetPrefix>logs/</TargetPrefix>
            </LoggingEnabled>
        </BucketLoggingStatus>"#;

        let config = parse_logging_config(xml).unwrap();
        assert_eq!(config.target_bucket, "log-bucket");
        assert_eq!(config.target_prefix, "logs/");
    }

    #[test]
    fn parse_logging_config_disabled() {
        let xml = r#"<BucketLoggingStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        </BucketLoggingStatus>"#;

        assert!(parse_logging_config(xml).is_none());
    }

    #[test]
    fn format_log_entry_contains_fields() {
        let request = AccessLogRequest {
            operation: "GET.OBJECT",
            key: Some("my-key.txt"),
            status: 200,
            request_id: "req-abc",
            method: "GET",
            path: "/my-bucket/my-key.txt",
        };
        let entry = format_access_log_entry("owner123", "my-bucket", &request);
        assert!(entry.contains("owner123"));
        assert!(entry.contains("my-bucket"));
        assert!(entry.contains("GET.OBJECT"));
        assert!(entry.contains("my-key.txt"));
        assert!(entry.contains("200"));
        assert!(entry.contains("req-abc"));
        assert!(entry.contains("\"GET /my-bucket/my-key.txt HTTP/1.1\""));
    }

    #[test]
    fn parse_logging_config_missing_target_bucket_returns_none() {
        let xml = r#"<BucketLoggingStatus>
            <LoggingEnabled><TargetPrefix>logs/</TargetPrefix></LoggingEnabled>
        </BucketLoggingStatus>"#;
        assert!(parse_logging_config(xml).is_none());
    }

    #[test]
    fn parse_logging_config_empty_prefix_defaults_to_empty_string() {
        let xml = r#"<BucketLoggingStatus>
            <LoggingEnabled><TargetBucket>log</TargetBucket></LoggingEnabled>
        </BucketLoggingStatus>"#;
        let cfg = parse_logging_config(xml).unwrap();
        assert_eq!(cfg.target_bucket, "log");
        assert_eq!(cfg.target_prefix, "");
    }

    #[test]
    fn format_log_entry_replaces_missing_key_with_dash() {
        let request = AccessLogRequest {
            operation: "GET.BUCKET",
            key: None,
            status: 200,
            request_id: "req-x",
            method: "GET",
            path: "/my-bucket",
        };
        let entry = format_access_log_entry("owner", "my-bucket", &request);
        assert!(entry.contains("REST.GET.BUCKET - "));
    }

    #[test]
    fn operation_name_maps_all_common_methods() {
        use http::Method;
        assert_eq!(operation_name(&Method::GET, None), "GET.BUCKET");
        assert_eq!(operation_name(&Method::GET, Some("k")), "GET.OBJECT");
        assert_eq!(operation_name(&Method::PUT, None), "PUT.BUCKET");
        assert_eq!(operation_name(&Method::PUT, Some("k")), "PUT.OBJECT");
        assert_eq!(operation_name(&Method::DELETE, None), "DELETE.BUCKET");
        assert_eq!(operation_name(&Method::DELETE, Some("k")), "DELETE.OBJECT");
        assert_eq!(operation_name(&Method::HEAD, None), "HEAD.BUCKET");
        assert_eq!(operation_name(&Method::HEAD, Some("k")), "HEAD.OBJECT");
        assert_eq!(operation_name(&Method::POST, None), "POST");
        assert_eq!(operation_name(&Method::POST, Some("k")), "POST");
        assert_eq!(operation_name(&Method::OPTIONS, None), "UNKNOWN");
    }
}
