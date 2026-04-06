use bytes::Bytes;
use chrono::Utc;
use md5::{Digest, Md5};
use uuid::Uuid;

use crate::state::{S3Object, SharedS3State};

/// Parsed logging configuration extracted from the XML stored on the bucket.
pub struct LoggingConfig {
    pub target_bucket: String,
    pub target_prefix: String,
}

/// Parse a `<BucketLoggingStatus>` XML body into a `LoggingConfig`, if logging
/// is enabled (i.e. the `<LoggingEnabled>` element is present).
pub fn parse_logging_config(xml: &str) -> Option<LoggingConfig> {
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

/// Generate an S3 access log line in a format similar to AWS.
///
/// See <https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html>
#[allow(clippy::too_many_arguments)]
pub fn format_access_log_entry(
    bucket_owner: &str,
    bucket: &str,
    operation: &str,
    key: Option<&str>,
    status: u16,
    request_id: &str,
    method: &str,
    path: &str,
) -> String {
    let now = Utc::now();
    let time = now.format("[%d/%b/%Y:%H:%M:%S %z]");
    let key_str = key.unwrap_or("-");
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
#[allow(clippy::too_many_arguments)]
pub fn maybe_write_access_log(
    state: &SharedS3State,
    source_bucket: &str,
    operation: &str,
    key: Option<&str>,
    status: u16,
    request_id: &str,
    method: &str,
    path: &str,
) {
    // Read logging config from the source bucket
    let logging_config_xml = {
        let st = state.read();
        st.buckets
            .get(source_bucket)
            .and_then(|b| b.logging_config.clone())
    };

    let config = match logging_config_xml.and_then(|xml| parse_logging_config(&xml)) {
        Some(c) => c,
        None => return,
    };

    let bucket_owner = {
        let st = state.read();
        st.buckets
            .get(source_bucket)
            .map(|b| b.acl_owner_id.clone())
            .unwrap_or_else(|| "unknown".to_string())
    };

    let entry = format_access_log_entry(
        &bucket_owner,
        source_bucket,
        operation,
        key,
        status,
        request_id,
        method,
        path,
    );

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
        data,
        content_type: "text/plain".to_string(),
        etag,
        size,
        last_modified: now,
        metadata: Default::default(),
        storage_class: "STANDARD".to_string(),
        tags: Default::default(),
        acl_grants: vec![],
        acl_owner_id: None,
        parts_count: None,
        part_sizes: None,
        sse_algorithm: None,
        sse_kms_key_id: None,
        bucket_key_enabled: None,
        version_id: None,
        is_delete_marker: false,
        content_encoding: None,
        website_redirect_location: None,
        restore_ongoing: None,
        restore_expiry: None,
        checksum_algorithm: None,
        checksum_value: None,
        lock_mode: None,
        lock_retain_until: None,
        lock_legal_hold: None,
    };

    let mut st = state.write();
    if let Some(target) = st.buckets.get_mut(&config.target_bucket) {
        target.objects.insert(log_key, log_object);
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

fn extract_tag(body: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = body.find(&open)?;
    let content_start = start + open.len();
    let end = body[content_start..].find(&close)?;
    Some(body[content_start..content_start + end].trim().to_string())
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
        let entry = format_access_log_entry(
            "owner123",
            "my-bucket",
            "GET.OBJECT",
            Some("my-key.txt"),
            200,
            "req-abc",
            "GET",
            "/my-bucket/my-key.txt",
        );
        assert!(entry.contains("owner123"));
        assert!(entry.contains("my-bucket"));
        assert!(entry.contains("GET.OBJECT"));
        assert!(entry.contains("my-key.txt"));
        assert!(entry.contains("200"));
        assert!(entry.contains("req-abc"));
        assert!(entry.contains("\"GET /my-bucket/my-key.txt HTTP/1.1\""));
    }
}
