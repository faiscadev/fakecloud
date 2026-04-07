use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Timelike, Utc};
use http::{HeaderMap, Method, StatusCode};
use md5::{Digest, Md5};

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_kms::state::SharedKmsState;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;

use crate::logging;
use crate::state::{AclGrant, S3Bucket, S3Object, SharedS3State};

mod acl;
mod buckets;
mod config;
mod lock;
mod multipart;
mod notifications;
mod objects;
mod tags;

// Re-export notification helpers for use in sub-modules
pub(super) use notifications::{
    deliver_notifications, normalize_notification_ids, normalize_replication_xml, replicate_object,
};

// Used only within this file (parse_cors_config)
use notifications::extract_all_xml_values;

// Re-exports used only in tests
#[cfg(test)]
use notifications::{
    event_matches, key_matches_filters, parse_notification_config, parse_replication_rules,
    NotificationTargetType,
};

pub struct S3Service {
    state: SharedS3State,
    delivery: Arc<DeliveryBus>,
    kms_state: Option<SharedKmsState>,
}

impl S3Service {
    pub fn new(state: SharedS3State, delivery: Arc<DeliveryBus>) -> Self {
        Self {
            state,
            delivery,
            kms_state: None,
        }
    }

    pub fn with_kms(mut self, kms_state: SharedKmsState) -> Self {
        self.kms_state = Some(kms_state);
        self
    }
}

#[async_trait]
impl AwsService for S3Service {
    fn service_name(&self) -> &str {
        "s3"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // S3 REST routing: method + path segments + query params
        let bucket = req.path_segments.first().map(|s| s.as_str());
        // Extract key from the raw path to preserve leading slashes and empty segments.
        // The raw path is like "/bucket/key/parts" — we strip the bucket prefix.
        let key = if let Some(b) = bucket {
            let prefix = format!("/{b}/");
            if req.raw_path.starts_with(&prefix) && req.raw_path.len() > prefix.len() {
                let raw_key = &req.raw_path[prefix.len()..];
                Some(
                    percent_encoding::percent_decode_str(raw_key)
                        .decode_utf8_lossy()
                        .into_owned(),
                )
            } else if req.path_segments.len() > 1 {
                let raw = req.path_segments[1..].join("/");
                Some(
                    percent_encoding::percent_decode_str(&raw)
                        .decode_utf8_lossy()
                        .into_owned(),
                )
            } else {
                None
            }
        } else {
            None
        };

        // Multipart upload operations (checked before main match)
        if let Some(b) = bucket {
            // POST /{bucket}/{key}?uploads — CreateMultipartUpload
            if req.method == Method::POST
                && key.is_some()
                && req.query_params.contains_key("uploads")
            {
                return self.create_multipart_upload(&req, b, key.as_deref().unwrap());
            }

            // POST /{bucket}/{key}?restore
            if req.method == Method::POST
                && key.is_some()
                && req.query_params.contains_key("restore")
            {
                return self.restore_object(&req, b, key.as_deref().unwrap());
            }

            // POST /{bucket}/{key}?uploadId=X — CompleteMultipartUpload
            if req.method == Method::POST && key.is_some() {
                if let Some(upload_id) = req.query_params.get("uploadId").cloned() {
                    return self.complete_multipart_upload(
                        &req,
                        b,
                        key.as_deref().unwrap(),
                        &upload_id,
                    );
                }
            }

            // PUT /{bucket}/{key}?partNumber=N&uploadId=X — UploadPart or UploadPartCopy
            if req.method == Method::PUT && key.is_some() {
                if let (Some(part_num_str), Some(upload_id)) = (
                    req.query_params.get("partNumber").cloned(),
                    req.query_params.get("uploadId").cloned(),
                ) {
                    if let Ok(part_number) = part_num_str.parse::<i64>() {
                        if req.headers.contains_key("x-amz-copy-source") {
                            return self.upload_part_copy(
                                &req,
                                b,
                                key.as_deref().unwrap(),
                                &upload_id,
                                part_number,
                            );
                        }
                        return self.upload_part(
                            &req,
                            b,
                            key.as_deref().unwrap(),
                            &upload_id,
                            part_number,
                        );
                    }
                }
            }

            // DELETE /{bucket}/{key}?uploadId=X — AbortMultipartUpload
            if req.method == Method::DELETE && key.is_some() {
                if let Some(upload_id) = req.query_params.get("uploadId").cloned() {
                    return self.abort_multipart_upload(b, key.as_deref().unwrap(), &upload_id);
                }
            }

            // GET /{bucket}?uploads — ListMultipartUploads
            if req.method == Method::GET
                && key.is_none()
                && req.query_params.contains_key("uploads")
            {
                return self.list_multipart_uploads(b);
            }

            // GET /{bucket}/{key}?uploadId=X — ListParts
            if req.method == Method::GET && key.is_some() {
                if let Some(upload_id) = req.query_params.get("uploadId").cloned() {
                    return self.list_parts(&req, b, key.as_deref().unwrap(), &upload_id);
                }
            }
        }

        // Handle OPTIONS preflight requests (CORS)
        if req.method == Method::OPTIONS {
            if let Some(b_name) = bucket {
                let cors_config = {
                    let state = self.state.read();
                    state
                        .buckets
                        .get(b_name)
                        .and_then(|b| b.cors_config.clone())
                };
                if let Some(ref config) = cors_config {
                    let origin = req
                        .headers
                        .get("origin")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("");
                    let request_method = req
                        .headers
                        .get("access-control-request-method")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("");
                    let rules = parse_cors_config(config);
                    if let Some(rule) = find_cors_rule(&rules, origin, Some(request_method)) {
                        let mut headers = HeaderMap::new();
                        let matched_origin = if rule.allowed_origins.contains(&"*".to_string()) {
                            "*"
                        } else {
                            origin
                        };
                        headers.insert(
                            "access-control-allow-origin",
                            matched_origin
                                .parse()
                                .unwrap_or_else(|_| http::HeaderValue::from_static("")),
                        );
                        headers.insert(
                            "access-control-allow-methods",
                            rule.allowed_methods
                                .join(", ")
                                .parse()
                                .unwrap_or_else(|_| http::HeaderValue::from_static("")),
                        );
                        if !rule.allowed_headers.is_empty() {
                            let ah = if rule.allowed_headers.contains(&"*".to_string()) {
                                req.headers
                                    .get("access-control-request-headers")
                                    .and_then(|v| v.to_str().ok())
                                    .unwrap_or("*")
                                    .to_string()
                            } else {
                                rule.allowed_headers.join(", ")
                            };
                            headers.insert(
                                "access-control-allow-headers",
                                ah.parse()
                                    .unwrap_or_else(|_| http::HeaderValue::from_static("")),
                            );
                        }
                        if let Some(max_age) = rule.max_age_seconds {
                            headers.insert(
                                "access-control-max-age",
                                max_age
                                    .to_string()
                                    .parse()
                                    .unwrap_or_else(|_| http::HeaderValue::from_static("")),
                            );
                        }
                        return Ok(AwsResponse {
                            status: StatusCode::OK,
                            content_type: String::new(),
                            body: Bytes::new(),
                            headers,
                        });
                    }
                }
                return Err(AwsServiceError::aws_error(
                    StatusCode::FORBIDDEN,
                    "CORSResponse",
                    "CORS is not enabled for this bucket",
                ));
            }
        }

        // Capture origin for CORS response headers
        let origin_header = req
            .headers
            .get("origin")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let mut result = match (&req.method, bucket, key.as_deref()) {
            // ListBuckets: GET /
            (&Method::GET, None, None) => self.list_buckets(&req),

            // Bucket-level operations (no key)
            (&Method::PUT, Some(b), None) => {
                if req.query_params.contains_key("tagging") {
                    self.put_bucket_tagging(&req, b)
                } else if req.query_params.contains_key("acl") {
                    self.put_bucket_acl(&req, b)
                } else if req.query_params.contains_key("versioning") {
                    self.put_bucket_versioning(&req, b)
                } else if req.query_params.contains_key("cors") {
                    self.put_bucket_cors(&req, b)
                } else if req.query_params.contains_key("notification") {
                    self.put_bucket_notification(&req, b)
                } else if req.query_params.contains_key("website") {
                    self.put_bucket_website(&req, b)
                } else if req.query_params.contains_key("accelerate") {
                    self.put_bucket_accelerate(&req, b)
                } else if req.query_params.contains_key("publicAccessBlock") {
                    self.put_public_access_block(&req, b)
                } else if req.query_params.contains_key("encryption") {
                    self.put_bucket_encryption(&req, b)
                } else if req.query_params.contains_key("lifecycle") {
                    self.put_bucket_lifecycle(&req, b)
                } else if req.query_params.contains_key("logging") {
                    self.put_bucket_logging(&req, b)
                } else if req.query_params.contains_key("policy") {
                    self.put_bucket_policy(&req, b)
                } else if req.query_params.contains_key("object-lock") {
                    self.put_object_lock_config(&req, b)
                } else if req.query_params.contains_key("replication") {
                    self.put_bucket_replication(&req, b)
                } else if req.query_params.contains_key("ownershipControls") {
                    self.put_bucket_ownership_controls(&req, b)
                } else if req.query_params.contains_key("inventory") {
                    self.put_bucket_inventory(&req, b)
                } else {
                    self.create_bucket(&req, b)
                }
            }
            (&Method::DELETE, Some(b), None) => {
                if req.query_params.contains_key("tagging") {
                    self.delete_bucket_tagging(&req, b)
                } else if req.query_params.contains_key("cors") {
                    self.delete_bucket_cors(b)
                } else if req.query_params.contains_key("website") {
                    self.delete_bucket_website(b)
                } else if req.query_params.contains_key("publicAccessBlock") {
                    self.delete_public_access_block(b)
                } else if req.query_params.contains_key("encryption") {
                    self.delete_bucket_encryption(b)
                } else if req.query_params.contains_key("lifecycle") {
                    self.delete_bucket_lifecycle(b)
                } else if req.query_params.contains_key("policy") {
                    self.delete_bucket_policy(b)
                } else if req.query_params.contains_key("replication") {
                    self.delete_bucket_replication(b)
                } else if req.query_params.contains_key("ownershipControls") {
                    self.delete_bucket_ownership_controls(b)
                } else if req.query_params.contains_key("inventory") {
                    self.delete_bucket_inventory(&req, b)
                } else {
                    self.delete_bucket(&req, b)
                }
            }
            (&Method::HEAD, Some(b), None) => self.head_bucket(b),
            (&Method::GET, Some(b), None) => {
                if req.query_params.contains_key("tagging") {
                    self.get_bucket_tagging(&req, b)
                } else if req.query_params.contains_key("location") {
                    self.get_bucket_location(b)
                } else if req.query_params.contains_key("acl") {
                    self.get_bucket_acl(&req, b)
                } else if req.query_params.contains_key("versioning") {
                    self.get_bucket_versioning(b)
                } else if req.query_params.contains_key("versions") {
                    self.list_object_versions(&req, b)
                } else if req.query_params.contains_key("object-lock") {
                    self.get_object_lock_configuration(b)
                } else if req.query_params.contains_key("cors") {
                    self.get_bucket_cors(b)
                } else if req.query_params.contains_key("notification") {
                    self.get_bucket_notification(b)
                } else if req.query_params.contains_key("website") {
                    self.get_bucket_website(b)
                } else if req.query_params.contains_key("accelerate") {
                    self.get_bucket_accelerate(b)
                } else if req.query_params.contains_key("publicAccessBlock") {
                    self.get_public_access_block(b)
                } else if req.query_params.contains_key("encryption") {
                    self.get_bucket_encryption(b)
                } else if req.query_params.contains_key("lifecycle") {
                    self.get_bucket_lifecycle(b)
                } else if req.query_params.contains_key("logging") {
                    self.get_bucket_logging(b)
                } else if req.query_params.contains_key("policy") {
                    self.get_bucket_policy(b)
                } else if req.query_params.contains_key("replication") {
                    self.get_bucket_replication(b)
                } else if req.query_params.contains_key("ownershipControls") {
                    self.get_bucket_ownership_controls(b)
                } else if req.query_params.contains_key("inventory") {
                    if req.query_params.contains_key("id") {
                        self.get_bucket_inventory(&req, b)
                    } else {
                        self.list_bucket_inventory_configurations(b)
                    }
                } else if req.query_params.get("list-type").map(|s| s.as_str()) == Some("2") {
                    self.list_objects_v2(&req, b)
                } else if req.query_params.is_empty() {
                    // If bucket has website config and no query params, serve index document
                    let website_config = {
                        let state = self.state.read();
                        state
                            .buckets
                            .get(b)
                            .and_then(|bkt| bkt.website_config.clone())
                    };
                    if let Some(ref config) = website_config {
                        if let Some(index_doc) = extract_xml_value(config, "Suffix").or_else(|| {
                            extract_xml_value(config, "IndexDocument").and_then(|inner| {
                                let open = "<Suffix>";
                                let close = "</Suffix>";
                                let s = inner.find(open)? + open.len();
                                let e = inner.find(close)?;
                                Some(inner[s..e].trim().to_string())
                            })
                        }) {
                            self.serve_website_object(&req, b, &index_doc, config)
                        } else {
                            self.list_objects_v1(&req, b)
                        }
                    } else {
                        self.list_objects_v1(&req, b)
                    }
                } else {
                    self.list_objects_v1(&req, b)
                }
            }

            // Object-level operations
            (&Method::PUT, Some(b), Some(k)) => {
                if req.query_params.contains_key("tagging") {
                    self.put_object_tagging(&req, b, k)
                } else if req.query_params.contains_key("acl") {
                    self.put_object_acl(&req, b, k)
                } else if req.query_params.contains_key("retention") {
                    self.put_object_retention(&req, b, k)
                } else if req.query_params.contains_key("legal-hold") {
                    self.put_object_legal_hold(&req, b, k)
                } else if req.headers.contains_key("x-amz-copy-source") {
                    self.copy_object(&req, b, k)
                } else {
                    self.put_object(&req, b, k)
                }
            }
            (&Method::GET, Some(b), Some(k)) => {
                if req.query_params.contains_key("tagging") {
                    self.get_object_tagging(&req, b, k)
                } else if req.query_params.contains_key("acl") {
                    self.get_object_acl(&req, b, k)
                } else if req.query_params.contains_key("retention") {
                    self.get_object_retention(&req, b, k)
                } else if req.query_params.contains_key("legal-hold") {
                    self.get_object_legal_hold(&req, b, k)
                } else if req.query_params.contains_key("attributes") {
                    self.get_object_attributes(&req, b, k)
                } else {
                    let result = self.get_object(&req, b, k);
                    // If object not found and bucket has website config, serve error document
                    let is_not_found = matches!(
                        &result,
                        Err(e) if e.code() == "NoSuchKey"
                    );
                    if is_not_found {
                        let website_config = {
                            let state = self.state.read();
                            state
                                .buckets
                                .get(b)
                                .and_then(|bkt| bkt.website_config.clone())
                        };
                        if let Some(ref config) = website_config {
                            if let Some(error_key) = extract_xml_value(config, "ErrorDocument")
                                .and_then(|inner| {
                                    let open = "<Key>";
                                    let close = "</Key>";
                                    let s = inner.find(open)? + open.len();
                                    let e = inner.find(close)?;
                                    Some(inner[s..e].trim().to_string())
                                })
                                .or_else(|| extract_xml_value(config, "Key"))
                            {
                                return self.serve_website_error(&req, b, &error_key);
                            }
                        }
                    }
                    result
                }
            }
            (&Method::DELETE, Some(b), Some(k)) => {
                if req.query_params.contains_key("tagging") {
                    self.delete_object_tagging(b, k)
                } else {
                    self.delete_object(&req, b, k)
                }
            }
            (&Method::HEAD, Some(b), Some(k)) => self.head_object(&req, b, k),

            // POST /{bucket}?delete — batch delete
            (&Method::POST, Some(b), None) if req.query_params.contains_key("delete") => {
                self.delete_objects(&req, b)
            }

            _ => Err(AwsServiceError::aws_error(
                StatusCode::METHOD_NOT_ALLOWED,
                "MethodNotAllowed",
                "The specified method is not allowed against this resource",
            )),
        };

        // Apply CORS headers to the response if Origin was present
        if let (Some(ref origin), Some(b_name)) = (&origin_header, bucket) {
            let cors_config = {
                let state = self.state.read();
                state
                    .buckets
                    .get(b_name)
                    .and_then(|b| b.cors_config.clone())
            };
            if let Some(ref config) = cors_config {
                let rules = parse_cors_config(config);
                if let Some(rule) = find_cors_rule(&rules, origin, None) {
                    if let Ok(ref mut resp) = result {
                        let matched_origin = if rule.allowed_origins.contains(&"*".to_string()) {
                            "*"
                        } else {
                            origin
                        };
                        resp.headers.insert(
                            "access-control-allow-origin",
                            matched_origin
                                .parse()
                                .unwrap_or_else(|_| http::HeaderValue::from_static("")),
                        );
                        if !rule.expose_headers.is_empty() {
                            resp.headers.insert(
                                "access-control-expose-headers",
                                rule.expose_headers
                                    .join(", ")
                                    .parse()
                                    .unwrap_or_else(|_| http::HeaderValue::from_static("")),
                            );
                        }
                    }
                }
            }
        }

        // Write S3 access log entry if the source bucket has logging enabled
        if let Some(b_name) = bucket {
            let status_code = match &result {
                Ok(resp) => resp.status.as_u16(),
                Err(e) => e.status().as_u16(),
            };
            let op = logging::operation_name(&req.method, key.as_deref());
            logging::maybe_write_access_log(
                &self.state,
                b_name,
                op,
                key.as_deref(),
                status_code,
                &req.request_id,
                req.method.as_str(),
                &req.raw_path,
            );
        }

        result
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            // Buckets
            "ListBuckets",
            "CreateBucket",
            "DeleteBucket",
            "HeadBucket",
            "GetBucketLocation",
            // Objects
            "PutObject",
            "GetObject",
            "DeleteObject",
            "HeadObject",
            "CopyObject",
            "DeleteObjects",
            "ListObjectsV2",
            "ListObjects",
            "ListObjectVersions",
            "GetObjectAttributes",
            "RestoreObject",
            // Object properties
            "PutObjectTagging",
            "GetObjectTagging",
            "DeleteObjectTagging",
            "PutObjectAcl",
            "GetObjectAcl",
            "PutObjectRetention",
            "GetObjectRetention",
            "PutObjectLegalHold",
            "GetObjectLegalHold",
            // Bucket configuration
            "PutBucketTagging",
            "GetBucketTagging",
            "DeleteBucketTagging",
            "PutBucketAcl",
            "GetBucketAcl",
            "PutBucketVersioning",
            "GetBucketVersioning",
            "PutBucketCors",
            "GetBucketCors",
            "DeleteBucketCors",
            "PutBucketNotificationConfiguration",
            "GetBucketNotificationConfiguration",
            "PutBucketWebsite",
            "GetBucketWebsite",
            "DeleteBucketWebsite",
            "PutBucketAccelerateConfiguration",
            "GetBucketAccelerateConfiguration",
            "PutPublicAccessBlock",
            "GetPublicAccessBlock",
            "DeletePublicAccessBlock",
            "PutBucketEncryption",
            "GetBucketEncryption",
            "DeleteBucketEncryption",
            "PutBucketLifecycleConfiguration",
            "GetBucketLifecycleConfiguration",
            "DeleteBucketLifecycle",
            "PutBucketLogging",
            "GetBucketLogging",
            "PutBucketPolicy",
            "GetBucketPolicy",
            "DeleteBucketPolicy",
            "PutObjectLockConfiguration",
            "GetObjectLockConfiguration",
            "PutBucketReplication",
            "GetBucketReplication",
            "DeleteBucketReplication",
            "PutBucketOwnershipControls",
            "GetBucketOwnershipControls",
            "DeleteBucketOwnershipControls",
            "PutBucketInventoryConfiguration",
            "GetBucketInventoryConfiguration",
            "DeleteBucketInventoryConfiguration",
            // Multipart uploads
            "CreateMultipartUpload",
            "UploadPart",
            "UploadPartCopy",
            "CompleteMultipartUpload",
            "AbortMultipartUpload",
            "ListParts",
            "ListMultipartUploads",
        ]
    }
}

// ---------------------------------------------------------------------------
// Conditional request helpers
// ---------------------------------------------------------------------------

/// Truncate a DateTime to second-level precision (HTTP dates have no sub-second info).
pub(crate) fn truncate_to_seconds(dt: DateTime<Utc>) -> DateTime<Utc> {
    dt.with_nanosecond(0).unwrap_or(dt)
}

pub(crate) fn check_get_conditionals(
    req: &AwsRequest,
    obj: &S3Object,
) -> Result<(), AwsServiceError> {
    let obj_etag = format!("\"{}\"", obj.etag);
    let obj_time = truncate_to_seconds(obj.last_modified);

    // If-Match
    if let Some(if_match) = req.headers.get("if-match").and_then(|v| v.to_str().ok()) {
        if !etag_matches(if_match, &obj_etag) {
            return Err(precondition_failed("If-Match"));
        }
    }

    // If-None-Match
    if let Some(if_none_match) = req
        .headers
        .get("if-none-match")
        .and_then(|v| v.to_str().ok())
    {
        if etag_matches(if_none_match, &obj_etag) {
            return Err(not_modified_with_etag(&obj_etag));
        }
    }

    // If-Unmodified-Since
    if let Some(since) = req
        .headers
        .get("if-unmodified-since")
        .and_then(|v| v.to_str().ok())
    {
        if let Some(dt) = parse_http_date(since) {
            if obj_time > dt {
                return Err(precondition_failed("If-Unmodified-Since"));
            }
        }
    }

    // If-Modified-Since
    if let Some(since) = req
        .headers
        .get("if-modified-since")
        .and_then(|v| v.to_str().ok())
    {
        if let Some(dt) = parse_http_date(since) {
            if obj_time <= dt {
                return Err(not_modified());
            }
        }
    }

    Ok(())
}

pub(crate) fn check_head_conditionals(
    req: &AwsRequest,
    obj: &S3Object,
) -> Result<(), AwsServiceError> {
    let obj_etag = format!("\"{}\"", obj.etag);
    let obj_time = truncate_to_seconds(obj.last_modified);

    // If-Match
    if let Some(if_match) = req.headers.get("if-match").and_then(|v| v.to_str().ok()) {
        if !etag_matches(if_match, &obj_etag) {
            return Err(AwsServiceError::aws_error(
                StatusCode::PRECONDITION_FAILED,
                "412",
                "Precondition Failed",
            ));
        }
    }

    // If-None-Match
    if let Some(if_none_match) = req
        .headers
        .get("if-none-match")
        .and_then(|v| v.to_str().ok())
    {
        if etag_matches(if_none_match, &obj_etag) {
            return Err(not_modified_with_etag(&obj_etag));
        }
    }

    // If-Unmodified-Since
    if let Some(since) = req
        .headers
        .get("if-unmodified-since")
        .and_then(|v| v.to_str().ok())
    {
        if let Some(dt) = parse_http_date(since) {
            if obj_time > dt {
                return Err(AwsServiceError::aws_error(
                    StatusCode::PRECONDITION_FAILED,
                    "412",
                    "Precondition Failed",
                ));
            }
        }
    }

    // If-Modified-Since
    if let Some(since) = req
        .headers
        .get("if-modified-since")
        .and_then(|v| v.to_str().ok())
    {
        if let Some(dt) = parse_http_date(since) {
            if obj_time <= dt {
                return Err(not_modified());
            }
        }
    }

    Ok(())
}

pub(crate) fn etag_matches(condition: &str, obj_etag: &str) -> bool {
    let condition = condition.trim();
    if condition == "*" {
        return true;
    }
    let clean_etag = obj_etag.replace('"', "");
    // Split on comma to handle multi-value If-Match / If-None-Match
    for part in condition.split(',') {
        let part = part.trim().replace('"', "");
        if part == clean_etag {
            return true;
        }
    }
    false
}

pub(crate) fn parse_http_date(s: &str) -> Option<DateTime<Utc>> {
    // Try RFC 2822 format: "Sat, 01 Jan 2000 00:00:00 GMT"
    if let Ok(dt) = DateTime::parse_from_rfc2822(s) {
        return Some(dt.with_timezone(&Utc));
    }
    // Try RFC 3339
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }
    // Try common HTTP date format: "%a, %d %b %Y %H:%M:%S GMT"
    if let Ok(dt) =
        chrono::NaiveDateTime::parse_from_str(s.trim_end_matches(" GMT"), "%a, %d %b %Y %H:%M:%S")
    {
        return Some(dt.and_utc());
    }
    // Try ISO 8601
    if let Ok(dt) = s.parse::<DateTime<Utc>>() {
        return Some(dt);
    }
    None
}

pub(crate) fn not_modified() -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::NOT_MODIFIED, "304", "Not Modified")
}

pub(crate) fn not_modified_with_etag(etag: &str) -> AwsServiceError {
    AwsServiceError::aws_error_with_headers(
        StatusCode::NOT_MODIFIED,
        "304",
        "Not Modified",
        vec![("etag".to_string(), etag.to_string())],
    )
}

pub(crate) fn precondition_failed(condition: &str) -> AwsServiceError {
    AwsServiceError::aws_error_with_fields(
        StatusCode::PRECONDITION_FAILED,
        "PreconditionFailed",
        "At least one of the pre-conditions you specified did not hold",
        vec![("Condition".to_string(), condition.to_string())],
    )
}

// ---------------------------------------------------------------------------
// ACL helpers
// ---------------------------------------------------------------------------

pub(crate) fn build_acl_xml(owner_id: &str, grants: &[AclGrant], _account_id: &str) -> String {
    let mut grants_xml = String::new();
    for g in grants {
        let grantee_xml = if g.grantee_type == "Group" {
            let uri = g.grantee_uri.as_deref().unwrap_or("");
            format!(
                "<Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\">\
                 <URI>{}</URI></Grantee>",
                xml_escape(uri),
            )
        } else {
            let id = g.grantee_id.as_deref().unwrap_or("");
            format!(
                "<Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">\
                 <ID>{}</ID></Grantee>",
                xml_escape(id),
            )
        };
        grants_xml.push_str(&format!(
            "<Grant>{grantee_xml}<Permission>{}</Permission></Grant>",
            xml_escape(&g.permission),
        ));
    }

    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Owner><ID>{owner_id}</ID><DisplayName>{owner_id}</DisplayName></Owner>\
         <AccessControlList>{grants_xml}</AccessControlList>\
         </AccessControlPolicy>",
        owner_id = xml_escape(owner_id),
    )
}

pub(crate) fn canned_acl_grants(acl: &str, owner_id: &str) -> Vec<AclGrant> {
    let owner_grant = AclGrant {
        grantee_type: "CanonicalUser".to_string(),
        grantee_id: Some(owner_id.to_string()),
        grantee_display_name: Some(owner_id.to_string()),
        grantee_uri: None,
        permission: "FULL_CONTROL".to_string(),
    };
    match acl {
        "private" => vec![owner_grant],
        "public-read" => vec![
            owner_grant,
            AclGrant {
                grantee_type: "Group".to_string(),
                grantee_id: None,
                grantee_display_name: None,
                grantee_uri: Some("http://acs.amazonaws.com/groups/global/AllUsers".to_string()),
                permission: "READ".to_string(),
            },
        ],
        "public-read-write" => vec![
            owner_grant,
            AclGrant {
                grantee_type: "Group".to_string(),
                grantee_id: None,
                grantee_display_name: None,
                grantee_uri: Some("http://acs.amazonaws.com/groups/global/AllUsers".to_string()),
                permission: "READ".to_string(),
            },
            AclGrant {
                grantee_type: "Group".to_string(),
                grantee_id: None,
                grantee_display_name: None,
                grantee_uri: Some("http://acs.amazonaws.com/groups/global/AllUsers".to_string()),
                permission: "WRITE".to_string(),
            },
        ],
        "authenticated-read" => vec![
            owner_grant,
            AclGrant {
                grantee_type: "Group".to_string(),
                grantee_id: None,
                grantee_display_name: None,
                grantee_uri: Some(
                    "http://acs.amazonaws.com/groups/global/AuthenticatedUsers".to_string(),
                ),
                permission: "READ".to_string(),
            },
        ],
        "bucket-owner-full-control" => vec![owner_grant],
        _ => vec![owner_grant],
    }
}

pub(crate) fn canned_acl_grants_for_object(acl: &str, owner_id: &str) -> Vec<AclGrant> {
    // For objects, canned ACLs work the same way
    canned_acl_grants(acl, owner_id)
}

pub(crate) fn parse_grant_headers(headers: &HeaderMap) -> Vec<AclGrant> {
    let mut grants = Vec::new();
    let header_permission_map = [
        ("x-amz-grant-read", "READ"),
        ("x-amz-grant-write", "WRITE"),
        ("x-amz-grant-read-acp", "READ_ACP"),
        ("x-amz-grant-write-acp", "WRITE_ACP"),
        ("x-amz-grant-full-control", "FULL_CONTROL"),
    ];

    for (header, permission) in &header_permission_map {
        if let Some(value) = headers.get(*header).and_then(|v| v.to_str().ok()) {
            // Parse "id=xxx" or "uri=xxx" or "emailAddress=xxx"
            for part in value.split(',') {
                let part = part.trim();
                if let Some((key, val)) = part.split_once('=') {
                    let val = val.trim().trim_matches('"');
                    let key = key.trim().to_lowercase();
                    match key.as_str() {
                        "id" => {
                            grants.push(AclGrant {
                                grantee_type: "CanonicalUser".to_string(),
                                grantee_id: Some(val.to_string()),
                                grantee_display_name: Some(val.to_string()),
                                grantee_uri: None,
                                permission: permission.to_string(),
                            });
                        }
                        "uri" | "url" => {
                            grants.push(AclGrant {
                                grantee_type: "Group".to_string(),
                                grantee_id: None,
                                grantee_display_name: None,
                                grantee_uri: Some(val.to_string()),
                                permission: permission.to_string(),
                            });
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    grants
}

pub(crate) fn parse_acl_xml(xml: &str) -> Result<Vec<AclGrant>, AwsServiceError> {
    // Check for Owner presence
    if xml.contains("<AccessControlPolicy") && !xml.contains("<Owner>") {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "MalformedACLError",
            "The XML you provided was not well-formed or did not validate against our published schema",
        ));
    }

    let valid_permissions = ["READ", "WRITE", "READ_ACP", "WRITE_ACP", "FULL_CONTROL"];

    let mut grants = Vec::new();
    let mut remaining = xml;
    while let Some(start) = remaining.find("<Grant>") {
        let after = &remaining[start + 7..];
        if let Some(end) = after.find("</Grant>") {
            let grant_body = &after[..end];

            // Extract permission
            let permission = extract_xml_value(grant_body, "Permission").unwrap_or_default();
            if !valid_permissions.contains(&permission.as_str()) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "MalformedACLError",
                    "The XML you provided was not well-formed or did not validate against our published schema",
                ));
            }

            // Determine grantee type
            if grant_body.contains("xsi:type=\"Group\"") || grant_body.contains("<URI>") {
                let uri = extract_xml_value(grant_body, "URI").unwrap_or_default();
                grants.push(AclGrant {
                    grantee_type: "Group".to_string(),
                    grantee_id: None,
                    grantee_display_name: None,
                    grantee_uri: Some(uri),
                    permission,
                });
            } else {
                let id = extract_xml_value(grant_body, "ID").unwrap_or_default();
                let display =
                    extract_xml_value(grant_body, "DisplayName").unwrap_or_else(|| id.clone());
                grants.push(AclGrant {
                    grantee_type: "CanonicalUser".to_string(),
                    grantee_id: Some(id),
                    grantee_display_name: Some(display),
                    grantee_uri: None,
                    permission,
                });
            }

            remaining = &after[end + 8..];
        } else {
            break;
        }
    }
    Ok(grants)
}

// ---------------------------------------------------------------------------
// Range helpers
// ---------------------------------------------------------------------------

pub(crate) enum RangeResult {
    Satisfiable { start: usize, end: usize },
    NotSatisfiable,
    Ignored,
}

pub(crate) fn parse_range_header(range_str: &str, total_size: usize) -> Option<RangeResult> {
    let range_str = range_str.strip_prefix("bytes=")?;
    let (start_str, end_str) = range_str.split_once('-')?;
    if start_str.is_empty() {
        let suffix_len: usize = end_str.parse().ok()?;
        if suffix_len == 0 || total_size == 0 {
            return Some(RangeResult::NotSatisfiable);
        }
        let start = total_size.saturating_sub(suffix_len);
        Some(RangeResult::Satisfiable {
            start,
            end: total_size - 1,
        })
    } else {
        let start: usize = start_str.parse().ok()?;
        if start >= total_size {
            return Some(RangeResult::NotSatisfiable);
        }
        let end = if end_str.is_empty() {
            total_size - 1
        } else {
            let e: usize = end_str.parse().ok()?;
            if e < start {
                return Some(RangeResult::Ignored);
            }
            std::cmp::min(e, total_size - 1)
        };
        Some(RangeResult::Satisfiable { start, end })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// S3 XML response with `application/xml` content type (unlike Query protocol's `text/xml`).
pub(crate) fn s3_xml(status: StatusCode, body: impl Into<Bytes>) -> AwsResponse {
    AwsResponse {
        status,
        content_type: "application/xml".to_string(),
        body: body.into(),
        headers: HeaderMap::new(),
    }
}

pub(crate) fn empty_response(status: StatusCode) -> AwsResponse {
    AwsResponse {
        status,
        content_type: "application/xml".to_string(),
        body: Bytes::new(),
        headers: HeaderMap::new(),
    }
}

/// Returns true when the object is stored in a "cold" storage class (GLACIER, DEEP_ARCHIVE)
/// and has NOT been restored (or restore is still in progress).
pub(crate) fn is_frozen(obj: &S3Object) -> bool {
    matches!(obj.storage_class.as_str(), "GLACIER" | "DEEP_ARCHIVE")
        && obj.restore_ongoing != Some(false)
}

pub(crate) fn no_such_bucket(bucket: &str) -> AwsServiceError {
    AwsServiceError::aws_error_with_fields(
        StatusCode::NOT_FOUND,
        "NoSuchBucket",
        "The specified bucket does not exist",
        vec![("BucketName".to_string(), bucket.to_string())],
    )
}

pub(crate) fn no_such_key(key: &str) -> AwsServiceError {
    AwsServiceError::aws_error_with_fields(
        StatusCode::NOT_FOUND,
        "NoSuchKey",
        "The specified key does not exist.",
        vec![("Key".to_string(), key.to_string())],
    )
}

pub(crate) fn no_such_upload(upload_id: &str) -> AwsServiceError {
    AwsServiceError::aws_error_with_fields(
        StatusCode::NOT_FOUND,
        "NoSuchUpload",
        "The specified upload does not exist. The upload ID may be invalid, \
         or the upload may have been aborted or completed.",
        vec![("UploadId".to_string(), upload_id.to_string())],
    )
}

pub(crate) fn no_such_key_with_detail(key: &str) -> AwsServiceError {
    AwsServiceError::aws_error_with_fields(
        StatusCode::NOT_FOUND,
        "NoSuchKey",
        "The specified key does not exist.",
        vec![("Key".to_string(), key.to_string())],
    )
}

pub(crate) fn compute_md5(data: &[u8]) -> String {
    let digest = Md5::digest(data);
    format!("{:x}", digest)
}

pub(crate) fn compute_checksum(algorithm: &str, data: &[u8]) -> String {
    match algorithm {
        "CRC32" => {
            let crc = crc32fast::hash(data);
            BASE64.encode(crc.to_be_bytes())
        }
        "SHA1" => {
            use sha1::Digest as _;
            let hash = sha1::Sha1::digest(data);
            BASE64.encode(hash)
        }
        "SHA256" => {
            use sha2::Digest as _;
            let hash = sha2::Sha256::digest(data);
            BASE64.encode(hash)
        }
        _ => String::new(),
    }
}

#[allow(dead_code)]
pub(crate) fn url_encode_key(s: &str) -> String {
    percent_encoding::utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC).to_string()
}

pub(crate) fn url_encode_s3_key(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 2);
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(byte as char);
            }
            _ => {
                out.push_str(&format!("%{:02X}", byte));
            }
        }
    }
    out
}

pub(crate) use fakecloud_aws::xml::xml_escape;

pub(crate) fn extract_user_metadata(
    headers: &HeaderMap,
) -> std::collections::HashMap<String, String> {
    let mut meta = std::collections::HashMap::new();
    for (name, value) in headers {
        if let Some(key) = name.as_str().strip_prefix("x-amz-meta-") {
            if let Ok(v) = value.to_str() {
                meta.insert(key.to_string(), v.to_string());
            }
        }
    }
    meta
}

pub(crate) fn is_valid_storage_class(class: &str) -> bool {
    matches!(
        class,
        "STANDARD"
            | "REDUCED_REDUNDANCY"
            | "STANDARD_IA"
            | "ONEZONE_IA"
            | "INTELLIGENT_TIERING"
            | "GLACIER"
            | "DEEP_ARCHIVE"
            | "GLACIER_IR"
            | "OUTPOSTS"
            | "SNOW"
            | "EXPRESS_ONEZONE"
    )
}

pub(crate) fn is_valid_bucket_name(name: &str) -> bool {
    if name.len() < 3 || name.len() > 63 {
        return false;
    }
    // Must start and end with alphanumeric
    let bytes = name.as_bytes();
    if !bytes[0].is_ascii_alphanumeric() || !bytes[bytes.len() - 1].is_ascii_alphanumeric() {
        return false;
    }
    // Only lowercase letters, digits, hyphens, dots (also allow underscores for compatibility)
    name.chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.' || c == '_')
}

pub(crate) fn is_valid_region(region: &str) -> bool {
    // Basic validation: region should match pattern like us-east-1, eu-west-2, etc.
    let valid_regions = [
        "us-east-1",
        "us-east-2",
        "us-west-1",
        "us-west-2",
        "af-south-1",
        "ap-east-1",
        "ap-south-1",
        "ap-south-2",
        "ap-southeast-1",
        "ap-southeast-2",
        "ap-southeast-3",
        "ap-southeast-4",
        "ap-northeast-1",
        "ap-northeast-2",
        "ap-northeast-3",
        "ca-central-1",
        "ca-west-1",
        "eu-central-1",
        "eu-central-2",
        "eu-west-1",
        "eu-west-2",
        "eu-west-3",
        "eu-south-1",
        "eu-south-2",
        "eu-north-1",
        "il-central-1",
        "me-south-1",
        "me-central-1",
        "sa-east-1",
        "cn-north-1",
        "cn-northwest-1",
        "us-gov-east-1",
        "us-gov-east-2",
        "us-gov-west-1",
        "us-iso-east-1",
        "us-iso-west-1",
        "us-isob-east-1",
        "us-isof-south-1",
    ];
    valid_regions.contains(&region)
}

pub(crate) fn resolve_object<'a>(
    b: &'a S3Bucket,
    key: &str,
    version_id: Option<&String>,
) -> Result<&'a S3Object, AwsServiceError> {
    if let Some(vid) = version_id {
        // "null" version ID refers to an object with no version_id (pre-versioning)
        if vid == "null" {
            // Check versions for a pre-versioning object (version_id == None or Some("null"))
            if let Some(versions) = b.object_versions.get(key) {
                if let Some(obj) = versions
                    .iter()
                    .find(|o| o.version_id.is_none() || o.version_id.as_deref() == Some("null"))
                {
                    return Ok(obj);
                }
            }
            // Also check current object if it has no version_id
            if let Some(obj) = b.objects.get(key) {
                if obj.version_id.is_none() || obj.version_id.as_deref() == Some("null") {
                    return Ok(obj);
                }
            }
        } else {
            // When a specific versionId is requested, check versions first
            if let Some(versions) = b.object_versions.get(key) {
                if let Some(obj) = versions
                    .iter()
                    .find(|o| o.version_id.as_deref() == Some(vid.as_str()))
                {
                    return Ok(obj);
                }
            }
            // Also check current object
            if let Some(obj) = b.objects.get(key) {
                if obj.version_id.as_deref() == Some(vid.as_str()) {
                    return Ok(obj);
                }
            }
        }
        // For versioned buckets, return NoSuchVersion; for non-versioned, return 400
        if b.versioning.is_some() {
            Err(AwsServiceError::aws_error_with_fields(
                StatusCode::NOT_FOUND,
                "NoSuchVersion",
                "The specified version does not exist.",
                vec![
                    ("Key".to_string(), key.to_string()),
                    ("VersionId".to_string(), vid.to_string()),
                ],
            ))
        } else {
            Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Invalid version id specified",
            ))
        }
    } else {
        b.objects.get(key).ok_or_else(|| no_such_key(key))
    }
}

pub(crate) fn make_delete_marker(key: &str, dm_id: &str) -> S3Object {
    S3Object {
        key: key.to_string(),
        data: Bytes::new(),
        content_type: String::new(),
        etag: String::new(),
        size: 0,
        last_modified: Utc::now(),
        metadata: std::collections::HashMap::new(),
        storage_class: "STANDARD".to_string(),
        tags: std::collections::HashMap::new(),
        acl_grants: vec![],
        acl_owner_id: None,
        parts_count: None,
        part_sizes: None,
        sse_algorithm: None,
        sse_kms_key_id: None,
        bucket_key_enabled: None,
        version_id: Some(dm_id.to_string()),
        is_delete_marker: true,
        content_encoding: None,
        website_redirect_location: None,
        restore_ongoing: None,
        restore_expiry: None,
        checksum_algorithm: None,
        checksum_value: None,
        lock_mode: None,
        lock_retain_until: None,
        lock_legal_hold: None,
    }
}

#[allow(dead_code)]
pub(crate) fn acl_xml(owner_id: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Owner><ID>{owner_id}</ID><DisplayName>{owner_id}</DisplayName></Owner>\
         <AccessControlList><Grant>\
         <Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">\
         <ID>{owner_id}</ID><DisplayName>{owner_id}</DisplayName></Grantee>\
         <Permission>FULL_CONTROL</Permission></Grant></AccessControlList>\
         </AccessControlPolicy>"
    )
}

/// Represents an object to delete in a batch delete request.
pub(crate) struct DeleteObjectEntry {
    key: String,
    version_id: Option<String>,
}

pub(crate) fn parse_delete_objects_xml(xml: &str) -> Vec<DeleteObjectEntry> {
    let mut entries = Vec::new();
    let mut remaining = xml;
    while let Some(obj_start) = remaining.find("<Object>") {
        let after = &remaining[obj_start + 8..];
        if let Some(obj_end) = after.find("</Object>") {
            let obj_body = &after[..obj_end];
            let key = extract_xml_value(obj_body, "Key");
            let version_id = extract_xml_value(obj_body, "VersionId");
            if let Some(k) = key {
                entries.push(DeleteObjectEntry { key: k, version_id });
            }
            remaining = &after[obj_end + 9..];
        } else {
            break;
        }
    }
    entries
}

/// Minimal XML parser for `<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag>...`.
/// Returns a Vec to preserve insertion order and detect duplicates.
pub(crate) fn parse_tagging_xml(xml: &str) -> Vec<(String, String)> {
    let mut tags = Vec::new();
    let mut remaining = xml;
    while let Some(tag_start) = remaining.find("<Tag>") {
        let after = &remaining[tag_start + 5..];
        if let Some(tag_end) = after.find("</Tag>") {
            let tag_body = &after[..tag_end];
            let key = extract_xml_value(tag_body, "Key");
            let value = extract_xml_value(tag_body, "Value");
            if let (Some(k), Some(v)) = (key, value) {
                tags.push((k, v));
            }
            remaining = &after[tag_end + 6..];
        } else {
            break;
        }
    }
    tags
}

pub(crate) fn validate_tags(tags: &[(String, String)]) -> Result<(), AwsServiceError> {
    // Check for duplicate keys
    let mut seen = std::collections::HashSet::new();
    for (k, _) in tags {
        if !seen.insert(k.as_str()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidTag",
                "Cannot provide multiple Tags with the same key",
            ));
        }
        // Check for aws: prefix
        if k.starts_with("aws:") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidTag",
                "System tags cannot be added/updated by requester",
            ));
        }
    }
    Ok(())
}

pub(crate) fn extract_xml_value(xml: &str, tag: &str) -> Option<String> {
    // Handle self-closing tags like <Value /> or <Value/>
    let self_closing1 = format!("<{tag} />");
    let self_closing2 = format!("<{tag}/>");
    if xml.contains(&self_closing1) || xml.contains(&self_closing2) {
        // Check if the self-closing tag appears before any open+close pair
        let self_pos = xml
            .find(&self_closing1)
            .or_else(|| xml.find(&self_closing2));
        let open = format!("<{tag}>");
        let open_pos = xml.find(&open);
        match (self_pos, open_pos) {
            (Some(sp), Some(op)) if sp < op => return Some(String::new()),
            (Some(_), None) => return Some(String::new()),
            _ => {}
        }
    }

    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml.find(&close)?;
    Some(xml[start..end].to_string())
}

/// Parse the CompleteMultipartUpload XML body into (part_number, etag) pairs.
pub(crate) fn parse_complete_multipart_xml(xml: &str) -> Vec<(u32, String)> {
    let mut parts = Vec::new();
    let mut remaining = xml;
    while let Some(part_start) = remaining.find("<Part>") {
        let after = &remaining[part_start + 6..];
        if let Some(part_end) = after.find("</Part>") {
            let part_body = &after[..part_end];
            let part_num =
                extract_xml_value(part_body, "PartNumber").and_then(|s| s.parse::<u32>().ok());
            let etag = extract_xml_value(part_body, "ETag")
                .map(|s| s.replace("&quot;", "").replace('"', ""));
            if let (Some(num), Some(e)) = (part_num, etag) {
                parts.push((num, e));
            }
            remaining = &after[part_end + 7..];
        } else {
            break;
        }
    }
    parts
}

pub(crate) fn parse_url_encoded_tags(s: &str) -> Vec<(String, String)> {
    let mut tags = Vec::new();
    for pair in s.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = match pair.find('=') {
            Some(pos) => (&pair[..pos], &pair[pos + 1..]),
            None => (pair, ""),
        };
        tags.push((
            percent_encoding::percent_decode_str(key)
                .decode_utf8_lossy()
                .to_string(),
            percent_encoding::percent_decode_str(value)
                .decode_utf8_lossy()
                .to_string(),
        ));
    }
    tags
}

/// Validate lifecycle configuration XML. Returns MalformedXML on invalid configs.
pub(crate) fn validate_lifecycle_xml(xml: &str) -> Result<(), AwsServiceError> {
    let malformed = || {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against our published schema",
        )
    };

    let mut remaining = xml;
    while let Some(rule_start) = remaining.find("<Rule>") {
        let after = &remaining[rule_start + 6..];
        if let Some(rule_end) = after.find("</Rule>") {
            let rule_body = &after[..rule_end];

            // Must have Filter or Prefix
            let has_filter = rule_body.contains("<Filter>")
                || rule_body.contains("<Filter/>")
                || rule_body.contains("<Filter />");

            // Check for <Prefix> at rule level (outside of <Filter>...</Filter>)
            let has_prefix_outside_filter = {
                if !rule_body.contains("<Prefix") {
                    false
                } else if !has_filter {
                    true // No filter means any Prefix is at rule level
                } else {
                    // Remove the Filter block and check if Prefix remains
                    let mut stripped = rule_body.to_string();
                    // Remove <Filter>...</Filter> or self-closing variants
                    if let Some(fs) = stripped.find("<Filter") {
                        if let Some(fe) = stripped.find("</Filter>") {
                            stripped = format!("{}{}", &stripped[..fs], &stripped[fe + 9..]);
                        }
                    }
                    stripped.contains("<Prefix")
                }
            };

            if !has_filter && !has_prefix_outside_filter {
                return Err(malformed());
            }
            // Can't have both Filter and rule-level Prefix
            if has_filter && has_prefix_outside_filter {
                return Err(malformed());
            }

            // Expiration: if has ExpiredObjectDeleteMarker, cannot also have Days or Date
            // (only check within <Expiration> block)
            if let Some(exp_start) = rule_body.find("<Expiration>") {
                if let Some(exp_end) = rule_body[exp_start..].find("</Expiration>") {
                    let exp_body = &rule_body[exp_start..exp_start + exp_end];
                    if exp_body.contains("<ExpiredObjectDeleteMarker>")
                        && (exp_body.contains("<Days>") || exp_body.contains("<Date>"))
                    {
                        return Err(malformed());
                    }
                }
            }

            // Filter validation
            if has_filter {
                if let Some(fs) = rule_body.find("<Filter>") {
                    if let Some(fe) = rule_body.find("</Filter>") {
                        let filter_body = &rule_body[fs + 8..fe];
                        let has_prefix_in_filter = filter_body.contains("<Prefix");
                        let has_tag_in_filter = filter_body.contains("<Tag>");
                        let has_and_in_filter = filter_body.contains("<And>");
                        // Can't have both Prefix and Tag without And
                        if has_prefix_in_filter && has_tag_in_filter && !has_and_in_filter {
                            return Err(malformed());
                        }
                        // Can't have Tag and And simultaneously at the Filter level
                        if has_tag_in_filter && has_and_in_filter {
                            // Check if the <Tag> is outside <And>
                            let and_start = filter_body.find("<And>").unwrap_or(0);
                            let tag_pos = filter_body.find("<Tag>").unwrap_or(0);
                            if tag_pos < and_start {
                                return Err(malformed());
                            }
                        }
                    }
                }
            }

            // NoncurrentVersionTransition must have NoncurrentDays and StorageClass
            if rule_body.contains("<NoncurrentVersionTransition>") {
                let mut nvt_remaining = rule_body;
                while let Some(nvt_start) = nvt_remaining.find("<NoncurrentVersionTransition>") {
                    let nvt_after = &nvt_remaining[nvt_start + 29..];
                    if let Some(nvt_end) = nvt_after.find("</NoncurrentVersionTransition>") {
                        let nvt_body = &nvt_after[..nvt_end];
                        if !nvt_body.contains("<NoncurrentDays>") {
                            return Err(malformed());
                        }
                        if !nvt_body.contains("<StorageClass>") {
                            return Err(malformed());
                        }
                        nvt_remaining = &nvt_after[nvt_end + 30..];
                    } else {
                        break;
                    }
                }
            }

            remaining = &after[rule_end + 7..];
        } else {
            break;
        }
    }

    Ok(())
}

/// Parsed CORS rule from bucket configuration XML.
pub(crate) struct CorsRule {
    allowed_origins: Vec<String>,
    allowed_methods: Vec<String>,
    allowed_headers: Vec<String>,
    expose_headers: Vec<String>,
    max_age_seconds: Option<u32>,
}

/// Parse CORS configuration XML into rules.
pub(crate) fn parse_cors_config(xml: &str) -> Vec<CorsRule> {
    let mut rules = Vec::new();
    let mut remaining = xml;
    while let Some(start) = remaining.find("<CORSRule>") {
        let after = &remaining[start + 10..];
        if let Some(end) = after.find("</CORSRule>") {
            let block = &after[..end];
            let allowed_origins = extract_all_xml_values(block, "AllowedOrigin");
            let allowed_methods = extract_all_xml_values(block, "AllowedMethod");
            let allowed_headers = extract_all_xml_values(block, "AllowedHeader");
            let expose_headers = extract_all_xml_values(block, "ExposeHeader");
            let max_age_seconds =
                extract_xml_value(block, "MaxAgeSeconds").and_then(|s| s.parse().ok());
            rules.push(CorsRule {
                allowed_origins,
                allowed_methods,
                allowed_headers,
                expose_headers,
                max_age_seconds,
            });
            remaining = &after[end + 11..];
        } else {
            break;
        }
    }
    rules
}

/// Match an origin against a CORS allowed origin pattern (supports "*" wildcard).
pub(crate) fn origin_matches(origin: &str, pattern: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    // Simple wildcard: *.example.com
    if let Some(suffix) = pattern.strip_prefix('*') {
        return origin.ends_with(suffix);
    }
    origin == pattern
}

/// Find the matching CORS rule for a given origin and method.
pub(crate) fn find_cors_rule<'a>(
    rules: &'a [CorsRule],
    origin: &str,
    method: Option<&str>,
) -> Option<&'a CorsRule> {
    rules.iter().find(|rule| {
        let origin_ok = rule
            .allowed_origins
            .iter()
            .any(|o| origin_matches(origin, o));
        let method_ok = match method {
            Some(m) => rule.allowed_methods.iter().any(|am| am == m),
            None => true,
        };
        origin_ok && method_ok
    })
}

/// Check if an object is locked (retention or legal hold) and should block mutation.
/// Returns an error string if locked, None if allowed.
pub(crate) fn check_object_lock_for_overwrite(
    obj: &S3Object,
    req: &AwsRequest,
) -> Option<&'static str> {
    // Legal hold blocks overwrite
    if obj.lock_legal_hold.as_deref() == Some("ON") {
        return Some("AccessDenied");
    }
    // Retention check
    if let (Some(mode), Some(until)) = (&obj.lock_mode, &obj.lock_retain_until) {
        if *until > Utc::now() {
            if mode == "COMPLIANCE" {
                return Some("AccessDenied");
            }
            if mode == "GOVERNANCE" {
                let bypass = req
                    .headers
                    .get("x-amz-bypass-governance-retention")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.eq_ignore_ascii_case("true"))
                    .unwrap_or(false);
                if !bypass {
                    return Some("AccessDenied");
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let xml = r#"<Delete><Object><Key>a.txt</Key></Object><Object><Key>b/c.txt</Key></Object></Delete>"#;
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
        let xml =
            r#"<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>"#;
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
            data: Bytes::from_static(b"x"),
            content_type: "text/plain".to_string(),
            etag: "\"abc\"".to_string(),
            size: 1,
            last_modified: Utc::now(),
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
            version_id: vid.map(|s| s.to_string()),
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
            data: Bytes::from_static(b"hello"),
            content_type: "text/plain".to_string(),
            etag: "abc".to_string(),
            size: 5,
            last_modified: Utc::now(),
            metadata: Default::default(),
            storage_class: "STANDARD".to_string(),
            tags: Default::default(),
            acl_grants: Vec::new(),
            acl_owner_id: None,
            parts_count: None,
            part_sizes: None,
            sse_algorithm: None,
            sse_kms_key_id: None,
            bucket_key_enabled: None,
            version_id: Some("v1".to_string()),
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
        assert_eq!(dest_obj.unwrap().data, Bytes::from_static(b"hello"));
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
}
