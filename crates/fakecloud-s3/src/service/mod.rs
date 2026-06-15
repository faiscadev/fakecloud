use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Timelike, Utc};
use http::{HeaderMap, Method, StatusCode};
use md5::{Digest, Md5};

use fakecloud_aws::arn::Arn;
use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_kms::SharedKmsState;
use fakecloud_persistence::{MemoryS3Store, S3Store, StoreError};

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;

use crate::logging;
use crate::state::{AclGrant, S3Bucket, S3Object, SharedS3State};

mod access_points;
mod acl;
mod buckets;
pub(crate) mod config;
mod lock;
mod multipart;
mod notifications;
mod objects;
mod tags;

// Re-export notification helpers for use in sub-modules
#[cfg(test)]
use notifications::replicate_object;
pub(super) use notifications::{
    deliver_notifications, normalize_notification_ids, normalize_replication_xml,
    replicate_through_store,
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
    pub(crate) kms_hook: Option<Arc<dyn fakecloud_core::delivery::KmsHook>>,
    store: Arc<dyn S3Store>,
}

/// Map a [`StoreError`] from the persistence layer to a 500 InternalError
/// response. Invoked at every mutation site when the write-through persistence
/// call fails: the in-memory mutation has already happened, but we surface the
/// failure to the client so they know to retry (and so logs/metrics flag it).
pub(crate) fn persistence_error(err: StoreError) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "InternalError",
        format!("persistence store error: {err}"),
    )
}

/// Convert a filesystem IO error from a disk-backed body read into an
/// InternalError response.
pub(crate) fn io_to_aws(err: std::io::Error) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "InternalError",
        format!("failed to read object body from disk: {err}"),
    )
}

impl S3Service {
    pub fn new(state: SharedS3State, delivery: Arc<DeliveryBus>) -> Self {
        Self::with_store(state, delivery, Arc::new(MemoryS3Store::new()))
    }

    pub fn with_store(
        state: SharedS3State,
        delivery: Arc<DeliveryBus>,
        store: Arc<dyn S3Store>,
    ) -> Self {
        Self {
            state,
            delivery,
            kms_state: None,
            kms_hook: None,
            store,
        }
    }

    pub fn with_kms(mut self, kms_state: SharedKmsState) -> Self {
        self.kms_state = Some(kms_state);
        self
    }

    pub fn with_kms_hook(mut self, hook: Arc<dyn fakecloud_core::delivery::KmsHook>) -> Self {
        self.kms_hook = Some(hook);
        self
    }

    /// Encrypt object body bytes for SSE-KMS storage. Returns ciphertext as
    /// raw bytes (a UTF-8 fakecloud-kms envelope) on success.
    ///
    /// Fail-closed: if the KMS hook reports an error (key denied, key not
    /// found, etc.), this returns `Err` so PutObject aborts with a 500
    /// rather than silently storing plaintext. AWS S3 has the same
    /// behavior — `KMS.NotFoundException` and friends surface as
    /// `AccessDenied` / `KMS.*` errors back to the caller. When no hook is
    /// wired (legacy / in-process tests with no KMS dependency), the
    /// plaintext is returned unchanged so existing tests keep working.
    pub(crate) fn encrypt_object_body(
        &self,
        account_id: &str,
        region: &str,
        bucket: &str,
        plaintext: &[u8],
        kms_key_id: Option<&str>,
    ) -> Result<bytes::Bytes, AwsServiceError> {
        let Some(hook) = &self.kms_hook else {
            return Ok(bytes::Bytes::copy_from_slice(plaintext));
        };
        let key = kms_key_id.filter(|k| !k.is_empty()).unwrap_or("aws/s3");
        let bucket_arn = Arn::s3(bucket).to_string();
        let mut ctx = std::collections::HashMap::new();
        ctx.insert("aws:s3:arn".to_string(), bucket_arn);
        match hook.encrypt(account_id, region, key, plaintext, "s3.amazonaws.com", ctx) {
            Ok(envelope) => Ok(bytes::Bytes::from(envelope.into_bytes())),
            Err(err) => {
                tracing::warn!(bucket = %bucket, error = %err, "SSE-KMS encrypt failed");
                Err(AwsServiceError::aws_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "KMS.InternalFailureException",
                    format!("Failed to encrypt object via KMS: {err}"),
                ))
            }
        }
    }

    /// Decrypt object body bytes that were stored as a fakecloud-kms
    /// envelope. Caller is expected to gate this on
    /// `obj.sse_algorithm == Some("aws:kms")`.
    ///
    /// Fail-closed: when a hook is wired and the bytes look like an
    /// envelope but don't decrypt (key revoked, malformed ciphertext),
    /// this returns `Err` so GetObject surfaces a 500. When no hook is
    /// wired, or the bytes aren't UTF-8 (legacy snapshots from before
    /// the hook landed), the bytes are returned unchanged.
    pub(crate) fn decrypt_object_body(
        &self,
        account_id: &str,
        bucket: &str,
        ciphertext: &[u8],
    ) -> Result<bytes::Bytes, AwsServiceError> {
        let Some(hook) = &self.kms_hook else {
            return Ok(bytes::Bytes::copy_from_slice(ciphertext));
        };
        // Stored envelope is base64 ASCII; non-UTF-8 bytes are pre-hook
        // legacy snapshots, return as-is.
        let envelope = match std::str::from_utf8(ciphertext) {
            Ok(s) => s,
            Err(_) => return Ok(bytes::Bytes::copy_from_slice(ciphertext)),
        };
        let bucket_arn = Arn::s3(bucket).to_string();
        let mut ctx = std::collections::HashMap::new();
        ctx.insert("aws:s3:arn".to_string(), bucket_arn);
        match hook.decrypt(account_id, envelope, "s3.amazonaws.com", ctx) {
            Ok(bytes) => Ok(bytes::Bytes::from(bytes)),
            Err(err) => {
                tracing::warn!(bucket = %bucket, error = %err, "SSE-KMS decrypt failed");
                Err(AwsServiceError::aws_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "KMS.InternalFailureException",
                    format!("Failed to decrypt object via KMS: {err}"),
                ))
            }
        }
    }
}

#[async_trait]
impl AwsService for S3Service {
    fn service_name(&self) -> &str {
        "s3"
    }

    async fn handle(&self, mut req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // PutObject / UploadPart enter dispatch via the streaming path
        // with `body = Bytes::new()` and the raw HTTP body available on
        // `req.body_stream`. Those handlers consume the stream directly
        // — spooling chunks to disk while computing MD5 + size in
        // constant memory — so a 1 GiB upload never materializes into
        // RAM. Every *other* PUT-on-bucket-key the streaming dispatch
        // flagged (PutObjectTagging, PutObjectAcl, PutObjectRetention,
        // PutObjectLegalHold, CopyObject, …) reads a small XML/JSON
        // body from `req.body`, so drain the stream for them.
        let is_put_to_key = req.method == Method::PUT
            && req.path_segments.len() >= 2
            && req
                .path_segments
                .first()
                .map(|s| !s.is_empty())
                .unwrap_or(false);
        let q = &req.query_params;
        let is_put_object = is_put_to_key
            && !q.contains_key("tagging")
            && !q.contains_key("acl")
            && !q.contains_key("retention")
            && !q.contains_key("legal-hold")
            && !q.contains_key("renameObject")
            && !q.contains_key("encryption")
            && !req.headers.contains_key("x-amz-copy-source");
        // UploadPart requires both partNumber AND uploadId; checking
        // only partNumber would skip body draining for stray PUTs that
        // happened to carry a partNumber query param without being a
        // real multipart upload.
        let is_upload_part =
            is_put_to_key && q.contains_key("partNumber") && q.contains_key("uploadId");
        if !is_put_object && !is_upload_part {
            if let Some(stream) = req.take_body_stream() {
                req.body = fakecloud_core::service::drain_request_stream(stream).await?;
            }
        }

        // Access point data plane: resolve alias -> bucket before routing.
        access_points::resolve_access_point(self, &mut req)?;

        let account_id = req.account_id.as_str();

        // S3 Control endpoint (access point management).
        let host = req
            .headers
            .get("host")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let is_control = host.to_ascii_lowercase().contains("s3-control");
        if is_control {
            let v1 = req.path_segments.first().map(|s| s.as_str());
            let v2 = req.path_segments.get(1).map(|s| s.as_str());
            let v3 = req.path_segments.get(2).map(|s| s.as_str());
            if v1 == Some("v20180820") && v2 == Some("accesspoint") {
                if let Some(name) = v3 {
                    match req.method {
                        Method::PUT => return self.create_access_point(account_id, &req, name),
                        Method::GET => return self.get_access_point(account_id, &req, name),
                        Method::DELETE => return self.delete_access_point(account_id, &req, name),
                        _ => {}
                    }
                } else if req.method == Method::GET {
                    return self.list_access_points(account_id, &req);
                }
            }
        }

        // S3 REST routing: method + path segments + query params
        //
        // Reject paths that start with `//` (an empty bucket segment). The
        // path-segment splitter further down filters empty segments out,
        // which would otherwise let a malformed URL like `PUT //my-key`
        // collapse to a valid `PUT /my-key` (CreateBucket) and silently
        // succeed. Real S3 rejects empty bucket segments with
        // InvalidBucketName, so mirror that.
        if req.raw_path.starts_with("//") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidBucketName",
                "The specified bucket is not valid: bucket name cannot be empty",
            ));
        }
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
                return self.create_multipart_upload(account_id, &req, b, key.as_deref().unwrap());
            }

            // POST /{bucket}/{key}?restore
            if req.method == Method::POST
                && key.is_some()
                && req.query_params.contains_key("restore")
            {
                return self.restore_object(account_id, &req, b, key.as_deref().unwrap());
            }

            // POST /{bucket}/{key}?uploadId=X — CompleteMultipartUpload
            if req.method == Method::POST && key.is_some() {
                if let Some(upload_id) = req.query_params.get("uploadId").cloned() {
                    return self.complete_multipart_upload(
                        account_id,
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
                                account_id,
                                &req,
                                b,
                                key.as_deref().unwrap(),
                                &upload_id,
                                part_number,
                            );
                        }
                        return self
                            .upload_part(
                                account_id,
                                &req,
                                b,
                                key.as_deref().unwrap(),
                                &upload_id,
                                part_number,
                            )
                            .await;
                    }
                }
            }

            // DELETE /{bucket}/{key}?uploadId=X — AbortMultipartUpload
            if req.method == Method::DELETE && key.is_some() {
                if let Some(upload_id) = req.query_params.get("uploadId").cloned() {
                    return self.abort_multipart_upload(
                        account_id,
                        b,
                        key.as_deref().unwrap(),
                        &upload_id,
                    );
                }
            }

            // GET /{bucket}?uploads — ListMultipartUploads
            if req.method == Method::GET
                && key.is_none()
                && req.query_params.contains_key("uploads")
            {
                return self.list_multipart_uploads(account_id, b);
            }

            // GET /{bucket}/{key}?uploadId=X — ListParts
            if req.method == Method::GET && key.is_some() {
                if let Some(upload_id) = req.query_params.get("uploadId").cloned() {
                    return self.list_parts(
                        account_id,
                        &req,
                        b,
                        key.as_deref().unwrap(),
                        &upload_id,
                    );
                }
            }
        }

        // Handle OPTIONS preflight requests (CORS)
        if req.method == Method::OPTIONS {
            if let Some(b_name) = bucket {
                let cors_config = {
                    let accounts = self.state.read();
                    let _empty_s3 = crate::state::S3State::new(&req.account_id, &req.region);
                    let state = accounts.get(&req.account_id).unwrap_or(&_empty_s3);
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
                            body: Bytes::new().into(),
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

        // Bucket-scoped sub-resource query params. If a request targets one
        // of these without a bucket in the path, S3 returns an error rather
        // than silently treating it as a top-level (service) operation.
        // Real AWS rejects e.g. `GET /?tagging` with a routing/validation
        // error; mirroring that prevents bucket-required ops from being
        // accidentally answered by ListBuckets / WriteGetObjectResponse.
        let bucket_subresources: &[&str] = &[
            "tagging",
            "acl",
            "versioning",
            "cors",
            "notification",
            "website",
            "accelerate",
            "publicAccessBlock",
            "encryption",
            "lifecycle",
            "logging",
            "policy",
            "policyStatus",
            "replication",
            "ownershipControls",
            "inventory",
            "analytics",
            "intelligent-tiering",
            "metrics",
            "requestPayment",
            "abac",
            "metadataConfiguration",
            "metadataTable",
            "metadataInventoryTable",
            "metadataJournalTable",
            "object-lock",
            "versions",
            "session",
            "retention",
            "legal-hold",
            "attributes",
            "torrent",
            "renameObject",
            "uploads",
            "restore",
            "select",
            // Bucket-required ops the earlier list missed: their query
            // markers (e.g. `?location` for GetBucketLocation,
            // `?delete` for DeleteObjects, `?list-type=2` for
            // ListObjectsV2) signal a bucket op so a request with the
            // marker but no bucket segment is malformed, not a service-
            // level call.
            "location",
            "delete",
            "list-type",
        ];
        if bucket.is_none()
            && bucket_subresources
                .iter()
                .any(|q| req.query_params.contains_key(*q))
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidBucketName",
                "The specified bucket is not valid: bucket name is required for this operation",
            ));
        }

        let mut result = match (&req.method, bucket, key.as_deref()) {
            // ListBuckets: GET /
            (&Method::GET, None, None) => {
                if req.query_params.get("x-id").map(|s| s.as_str()) == Some("ListDirectoryBuckets")
                {
                    self.list_directory_buckets(account_id, &req)
                } else {
                    self.list_buckets(account_id, &req)
                }
            }

            // Bucket-level operations (no key)
            (&Method::PUT, Some(b), None) => {
                if req.query_params.contains_key("tagging") {
                    self.put_bucket_tagging(account_id, &req, b)
                } else if req.query_params.contains_key("acl") {
                    self.put_bucket_acl(account_id, &req, b)
                } else if req.query_params.contains_key("versioning") {
                    self.put_bucket_versioning(account_id, &req, b)
                } else if req.query_params.contains_key("cors") {
                    self.put_bucket_cors(account_id, &req, b)
                } else if req.query_params.contains_key("notification") {
                    self.put_bucket_notification(account_id, &req, b)
                } else if req.query_params.contains_key("website") {
                    self.put_bucket_website(account_id, &req, b)
                } else if req.query_params.contains_key("accelerate") {
                    self.put_bucket_accelerate(account_id, &req, b)
                } else if req.query_params.contains_key("publicAccessBlock") {
                    self.put_public_access_block(account_id, &req, b)
                } else if req.query_params.contains_key("encryption") {
                    self.put_bucket_encryption(account_id, &req, b)
                } else if req.query_params.contains_key("lifecycle") {
                    self.put_bucket_lifecycle(account_id, &req, b)
                } else if req.query_params.contains_key("logging") {
                    self.put_bucket_logging(account_id, &req, b)
                } else if req.query_params.contains_key("policy") {
                    self.put_bucket_policy(account_id, &req, b)
                } else if req.query_params.contains_key("object-lock") {
                    self.put_object_lock_config(account_id, &req, b)
                } else if req.query_params.contains_key("replication") {
                    self.put_bucket_replication(account_id, &req, b)
                } else if req.query_params.contains_key("ownershipControls") {
                    self.put_bucket_ownership_controls(account_id, &req, b)
                } else if req.query_params.contains_key("inventory") {
                    self.put_bucket_inventory(account_id, &req, b)
                } else if req.query_params.contains_key("analytics") {
                    self.put_bucket_analytics_config(account_id, &req, b)
                } else if req.query_params.contains_key("intelligent-tiering") {
                    self.put_bucket_intelligent_tiering_config(account_id, &req, b)
                } else if req.query_params.contains_key("metrics") {
                    self.put_bucket_metrics_config(account_id, &req, b)
                } else if req.query_params.contains_key("requestPayment") {
                    self.put_bucket_request_payment(account_id, &req, b)
                } else if req.query_params.contains_key("abac") {
                    self.put_bucket_abac(account_id, &req, b)
                } else if req.query_params.contains_key("metadataInventoryTable") {
                    self.update_bucket_metadata_inventory_table(account_id, &req, b)
                } else if req.query_params.contains_key("metadataJournalTable") {
                    self.update_bucket_metadata_journal_table(account_id, &req, b)
                } else {
                    self.create_bucket(account_id, &req, b)
                }
            }
            (&Method::DELETE, Some(b), None) => {
                if req.query_params.contains_key("tagging") {
                    self.delete_bucket_tagging(account_id, &req, b)
                } else if req.query_params.contains_key("cors") {
                    self.delete_bucket_cors(account_id, b)
                } else if req.query_params.contains_key("website") {
                    self.delete_bucket_website(account_id, b)
                } else if req.query_params.contains_key("publicAccessBlock") {
                    self.delete_public_access_block(account_id, b)
                } else if req.query_params.contains_key("encryption") {
                    self.delete_bucket_encryption(account_id, b)
                } else if req.query_params.contains_key("lifecycle") {
                    self.delete_bucket_lifecycle(account_id, b)
                } else if req.query_params.contains_key("policy") {
                    self.delete_bucket_policy(account_id, b)
                } else if req.query_params.contains_key("replication") {
                    self.delete_bucket_replication(account_id, b)
                } else if req.query_params.contains_key("ownershipControls") {
                    self.delete_bucket_ownership_controls(account_id, b)
                } else if req.query_params.contains_key("inventory") {
                    self.delete_bucket_inventory(account_id, &req, b)
                } else if req.query_params.contains_key("analytics") {
                    self.delete_bucket_analytics_config(account_id, &req, b)
                } else if req.query_params.contains_key("intelligent-tiering") {
                    self.delete_bucket_intelligent_tiering_config(account_id, &req, b)
                } else if req.query_params.contains_key("metrics") {
                    self.delete_bucket_metrics_config(account_id, &req, b)
                } else if req.query_params.contains_key("metadataConfiguration") {
                    self.delete_bucket_metadata_config(account_id, b)
                } else if req.query_params.contains_key("metadataTable") {
                    self.delete_bucket_metadata_table_config(account_id, b)
                } else {
                    self.delete_bucket(account_id, &req, b)
                }
            }
            (&Method::HEAD, Some(b), None) => self.head_bucket(account_id, b),
            (&Method::GET, Some(b), None) => {
                if req.query_params.contains_key("tagging") {
                    self.get_bucket_tagging(account_id, &req, b)
                } else if req.query_params.contains_key("location") {
                    self.get_bucket_location(account_id, b)
                } else if req.query_params.contains_key("acl") {
                    self.get_bucket_acl(account_id, &req, b)
                } else if req.query_params.contains_key("versioning") {
                    self.get_bucket_versioning(account_id, b)
                } else if req.query_params.contains_key("versions") {
                    self.list_object_versions(account_id, &req, b)
                } else if req.query_params.contains_key("object-lock") {
                    self.get_object_lock_configuration(account_id, b)
                } else if req.query_params.contains_key("cors") {
                    self.get_bucket_cors(account_id, b)
                } else if req.query_params.contains_key("notification") {
                    self.get_bucket_notification(account_id, b)
                } else if req.query_params.contains_key("website") {
                    self.get_bucket_website(account_id, b)
                } else if req.query_params.contains_key("accelerate") {
                    self.get_bucket_accelerate(account_id, b)
                } else if req.query_params.contains_key("publicAccessBlock") {
                    self.get_public_access_block(account_id, b)
                } else if req.query_params.contains_key("encryption") {
                    self.get_bucket_encryption(account_id, b)
                } else if req.query_params.contains_key("lifecycle") {
                    self.get_bucket_lifecycle(account_id, b)
                } else if req.query_params.contains_key("logging") {
                    self.get_bucket_logging(account_id, b)
                } else if req.query_params.contains_key("policy") {
                    self.get_bucket_policy(account_id, b)
                } else if req.query_params.contains_key("replication") {
                    self.get_bucket_replication(account_id, b)
                } else if req.query_params.contains_key("ownershipControls") {
                    self.get_bucket_ownership_controls(account_id, b)
                } else if req.query_params.contains_key("inventory") {
                    if req.query_params.contains_key("id") {
                        self.get_bucket_inventory(account_id, &req, b)
                    } else {
                        self.list_bucket_inventory_configurations(account_id, b)
                    }
                } else if req.query_params.contains_key("analytics") {
                    if req.query_params.contains_key("id") {
                        self.get_bucket_analytics_config(account_id, &req, b)
                    } else {
                        self.list_bucket_analytics_configurations(account_id, b)
                    }
                } else if req.query_params.contains_key("intelligent-tiering") {
                    if req.query_params.contains_key("id") {
                        self.get_bucket_intelligent_tiering_config(account_id, &req, b)
                    } else {
                        self.list_bucket_intelligent_tiering_configurations(account_id, b)
                    }
                } else if req.query_params.contains_key("metrics") {
                    if req.query_params.contains_key("id") {
                        self.get_bucket_metrics_config(account_id, &req, b)
                    } else {
                        self.list_bucket_metrics_configurations(account_id, b)
                    }
                } else if req.query_params.contains_key("requestPayment") {
                    self.get_bucket_request_payment(account_id, b)
                } else if req.query_params.contains_key("abac") {
                    self.get_bucket_abac(account_id, b)
                } else if req.query_params.contains_key("policyStatus") {
                    self.get_bucket_policy_status(account_id, b)
                } else if req.query_params.contains_key("metadataConfiguration") {
                    self.get_bucket_metadata_config(account_id, b)
                } else if req.query_params.contains_key("metadataTable") {
                    self.get_bucket_metadata_table_config(account_id, b)
                } else if req.query_params.contains_key("session") {
                    self.create_session(account_id, &req, b)
                } else if req.query_params.get("list-type").map(|s| s.as_str()) == Some("2") {
                    self.list_objects_v2(account_id, &req, b)
                } else if req.query_params.is_empty() {
                    // If bucket has website config and no query params, serve index document
                    let website_config = {
                        let accounts = self.state.read();
                        let _empty_s3 = crate::state::S3State::new(&req.account_id, &req.region);
                        let state = accounts.get(&req.account_id).unwrap_or(&_empty_s3);
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
                            self.serve_website_object(account_id, &req, b, &index_doc, config)
                        } else {
                            self.list_objects_v1(account_id, &req, b)
                        }
                    } else {
                        self.list_objects_v1(account_id, &req, b)
                    }
                } else {
                    self.list_objects_v1(account_id, &req, b)
                }
            }

            // Object-level operations
            (&Method::PUT, Some(b), Some(k)) => {
                if req.query_params.contains_key("tagging") {
                    self.put_object_tagging(account_id, &req, b, k)
                } else if req.query_params.contains_key("acl") {
                    self.put_object_acl(account_id, &req, b, k)
                } else if req.query_params.contains_key("retention") {
                    self.put_object_retention(account_id, &req, b, k)
                } else if req.query_params.contains_key("legal-hold") {
                    self.put_object_legal_hold(account_id, &req, b, k)
                } else if req.query_params.contains_key("renameObject") {
                    self.rename_object(account_id, &req, b, k)
                } else if req.query_params.contains_key("encryption") {
                    self.update_object_encryption(account_id, &req, b, k)
                } else if req.headers.contains_key("x-amz-copy-source") {
                    self.copy_object(account_id, &req, b, k)
                } else {
                    self.put_object(account_id, &req, b, k).await
                }
            }
            (&Method::GET, Some(b), Some(k)) => {
                if req.query_params.contains_key("tagging") {
                    self.get_object_tagging(account_id, &req, b, k)
                } else if req.query_params.contains_key("acl") {
                    self.get_object_acl(account_id, &req, b, k)
                } else if req.query_params.contains_key("retention") {
                    self.get_object_retention(account_id, &req, b, k)
                } else if req.query_params.contains_key("legal-hold") {
                    self.get_object_legal_hold(account_id, &req, b, k)
                } else if req.query_params.contains_key("attributes") {
                    self.get_object_attributes(account_id, &req, b, k)
                } else if req.query_params.contains_key("torrent") {
                    self.get_object_torrent(account_id, &req, b, k)
                } else {
                    let result = self.get_object(account_id, &req, b, k);
                    // If object not found and bucket has website config, serve error document
                    let is_not_found = matches!(
                        &result,
                        Err(e) if e.code() == "NoSuchKey"
                    );
                    if is_not_found {
                        let website_config = {
                            let accounts = self.state.read();
                            let _empty_s3 =
                                crate::state::S3State::new(&req.account_id, &req.region);
                            let state = accounts.get(&req.account_id).unwrap_or(&_empty_s3);
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
                                return self.serve_website_error(account_id, &req, b, &error_key);
                            }
                        }
                    }
                    result
                }
            }
            (&Method::DELETE, Some(b), Some(k)) => {
                if req.query_params.contains_key("tagging") {
                    self.delete_object_tagging(account_id, b, k)
                } else {
                    self.delete_object(account_id, &req, b, k)
                }
            }
            (&Method::HEAD, Some(b), Some(k)) => self.head_object(account_id, &req, b, k),

            // POST /{bucket}?delete — batch delete
            (&Method::POST, Some(b), None) if req.query_params.contains_key("delete") => {
                self.delete_objects(account_id, &req, b)
            }
            (&Method::POST, Some(b), None)
                if req.query_params.contains_key("metadataConfiguration") =>
            {
                self.create_bucket_metadata_config(account_id, &req, b)
            }
            (&Method::POST, Some(b), None) if req.query_params.contains_key("metadataTable") => {
                self.create_bucket_metadata_table_config(account_id, &req, b)
            }
            (&Method::POST, Some(b), Some(k))
                if req.query_params.get("select-type").map(|s| s.as_str()) == Some("2") =>
            {
                self.select_object_content(account_id, &req, b, k)
            }
            (&Method::POST, Some("WriteGetObjectResponse"), None) => {
                self.write_get_object_response(account_id, &req)
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
                let accounts = self.state.read();
                let _empty_s3 = crate::state::S3State::new(&req.account_id, &req.region);
                let state = accounts.get(&req.account_id).unwrap_or(&_empty_s3);
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
                &self.store,
                b_name,
                &logging::AccessLogRequest {
                    operation: op,
                    key: key.as_deref(),
                    status: status_code,
                    request_id: &req.request_id,
                    method: req.method.as_str(),
                    path: &req.raw_path,
                },
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
            "ListBucketInventoryConfigurations",
            "PutBucketAnalyticsConfiguration",
            "GetBucketAnalyticsConfiguration",
            "DeleteBucketAnalyticsConfiguration",
            "ListBucketAnalyticsConfigurations",
            "PutBucketIntelligentTieringConfiguration",
            "GetBucketIntelligentTieringConfiguration",
            "DeleteBucketIntelligentTieringConfiguration",
            "ListBucketIntelligentTieringConfigurations",
            "PutBucketMetricsConfiguration",
            "GetBucketMetricsConfiguration",
            "DeleteBucketMetricsConfiguration",
            "ListBucketMetricsConfigurations",
            "PutBucketRequestPayment",
            "GetBucketRequestPayment",
            "PutBucketAbac",
            "GetBucketAbac",
            "GetBucketPolicyStatus",
            "CreateBucketMetadataConfiguration",
            "GetBucketMetadataConfiguration",
            "DeleteBucketMetadataConfiguration",
            "CreateBucketMetadataTableConfiguration",
            "GetBucketMetadataTableConfiguration",
            "DeleteBucketMetadataTableConfiguration",
            "UpdateBucketMetadataInventoryTableConfiguration",
            "UpdateBucketMetadataJournalTableConfiguration",
            "GetObjectTorrent",
            "RenameObject",
            "SelectObjectContent",
            "UpdateObjectEncryption",
            "WriteGetObjectResponse",
            "ListDirectoryBuckets",
            "CreateSession",
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

    fn iam_enforceable(&self) -> bool {
        true
    }

    /// S3 resources are either:
    /// - Bucket ARN (`arn:aws:s3:::bucket`) for bucket-level actions
    /// - Object ARN (`arn:aws:s3:::bucket/key`) for object-level actions
    /// - Wildcard (`*`) for `ListBuckets` which doesn't target a specific
    ///   resource
    ///
    /// S3 ARNs notably omit the account id and region — this is the one
    /// AWS service that carries neither in its ARN, because bucket names
    /// are globally unique.
    fn iam_action_for(&self, request: &AwsRequest) -> Option<fakecloud_core::auth::IamAction> {
        // S3 doesn't set `request.action` — the handler dispatches on
        // method + path + query params directly. Re-derive the action
        // name here so enforcement can match against IAM policies the
        // same way the real service would.
        let bucket = request.path_segments.first().map(|s| s.as_str());
        let key = if request.path_segments.len() > 1 {
            Some(request.path_segments[1..].join("/"))
        } else {
            None
        };
        let action = s3_detect_action(
            request.method.as_str(),
            bucket,
            key.as_deref(),
            &request.query_params,
        )?;
        let resource = s3_resource_for(action, bucket, key.as_deref());
        Some(fakecloud_core::auth::IamAction {
            service: "s3",
            action,
            resource,
        })
    }

    fn iam_condition_keys_for(
        &self,
        request: &AwsRequest,
        action: &fakecloud_core::auth::IamAction,
    ) -> std::collections::BTreeMap<String, Vec<String>> {
        s3_condition_keys(action.action, &request.query_params)
    }

    fn resource_tags_for(
        &self,
        resource_arn: &str,
    ) -> Option<std::collections::HashMap<String, String>> {
        s3_resource_tags(&self.state, resource_arn)
            .map(|m| m.into_iter().collect::<std::collections::HashMap<_, _>>())
    }

    fn request_tags_from(
        &self,
        request: &AwsRequest,
        action: &str,
    ) -> Option<std::collections::HashMap<String, String>> {
        s3_request_tags(request, action)
            .map(|m| m.into_iter().collect::<std::collections::HashMap<_, _>>())
    }
}

/// Extract service-specific IAM condition keys from an S3 request.
///
/// Today only `ListObjects` / `ListObjectsV2` expose keys (`s3:prefix`,
/// `s3:delimiter`, `s3:max-keys`) via their query params. Other actions
/// return an empty map so the evaluator's safe-fail semantics treat any
/// policy condition referencing an unknown key as "doesn't apply".
fn s3_condition_keys(
    action: &str,
    query: &std::collections::HashMap<String, String>,
) -> std::collections::BTreeMap<String, Vec<String>> {
    let mut out = std::collections::BTreeMap::new();
    if matches!(action, "ListObjects" | "ListObjectsV2") {
        // Both list variants share the same query param shape.
        if let Some(prefix) = query.get("prefix") {
            out.insert("s3:prefix".to_string(), vec![prefix.clone()]);
        }
        if let Some(delimiter) = query.get("delimiter") {
            out.insert("s3:delimiter".to_string(), vec![delimiter.clone()]);
        }
        if let Some(max_keys) = query.get("max-keys") {
            out.insert("s3:max-keys".to_string(), vec![max_keys.clone()]);
        }
    }
    out
}

/// Look up resource tags for an S3 ARN.
///
/// Bucket-level ARN (`arn:aws:s3:::bucket`) -> bucket tags.
/// Object-level ARN (`arn:aws:s3:::bucket/key`) -> object tags.
/// `*` (ListBuckets) -> `Some(empty)` (no resource to tag).
fn s3_resource_tags(
    state: &SharedS3State,
    resource_arn: &str,
) -> Option<std::collections::BTreeMap<String, String>> {
    if resource_arn == "*" {
        return Some(std::collections::BTreeMap::new());
    }
    // S3 ARNs: arn:aws:s3:::bucket or arn:aws:s3:::bucket/key
    let after_prefix = resource_arn.strip_prefix("arn:aws:s3:::")?;
    let mas = state.read();
    // S3 bucket names are globally unique; scan all accounts to find the bucket
    let bucket_name = after_prefix.split('/').next().unwrap_or(after_prefix);
    let state = mas
        .find_account(|s| s.buckets.contains_key(bucket_name))
        .and_then(|id| mas.get(id))
        .or_else(|| Some(mas.default_ref()))?;
    if let Some(slash_pos) = after_prefix.find('/') {
        // Object-level: bucket/key
        let bucket_name = &after_prefix[..slash_pos];
        let key = &after_prefix[slash_pos + 1..];
        let bucket = state.buckets.get(bucket_name)?;
        // Try current objects first, then versioned objects (latest version)
        if let Some(obj) = bucket.objects.get(key) {
            Some(obj.tags.clone())
        } else if let Some(versions) = bucket.object_versions.get(key) {
            versions.last().map(|v| v.tags.clone())
        } else {
            // Object doesn't exist yet (e.g. PutObject creating it)
            Some(std::collections::BTreeMap::new())
        }
    } else {
        // Bucket-level
        let bucket = state.buckets.get(after_prefix)?;
        Some(bucket.tags.clone())
    }
}

/// Extract tags from an S3 request body/headers.
///
/// S3 sends tags via:
/// - `x-amz-tagging` header (URL-encoded `key=value&...`) on PutObject
/// - XML body on PutBucketTagging / PutObjectTagging
fn s3_request_tags(
    request: &AwsRequest,
    action: &str,
) -> Option<std::collections::BTreeMap<String, String>> {
    match action {
        "PutObject" | "CopyObject" | "CreateMultipartUpload" => {
            // Tags come via x-amz-tagging header
            if let Some(tagging) = request.headers.get("x-amz-tagging") {
                let tags = parse_url_encoded_tags(tagging.to_str().unwrap_or(""));
                Some(tags.into_iter().collect())
            } else {
                Some(std::collections::BTreeMap::new())
            }
        }
        "PutBucketTagging" | "PutObjectTagging" => {
            // Tags come in XML body
            let body = std::str::from_utf8(&request.body).unwrap_or("");
            let tags = parse_tagging_xml(body);
            Some(tags.into_iter().collect())
        }
        _ => Some(std::collections::BTreeMap::new()),
    }
}

/// Derive the IAM action name from an S3 REST request. Handles the
/// common cases (GetObject, PutObject, DeleteObject, ListObjectsV2,
/// CreateBucket, ...) plus a subset of sub-resource operations
/// (`?acl`, `?tagging`, `?versioning`, `?policy`, `?cors`, `?website`,
/// `?lifecycle`, `?encryption`, `?logging`, `?notification`, `?replication`,
/// `?ownershipControls`, `?publicAccessBlock`, `?accelerate`, `?inventory`,
/// `?object-lock`, `?uploads`, `?uploadId`).
///
/// Returns `None` for requests that don't map to a known action — the
/// dispatch layer then skips enforcement for that request rather than
/// guessing (and a warn log fires via the "service is iam_enforceable
/// but has no mapping" branch in dispatch.rs).
fn s3_detect_action(
    method: &str,
    bucket: Option<&str>,
    key: Option<&str>,
    query: &std::collections::HashMap<String, String>,
) -> Option<&'static str> {
    let has = |q: &str| query.contains_key(q);
    let is_get = method == "GET";
    let is_put = method == "PUT";
    let is_post = method == "POST";
    let is_delete = method == "DELETE";

    // Service root
    if bucket.is_none() {
        return match method {
            "GET" => Some("ListBuckets"),
            _ => None,
        };
    }
    let has_key = key.is_some();

    // Multipart sub-resource forms
    if has_key && is_post && has("uploads") {
        return Some("CreateMultipartUpload");
    }
    if has_key && is_post && has("uploadId") {
        return Some("CompleteMultipartUpload");
    }
    if has_key && is_put && has("partNumber") && has("uploadId") {
        return Some("UploadPart");
    }
    if has_key && is_delete && has("uploadId") {
        return Some("AbortMultipartUpload");
    }
    if has_key && is_get && has("uploadId") {
        return Some("ListParts");
    }
    if !has_key && is_get && has("uploads") {
        return Some("ListMultipartUploads");
    }

    // Sub-resource-keyed actions (?acl, ?tagging, ...). Order matters
    // since a request can carry multiple; we pick the most specific.
    // Object-level sub-resources come first (key present).
    if has_key {
        if has("tagging") {
            return Some(match method {
                "GET" => "GetObjectTagging",
                "PUT" => "PutObjectTagging",
                "DELETE" => "DeleteObjectTagging",
                _ => return None,
            });
        }
        if has("acl") {
            return Some(match method {
                "GET" => "GetObjectAcl",
                "PUT" => "PutObjectAcl",
                _ => return None,
            });
        }
        if has("retention") {
            return Some(match method {
                "GET" => "GetObjectRetention",
                "PUT" => "PutObjectRetention",
                _ => return None,
            });
        }
        if has("legal-hold") {
            return Some(match method {
                "GET" => "GetObjectLegalHold",
                "PUT" => "PutObjectLegalHold",
                _ => return None,
            });
        }
        // Identified by cubic on PR #399: both ?attributes and ?restore
        // need method guards — otherwise e.g. GET /bucket/key?restore
        // would be classified as RestoreObject (POST-only in AWS) and
        // IAM-evaluated against s3:RestoreObject instead of s3:GetObject.
        if has("attributes") && is_get {
            return Some("GetObjectAttributes");
        }
        if has("restore") && is_post {
            return Some("RestoreObject");
        }
    }

    // Bucket-level sub-resources (key absent).
    if !has_key {
        if has("tagging") {
            return Some(match method {
                "GET" => "GetBucketTagging",
                "PUT" => "PutBucketTagging",
                "DELETE" => "DeleteBucketTagging",
                _ => return None,
            });
        }
        if has("acl") {
            return Some(match method {
                "GET" => "GetBucketAcl",
                "PUT" => "PutBucketAcl",
                _ => return None,
            });
        }
        if has("versioning") {
            return Some(match method {
                "GET" => "GetBucketVersioning",
                "PUT" => "PutBucketVersioning",
                _ => return None,
            });
        }
        if has("cors") {
            return Some(match method {
                "GET" => "GetBucketCors",
                "PUT" => "PutBucketCors",
                "DELETE" => "DeleteBucketCors",
                _ => return None,
            });
        }
        if has("policy") {
            return Some(match method {
                "GET" => "GetBucketPolicy",
                "PUT" => "PutBucketPolicy",
                "DELETE" => "DeleteBucketPolicy",
                _ => return None,
            });
        }
        if has("website") {
            return Some(match method {
                "GET" => "GetBucketWebsite",
                "PUT" => "PutBucketWebsite",
                "DELETE" => "DeleteBucketWebsite",
                _ => return None,
            });
        }
        if has("lifecycle") {
            return Some(match method {
                "GET" => "GetBucketLifecycleConfiguration",
                "PUT" => "PutBucketLifecycleConfiguration",
                "DELETE" => "DeleteBucketLifecycle",
                _ => return None,
            });
        }
        if has("encryption") {
            return Some(match method {
                "GET" => "GetBucketEncryption",
                "PUT" => "PutBucketEncryption",
                "DELETE" => "DeleteBucketEncryption",
                _ => return None,
            });
        }
        if has("logging") {
            return Some(match method {
                "GET" => "GetBucketLogging",
                "PUT" => "PutBucketLogging",
                _ => return None,
            });
        }
        if has("notification") {
            return Some(match method {
                "GET" => "GetBucketNotificationConfiguration",
                "PUT" => "PutBucketNotificationConfiguration",
                _ => return None,
            });
        }
        if has("replication") {
            return Some(match method {
                "GET" => "GetBucketReplication",
                "PUT" => "PutBucketReplication",
                "DELETE" => "DeleteBucketReplication",
                _ => return None,
            });
        }
        if has("ownershipControls") {
            return Some(match method {
                "GET" => "GetBucketOwnershipControls",
                "PUT" => "PutBucketOwnershipControls",
                "DELETE" => "DeleteBucketOwnershipControls",
                _ => return None,
            });
        }
        if has("publicAccessBlock") {
            return Some(match method {
                "GET" => "GetPublicAccessBlock",
                "PUT" => "PutPublicAccessBlock",
                "DELETE" => "DeletePublicAccessBlock",
                _ => return None,
            });
        }
        if has("accelerate") {
            return Some(match method {
                "GET" => "GetBucketAccelerateConfiguration",
                "PUT" => "PutBucketAccelerateConfiguration",
                _ => return None,
            });
        }
        if has("inventory") {
            return Some(match method {
                "GET" => "GetBucketInventoryConfiguration",
                "PUT" => "PutBucketInventoryConfiguration",
                "DELETE" => "DeleteBucketInventoryConfiguration",
                _ => return None,
            });
        }
        if has("analytics") {
            return Some(match method {
                "GET" if has("id") => "GetBucketAnalyticsConfiguration",
                "GET" => "ListBucketAnalyticsConfigurations",
                "PUT" => "PutBucketAnalyticsConfiguration",
                "DELETE" => "DeleteBucketAnalyticsConfiguration",
                _ => return None,
            });
        }
        if has("intelligent-tiering") {
            return Some(match method {
                "GET" if has("id") => "GetBucketIntelligentTieringConfiguration",
                "GET" => "ListBucketIntelligentTieringConfigurations",
                "PUT" => "PutBucketIntelligentTieringConfiguration",
                "DELETE" => "DeleteBucketIntelligentTieringConfiguration",
                _ => return None,
            });
        }
        if has("metrics") {
            return Some(match method {
                "GET" if has("id") => "GetBucketMetricsConfiguration",
                "GET" => "ListBucketMetricsConfigurations",
                "PUT" => "PutBucketMetricsConfiguration",
                "DELETE" => "DeleteBucketMetricsConfiguration",
                _ => return None,
            });
        }
        if has("requestPayment") {
            return Some(match method {
                "GET" => "GetBucketRequestPayment",
                "PUT" => "PutBucketRequestPayment",
                _ => return None,
            });
        }
        if has("policyStatus") && is_get {
            return Some("GetBucketPolicyStatus");
        }
        if has("metadataConfiguration") {
            return Some(match method {
                "GET" => "GetBucketMetadataConfiguration",
                "POST" => "CreateBucketMetadataConfiguration",
                "DELETE" => "DeleteBucketMetadataConfiguration",
                _ => return None,
            });
        }
        if has("metadataTable") {
            return Some(match method {
                "GET" => "GetBucketMetadataTableConfiguration",
                "POST" => "CreateBucketMetadataTableConfiguration",
                "DELETE" => "DeleteBucketMetadataTableConfiguration",
                _ => return None,
            });
        }
        if has("metadataInventoryTable") && is_put {
            return Some("UpdateBucketMetadataInventoryTableConfiguration");
        }
        if has("metadataJournalTable") && is_put {
            return Some("UpdateBucketMetadataJournalTableConfiguration");
        }
        if has("abac") && is_put {
            return Some("PutBucketAbacConfiguration");
        }
        if has("renameObject") && is_put {
            return Some("RenameObject");
        }
        if has("object-lock") {
            return Some(match method {
                "GET" => "GetObjectLockConfiguration",
                "PUT" => "PutObjectLockConfiguration",
                _ => return None,
            });
        }
        if has("location") {
            return Some("GetBucketLocation");
        }
        if is_post && has("delete") {
            return Some("DeleteObjects");
        }
        if is_get && has("versions") {
            return Some("ListObjectVersions");
        }
    }

    // Plain bucket/object methods.
    match (method, has_key) {
        ("GET", true) => Some("GetObject"),
        ("PUT", true) => {
            // CopyObject uses x-amz-copy-source but we don't have headers
            // handy here — treat both PutObject and CopyObject as PutObject
            // for IAM purposes; CopyObject additionally requires
            // s3:GetObject on the source but that's evaluated per-request
            // by real AWS, not on the PUT call itself.
            Some("PutObject")
        }
        ("DELETE", true) => Some("DeleteObject"),
        ("HEAD", true) => Some("HeadObject"),
        ("GET", false) => {
            if query.contains_key("list-type") {
                Some("ListObjectsV2")
            } else {
                Some("ListObjects")
            }
        }
        ("PUT", false) => Some("CreateBucket"),
        ("DELETE", false) => Some("DeleteBucket"),
        ("HEAD", false) => Some("HeadBucket"),
        _ => None,
    }
}

/// Build the S3 resource ARN for an action. Returns `*` for
/// `ListBuckets` (account-scoped), a bucket ARN for bucket-level
/// configuration actions, or an object ARN for object-level actions.
fn s3_resource_for(action: &'static str, bucket: Option<&str>, key: Option<&str>) -> String {
    // Object-level actions work on `bucket/key`.
    const OBJECT_ACTIONS: &[&str] = &[
        "PutObject",
        "GetObject",
        "DeleteObject",
        "HeadObject",
        "CopyObject",
        "GetObjectAttributes",
        "RestoreObject",
        "PutObjectTagging",
        "GetObjectTagging",
        "DeleteObjectTagging",
        "PutObjectAcl",
        "GetObjectAcl",
        "PutObjectRetention",
        "GetObjectRetention",
        "PutObjectLegalHold",
        "GetObjectLegalHold",
        "CreateMultipartUpload",
        "UploadPart",
        "UploadPartCopy",
        "CompleteMultipartUpload",
        "AbortMultipartUpload",
        "ListParts",
    ];
    if action == "ListBuckets" {
        return "*".to_string();
    }
    let Some(bucket) = bucket else {
        return "*".to_string();
    };
    if OBJECT_ACTIONS.contains(&action) {
        match key {
            Some(k) if !k.is_empty() => Arn::s3(&format!("{bucket}/{k}")).to_string(),
            _ => Arn::s3(&format!("{bucket}/*")).to_string(),
        }
    } else {
        // Bucket-level actions (ListObjectsV2, GetBucketTagging, ...).
        Arn::s3(bucket).to_string()
    }
}

// Conditional request helpers

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

// ACL helpers

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

// Range helpers

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

// Helpers

/// S3 XML response with `application/xml` content type (unlike Query protocol's `text/xml`).
pub(crate) fn s3_xml(status: StatusCode, body: impl Into<Bytes>) -> AwsResponse {
    AwsResponse {
        status,
        content_type: "application/xml".to_string(),
        body: body.into().into(),
        headers: HeaderMap::new(),
    }
}

pub(crate) fn empty_response(status: StatusCode) -> AwsResponse {
    AwsResponse {
        status,
        content_type: "application/xml".to_string(),
        body: Bytes::new().into(),
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
        // CRC32C and CRC64NVME use the same crates as the streaming path so
        // CopyObject / CompleteMultipartUpload produce identical results to
        // a streaming PutObject (1.7).
        "CRC32C" => {
            let crc = crc32c::crc32c(data);
            BASE64.encode(crc.to_be_bytes())
        }
        "CRC64NVME" => {
            let mut hasher = crc64fast_nvme::Digest::new();
            hasher.write(data);
            BASE64.encode(hasher.sum64().to_be_bytes())
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

/// Streaming variant of [`compute_checksum`] for spool files. Reads
/// the file in 1 MiB chunks and feeds each chunk into the hasher so a
/// 1 GiB upload computes its CRC32 / SHA-1 / SHA-256 in constant
/// memory rather than via `tokio::fs::read` (which would allocate the
/// whole file as one buffer).
pub(crate) async fn compute_checksum_streaming(
    algorithm: &str,
    path: &std::path::Path,
) -> Result<String, std::io::Error> {
    use tokio::io::AsyncReadExt;
    let mut file = tokio::fs::File::open(path).await?;
    let mut buf = vec![0u8; 1024 * 1024];
    match algorithm {
        "CRC32" => {
            let mut hasher = crc32fast::Hasher::new();
            loop {
                let n = file.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            Ok(BASE64.encode(hasher.finalize().to_be_bytes()))
        }
        "CRC32C" => {
            let mut crc: u32 = 0;
            loop {
                let n = file.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                crc = crc32c::crc32c_append(crc, &buf[..n]);
            }
            Ok(BASE64.encode(crc.to_be_bytes()))
        }
        "CRC64NVME" => {
            let mut hasher = crc64fast_nvme::Digest::new();
            loop {
                let n = file.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                hasher.write(&buf[..n]);
            }
            Ok(BASE64.encode(hasher.sum64().to_be_bytes()))
        }
        "SHA1" => {
            use sha1::Digest as _;
            let mut hasher = sha1::Sha1::new();
            loop {
                let n = file.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            Ok(BASE64.encode(hasher.finalize()))
        }
        "SHA256" => {
            use sha2::Digest as _;
            let mut hasher = sha2::Sha256::new();
            loop {
                let n = file.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            Ok(BASE64.encode(hasher.finalize()))
        }
        _ => Ok(String::new()),
    }
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
) -> std::collections::BTreeMap<String, String> {
    let mut meta = std::collections::BTreeMap::new();
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
        last_modified: Utc::now(),
        storage_class: "STANDARD".to_string(),
        version_id: Some(dm_id.to_string()),
        is_delete_marker: true,
        ..Default::default()
    }
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

/// Returns true when the request body's top-level <Quiet> element is "true".
/// AWS docs: when Quiet=true, only failed deletes are emitted.
pub(crate) fn parse_delete_objects_quiet(xml: &str) -> bool {
    extract_xml_value(xml, "Quiet")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
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
    // Search for the closing tag AFTER the opening one so a malformed body
    // (closing tag before opening) can't make end < start and panic the
    // slice (bug-audit 2026-05-28, 2.2).
    let end = xml[start..].find(&close)? + start;
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
mod tests;

#[cfg(test)]
mod extract_xml_value_tests {
    use super::extract_xml_value;

    #[test]
    fn returns_inner_value() {
        assert_eq!(
            extract_xml_value("<Root><Key>value</Key></Root>", "Key"),
            Some("value".to_string())
        );
    }

    #[test]
    fn missing_tag_is_none() {
        assert_eq!(extract_xml_value("<Root></Root>", "Key"), None);
    }

    // bug-audit 2026-05-28, 2.2: a closing tag before the opening one used to
    // slice with end < start and panic; must return None instead.
    #[test]
    fn close_before_open_does_not_panic() {
        assert_eq!(extract_xml_value("</Key>oops<Key>value", "Key"), None);
    }

    #[test]
    fn open_without_close_is_none() {
        assert_eq!(extract_xml_value("<Key>value", "Key"), None);
    }
}

#[cfg(test)]
mod compute_checksum_tests {
    use super::{compute_checksum, compute_checksum_streaming};

    // bug-audit 2026-06-15, 1.7: CopyObject / CompleteMultipartUpload routed
    // through the non-streaming compute_checksum, whose `_ =>` arm returned an
    // empty string for CRC32C / CRC64NVME -> GetObjectAttributes reported an
    // empty checksum and integrity checks silently broke.
    #[test]
    fn crc32c_is_non_empty_and_matches_known_vector() {
        // CRC32C of "123456789" is 0xE3069283; base64 of those 4 BE bytes.
        let out = compute_checksum("CRC32C", b"123456789");
        assert!(!out.is_empty());
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(&out)
            .unwrap();
        assert_eq!(bytes, 0xE306_9283u32.to_be_bytes());
    }

    #[test]
    fn crc64nvme_is_non_empty() {
        let out = compute_checksum("CRC64NVME", b"hello world");
        assert!(!out.is_empty());
        // 8 BE bytes of a u64.
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(&out)
            .unwrap();
        assert_eq!(bytes.len(), 8);
    }

    #[tokio::test]
    async fn non_streaming_matches_streaming_for_all_algorithms() {
        let data = b"the quick brown fox jumps over the lazy dog".repeat(100);
        let tmp = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(tmp.path(), &data).await.unwrap();

        for algo in ["CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256"] {
            let direct = compute_checksum(algo, &data);
            let streamed = compute_checksum_streaming(algo, tmp.path()).await.unwrap();
            assert!(!direct.is_empty(), "{algo} direct empty");
            assert_eq!(direct, streamed, "{algo} direct != streaming");
        }
    }

    use base64::Engine as _;
}
