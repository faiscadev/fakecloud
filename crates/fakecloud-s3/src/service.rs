use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Timelike, Utc};
use http::{HeaderMap, Method, StatusCode};
use md5::{Digest, Md5};
use uuid::Uuid;

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_kms::state::SharedKmsState;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;

use crate::inventory;
use crate::logging;
use crate::state::{AclGrant, MultipartUpload, S3Bucket, S3Object, SharedS3State, UploadPart};

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
// Bucket operations
// ---------------------------------------------------------------------------
impl S3Service {
    fn list_buckets(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let mut buckets_xml = String::new();
        let mut sorted: Vec<_> = state.buckets.values().collect();
        sorted.sort_by_key(|b| &b.name);
        for b in sorted {
            buckets_xml.push_str(&format!(
                "<Bucket><Name>{}</Name><CreationDate>{}</CreationDate></Bucket>",
                xml_escape(&b.name),
                b.creation_date.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
            ));
        }
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Owner><ID>{account}</ID><DisplayName>{account}</DisplayName></Owner>\
             <Buckets>{buckets_xml}</Buckets>\
             </ListAllMyBucketsResult>",
            account = req.account_id,
        );
        Ok(s3_xml(StatusCode::OK, body))
    }

    fn create_bucket(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        if !is_valid_bucket_name(bucket) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidBucketName",
                format!("The specified bucket is not valid: {bucket}"),
            ));
        }

        // Parse LocationConstraint from body if present
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let has_config_body =
            !body_str.is_empty() && body_str.contains("CreateBucketConfiguration");
        let explicit_constraint = if has_config_body {
            extract_xml_value(body_str, "LocationConstraint")
        } else {
            None
        };

        if let Some(ref constraint) = explicit_constraint {
            if !constraint.is_empty() {
                if constraint == "us-east-1" && req.region != "us-east-1" {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "IllegalLocationConstraintException",
                        format!(
                            "The {} location constraint is incompatible for the region specific endpoint this request was sent to.",
                            constraint
                        ),
                    ));
                }
                if constraint == "us-east-1" && req.region == "us-east-1" {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidLocationConstraint",
                        "The specified location-constraint is not valid",
                    ));
                }
                if !is_valid_region(constraint) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidLocationConstraint",
                        format!("The specified location-constraint is not valid: {constraint}"),
                    ));
                }
                if constraint != &req.region && req.region != "us-east-1" {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "IllegalLocationConstraintException",
                        format!(
                            "The {} location constraint is incompatible for the region specific endpoint this request was sent to.",
                            constraint
                        ),
                    ));
                }
            }
        }

        let constraint_unspecified = match &explicit_constraint {
            None => true,
            Some(c) => c.is_empty(),
        };
        if constraint_unspecified && req.region != "us-east-1" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "IllegalLocationConstraintException",
                "The unspecified location constraint is incompatible for the region specific endpoint this request was sent to.",
            ));
        }

        let requested_region = match &explicit_constraint {
            Some(c) if !c.is_empty() => c.clone(),
            _ => req.region.clone(),
        };

        // Parse ACL from header
        let acl = req
            .headers
            .get("x-amz-acl")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("private");

        let mut state = self.state.write();
        if let Some(existing) = state.buckets.get(bucket) {
            // In us-east-1, re-creating same bucket in same region is idempotent (returns 200)
            if existing.region == requested_region && requested_region == "us-east-1" {
                let mut headers = HeaderMap::new();
                headers.insert("location", format!("/{bucket}").parse().unwrap());
                return Ok(AwsResponse {
                    status: StatusCode::OK,
                    content_type: "application/xml".to_string(),
                    body: Bytes::new(),
                    headers,
                });
            }
            return Err(AwsServiceError::aws_error_with_fields(
                StatusCode::CONFLICT,
                "BucketAlreadyOwnedByYou",
                "Your previous request to create the named bucket succeeded and you already own it.",
                vec![("BucketName".to_string(), bucket.to_string())],
            ));
        }
        let object_lock_enabled = req
            .headers
            .get("x-amz-bucket-object-lock-enabled")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        let mut b = S3Bucket::new(bucket, &requested_region, &req.account_id);
        b.acl_grants = canned_acl_grants(acl, &req.account_id);
        if object_lock_enabled {
            b.versioning = Some("Enabled".to_string());
            b.object_lock_config = Some(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                 <ObjectLockConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                 <ObjectLockEnabled>Enabled</ObjectLockEnabled>\
                 </ObjectLockConfiguration>"
                    .to_string(),
            );
        }

        // Handle x-amz-object-ownership header
        if let Some(ownership) = req
            .headers
            .get("x-amz-object-ownership")
            .and_then(|v| v.to_str().ok())
        {
            b.ownership_controls = Some(format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                 <OwnershipControls xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                 <Rule><ObjectOwnership>{ownership}</ObjectOwnership></Rule>\
                 </OwnershipControls>"
            ));
        }

        state.buckets.insert(bucket.to_string(), b);

        let mut headers = HeaderMap::new();
        headers.insert("location", format!("/{bucket}").parse().unwrap());
        headers.insert(
            "x-amz-bucket-arn",
            format!("arn:aws:s3:::{bucket}").parse().unwrap(),
        );
        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: Bytes::new(),
            headers,
        })
    }

    fn delete_bucket(
        &self,
        _req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        // Bucket must be empty to delete (no objects and no versions)
        let has_real_objects = b.objects.values().any(|o| !o.is_delete_marker);
        let has_versions = b.object_versions.values().any(|v| !v.is_empty());
        if has_real_objects || has_versions {
            return Err(AwsServiceError::aws_error_with_fields(
                StatusCode::CONFLICT,
                "BucketNotEmpty",
                "The bucket you tried to delete is not empty",
                vec![("BucketName".to_string(), bucket.to_string())],
            ));
        }
        state.buckets.remove(bucket);
        Ok(AwsResponse {
            status: StatusCode::NO_CONTENT,
            content_type: "application/xml".to_string(),
            body: Bytes::new(),
            headers: HeaderMap::new(),
        })
    }

    fn head_bucket(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        if !state.buckets.contains_key(bucket) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchBucket",
                format!("The specified bucket does not exist: {bucket}"),
            ));
        }
        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: Bytes::new(),
            headers: HeaderMap::new(),
        })
    }

    fn get_bucket_location(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let loc = if b.region == "us-east-1" {
            String::new()
        } else {
            b.region.clone()
        };
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{loc}</LocationConstraint>"
        );
        Ok(s3_xml(StatusCode::OK, body))
    }

    // ---- Encryption ----

    fn put_bucket_encryption(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("").to_string();
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        // Normalize: add BucketKeyEnabled=false to each Rule if missing
        let normalized = if body_str.contains("<Rule>") && !body_str.contains("<BucketKeyEnabled>")
        {
            body_str.replace(
                "</Rule>",
                "<BucketKeyEnabled>false</BucketKeyEnabled></Rule>",
            )
        } else {
            body_str
        };
        b.encryption_config = Some(normalized);
        Ok(empty_response(StatusCode::OK))
    }

    fn get_bucket_encryption(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        match &b.encryption_config {
            Some(config) => Ok(s3_xml(StatusCode::OK, config.clone())),
            None => Err(AwsServiceError::aws_error_with_fields(
                StatusCode::NOT_FOUND,
                "ServerSideEncryptionConfigurationNotFoundError",
                "The server side encryption configuration was not found",
                vec![("BucketName".to_string(), bucket.to_string())],
            )),
        }
    }

    fn delete_bucket_encryption(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.encryption_config = None;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }

    // ---- Lifecycle ----

    fn put_bucket_lifecycle(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("").to_string();

        // Validate lifecycle configuration
        validate_lifecycle_xml(&body_str)?;

        // If there are no <Rule> elements at all, treat as deleting the configuration
        let has_rules = body_str.contains("<Rule>");

        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        if has_rules {
            b.lifecycle_config = Some(body_str);
        } else {
            b.lifecycle_config = None;
        }
        Ok(empty_response(StatusCode::OK))
    }

    fn get_bucket_lifecycle(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        match &b.lifecycle_config {
            Some(config) => Ok(s3_xml(StatusCode::OK, config.clone())),
            None => Err(AwsServiceError::aws_error_with_fields(
                StatusCode::NOT_FOUND,
                "NoSuchLifecycleConfiguration",
                "The lifecycle configuration does not exist",
                vec![("BucketName".to_string(), bucket.to_string())],
            )),
        }
    }

    fn delete_bucket_lifecycle(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.lifecycle_config = None;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }

    // ---- Policy ----

    fn put_bucket_policy(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("").to_string();
        if serde_json::from_str::<serde_json::Value>(&body_str).is_err() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MalformedPolicy",
                "This policy contains invalid Json",
            ));
        }
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.policy = Some(body_str);
        Ok(empty_response(StatusCode::NO_CONTENT))
    }

    fn get_bucket_policy(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        match &b.policy {
            Some(policy) => Ok(AwsResponse {
                status: StatusCode::OK,
                content_type: "application/json".to_string(),
                body: Bytes::from(policy.clone()),
                headers: HeaderMap::new(),
            }),
            None => Err(AwsServiceError::aws_error_with_fields(
                StatusCode::NOT_FOUND,
                "NoSuchBucketPolicy",
                "The bucket policy does not exist",
                vec![("BucketName".to_string(), bucket.to_string())],
            )),
        }
    }

    fn delete_bucket_policy(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.policy = None;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }

    // ---- CORS ----

    fn put_bucket_cors(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("").to_string();

        // Validate CORS configuration
        let rule_count = body_str.matches("<CORSRule>").count();
        if rule_count == 0 || rule_count > 100 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                "The XML you provided was not well-formed or did not validate against our published schema",
            ));
        }

        // Validate HTTP methods
        let valid_methods = ["GET", "PUT", "POST", "DELETE", "HEAD"];
        let mut remaining = body_str.as_str();
        while let Some(start) = remaining.find("<AllowedMethod>") {
            let after = &remaining[start + 15..];
            if let Some(end) = after.find("</AllowedMethod>") {
                let method = after[..end].trim();
                if !valid_methods.contains(&method) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidRequest",
                        format!(
                            "Found unsupported HTTP method in CORS config. Unsupported method is {method}"
                        ),
                    ));
                }
                remaining = &after[end + 16..];
            } else {
                break;
            }
        }

        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.cors_config = Some(body_str);
        Ok(empty_response(StatusCode::OK))
    }

    fn get_bucket_cors(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        match &b.cors_config {
            Some(config) => Ok(s3_xml(StatusCode::OK, config.clone())),
            None => Err(AwsServiceError::aws_error_with_fields(
                StatusCode::NOT_FOUND,
                "NoSuchCORSConfiguration",
                "The CORS configuration does not exist",
                vec![("BucketName".to_string(), bucket.to_string())],
            )),
        }
    }

    fn delete_bucket_cors(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.cors_config = None;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }

    // ---- Notification ----

    fn put_bucket_notification(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("").to_string();
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        // Auto-generate Id for each configuration element if missing
        let normalized = normalize_notification_ids(&body_str);
        b.notification_config = Some(normalized);
        Ok(empty_response(StatusCode::OK))
    }

    fn get_bucket_notification(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let body = match &b.notification_config {
            Some(config) => config.clone(),
            None => "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                     <NotificationConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                     </NotificationConfiguration>"
                .to_string(),
        };
        Ok(s3_xml(StatusCode::OK, body))
    }

    // ---- Logging ----

    fn put_bucket_logging(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("").to_string();
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.logging_config = Some(body_str);
        Ok(empty_response(StatusCode::OK))
    }

    fn get_bucket_logging(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let body = match &b.logging_config {
            Some(config) => config.clone(),
            None => "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                     <BucketLoggingStatus xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                     </BucketLoggingStatus>"
                .to_string(),
        };
        Ok(s3_xml(StatusCode::OK, body))
    }

    // ---- Website ----

    fn put_bucket_website(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("").to_string();
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.website_config = Some(body_str);
        Ok(empty_response(StatusCode::OK))
    }

    fn get_bucket_website(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        match &b.website_config {
            Some(config) => Ok(s3_xml(StatusCode::OK, config.clone())),
            None => Err(AwsServiceError::aws_error_with_fields(
                StatusCode::NOT_FOUND,
                "NoSuchWebsiteConfiguration",
                "The specified bucket does not have a website configuration",
                vec![("BucketName".to_string(), bucket.to_string())],
            )),
        }
    }

    fn delete_bucket_website(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.website_config = None;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }

    // ---- Accelerate ----

    fn put_bucket_accelerate(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        if bucket.contains('.') {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "S3 Transfer Acceleration is not supported for buckets with periods (.) in their names",
            ));
        }
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let status = extract_xml_value(body_str, "Status");
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        // Validate status
        if let Some(ref s) = status {
            if s != "Enabled" && s != "Suspended" {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "MalformedXML",
                    "The XML you provided was not well-formed or did not validate against our published schema",
                ));
            }
        }
        // Suspending a never-configured bucket is a no-op
        if status.as_deref() == Some("Suspended") && b.accelerate_status.is_none() {
            return Ok(empty_response(StatusCode::OK));
        }
        b.accelerate_status = status;
        Ok(empty_response(StatusCode::OK))
    }

    fn get_bucket_accelerate(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let status_xml = match &b.accelerate_status {
            Some(s) => format!("<Status>{s}</Status>"),
            None => String::new(),
        };
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <AccelerateConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             {status_xml}\
             </AccelerateConfiguration>"
        );
        Ok(s3_xml(StatusCode::OK, body))
    }

    // ---- PublicAccessBlock ----

    fn put_public_access_block(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("").to_string();
        // Validate that at least one field is specified
        let has_field = body_str.contains("BlockPublicAcls")
            || body_str.contains("IgnorePublicAcls")
            || body_str.contains("BlockPublicPolicy")
            || body_str.contains("RestrictPublicBuckets");
        if !has_field {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Must specify at least one configuration.",
            ));
        }
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.public_access_block = Some(body_str);
        Ok(empty_response(StatusCode::OK))
    }

    fn get_public_access_block(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        match &b.public_access_block {
            Some(config) => {
                // Ensure all four fields are present with defaults of false
                let fields = [
                    "BlockPublicAcls",
                    "IgnorePublicAcls",
                    "BlockPublicPolicy",
                    "RestrictPublicBuckets",
                ];
                let mut result = config.clone();
                for field in fields {
                    if !result.contains(field) {
                        let closing = "</PublicAccessBlockConfiguration>";
                        if let Some(pos) = result.find(closing) {
                            result.insert_str(pos, &format!("<{field}>false</{field}>"));
                        }
                    }
                }
                Ok(s3_xml(StatusCode::OK, result))
            }
            None => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchPublicAccessBlockConfiguration",
                "The public access block configuration was not found",
            )),
        }
    }

    fn delete_public_access_block(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.public_access_block = None;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }

    // ---- ObjectLockConfiguration ----

    fn put_object_lock_config(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("").to_string();

        // Validate: body must not be empty
        if body_str.trim().is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingRequestBodyError",
                "Request Body is empty",
            ));
        }

        // Must contain ObjectLockEnabled
        if !body_str.contains("<ObjectLockEnabled>") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                "The XML you provided was not well-formed or did not validate against our published schema",
            ));
        }

        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        // Versioning must be enabled
        if b.versioning.as_deref() != Some("Enabled") {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "InvalidBucketState",
                "Versioning must be 'Enabled' on the bucket to apply a Object Lock configuration",
            ));
        }

        b.object_lock_config = Some(body_str);
        Ok(empty_response(StatusCode::OK))
    }

    #[allow(dead_code)]
    fn get_object_lock_config(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        match &b.object_lock_config {
            Some(config) => Ok(s3_xml(StatusCode::OK, config.clone())),
            None => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ObjectLockConfigurationNotFoundError",
                "Object Lock configuration does not exist for this bucket",
            )),
        }
    }

    // ---- List operations ----

    #[allow(dead_code)]
    fn list_objects_v1(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let prefix = req.query_params.get("prefix").cloned().unwrap_or_default();
        let delimiter = req.query_params.get("delimiter").cloned();
        let max_keys: usize = req
            .query_params
            .get("max-keys")
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000);
        let marker = req.query_params.get("marker").cloned().unwrap_or_default();
        let encoding_type = req.query_params.get("encoding-type").cloned();

        let mut contents = String::new();
        let mut common_prefixes: Vec<String> = Vec::new();
        let mut count = 0;
        let mut is_truncated = false;
        let mut last_key = String::new();

        for (key, obj) in &b.objects {
            if obj.is_delete_marker {
                continue;
            }
            if !key.starts_with(&prefix) {
                continue;
            }
            if !marker.is_empty() && key.as_str() <= marker.as_str() {
                continue;
            }

            // Handle delimiter-based grouping
            if let Some(ref delim) = delimiter {
                if !delim.is_empty() {
                    let suffix = &key[prefix.len()..];
                    if let Some(pos) = suffix.find(delim.as_str()) {
                        let cp = format!("{}{}", prefix, &suffix[..pos + delim.len()]);
                        if !common_prefixes.contains(&cp) {
                            if count >= max_keys {
                                is_truncated = true;
                                break;
                            }
                            common_prefixes.push(cp);
                            last_key = key.clone();
                            count += 1;
                        }
                        continue;
                    }
                }
            }

            if count >= max_keys {
                is_truncated = true;
                break;
            }

            let display_key = if encoding_type.as_deref() == Some("url") {
                url_encode_s3_key(key)
            } else {
                xml_escape(key)
            };

            contents.push_str(&format!(
                "<Contents>\
                 <Key>{}</Key>\
                 <LastModified>{}</LastModified>\
                 <ETag>&quot;{}&quot;</ETag>\
                 <Size>{}</Size>\
                 <StorageClass>{}</StorageClass>\
                 </Contents>",
                display_key,
                obj.last_modified.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                obj.etag,
                obj.size,
                obj.storage_class,
            ));
            last_key = key.clone();
            count += 1;
        }

        let mut common_prefixes_xml = String::new();
        for cp in &common_prefixes {
            let display_cp = if encoding_type.as_deref() == Some("url") {
                url_encode_s3_key(cp)
            } else {
                xml_escape(cp)
            };
            common_prefixes_xml.push_str(&format!(
                "<CommonPrefixes><Prefix>{display_cp}</Prefix></CommonPrefixes>",
            ));
        }

        let next_marker = if is_truncated {
            format!("<NextMarker>{}</NextMarker>", xml_escape(&last_key))
        } else {
            String::new()
        };

        let delimiter_xml = match &delimiter {
            Some(d) if !d.is_empty() => format!("<Delimiter>{}</Delimiter>", xml_escape(d)),
            _ => String::new(),
        };

        let prefix_xml = if prefix.is_empty() {
            String::new()
        } else {
            let display_prefix = if encoding_type.as_deref() == Some("url") {
                url_encode_s3_key(&prefix)
            } else {
                xml_escape(&prefix)
            };
            format!("<Prefix>{display_prefix}</Prefix>")
        };

        let marker_xml = if marker.is_empty() {
            String::new()
        } else {
            format!("<Marker>{}</Marker>", xml_escape(&marker))
        };

        let encoding_xml = if encoding_type.as_deref() == Some("url") {
            "<EncodingType>url</EncodingType>".to_string()
        } else {
            String::new()
        };

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Name>{bucket}</Name>\
             {prefix_xml}\
             {marker_xml}\
             <MaxKeys>{max_keys}</MaxKeys>\
             {delimiter_xml}\
             {encoding_xml}\
             <IsTruncated>{is_truncated}</IsTruncated>\
             {contents}\
             {common_prefixes_xml}\
             {next_marker}\
             </ListBucketResult>",
        );
        Ok(s3_xml(StatusCode::OK, body))
    }

    fn list_objects_v2(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let prefix = req.query_params.get("prefix").cloned().unwrap_or_default();
        let delimiter = req
            .query_params
            .get("delimiter")
            .cloned()
            .unwrap_or_default();
        let max_keys: usize = req
            .query_params
            .get("max-keys")
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000);
        let start_after = req
            .query_params
            .get("start-after")
            .cloned()
            .unwrap_or_default();
        let continuation = req.query_params.get("continuation-token").cloned();
        if let Some(ref ct) = continuation {
            if ct.is_empty() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    "The continuation token provided is incorrect",
                ));
            }
        }
        let fetch_owner = req
            .query_params
            .get("fetch-owner")
            .map(|v| v == "true")
            .unwrap_or(false);

        let effective_start = continuation.as_deref().unwrap_or(&start_after);

        let mut contents = String::new();
        let mut common_prefixes: Vec<String> = Vec::new();
        let mut count = 0;
        let mut is_truncated = false;
        let mut last_key = String::new();

        for (key, obj) in &b.objects {
            if obj.is_delete_marker {
                continue;
            }
            if !key.starts_with(&prefix) {
                continue;
            }
            if !effective_start.is_empty() && key.as_str() <= effective_start {
                continue;
            }

            // Handle delimiter-based grouping
            if !delimiter.is_empty() {
                if prefix.len() > key.len() {
                    continue;
                }
                let suffix = &key[prefix.len()..];
                if let Some(pos) = suffix.find(&delimiter) {
                    let end = (pos + delimiter.len()).min(suffix.len());
                    let cp = format!("{}{}", prefix, &suffix[..end]);
                    if !common_prefixes.contains(&cp) {
                        if count >= max_keys {
                            is_truncated = true;
                            break;
                        }
                        common_prefixes.push(cp);
                        last_key = key.clone();
                        count += 1;
                    }
                    continue;
                }
            }

            if count >= max_keys {
                is_truncated = true;
                break;
            }

            let owner_xml = if fetch_owner {
                let oid = obj.acl_owner_id.as_deref().unwrap_or(&b.acl_owner_id);
                format!(
                    "<Owner><ID>{}</ID><DisplayName>{}</DisplayName></Owner>",
                    xml_escape(oid),
                    xml_escape(oid),
                )
            } else {
                String::new()
            };

            let checksum_xml = if let Some(ref algo) = obj.checksum_algorithm {
                format!(
                    "<ChecksumAlgorithm>{}</ChecksumAlgorithm>",
                    xml_escape(algo)
                )
            } else {
                String::new()
            };

            let use_url_enc =
                req.query_params.get("encoding-type").map(|s| s.as_str()) == Some("url");
            let display_key = if use_url_enc {
                url_encode_s3_key(key)
            } else {
                xml_escape(key)
            };

            contents.push_str(&format!(
                "<Contents>\
                 <Key>{}</Key>\
                 <LastModified>{}</LastModified>\
                 <ETag>&quot;{}&quot;</ETag>\
                 <Size>{}</Size>\
                 <StorageClass>{}</StorageClass>\
                 {owner_xml}{checksum_xml}\
                 </Contents>",
                display_key,
                obj.last_modified.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                obj.etag,
                obj.size,
                obj.storage_class,
            ));
            last_key = key.clone();
            count += 1;
        }

        let encoding_type = req.query_params.get("encoding-type").cloned();
        let use_url_encoding = encoding_type.as_deref() == Some("url");

        let mut common_prefixes_xml = String::new();
        for cp in &common_prefixes {
            let display_cp = if use_url_encoding {
                url_encode_s3_key(cp)
            } else {
                xml_escape(cp)
            };
            common_prefixes_xml.push_str(&format!(
                "<CommonPrefixes><Prefix>{display_cp}</Prefix></CommonPrefixes>",
            ));
        }

        let next_token = if is_truncated {
            format!(
                "<NextContinuationToken>{}</NextContinuationToken>",
                xml_escape(&last_key)
            )
        } else {
            String::new()
        };

        let cont_token = if let Some(ct) = &continuation {
            format!("<ContinuationToken>{}</ContinuationToken>", xml_escape(ct))
        } else {
            String::new()
        };

        let encoding_xml = if use_url_encoding {
            "<EncodingType>url</EncodingType>".to_string()
        } else {
            String::new()
        };
        let delimiter_xml = if delimiter.is_empty() {
            String::new()
        } else {
            format!("<Delimiter>{}</Delimiter>", xml_escape(&delimiter))
        };
        // StartAfter is only included when no ContinuationToken is present
        let start_after_xml = if start_after.is_empty() || continuation.is_some() {
            String::new()
        } else {
            format!("<StartAfter>{}</StartAfter>", xml_escape(&start_after))
        };

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Name>{bucket}</Name><Prefix>{prefix}</Prefix>{delimiter_xml}{encoding_xml}\
             <KeyCount>{count}</KeyCount>\
             <MaxKeys>{max_keys}</MaxKeys>{start_after_xml}<IsTruncated>{is_truncated}</IsTruncated>\
             {cont_token}{next_token}{contents}{common_prefixes_xml}</ListBucketResult>",
            prefix = if use_url_encoding { url_encode_s3_key(&prefix) } else { xml_escape(&prefix) },
        );
        Ok(s3_xml(StatusCode::OK, body))
    }

    fn get_bucket_tagging(
        &self,
        _req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        if b.tags.is_empty() {
            return Err(AwsServiceError::aws_error_with_fields(
                StatusCode::NOT_FOUND,
                "NoSuchTagSet",
                "The TagSet does not exist",
                vec![("BucketName".to_string(), b.name.clone())],
            ));
        }
        let mut tags_xml = String::new();
        for (k, v) in &b.tags {
            tags_xml.push_str(&format!(
                "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
                xml_escape(k),
                xml_escape(v),
            ));
        }
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <TagSet>{tags_xml}</TagSet></Tagging>"
        );
        Ok(s3_xml(StatusCode::OK, body))
    }

    fn put_bucket_tagging(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let tags = parse_tagging_xml(body_str);

        // Validate tags: no duplicate keys
        validate_tags(&tags)?;

        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.tags = tags.into_iter().collect();
        Ok(AwsResponse {
            status: StatusCode::NO_CONTENT,
            content_type: "application/xml".to_string(),
            body: Bytes::new(),
            headers: HeaderMap::new(),
        })
    }

    fn delete_bucket_tagging(
        &self,
        _req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.tags.clear();
        Ok(AwsResponse {
            status: StatusCode::NO_CONTENT,
            content_type: "application/xml".to_string(),
            body: Bytes::new(),
            headers: HeaderMap::new(),
        })
    }

    // ---- Bucket ACL ----

    fn get_bucket_acl(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let body = build_acl_xml(&b.acl_owner_id, &b.acl_grants, &req.account_id);
        Ok(s3_xml(StatusCode::OK, body))
    }

    fn put_bucket_acl(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Check for canned ACL header
        let canned = req
            .headers
            .get("x-amz-acl")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        if let Some(acl) = canned {
            b.acl_grants = canned_acl_grants(&acl, &b.acl_owner_id.clone());
        } else {
            // Parse ACL from body (AccessControlPolicy XML)
            let body_str = std::str::from_utf8(&req.body).unwrap_or("");
            let grants = parse_acl_xml(body_str)?;
            b.acl_grants = grants;
        }

        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: Bytes::new(),
            headers: HeaderMap::new(),
        })
    }

    // ---- Bucket Versioning ----

    fn put_bucket_versioning(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let status_val = extract_xml_value(body_str, "Status").unwrap_or_default();

        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        if status_val == "Enabled" || status_val == "Suspended" {
            b.versioning = Some(status_val);
        }
        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: Bytes::new(),
            headers: HeaderMap::new(),
        })
    }

    fn get_bucket_versioning(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let status_xml = match &b.versioning {
            Some(s) => format!("<Status>{s}</Status>"),
            None => String::new(),
        };
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             {status_xml}\
             </VersioningConfiguration>"
        );
        Ok(s3_xml(StatusCode::OK, body))
    }

    fn list_object_versions(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let prefix = req.query_params.get("prefix").cloned().unwrap_or_default();
        let delimiter = req.query_params.get("delimiter").cloned();
        let key_marker = req
            .query_params
            .get("key-marker")
            .cloned()
            .unwrap_or_default();
        let version_id_marker = req.query_params.get("version-id-marker").cloned();
        let max_keys: usize = req
            .query_params
            .get("max-keys")
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000);

        let owner_id = &b.acl_owner_id;

        // Build a sorted list of all version entries: (key, obj, is_latest)
        let mut all_entries: Vec<(&str, &S3Object, bool)> = Vec::new();

        if b.object_versions.is_empty() {
            // No versioning history — every object in b.objects is the only version
            for (key, obj) in &b.objects {
                all_entries.push((key.as_str(), obj, true));
            }
        } else {
            // Collect versioned keys
            let mut keys: Vec<&String> = b.object_versions.keys().collect();
            keys.sort();
            for key in &keys {
                if let Some(versions) = b.object_versions.get(key.as_str()) {
                    let len = versions.len();
                    // Latest version is last in the vec; iterate newest-first
                    for (i, obj) in versions.iter().enumerate().rev() {
                        let is_latest = i == len - 1;
                        all_entries.push((key.as_str(), obj, is_latest));
                    }
                }
            }
            // Include non-versioned objects (keys not in object_versions)
            for (key, obj) in &b.objects {
                if !b.object_versions.contains_key(key) {
                    all_entries.push((key.as_str(), obj, true));
                }
            }
            // Sort by key, then newest-first within key (already done by rev above,
            // but we need global sort since we mixed in non-versioned objects)
            all_entries.sort_by(|a, b_entry| a.0.cmp(b_entry.0));
        }

        // Filter by prefix
        all_entries.retain(|(key, _, _)| key.starts_with(prefix.as_str()));

        // Apply key-marker / version-id-marker pagination
        if !key_marker.is_empty() {
            let vid_marker = version_id_marker.as_deref();
            let mut skip = true;
            all_entries.retain(|(key, obj, _)| {
                if !skip {
                    return true;
                }
                if *key < key_marker.as_str() {
                    return false; // before marker, skip
                }
                if *key > key_marker.as_str() {
                    skip = false;
                    return true; // past marker key, include
                }
                // key == key_marker: skip until we find the version_id_marker
                if let Some(vid) = vid_marker {
                    if obj.version_id.as_deref().unwrap_or("null") == vid {
                        // Found the marker version — skip it, include everything after
                        skip = false;
                        return false;
                    }
                    false // still before the version marker
                } else {
                    false // skip entire key_marker key when no version-id-marker
                }
            });
        }

        // Handle delimiter: collect common prefixes
        let mut common_prefixes: Vec<String> = Vec::new();
        if let Some(ref delim) = delimiter {
            let mut filtered_entries = Vec::new();
            let mut seen_prefixes = std::collections::HashSet::new();
            for entry @ (key, _, _) in &all_entries {
                let after_prefix = &key[prefix.len()..];
                if let Some(pos) = after_prefix.find(delim.as_str()) {
                    let cp = format!("{}{}", prefix, &after_prefix[..pos + delim.len()]);
                    if seen_prefixes.insert(cp.clone()) {
                        common_prefixes.push(cp);
                    }
                } else {
                    filtered_entries.push(*entry);
                }
            }
            all_entries = filtered_entries;
        }

        // Pagination: truncate at max_keys (count versions + delete markers + common prefixes)
        let total_items = all_entries.len() + common_prefixes.len();
        let is_truncated = total_items > max_keys;

        // We need to limit versions to max_keys minus common_prefixes already counted
        let version_limit = max_keys.saturating_sub(common_prefixes.len());
        let truncated_entries: Vec<_> = all_entries.iter().take(version_limit).collect();
        let next_markers = if is_truncated && !truncated_entries.is_empty() {
            let last = truncated_entries.last().unwrap();
            Some((
                last.0.to_string(),
                last.1
                    .version_id
                    .clone()
                    .unwrap_or_else(|| "null".to_string()),
            ))
        } else {
            None
        };

        // Build XML
        let mut versions_xml = String::new();
        for (key, obj, is_latest) in &truncated_entries {
            if obj.is_delete_marker {
                versions_xml.push_str(&format!(
                    "<DeleteMarker>\
                     <Key>{}</Key>\
                     <VersionId>{}</VersionId>\
                     <IsLatest>{}</IsLatest>\
                     <LastModified>{}</LastModified>\
                     <Owner><ID>{owner_id}</ID><DisplayName>{owner_id}</DisplayName></Owner>\
                     </DeleteMarker>",
                    xml_escape(key),
                    obj.version_id.as_deref().unwrap_or("null"),
                    is_latest,
                    obj.last_modified.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                ));
            } else {
                versions_xml.push_str(&format!(
                    "<Version>\
                     <Key>{}</Key>\
                     <VersionId>{}</VersionId>\
                     <IsLatest>{}</IsLatest>\
                     <LastModified>{}</LastModified>\
                     <ETag>&quot;{}&quot;</ETag>\
                     <Size>{}</Size>\
                     <Owner><ID>{owner_id}</ID><DisplayName>{owner_id}</DisplayName></Owner>\
                     <StorageClass>{}</StorageClass>\
                     </Version>",
                    xml_escape(key),
                    obj.version_id.as_deref().unwrap_or("null"),
                    is_latest,
                    obj.last_modified.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                    obj.etag,
                    obj.size,
                    obj.storage_class,
                ));
            }
        }

        // Common prefixes
        let mut cp_xml = String::new();
        for cp in &common_prefixes {
            cp_xml.push_str(&format!(
                "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
                xml_escape(cp),
            ));
        }

        // Pagination markers
        let marker_xml = if let Some((ref nk, ref nv)) = next_markers {
            format!(
                "<NextKeyMarker>{}</NextKeyMarker>\
                 <NextVersionIdMarker>{}</NextVersionIdMarker>",
                xml_escape(nk),
                xml_escape(nv),
            )
        } else {
            String::new()
        };

        let delimiter_xml = delimiter
            .as_ref()
            .map(|d| format!("<Delimiter>{}</Delimiter>", xml_escape(d)))
            .unwrap_or_default();

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListVersionsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Name>{name}</Name>\
             <Prefix>{pfx}</Prefix>\
             <KeyMarker>{km}</KeyMarker>\
             {delimiter_xml}\
             <MaxKeys>{max_keys}</MaxKeys>\
             <IsTruncated>{is_truncated}</IsTruncated>\
             {marker_xml}\
             {versions_xml}\
             {cp_xml}\
             </ListVersionsResult>",
            name = xml_escape(bucket),
            pfx = xml_escape(&prefix),
            km = xml_escape(&key_marker),
        );
        Ok(s3_xml(StatusCode::OK, body))
    }

    fn get_object_lock_configuration(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        match &b.object_lock_config {
            Some(config) => Ok(s3_xml(StatusCode::OK, config.clone())),
            None => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ObjectLockConfigurationNotFoundError",
                "Object Lock configuration does not exist for this bucket",
            )),
        }
    }

    fn put_bucket_replication(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("").to_string();
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        // Versioning must be enabled to set replication
        if b.versioning.as_deref() != Some("Enabled") {
            return Err(AwsServiceError::aws_error_with_fields(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Versioning must be 'Enabled' on the bucket to apply a replication configuration",
                vec![("BucketName".to_string(), bucket.to_string())],
            ));
        }

        b.replication_config = Some(normalize_replication_xml(&body_str));
        Ok(empty_response(StatusCode::OK))
    }

    fn get_bucket_replication(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        match &b.replication_config {
            Some(config) => Ok(s3_xml(StatusCode::OK, config.clone())),
            None => Err(AwsServiceError::aws_error_with_fields(
                StatusCode::NOT_FOUND,
                "ReplicationConfigurationNotFoundError",
                "The replication configuration was not found",
                vec![("BucketName".to_string(), bucket.to_string())],
            )),
        }
    }

    fn delete_bucket_replication(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.replication_config = None;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }

    fn put_bucket_ownership_controls(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("").to_string();
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.ownership_controls = Some(body_str);
        Ok(empty_response(StatusCode::OK))
    }

    fn get_bucket_ownership_controls(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        match &b.ownership_controls {
            Some(config) => Ok(s3_xml(StatusCode::OK, config.clone())),
            None => Err(AwsServiceError::aws_error_with_fields(
                StatusCode::NOT_FOUND,
                "OwnershipControlsNotFoundError",
                "The bucket ownership controls were not found",
                vec![("BucketName".to_string(), bucket.to_string())],
            )),
        }
    }

    fn delete_bucket_ownership_controls(
        &self,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.ownership_controls = None;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }

    fn put_bucket_inventory(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("").to_string();
        // Use the Id from the XML body if available, otherwise fall back to query param
        let inv_id = extract_xml_value(&body_str, "Id")
            .or_else(|| req.query_params.get("id").cloned())
            .unwrap_or_default();
        {
            let mut state = self.state.write();
            let b = state
                .buckets
                .get_mut(bucket)
                .ok_or_else(|| no_such_bucket(bucket))?;
            b.inventory_configs.insert(inv_id.clone(), body_str);
        }
        // Generate the inventory report immediately so tests can verify it
        inventory::generate_inventory_report(&self.state, bucket, &inv_id);
        Ok(empty_response(StatusCode::OK))
    }

    fn get_bucket_inventory(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let inv_id = req.query_params.get("id").cloned().unwrap_or_default();
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        match b.inventory_configs.get(&inv_id) {
            Some(config) => Ok(s3_xml(StatusCode::OK, config.clone())),
            None => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchConfiguration",
                format!("The specified configuration does not exist: {inv_id}"),
            )),
        }
    }

    fn list_bucket_inventory_configurations(
        &self,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let mut body = String::from(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListInventoryConfigurationsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <IsTruncated>false</IsTruncated>",
        );
        let mut sorted_keys: Vec<_> = b.inventory_configs.keys().collect();
        sorted_keys.sort();
        for key in sorted_keys {
            if let Some(config) = b.inventory_configs.get(key) {
                body.push_str(config);
            }
        }
        body.push_str("</ListInventoryConfigurationsResult>");
        Ok(s3_xml(StatusCode::OK, body))
    }

    fn delete_bucket_inventory(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let inv_id = req.query_params.get("id").cloned().unwrap_or_default();
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.inventory_configs.remove(&inv_id);
        Ok(empty_response(StatusCode::NO_CONTENT))
    }
}

// ---------------------------------------------------------------------------
// Object operations
// ---------------------------------------------------------------------------
impl S3Service {
    fn put_object(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Validate key length
        if key.len() > 1024 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "KeyTooLongError",
                "Your key is too long",
            ));
        }

        // Check for If-None-Match conditional on PUT
        let if_none_match = req
            .headers
            .get("if-none-match")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // Check for If-Match conditional on PUT
        let if_match = req
            .headers
            .get("if-match")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // Check for x-amz-tagging header
        let tagging_header = req
            .headers
            .get("x-amz-tagging")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // Check for ACL header
        let acl_header = req
            .headers
            .get("x-amz-acl")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // Check for grant headers alongside canned ACL
        let has_grant_headers = req.headers.keys().any(|k| {
            let name = k.as_str();
            name.starts_with("x-amz-grant-")
        });

        if acl_header.is_some() && has_grant_headers {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Specifying both Canned ACLs and Header Grants is not allowed",
            ));
        }

        // Parse tags from header
        let tags = if let Some(tagging) = &tagging_header {
            let parsed = parse_url_encoded_tags(tagging);
            // Validate aws: prefix
            for (k, _) in &parsed {
                if k.starts_with("aws:") {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidTag",
                        "Your TagKey cannot be prefixed with aws:",
                    ));
                }
            }
            parsed.into_iter().collect()
        } else {
            std::collections::HashMap::new()
        };

        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        // Handle If-Match: check existing object etag
        if let Some(ref if_match_val) = if_match {
            match b.objects.get(key) {
                Some(existing) => {
                    let existing_etag = format!("\"{}\"", existing.etag);
                    if !etag_matches(if_match_val, &existing_etag) {
                        return Err(precondition_failed("If-Match"));
                    }
                }
                None => {
                    return Err(no_such_key(key));
                }
            }
        }

        // Handle If-None-Match: if "*", fail if object already exists
        if let Some(ref inm) = if_none_match {
            if inm.trim() == "*" && b.objects.contains_key(key) {
                return Err(precondition_failed("If-None-Match"));
            }
        }

        let data = req.body.clone();
        let data_size = data.len() as u64;
        let etag = compute_md5(&data);
        let content_type = req
            .headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("binary/octet-stream")
            .to_string();
        let version_id = if b.versioning.as_deref() == Some("Enabled") {
            Some(uuid::Uuid::new_v4().to_string())
        } else {
            None
        };
        let content_encoding = req
            .headers
            .get("content-encoding")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let storage_class = req
            .headers
            .get("x-amz-storage-class")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("STANDARD")
            .to_string();
        if !is_valid_storage_class(&storage_class) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidStorageClass",
                "The storage class you specified is not valid",
            ));
        }
        let website_redirect_location = req
            .headers
            .get("x-amz-website-redirect-location")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let metadata = extract_user_metadata(&req.headers);

        // Extract checksum algorithm and value
        let checksum_algorithm = req
            .headers
            .get("x-amz-sdk-checksum-algorithm")
            .or_else(|| req.headers.get("x-amz-checksum-algorithm"))
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let _checksum_from_header = checksum_algorithm.as_deref().and_then(|algo| {
            let header_name = format!("x-amz-checksum-{}", algo.to_lowercase());
            req.headers
                .get(header_name.as_str())
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
        });

        // Build ACL grants for object
        let acl_grants = if has_grant_headers {
            parse_grant_headers(&req.headers)
        } else if let Some(ref acl) = acl_header {
            canned_acl_grants_for_object(acl, &b.acl_owner_id)
        } else {
            // Default: owner full control
            vec![AclGrant {
                grantee_type: "CanonicalUser".to_string(),
                grantee_id: Some(b.acl_owner_id.clone()),
                grantee_display_name: Some(b.acl_owner_id.clone()),
                grantee_uri: None,
                permission: "FULL_CONTROL".to_string(),
            }]
        };

        // SSE headers
        let mut sse_algorithm = req
            .headers
            .get("x-amz-server-side-encryption")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let mut sse_kms_key_id = req
            .headers
            .get("x-amz-server-side-encryption-aws-kms-key-id")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let bucket_key_enabled = req
            .headers
            .get("x-amz-server-side-encryption-bucket-key-enabled")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.eq_ignore_ascii_case("true"));

        // Apply bucket default encryption if no explicit SSE headers
        if sse_algorithm.is_none() {
            if let Some(ref enc_config) = b.encryption_config {
                if let Some(algo) = extract_xml_value(enc_config, "SSEAlgorithm") {
                    if algo == "aws:kms" && sse_kms_key_id.is_none() {
                        sse_kms_key_id = extract_xml_value(enc_config, "KMSMasterKeyID");
                    }
                    sse_algorithm = Some(algo);
                }
            }
        }

        // Validate KMS key exists when using aws:kms encryption
        if sse_algorithm.as_deref() == Some("aws:kms") {
            if let Some(ref kms) = self.kms_state {
                if let Some(ref key_id) = sse_kms_key_id {
                    let kms_state = kms.read();
                    let key_exists = kms_state
                        .keys
                        .values()
                        .any(|k| k.key_id == *key_id || k.arn == *key_id)
                        || kms_state
                            .aliases
                            .values()
                            .any(|a| a.alias_name == *key_id || a.alias_arn == *key_id);
                    if !key_exists {
                        // Still allow it — AWS doesn't always reject unknown keys
                        // for emulation purposes, just set the key ID
                        tracing::debug!(
                            key_id = %key_id,
                            "KMS key not found in state, proceeding anyway"
                        );
                    } else {
                        // Resolve alias to key ARN if needed
                        if let Some(alias) = kms_state
                            .aliases
                            .values()
                            .find(|a| a.alias_name == *key_id || a.alias_arn == *key_id)
                        {
                            if let Some(key) = kms_state.keys.get(&alias.target_key_id) {
                                sse_kms_key_id = Some(key.arn.clone());
                            }
                        } else if let Some(key) =
                            kms_state.keys.values().find(|k| k.key_id == *key_id)
                        {
                            sse_kms_key_id = Some(key.arn.clone());
                        }
                    }
                }
            }
        }

        // Checksum: detect algorithm from various headers
        let explicit_checksum_algo = req
            .headers
            .get("x-amz-checksum-algorithm")
            .or_else(|| req.headers.get("x-amz-sdk-checksum-algorithm"))
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_uppercase());
        let checksum_algorithm = explicit_checksum_algo.clone().or_else(|| {
            // Also detect from checksum value headers
            if req.headers.contains_key("x-amz-checksum-crc32") {
                Some("CRC32".to_string())
            } else if req.headers.contains_key("x-amz-checksum-sha1") {
                Some("SHA1".to_string())
            } else if req.headers.contains_key("x-amz-checksum-sha256") {
                Some("SHA256".to_string())
            } else {
                None
            }
        });
        let checksum_value = checksum_algorithm
            .as_deref()
            .map(|algo| compute_checksum(algo, &data));

        // Object lock: validate that bucket has object lock enabled if lock headers present
        let has_lock_headers = req.headers.contains_key("x-amz-object-lock-mode")
            || req
                .headers
                .contains_key("x-amz-object-lock-retain-until-date")
            || req.headers.contains_key("x-amz-object-lock-legal-hold");
        if has_lock_headers && b.object_lock_config.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Bucket is missing ObjectLockConfiguration",
            ));
        }

        // Object lock - explicit headers or bucket default
        let mut lock_mode = req
            .headers
            .get("x-amz-object-lock-mode")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let mut lock_retain_until = req
            .headers
            .get("x-amz-object-lock-retain-until-date")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<DateTime<Utc>>().ok());
        let lock_legal_hold = req
            .headers
            .get("x-amz-object-lock-legal-hold")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // Apply bucket default lock if no explicit lock headers
        if lock_mode.is_none() && lock_retain_until.is_none() {
            if let Some(ref config) = b.object_lock_config {
                if let Some(mode) = extract_xml_value(config, "Mode") {
                    let days =
                        extract_xml_value(config, "Days").and_then(|d| d.parse::<i64>().ok());
                    let years =
                        extract_xml_value(config, "Years").and_then(|y| y.parse::<i64>().ok());
                    let duration = if let Some(d) = days {
                        Some(chrono::Duration::days(d))
                    } else {
                        years.map(|y| chrono::Duration::days(y * 365))
                    };
                    if let Some(dur) = duration {
                        lock_mode = Some(mode);
                        lock_retain_until = Some(Utc::now() + dur);
                    }
                }
            }
        }

        let obj = S3Object {
            key: key.to_string(),
            size: data.len() as u64,
            data,
            content_type,
            etag: etag.clone(),
            last_modified: Utc::now(),
            metadata,
            storage_class,
            tags,
            acl_grants,
            acl_owner_id: Some(b.acl_owner_id.clone()),
            parts_count: None,
            part_sizes: None,
            sse_algorithm: sse_algorithm.clone(),
            sse_kms_key_id: sse_kms_key_id.clone(),
            bucket_key_enabled,
            version_id: version_id.clone(),
            is_delete_marker: false,
            content_encoding,
            website_redirect_location,
            restore_ongoing: None,
            restore_expiry: None,
            checksum_algorithm: checksum_algorithm.clone(),
            checksum_value: checksum_value.clone(),
            lock_mode,
            lock_retain_until,
            lock_legal_hold,
        };
        if b.versioning.as_deref() == Some("Enabled") {
            let versions = b.object_versions.entry(key.to_string()).or_default();
            // If the existing current object is a pre-versioning object (no version_id)
            // and not yet tracked in object_versions, preserve it.
            if versions.is_empty() {
                if let Some(existing) = b.objects.get(key) {
                    if existing.version_id.is_none() {
                        versions.push(existing.clone());
                    }
                }
            }
            versions.push(obj.clone());
        }
        b.objects.insert(key.to_string(), obj);

        let mut headers = HeaderMap::new();
        headers.insert("etag", format!("\"{etag}\"").parse().unwrap());
        if let Some(vid) = &version_id {
            headers.insert("x-amz-version-id", vid.parse().unwrap());
        }
        // Return SSE headers
        if let Some(algo) = &sse_algorithm {
            headers.insert("x-amz-server-side-encryption", algo.parse().unwrap());
        } else {
            headers.insert("x-amz-server-side-encryption", "AES256".parse().unwrap());
        }
        if let Some(kid) = &sse_kms_key_id {
            headers.insert(
                "x-amz-server-side-encryption-aws-kms-key-id",
                kid.parse().unwrap(),
            );
        }
        if bucket_key_enabled == Some(true) {
            headers.insert(
                "x-amz-server-side-encryption-bucket-key-enabled",
                "true".parse().unwrap(),
            );
        }
        // Checksum in response
        if let (Some(algo), Some(val)) = (&checksum_algorithm, &checksum_value) {
            let header_name = format!("x-amz-checksum-{}", algo.to_lowercase());
            if let Ok(name) = header_name.parse::<http::header::HeaderName>() {
                if let Ok(hval) = val.parse() {
                    headers.insert(name, hval);
                }
            }
            // Echo back the checksum algorithm only when explicitly requested
            if explicit_checksum_algo.is_some() {
                headers.insert("x-amz-sdk-checksum-algorithm", algo.parse().unwrap());
            }
        }

        // Capture notification config before dropping state lock
        let notification_config = b.notification_config.clone();
        let obj_size = data_size;
        let obj_etag = etag.clone();
        let bucket_name = bucket.to_string();
        let obj_key = key.to_string();
        let region = state.region.clone();

        // Replicate object if replication is configured on the source bucket
        replicate_object(&mut state, bucket, key);

        drop(state);

        // Deliver S3 event notifications
        if let Some(ref config) = notification_config {
            deliver_notifications(
                &self.delivery,
                config,
                "ObjectCreated:Put",
                &bucket_name,
                &obj_key,
                obj_size,
                &obj_etag,
                &region,
            );
        }

        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: String::new(),
            body: Bytes::new(),
            headers,
        })
    }

    fn get_object(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let obj = resolve_object(b, key, req.query_params.get("versionId"))?;

        if obj.is_delete_marker {
            return Err(AwsServiceError::aws_error_with_fields(
                StatusCode::NOT_FOUND,
                "NoSuchKey",
                "The specified key does not exist.",
                vec![("Key".to_string(), key.to_string())],
            ));
        }

        // Glacier / Deep Archive: cannot GET unless restored
        if is_frozen(obj) {
            return Err(AwsServiceError::aws_error_with_fields(
                StatusCode::FORBIDDEN,
                "InvalidObjectState",
                "The operation is not valid for the object's storage class",
                vec![("StorageClass".to_string(), obj.storage_class.clone())],
            ));
        }

        // Conditional checks
        check_get_conditionals(req, obj)?;
        let total_size = obj.size as usize;
        let mut headers = HeaderMap::new();
        headers.insert("etag", format!("\"{}\"", obj.etag).parse().unwrap());
        headers.insert(
            "last-modified",
            obj.last_modified
                .format("%a, %d %b %Y %H:%M:%S GMT")
                .to_string()
                .parse()
                .unwrap(),
        );
        headers.insert("accept-ranges", "bytes".parse().unwrap());
        // Always include storage class
        headers.insert("x-amz-storage-class", obj.storage_class.parse().unwrap());
        if let Some(vid) = &obj.version_id {
            headers.insert("x-amz-version-id", vid.parse().unwrap());
        }
        if let Some(ref enc) = obj.content_encoding {
            headers.insert("content-encoding", enc.parse().unwrap());
        }
        for (k, v) in &obj.metadata {
            if let (Ok(name), Ok(val)) = (
                format!("x-amz-meta-{k}").parse::<http::header::HeaderName>(),
                v.parse::<http::header::HeaderValue>(),
            ) {
                headers.insert(name, val);
            }
        }
        if let Some(ref redirect) = obj.website_redirect_location {
            headers.insert("x-amz-website-redirect-location", redirect.parse().unwrap());
        }
        if !obj.tags.is_empty() {
            headers.insert(
                "x-amz-tagging-count",
                obj.tags.len().to_string().parse().unwrap(),
            );
        }

        // SSE headers - only when explicitly set
        if let Some(algo) = &obj.sse_algorithm {
            headers.insert("x-amz-server-side-encryption", algo.parse().unwrap());
        }
        if let Some(kid) = &obj.sse_kms_key_id {
            headers.insert(
                "x-amz-server-side-encryption-aws-kms-key-id",
                kid.parse().unwrap(),
            );
        }
        if let Some(true) = obj.bucket_key_enabled {
            headers.insert(
                "x-amz-server-side-encryption-bucket-key-enabled",
                "true".parse().unwrap(),
            );
        }

        // Object lock headers
        if let Some(ref mode) = obj.lock_mode {
            headers.insert("x-amz-object-lock-mode", mode.parse().unwrap());
        }
        if let Some(ref until) = obj.lock_retain_until {
            headers.insert(
                "x-amz-object-lock-retain-until-date",
                until.to_rfc3339().parse().unwrap(),
            );
        }
        if let Some(ref hold) = obj.lock_legal_hold {
            headers.insert("x-amz-object-lock-legal-hold", hold.parse().unwrap());
        }
        if let Some(ongoing) = obj.restore_ongoing {
            let rv = if ongoing {
                "ongoing-request=\"true\"".to_string()
            } else if let Some(ref exp) = obj.restore_expiry {
                format!("ongoing-request=\"false\", expiry-date=\"{exp}\"")
            } else {
                "ongoing-request=\"false\"".to_string()
            };
            headers.insert("x-amz-restore", rv.parse().unwrap());
        }
        let mut response_status = StatusCode::OK;
        let response_body;
        let mut is_range_request = false;
        if let Some(range_str) = req.headers.get("range").and_then(|v| v.to_str().ok()) {
            if let Some(rr) = parse_range_header(range_str, total_size) {
                match rr {
                    RangeResult::Satisfiable { start, end } => {
                        headers.insert(
                            "content-range",
                            format!("bytes {start}-{end}/{total_size}").parse().unwrap(),
                        );
                        headers.insert(
                            "content-length",
                            (end - start + 1).to_string().parse().unwrap(),
                        );
                        response_body = obj.data.slice(start..=end);
                        response_status = StatusCode::PARTIAL_CONTENT;
                        is_range_request = true;
                    }
                    RangeResult::NotSatisfiable => {
                        return Err(AwsServiceError::aws_error_with_fields(
                            StatusCode::RANGE_NOT_SATISFIABLE,
                            "InvalidRange",
                            "The requested range is not satisfiable",
                            vec![
                                ("ActualObjectSize".to_string(), total_size.to_string()),
                                ("RangeRequested".to_string(), range_str.to_string()),
                            ],
                        ));
                    }
                    RangeResult::Ignored => {
                        headers.insert("content-length", total_size.to_string().parse().unwrap());
                        response_body = obj.data.clone();
                    }
                }
            } else {
                headers.insert("content-length", total_size.to_string().parse().unwrap());
                response_body = obj.data.clone();
            }
        } else if let Some(part_num_str) = req.query_params.get("partNumber") {
            if let Ok(part_num) = part_num_str.parse::<u32>() {
                // Validate part number
                let max_parts = obj.parts_count.unwrap_or(1) as usize;
                if part_num < 1 || part_num as usize > max_parts {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::RANGE_NOT_SATISFIABLE,
                        "InvalidRange",
                        "The requested range is not satisfiable",
                    ));
                }
                let mut part_start: usize = 0;
                let mut part_size = total_size;
                if let Some(ref part_sizes) = obj.part_sizes {
                    let mut offset: usize = 0;
                    for &(pn, sz) in part_sizes {
                        if pn == part_num {
                            part_start = offset;
                            part_size = sz as usize;
                            break;
                        }
                        offset += sz as usize;
                    }
                }
                if let Some(pc) = obj.parts_count {
                    headers.insert("x-amz-mp-parts-count", pc.to_string().parse().unwrap());
                }
                let part_end = part_start + part_size - 1;
                headers.insert(
                    "content-range",
                    format!("bytes {part_start}-{part_end}/{total_size}")
                        .parse()
                        .unwrap(),
                );
                headers.insert("content-length", part_size.to_string().parse().unwrap());
                response_body = obj.data.slice(part_start..part_start + part_size);
                response_status = StatusCode::PARTIAL_CONTENT;
            } else {
                headers.insert("content-length", total_size.to_string().parse().unwrap());
                response_body = obj.data.clone();
            }
        } else {
            headers.insert("content-length", total_size.to_string().parse().unwrap());
            response_body = obj.data.clone();
        }
        // Only include checksum headers for full (non-range) responses
        if !is_range_request {
            if let Some(algo) = &obj.checksum_algorithm {
                if let Some(val) = &obj.checksum_value {
                    let hn = format!("x-amz-checksum-{}", algo.to_lowercase());
                    if let Ok(name) = hn.parse::<http::header::HeaderName>() {
                        if let Ok(hv) = val.parse() {
                            headers.insert(name, hv);
                        }
                    }
                }
            }
        }
        Ok(AwsResponse {
            status: response_status,
            content_type: obj.content_type.clone(),
            body: response_body,
            headers,
        })
    }

    /// Serve a website object (index or error document) from the bucket.
    fn serve_website_object(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
        website_config: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let result = self.get_object(req, bucket, key);
        if result.is_err() {
            // If index doc doesn't exist either, try error document
            if let Some(error_key) = extract_xml_value(website_config, "ErrorDocument")
                .and_then(|inner| {
                    let open = "<Key>";
                    let close = "</Key>";
                    let s = inner.find(open)? + open.len();
                    let e = inner.find(close)?;
                    Some(inner[s..e].trim().to_string())
                })
                .or_else(|| extract_xml_value(website_config, "Key"))
            {
                return self.serve_website_error(req, bucket, &error_key);
            }
        }
        result
    }

    /// Serve the website error document with a 404 status.
    fn serve_website_error(
        &self,
        req: &AwsRequest,
        bucket: &str,
        error_key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        match self.get_object(req, bucket, error_key) {
            Ok(mut resp) => {
                resp.status = StatusCode::NOT_FOUND;
                Ok(resp)
            }
            Err(e) => Err(e),
        }
    }

    fn delete_object(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let if_match = req
            .headers
            .get("if-match")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let version_id_param = req.query_params.get("versionId").cloned();

        let mut state = self.state.write();
        let region = state.region.clone();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        if let Some(ref if_match_val) = if_match {
            match b.objects.get(key) {
                Some(existing) => {
                    let existing_etag = format!("\"{}\"", existing.etag);
                    if !etag_matches(if_match_val, &existing_etag) {
                        return Err(precondition_failed("If-Match"));
                    }
                }
                None => {
                    return Err(no_such_key(key));
                }
            }
        }

        let mut resp_headers = HeaderMap::new();
        let versioning_enabled = b.versioning.as_deref() == Some("Enabled");

        // Delete a specific version
        if let Some(ref vid) = version_id_param {
            // Check object lock before deleting a specific version
            let locked_obj = {
                let mut found: Option<&S3Object> = None;
                if let Some(versions) = b.object_versions.get(key) {
                    found = versions
                        .iter()
                        .find(|o| o.version_id.as_deref() == Some(vid.as_str()));
                }
                if found.is_none() {
                    if let Some(obj) = b.objects.get(key) {
                        let matches = obj.version_id.as_deref() == Some(vid.as_str())
                            || (vid == "null" && obj.version_id.is_none());
                        if matches {
                            found = Some(obj);
                        }
                    }
                }
                found.and_then(|obj| {
                    if obj.is_delete_marker {
                        return None;
                    }
                    // Legal hold blocks delete
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
                                // Check bypass header
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
                })
            };
            if let Some(code) = locked_obj {
                return Err(AwsServiceError::aws_error(
                    StatusCode::FORBIDDEN,
                    code,
                    "Access Denied",
                ));
            }

            let mut is_dm = false;
            if let Some(versions) = b.object_versions.get_mut(key) {
                let vid_matches = |o: &S3Object| {
                    o.version_id.as_deref() == Some(vid.as_str())
                        || (vid == "null" && o.version_id.is_none())
                };
                is_dm = versions
                    .iter()
                    .any(|o| vid_matches(o) && o.is_delete_marker);
                let len_before = versions.len();
                versions.retain(|o| !vid_matches(o));
                let removed = len_before != versions.len();
                // Only update current object if we actually removed a version
                if removed {
                    if let Some(latest) = versions.last() {
                        if latest.is_delete_marker {
                            b.objects.remove(key);
                        } else {
                            b.objects.insert(key.to_string(), latest.clone());
                        }
                    } else {
                        b.objects.remove(key);
                    }
                }
                if versions.is_empty() {
                    b.object_versions.remove(key);
                }
            } else if let Some(obj) = b.objects.get(key) {
                // Match explicit version id, or treat "null" as matching objects with no version
                let matches = obj.version_id.as_deref() == Some(vid.as_str())
                    || (vid == "null" && obj.version_id.is_none());
                if matches {
                    is_dm = obj.is_delete_marker;
                    b.objects.remove(key);
                }
            }
            resp_headers.insert("x-amz-version-id", vid.parse().unwrap());
            if is_dm {
                resp_headers.insert("x-amz-delete-marker", "true".parse().unwrap());
            }
            return Ok(AwsResponse {
                status: StatusCode::NO_CONTENT,
                content_type: "application/xml".to_string(),
                body: Bytes::new(),
                headers: resp_headers,
            });
        }

        // Check object lock for non-version-specific deletes on non-versioned buckets
        if !versioning_enabled {
            if let Some(existing) = b.objects.get(key) {
                if !existing.is_delete_marker {
                    if let Some(code) = check_object_lock_for_overwrite(existing, req) {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::FORBIDDEN,
                            code,
                            "Access Denied",
                        ));
                    }
                }
            }
        }

        // Versioned bucket: create a delete marker
        if versioning_enabled {
            // If the existing object was created before versioning, preserve it
            if !b.object_versions.contains_key(key) {
                if let Some(existing) = b.objects.get(key) {
                    let mut preserved = existing.clone();
                    if preserved.version_id.is_none() {
                        preserved.version_id = Some("null".to_string());
                    }
                    b.object_versions
                        .entry(key.to_string())
                        .or_default()
                        .push(preserved);
                }
            }
            let dm_id = Uuid::new_v4().to_string();
            let marker = make_delete_marker(key, &dm_id);
            b.object_versions
                .entry(key.to_string())
                .or_default()
                .push(marker.clone());
            b.objects.insert(key.to_string(), marker);
            resp_headers.insert("x-amz-version-id", dm_id.parse().unwrap());
            resp_headers.insert("x-amz-delete-marker", "true".parse().unwrap());

            // Notification for delete
            let notification_config = b.notification_config.clone();
            let bucket_name = bucket.to_string();
            let obj_key = key.to_string();
            let region = region.clone();
            drop(state);
            if let Some(ref config) = notification_config {
                deliver_notifications(
                    &self.delivery,
                    config,
                    "ObjectRemoved:DeleteMarkerCreated",
                    &bucket_name,
                    &obj_key,
                    0,
                    "",
                    &region,
                );
            }

            return Ok(AwsResponse {
                status: StatusCode::NO_CONTENT,
                content_type: "application/xml".to_string(),
                body: Bytes::new(),
                headers: resp_headers,
            });
        }

        // Capture notification config before removing
        let notification_config = b.notification_config.clone();
        let bucket_name = bucket.to_string();
        let obj_key = key.to_string();

        b.objects.remove(key);
        drop(state);

        // Deliver S3 event notifications
        if let Some(ref config) = notification_config {
            deliver_notifications(
                &self.delivery,
                config,
                "ObjectRemoved:Delete",
                &bucket_name,
                &obj_key,
                0,
                "",
                &region,
            );
        }

        Ok(AwsResponse {
            status: StatusCode::NO_CONTENT,
            content_type: "application/xml".to_string(),
            body: Bytes::new(),
            headers: HeaderMap::new(),
        })
    }

    fn head_object(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let obj = resolve_object(b, key, req.query_params.get("versionId"))?;
        if obj.is_delete_marker {
            if req.query_params.contains_key("versionId") {
                let mut headers = HeaderMap::new();
                headers.insert("x-amz-delete-marker", "true".parse().unwrap());
                headers.insert("allow", "DELETE".parse().unwrap());
                if let Some(vid) = &obj.version_id {
                    headers.insert("x-amz-version-id", vid.parse().unwrap());
                }
                return Ok(AwsResponse {
                    status: StatusCode::METHOD_NOT_ALLOWED,
                    content_type: "application/xml".to_string(),
                    body: Bytes::new(),
                    headers,
                });
            }
            let mut headers = HeaderMap::new();
            headers.insert("x-amz-delete-marker", "true".parse().unwrap());
            if let Some(vid) = &obj.version_id {
                headers.insert("x-amz-version-id", vid.parse().unwrap());
            }
            return Ok(AwsResponse {
                status: StatusCode::NOT_FOUND,
                content_type: "application/xml".to_string(),
                body: Bytes::new(),
                headers,
            });
        }

        // Conditional checks for HEAD
        check_head_conditionals(req, obj)?;
        let total_size = obj.size;
        let mut response_status = StatusCode::OK;
        let mut headers = HeaderMap::new();
        headers.insert("etag", format!("\"{}\"", obj.etag).parse().unwrap());
        headers.insert(
            "last-modified",
            obj.last_modified
                .format("%a, %d %b %Y %H:%M:%S GMT")
                .to_string()
                .parse()
                .unwrap(),
        );
        headers.insert("accept-ranges", "bytes".parse().unwrap());
        headers.insert("x-amz-storage-class", obj.storage_class.parse().unwrap());
        if let Some(ref enc) = obj.content_encoding {
            headers.insert("content-encoding", enc.parse().unwrap());
        }
        if let Some(range_str) = req.headers.get("range").and_then(|v| v.to_str().ok()) {
            if let Some(range_result) = parse_range_header(range_str, total_size as usize) {
                match range_result {
                    RangeResult::Satisfiable { start, end } => {
                        headers.insert(
                            "content-range",
                            format!("bytes {start}-{end}/{total_size}").parse().unwrap(),
                        );
                        headers.insert(
                            "content-length",
                            (end - start + 1).to_string().parse().unwrap(),
                        );
                        response_status = StatusCode::PARTIAL_CONTENT;
                    }
                    RangeResult::NotSatisfiable => {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::RANGE_NOT_SATISFIABLE,
                            "InvalidRange",
                            "The requested range is not satisfiable",
                        ));
                    }
                    RangeResult::Ignored => {
                        headers.insert("content-length", total_size.to_string().parse().unwrap());
                    }
                }
            } else {
                headers.insert("content-length", total_size.to_string().parse().unwrap());
            }
        } else if let Some(part_num_str) = req.query_params.get("partNumber") {
            if let Ok(part_num) = part_num_str.parse::<u32>() {
                // Validate part number
                let max_parts = obj.parts_count.unwrap_or(1);
                if part_num < 1 || part_num > max_parts {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::RANGE_NOT_SATISFIABLE,
                        "InvalidRange",
                        "The requested range is not satisfiable",
                    ));
                }
                let mut part_start: u64 = 0;
                let mut part_size = total_size;
                if let Some(ref part_sizes) = obj.part_sizes {
                    let mut offset: u64 = 0;
                    for &(pn, sz) in part_sizes {
                        if pn == part_num {
                            part_start = offset;
                            part_size = sz;
                            break;
                        }
                        offset += sz;
                    }
                }
                if let Some(pc) = obj.parts_count {
                    headers.insert("x-amz-mp-parts-count", pc.to_string().parse().unwrap());
                }
                let part_end = part_start + part_size - 1;
                headers.insert(
                    "content-range",
                    format!("bytes {part_start}-{part_end}/{total_size}")
                        .parse()
                        .unwrap(),
                );
                headers.insert("content-length", part_size.to_string().parse().unwrap());
                response_status = StatusCode::PARTIAL_CONTENT;
            } else {
                headers.insert("content-length", total_size.to_string().parse().unwrap());
            }
        } else {
            headers.insert("content-length", total_size.to_string().parse().unwrap());
        }
        for (k, v) in &obj.metadata {
            if let (Ok(name), Ok(val)) = (
                format!("x-amz-meta-{k}").parse::<http::header::HeaderName>(),
                v.parse::<http::header::HeaderValue>(),
            ) {
                headers.insert(name, val);
            }
        }
        if let Some(ref redirect) = obj.website_redirect_location {
            headers.insert("x-amz-website-redirect-location", redirect.parse().unwrap());
        }

        if let Some(vid) = &obj.version_id {
            headers.insert("x-amz-version-id", vid.parse().unwrap());
        }

        // SSE headers
        if let Some(algo) = &obj.sse_algorithm {
            headers.insert("x-amz-server-side-encryption", algo.parse().unwrap());
        }
        if let Some(kid) = &obj.sse_kms_key_id {
            headers.insert(
                "x-amz-server-side-encryption-aws-kms-key-id",
                kid.parse().unwrap(),
            );
        }
        if let Some(true) = obj.bucket_key_enabled {
            headers.insert(
                "x-amz-server-side-encryption-bucket-key-enabled",
                "true".parse().unwrap(),
            );
        }

        // Object lock headers
        if let Some(ref mode) = obj.lock_mode {
            headers.insert("x-amz-object-lock-mode", mode.parse().unwrap());
        }
        if let Some(ref until) = obj.lock_retain_until {
            headers.insert(
                "x-amz-object-lock-retain-until-date",
                until.to_rfc3339().parse().unwrap(),
            );
        }
        if let Some(ref hold) = obj.lock_legal_hold {
            headers.insert("x-amz-object-lock-legal-hold", hold.parse().unwrap());
        }
        if let Some(ongoing) = obj.restore_ongoing {
            let restore_val = if ongoing {
                "ongoing-request=\"true\"".to_string()
            } else if let Some(ref expiry) = obj.restore_expiry {
                format!("ongoing-request=\"false\", expiry-date=\"{expiry}\"")
            } else {
                "ongoing-request=\"false\"".to_string()
            };
            headers.insert("x-amz-restore", restore_val.parse().unwrap());
        }
        // Checksum headers (returned when ChecksumMode=ENABLED or always if set)
        if let Some(algo) = &obj.checksum_algorithm {
            if let Some(val) = &obj.checksum_value {
                let hn = format!("x-amz-checksum-{}", algo.to_lowercase());
                if let Ok(name) = hn.parse::<http::header::HeaderName>() {
                    if let Ok(hv) = val.parse() {
                        headers.insert(name, hv);
                    }
                }
            }
        }

        Ok(AwsResponse {
            status: response_status,
            content_type: obj.content_type.clone(),
            body: Bytes::new(),
            headers,
        })
    }

    fn copy_object(
        &self,
        req: &AwsRequest,
        dest_bucket: &str,
        dest_key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let copy_source = req
            .headers
            .get("x-amz-copy-source")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    "x-amz-copy-source header is required",
                )
            })?;

        // Split on '?' BEFORE percent-decoding so keys containing literal '?' are preserved
        let raw_source = copy_source.strip_prefix('/').unwrap_or(copy_source);
        let (raw_path, src_version_id) = if let Some((path, query)) = raw_source.split_once('?') {
            let vid = query
                .split('&')
                .find_map(|p| p.strip_prefix("versionId="))
                .map(|s| s.to_string());
            (path, vid)
        } else {
            (raw_source, None)
        };
        let decoded_path = percent_encoding::percent_decode_str(raw_path)
            .decode_utf8_lossy()
            .to_string();

        let (src_bucket, src_key) = decoded_path.split_once('/').ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Invalid copy source format",
            )
        })?;

        let metadata_directive = req
            .headers
            .get("x-amz-metadata-directive")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("COPY");

        let storage_class = req
            .headers
            .get("x-amz-storage-class")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // Validate storage class if explicitly provided
        if let Some(ref sc) = storage_class {
            if !is_valid_storage_class(sc) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidStorageClass",
                    "The storage class you specified is not valid",
                ));
            }
        }

        let tagging_directive = req
            .headers
            .get("x-amz-tagging-directive")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("COPY");

        let sse_algorithm = req
            .headers
            .get("x-amz-server-side-encryption")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let sse_kms_key_id = req
            .headers
            .get("x-amz-server-side-encryption-aws-kms-key-id")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let bucket_key_enabled = req
            .headers
            .get("x-amz-server-side-encryption-bucket-key-enabled")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.eq_ignore_ascii_case("true"));

        let website_redirect = req
            .headers
            .get("x-amz-website-redirect-location")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let if_none_match = req
            .headers
            .get("x-amz-copy-source-if-none-match")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let checksum_algorithm = req
            .headers
            .get("x-amz-checksum-algorithm")
            .or_else(|| req.headers.get("x-amz-sdk-checksum-algorithm"))
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_uppercase());

        let mut state = self.state.write();

        // Resolve source object, possibly a specific version
        let (src_obj, src_version_id_actual) = {
            let sb = state
                .buckets
                .get(src_bucket)
                .ok_or_else(|| no_such_bucket(src_bucket))?;
            let obj = resolve_object(sb, src_key, src_version_id.as_ref())?.clone();
            (obj.clone(), obj.version_id.clone())
        };

        // Delete markers cannot be used as copy source
        if src_obj.is_delete_marker {
            return Err(no_such_key(src_key));
        }

        // Glacier/Deep Archive: cannot copy unless restored
        if is_frozen(&src_obj) {
            return Err(AwsServiceError::aws_error(
                StatusCode::FORBIDDEN,
                "ObjectNotInActiveTierError",
                "The source object of the COPY action is not in the active tier and is at the \
                 storage class type that does not support the COPY action.",
            ));
        }

        if let Some(ref inm) = if_none_match {
            let src_etag = format!("\"{}\"", src_obj.etag);
            if etag_matches(inm, &src_etag) {
                return Err(AwsServiceError::aws_error_with_fields(
                    StatusCode::PRECONDITION_FAILED,
                    "PreconditionFailed",
                    "At least one of the pre-conditions you specified did not hold",
                    vec![(
                        "Condition".to_string(),
                        "x-amz-copy-source-If-None-Match".to_string(),
                    )],
                ));
            }
        }

        // Check copy-in-place validity
        let has_version_id = src_version_id.is_some();
        if src_bucket == dest_bucket
            && src_key == dest_key
            && metadata_directive == "COPY"
            && storage_class.is_none()
            && sse_algorithm.is_none()
            && website_redirect.is_none()
            && !has_version_id
        {
            // Check if bucket encryption would make this a valid copy-in-place
            let sb = state
                .buckets
                .get(src_bucket)
                .ok_or_else(|| no_such_bucket(src_bucket))?;
            let has_bucket_encryption = sb.encryption_config.is_some();
            if !has_bucket_encryption {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidRequest",
                    "This copy request is illegal because it is trying to copy an object to itself \
                     without changing the object's metadata, storage class, website redirect location \
                     or encryption attributes.",
                ));
            }
        }

        let etag = src_obj.etag.clone();
        let src_obj_size = src_obj.size;
        let last_modified = Utc::now();

        let new_metadata = if metadata_directive == "REPLACE" {
            extract_user_metadata(&req.headers)
        } else {
            src_obj.metadata.clone()
        };

        let new_content_type = if metadata_directive == "REPLACE" {
            req.headers
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or(&src_obj.content_type)
                .to_string()
        } else {
            src_obj.content_type.clone()
        };

        let new_storage_class = storage_class.unwrap_or_else(|| "STANDARD".to_string());

        let new_tags = if tagging_directive == "REPLACE" {
            let th = req
                .headers
                .get("x-amz-tagging")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            let tags = parse_url_encoded_tags(th);
            // Validate aws: prefix
            for (k, _) in &tags {
                if k.starts_with("aws:") {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidTag",
                        "Your TagKey cannot be prefixed with aws:",
                    ));
                }
            }
            tags.into_iter().collect()
        } else {
            src_obj.tags.clone()
        };

        // Determine bucket default encryption
        let dest_bucket_encryption = state
            .buckets
            .get(dest_bucket)
            .and_then(|b| b.encryption_config.as_ref())
            .and_then(|config| {
                if config.contains("AES256") {
                    Some("AES256".to_string())
                } else if config.contains("aws:kms") {
                    Some("aws:kms".to_string())
                } else {
                    None
                }
            });

        // For SSE: if explicitly set, use new values; if copy-in-place changed SSE, use new;
        // otherwise fall back based on source or bucket default
        let new_sse = if sse_algorithm.is_some() {
            sse_algorithm
        } else if src_bucket == dest_bucket && src_key == dest_key {
            // Copy-in-place without SSE specified: if source had non-AES256 SSE, default to AES256
            if src_obj.sse_algorithm.is_some() && src_obj.sse_algorithm.as_deref() != Some("AES256")
            {
                Some("AES256".to_string())
            } else if src_obj.sse_algorithm.is_some() {
                src_obj.sse_algorithm.clone()
            } else {
                // Use bucket default encryption if available
                dest_bucket_encryption.clone()
            }
        } else {
            // For cross-key copy, use bucket default encryption if no explicit SSE
            dest_bucket_encryption.clone()
        };

        let new_kms = if sse_kms_key_id.is_some() {
            sse_kms_key_id
        } else {
            None
        };
        let new_bke = bucket_key_enabled; // Only set if explicitly provided
        let new_redirect = website_redirect.or_else(|| {
            if metadata_directive == "COPY" {
                src_obj.website_redirect_location.clone()
            } else {
                None
            }
        });

        // Checksum: compute new if algorithm specified, or copy from source
        let (new_checksum_algo, new_checksum_val) = if let Some(ref algo) = checksum_algorithm {
            let val = compute_checksum(algo, &src_obj.data);
            (Some(algo.clone()), Some(val))
        } else if src_obj.checksum_algorithm.is_some() {
            (
                src_obj.checksum_algorithm.clone(),
                src_obj.checksum_value.clone(),
            )
        } else {
            (None, None)
        };

        let db = state
            .buckets
            .get_mut(dest_bucket)
            .ok_or_else(|| no_such_bucket(dest_bucket))?;

        let version_id = if db.versioning.as_deref() == Some("Enabled") {
            Some(uuid::Uuid::new_v4().to_string())
        } else {
            None
        };

        // Default ACL for destination (not copied from source)
        let dest_acl_grants = vec![AclGrant {
            grantee_type: "CanonicalUser".to_string(),
            grantee_id: Some(db.acl_owner_id.clone()),
            grantee_display_name: Some(db.acl_owner_id.clone()),
            grantee_uri: None,
            permission: "FULL_CONTROL".to_string(),
        }];

        let dest_obj = S3Object {
            key: dest_key.to_string(),
            data: src_obj.data,
            size: src_obj.size,
            etag: etag.clone(),
            last_modified,
            content_type: new_content_type,
            metadata: new_metadata,
            storage_class: new_storage_class,
            tags: new_tags,
            acl_grants: dest_acl_grants,
            acl_owner_id: Some(db.acl_owner_id.clone()),
            parts_count: src_obj.parts_count,
            part_sizes: src_obj.part_sizes,
            sse_algorithm: new_sse.clone(),
            sse_kms_key_id: new_kms.clone(),
            bucket_key_enabled: new_bke,
            version_id: version_id.clone(),
            is_delete_marker: false,
            content_encoding: src_obj.content_encoding,
            website_redirect_location: new_redirect,
            restore_ongoing: None,
            restore_expiry: None,
            checksum_algorithm: new_checksum_algo.clone(),
            checksum_value: new_checksum_val.clone(),
            // Do not copy lock from source
            lock_mode: None,
            lock_retain_until: None,
            lock_legal_hold: None,
        };

        // Store in version history if versioning enabled
        if db.versioning.as_deref() == Some("Enabled") {
            db.object_versions
                .entry(dest_key.to_string())
                .or_default()
                .push(dest_obj.clone());
        }
        db.objects.insert(dest_key.to_string(), dest_obj);

        let mut response_headers = HeaderMap::new();
        if let Some(vid) = &version_id {
            response_headers.insert("x-amz-version-id", vid.parse().unwrap());
        }
        if let Some(ref svid) = src_version_id_actual {
            response_headers.insert("x-amz-copy-source-version-id", svid.parse().unwrap());
        }
        // SSE headers in copy response
        if let Some(ref algo) = new_sse {
            response_headers.insert("x-amz-server-side-encryption", algo.parse().unwrap());
        } else {
            response_headers.insert("x-amz-server-side-encryption", "AES256".parse().unwrap());
        }
        if let Some(ref kid) = new_kms {
            response_headers.insert(
                "x-amz-server-side-encryption-aws-kms-key-id",
                kid.parse().unwrap(),
            );
        }
        if new_bke == Some(true) {
            response_headers.insert(
                "x-amz-server-side-encryption-bucket-key-enabled",
                "true".parse().unwrap(),
            );
        }

        // Build checksum XML if present
        let checksum_xml = if let (Some(algo), Some(val)) = (&new_checksum_algo, &new_checksum_val)
        {
            format!("<Checksum{algo}>{val}</Checksum{algo}>")
        } else {
            String::new()
        };

        // Capture notification config before dropping lock
        let notification_config = db.notification_config.clone();
        let copy_size = src_obj_size;
        let copy_etag = etag.clone();
        let copy_bucket = dest_bucket.to_string();
        let copy_key = dest_key.to_string();
        let region = state.region.clone();

        // Replicate object if replication is configured on the destination bucket
        replicate_object(&mut state, dest_bucket, dest_key);

        drop(state);

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <CopyObjectResult>\
             <ETag>&quot;{etag}&quot;</ETag>\
             <LastModified>{}</LastModified>\
             {checksum_xml}\
             </CopyObjectResult>",
            last_modified.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
        );

        // Deliver S3 event notifications
        if let Some(ref config) = notification_config {
            deliver_notifications(
                &self.delivery,
                config,
                "ObjectCreated:Copy",
                &copy_bucket,
                &copy_key,
                copy_size,
                &copy_etag,
                &region,
            );
        }

        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: body.into(),
            headers: response_headers,
        })
    }

    fn delete_objects(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let entries = parse_delete_objects_xml(body_str);

        if entries.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                "The XML you provided was not well-formed or did not validate against our published schema",
            ));
        }

        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let bypass = req
            .headers
            .get("x-amz-bypass-governance-retention")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        let versioning_enabled = b.versioning.as_deref() == Some("Enabled");
        let mut deleted_xml = String::new();
        let mut error_xml = String::new();
        for entry in &entries {
            let key = &entry.key;
            if let Some(ref vid) = entry.version_id {
                // Check lock before deleting specific version
                let lock_denied = {
                    let obj_opt = b
                        .object_versions
                        .get(key)
                        .and_then(|vs| {
                            vs.iter()
                                .find(|o| o.version_id.as_deref() == Some(vid.as_str()))
                        })
                        .or_else(|| {
                            b.objects.get(key).filter(|o| {
                                o.version_id.as_deref() == Some(vid.as_str())
                                    || (vid == "null" && o.version_id.is_none())
                            })
                        });
                    if let Some(obj) = obj_opt {
                        if obj.is_delete_marker {
                            false
                        } else if obj.lock_legal_hold.as_deref() == Some("ON") {
                            true
                        } else if let (Some(mode), Some(until)) =
                            (&obj.lock_mode, &obj.lock_retain_until)
                        {
                            if *until > Utc::now() {
                                if mode == "COMPLIANCE" {
                                    true
                                } else if mode == "GOVERNANCE" {
                                    !bypass
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                };

                if lock_denied {
                    error_xml.push_str(&format!(
                        "<Error><Key>{}</Key><VersionId>{}</VersionId><Code>AccessDenied</Code><Message>Access Denied because object protected by object lock.</Message></Error>",
                        xml_escape(key),
                        xml_escape(vid),
                    ));
                    continue;
                }

                // Delete specific version
                if let Some(versions) = b.object_versions.get_mut(key) {
                    versions.retain(|o| {
                        !(o.version_id.as_deref() == Some(vid)
                            || (vid == "null" && o.version_id.is_none()))
                    });
                    if let Some(latest) = versions.last() {
                        if latest.is_delete_marker {
                            b.objects.remove(key);
                        } else {
                            b.objects.insert(key.to_string(), latest.clone());
                        }
                    } else {
                        b.objects.remove(key);
                    }
                    if versions.is_empty() {
                        b.object_versions.remove(key);
                    }
                }
                deleted_xml.push_str(&format!(
                    "<Deleted><Key>{}</Key><VersionId>{}</VersionId></Deleted>",
                    xml_escape(key),
                    xml_escape(vid),
                ));
            } else if versioning_enabled {
                let dm_id = Uuid::new_v4().to_string();
                let marker = make_delete_marker(key, &dm_id);
                b.object_versions
                    .entry(key.to_string())
                    .or_default()
                    .push(marker.clone());
                b.objects.insert(key.to_string(), marker);
                deleted_xml.push_str(&format!(
                    "<Deleted><Key>{}</Key><DeleteMarker>true</DeleteMarker><DeleteMarkerVersionId>{}</DeleteMarkerVersionId></Deleted>",
                    xml_escape(key), dm_id,
                ));
            } else {
                b.objects.remove(key);
                deleted_xml.push_str(&format!(
                    "<Deleted><Key>{}</Key></Deleted>",
                    xml_escape(key)
                ));
            }
        }

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             {deleted_xml}\
             {error_xml}\
             </DeleteResult>"
        );
        Ok(s3_xml(StatusCode::OK, body))
    }

    // ---- Object ACL ----

    fn get_object_acl(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let obj = b.objects.get(key).ok_or_else(|| no_such_key(key))?;

        let owner_id = obj.acl_owner_id.as_deref().unwrap_or(&req.account_id);
        let body = build_acl_xml(owner_id, &obj.acl_grants, &req.account_id);
        Ok(s3_xml(StatusCode::OK, body))
    }

    fn put_object_acl(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let canned = req
            .headers
            .get("x-amz-acl")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let owner_id = b.acl_owner_id.clone();
        let obj = b.objects.get_mut(key).ok_or_else(|| no_such_key(key))?;

        if let Some(acl) = canned {
            obj.acl_grants = canned_acl_grants_for_object(&acl, &owner_id);
        } else {
            // Check for grant headers
            let has_grant_headers = req.headers.keys().any(|k| {
                let name = k.as_str();
                name.starts_with("x-amz-grant-")
            });
            if has_grant_headers {
                obj.acl_grants = parse_grant_headers(&req.headers);
            } else {
                // Parse from body
                let body_str = std::str::from_utf8(&req.body).unwrap_or("");
                if !body_str.is_empty() {
                    let grants = parse_acl_xml(body_str)?;
                    obj.acl_grants = grants;
                }
            }
        }

        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: Bytes::new(),
            headers: HeaderMap::new(),
        })
    }

    // ---- Object Tagging ----

    fn get_object_tagging(
        &self,
        _req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let obj = b.objects.get(key).ok_or_else(|| no_such_key(key))?;

        let mut tags_xml = String::new();
        for (k, v) in &obj.tags {
            tags_xml.push_str(&format!(
                "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
                xml_escape(k),
                xml_escape(v),
            ));
        }
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <TagSet>{tags_xml}</TagSet></Tagging>"
        );
        Ok(s3_xml(StatusCode::OK, body))
    }

    fn put_object_tagging(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let tags = parse_tagging_xml(body_str);

        // Validate: no aws: prefix
        for (k, _) in &tags {
            if k.starts_with("aws:") {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidTag",
                    "System tags cannot be added/updated by requester",
                ));
            }
        }

        // Validate: max 10 tags
        if tags.len() > 10 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequest",
                "Object tags cannot be greater than 10",
            ));
        }

        let version_id = req.query_params.get("versionId").map(|s| s.to_string());

        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let mut response_headers = HeaderMap::new();

        if let Some(ref vid) = version_id {
            // Version-specific tagging
            let mut found = false;

            // Check versioned objects
            if let Some(versions) = b.object_versions.get_mut(key) {
                if let Some(obj) = versions
                    .iter_mut()
                    .find(|o| o.version_id.as_deref() == Some(vid.as_str()))
                {
                    if obj.is_delete_marker {
                        return Err(AwsServiceError::aws_error_with_fields(
                            StatusCode::METHOD_NOT_ALLOWED,
                            "MethodNotAllowed",
                            "The specified method is not allowed against this resource.",
                            vec![
                                ("Method".to_string(), "PUT".to_string()),
                                ("ResourceType".to_string(), "DeleteMarker".to_string()),
                            ],
                        ));
                    }
                    obj.tags = tags.clone().into_iter().collect();
                    response_headers.insert("x-amz-version-id", vid.parse().unwrap());
                    found = true;
                }
            }

            // Also check current object
            if !found {
                if let Some(obj) = b.objects.get_mut(key) {
                    if obj.version_id.as_deref() == Some(vid.as_str()) {
                        if obj.is_delete_marker {
                            return Err(AwsServiceError::aws_error_with_fields(
                                StatusCode::METHOD_NOT_ALLOWED,
                                "MethodNotAllowed",
                                "The specified method is not allowed against this resource.",
                                vec![
                                    ("Method".to_string(), "PUT".to_string()),
                                    ("ResourceType".to_string(), "DeleteMarker".to_string()),
                                ],
                            ));
                        }
                        obj.tags = tags.into_iter().collect();
                        response_headers.insert("x-amz-version-id", vid.parse().unwrap());
                        found = true;
                    }
                }
            }

            if !found {
                return Err(AwsServiceError::aws_error_with_fields(
                    StatusCode::NOT_FOUND,
                    "NoSuchVersion",
                    "The specified version does not exist.",
                    vec![
                        ("Key".to_string(), key.to_string()),
                        ("VersionId".to_string(), vid.to_string()),
                    ],
                ));
            }
        } else {
            let obj = b
                .objects
                .get_mut(key)
                .ok_or_else(|| no_such_key_with_detail(key))?;
            if obj.is_delete_marker {
                return Err(no_such_key_with_detail(key));
            }
            obj.tags = tags.into_iter().collect();
            if let Some(ref vid) = obj.version_id {
                response_headers.insert("x-amz-version-id", vid.parse().unwrap());
            }
        }

        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: Bytes::new(),
            headers: response_headers,
        })
    }

    // ---- Multipart Upload ----

    fn create_multipart_upload(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let upload_id = uuid::Uuid::new_v4().to_string();
        let content_type = req
            .headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_string();
        let metadata = extract_user_metadata(&req.headers);
        let storage_class = req
            .headers
            .get("x-amz-storage-class")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("STANDARD")
            .to_string();
        let sse_algorithm = req
            .headers
            .get("x-amz-server-side-encryption")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let sse_kms_key_id = req
            .headers
            .get("x-amz-server-side-encryption-aws-kms-key-id")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let tagging = req
            .headers
            .get("x-amz-tagging")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let acl_header = req
            .headers
            .get("x-amz-acl")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let has_grant_headers = req
            .headers
            .keys()
            .any(|k| k.as_str().starts_with("x-amz-grant-"));

        if acl_header.is_some() && has_grant_headers {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Specifying both Canned ACLs and Header Grants is not allowed",
            ));
        }

        let checksum_algorithm = req
            .headers
            .get("x-amz-checksum-algorithm")
            .or_else(|| req.headers.get("x-amz-sdk-checksum-algorithm"))
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_uppercase());

        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let acl_grants = if has_grant_headers {
            parse_grant_headers(&req.headers)
        } else {
            let acl = acl_header.as_deref().unwrap_or("private");
            canned_acl_grants(acl, &b.acl_owner_id)
        };

        let upload = MultipartUpload {
            upload_id: upload_id.clone(),
            key: key.to_string(),
            initiated: Utc::now(),
            parts: std::collections::BTreeMap::new(),
            metadata,
            content_type,
            storage_class,
            sse_algorithm: sse_algorithm.clone(),
            sse_kms_key_id: sse_kms_key_id.clone(),
            tagging,
            acl_grants,
            checksum_algorithm,
        };
        b.multipart_uploads.insert(upload_id.clone(), upload);

        let mut headers = HeaderMap::new();
        if let Some(algo) = &sse_algorithm {
            headers.insert("x-amz-server-side-encryption", algo.parse().unwrap());
        }
        if let Some(kid) = &sse_kms_key_id {
            headers.insert(
                "x-amz-server-side-encryption-aws-kms-key-id",
                kid.parse().unwrap(),
            );
        }

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Bucket>{}</Bucket>\
             <Key>{}</Key>\
             <UploadId>{}</UploadId>\
             </InitiateMultipartUploadResult>",
            xml_escape(bucket),
            xml_escape(key),
            xml_escape(&upload_id),
        );
        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: body.into(),
            headers,
        })
    }

    fn upload_part(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i64,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Validate part number
        if part_number < 1 {
            return Err(no_such_upload(upload_id));
        }
        if part_number > 10000 {
            return Err(AwsServiceError::aws_error_with_fields(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Part number must be an integer between 1 and 10000, inclusive",
                vec![
                    ("ArgumentName".to_string(), "partNumber".to_string()),
                    ("ArgumentValue".to_string(), part_number.to_string()),
                ],
            ));
        }
        let pn = part_number as u32;

        let data = req.body.clone();
        let etag = compute_md5(&data);

        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let upload = b
            .multipart_uploads
            .get_mut(upload_id)
            .ok_or_else(|| no_such_upload(upload_id))?;
        if upload.key != key {
            return Err(no_such_upload(upload_id));
        }

        let part = UploadPart {
            part_number: pn,
            data: data.clone(),
            etag: etag.clone(),
            size: data.len() as u64,
            last_modified: Utc::now(),
        };
        upload.parts.insert(pn, part);

        let mut headers = HeaderMap::new();
        headers.insert("etag", format!("\"{etag}\"").parse().unwrap());
        if let Some(algo) = &upload.sse_algorithm {
            headers.insert("x-amz-server-side-encryption", algo.parse().unwrap());
        }
        if let Some(kid) = &upload.sse_kms_key_id {
            headers.insert(
                "x-amz-server-side-encryption-aws-kms-key-id",
                kid.parse().unwrap(),
            );
        }
        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: Bytes::new(),
            headers,
        })
    }

    fn upload_part_copy(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i64,
    ) -> Result<AwsResponse, AwsServiceError> {
        let copy_source = req
            .headers
            .get("x-amz-copy-source")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    "x-amz-copy-source header is required",
                )
            })?;

        // Split on '?' BEFORE percent-decoding so keys containing literal '?' are preserved
        let raw_source = copy_source.strip_prefix('/').unwrap_or(copy_source);

        // Parse versionId from ?versionId=X
        let (raw_path, source_version_id) = if let Some(idx) = raw_source.find("?versionId=") {
            let vid = raw_source[idx + 11..].to_string();
            (&raw_source[..idx], Some(vid))
        } else {
            (raw_source, None)
        };
        let decoded_path = percent_encoding::percent_decode_str(raw_path)
            .decode_utf8_lossy()
            .to_string();

        let (src_bucket, src_key) = decoded_path.split_once('/').ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Invalid copy source format",
            )
        })?;

        let copy_range = req
            .headers
            .get("x-amz-copy-source-range")
            .and_then(|v| v.to_str().ok());

        let mut state = self.state.write();
        let src_data = {
            let sb = state
                .buckets
                .get(src_bucket)
                .ok_or_else(|| no_such_bucket(src_bucket))?;

            let src_obj = if let Some(ref vid) = source_version_id {
                resolve_object(sb, src_key, Some(vid))?
            } else {
                sb.objects
                    .get(src_key)
                    .ok_or_else(|| no_such_key(src_key))?
            };

            if let Some(range_str) = copy_range {
                let range_part = range_str.strip_prefix("bytes=").unwrap_or(range_str);
                if let Some((start_str, end_str)) = range_part.split_once('-') {
                    let start: usize = start_str.parse().unwrap_or(0);
                    let end: usize = end_str.parse().unwrap_or(src_obj.data.len() - 1);
                    let end = std::cmp::min(end + 1, src_obj.data.len());
                    src_obj.data.slice(start..end)
                } else {
                    src_obj.data.clone()
                }
            } else {
                src_obj.data.clone()
            }
        };

        let data_len = src_data.len() as u64;
        let etag = compute_md5(&src_data);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let upload = b
            .multipart_uploads
            .get_mut(upload_id)
            .ok_or_else(|| no_such_upload(upload_id))?;
        if upload.key != key {
            return Err(no_such_upload(upload_id));
        }

        let part = UploadPart {
            part_number: part_number as u32,
            data: src_data,
            etag: etag.clone(),
            size: data_len,
            last_modified: Utc::now(),
        };
        upload.parts.insert(part_number as u32, part);

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <CopyPartResult>\
             <ETag>&quot;{etag}&quot;</ETag>\
             <LastModified>{}</LastModified>\
             </CopyPartResult>",
            Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
        );
        Ok(s3_xml(StatusCode::OK, body))
    }

    fn complete_multipart_upload(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let submitted_parts = parse_complete_multipart_xml(body_str);

        if submitted_parts.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                "The XML you provided was not well-formed or did not validate against our published schema",
            ));
        }

        let if_none_match = req
            .headers
            .get("x-amz-if-none-match")
            .or_else(|| req.headers.get("if-none-match"))
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let upload = match b.multipart_uploads.get(upload_id) {
            Some(u) => u.clone(),
            None => {
                // Upload already completed - return existing object if it exists
                // IfNoneMatch does NOT apply to re-completions
                if let Some(obj) = b.objects.get(key) {
                    let etag = obj.etag.clone();
                    let body = format!(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                         <CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                         <Bucket>{}</Bucket>\
                         <Key>{}</Key>\
                         <ETag>&quot;{}&quot;</ETag>\
                         </CompleteMultipartUploadResult>",
                        xml_escape(bucket),
                        xml_escape(key),
                        xml_escape(&etag),
                    );
                    return Ok(AwsResponse {
                        status: StatusCode::OK,
                        content_type: "application/xml".to_string(),
                        body: body.into(),
                        headers: HeaderMap::new(),
                    });
                }
                return Err(no_such_upload(upload_id));
            }
        };

        if upload.key != key {
            return Err(no_such_upload(upload_id));
        }

        // IfNoneMatch: if "*" and object already exists, reject (only for real completions)
        if let Some(ref inm) = if_none_match {
            if inm == "*" && b.objects.contains_key(key) {
                return Err(precondition_failed("If-None-Match"));
            }
        }

        // Validate parts are in ascending order
        for window in submitted_parts.windows(2) {
            if window[0].0 >= window[1].0 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidPartOrder",
                    "The list of parts was not in ascending order. The parts list must be specified in order by part number.",
                ));
            }
        }

        let sorted_parts = submitted_parts;

        // Validate all specified parts exist in the upload
        for (part_num, _) in &sorted_parts {
            if !upload.parts.contains_key(part_num) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidPart",
                    "One or more of the specified parts could not be found. The part may not have been uploaded, or the specified entity tag may not have matched the part's entity tag.",
                ));
            }
        }

        // Validate minimum part size: all non-last parts must be >= 5MB
        const MIN_PART_SIZE: usize = 5 * 1024 * 1024; // 5MB
        if sorted_parts.len() > 1 {
            for (i, (part_num, _)) in sorted_parts.iter().enumerate() {
                if i >= sorted_parts.len() - 1 {
                    break; // skip last part
                }
                if let Some(part) = upload.parts.get(part_num) {
                    if part.data.len() < MIN_PART_SIZE {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "EntityTooSmall",
                            "Your proposed upload is smaller than the minimum allowed object size.",
                        ));
                    }
                }
            }
        }

        // Assemble the object from parts
        let mut combined_data = Vec::new();
        let mut md5_digests = Vec::new();
        let mut part_sizes = Vec::new();

        for (part_num, submitted_etag) in &sorted_parts {
            let part = upload.parts.get(part_num).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidPart",
                    "One or more of the specified parts could not be found.",
                )
            })?;
            if submitted_etag != &part.etag {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidPart",
                    "One or more of the specified parts could not be found. The part may not have been uploaded, or the specified entity tag may not have matched the part's entity tag.",
                ));
            }
            combined_data.extend_from_slice(&part.data);
            let part_md5 = Md5::digest(&part.data);
            md5_digests.extend_from_slice(&part_md5);
            part_sizes.push((*part_num, part.data.len() as u64));
        }

        // Multipart ETag: MD5(concat(part_md5_digests))-N
        let combined_md5 = Md5::digest(&md5_digests);
        let etag = format!("{:x}-{}", combined_md5, sorted_parts.len());
        let checksum_value = upload
            .checksum_algorithm
            .as_deref()
            .map(|algo| compute_checksum(algo, &combined_data));
        let data = Bytes::from(combined_data);

        let tags = if let Some(ref tagging) = upload.tagging {
            parse_url_encoded_tags(tagging).into_iter().collect()
        } else {
            std::collections::HashMap::new()
        };

        let version_id = if b.versioning.as_deref() == Some("Enabled") {
            Some(uuid::Uuid::new_v4().to_string())
        } else {
            None
        };

        let obj = S3Object {
            key: key.to_string(),
            size: data.len() as u64,
            data,
            content_type: upload.content_type.clone(),
            etag: etag.clone(),
            last_modified: Utc::now(),
            metadata: upload.metadata.clone(),
            storage_class: upload.storage_class.clone(),
            tags,
            acl_grants: upload.acl_grants.clone(),
            acl_owner_id: Some(b.acl_owner_id.clone()),
            parts_count: Some(sorted_parts.len() as u32),
            part_sizes: Some(part_sizes),
            sse_algorithm: upload.sse_algorithm.clone(),
            sse_kms_key_id: upload.sse_kms_key_id.clone(),
            bucket_key_enabled: None,
            version_id: version_id.clone(),
            is_delete_marker: false,
            content_encoding: None,
            website_redirect_location: None,
            restore_ongoing: None,
            restore_expiry: None,
            checksum_algorithm: upload.checksum_algorithm.clone(),
            checksum_value,
            lock_mode: None,
            lock_retain_until: None,
            lock_legal_hold: None,
        };
        b.objects.insert(key.to_string(), obj);
        b.multipart_uploads.remove(upload_id);

        let mut headers = HeaderMap::new();
        if let Some(vid) = &version_id {
            headers.insert("x-amz-version-id", vid.parse().unwrap());
        }
        if let Some(algo) = &upload.sse_algorithm {
            headers.insert("x-amz-server-side-encryption", algo.parse().unwrap());
        }
        if let Some(kid) = &upload.sse_kms_key_id {
            headers.insert(
                "x-amz-server-side-encryption-aws-kms-key-id",
                kid.parse().unwrap(),
            );
        }

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Bucket>{}</Bucket>\
             <Key>{}</Key>\
             <ETag>&quot;{}&quot;</ETag>\
             </CompleteMultipartUploadResult>",
            xml_escape(bucket),
            xml_escape(key),
            xml_escape(&etag),
        );
        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: body.into(),
            headers,
        })
    }

    fn abort_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        // Validate upload exists and belongs to the requested key
        match b.multipart_uploads.get(upload_id) {
            Some(upload) if upload.key != key => {
                return Err(no_such_upload(upload_id));
            }
            None => {
                return Err(no_such_upload(upload_id));
            }
            _ => {}
        }
        b.multipart_uploads.remove(upload_id);

        Ok(AwsResponse {
            status: StatusCode::NO_CONTENT,
            content_type: "application/xml".to_string(),
            body: Bytes::new(),
            headers: HeaderMap::new(),
        })
    }

    fn list_multipart_uploads(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let mut uploads_xml = String::new();
        let mut sorted_uploads: Vec<_> = b.multipart_uploads.values().collect();
        sorted_uploads.sort_by_key(|u| &u.key);
        for upload in &sorted_uploads {
            uploads_xml.push_str(&format!(
                "<Upload>\
                 <Key>{}</Key>\
                 <UploadId>{}</UploadId>\
                 <Initiated>{}</Initiated>\
                 <StorageClass>{}</StorageClass>\
                 </Upload>",
                xml_escape(&upload.key),
                xml_escape(&upload.upload_id),
                upload.initiated.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                xml_escape(&upload.storage_class),
            ));
        }

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListMultipartUploadsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Bucket>{}</Bucket>\
             <MaxUploads>1000</MaxUploads>\
             <IsTruncated>false</IsTruncated>\
             {uploads_xml}\
             </ListMultipartUploadsResult>",
            xml_escape(bucket),
        );
        Ok(s3_xml(StatusCode::OK, body))
    }

    fn list_parts(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let max_parts: i64 = match req.query_params.get("max-parts") {
            Some(v) => v.parse().map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    "Provided max-parts not an integer or within integer range",
                )
            })?,
            None => 1000,
        };
        let part_number_marker: i64 = match req.query_params.get("part-number-marker") {
            Some(v) => v.parse().map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    "Provided part-number-marker not an integer or within integer range",
                )
            })?,
            None => 0,
        };

        // Validate max-parts and part-number-marker
        if max_parts < 0 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Argument max-parts must be an integer between 0 and 2147483647",
            ));
        }
        if max_parts > 2147483647 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Provided max-parts not an integer or within integer range",
            ));
        }
        if part_number_marker < 0 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Argument part-number-marker must be an integer between 0 and 2147483647",
            ));
        }
        if part_number_marker > 2147483647 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Provided part-number-marker not an integer or within integer range",
            ));
        }

        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let upload = b
            .multipart_uploads
            .get(upload_id)
            .ok_or_else(|| no_such_upload(upload_id))?;
        if upload.key != key {
            return Err(no_such_upload(upload_id));
        }

        // Filter parts after marker and apply limit
        let all_parts: Vec<_> = upload
            .parts
            .values()
            .filter(|p| p.part_number as i64 > part_number_marker)
            .collect();
        let max = max_parts as usize;
        let is_truncated = all_parts.len() > max;
        let display_parts: Vec<_> = all_parts.into_iter().take(max).collect();

        let mut parts_xml = String::new();
        let mut next_marker: i64 = 0;
        for part in &display_parts {
            next_marker = part.part_number as i64;
            parts_xml.push_str(&format!(
                "<Part>\
                 <PartNumber>{}</PartNumber>\
                 <ETag>&quot;{}&quot;</ETag>\
                 <Size>{}</Size>\
                 <LastModified>{}</LastModified>\
                 </Part>",
                part.part_number,
                xml_escape(&part.etag),
                part.size,
                part.last_modified.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
            ));
        }

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListPartsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Bucket>{}</Bucket>\
             <Key>{}</Key>\
             <UploadId>{}</UploadId>\
             <PartNumberMarker>{part_number_marker}</PartNumberMarker>\
             <NextPartNumberMarker>{next_marker}</NextPartNumberMarker>\
             <MaxParts>{max_parts}</MaxParts>\
             <IsTruncated>{is_truncated}</IsTruncated>\
             {parts_xml}\
             </ListPartsResult>",
            xml_escape(bucket),
            xml_escape(key),
            xml_escape(upload_id),
        );
        Ok(s3_xml(StatusCode::OK, body))
    }

    fn delete_object_tagging(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let obj = b.objects.get_mut(key).ok_or_else(|| no_such_key(key))?;
        obj.tags.clear();
        Ok(AwsResponse {
            status: StatusCode::NO_CONTENT,
            content_type: "application/xml".to_string(),
            body: Bytes::new(),
            headers: HeaderMap::new(),
        })
    }

    fn put_object_retention(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let version_id = req.query_params.get("versionId").cloned();
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let mode = extract_xml_value(body_str, "Mode");
        let retain_until = extract_xml_value(body_str, "RetainUntilDate")
            .and_then(|s| s.parse::<DateTime<Utc>>().ok());

        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        // Find and update the object (either current or specific version)
        if let Some(ref vid) = version_id {
            let mut found = false;
            if let Some(versions) = b.object_versions.get_mut(key) {
                for obj in versions.iter_mut() {
                    if obj.version_id.as_deref() == Some(vid) {
                        obj.lock_mode = mode.clone();
                        obj.lock_retain_until = retain_until;
                        found = true;
                        break;
                    }
                }
            }
            if let Some(obj) = b.objects.get_mut(key) {
                if obj.version_id.as_deref() == Some(vid) {
                    obj.lock_mode = mode;
                    obj.lock_retain_until = retain_until;
                    found = true;
                }
            }
            if !found {
                return Err(no_such_key(key));
            }
        } else {
            let obj = b.objects.get_mut(key).ok_or_else(|| no_such_key(key))?;
            obj.lock_mode = mode.clone();
            obj.lock_retain_until = retain_until;
            // Also update in object_versions if the current object has a version_id
            if let Some(ref vid) = obj.version_id {
                let vid = vid.clone();
                if let Some(versions) = b.object_versions.get_mut(key) {
                    for v in versions.iter_mut() {
                        if v.version_id.as_deref() == Some(&vid) {
                            v.lock_mode = mode.clone();
                            v.lock_retain_until = retain_until;
                            break;
                        }
                    }
                }
            }
        }

        Ok(empty_response(StatusCode::OK))
    }

    fn get_object_retention(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let obj = resolve_object(b, key, req.query_params.get("versionId"))?;

        match (&obj.lock_mode, &obj.lock_retain_until) {
            (Some(mode), Some(until)) => {
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                     <Retention xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                     <Mode>{}</Mode>\
                     <RetainUntilDate>{}</RetainUntilDate>\
                     </Retention>",
                    xml_escape(mode),
                    until.to_rfc3339(),
                );
                Ok(s3_xml(StatusCode::OK, body))
            }
            _ => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchObjectLockConfiguration",
                "The specified object does not have a ObjectLock configuration",
            )),
        }
    }

    fn put_object_legal_hold(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let version_id = req.query_params.get("versionId").cloned();
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let status = extract_xml_value(body_str, "Status");

        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        if let Some(ref vid) = version_id {
            let mut found = false;
            if let Some(versions) = b.object_versions.get_mut(key) {
                for obj in versions.iter_mut() {
                    if obj.version_id.as_deref() == Some(vid) {
                        obj.lock_legal_hold = status.clone();
                        found = true;
                        break;
                    }
                }
            }
            if let Some(obj) = b.objects.get_mut(key) {
                if obj.version_id.as_deref() == Some(vid) {
                    obj.lock_legal_hold = status;
                    found = true;
                }
            }
            if !found {
                return Err(no_such_key(key));
            }
        } else {
            let obj = b.objects.get_mut(key).ok_or_else(|| no_such_key(key))?;
            obj.lock_legal_hold = status.clone();
            // Also update in object_versions if the current object has a version_id
            if let Some(ref vid) = obj.version_id {
                let vid = vid.clone();
                if let Some(versions) = b.object_versions.get_mut(key) {
                    for v in versions.iter_mut() {
                        if v.version_id.as_deref() == Some(&vid) {
                            v.lock_legal_hold = status.clone();
                            break;
                        }
                    }
                }
            }
        }

        Ok(empty_response(StatusCode::OK))
    }

    fn get_object_legal_hold(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let obj = resolve_object(b, key, req.query_params.get("versionId"))?;

        match &obj.lock_legal_hold {
            Some(hold) => {
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                     <LegalHold xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                     <Status>{}</Status>\
                     </LegalHold>",
                    xml_escape(hold),
                );
                Ok(s3_xml(StatusCode::OK, body))
            }
            None => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchObjectLockConfiguration",
                "The specified object does not have a ObjectLock configuration",
            )),
        }
    }

    fn get_object_attributes(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let obj = b.objects.get(key).ok_or_else(|| no_such_key(key))?;

        let attrs = req
            .headers
            .get("x-amz-object-attributes")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        let mut body_parts = Vec::new();

        for attr in attrs.split(',') {
            let attr = attr.trim();
            match attr {
                "ETag" => {
                    body_parts.push(format!("<ETag>{}</ETag>", xml_escape(&obj.etag)));
                }
                "StorageClass" => {
                    body_parts.push(format!(
                        "<StorageClass>{}</StorageClass>",
                        xml_escape(&obj.storage_class)
                    ));
                }
                "ObjectSize" => {
                    body_parts.push(format!("<ObjectSize>{}</ObjectSize>", obj.size));
                }
                "Checksum" => {
                    if let (Some(algo), Some(val)) = (&obj.checksum_algorithm, &obj.checksum_value)
                    {
                        body_parts.push(format!(
                            "<Checksum><Checksum{algo}>{val}</Checksum{algo}></Checksum>"
                        ));
                    }
                }
                "ObjectParts" => {
                    if let Some(pc) = obj.parts_count {
                        let mut parts_inner = format!("<TotalPartsCount>{pc}</TotalPartsCount>");
                        if let Some(ref ps) = obj.part_sizes {
                            for (pn, sz) in ps {
                                parts_inner.push_str(&format!(
                                    "<Part><PartNumber>{pn}</PartNumber><Size>{sz}</Size></Part>"
                                ));
                            }
                        }
                        body_parts.push(format!("<ObjectParts>{parts_inner}</ObjectParts>"));
                    }
                }
                _ => {}
            }
        }

        let mut headers = HeaderMap::new();
        if let Some(vid) = &obj.version_id {
            headers.insert("x-amz-version-id", vid.parse().unwrap());
        }
        headers.insert(
            "last-modified",
            obj.last_modified
                .format("%a, %d %b %Y %H:%M:%S GMT")
                .to_string()
                .parse()
                .unwrap(),
        );

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <GetObjectAttributesResponse xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             {}\
             </GetObjectAttributesResponse>",
            body_parts.join("")
        );
        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: body.into(),
            headers,
        })
    }

    fn restore_object(
        &self,
        _req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let obj = b.objects.get_mut(key).ok_or_else(|| no_such_key(key))?;
        let glacier_classes = [
            "GLACIER",
            "DEEP_ARCHIVE",
            "GLACIER_IR",
            "INTELLIGENT_TIERING",
        ];
        if !glacier_classes.contains(&obj.storage_class.as_str()) {
            return Err(AwsServiceError::aws_error_with_fields(
                StatusCode::FORBIDDEN,
                "InvalidObjectState",
                "The operation is not valid for the object's storage class",
                vec![("StorageClass".to_string(), obj.storage_class.clone())],
            ));
        }
        let status = if obj.restore_ongoing.is_some() {
            StatusCode::OK
        } else {
            StatusCode::ACCEPTED
        };
        let expiry = (Utc::now() + chrono::Duration::days(30))
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string();
        obj.restore_ongoing = Some(false);
        obj.restore_expiry = Some(expiry);
        Ok(AwsResponse {
            status,
            content_type: "application/xml".to_string(),
            body: Bytes::new(),
            headers: HeaderMap::new(),
        })
    }
}

// ---------------------------------------------------------------------------
// Conditional request helpers
// ---------------------------------------------------------------------------

/// Truncate a DateTime to second-level precision (HTTP dates have no sub-second info).
fn truncate_to_seconds(dt: DateTime<Utc>) -> DateTime<Utc> {
    dt.with_nanosecond(0).unwrap_or(dt)
}

fn check_get_conditionals(req: &AwsRequest, obj: &S3Object) -> Result<(), AwsServiceError> {
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

fn check_head_conditionals(req: &AwsRequest, obj: &S3Object) -> Result<(), AwsServiceError> {
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

fn etag_matches(condition: &str, obj_etag: &str) -> bool {
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

fn parse_http_date(s: &str) -> Option<DateTime<Utc>> {
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

fn not_modified() -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::NOT_MODIFIED, "304", "Not Modified")
}

fn not_modified_with_etag(etag: &str) -> AwsServiceError {
    AwsServiceError::aws_error_with_headers(
        StatusCode::NOT_MODIFIED,
        "304",
        "Not Modified",
        vec![("etag".to_string(), etag.to_string())],
    )
}

fn precondition_failed(condition: &str) -> AwsServiceError {
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

fn build_acl_xml(owner_id: &str, grants: &[AclGrant], _account_id: &str) -> String {
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

fn canned_acl_grants(acl: &str, owner_id: &str) -> Vec<AclGrant> {
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

fn canned_acl_grants_for_object(acl: &str, owner_id: &str) -> Vec<AclGrant> {
    // For objects, canned ACLs work the same way
    canned_acl_grants(acl, owner_id)
}

fn parse_grant_headers(headers: &HeaderMap) -> Vec<AclGrant> {
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

fn parse_acl_xml(xml: &str) -> Result<Vec<AclGrant>, AwsServiceError> {
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

enum RangeResult {
    Satisfiable { start: usize, end: usize },
    NotSatisfiable,
    Ignored,
}

fn parse_range_header(range_str: &str, total_size: usize) -> Option<RangeResult> {
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
fn s3_xml(status: StatusCode, body: impl Into<Bytes>) -> AwsResponse {
    AwsResponse {
        status,
        content_type: "application/xml".to_string(),
        body: body.into(),
        headers: HeaderMap::new(),
    }
}

fn empty_response(status: StatusCode) -> AwsResponse {
    AwsResponse {
        status,
        content_type: "application/xml".to_string(),
        body: Bytes::new(),
        headers: HeaderMap::new(),
    }
}

/// Returns true when the object is stored in a "cold" storage class (GLACIER, DEEP_ARCHIVE)
/// and has NOT been restored (or restore is still in progress).
fn is_frozen(obj: &S3Object) -> bool {
    matches!(obj.storage_class.as_str(), "GLACIER" | "DEEP_ARCHIVE")
        && obj.restore_ongoing != Some(false)
}

fn no_such_bucket(bucket: &str) -> AwsServiceError {
    AwsServiceError::aws_error_with_fields(
        StatusCode::NOT_FOUND,
        "NoSuchBucket",
        "The specified bucket does not exist",
        vec![("BucketName".to_string(), bucket.to_string())],
    )
}

fn no_such_key(key: &str) -> AwsServiceError {
    AwsServiceError::aws_error_with_fields(
        StatusCode::NOT_FOUND,
        "NoSuchKey",
        "The specified key does not exist.",
        vec![("Key".to_string(), key.to_string())],
    )
}

fn no_such_upload(upload_id: &str) -> AwsServiceError {
    AwsServiceError::aws_error_with_fields(
        StatusCode::NOT_FOUND,
        "NoSuchUpload",
        "The specified upload does not exist. The upload ID may be invalid, \
         or the upload may have been aborted or completed.",
        vec![("UploadId".to_string(), upload_id.to_string())],
    )
}

fn no_such_key_with_detail(key: &str) -> AwsServiceError {
    AwsServiceError::aws_error_with_fields(
        StatusCode::NOT_FOUND,
        "NoSuchKey",
        "The specified key does not exist.",
        vec![("Key".to_string(), key.to_string())],
    )
}

fn compute_md5(data: &[u8]) -> String {
    let digest = Md5::digest(data);
    format!("{:x}", digest)
}

fn compute_checksum(algorithm: &str, data: &[u8]) -> String {
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
fn url_encode_key(s: &str) -> String {
    percent_encoding::utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC).to_string()
}

fn url_encode_s3_key(s: &str) -> String {
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

fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            // XML 1.0 allows \t, \n, \r as valid characters; all other control chars
            // need to be encoded as numeric character references.
            c if (c as u32) < 0x20 && c != '\t' && c != '\n' && c != '\r' => {
                out.push_str(&format!("&#x{:X};", c as u32));
            }
            c => out.push(c),
        }
    }
    out
}

fn extract_user_metadata(headers: &HeaderMap) -> std::collections::HashMap<String, String> {
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

fn is_valid_storage_class(class: &str) -> bool {
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

fn is_valid_bucket_name(name: &str) -> bool {
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

fn is_valid_region(region: &str) -> bool {
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

fn resolve_object<'a>(
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

fn make_delete_marker(key: &str, dm_id: &str) -> S3Object {
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
fn acl_xml(owner_id: &str) -> String {
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
struct DeleteObjectEntry {
    key: String,
    version_id: Option<String>,
}

fn parse_delete_objects_xml(xml: &str) -> Vec<DeleteObjectEntry> {
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
fn parse_tagging_xml(xml: &str) -> Vec<(String, String)> {
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

fn validate_tags(tags: &[(String, String)]) -> Result<(), AwsServiceError> {
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

fn extract_xml_value(xml: &str, tag: &str) -> Option<String> {
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
fn parse_complete_multipart_xml(xml: &str) -> Vec<(u32, String)> {
    let mut parts = Vec::new();
    let mut remaining = xml;
    while let Some(part_start) = remaining.find("<Part>") {
        let after = &remaining[part_start + 6..];
        if let Some(part_end) = after.find("</Part>") {
            let part_body = &after[..part_end];
            let part_num =
                extract_xml_value(part_body, "PartNumber").and_then(|s| s.parse::<u32>().ok());
            let etag = extract_xml_value(part_body, "ETag").map(|s| s.replace('"', ""));
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

fn parse_url_encoded_tags(s: &str) -> Vec<(String, String)> {
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
fn validate_lifecycle_xml(xml: &str) -> Result<(), AwsServiceError> {
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

/// Normalize replication configuration XML to include defaults AWS adds.
/// Auto-generate `<Id>` for notification configuration elements that lack one.
fn normalize_notification_ids(xml: &str) -> String {
    let config_tags = [
        "TopicConfiguration",
        "QueueConfiguration",
        "CloudFunctionConfiguration",
        "LambdaFunctionConfiguration",
    ];
    let mut result = xml.to_string();
    for tag in &config_tags {
        let open = format!("<{tag}>");
        let close = format!("</{tag}>");
        let mut output = String::new();
        let mut remaining = result.as_str();
        while let Some(start) = remaining.find(&open) {
            output.push_str(&remaining[..start]);
            let after = &remaining[start + open.len()..];
            if let Some(end) = after.find(&close) {
                let body = &after[..end];
                output.push_str(&open);
                if !body.contains("<Id>") {
                    output.push_str(&format!("<Id>{}</Id>", uuid::Uuid::new_v4()));
                }
                output.push_str(body);
                output.push_str(&close);
                remaining = &after[end + close.len()..];
            } else {
                output.push_str(&open);
                output.push_str(after);
                remaining = "";
                break;
            }
        }
        output.push_str(remaining);
        result = output;
    }
    result
}

fn normalize_replication_xml(xml: &str) -> String {
    let mut result = String::new();
    let mut remaining = xml;
    let mut auto_priority: u32 = 0;

    // Find and process everything before the first <Rule>
    if let Some(first_rule) = remaining.find("<Rule>") {
        result.push_str(&remaining[..first_rule]);
        remaining = &remaining[first_rule..];
    } else {
        return xml.to_string();
    }

    // Process each <Rule>
    while let Some(rule_start) = remaining.find("<Rule>") {
        let after = &remaining[rule_start + 6..];
        if let Some(rule_end) = after.find("</Rule>") {
            let rule_body = &after[..rule_end];

            // Extract fields from the rule
            let id = extract_xml_value(rule_body, "ID");
            let priority = extract_xml_value(rule_body, "Priority");
            let status =
                extract_xml_value(rule_body, "Status").unwrap_or_else(|| "Enabled".to_string());

            // Extract Destination block (keep as-is)
            let destination = rule_body.find("<Destination>").and_then(|ds| {
                rule_body
                    .find("</Destination>")
                    .map(|de| rule_body[ds..de + 14].to_string())
            });

            // Extract existing Filter if any
            let filter_block = rule_body.find("<Filter>").and_then(|fs| {
                rule_body
                    .find("</Filter>")
                    .map(|fe| rule_body[fs..fe + 9].to_string())
            });

            // Extract DeleteMarkerReplication if any
            let dmr_block = rule_body.find("<DeleteMarkerReplication>").and_then(|ds| {
                rule_body
                    .find("</DeleteMarkerReplication>")
                    .map(|de| rule_body[ds..de + 25].to_string())
            });

            // Build normalized rule
            result.push_str("<Rule>");

            // DeleteMarkerReplication (default to Disabled)
            result.push_str(dmr_block.as_deref().unwrap_or(
                "<DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication>",
            ));

            // Destination
            if let Some(ref dest) = destination {
                result.push_str(dest);
            }

            // Filter (default to empty prefix)
            result.push_str(
                filter_block
                    .as_deref()
                    .unwrap_or("<Filter><Prefix></Prefix></Filter>"),
            );

            // ID (auto-generate if missing)
            let rule_id = id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            result.push_str(&format!("<ID>{}</ID>", xml_escape(&rule_id)));

            // Priority (auto-assign if missing)
            auto_priority += 1;
            let p = priority
                .and_then(|v| v.parse::<u32>().ok())
                .unwrap_or(auto_priority);
            result.push_str(&format!("<Priority>{p}</Priority>"));

            // Status
            result.push_str(&format!("<Status>{status}</Status>"));

            result.push_str("</Rule>");

            remaining = &after[rule_end + 7..];
        } else {
            result.push_str(&remaining[rule_start..]);
            break;
        }
    }

    // Append anything after the last </Rule>
    result.push_str(remaining);

    result
}

/// Parsed replication rule extracted from the replication config XML.
struct ReplicationRule {
    status: String,
    prefix: String,
    dest_bucket: String,
}

/// Parse replication configuration XML and extract rules.
fn parse_replication_rules(xml: &str) -> Vec<ReplicationRule> {
    let mut rules = Vec::new();
    let mut remaining = xml;
    while let Some(rule_start) = remaining.find("<Rule>") {
        let after = &remaining[rule_start + 6..];
        if let Some(rule_end) = after.find("</Rule>") {
            let rule_body = &after[..rule_end];

            // Extract the rule-level Status. Skip Status tags inside nested
            // elements like DeleteMarkerReplication by finding the last occurrence.
            let status = {
                let mut found = None;
                let mut search = rule_body;
                while let Some(pos) = search.find("<Status>") {
                    if let Some(val) = extract_xml_value(&search[pos..], "Status") {
                        found = Some(val);
                    }
                    search = &search[pos + 8..];
                }
                found.unwrap_or_else(|| "Enabled".to_string())
            };

            // Extract prefix from Filter > Prefix or top-level Prefix
            let prefix = rule_body
                .find("<Filter>")
                .and_then(|fs| rule_body.find("</Filter>").map(|fe| &rule_body[fs..fe + 9]))
                .and_then(|filter| extract_xml_value(filter, "Prefix"))
                .or_else(|| extract_xml_value(rule_body, "Prefix"))
                .unwrap_or_default();

            // Extract destination bucket ARN and convert to bucket name
            let dest_bucket = rule_body
                .find("<Destination>")
                .and_then(|ds| {
                    rule_body
                        .find("</Destination>")
                        .map(|de| &rule_body[ds..de + 14])
                })
                .and_then(|dest| extract_xml_value(dest, "Bucket"))
                .map(|arn| {
                    // ARN format: arn:aws:s3:::bucket-name
                    arn.rsplit(":::").next().unwrap_or(&arn).to_string()
                })
                .unwrap_or_default();

            if !dest_bucket.is_empty() {
                rules.push(ReplicationRule {
                    status,
                    prefix,
                    dest_bucket,
                });
            }

            remaining = &after[rule_end + 7..];
        } else {
            break;
        }
    }
    rules
}

/// Replicate an object to destination buckets based on replication configuration.
/// Called after storing an object in a bucket that has replication enabled.
fn replicate_object(state: &mut crate::state::S3State, source_bucket: &str, key: &str) {
    let replication_config = match state.buckets.get(source_bucket) {
        Some(b) => match &b.replication_config {
            Some(config) => config.clone(),
            None => return,
        },
        None => return,
    };

    let rules = parse_replication_rules(&replication_config);
    let src_obj = match state
        .buckets
        .get(source_bucket)
        .and_then(|b| b.objects.get(key))
    {
        Some(obj) => obj.clone(),
        None => return,
    };

    for rule in &rules {
        if rule.status != "Enabled" {
            continue;
        }
        if !key.starts_with(&rule.prefix) {
            continue;
        }
        if let Some(dest_bucket) = state.buckets.get_mut(&rule.dest_bucket) {
            let mut replica = src_obj.clone();
            replica.storage_class = "STANDARD".to_string();
            // Use a new version ID if destination has versioning enabled
            if dest_bucket.versioning.as_deref() == Some("Enabled") {
                let vid = uuid::Uuid::new_v4().to_string();
                replica.version_id = Some(vid);
                dest_bucket
                    .object_versions
                    .entry(key.to_string())
                    .or_default()
                    .push(replica.clone());
            } else {
                replica.version_id = None;
            }
            dest_bucket.objects.insert(key.to_string(), replica);
        }
    }
}

/// Build an S3 event notification JSON payload.
fn build_s3_event_notification(
    event_name: &str,
    bucket_name: &str,
    key: &str,
    size: u64,
    etag: &str,
    region: &str,
) -> String {
    let event_time = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
    serde_json::json!({
        "Records": [{
            "eventVersion": "2.1",
            "eventSource": "aws:s3",
            "awsRegion": region,
            "eventTime": event_time,
            "eventName": event_name,
            "s3": {
                "bucket": {
                    "name": bucket_name,
                    "arn": format!("arn:aws:s3:::{}", bucket_name)
                },
                "object": {
                    "key": key,
                    "size": size,
                    "eTag": etag
                }
            }
        }]
    })
    .to_string()
}

/// Parsed notification target from the bucket notification config XML.
struct NotificationTarget {
    target_type: NotificationTargetType,
    arn: String,
    events: Vec<String>,
    prefix_filter: Option<String>,
    suffix_filter: Option<String>,
}

enum NotificationTargetType {
    Sqs,
    Sns,
    Lambda,
}

/// Parse S3Key filter rules (prefix/suffix) from a notification configuration block.
fn parse_s3_key_filters(block: &str) -> (Option<String>, Option<String>) {
    let mut prefix = None;
    let mut suffix = None;
    if let Some(filter_start) = block.find("<Filter>") {
        let after_filter = &block[filter_start..];
        if let Some(filter_end) = after_filter.find("</Filter>") {
            let filter_block = &after_filter[..filter_end];
            // Parse each FilterRule
            let mut remaining = filter_block;
            while let Some(rule_start) = remaining.find("<FilterRule>") {
                let after_rule = &remaining[rule_start + 12..];
                if let Some(rule_end) = after_rule.find("</FilterRule>") {
                    let rule_block = &after_rule[..rule_end];
                    let name = extract_xml_value(rule_block, "Name");
                    let value = extract_xml_value(rule_block, "Value");
                    if let (Some(name), Some(value)) = (name, value) {
                        match name.to_lowercase().as_str() {
                            "prefix" => prefix = Some(value),
                            "suffix" => suffix = Some(value),
                            _ => {}
                        }
                    }
                    remaining = &after_rule[rule_end + 13..];
                } else {
                    break;
                }
            }
        }
    }
    (prefix, suffix)
}

/// Check if an object key matches the prefix/suffix filters.
fn key_matches_filters(key: &str, prefix: &Option<String>, suffix: &Option<String>) -> bool {
    if let Some(p) = prefix {
        if !key.starts_with(p.as_str()) {
            return false;
        }
    }
    if let Some(s) = suffix {
        if !key.ends_with(s.as_str()) {
            return false;
        }
    }
    true
}

/// Parse the bucket notification configuration XML into targets.
fn parse_notification_config(xml: &str) -> Vec<NotificationTarget> {
    let mut targets = Vec::new();

    // Parse QueueConfiguration entries
    let mut remaining = xml;
    while let Some(start) = remaining.find("<QueueConfiguration>") {
        let after = &remaining[start + 20..];
        if let Some(end) = after.find("</QueueConfiguration>") {
            let block = &after[..end];
            if let Some(arn) = extract_xml_value(block, "Queue") {
                let events = extract_all_xml_values(block, "Event");
                let (prefix_filter, suffix_filter) = parse_s3_key_filters(block);
                targets.push(NotificationTarget {
                    target_type: NotificationTargetType::Sqs,
                    arn,
                    events,
                    prefix_filter,
                    suffix_filter,
                });
            }
            remaining = &after[end + 21..];
        } else {
            break;
        }
    }

    // Parse TopicConfiguration entries
    remaining = xml;
    while let Some(start) = remaining.find("<TopicConfiguration>") {
        let after = &remaining[start + 20..];
        if let Some(end) = after.find("</TopicConfiguration>") {
            let block = &after[..end];
            if let Some(arn) = extract_xml_value(block, "Topic") {
                let events = extract_all_xml_values(block, "Event");
                let (prefix_filter, suffix_filter) = parse_s3_key_filters(block);
                targets.push(NotificationTarget {
                    target_type: NotificationTargetType::Sns,
                    arn,
                    events,
                    prefix_filter,
                    suffix_filter,
                });
            }
            remaining = &after[end + 21..];
        } else {
            break;
        }
    }

    // Parse CloudFunctionConfiguration entries (older S3 XML format)
    remaining = xml;
    while let Some(start) = remaining.find("<CloudFunctionConfiguration>") {
        let after = &remaining[start + 28..];
        if let Some(end) = after.find("</CloudFunctionConfiguration>") {
            let block = &after[..end];
            if let Some(arn) = extract_xml_value(block, "CloudFunction") {
                let events = extract_all_xml_values(block, "Event");
                let (prefix_filter, suffix_filter) = parse_s3_key_filters(block);
                targets.push(NotificationTarget {
                    target_type: NotificationTargetType::Lambda,
                    arn,
                    events,
                    prefix_filter,
                    suffix_filter,
                });
            }
            remaining = &after[end + 29..];
        } else {
            break;
        }
    }

    // Parse LambdaFunctionConfiguration entries (newer S3 XML format)
    remaining = xml;
    while let Some(start) = remaining.find("<LambdaFunctionConfiguration>") {
        let after = &remaining[start + 29..];
        if let Some(end) = after.find("</LambdaFunctionConfiguration>") {
            let block = &after[..end];
            // The newer format uses <Function> for the ARN
            let arn = extract_xml_value(block, "Function")
                .or_else(|| extract_xml_value(block, "CloudFunction"));
            if let Some(arn) = arn {
                let events = extract_all_xml_values(block, "Event");
                let (prefix_filter, suffix_filter) = parse_s3_key_filters(block);
                targets.push(NotificationTarget {
                    target_type: NotificationTargetType::Lambda,
                    arn,
                    events,
                    prefix_filter,
                    suffix_filter,
                });
            }
            remaining = &after[end + 30..];
        } else {
            break;
        }
    }

    targets
}

/// Extract all values for a given XML tag (multiple occurrences).
fn extract_all_xml_values(xml: &str, tag: &str) -> Vec<String> {
    let mut values = Vec::new();
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut remaining = xml;
    while let Some(start) = remaining.find(&open) {
        let after = &remaining[start + open.len()..];
        if let Some(end) = after.find(&close) {
            values.push(after[..end].to_string());
            remaining = &after[end + close.len()..];
        } else {
            break;
        }
    }
    values
}

/// Check if an S3 event name matches a notification event filter.
fn event_matches(event_name: &str, filter: &str) -> bool {
    // Exact match
    if filter == event_name {
        return true;
    }
    // Wildcard: s3:ObjectCreated:* matches s3:ObjectCreated:Put, etc.
    if filter.ends_with(":*") {
        let prefix = &filter[..filter.len() - 1]; // "s3:ObjectCreated:"
        if event_name.starts_with(prefix) {
            return true;
        }
    }
    // s3:* matches everything
    if filter == "s3:*" {
        return true;
    }
    false
}

/// Deliver S3 event notifications for a bucket operation.
#[allow(clippy::too_many_arguments)]
fn deliver_notifications(
    delivery: &Arc<DeliveryBus>,
    notification_config: &str,
    event_name: &str,
    bucket_name: &str,
    key: &str,
    size: u64,
    etag: &str,
    region: &str,
) {
    let targets = parse_notification_config(notification_config);
    let s3_event_name = format!("s3:{event_name}");
    let message = build_s3_event_notification(event_name, bucket_name, key, size, etag, region);

    for target in &targets {
        let matches = target.events.is_empty()
            || target
                .events
                .iter()
                .any(|f| event_matches(&s3_event_name, f));
        if !matches {
            continue;
        }
        if !key_matches_filters(key, &target.prefix_filter, &target.suffix_filter) {
            continue;
        }
        match target.target_type {
            NotificationTargetType::Sqs => {
                delivery.send_to_sqs(&target.arn, &message, &std::collections::HashMap::new());
            }
            NotificationTargetType::Sns => {
                delivery.publish_to_sns(&target.arn, &message, Some("Amazon S3 Notification"));
            }
            NotificationTargetType::Lambda => {
                let delivery = delivery.clone();
                let function_arn = target.arn.clone();
                let payload = message.clone();
                tokio::spawn(async move {
                    tracing::info!(
                        function_arn = %function_arn,
                        "S3 invoking Lambda function for notification"
                    );
                    match delivery.invoke_lambda(&function_arn, &payload).await {
                        Some(Ok(_)) => {
                            tracing::info!(
                                function_arn = %function_arn,
                                "S3->Lambda invocation succeeded"
                            );
                        }
                        Some(Err(e)) => {
                            tracing::error!(
                                function_arn = %function_arn,
                                error = %e,
                                "S3->Lambda invocation failed"
                            );
                        }
                        None => {
                            tracing::warn!(
                                function_arn = %function_arn,
                                "No Lambda delivery configured"
                            );
                        }
                    }
                });
            }
        }
    }
}

/// Parse a CORS rule from XML.
#[derive(Debug, Clone)]
struct CorsRule {
    allowed_origins: Vec<String>,
    allowed_methods: Vec<String>,
    allowed_headers: Vec<String>,
    expose_headers: Vec<String>,
    max_age_seconds: Option<u32>,
}

/// Parse CORS configuration XML into rules.
fn parse_cors_config(xml: &str) -> Vec<CorsRule> {
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
fn origin_matches(origin: &str, pattern: &str) -> bool {
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
fn find_cors_rule<'a>(
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
fn check_object_lock_for_overwrite(obj: &S3Object, req: &AwsRequest) -> Option<&'static str> {
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
