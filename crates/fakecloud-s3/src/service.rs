use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use http::{HeaderMap, Method, StatusCode};
use md5::{Digest, Md5};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::{MultipartUpload, S3Bucket, S3Object, SharedS3State};

pub struct S3Service {
    state: SharedS3State,
}

impl S3Service {
    pub fn new(state: SharedS3State) -> Self {
        Self { state }
    }
}

#[async_trait]
impl AwsService for S3Service {
    fn service_name(&self) -> &str {
        "s3"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let bucket = req.path_segments.first().map(|s| s.as_str());
        let key = if req.path_segments.len() > 1 {
            Some(req.path_segments[1..].join("/"))
        } else {
            None
        };

        match (&req.method, bucket, key.as_deref()) {
            (&Method::GET, None, None) => self.list_buckets(&req),

            (&Method::PUT, Some(b), None) => self.route_put_bucket(&req, b),
            (&Method::DELETE, Some(b), None) => self.route_delete_bucket(&req, b),
            (&Method::HEAD, Some(b), None) => self.head_bucket(b),
            (&Method::GET, Some(b), None) => self.route_get_bucket(&req, b),

            (&Method::PUT, Some(b), Some(k)) => {
                if req.headers.contains_key("x-amz-copy-source") {
                    self.copy_object(&req, b, k)
                } else {
                    self.put_object(&req, b, k)
                }
            }
            (&Method::GET, Some(b), Some(k)) => self.get_object(b, k),
            (&Method::DELETE, Some(b), Some(k)) => self.delete_object(b, k),
            (&Method::HEAD, Some(b), Some(k)) => self.head_object(b, k),

            (&Method::POST, Some(b), None) if req.query_params.contains_key("delete") => {
                self.delete_objects(&req, b)
            }
            (&Method::POST, Some(b), Some(k)) if req.query_params.contains_key("uploads") => {
                self.create_multipart_upload(&req, b, k)
            }

            _ => Err(AwsServiceError::aws_error(
                StatusCode::METHOD_NOT_ALLOWED,
                "MethodNotAllowed",
                "The specified method is not allowed against this resource",
            )),
        }
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            "ListBuckets",
            "CreateBucket",
            "DeleteBucket",
            "HeadBucket",
            "ListObjectsV2",
            "ListObjects",
            "PutObject",
            "GetObject",
            "DeleteObject",
            "HeadObject",
            "CopyObject",
            "DeleteObjects",
            "GetBucketLocation",
            "GetBucketTagging",
            "PutBucketTagging",
            "DeleteBucketTagging",
            "GetBucketVersioning",
            "PutBucketVersioning",
            "GetBucketEncryption",
            "PutBucketEncryption",
            "DeleteBucketEncryption",
            "GetBucketLifecycleConfiguration",
            "PutBucketLifecycleConfiguration",
            "DeleteBucketLifecycle",
            "GetBucketPolicy",
            "PutBucketPolicy",
            "DeleteBucketPolicy",
            "GetBucketCors",
            "PutBucketCors",
            "DeleteBucketCors",
            "GetBucketAcl",
            "PutBucketAcl",
            "GetBucketNotificationConfiguration",
            "PutBucketNotificationConfiguration",
            "GetBucketLogging",
            "PutBucketLogging",
            "GetBucketWebsite",
            "PutBucketWebsite",
            "DeleteBucketWebsite",
            "GetBucketAccelerateConfiguration",
            "PutBucketAccelerateConfiguration",
            "GetPublicAccessBlock",
            "PutPublicAccessBlock",
            "DeletePublicAccessBlock",
            "GetObjectLockConfiguration",
            "PutObjectLockConfiguration",
            "ListObjectVersions",
            "CreateMultipartUpload",
        ]
    }
}

// ---------------------------------------------------------------------------
// Routing helpers
// ---------------------------------------------------------------------------
impl S3Service {
    fn route_put_bucket(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        if req.query_params.contains_key("tagging") {
            self.put_bucket_tagging(req, bucket)
        } else if req.query_params.contains_key("versioning") {
            self.put_bucket_versioning(req, bucket)
        } else if req.query_params.contains_key("encryption") {
            self.put_bucket_encryption(req, bucket)
        } else if req.query_params.contains_key("lifecycle") {
            self.put_bucket_lifecycle(req, bucket)
        } else if req.query_params.contains_key("policy") {
            self.put_bucket_policy(req, bucket)
        } else if req.query_params.contains_key("cors") {
            self.put_bucket_cors(req, bucket)
        } else if req.query_params.contains_key("acl") {
            self.put_bucket_acl(req, bucket)
        } else if req.query_params.contains_key("notification") {
            self.put_bucket_notification(req, bucket)
        } else if req.query_params.contains_key("logging") {
            self.put_bucket_logging(req, bucket)
        } else if req.query_params.contains_key("website") {
            self.put_bucket_website(req, bucket)
        } else if req.query_params.contains_key("accelerate") {
            self.put_bucket_accelerate(req, bucket)
        } else if req.query_params.contains_key("publicAccessBlock") {
            self.put_public_access_block(req, bucket)
        } else if req.query_params.contains_key("object-lock") {
            self.put_object_lock_config(req, bucket)
        } else {
            self.create_bucket(req, bucket)
        }
    }

    fn route_get_bucket(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        if req.query_params.contains_key("tagging") {
            self.get_bucket_tagging(req, bucket)
        } else if req.query_params.contains_key("location") {
            self.get_bucket_location(bucket)
        } else if req.query_params.contains_key("versioning") {
            self.get_bucket_versioning(bucket)
        } else if req.query_params.contains_key("encryption") {
            self.get_bucket_encryption(bucket)
        } else if req.query_params.contains_key("lifecycle") {
            self.get_bucket_lifecycle(bucket)
        } else if req.query_params.contains_key("policy") {
            self.get_bucket_policy(bucket)
        } else if req.query_params.contains_key("cors") {
            self.get_bucket_cors(bucket)
        } else if req.query_params.contains_key("acl") {
            self.get_bucket_acl(req, bucket)
        } else if req.query_params.contains_key("notification") {
            self.get_bucket_notification(bucket)
        } else if req.query_params.contains_key("logging") {
            self.get_bucket_logging(bucket)
        } else if req.query_params.contains_key("website") {
            self.get_bucket_website(bucket)
        } else if req.query_params.contains_key("accelerate") {
            self.get_bucket_accelerate(bucket)
        } else if req.query_params.contains_key("publicAccessBlock") {
            self.get_public_access_block(bucket)
        } else if req.query_params.contains_key("object-lock") {
            self.get_object_lock_config(bucket)
        } else if req.query_params.contains_key("versions") {
            self.list_object_versions(req, bucket)
        } else if req.query_params.get("list-type").map(|s| s.as_str()) == Some("2") {
            self.list_objects_v2(req, bucket)
        } else {
            self.list_objects(req, bucket)
        }
    }

    fn route_delete_bucket(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        if req.query_params.contains_key("tagging") {
            self.delete_bucket_tagging(req, bucket)
        } else if req.query_params.contains_key("encryption") {
            self.delete_bucket_encryption(bucket)
        } else if req.query_params.contains_key("lifecycle") {
            self.delete_bucket_lifecycle(bucket)
        } else if req.query_params.contains_key("policy") {
            self.delete_bucket_policy(bucket)
        } else if req.query_params.contains_key("cors") {
            self.delete_bucket_cors(bucket)
        } else if req.query_params.contains_key("website") {
            self.delete_bucket_website(bucket)
        } else if req.query_params.contains_key("publicAccessBlock") {
            self.delete_public_access_block(bucket)
        } else {
            self.delete_bucket(req, bucket)
        }
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
        Ok(AwsResponse::xml(StatusCode::OK, body))
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

        let mut state = self.state.write();
        if state.buckets.contains_key(bucket) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "BucketAlreadyOwnedByYou",
                "Your previous request to create the named bucket succeeded and you already own it.",
            ));
        }
        state
            .buckets
            .insert(bucket.to_string(), S3Bucket::new(bucket, &req.region));

        let mut headers = HeaderMap::new();
        headers.insert("location", format!("/{bucket}").parse().unwrap());
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
        if !b.objects.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "BucketNotEmpty",
                "The bucket you tried to delete is not empty",
            ));
        }
        state.buckets.remove(bucket);
        Ok(empty_response(StatusCode::NO_CONTENT))
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
        Ok(empty_response(StatusCode::OK))
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
        Ok(AwsResponse::xml(StatusCode::OK, body))
    }

    // ---- Versioning ----

    fn put_bucket_versioning(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let status = extract_xml_value(body_str, "Status");
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.versioning_status = status;
        Ok(empty_response(StatusCode::OK))
    }

    fn get_bucket_versioning(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let status_xml = match &b.versioning_status {
            Some(s) => format!("<Status>{s}</Status>"),
            None => String::new(),
        };
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             {status_xml}\
             </VersioningConfiguration>"
        );
        Ok(AwsResponse::xml(StatusCode::OK, body))
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
        b.encryption_config = Some(body_str);
        Ok(empty_response(StatusCode::OK))
    }

    fn get_bucket_encryption(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        match &b.encryption_config {
            Some(config) => Ok(AwsResponse::xml(StatusCode::OK, config.clone())),
            None => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ServerSideEncryptionConfigurationNotFoundError",
                "The server side encryption configuration was not found",
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
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.lifecycle_config = Some(body_str);
        Ok(empty_response(StatusCode::OK))
    }

    fn get_bucket_lifecycle(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        match &b.lifecycle_config {
            Some(config) => Ok(AwsResponse::xml(StatusCode::OK, config.clone())),
            None => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchLifecycleConfiguration",
                "The lifecycle configuration does not exist",
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
            None => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchBucketPolicy",
                "The bucket policy does not exist",
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
            Some(config) => Ok(AwsResponse::xml(StatusCode::OK, config.clone())),
            None => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchCORSConfiguration",
                "The CORS configuration does not exist",
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

    // ---- ACL ----

    fn put_bucket_acl(
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
        if body_str.is_empty() {
            let canned = req
                .headers
                .get("x-amz-acl")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("private")
                .to_string();
            b.acl = Some(canned);
        } else {
            b.acl = Some(body_str);
        }
        Ok(empty_response(StatusCode::OK))
    }

    fn get_bucket_acl(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let _b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let owner_id = &req.account_id;
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Owner><ID>{owner_id}</ID><DisplayName>{owner_id}</DisplayName></Owner>\
             <AccessControlList>\
             <Grant>\
             <Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">\
             <ID>{owner_id}</ID><DisplayName>{owner_id}</DisplayName>\
             </Grantee>\
             <Permission>FULL_CONTROL</Permission>\
             </Grant>\
             </AccessControlList>\
             </AccessControlPolicy>"
        );
        Ok(AwsResponse::xml(StatusCode::OK, body))
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
        b.notification_config = Some(body_str);
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
        Ok(AwsResponse::xml(StatusCode::OK, body))
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
        Ok(AwsResponse::xml(StatusCode::OK, body))
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
            Some(config) => Ok(AwsResponse::xml(StatusCode::OK, config.clone())),
            None => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchWebsiteConfiguration",
                "The specified bucket does not have a website configuration",
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
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let status = extract_xml_value(body_str, "Status");
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
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
        Ok(AwsResponse::xml(StatusCode::OK, body))
    }

    // ---- PublicAccessBlock ----

    fn put_public_access_block(
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
            Some(config) => Ok(AwsResponse::xml(StatusCode::OK, config.clone())),
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
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.object_lock_config = Some(body_str);
        Ok(empty_response(StatusCode::OK))
    }

    fn get_object_lock_config(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        match &b.object_lock_config {
            Some(config) => Ok(AwsResponse::xml(StatusCode::OK, config.clone())),
            None => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ObjectLockConfigurationNotFoundError",
                "Object Lock configuration does not exist for this bucket",
            )),
        }
    }

    // ---- Tagging ----

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
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchTagSet",
                "The TagSet does not exist",
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
        Ok(AwsResponse::xml(StatusCode::OK, body))
    }

    fn put_bucket_tagging(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let tags = parse_tagging_xml(body_str);
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.tags = tags;
        Ok(empty_response(StatusCode::NO_CONTENT))
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
        Ok(empty_response(StatusCode::NO_CONTENT))
    }

    // ---- List operations ----

    fn list_objects(&self, req: &AwsRequest, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
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
        let marker = req.query_params.get("marker").cloned().unwrap_or_default();

        let mut contents = String::new();
        let mut common_prefixes: Vec<String> = Vec::new();
        let mut count = 0;
        let mut is_truncated = false;
        let mut last_key = String::new();

        for (key, obj) in &b.objects {
            if !key.starts_with(&prefix) {
                continue;
            }
            if !marker.is_empty() && key.as_str() <= marker.as_str() {
                continue;
            }

            if !delimiter.is_empty() {
                let suffix = &key[prefix.len()..];
                if let Some(pos) = suffix.find(&delimiter) {
                    let cp = format!("{}{}", prefix, &suffix[..=pos]);
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

            contents.push_str(&format!(
                "<Contents>\
                 <Key>{}</Key>\
                 <LastModified>{}</LastModified>\
                 <ETag>&quot;{}&quot;</ETag>\
                 <Size>{}</Size>\
                 <StorageClass>{}</StorageClass>\
                 </Contents>",
                xml_escape(key),
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
            common_prefixes_xml.push_str(&format!(
                "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
                xml_escape(cp),
            ));
        }

        let next_marker = if is_truncated {
            format!("<NextMarker>{}</NextMarker>", xml_escape(&last_key))
        } else {
            String::new()
        };

        let marker_xml = if !marker.is_empty() {
            format!("<Marker>{}</Marker>", xml_escape(&marker))
        } else {
            "<Marker/>".to_string()
        };

        let delimiter_xml = if !delimiter.is_empty() {
            format!("<Delimiter>{}</Delimiter>", xml_escape(&delimiter))
        } else {
            String::new()
        };

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Name>{bucket}</Name>\
             <Prefix>{prefix}</Prefix>\
             {marker_xml}\
             <MaxKeys>{max_keys}</MaxKeys>\
             {delimiter_xml}\
             <IsTruncated>{is_truncated}</IsTruncated>\
             {next_marker}\
             {contents}\
             {common_prefixes_xml}\
             </ListBucketResult>",
            prefix = xml_escape(&prefix),
        );
        Ok(AwsResponse::xml(StatusCode::OK, body))
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

        let effective_start = continuation.as_deref().unwrap_or(&start_after);

        let mut contents = String::new();
        let mut common_prefixes: Vec<String> = Vec::new();
        let mut count = 0;
        let mut is_truncated = false;
        let mut last_key = String::new();

        for (key, obj) in &b.objects {
            if !key.starts_with(&prefix) {
                continue;
            }
            if !effective_start.is_empty() && key.as_str() <= effective_start {
                continue;
            }

            if !delimiter.is_empty() {
                let suffix = &key[prefix.len()..];
                if let Some(pos) = suffix.find(&delimiter) {
                    let cp = format!("{}{}", prefix, &suffix[..=pos]);
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

            contents.push_str(&format!(
                "<Contents>\
                 <Key>{}</Key>\
                 <LastModified>{}</LastModified>\
                 <ETag>&quot;{}&quot;</ETag>\
                 <Size>{}</Size>\
                 <StorageClass>{}</StorageClass>\
                 </Contents>",
                xml_escape(key),
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
            common_prefixes_xml.push_str(&format!(
                "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
                xml_escape(cp),
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

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Name>{bucket}</Name>\
             <Prefix>{prefix}</Prefix>\
             <KeyCount>{count}</KeyCount>\
             <MaxKeys>{max_keys}</MaxKeys>\
             <IsTruncated>{is_truncated}</IsTruncated>\
             {cont_token}\
             {next_token}\
             {contents}\
             {common_prefixes_xml}\
             </ListBucketResult>",
            prefix = xml_escape(&prefix),
        );
        Ok(AwsResponse::xml(StatusCode::OK, body))
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
        let max_keys: usize = req
            .query_params
            .get("max-keys")
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000);

        let mut versions_xml = String::new();
        let mut count = 0;
        let mut is_truncated = false;

        for (key, obj) in &b.objects {
            if !key.starts_with(&prefix) {
                continue;
            }
            if count >= max_keys {
                is_truncated = true;
                break;
            }
            versions_xml.push_str(&format!(
                "<Version>\
                 <Key>{}</Key>\
                 <VersionId>null</VersionId>\
                 <IsLatest>true</IsLatest>\
                 <LastModified>{}</LastModified>\
                 <ETag>&quot;{}&quot;</ETag>\
                 <Size>{}</Size>\
                 <StorageClass>{}</StorageClass>\
                 </Version>",
                xml_escape(key),
                obj.last_modified.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                obj.etag,
                obj.size,
                obj.storage_class,
            ));
            count += 1;
        }

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListVersionsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Name>{bucket}</Name>\
             <Prefix>{prefix}</Prefix>\
             <MaxKeys>{max_keys}</MaxKeys>\
             <IsTruncated>{is_truncated}</IsTruncated>\
             {versions_xml}\
             </ListVersionsResult>",
            prefix = xml_escape(&prefix),
        );
        Ok(AwsResponse::xml(StatusCode::OK, body))
    }

    // ---- CreateMultipartUpload ----

    fn create_multipart_upload(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        if !state.buckets.contains_key(bucket) {
            return Err(no_such_bucket(bucket));
        }

        let content_type = req
            .headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("binary/octet-stream")
            .to_string();
        let storage_class = req
            .headers
            .get("x-amz-storage-class")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let metadata = extract_user_metadata(&req.headers);

        let upload_id = format!(
            "{:x}",
            Md5::digest(
                format!(
                    "{}/{}/{}",
                    bucket,
                    key,
                    Utc::now().timestamp_nanos_opt().unwrap_or(0)
                )
                .as_bytes()
            )
        );

        let upload = MultipartUpload {
            upload_id: upload_id.clone(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            parts: std::collections::HashMap::new(),
            initiated: Utc::now(),
            storage_class,
            metadata,
            content_type,
        };
        state.multipart_uploads.insert(upload_id.clone(), upload);

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Bucket>{}</Bucket>\
             <Key>{}</Key>\
             <UploadId>{upload_id}</UploadId>\
             </InitiateMultipartUploadResult>",
            xml_escape(bucket),
            xml_escape(key),
        );
        Ok(AwsResponse::xml(StatusCode::OK, body))
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
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let data = req.body.clone();
        let etag = compute_md5(&data);
        let content_type = req
            .headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_string();

        let metadata = extract_user_metadata(&req.headers);

        let obj = S3Object {
            key: key.to_string(),
            size: data.len() as u64,
            data,
            content_type,
            etag: etag.clone(),
            last_modified: Utc::now(),
            metadata,
            storage_class: "STANDARD".to_string(),
        };
        b.objects.insert(key.to_string(), obj);

        let mut headers = HeaderMap::new();
        headers.insert("etag", format!("\"{etag}\"").parse().unwrap());
        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: Bytes::new(),
            headers,
        })
    }

    fn get_object(&self, bucket: &str, key: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let obj = b.objects.get(key).ok_or_else(|| no_such_key(key))?;

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
        headers.insert("content-length", obj.size.to_string().parse().unwrap());
        for (k, v) in &obj.metadata {
            if let (Ok(name), Ok(val)) = (
                format!("x-amz-meta-{k}").parse::<http::header::HeaderName>(),
                v.parse::<http::header::HeaderValue>(),
            ) {
                headers.insert(name, val);
            }
        }

        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: obj.content_type.clone(),
            body: obj.data.clone(),
            headers,
        })
    }

    fn delete_object(&self, bucket: &str, key: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.objects.remove(key);
        Ok(empty_response(StatusCode::NO_CONTENT))
    }

    fn head_object(&self, bucket: &str, key: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let obj = b.objects.get(key).ok_or_else(|| no_such_key(key))?;

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
        headers.insert("content-length", obj.size.to_string().parse().unwrap());

        Ok(AwsResponse {
            status: StatusCode::OK,
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

        let decoded = percent_encoding::percent_decode_str(copy_source)
            .decode_utf8_lossy()
            .to_string();
        let source = decoded.strip_prefix('/').unwrap_or(&decoded);
        let (src_bucket, src_key) = source.split_once('/').ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Invalid copy source format",
            )
        })?;

        let mut state = self.state.write();

        let src_obj = {
            let sb = state
                .buckets
                .get(src_bucket)
                .ok_or_else(|| no_such_bucket(src_bucket))?;
            sb.objects
                .get(src_key)
                .ok_or_else(|| no_such_key(src_key))?
                .clone()
        };

        let etag = src_obj.etag.clone();
        let last_modified = Utc::now();

        let db = state
            .buckets
            .get_mut(dest_bucket)
            .ok_or_else(|| no_such_bucket(dest_bucket))?;
        db.objects.insert(
            dest_key.to_string(),
            S3Object {
                key: dest_key.to_string(),
                last_modified,
                ..src_obj
            },
        );

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <CopyObjectResult>\
             <ETag>&quot;{etag}&quot;</ETag>\
             <LastModified>{}</LastModified>\
             </CopyObjectResult>",
            last_modified.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
        );
        Ok(AwsResponse::xml(StatusCode::OK, body))
    }

    fn delete_objects(
        &self,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let keys = parse_delete_objects_xml(body_str);

        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let mut deleted_xml = String::new();
        for key in &keys {
            b.objects.remove(key);
            deleted_xml.push_str(&format!(
                "<Deleted><Key>{}</Key></Deleted>",
                xml_escape(key),
            ));
        }

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             {deleted_xml}\
             </DeleteResult>"
        );
        Ok(AwsResponse::xml(StatusCode::OK, body))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn no_such_bucket(bucket: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "NoSuchBucket",
        format!("The specified bucket does not exist: {bucket}"),
    )
}

fn no_such_key(key: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "NoSuchKey",
        format!("The specified key does not exist: {key}"),
    )
}

fn empty_response(status: StatusCode) -> AwsResponse {
    AwsResponse {
        status,
        content_type: "application/xml".to_string(),
        body: Bytes::new(),
        headers: HeaderMap::new(),
    }
}

fn compute_md5(data: &[u8]) -> String {
    let digest = Md5::digest(data);
    format!("{:x}", digest)
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
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

fn is_valid_bucket_name(name: &str) -> bool {
    if name.len() < 3 || name.len() > 63 {
        return false;
    }
    let bytes = name.as_bytes();
    if !bytes[0].is_ascii_alphanumeric() || !bytes[bytes.len() - 1].is_ascii_alphanumeric() {
        return false;
    }
    name.chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.')
}

fn parse_delete_objects_xml(xml: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut remaining = xml;
    while let Some(start) = remaining.find("<Key>") {
        let after = &remaining[start + 5..];
        if let Some(end) = after.find("</Key>") {
            keys.push(after[..end].to_string());
            remaining = &after[end + 6..];
        } else {
            break;
        }
    }
    keys
}

fn parse_tagging_xml(xml: &str) -> std::collections::HashMap<String, String> {
    let mut tags = std::collections::HashMap::new();
    let mut remaining = xml;
    while let Some(tag_start) = remaining.find("<Tag>") {
        let after = &remaining[tag_start + 5..];
        if let Some(tag_end) = after.find("</Tag>") {
            let tag_body = &after[..tag_end];
            let key = extract_xml_value(tag_body, "Key");
            let value = extract_xml_value(tag_body, "Value");
            if let (Some(k), Some(v)) = (key, value) {
                tags.insert(k, v);
            }
            remaining = &after[tag_end + 6..];
        } else {
            break;
        }
    }
    tags
}

fn extract_xml_value(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml.find(&close)?;
    Some(xml[start..end].to_string())
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
        let keys = parse_delete_objects_xml(xml);
        assert_eq!(keys, vec!["a.txt", "b/c.txt"]);
    }

    #[test]
    fn parse_tags_xml() {
        let xml =
            r#"<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>"#;
        let tags = parse_tagging_xml(xml);
        assert_eq!(tags.get("env").unwrap(), "prod");
    }

    #[test]
    fn md5_hash() {
        let hash = compute_md5(b"hello");
        assert_eq!(hash, "5d41402abc4b2a76b9719d911017c592");
    }
}
