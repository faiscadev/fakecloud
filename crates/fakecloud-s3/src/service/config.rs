use http::{HeaderMap, StatusCode};

use bytes::Bytes;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::inventory;

use super::{
    build_acl_xml, canned_acl_grants, empty_response, extract_xml_value, no_such_bucket,
    normalize_notification_ids, normalize_replication_xml, parse_acl_xml, parse_tagging_xml,
    s3_xml, validate_lifecycle_xml, validate_tags, xml_escape, S3Service,
};

impl S3Service {
    // ---- Encryption ----

    pub(super) fn put_bucket_encryption(
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

    pub(super) fn get_bucket_encryption(
        &self,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn delete_bucket_encryption(
        &self,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.encryption_config = None;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }

    // ---- Lifecycle ----

    pub(super) fn put_bucket_lifecycle(
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

    pub(super) fn get_bucket_lifecycle(
        &self,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn delete_bucket_lifecycle(
        &self,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.lifecycle_config = None;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }

    // ---- Policy ----

    pub(super) fn put_bucket_policy(
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

    pub(super) fn get_bucket_policy(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn delete_bucket_policy(
        &self,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.policy = None;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }

    // ---- CORS ----

    pub(super) fn put_bucket_cors(
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

    pub(super) fn get_bucket_cors(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn delete_bucket_cors(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.cors_config = None;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }

    // ---- Notification ----

    pub(super) fn put_bucket_notification(
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
        // Check if EventBridgeConfiguration XML element is present (opening tag or self-closing)
        b.eventbridge_enabled = body_str.contains("<EventBridgeConfiguration");
        // Auto-generate Id for each configuration element if missing
        let normalized = normalize_notification_ids(&body_str);
        b.notification_config = Some(normalized);
        Ok(empty_response(StatusCode::OK))
    }

    pub(super) fn get_bucket_notification(
        &self,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let mut body = match &b.notification_config {
            Some(config) => config.clone(),
            None => "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                     <NotificationConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                     </NotificationConfiguration>"
                .to_string(),
        };
        // Ensure EventBridgeConfiguration is in response if enabled
        if b.eventbridge_enabled && !body.contains("EventBridgeConfiguration") {
            if let Some(pos) = body.find("</NotificationConfiguration>") {
                body.insert_str(pos, "<EventBridgeConfiguration/>");
            }
        }
        Ok(s3_xml(StatusCode::OK, body))
    }

    // ---- Logging ----

    pub(super) fn put_bucket_logging(
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

    pub(super) fn get_bucket_logging(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn put_bucket_website(
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

    pub(super) fn get_bucket_website(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn delete_bucket_website(
        &self,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.website_config = None;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }

    // ---- Accelerate ----

    pub(super) fn put_bucket_accelerate(
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

    pub(super) fn get_bucket_accelerate(
        &self,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn put_public_access_block(
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

    pub(super) fn get_public_access_block(
        &self,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn delete_public_access_block(
        &self,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.public_access_block = None;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }

    // ---- ObjectLockConfiguration ----

    pub(super) fn put_object_lock_config(
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

    pub(super) fn get_bucket_tagging(
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

    pub(super) fn put_bucket_tagging(
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

    pub(super) fn delete_bucket_tagging(
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

    pub(super) fn get_bucket_acl(
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

    pub(super) fn put_bucket_acl(
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

    pub(super) fn put_bucket_versioning(
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

    pub(super) fn get_bucket_versioning(
        &self,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
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
    pub(super) fn get_object_lock_configuration(
        &self,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn put_bucket_replication(
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

    pub(super) fn get_bucket_replication(
        &self,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn delete_bucket_replication(
        &self,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.replication_config = None;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }

    pub(super) fn put_bucket_ownership_controls(
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

    pub(super) fn get_bucket_ownership_controls(
        &self,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn delete_bucket_ownership_controls(
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

    pub(super) fn put_bucket_inventory(
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

    pub(super) fn get_bucket_inventory(
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

    pub(super) fn list_bucket_inventory_configurations(
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

    pub(super) fn delete_bucket_inventory(
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
