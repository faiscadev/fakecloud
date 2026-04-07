use http::{HeaderMap, StatusCode};

use bytes::Bytes;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::S3Bucket;

use super::{
    canned_acl_grants, extract_xml_value, is_valid_bucket_name, is_valid_region, no_such_bucket,
    s3_xml, xml_escape, S3Service,
};

impl S3Service {
    pub(super) fn list_buckets(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn create_bucket(
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

    pub(super) fn delete_bucket(
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

    pub(super) fn head_bucket(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn get_bucket_location(&self, bucket: &str) -> Result<AwsResponse, AwsServiceError> {
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
}
