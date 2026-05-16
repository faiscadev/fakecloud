use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use http::{HeaderMap, StatusCode};

use bytes::Bytes;
use fakecloud_aws::arn::Arn;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::persistence::bucket_meta_snapshot;
use crate::state::S3Bucket;

use super::{
    canned_acl_grants, extract_xml_value, is_valid_bucket_name, is_valid_region, no_such_bucket,
    s3_xml, xml_escape, S3Service,
};

impl S3Service {
    pub(super) fn list_buckets(
        &self,
        account_id: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let prefix = req.query_params.get("prefix").cloned();
        let bucket_region_filter = req.query_params.get("bucket-region").cloned();

        let max_buckets: usize = match req.query_params.get("max-buckets") {
            Some(v) => match v.parse::<i64>() {
                Ok(n) if (1..=10_000).contains(&n) => n as usize,
                _ => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidArgument",
                        "max-buckets must be between 1 and 10000",
                    ));
                }
            },
            None => 10_000,
        };

        let continuation_token = req.query_params.get("continuation-token").cloned();
        let token_after: Option<String> = match continuation_token.as_deref() {
            None => None,
            Some("") => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    "The continuation token provided is incorrect",
                ));
            }
            Some(tok) => match BASE64
                .decode(tok.as_bytes())
                .ok()
                .and_then(|d| String::from_utf8(d).ok())
            {
                Some(s) => Some(s),
                None => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidArgument",
                        "The continuation token provided is incorrect",
                    ));
                }
            },
        };

        let accts = self.state.read();
        let __empty = crate::state::S3State::new(account_id, "us-east-1");
        let state = accts.get(account_id).unwrap_or(&__empty);

        let mut filtered: Vec<&S3Bucket> = state
            .buckets
            .values()
            .filter(|b| {
                if let Some(p) = &prefix {
                    if !b.name.starts_with(p) {
                        return false;
                    }
                }
                if let Some(r) = &bucket_region_filter {
                    if &b.region != r {
                        return false;
                    }
                }
                true
            })
            .collect();
        filtered.sort_by(|a, b| a.name.cmp(&b.name));

        let start_index = match &token_after {
            Some(after) => filtered.partition_point(|b| b.name.as_str() <= after.as_str()),
            None => 0,
        };
        let end_index = (start_index + max_buckets).min(filtered.len());
        let page = &filtered[start_index..end_index];
        let next_continuation = if end_index < filtered.len() {
            page.last().map(|b| BASE64.encode(b.name.as_bytes()))
        } else {
            None
        };

        let mut buckets_xml = String::new();
        for b in page {
            buckets_xml.push_str(&format!(
                "<Bucket><Name>{name}</Name><CreationDate>{cd}</CreationDate><BucketRegion>{region}</BucketRegion><BucketArn>arn:aws:s3:::{name}</BucketArn></Bucket>",
                name = xml_escape(&b.name),
                cd = b.creation_date.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                region = xml_escape(&b.region),
            ));
        }

        let mut tail_xml = String::new();
        if let Some(p) = &prefix {
            tail_xml.push_str(&format!("<Prefix>{}</Prefix>", xml_escape(p)));
        }
        if let Some(r) = &bucket_region_filter {
            tail_xml.push_str(&format!("<BucketRegion>{}</BucketRegion>", xml_escape(r)));
        }
        if let Some(nct) = &next_continuation {
            tail_xml.push_str(&format!(
                "<ContinuationToken>{}</ContinuationToken>",
                xml_escape(nct),
            ));
        }

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Owner><ID>{account}</ID><DisplayName>{account}</DisplayName></Owner>\
             <Buckets>{buckets_xml}</Buckets>\
             {tail_xml}\
             </ListAllMyBucketsResult>",
            account = account_id,
        );
        Ok(s3_xml(StatusCode::OK, body))
    }

    pub(super) fn create_bucket(
        &self,
        account_id: &str,
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

        let mut accts = self.state.write();
        // Check global uniqueness across all accounts before creating
        for (other_account_id, acct_state) in accts.iter() {
            if acct_state.buckets.contains_key(bucket) {
                if other_account_id == account_id {
                    // Same account owns it — fall through to idempotency / BucketAlreadyOwnedByYou logic below
                    break;
                }
                return Err(AwsServiceError::aws_error(
                    StatusCode::CONFLICT,
                    "BucketAlreadyExists",
                    "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again.",
                ));
            }
        }
        let state = accts.get_or_create(account_id);
        if let Some(existing) = state.buckets.get(bucket) {
            // In us-east-1, re-creating same bucket in same region is idempotent (returns 200)
            if existing.region == requested_region && requested_region == "us-east-1" {
                let mut headers = HeaderMap::new();
                headers.insert("location", format!("/{bucket}").parse().unwrap());
                return Ok(AwsResponse {
                    status: StatusCode::OK,
                    content_type: "application/xml".to_string(),
                    body: Bytes::new().into(),
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

        let meta = bucket_meta_snapshot(&b);
        state.buckets.insert(bucket.to_string(), b);
        self.store
            .put_bucket_meta(bucket, &meta)
            .map_err(super::persistence_error)?;

        let mut headers = HeaderMap::new();
        headers.insert("location", format!("/{bucket}").parse().unwrap());
        headers.insert(
            "x-amz-bucket-arn",
            Arn::s3(bucket).to_string().parse().unwrap(),
        );
        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: Bytes::new().into(),
            headers,
        })
    }

    pub(super) fn delete_bucket(
        &self,
        account_id: &str,
        _req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
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
        self.store
            .delete_bucket(bucket)
            .map_err(super::persistence_error)?;
        Ok(AwsResponse {
            status: StatusCode::NO_CONTENT,
            content_type: "application/xml".to_string(),
            body: Bytes::new().into(),
            headers: HeaderMap::new(),
        })
    }

    pub(super) fn head_bucket(
        &self,
        account_id: &str,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let __empty = crate::state::S3State::new(account_id, "us-east-1");
        let state = accts.get(account_id).unwrap_or(&__empty);
        // HeadBucket's Smithy model declares `NotFound` as the missing-bucket
        // error (com.amazonaws.s3#HeadBucket -> errors: [NotFound]). HEAD
        // responses carry no body — clients read the error from
        // `x-amz-error-code` — so the code emitted here is the only signal
        // the SDK has. Use the model-declared code instead of the
        // operation-agnostic `NoSuchBucket` to keep the wire format aligned
        // with the contract.
        let b = state.buckets.get(bucket).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFound",
                format!("The specified bucket does not exist: {bucket}"),
            )
        })?;
        let mut headers = HeaderMap::new();
        if let Ok(v) = http::HeaderValue::from_str(&b.region) {
            headers.insert("x-amz-bucket-region", v);
        }
        // Region buckets are the only type fakecloud creates; AWS Toolkit
        // checks this header to disambiguate from "directory bucket" /
        // "access point alias" forms.
        headers.insert(
            "x-amz-bucket-location-type",
            http::HeaderValue::from_static("Region"),
        );
        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: Bytes::new().into(),
            headers,
        })
    }

    pub(super) fn get_bucket_location(
        &self,
        account_id: &str,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let __empty = crate::state::S3State::new(account_id, "us-east-1");
        let state = accts.get(account_id).unwrap_or(&__empty);
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
