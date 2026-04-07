use http::{HeaderMap, StatusCode};

use bytes::Bytes;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::{
    build_acl_xml, canned_acl_grants_for_object, no_such_bucket, no_such_key, parse_acl_xml,
    parse_grant_headers, s3_xml, S3Service,
};

impl S3Service {
    pub(super) fn get_object_acl(
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

    pub(super) fn put_object_acl(
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
}
