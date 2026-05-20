//! `S3Service` `policy` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl S3Service {
    // ---- Policy ----

    pub(crate) fn put_bucket_policy(
        &self,
        account_id: &str,
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
        // Enforce PublicAccessBlock.BlockPublicPolicy: AWS rejects
        // PutBucketPolicy that grants public access while the flag is
        // set, with `AccessDenied` and a body explaining the gate.
        if let Some(flags) = self.pab_flags(account_id, bucket) {
            if flags.block_public_policy && policy_is_public(&body_str) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::FORBIDDEN,
                    "AccessDenied",
                    "User is not authorized to perform: s3:PutBucketPolicy. Reason: Public Access Block (BlockPublicPolicy)",
                ));
            }
        }
        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.policy = Some(body_str.clone());
        self.store
            .put_bucket_subresource(bucket, BucketSubresource::Policy, &body_str)
            .map_err(crate::service::persistence_error)?;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }

    pub(crate) fn get_bucket_policy(
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
        match &b.policy {
            Some(policy) => Ok(AwsResponse {
                status: StatusCode::OK,
                content_type: "application/json".to_string(),
                body: Bytes::from(policy.clone()).into(),
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

    pub(crate) fn delete_bucket_policy(
        &self,
        account_id: &str,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.policy = None;
        self.store
            .delete_bucket_subresource(bucket, BucketSubresource::Policy)
            .map_err(crate::service::persistence_error)?;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }

    // ---- Policy Status ----

    pub(crate) fn get_bucket_policy_status(
        &self,
        account_id: &str,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let empty = crate::state::S3State::new(account_id, "us-east-1");
        let state = accts.get(account_id).unwrap_or(&empty);
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        // Real JSON parse instead of substring scanning so a policy
        // containing the literal string `"Principal":"*"` inside a
        // Description field doesn't get falsely classified public.
        let is_public = b.policy.as_deref().map(policy_is_public).unwrap_or(false);
        let body = format!("<PolicyStatus><IsPublic>{is_public}</IsPublic></PolicyStatus>");
        Ok(s3_xml(StatusCode::OK, body))
    }
}
