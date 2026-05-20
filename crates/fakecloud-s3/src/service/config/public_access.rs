//! `S3Service` `public_access` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl S3Service {
    // ---- PublicAccessBlock helpers ----

    /// Read each `PublicAccessBlock` flag from the bucket's stored XML.
    /// Missing fields default to `false` per AWS, which mirrors the
    /// `GetPublicAccessBlock` echo path. Returns `None` when no
    /// configuration is set, so callers can short-circuit.
    pub(crate) fn pab_flags(
        &self,
        account_id: &str,
        bucket: &str,
    ) -> Option<PublicAccessBlockFlags> {
        let accts = self.state.read();
        let state = accts.get(account_id)?;
        let b = state.buckets.get(bucket)?;
        let xml = b.public_access_block.as_ref()?;
        Some(PublicAccessBlockFlags::parse(xml))
    }

    // ---- PublicAccessBlock ----

    pub(crate) fn put_public_access_block(
        &self,
        account_id: &str,
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
        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.public_access_block = Some(body_str.clone());
        self.store
            .put_bucket_subresource(bucket, BucketSubresource::PublicAccessBlock, &body_str)
            .map_err(crate::service::persistence_error)?;
        Ok(empty_response(StatusCode::OK))
    }

    pub(crate) fn get_public_access_block(
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

    pub(crate) fn delete_public_access_block(
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
        b.public_access_block = None;
        self.store
            .delete_bucket_subresource(bucket, BucketSubresource::PublicAccessBlock)
            .map_err(crate::service::persistence_error)?;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }
}
