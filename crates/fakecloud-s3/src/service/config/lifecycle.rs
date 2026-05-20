//! `S3Service` `lifecycle` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl S3Service {
    // ---- Lifecycle ----

    pub(crate) fn put_bucket_lifecycle(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("").to_string();

        // Validate lifecycle configuration
        validate_lifecycle_xml(&body_str)?;

        // If there are no <Rule> elements at all, treat as deleting the configuration
        let has_rules = body_str.contains("<Rule>");

        let tdmos = req
            .headers
            .get(LIFECYCLE_TDMOS_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        if has_rules {
            b.lifecycle_config = Some(body_str.clone());
            b.lifecycle_transition_default_min_size = tdmos.clone();
            self.store
                .put_bucket_subresource(bucket, BucketSubresource::Lifecycle, &body_str)
                .map_err(crate::service::persistence_error)?;
        } else {
            b.lifecycle_config = None;
            b.lifecycle_transition_default_min_size = None;
            self.store
                .delete_bucket_subresource(bucket, BucketSubresource::Lifecycle)
                .map_err(crate::service::persistence_error)?;
        }
        let meta = bucket_meta_snapshot(b);
        self.store
            .put_bucket_meta(bucket, &meta)
            .map_err(crate::service::persistence_error)?;
        let mut resp = empty_response(StatusCode::OK);
        insert_tdmos_header(&mut resp.headers, tdmos.as_deref());
        Ok(resp)
    }

    pub(crate) fn get_bucket_lifecycle(
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
        match &b.lifecycle_config {
            Some(config) => {
                let mut resp = s3_xml(StatusCode::OK, config.clone());
                insert_tdmos_header(
                    &mut resp.headers,
                    b.lifecycle_transition_default_min_size.as_deref(),
                );
                Ok(resp)
            }
            None => Err(AwsServiceError::aws_error_with_fields(
                StatusCode::NOT_FOUND,
                "NoSuchLifecycleConfiguration",
                "The lifecycle configuration does not exist",
                vec![("BucketName".to_string(), bucket.to_string())],
            )),
        }
    }

    pub(crate) fn delete_bucket_lifecycle(
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
        b.lifecycle_config = None;
        self.store
            .delete_bucket_subresource(bucket, BucketSubresource::Lifecycle)
            .map_err(crate::service::persistence_error)?;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }
}
