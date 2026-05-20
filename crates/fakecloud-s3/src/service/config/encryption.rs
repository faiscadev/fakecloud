//! `S3Service` `encryption` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl S3Service {
    // ---- Encryption ----

    pub(crate) fn put_bucket_encryption(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("").to_string();
        // aws:kms / aws:kms:dsse rules require KMSMasterKeyID — AWS
        // rejects these with InvalidArgument when the field is
        // missing or empty, since the bucket would otherwise have
        // no key to encrypt against.
        if (body_str.contains("aws:kms") || body_str.contains("aws:kms:dsse"))
            && !has_kms_master_key_id(&body_str)
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Default KMS encryption requires KMSMasterKeyID",
            ));
        }
        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
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
        b.encryption_config = Some(normalized.clone());
        self.store
            .put_bucket_subresource(bucket, BucketSubresource::Encryption, &normalized)
            .map_err(crate::service::persistence_error)?;
        Ok(empty_response(StatusCode::OK))
    }

    pub(crate) fn get_bucket_encryption(
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

    pub(crate) fn delete_bucket_encryption(
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
        b.encryption_config = None;
        self.store
            .delete_bucket_subresource(bucket, BucketSubresource::Encryption)
            .map_err(crate::service::persistence_error)?;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }
}
