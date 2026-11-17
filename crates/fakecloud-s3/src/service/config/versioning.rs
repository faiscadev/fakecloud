//! `S3Service` `versioning` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl S3Service {
    // ---- Bucket Versioning ----

    pub(crate) fn put_bucket_versioning(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let status_val = extract_xml_value(body_str, "Status").unwrap_or_default();
        let mfa_delete_val = extract_xml_value(body_str, "MfaDelete");

        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        // AWS refuses to suspend versioning on an Object Lock bucket: lock
        // retention is enforced per version, and suspending would let a null
        // version overwrite a retained one.
        if status_val == "Suspended" && b.object_lock_config.is_some() {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "InvalidBucketState",
                "An Object Lock configuration is present on this bucket, so the versioning state cannot be changed.",
            ));
        }
        // Nor on a replication source: replication needs versioning enabled
        // on the source, and the configuration must be removed before it can
        // be suspended ("If you attempt to disable versioning on the source
        // bucket, Amazon S3 returns an error", S3 User Guide, Requirements
        // and considerations for replication).
        if status_val == "Suspended" && b.replication_config.is_some() {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "InvalidBucketState",
                "A replication configuration is present on this bucket, so the versioning state cannot be changed.",
            ));
        }
        if status_val == "Enabled" || status_val == "Suspended" {
            b.versioning = Some(status_val);
        }
        if let Some(mfa) = mfa_delete_val {
            if mfa == "Enabled" || mfa == "Disabled" {
                b.mfa_delete = Some(mfa);
            }
        }
        let meta = bucket_meta_snapshot(account_id, b);
        self.store
            .put_bucket_meta(account_id, bucket, &meta)
            .map_err(crate::service::persistence_error)?;
        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: Bytes::new().into(),
            headers: HeaderMap::new(),
        })
    }

    pub(crate) fn get_bucket_versioning(
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
        let status_xml = match &b.versioning {
            Some(s) => format!("<Status>{s}</Status>"),
            None => String::new(),
        };
        let mfa_xml = match &b.mfa_delete {
            Some(s) => format!("<MfaDelete>{s}</MfaDelete>"),
            None => String::new(),
        };
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             {status_xml}\
             {mfa_xml}\
             </VersioningConfiguration>"
        );
        Ok(s3_xml(StatusCode::OK, body))
    }
}
