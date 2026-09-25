//! `S3Service` `ownership` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl S3Service {
    pub(crate) fn put_bucket_ownership_controls(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Reject non-UTF-8 bodies instead of silently coercing to ""
        // and erasing the stored ownership config.
        let body_str = std::str::from_utf8(&req.body)
            .map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "MalformedXML",
                    "PutBucketOwnershipControls body is not valid UTF-8",
                )
            })?
            .to_string();
        // Validated like the create-time header: an unrecognized value would be
        // stored and come back after a restart meaning nothing, and
        // `bucket_owner_enforced` matches BucketOwnerEnforced exactly, so a
        // bucket could silently return to ACLs-enabled.
        // Every occurrence, not just the first: `bucket_owner_enforced` matches
        // the stored document as text, so a second rule (or a comment) carrying
        // BucketOwnerEnforced would turn ACLs off for the bucket while
        // GetBucketOwnershipControls reported something else entirely.
        let mut seen = 0usize;
        let mut rest = body_str.as_str();
        while let Some(start) = rest.find("<ObjectOwnership>") {
            let after = &rest[start + "<ObjectOwnership>".len()..];
            let Some(end) = after.find("</ObjectOwnership>") else {
                break;
            };
            let value = after[..end].trim();
            if !crate::service::OBJECT_OWNERSHIP_VALUES.contains(&value) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    format!("Invalid ObjectOwnership value: {value}"),
                ));
            }
            seen += 1;
            rest = &after[end..];
        }
        let single = seen == 1;
        let value = extract_xml_value(&body_str, "ObjectOwnership").unwrap_or_default();
        // `bucket_owner_enforced` looks for the literal anywhere in the stored
        // document, so it must appear exactly when it is the rule's value --
        // never in a second rule or a comment.
        let enforced_mentions = body_str.matches("BucketOwnerEnforced").count();
        let expected = usize::from(value == "BucketOwnerEnforced");
        if !single || enforced_mentions != expected {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                "OwnershipControls must carry exactly one Rule with a valid ObjectOwnership",
            ));
        }
        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        // Persist before mutating memory, as the ACL paths do: a store failure
        // would otherwise leave memory ahead of disk, and a restart would
        // silently revert what the caller was told had failed.
        self.store
            .put_bucket_subresource(bucket, BucketSubresource::Ownership, &body_str)
            .map_err(crate::service::persistence_error)?;
        b.ownership_controls = Some(body_str.clone());
        Ok(empty_response(StatusCode::OK))
    }

    pub(crate) fn get_bucket_ownership_controls(
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

    pub(crate) fn delete_bucket_ownership_controls(
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
        b.ownership_controls = None;
        self.store
            .delete_bucket_subresource(bucket, BucketSubresource::Ownership)
            .map_err(crate::service::persistence_error)?;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }
}
