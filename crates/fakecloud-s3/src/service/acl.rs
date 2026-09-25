use http::{HeaderMap, StatusCode};

use bytes::Bytes;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::persistence::object_meta_snapshot;

use super::{
    build_acl_xml, canned_acl_grants_for_object, no_such_key, parse_acl_xml, s3_xml, S3Service,
};

impl S3Service {
    pub(super) fn get_object_acl(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let __empty = crate::state::S3State::new(account_id, "us-east-1");
        let state = accts.get(account_id).unwrap_or(&__empty);
        // GetObjectAcl only declares NoSuchKey per the Smithy model;
        // collapse missing-bucket into NoSuchKey for strict conformance.
        let b = state.buckets.get(bucket).ok_or_else(|| no_such_key(key))?;
        let obj = b.objects.get(key).ok_or_else(|| no_such_key(key))?;

        let owner_id = obj.acl_owner_id.as_deref().unwrap_or(&req.account_id);
        let body = build_acl_xml(owner_id, &obj.acl_grants, &req.account_id);
        Ok(s3_xml(StatusCode::OK, body))
    }

    pub(super) fn put_object_acl(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let canned = req
            .headers
            .get("x-amz-acl")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        // Validated before the key is resolved, as S3 does: a bad canned value
        // is an InvalidArgument whether or not the key exists.
        if let Some(acl) = canned.as_deref() {
            super::validate_object_canned_acl(acl)?;
        }
        super::reject_conflicting_acl_sources(canned.as_deref(), &req.headers, &req.body)?;

        if self.bucket_owner_enforced(account_id, bucket) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AccessControlListNotSupported",
                "The bucket does not allow ACLs",
            ));
        }

        // Snapshot PAB before taking the write lock — `pab_flags`
        // reads its own lock and would deadlock under the same
        // upgrade.
        let pab = self.pab_flags(account_id, bucket);
        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        // PutObjectAcl only declares NoSuchKey per the Smithy model;
        // collapse missing-bucket into NoSuchKey for strict conformance.
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_key(key))?;
        let owner_id = b.acl_owner_id.clone();
        let obj = b.objects.get_mut(key).ok_or_else(|| no_such_key(key))?;

        let proposed_grants = if let Some(acl) = &canned {
            canned_acl_grants_for_object(acl, &owner_id)
        } else {
            if super::has_grant_headers(&req.headers) {
                super::resolved_grant_headers(&req.headers)?
            } else {
                // No canned header, no grant header, no body names no ACL at
                // all. Re-persisting the object's current grants would answer
                // 200 for a request that asked for nothing, which the caller
                // cannot tell from an applied change -- the same rule
                // PutBucketAcl applies.
                let body_str = std::str::from_utf8(&req.body).unwrap_or("");
                if body_str.trim().is_empty() {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "MalformedACLError",
                        "The XML you provided was not well-formed or did not validate against our published schema",
                    ));
                }
                // A body that is not an AccessControlPolicy at all (JSON, a
                // misspelled root) parses to zero grants, and taking that as
                // "remove every grant" strips the object's owner FULL_CONTROL
                // and writes the empty list into its meta. Same rule the bucket
                // path applies.
                if !body_str.contains("<AccessControlPolicy") {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "MalformedXML",
                        "The XML you provided was not well-formed or did not validate against our published schema",
                    ));
                }
                parse_acl_xml(body_str)?
            }
        };

        if let Some(flags) = pab {
            if flags.block_public_acls
                && super::config::grants_are_public(&proposed_grants)
                && !super::config::grants_are_public(&obj.acl_grants)
            {
                return Err(AwsServiceError::aws_error(
                    StatusCode::FORBIDDEN,
                    "AccessDenied",
                    "User is not authorized to perform: s3:PutObjectAcl. Reason: Public Access Block (BlockPublicAcls)",
                ));
            }
        }
        // Persist before touching memory, as put_bucket_acl does: assigning
        // first would leave the in-memory ACL ahead of the stored one, and a
        // restart would silently revert what the caller was told had failed.
        //
        // The snapshot has to carry the PROPOSED grants, not the object's
        // current ones -- `object_meta_snapshot` copies `acl_grants`, so
        // snapshotting before the assignment would persist the old ACL and
        // leave the new one memory-only.
        let mut meta = object_meta_snapshot(obj);
        meta.acl_grants = proposed_grants
            .iter()
            .map(fakecloud_persistence::AclGrantSnapshot::from)
            .collect();
        self.store
            .put_object_meta(bucket, key, meta.version_id.as_deref(), &meta)
            .map_err(super::persistence_error)?;
        obj.acl_grants = proposed_grants.clone();
        // A versioned bucket keeps a second copy of this version in
        // `object_versions`, and `resolve_object` reads THAT one for a
        // versionId request. Leaving it stale made GetObjectAcl answer
        // differently depending on whether a versionId was passed, and a later
        // delete re-derived the current object from the stale copy, silently
        // reverting the change that had already been persisted.
        let version_id = obj.version_id.clone();
        if let Some(versions) = b.object_versions.get_mut(key) {
            for v in versions.iter_mut() {
                if v.version_id == version_id {
                    v.acl_grants = proposed_grants.clone();
                }
            }
        }
        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: Bytes::new().into(),
            headers: HeaderMap::new(),
        })
    }

    // ---- Object Tagging ----
}
