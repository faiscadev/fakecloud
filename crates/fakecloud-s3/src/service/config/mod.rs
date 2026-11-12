use http::{HeaderMap, StatusCode};

use bytes::Bytes;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_persistence::{
    AclGrantSnapshot, AclSnapshot, BucketSubresource, InventorySnapshot, TagsSnapshot,
};

use crate::inventory;
use crate::persistence::{bucket_meta_snapshot, object_meta_snapshot};

use super::{
    build_acl_xml, canned_acl_grants, empty_response, extract_xml_value, has_grant_headers,
    no_such_bucket, normalize_notification_ids, normalize_replication_xml, parse_acl_xml,
    parse_tagging_xml, reject_conflicting_acl_sources, resolved_grant_headers, s3_xml,
    validate_lifecycle_xml, validate_tags, xml_escape, S3Service, BUCKET_CANNED_ACLS,
};

/// Decoded `PublicAccessBlockConfiguration` flags read by the request
/// path. `GetPublicAccessBlock` echoes the bucket's stored XML directly
/// (see `get_public_access_block`), so flags absent here still
/// round-trip — they just aren't enforced. `RestrictPublicBuckets`
/// is intentionally omitted until the effective-policy evaluator
/// lands; it's a feature gap, not a parse gap.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PublicAccessBlockFlags {
    pub block_public_acls: bool,
    pub ignore_public_acls: bool,
    pub block_public_policy: bool,
}

impl PublicAccessBlockFlags {
    pub(crate) fn parse(xml: &str) -> Self {
        fn flag(xml: &str, name: &str) -> bool {
            let open = format!("<{name}>");
            let close = format!("</{name}>");
            let Some(start) = xml.find(&open) else {
                return false;
            };
            let value_start = start + open.len();
            let Some(end_offset) = xml[value_start..].find(&close) else {
                return false;
            };
            xml[value_start..value_start + end_offset]
                .trim()
                .eq_ignore_ascii_case("true")
        }
        Self {
            block_public_acls: flag(xml, "BlockPublicAcls"),
            ignore_public_acls: flag(xml, "IgnorePublicAcls"),
            block_public_policy: flag(xml, "BlockPublicPolicy"),
        }
    }
}

/// Detect whether a parsed bucket-policy document grants access to an
/// anonymous principal. Mirrors AWS's "is public" classifier:
/// `Effect=Allow` with `Principal:"*"` or `{"AWS":"*"}` (or a list
/// containing `"*"`) and no `Condition` block constraining the
/// principal further. We deliberately mark conditioned policies as
/// non-public — that matches AWS's `BlockPublicPolicy` behavior since
/// IP / VPC / source-account conditions specifically narrow the
/// principal.
pub(crate) fn policy_is_public(policy_json: &str) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(policy_json) else {
        return false;
    };
    let statements = match value.get("Statement") {
        Some(serde_json::Value::Array(a)) => a.clone(),
        Some(s) => vec![s.clone()],
        None => return false,
    };
    statements.iter().any(|st| {
        if st.get("Effect").and_then(|v| v.as_str()) != Some("Allow") {
            return false;
        }
        if st.get("Condition").is_some() {
            return false;
        }
        principal_includes_wildcard(st.get("Principal").unwrap_or(&serde_json::Value::Null))
    })
}

fn principal_includes_wildcard(p: &serde_json::Value) -> bool {
    match p {
        serde_json::Value::String(s) => s == "*",
        serde_json::Value::Object(m) => m.values().any(value_contains_star),
        _ => false,
    }
}

fn value_contains_star(v: &serde_json::Value) -> bool {
    match v {
        serde_json::Value::String(s) => s == "*",
        serde_json::Value::Array(arr) => arr.iter().any(value_contains_star),
        _ => false,
    }
}

/// Detect whether a set of ACL grants grants to a public group
/// (AllUsers / AuthenticatedUsers). Used by PutBucketAcl /
/// PutObjectAcl to gate the `BlockPublicAcls` flag.
pub(crate) fn grants_are_public(grants: &[crate::state::AclGrant]) -> bool {
    grants.iter().any(|g| {
        g.grantee_uri
            .as_deref()
            .map(|u| {
                u.contains("acs.amazonaws.com/groups/global/AllUsers")
                    || u.contains("acs.amazonaws.com/groups/global/AuthenticatedUsers")
            })
            .unwrap_or(false)
    })
}

/// Whether a bucket ACL gives nothing to anyone but the bucket owner, which
/// is the only ACL S3 lets BucketOwnerEnforced be applied over.
pub(crate) fn grants_are_owner_only(grants: &[crate::state::AclGrant], owner_id: &str) -> bool {
    grants
        .iter()
        .all(|g| g.grantee_type == "CanonicalUser" && g.grantee_id.as_deref() == Some(owner_id))
}

/// What reading an ACL returns while ACLs are disabled: S3 "always return[s]
/// a response that shows full control for the bucket owner", whatever grants
/// are stored (they come back if ACLs are re-enabled).
pub(crate) fn owner_full_control(owner_id: &str) -> Vec<crate::state::AclGrant> {
    vec![crate::state::AclGrant {
        grantee_type: "CanonicalUser".to_string(),
        grantee_id: Some(owner_id.to_string()),
        grantee_display_name: Some(owner_id.to_string()),
        grantee_uri: None,
        permission: "FULL_CONTROL".to_string(),
    }]
}

/// Whether a stored `OwnershipControls` document disables ACLs.
pub(crate) fn ownership_xml_enforced(xml: Option<&str>) -> bool {
    xml.is_some_and(|x| x.contains("BucketOwnerEnforced"))
}

const LIFECYCLE_TDMOS_HEADER: &str = "x-amz-transition-default-minimum-object-size";

/// Default value real AWS returns on `GetBucketLifecycleConfiguration` for
/// general purpose buckets when none was supplied at PUT. The Terraform
/// provider's stable-state waiter compares this against its schema default
/// (`all_storage_classes_128K`) — omitting the header makes the waiter loop
/// indefinitely.
const LIFECYCLE_TDMOS_DEFAULT: &str = "all_storage_classes_128K";

fn insert_tdmos_header(headers: &mut HeaderMap, value: Option<&str>) {
    let v = value.unwrap_or(LIFECYCLE_TDMOS_DEFAULT);
    if let Ok(parsed) = v.parse() {
        headers.insert(LIFECYCLE_TDMOS_HEADER, parsed);
    }
}

mod accelerate;
mod analytics;
mod bucket_inventory;
mod cors;
pub(crate) use cors::validate_cors_xml;
mod encryption;
pub(crate) use encryption::validate_bucket_encryption_xml;
mod intelligent_tiering;
mod lifecycle;
mod logging;
mod metrics;
mod notification;
mod ownership;
mod policy;
mod public_access;
mod replication;
mod request_payment;
mod tagging;
mod versioning;
mod website;

impl S3Service {
    // ---- ObjectLockConfiguration ----

    pub(super) fn put_object_lock_config(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("").to_string();

        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        // Versioning must be enabled. Checked before the body: S3 answers an
        // unversioned bucket with InvalidBucketState even when the body is
        // empty (recorded).
        if b.versioning.as_deref() != Some("Enabled") {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "InvalidBucketState",
                "Versioning must be 'Enabled' on the bucket to apply a Object Lock configuration",
            ));
        }

        // Validate: body must not be empty
        if body_str.trim().is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingRequestBodyError",
                "Request Body is empty",
            ));
        }

        // Must contain ObjectLockEnabled
        if !body_str.contains("<ObjectLockEnabled>") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                "The XML you provided was not well-formed or did not validate against our published schema",
            ));
        }
        validate_default_retention(&body_str)?;

        b.object_lock_config = Some(body_str.clone());
        self.store
            .put_bucket_subresource(bucket, BucketSubresource::ObjectLock, &body_str)
            .map_err(crate::service::persistence_error)?;
        Ok(empty_response(StatusCode::OK))
    }

    // ---- Bucket ACL ----

    pub(super) fn get_bucket_acl(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let __empty = crate::state::S3State::new(account_id, "us-east-1");
        let state = accts.get(account_id).unwrap_or(&__empty);
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        let body = if ownership_xml_enforced(b.ownership_controls.as_deref()) {
            build_acl_xml(
                &b.acl_owner_id,
                &owner_full_control(&b.acl_owner_id),
                &req.account_id,
            )
        } else {
            build_acl_xml(&b.acl_owner_id, &b.acl_grants, &req.account_id)
        };
        Ok(s3_xml(StatusCode::OK, body))
    }

    pub(super) fn put_bucket_acl(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        // An unrecognized canned value used to fall through `canned_acl_grants`'
        // catch-all to owner-only, silently stripping every public grant -- and
        // that wipe is now written to `acl.toml`, so it outlives the process.
        //
        // Checked before the bucket is resolved, matching PutObjectAcl and S3
        // itself: a bad header is an InvalidArgument whether or not the target
        // exists.
        let canned = req
            .headers
            .get("x-amz-acl")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        if let Some(acl) = canned.as_deref() {
            if !BUCKET_CANNED_ACLS.contains(&acl) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    format!("Invalid x-amz-acl value: {acl}"),
                ));
            }
        }
        reject_conflicting_acl_sources(canned.as_deref(), &req.headers, &req.body)?;

        // BucketOwnerEnforced disables ACLs on this bucket entirely;
        // any ACL-mutating call rejects with
        // `AccessControlListNotSupported` so the caller can't write a
        // grant the bucket would silently ignore.
        if self.bucket_owner_enforced(account_id, bucket) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AccessControlListNotSupported",
                "The bucket does not allow ACLs",
            ));
        }

        let pab = self.pab_flags(account_id, bucket);
        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;

        // Canned header, else the `x-amz-grant-*` headers, else the XML body --
        // the precedence PutObjectAcl uses. Without the grant-header arm a
        // `put-bucket-acl --grant-read uri=...` request fell through to an
        // empty body and wiped the bucket's ACL to nothing.
        let proposed_grants = if let Some(acl) = &canned {
            canned_acl_grants(acl, &b.acl_owner_id.clone())
        } else if has_grant_headers(&req.headers) {
            resolved_grant_headers(&req.headers)?
        } else {
            let body_str = std::str::from_utf8(&req.body).unwrap_or("");
            parse_acl_xml(body_str)?
        };
        // A request that names no ACL at all is malformed rather than "remove
        // every grant". An explicit `<AccessControlList/>` IS the documented
        // way to drop all grants, so zero grants on its own is not an error --
        // but a body that is not an AccessControlPolicy at all (whitespace,
        // JSON, a misspelled root) parses to zero grants too, and taking that
        // as "drop everything" wipes the bucket's ACL durably.
        if canned.is_none() && !has_grant_headers(&req.headers) {
            let body_str = std::str::from_utf8(&req.body).unwrap_or("");
            if body_str.trim().is_empty() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "MalformedACLError",
                    "The XML you provided was not well-formed or did not validate against our published schema",
                ));
            }
            if !body_str.contains("<AccessControlPolicy") {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "MalformedXML",
                    "The XML you provided was not well-formed or did not validate against our published schema",
                ));
            }
        }

        // PublicAccessBlock.BlockPublicAcls rejects any new grant that
        // would expose the bucket to AllUsers / AuthenticatedUsers,
        // whether sourced via canned ACL header, x-amz-grant-* headers,
        // or AccessControlPolicy XML body.
        if let Some(flags) = pab {
            if flags.block_public_acls && grants_are_public(&proposed_grants) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::FORBIDDEN,
                    "AccessDenied",
                    "User is not authorized to perform: s3:PutBucketAcl. Reason: Public Access Block (BlockPublicAcls)",
                ));
            }
        }
        // Serialize AND persist before touching memory, so a failure at either
        // step returns an error having changed nothing. Assigning first would
        // leave the in-memory ACL ahead of the one on disk, and a restart would
        // then silently revert what the caller was told had failed.
        //
        // Never fall back to an empty document either: the loader takes the
        // presence of `acl.toml` to mean the bucket has an explicit ACL and
        // skips the default owner grant, so an empty file would restore the
        // bucket with no grants at all -- worse than no file.
        let snap = AclSnapshot {
            owner_id: b.acl_owner_id.clone(),
            grants: proposed_grants.iter().map(AclGrantSnapshot::from).collect(),
        };
        let payload = toml::to_string(&snap).map_err(|e| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                format!("failed to serialize bucket ACL: {e}"),
            )
        })?;
        self.store
            .put_bucket_subresource(bucket, BucketSubresource::Acl, &payload)
            .map_err(crate::service::persistence_error)?;
        b.acl_grants = proposed_grants;
        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/xml".to_string(),
            body: Bytes::new().into(),
            headers: HeaderMap::new(),
        })
    }
    pub(super) fn get_object_lock_configuration(
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
        match &b.object_lock_config {
            Some(config) => Ok(s3_xml(StatusCode::OK, config.clone())),
            None => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ObjectLockConfigurationNotFoundError",
                "Object Lock configuration does not exist for this bucket",
            )),
        }
    }

    /// The canonical user the bucket's ACL is owned by, which is who "bucket
    /// owner full control" means. Usually the caller, but not always: a bucket
    /// persisted by one account is hydrated into the configured default account
    /// on restart while keeping its own stored `acl_owner_id`.
    ///
    /// `None` when the bucket is not in memory. That is not an error here, and
    /// callers may fall back to the caller's own id: every consumer of this pairs
    /// it with [`S3Service::bucket_owner_enforced`], which returns `false` for
    /// exactly the same two misses, so the fallback value is never the one an
    /// answer turns on -- and the missing bucket is reported by the operation
    /// itself.
    pub(super) fn bucket_acl_owner_id(&self, account_id: &str, bucket: &str) -> Option<String> {
        let accts = self.state.read();
        let state = accts.get(account_id)?;
        let b = state.buckets.get(bucket)?;
        Some(b.acl_owner_id.clone())
    }

    /// Returns true when the bucket has
    /// `ObjectOwnership=BucketOwnerEnforced` set in its
    /// `OwnershipControls` configuration. Under that mode AWS
    /// disables ACLs entirely — every ACL-mutating call must reject
    /// with `AccessControlListNotSupported` so callers don't
    /// silently no-op against a bucket that ignores their grants.
    pub(super) fn bucket_owner_enforced(&self, account_id: &str, bucket: &str) -> bool {
        let accts = self.state.read();
        let Some(state) = accts.get(account_id) else {
            return false;
        };
        let Some(b) = state.buckets.get(bucket) else {
            return false;
        };
        ownership_xml_enforced(b.ownership_controls.as_deref())
    }

    // ---- ABAC ----

    pub(super) fn put_bucket_abac(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("").to_string();
        {
            let mut accts = self.state.write();
            let state = accts.get_or_create(account_id);
            let b = state
                .buckets
                .get_mut(bucket)
                .ok_or_else(|| no_such_bucket(bucket))?;
            b.abac_config = Some(body_str.clone());
        }
        self.store
            .put_bucket_subresource(bucket, BucketSubresource::Abac, &body_str)
            .map_err(crate::service::persistence_error)?;
        Ok(empty_response(StatusCode::OK))
    }
    pub(super) fn get_bucket_abac(
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
        let body = b
            .abac_config
            .clone()
            .unwrap_or_else(|| "<BucketAbacConfiguration/>".to_string());
        Ok(s3_xml(StatusCode::OK, body))
    }

    // ---- Metadata configurations ----

    pub(super) fn create_bucket_metadata_config(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("").to_string();
        {
            let mut accts = self.state.write();
            let state = accts.get_or_create(account_id);
            let b = state
                .buckets
                .get_mut(bucket)
                .ok_or_else(|| no_such_bucket(bucket))?;
            b.metadata_configuration = Some(body_str.clone());
        }
        self.store
            .put_bucket_subresource(bucket, BucketSubresource::MetadataConfiguration, &body_str)
            .map_err(crate::service::persistence_error)?;
        Ok(empty_response(StatusCode::OK))
    }
    pub(super) fn get_bucket_metadata_config(
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
        let body = b
            .metadata_configuration
            .clone()
            .unwrap_or_else(|| "<GetBucketMetadataConfigurationResult/>".to_string());
        Ok(s3_xml(StatusCode::OK, body))
    }
    pub(super) fn delete_bucket_metadata_config(
        &self,
        account_id: &str,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        {
            let mut accts = self.state.write();
            let state = accts.get_or_create(account_id);
            let b = state
                .buckets
                .get_mut(bucket)
                .ok_or_else(|| no_such_bucket(bucket))?;
            b.metadata_configuration = None;
        }
        self.store
            .delete_bucket_subresource(bucket, BucketSubresource::MetadataConfiguration)
            .map_err(crate::service::persistence_error)?;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }

    pub(super) fn create_bucket_metadata_table_config(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("").to_string();
        {
            let mut accts = self.state.write();
            let state = accts.get_or_create(account_id);
            let b = state
                .buckets
                .get_mut(bucket)
                .ok_or_else(|| no_such_bucket(bucket))?;
            b.metadata_table_configuration = Some(body_str.clone());
        }
        self.store
            .put_bucket_subresource(
                bucket,
                BucketSubresource::MetadataTableConfiguration,
                &body_str,
            )
            .map_err(crate::service::persistence_error)?;
        Ok(empty_response(StatusCode::OK))
    }
    pub(super) fn get_bucket_metadata_table_config(
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
        let body = b
            .metadata_table_configuration
            .clone()
            .unwrap_or_else(|| "<GetBucketMetadataTableConfigurationResult/>".to_string());
        Ok(s3_xml(StatusCode::OK, body))
    }
    pub(super) fn delete_bucket_metadata_table_config(
        &self,
        account_id: &str,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        {
            let mut accts = self.state.write();
            let state = accts.get_or_create(account_id);
            let b = state
                .buckets
                .get_mut(bucket)
                .ok_or_else(|| no_such_bucket(bucket))?;
            b.metadata_table_configuration = None;
        }
        self.store
            .delete_bucket_subresource(bucket, BucketSubresource::MetadataTableConfiguration)
            .map_err(crate::service::persistence_error)?;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }
    pub(super) fn update_bucket_metadata_inventory_table(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("").to_string();
        let combined = {
            let mut accts = self.state.write();
            let state = accts.get_or_create(account_id);
            let b = state
                .buckets
                .get_mut(bucket)
                .ok_or_else(|| no_such_bucket(bucket))?;
            // Composite metadata configuration: append/replace inventory table block.
            let combined = match b.metadata_configuration.as_deref() {
                Some(prev) => format!("{prev}\n<InventoryTable>{body_str}</InventoryTable>"),
                None => format!("<InventoryTable>{body_str}</InventoryTable>"),
            };
            b.metadata_configuration = Some(combined.clone());
            combined
        };
        // Persist the updated composite configuration the same way the create
        // path does, so the inventory-table block survives a restart instead
        // of living only in RAM.
        self.store
            .put_bucket_subresource(bucket, BucketSubresource::MetadataConfiguration, &combined)
            .map_err(crate::service::persistence_error)?;
        Ok(empty_response(StatusCode::OK))
    }
    pub(super) fn update_bucket_metadata_journal_table(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("").to_string();
        let combined = {
            let mut accts = self.state.write();
            let state = accts.get_or_create(account_id);
            let b = state
                .buckets
                .get_mut(bucket)
                .ok_or_else(|| no_such_bucket(bucket))?;
            let combined = match b.metadata_configuration.as_deref() {
                Some(prev) => format!("{prev}\n<JournalTable>{body_str}</JournalTable>"),
                None => format!("<JournalTable>{body_str}</JournalTable>"),
            };
            b.metadata_configuration = Some(combined.clone());
            combined
        };
        // Persist the updated composite configuration (see the inventory-table
        // sibling above) so the journal-table block survives a restart.
        self.store
            .put_bucket_subresource(bucket, BucketSubresource::MetadataConfiguration, &combined)
            .map_err(crate::service::persistence_error)?;
        Ok(empty_response(StatusCode::OK))
    }

    // ---- ListDirectoryBuckets / CreateSession (S3 Express) ----

    pub(super) fn list_directory_buckets(
        &self,
        account_id: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // The Smithy model constrains `max-directory-buckets` to the
        // range [0, 1000]; values outside that range are an
        // InvalidArgument from real S3 Express.
        if let Some(raw) = req.query_params.get("max-directory-buckets") {
            match raw.parse::<i64>() {
                Ok(v) if (0..=1000).contains(&v) => {}
                _ => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidArgument",
                        format!("max-directory-buckets must be between 0 and 1000 (was {raw})"),
                    ));
                }
            }
        }
        // This listing never hands out a continuation token, so any token is
        // one S3 did not issue. Recorded against S3: a token of at most 1024
        // characters is 400 InvalidRequest "Invalid Token", a longer one 400
        // InvalidRequest "Request invalid".
        if let Some(token) = req
            .query_params
            .get("continuation-token")
            .filter(|t| !t.is_empty())
        {
            let message = if token.chars().count() > 1024 {
                "Request invalid"
            } else {
                "Invalid Token"
            };
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                message,
            ));
        }
        // S3 Express directory buckets are not modeled separately in
        // fakecloud; return an empty list per the documented schema so
        // SDK calls succeed.
        let _ = account_id;
        let body = "<ListDirectoryBucketsResult><Buckets/><ContinuationToken/></ListDirectoryBucketsResult>".to_string();
        Ok(s3_xml(StatusCode::OK, body))
    }

    pub(super) fn create_session(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        // A session is for a bucket that exists: S3 answers CreateSession
        // for a missing one with NoSuchBucket (recorded).
        if !self
            .state
            .read()
            .get(account_id)
            .is_some_and(|s| s.buckets.contains_key(bucket))
        {
            return Err(no_such_bucket(bucket));
        }
        // The session's default encryption: AES256 or aws:kms. Anything
        // else is refused the way PutObject refuses an unknown method.
        if let Some(sse) = req
            .headers
            .get("x-amz-server-side-encryption")
            .and_then(|v| v.to_str().ok())
        {
            if sse != "AES256" && sse != "aws:kms" {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    "The encryption method specified is not supported",
                ));
            }
        }
        // Issue ephemeral credentials scoped to the directory bucket. These
        // are not usable for actual SigV4 in the emulator (the S3 Express
        // session-token flow isn't enforced) but the response shape is
        // what SDKs expect.
        let body = format!(
            "<CreateSessionResult><Credentials><AccessKeyId>FAKEACCESSKEY</AccessKeyId><SecretAccessKey>FAKESECRET</SecretAccessKey><SessionToken>FAKESESSION-{}</SessionToken><Expiration>2099-01-01T00:00:00Z</Expiration></Credentials></CreateSessionResult>",
            xml_escape(bucket)
        );
        Ok(s3_xml(StatusCode::OK, body))
    }

    // ---- Object-level extras ----

    pub(super) fn rename_object(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        // RenameObject is an S3 Express op: source key is taken from the
        // x-amz-rename-source header; destination is the request URI key.
        let source_key = req
            .headers
            .get("x-amz-rename-source")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    "x-amz-rename-source header is required for RenameObject.",
                )
            })?
            .trim_start_matches('/')
            .to_string();
        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let obj = b.objects.remove(&source_key).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchKey",
                format!("Source key {source_key} does not exist."),
            )
        })?;
        let src_version = obj.version_id.clone();
        b.objects.insert(key.to_string(), obj);

        // Persist the move through the store: write the object under the new
        // key and delete the old key. Without this the rename lived only in
        // memory and reverted to the old key on restart in disk mode.
        let (meta, body_bytes) = {
            let o = state
                .buckets
                .get(bucket)
                .and_then(|bb| bb.objects.get(key))
                .ok_or_else(|| no_such_bucket(bucket))?;
            let bytes = state
                .read_body(&o.body)
                .map_err(crate::service::io_to_aws)?;
            (object_meta_snapshot(o), bytes)
        };
        let new_body = crate::service::objects::run_blocking_io(|| {
            self.store.put_object(
                bucket,
                key,
                meta.version_id.as_deref(),
                fakecloud_persistence::BodySource::Bytes(body_bytes),
                &meta,
            )
        })
        .map_err(crate::service::persistence_error)?;
        // Annotations travel with the object; they are stored per record, not
        // in its metadata.
        if let Some(o) = state.buckets.get(bucket).and_then(|bb| bb.objects.get(key)) {
            for a in o.annotations.values() {
                self.store
                    .put_annotation(
                        bucket,
                        key,
                        meta.version_id.as_deref(),
                        &crate::persistence::annotation_record(a),
                        &a.payload,
                    )
                    .map_err(crate::service::persistence_error)?;
            }
        }
        self.store
            .delete_object(bucket, &source_key, src_version.as_deref())
            .map_err(crate::service::persistence_error)?;
        if let Some(o) = state
            .buckets
            .get_mut(bucket)
            .and_then(|bb| bb.objects.get_mut(key))
        {
            o.body = new_body;
        }
        Ok(empty_response(StatusCode::OK))
    }

    pub(super) fn update_object_encryption(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        // The new encryption is the request body's `ObjectEncryption`
        // (S3 API reference, UpdateObjectEncryption: `<SSE-KMS>` with
        // `KMSKeyArn` and `BucketKeyEnabled`), the only form the API has. An
        // empty body is MissingRequestBodyError and a body that is not an
        // `ObjectEncryption` document MalformedXML (both recorded).
        if req.body.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingRequestBodyError",
                "Request Body is empty",
            ));
        }
        let body = std::str::from_utf8(&req.body).map_err(|_| update_encryption_malformed_xml())?;
        let encryption = parse_object_encryption(body)?;
        // `BucketKeyEnabled` "defaults to false" (the model's SSEKMSEncryption).
        let new_bucket_key = encryption.bucket_key_enabled.unwrap_or(false);
        let version_id = req.query_params.get("versionId").cloned();

        // Snapshot what we need to crunch the body re-encryption
        // outside the write lock, so the encrypt/decrypt hooks (which
        // re-enter KMS state) don't deadlock against an outstanding
        // S3 write lock.
        let (existing_bytes, snapshot, bucket_region) = {
            let accts = self.state.read();
            let __empty = crate::state::S3State::new(account_id, "us-east-1");
            let state = accts.get(account_id).unwrap_or(&__empty);
            // UpdateObjectEncryption only declares AccessDenied / InvalidRequest
            // / NoSuchKey per the Smithy model; collapse missing-bucket into
            // NoSuchKey for strict conformance.
            let b = state.buckets.get(bucket).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "NoSuchKey",
                    format!("Key {key} does not exist."),
                )
            })?;
            let obj = target_version(b, key, version_id.as_deref())?;
            check_update_object_encryption(
                obj.sse_algorithm.as_deref(),
                Some(&encryption.kms_key_arn),
                account_id,
            )?;
            check_not_object_locked(obj, chrono::Utc::now())?;
            let bytes = state
                .read_body(&obj.body)
                .map_err(crate::service::io_to_aws)?;
            (bytes, VersionSnapshot::of(obj), b.region.clone())
        };

        // The object records the ARN of the key it is sealed under (resolved
        // in the bucket's region), as PutObject does.
        let new_kms_key_id = self
            .resolve_object_kms_key(
                account_id,
                &bucket_region,
                Some(encryption.kms_key_arn.clone()),
            )
            .unwrap_or_else(|| encryption.kms_key_arn.clone());

        // The body is resealed when the object moves to SSE-KMS, to another
        // KMS key, or to another S3 Bucket Key setting (which changes the
        // encryption context: the bucket's ARN instead of the object's):
        // recording the new key without resealing would leave the body under
        // the old one, unreadable once that key is disabled, deleted or
        // denied. Otherwise only metadata changes.
        let old_kms = snapshot.sse_algorithm.as_deref() == Some("aws:kms");
        let reseal = !old_kms
            || snapshot.sse_kms_key_id.as_deref() != Some(new_kms_key_id.as_str())
            || snapshot.bucket_key != new_bucket_key;
        let new_bytes = if reseal {
            let plaintext = if old_kms {
                self.decrypt_object_body(
                    req,
                    account_id,
                    bucket,
                    key,
                    snapshot.bucket_key,
                    &existing_bytes,
                    snapshot.sse_kms_key_id.as_deref(),
                )?
            } else {
                existing_bytes
            };
            Some(self.encrypt_object_body(
                req,
                account_id,
                &bucket_region,
                bucket,
                key,
                new_bucket_key,
                &plaintext,
                Some(&new_kms_key_id),
            )?)
        } else {
            None
        };

        // Commit under the write lock, to the version the snapshot read: a
        // write that replaced it meanwhile, or an Object Lock placed on it,
        // wins over this request.
        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        let mut updated = {
            let current = target_version(b, key, version_id.as_deref())
                .ok()
                .filter(|obj| snapshot.is(obj))
                .ok_or_else(update_encryption_conflict)?;
            check_not_object_locked(current, chrono::Utc::now())?;
            current.clone()
        };
        updated.sse_algorithm = Some("aws:kms".to_string());
        updated.sse_kms_key_id = Some(new_kms_key_id.clone());
        updated.bucket_key_enabled = Some(new_bucket_key);
        // Persist first, from the updated copy, and change memory only once
        // the store took it: a store failure leaves the object as it was in
        // both, as commit_object_version_with does. The slot is the
        // version's own; resealed, the same version with new bytes keeps its
        // annotations (AWS: the operation "preserves all object metadata
        // properties").
        let meta = object_meta_snapshot(&updated);
        let slot = snapshot.version_id.as_deref();
        let new_body = match new_bytes {
            None => {
                self.store
                    .put_object_meta(bucket, key, slot, &meta)
                    .map_err(crate::service::persistence_error)?;
                None
            }
            Some(new_bytes) => Some(
                crate::service::objects::run_blocking_io(|| {
                    self.store.replace_object_body(
                        bucket,
                        key,
                        slot,
                        fakecloud_persistence::BodySource::Bytes(new_bytes),
                        &meta,
                    )
                })
                .map_err(crate::service::persistence_error)?,
            ),
        };
        for_each_copy(b, key, &snapshot, |obj| {
            obj.sse_algorithm = updated.sse_algorithm.clone();
            obj.sse_kms_key_id = updated.sse_kms_key_id.clone();
            obj.bucket_key_enabled = updated.bucket_key_enabled;
            if let Some(body) = &new_body {
                obj.body = body.clone();
            }
        });
        Ok(empty_response(StatusCode::OK))
    }

    pub(super) fn get_object_torrent(
        &self,
        account_id: &str,
        _req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let empty = crate::state::S3State::new(account_id, "us-east-1");
        let state = accts.get(account_id).unwrap_or(&empty);
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        if !b.objects.contains_key(key) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchKey",
                format!("Key {key} does not exist."),
            ));
        }
        // Return a stub torrent file body. Real S3 disabled torrent in
        // 2024; keep an honest tiny payload here so callers see the route
        // is wired.
        let body = b"d8:announce0:e".to_vec();
        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/x-bittorrent".to_string(),
            body: Bytes::from(body).into(),
            headers: HeaderMap::new(),
        })
    }

    pub(super) fn select_object_content(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("");
        let request: crate::select::SelectRequest =
            quick_xml::de::from_str(body_str).map_err(|e| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "MalformedXML",
                    format!("Invalid SelectObjectContent request: {e}"),
                )
            })?;

        if request.ExpressionType != "SQL" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidExpressionType",
                "Only SQL expressions are supported",
            ));
        }

        let codec = match request.InputSerialization.compression_type.as_deref() {
            None => crate::compression::Codec::None,
            Some(name) => match crate::compression::Codec::from_name(name) {
                Some(
                    c @ (crate::compression::Codec::None
                    | crate::compression::Codec::Gzip
                    | crate::compression::Codec::Bzip2),
                ) => c,
                _ => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidCompressionFormat",
                        format!("The file is not in a supported compression format: {name}"),
                    ))
                }
            },
        };

        // Resolve the object and open its body under the read guard, then
        // drop the guard before reading: the whole object is scanned, and
        // holding the global S3 lock for that would block every writer.
        let handle = {
            let accts = self.state.read();
            let empty = crate::state::S3State::new(account_id, "us-east-1");
            let state = accts.get(account_id).unwrap_or(&empty);
            let b = state
                .buckets
                .get(bucket)
                .ok_or_else(|| no_such_bucket(bucket))?;
            let obj = b.objects.get(key).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "NoSuchKey",
                    format!("Key {key} does not exist."),
                )
            })?;
            crate::body_io::ObjectBodyHandle::open(&obj.body).map_err(|e| {
                AwsServiceError::aws_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "ServiceException",
                    format!("Failed to read object body: {e}"),
                )
            })?
        };
        let raw_bytes = crate::service::run_blocking_io(|| handle.read_all()).map_err(|e| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ServiceException",
                format!("Failed to read object body: {e}"),
            )
        })?;
        // BytesScanned counts the stored (compressed) bytes, as on AWS.
        let bytes_scanned = raw_bytes.len() as u64;
        let object_bytes = if codec == crate::compression::Codec::None {
            raw_bytes
        } else {
            bytes::Bytes::from(
                crate::compression::decompress(codec, &raw_bytes).map_err(|_| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidCompressionFormat",
                        "The file is not in a supported compression format.",
                    )
                })?,
            )
        };

        let query = crate::select::parse_sql(&request.Expression).map_err(|e| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidExpression",
                format!("Failed to parse SQL expression: {e}"),
            )
        })?;

        if !query.from.eq_ignore_ascii_case("s3object") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidExpression",
                "Only FROM s3object is supported",
            ));
        }

        // Parse input
        let (headers, rows) = if let Some(csv_input) = request.InputSerialization.CSV {
            let has_header = csv_input.file_header_info.as_deref() == Some("USE");
            let field_delimiter = csv_input
                .field_delimiter
                .as_deref()
                .and_then(|s| s.chars().next())
                .unwrap_or(',');
            let record_delimiter = csv_input
                .record_delimiter
                .as_deref()
                .and_then(|s| s.chars().next())
                .unwrap_or('\n');
            crate::select::parse_csv(&object_bytes, has_header, field_delimiter, record_delimiter)
        } else if request.InputSerialization.JSON.is_some() {
            crate::select::parse_json_lines(&object_bytes)
        } else {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Only CSV and JSON input are supported",
            ));
        };

        // Evaluate query
        let (result_rows, out_headers) = crate::select::evaluate_query(&query, &headers, &rows);

        // Format output
        let output_bytes = if let Some(csv_output) = request.OutputSerialization.CSV {
            let fd = csv_output.field_delimiter.as_deref().unwrap_or(",");
            let rd = csv_output.record_delimiter.as_deref().unwrap_or("\n");
            crate::select::format_csv(&result_rows, fd, rd)
        } else if request.OutputSerialization.JSON.is_some() {
            let json_headers = out_headers.or(headers);
            crate::select::format_json_lines(&result_rows, &json_headers)
        } else {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Only CSV and JSON output are supported",
            ));
        };

        // Build eventstream response
        let mut body = Vec::new();
        body.extend(crate::eventstream::records_event_frame(&output_bytes));
        let bytes_processed = object_bytes.len() as u64;
        let bytes_returned = output_bytes.len() as u64;
        body.extend(crate::eventstream::stats_event_frame(
            bytes_scanned,
            bytes_processed,
            bytes_returned,
        ));
        body.extend(crate::eventstream::end_event_frame());

        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/vnd.amazon.eventstream".to_string(),
            body: body.into(),
            headers: HeaderMap::new(),
        })
    }

    pub(super) fn write_get_object_response(
        &self,
        account_id: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let route = req
            .headers
            .get("x-amz-request-route")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequest",
                    "x-amz-request-route header is required",
                )
            })?;

        let token = req
            .headers
            .get("x-amz-request-token")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequest",
                    "x-amz-request-token header is required",
                )
            })?;

        let fwd_status = req
            .headers
            .get("x-amz-fwd-status")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok());

        let fwd_error_message = req
            .headers
            .get("x-amz-fwd-error-message")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let content_type = req
            .headers
            .get("x-amz-fwd-header-Content-Type")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let encryption = req
            .headers
            .get("x-amz-server-side-encryption")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let kms_key_id = req
            .headers
            .get("x-amz-server-side-encryption-aws-kms-key-id")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let metadata = crate::service::extract_user_metadata(&req.headers);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        state.object_lambda_responses.insert(
            token.clone(),
            crate::state::ObjectLambdaResponse {
                route,
                token,
                body: req.body.to_vec(),
                content_type,
                fwd_status,
                fwd_error_message,
                metadata,
                encryption,
                kms_key_id,
                stored_at: chrono::Utc::now(),
            },
        );

        Ok(empty_response(StatusCode::OK))
    }
}

// ── Shared helpers for analytics/intelligent-tiering/metrics named configs ──

#[derive(Clone, Copy)]
enum ConfigKind {
    Analytics,
    IntelligentTiering,
    Metrics,
}

impl ConfigKind {
    fn list_root(&self) -> &'static str {
        match self {
            ConfigKind::Analytics => "ListBucketAnalyticsConfigurationResult",
            ConfigKind::IntelligentTiering => "ListBucketIntelligentTieringConfigurationsOutput",
            ConfigKind::Metrics => "ListMetricsConfigurationsResult",
        }
    }
    fn subresource(&self) -> BucketSubresource {
        match self {
            ConfigKind::Analytics => BucketSubresource::Analytics,
            ConfigKind::IntelligentTiering => BucketSubresource::IntelligentTiering,
            ConfigKind::Metrics => BucketSubresource::Metrics,
        }
    }
}

fn config_map(
    bucket: &mut crate::state::S3Bucket,
    kind: ConfigKind,
) -> &mut std::collections::BTreeMap<String, String> {
    match kind {
        ConfigKind::Analytics => &mut bucket.analytics_configs,
        ConfigKind::IntelligentTiering => &mut bucket.intelligent_tiering_configs,
        ConfigKind::Metrics => &mut bucket.metrics_configs,
    }
}

fn config_map_ref(
    bucket: &crate::state::S3Bucket,
    kind: ConfigKind,
) -> &std::collections::BTreeMap<String, String> {
    match kind {
        ConfigKind::Analytics => &bucket.analytics_configs,
        ConfigKind::IntelligentTiering => &bucket.intelligent_tiering_configs,
        ConfigKind::Metrics => &bucket.metrics_configs,
    }
}

fn store_named_config(
    svc: &S3Service,
    account_id: &str,
    req: &AwsRequest,
    bucket: &str,
    kind: ConfigKind,
) -> Result<AwsResponse, AwsServiceError> {
    let id = req.query_params.get("id").cloned().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
            "Missing required query parameter: id",
        )
    })?;
    let body_str = std::str::from_utf8(&req.body).unwrap_or("").to_string();
    let payload = {
        let mut accts = svc.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        config_map(b, kind).insert(id, body_str);
        crate::service::toml_or_internal_error(config_map(b, kind))?
    };
    svc.store
        .put_bucket_subresource(bucket, kind.subresource(), &payload)
        .map_err(crate::service::persistence_error)?;
    Ok(empty_response(StatusCode::OK))
}

/// S3's message for a configuration id the bucket does not have (recorded).
const NO_SUCH_CONFIGURATION_MESSAGE: &str = "The specified configuration does not exist.";

pub(super) fn no_such_configuration() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "NoSuchConfiguration",
        NO_SUCH_CONFIGURATION_MESSAGE,
    )
}

/// The `id` of a Delete{Analytics,Metrics,Inventory,IntelligentTiering}
/// configuration request. Recorded against S3: without one, the request is
/// 400 InvalidConfigurationId; an id the bucket does not have is 404
/// NoSuchConfiguration.
pub(super) fn delete_config_id(req: &AwsRequest) -> Result<String, AwsServiceError> {
    req.query_params.get("id").cloned().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidConfigurationId",
            "The specified configuration id is invalid.",
        )
    })
}

fn get_named_config(
    svc: &S3Service,
    account_id: &str,
    req: &AwsRequest,
    bucket: &str,
    kind: ConfigKind,
) -> Result<AwsResponse, AwsServiceError> {
    let id = req.query_params.get("id").cloned().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
            "Missing required query parameter: id",
        )
    })?;
    let accts = svc.state.read();
    let empty = crate::state::S3State::new(account_id, "us-east-1");
    let state = accts.get(account_id).unwrap_or(&empty);
    let b = state
        .buckets
        .get(bucket)
        .ok_or_else(|| no_such_bucket(bucket))?;
    let body = config_map_ref(b, kind).get(&id).cloned().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "NoSuchConfiguration",
            NO_SUCH_CONFIGURATION_MESSAGE,
        )
    })?;
    Ok(s3_xml(StatusCode::OK, body))
}

fn delete_named_config(
    svc: &S3Service,
    account_id: &str,
    req: &AwsRequest,
    bucket: &str,
    kind: ConfigKind,
) -> Result<AwsResponse, AwsServiceError> {
    let id = delete_config_id(req)?;
    let (empty, payload) = {
        let mut accts = svc.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        if config_map(b, kind).remove(&id).is_none() {
            return Err(no_such_configuration());
        }
        let map = config_map(b, kind);
        let is_empty = map.is_empty();
        // Not `unwrap_or_default`: writing an empty document in the non-empty
        // branch would drop the configurations that remain.
        (is_empty, crate::service::toml_or_internal_error(map)?)
    };
    if empty {
        svc.store
            .delete_bucket_subresource(bucket, kind.subresource())
            .map_err(crate::service::persistence_error)?;
    } else {
        svc.store
            .put_bucket_subresource(bucket, kind.subresource(), &payload)
            .map_err(crate::service::persistence_error)?;
    }
    Ok(empty_response(StatusCode::NO_CONTENT))
}

fn list_named_config(
    svc: &S3Service,
    account_id: &str,
    bucket: &str,
    kind: ConfigKind,
) -> Result<AwsResponse, AwsServiceError> {
    let accts = svc.state.read();
    let empty = crate::state::S3State::new(account_id, "us-east-1");
    let state = accts.get(account_id).unwrap_or(&empty);
    let b = state
        .buckets
        .get(bucket)
        .ok_or_else(|| no_such_bucket(bucket))?;
    // Stored entries already include the per-config wrapper element
    // (e.g. <AnalyticsConfiguration>...</AnalyticsConfiguration>), so
    // emit them directly. Wrapping them in another <Member> here would
    // produce a nested element list that breaks SDK parsing.
    let entries: Vec<String> = config_map_ref(b, kind).values().cloned().collect();
    let body = format!(
        "<{root}>{entries}<IsTruncated>false</IsTruncated></{root}>",
        root = kind.list_root(),
        entries = entries.join(""),
    );
    Ok(s3_xml(StatusCode::OK, body))
}

/// S3's bounds on an Object Lock default retention period: `Days` 1-36500 or
/// `Years` 1-100, never both. An unbounded value would otherwise be applied to
/// every later PutObject's retain-until date.
fn validate_default_retention(body: &str) -> Result<(), AwsServiceError> {
    let days = extract_xml_value(body, "Days");
    let years = extract_xml_value(body, "Years");
    if days.is_some() && years.is_some() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against our published schema",
        ));
    }
    for (value, max) in [(days, 36_500_i64), (years, 100)] {
        let Some(value) = value else {
            continue;
        };
        match value.trim().parse::<i64>() {
            Ok(n) if n > max => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    "Default retention period is too large.",
                ))
            }
            Ok(n) if n >= 1 => {}
            _ => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    "Default retention period must be a positive integer value.",
                ))
            }
        }
    }
    Ok(())
}

/// UpdateObjectEncryption's answer to a body that is not an
/// `ObjectEncryption` document (recorded for `<Foo/>` and `garbage`).
fn update_encryption_malformed_xml() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "MalformedXML",
        "The XML you provided was not well-formed or did not validate against our published schema",
    )
}

/// What an `UpdateObjectEncryption` body holds, read in one pass over its
/// XML: whether it is a document whose one root element is
/// `ObjectEncryption`, whether that root has an `SSE-KMS` child, and the
/// text of that child's `KMSKeyArn` and `BucketKeyEnabled`. Elements are
/// matched by local name (any namespace or prefix); comments, processing
/// instructions and the DOCTYPE are skipped, and text is unescaped with the
/// predefined XML entities and character references only (an entity the
/// DOCTYPE declares is not expanded, and makes the body malformed). `None`
/// when the body is not such a document.
#[derive(Default)]
struct ObjectEncryptionDocument {
    has_sse_kms: bool,
    kms_key_arn: Option<String>,
    bucket_key_enabled: Option<String>,
}

fn read_object_encryption_document(body: &str) -> Option<ObjectEncryptionDocument> {
    use quick_xml::events::Event;
    let body = body.strip_prefix('\u{feff}').unwrap_or(body);
    let mut reader = quick_xml::Reader::from_str(body);
    let mut path: Vec<Vec<u8>> = Vec::new();
    let mut roots = 0usize;
    let mut doc = ObjectEncryptionDocument::default();
    // The field whose text is being read, and that text so far.
    let mut field: Option<(&'static str, String)> = None;
    let field_at = |path: &[Vec<u8>]| -> Option<&'static str> {
        match path {
            [_, sse, leaf] if sse.as_slice() == b"SSE-KMS" => match leaf.as_slice() {
                b"KMSKeyArn" => Some("KMSKeyArn"),
                b"BucketKeyEnabled" => Some("BucketKeyEnabled"),
                _ => None,
            },
            _ => None,
        }
    };
    loop {
        match reader.read_event().ok()? {
            // A value field (KMSKeyArn, BucketKeyEnabled) holds text only.
            Event::Start(_) | Event::Empty(_) if field.is_some() => return None,
            Event::Start(e) => {
                let name = e.local_name().as_ref().to_vec();
                if path.is_empty() {
                    roots += 1;
                    if roots > 1 || name != b"ObjectEncryption" {
                        return None;
                    }
                }
                path.push(name);
                if path.len() == 2 && path[1] == b"SSE-KMS" {
                    doc.has_sse_kms = true;
                }
                field = field_at(&path).map(|f| (f, String::new()));
            }
            Event::Empty(e) => {
                let name = e.local_name().as_ref().to_vec();
                if path.is_empty() {
                    roots += 1;
                    if roots > 1 || name != b"ObjectEncryption" {
                        return None;
                    }
                }
                path.push(name);
                if path.len() == 2 && path[1] == b"SSE-KMS" {
                    doc.has_sse_kms = true;
                }
                if let Some(f) = field_at(&path) {
                    doc.set(f, String::new());
                }
                path.pop();
            }
            Event::End(_) => {
                if let Some((f, text)) = field.take() {
                    doc.set(f, text);
                }
                path.pop()?;
            }
            Event::Text(text) => {
                if path.is_empty() {
                    if !text.iter().all(u8::is_ascii_whitespace) {
                        return None;
                    }
                } else if let Some((_, value)) = field.as_mut() {
                    value.push_str(&text.unescape().ok()?);
                }
            }
            Event::CData(data) => {
                if path.is_empty() {
                    return None;
                }
                if let Some((_, value)) = field.as_mut() {
                    value.push_str(std::str::from_utf8(&data).ok()?);
                }
            }
            Event::Eof => break,
            // Comments, processing instructions, the XML declaration and
            // the DOCTYPE carry no content.
            _ => {}
        }
    }
    (path.is_empty() && roots == 1).then_some(doc)
}

impl ObjectEncryptionDocument {
    fn set(&mut self, field: &str, value: String) {
        match field {
            "KMSKeyArn" => self.kms_key_arn = Some(value),
            _ => self.bucket_key_enabled = Some(value),
        }
    }
}

/// S3's answer when another request changed the object while this one was
/// resealing it (Error Responses: `OperationAborted`, 409, "A conflicting
/// conditional operation is currently in progress against this resource").
fn update_encryption_conflict() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::CONFLICT,
        "OperationAborted",
        "A conflicting conditional operation is currently in progress against this resource. Try again.",
    )
}

/// The object version an `UpdateObjectEncryption` addresses: the one
/// `version_id` names (`null` for the null version), else the current one,
/// which may be a delete marker. An unknown version id is 400
/// `InvalidArgument` "Invalid version id specified" (recorded).
fn target_version<'a>(
    b: &'a crate::state::S3Bucket,
    key: &str,
    version_id: Option<&str>,
) -> Result<&'a crate::state::S3Object, AwsServiceError> {
    let Some(vid) = version_id else {
        return b.objects.get(key).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchKey",
                format!("Key {key} does not exist."),
            )
        });
    };
    let names = |obj: &crate::state::S3Object| {
        obj.version_id.as_deref() == Some(vid) || (vid == "null" && obj.version_id.is_none())
    };
    b.object_versions
        .get(key)
        .and_then(|versions| versions.iter().find(|o| names(o)))
        .or_else(|| b.objects.get(key).filter(|o| names(o)))
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Invalid version id specified",
            )
        })
}

/// What an `UpdateObjectEncryption` read of an object version, kept to
/// commit to that same version: its identity (version id, ETag, write time,
/// delete marker) and the encryption it had.
struct VersionSnapshot {
    version_id: Option<String>,
    etag: String,
    last_modified: chrono::DateTime<chrono::Utc>,
    is_delete_marker: bool,
    sse_algorithm: Option<String>,
    sse_kms_key_id: Option<String>,
    bucket_key: bool,
}

impl VersionSnapshot {
    fn of(obj: &crate::state::S3Object) -> Self {
        Self {
            version_id: obj.version_id.clone(),
            etag: obj.etag.clone(),
            last_modified: obj.last_modified,
            is_delete_marker: obj.is_delete_marker,
            sse_algorithm: obj.sse_algorithm.clone(),
            sse_kms_key_id: obj.sse_kms_key_id.clone(),
            bucket_key: obj.bucket_key_enabled == Some(true),
        }
    }

    /// Whether `obj` is still the version this snapshot read, encrypted as
    /// it was: a write that replaced it carries another write time (and
    /// usually ETag and id), and a concurrent UpdateObjectEncryption another
    /// key or Bucket Key setting, whose body this request did not reseal.
    fn is(&self, obj: &crate::state::S3Object) -> bool {
        obj.version_id == self.version_id
            && obj.etag == self.etag
            && obj.last_modified == self.last_modified
            && obj.is_delete_marker == self.is_delete_marker
            && obj.sse_algorithm == self.sse_algorithm
            && obj.sse_kms_key_id == self.sse_kms_key_id
            && (obj.bucket_key_enabled == Some(true)) == self.bucket_key
    }
}

/// Apply `f` to every copy of the version `snapshot` read: the current
/// object and its entry in the version history, which a versioned bucket
/// keeps side by side.
fn for_each_copy(
    b: &mut crate::state::S3Bucket,
    key: &str,
    snapshot: &VersionSnapshot,
    mut f: impl FnMut(&mut crate::state::S3Object),
) {
    if let Some(obj) = b.objects.get_mut(key).filter(|o| snapshot.is(o)) {
        f(obj);
    }
    if let Some(versions) = b.object_versions.get_mut(key) {
        for obj in versions.iter_mut().filter(|o| snapshot.is(o)) {
            f(obj);
        }
    }
}

/// The new encryption an `UpdateObjectEncryption` body names.
struct RequestedObjectEncryption {
    kms_key_arn: String,
    bucket_key_enabled: Option<bool>,
}

/// UpdateObjectEncryption's `InvalidRequest`, with the message the S3 API
/// reference gives for the reason.
fn update_encryption_invalid(message: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidRequest", message)
}

/// Parse an `UpdateObjectEncryption` body (`<ObjectEncryption>` holding an
/// `<SSE-KMS>` with `KMSKeyArn` and an optional `BucketKeyEnabled`; SSE-KMS
/// is the only encryption type it can set; see
/// [`read_object_encryption_document`]). A body that is not such a document
/// is MalformedXML; otherwise what the S3 API reference lists as
/// `InvalidRequest` is refused: no or another encryption type, and a
/// `BucketKeyEnabled` other than `true` / `false`. A missing key is checked
/// with the object (see [`check_update_object_encryption`]).
fn parse_object_encryption(body: &str) -> Result<RequestedObjectEncryption, AwsServiceError> {
    let doc = read_object_encryption_document(body).ok_or_else(update_encryption_malformed_xml)?;
    if !doc.has_sse_kms {
        return Err(update_encryption_invalid(
            "Requests that modify an object encryption configuration require a valid new encryption type. Valid values are SSEKMS. Modify the request to specify SSE-KMS for the updated encryption type, and then try again.",
        ));
    }
    let bucket_key_enabled = match doc.bucket_key_enabled {
        None => None,
        Some(v) => match v.trim() {
            "true" => Some(true),
            "false" => Some(false),
            _ => {
                return Err(update_encryption_invalid(
                    "The BucketKeyEnabled value isn't valid. Valid values are true or false. Modify the request to specify a valid value, and then try again.",
                ))
            }
        },
    };
    Ok(RequestedObjectEncryption {
        kms_key_arn: doc
            .kms_key_arn
            .map(|v| v.trim().to_string())
            .unwrap_or_default(),
        bucket_key_enabled,
    })
}

/// The S3 API reference's conditions on an `UpdateObjectEncryption`: the
/// object must be encrypted with SSE-S3 or SSE-KMS (`source`, its recorded
/// algorithm), the new key must be named by a KMS key ARN, and the key must
/// belong to the bucket owner's account (`bucket_account`). The key-ARN
/// refusals carry the codes and messages recorded against S3
/// (`tests/fixtures/update-object-encryption/`); the others the reference's
/// text. Whether the key exists is KMS's to say, when the object is resealed
/// (`KMS.NotFoundException`, as recorded).
fn check_update_object_encryption(
    source: Option<&str>,
    kms_key_arn: Option<&str>,
    bucket_account: &str,
) -> Result<(), AwsServiceError> {
    match source {
        Some("AES256") | Some("aws:kms") => {}
        Some(_) => {
            return Err(update_encryption_invalid(
                "The UpdateObjectEncryption operation doesn't support source objects with the encryption type DSSE-KMS or SSE-C. Only source objects encrypted with SSE-S3 or SSE-KMS are supported.",
            ))
        }
        // Recorded for a delete marker (the current version of a deleted
        // key), and taken to be the answer for any unencrypted version.
        None => {
            return Err(update_encryption_invalid(
                "Unencrypted source objects are not supported.",
            ))
        }
    }
    let arn = kms_key_arn.unwrap_or_default();
    if arn.is_empty() {
        return Err(update_encryption_invalid(
            "Requests that modify an object's encryption type to SSE-KMS require an AWS KMS key Amazon Resource Name (ARN). Modify the request to specify a KMS key ARN, and then try again.",
        ));
    }
    if !arn.starts_with("arn:") {
        return Err(update_encryption_invalid(
            "Requests modifying object encryption configuration to SSE-KMS require a valid target kms key arn: Malformed ARN - doesn't start with 'arn:'",
        ));
    }
    // Recorded for an alias ARN; any other ARN that is not a KMS key's is
    // taken to get the same answer.
    let fields: Vec<&str> = arn.splitn(6, ':').collect();
    let key_arn = match fields.as_slice() {
        ["arn", partition, "kms", region, account, resource]
            if !partition.is_empty()
                && !region.is_empty()
                && !account.is_empty()
                && resource
                    .strip_prefix("key/")
                    .is_some_and(|id| !id.is_empty()) =>
        {
            Some(*account)
        }
        _ => None,
    };
    let Some(key_account) = key_arn else {
        return Err(update_encryption_invalid(
            "Invalid KMS Key ARN format. You must provide the full KMS Key ARN to make an UpdateObjectEncryption request",
        ));
    };
    if key_account != bucket_account {
        return Err(AwsServiceError::aws_error(
            StatusCode::FORBIDDEN,
            "AccessDenied",
            "The KMS key must be from the same account as the bucket",
        ));
    }
    Ok(())
}

/// The S3 API reference's Object Lock refusal of `UpdateObjectEncryption`:
/// an object under a legal hold or an unexpired retention period (governance
/// or compliance mode) at `now` keeps its encryption, `AccessDenied` with
/// the reference's message.
fn check_not_object_locked(
    obj: &crate::state::S3Object,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(), AwsServiceError> {
    let legal_hold = obj.lock_legal_hold.as_deref() == Some("ON");
    let retained =
        obj.lock_mode.is_some() && obj.lock_retain_until.is_some_and(|until| until > now);
    if legal_hold || retained {
        return Err(AwsServiceError::aws_error(
            StatusCode::FORBIDDEN,
            "AccessDenied",
            "The encryption type for the specified object can\u{2019}t be updated because that object is protected by S3 Object Lock. If the object has a governance-mode retention period or a legal hold, you must first remove the Object Lock status on the object before you issue your UpdateObjectEncryption request. You can't use the UpdateObjectEncryption operation with objects that have an Object Lock compliance mode retention period applied to them.",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod update_object_encryption_tests {
    use super::*;

    const ARN: &str = "arn:aws:kms:us-east-1:123456789012:key/0b5f2a4e";

    fn code(result: Result<impl std::fmt::Debug, AwsServiceError>) -> String {
        result.unwrap_err().code().to_string()
    }

    /// The body is a well-formed document whose root is `ObjectEncryption`,
    /// in any namespace or prefix; anything else is MalformedXML.
    #[test]
    fn only_an_object_encryption_document_is_a_body() {
        for body in [
            "<ObjectEncryption/>",
            r#"<?xml version="1.0" encoding="UTF-8"?><ObjectEncryption xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><SSE-KMS><KMSKeyArn>a</KMSKeyArn></SSE-KMS></ObjectEncryption>"#,
            r#"<s3:ObjectEncryption xmlns:s3="http://s3.amazonaws.com/doc/2006-03-01/"></s3:ObjectEncryption>"#,
            "  <ObjectEncryption></ObjectEncryption>\n",
            "\u{feff}<ObjectEncryption/>",
        ] {
            assert!(read_object_encryption_document(body).is_some(), "{body}");
        }
        for body in [
            "",
            "garbage",
            "<Foo/>",
            "<ObjectEncryptionX/>",
            "<ObjectEncryption>",
            "<ObjectEncryption><SSE-KMS></ObjectEncryption>",
            "<ObjectEncryption></ObjectEncryption><ObjectEncryption/>",
            "<ObjectEncryption/>trailing",
            "</ObjectEncryption>",
            "<Wrapper><ObjectEncryption/></Wrapper>",
            "<ObjectEncryption><SSE-KMS><KMSKeyArn>&bogus;</KMSKeyArn></SSE-KMS></ObjectEncryption>",
            // A value field holds text only.
            "<ObjectEncryption><SSE-KMS><KMSKeyArn>arn:aws:kms:us-east-1:123456789012:key/k<x>y</x></KMSKeyArn></SSE-KMS></ObjectEncryption>",
            "<ObjectEncryption><SSE-KMS><KMSKeyArn><x/>arn</KMSKeyArn></SSE-KMS></ObjectEncryption>",
            "<ObjectEncryption><SSE-KMS><BucketKeyEnabled><b>true</b></BucketKeyEnabled></SSE-KMS></ObjectEncryption>",
        ] {
            assert!(read_object_encryption_document(body).is_none(), "{body}");
            assert_eq!(
                code(parse_object_encryption(body).map(|_| ())),
                "MalformedXML",
                "{body}"
            );
        }
    }

    /// Values are read from the elements themselves: a commented-out
    /// `KMSKeyArn` is not one, prefixed children are, a leading BOM is
    /// dropped, and entity and character references are unescaped.
    #[test]
    fn values_come_from_the_parsed_elements() {
        let parsed = |body: &str| parse_object_encryption(body).ok().unwrap();
        let commented = format!(
            "<ObjectEncryption><SSE-KMS><!-- <KMSKeyArn>arn:aws:kms:us-east-1:123456789012:key/wrong</KMSKeyArn> --><KMSKeyArn>{ARN}</KMSKeyArn></SSE-KMS></ObjectEncryption>"
        );
        assert_eq!(parsed(&commented).kms_key_arn, ARN);
        let only_commented =
            format!("<ObjectEncryption><SSE-KMS><!-- <KMSKeyArn>{ARN}</KMSKeyArn> --></SSE-KMS></ObjectEncryption>");
        assert_eq!(parsed(&only_commented).kms_key_arn, "");

        let prefixed = format!(
            r#"<s3:ObjectEncryption xmlns:s3="http://s3.amazonaws.com/doc/2006-03-01/"><s3:SSE-KMS><s3:KMSKeyArn>{ARN}</s3:KMSKeyArn><s3:BucketKeyEnabled>true</s3:BucketKeyEnabled></s3:SSE-KMS></s3:ObjectEncryption>"#
        );
        let p = parsed(&prefixed);
        assert_eq!(p.kms_key_arn, ARN);
        assert_eq!(p.bucket_key_enabled, Some(true));

        let bom = format!(
            "\u{feff}<?xml version=\"1.0\"?><ObjectEncryption><SSE-KMS><KMSKeyArn>{ARN}</KMSKeyArn></SSE-KMS></ObjectEncryption>"
        );
        assert_eq!(parsed(&bom).kms_key_arn, ARN);

        let escaped = "<ObjectEncryption><SSE-KMS><KMSKeyArn>arn:aws:kms:us-east-1:123456789012:key/a&amp;b&#47;c&#x3c;</KMSKeyArn></SSE-KMS></ObjectEncryption>";
        assert_eq!(
            parsed(escaped).kms_key_arn,
            "arn:aws:kms:us-east-1:123456789012:key/a&b/c<"
        );

        // A KMSKeyArn outside SSE-KMS is not the key.
        let misplaced = format!(
            "<ObjectEncryption><KMSKeyArn>{ARN}</KMSKeyArn><SSE-KMS></SSE-KMS></ObjectEncryption>"
        );
        assert_eq!(parsed(&misplaced).kms_key_arn, "");
        // A DOCTYPE is skipped; the entity it declares is not expanded.
        let doctype = r#"<!DOCTYPE x [<!ENTITY e "arn:aws:kms:us-east-1:123456789012:key/x">]><ObjectEncryption><SSE-KMS><KMSKeyArn>&e;</KMSKeyArn></SSE-KMS></ObjectEncryption>"#;
        assert_eq!(
            code(parse_object_encryption(doctype).map(|_| ())),
            "MalformedXML"
        );
    }

    #[test]
    fn the_body_must_name_sse_kms_and_a_boolean_bucket_key() {
        let body = |inner: &str| format!("<ObjectEncryption>{inner}</ObjectEncryption>");
        let parsed = parse_object_encryption(&body(&format!(
            "<SSE-KMS><KMSKeyArn>{ARN}</KMSKeyArn><BucketKeyEnabled>false</BucketKeyEnabled></SSE-KMS>"
        )))
        .ok()
        .unwrap();
        assert_eq!(parsed.kms_key_arn, ARN);
        assert_eq!(parsed.bucket_key_enabled, Some(false));
        let parsed = parse_object_encryption(&body(&format!(
            "<SSE-KMS><KMSKeyArn>{ARN}</KMSKeyArn></SSE-KMS>"
        )))
        .ok()
        .unwrap();
        assert_eq!(parsed.bucket_key_enabled, None);
        assert_eq!(
            code(parse_object_encryption(&body("")).map(|_| ())),
            "InvalidRequest"
        );
        assert_eq!(
            code(
                parse_object_encryption(&body(&format!(
                    "<SSE-KMS><KMSKeyArn>{ARN}</KMSKeyArn><BucketKeyEnabled>yes</BucketKeyEnabled></SSE-KMS>"
                )))
                .map(|_| ())
            ),
            "InvalidRequest"
        );
    }

    #[test]
    fn only_encrypted_objects_move_to_a_well_formed_key_of_the_bucket_owner() {
        let owner = "123456789012";
        for source in ["AES256", "aws:kms"] {
            check_update_object_encryption(Some(source), Some(ARN), owner).unwrap();
        }
        for (source, key, expected) in [
            (None, Some(ARN), "InvalidRequest"),
            (Some("aws:kms:dsse"), Some(ARN), "InvalidRequest"),
            (Some("AES256"), None, "InvalidRequest"),
            (Some("AES256"), Some(""), "InvalidRequest"),
            (Some("AES256"), Some("alias/mine"), "InvalidRequest"),
            (Some("AES256"), Some("0b5f2a4e"), "InvalidRequest"),
            (
                Some("AES256"),
                Some("arn:aws:kms:us-east-1:123456789012:alias/mine"),
                "InvalidRequest",
            ),
            (
                Some("AES256"),
                Some("arn:aws:kms:us-east-1:210987654321:key/0b5f2a4e"),
                "AccessDenied",
            ),
        ] {
            assert_eq!(
                code(check_update_object_encryption(source, key, owner)),
                expected,
                "{source:?} {key:?}"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use fakecloud_core::delivery::DeliveryBus;
    use fakecloud_core::service::{AwsRequest, AwsServiceError, RequestBodyStream};
    use http::{HeaderMap, Method, StatusCode};
    use parking_lot::Mutex;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_service() -> S3Service {
        let state: crate::SharedS3State = Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        S3Service::new(state, Arc::new(DeliveryBus::new()))
    }

    fn make_request(headers: HeaderMap, body: Bytes) -> AwsRequest {
        let stream_body = RequestBodyStream::from(body.clone());
        AwsRequest {
            service: "s3".to_string(),
            action: "WriteGetObjectResponse".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test-req-id".to_string(),
            headers,
            query_params: HashMap::new(),
            body,
            body_stream: Mutex::new(Some(stream_body)),
            path_segments: vec!["WriteGetObjectResponse".to_string()],
            raw_path: "/WriteGetObjectResponse".to_string(),
            raw_query: "".to_string(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn assert_aws_err(
        result: Result<AwsResponse, AwsServiceError>,
        expect_code: &str,
    ) -> AwsServiceError {
        let err = match result {
            Ok(_) => panic!("expected error, got Ok response"),
            Err(e) => e,
        };
        match &err {
            AwsServiceError::AwsError { code, .. } => {
                assert_eq!(code, expect_code, "unexpected error code");
            }
            other => panic!("expected AwsError, got {other:?}"),
        }
        err
    }

    #[test]
    fn write_get_object_response_stores_body_and_headers() {
        let svc = make_service();
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-request-route", "route-1".parse().unwrap());
        headers.insert("x-amz-request-token", "token-1".parse().unwrap());
        headers.insert(
            "x-amz-fwd-header-Content-Type",
            "text/plain".parse().unwrap(),
        );
        headers.insert("x-amz-meta-custom", "value".parse().unwrap());

        let req = make_request(headers, Bytes::from_static(b"hello object lambda"));
        let resp = svc.write_get_object_response("123456789012", &req).unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        let accounts = svc.state.read();
        let state = accounts.get("123456789012").unwrap();
        let stored = state.object_lambda_responses.get("token-1").unwrap();
        assert_eq!(stored.route, "route-1");
        assert_eq!(stored.token, "token-1");
        assert_eq!(stored.body, b"hello object lambda");
        assert_eq!(stored.content_type, Some("text/plain".to_string()));
        assert_eq!(stored.metadata.get("custom"), Some(&"value".to_string()));
        // stored_at is recorded for introspection.
        assert!(stored.stored_at <= chrono::Utc::now());
    }

    #[test]
    fn write_get_object_response_missing_route_rejected() {
        let svc = make_service();
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-request-token", "token-2".parse().unwrap());

        let req = make_request(headers, Bytes::new());
        let err = assert_aws_err(
            svc.write_get_object_response("123456789012", &req),
            "BadRequest",
        );
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn write_get_object_response_missing_token_rejected() {
        let svc = make_service();
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-request-route", "route-3".parse().unwrap());

        let req = make_request(headers, Bytes::new());
        let err = assert_aws_err(
            svc.write_get_object_response("123456789012", &req),
            "BadRequest",
        );
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }
}
