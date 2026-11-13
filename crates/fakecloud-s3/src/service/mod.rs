use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Timelike, Utc};
use http::{HeaderMap, Method, StatusCode};
use md5::{Digest, Md5};

use fakecloud_aws::arn::{arn_resource, Arn};
use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_kms::SharedKmsState;
use fakecloud_persistence::{MemoryS3Store, S3Store, StoreError};

use crate::state::{AclGrant, S3Bucket, S3Object, SharedS3State};

mod access_points;
mod acl;
mod annotations;
mod buckets;
pub(crate) mod config;
mod control_tags;
mod iam_actions;
#[rustfmt::skip]
mod iam_reference;
mod lock;
mod response;
mod route;
mod routing;
use crate::state::ObjectLockSettings;
use lock::has_object_lock_headers;
mod multipart;
pub(crate) mod notifications;
mod objects;
pub(crate) use objects::{
    commit_object_version, commit_object_version_with, is_null_version, run_blocking_io,
};
pub(crate) mod replication;
mod request_checks;
mod tags;

// Re-export notification helpers for use in sub-modules
pub(super) use notifications::{deliver_notifications, normalize_notification_ids};
pub(super) use replication::normalize_replication_xml;

// Used only within this file (parse_cors_config)
use notifications::extract_all_xml_values;

// Re-exports used only in tests
#[cfg(test)]
use notifications::{
    event_matches, key_matches_filters, parse_notification_config, NotificationTargetType,
};
#[cfg(test)]
use replication::parse_replication_rules;

/// `Vary` value S3 attaches to any response it evaluates CORS for.
///
/// A response carrying `Access-Control-Allow-Origin` is specific to the
/// `Origin` that asked for it. Without `Vary`, a shared cache (or the browser's
/// own HTTP cache) stores it under a key that ignores `Origin` and reuses it
/// for a request from a *different* origin — handing out one origin's
/// allowance, or handing an allowed origin a stored response that was rejected
/// for someone else. Like S3, this is emitted only for requests that carry
/// `Origin`; a response cached from an `Origin`-less load carries no `Vary`
/// (and no `Access-Control-Allow-Origin`) either way.
const CORS_VARY: &str = "Origin, Access-Control-Request-Headers, Access-Control-Request-Method";

/// Read a header that gates CORS approval, as a single trimmed value.
///
/// Returns `None` when the header is absent, blank, unreadable, or sent on more
/// than one line: none of those is a single value to evaluate, and every one of
/// them must fail closed rather than silently using the first line.
fn single_cors_header<'a>(headers: &'a http::HeaderMap, name: &str) -> Option<&'a str> {
    let mut seen: Option<&str> = None;
    for line in headers.get_all(name) {
        let value = line.to_str().ok()?.trim();
        match seen {
            // Repeating the same value is how a proxy or CDN duplicates a
            // header; that is still one value, so it is not a conflict.
            Some(first) if first == value => {}
            Some(_) => return None,
            None => seen = Some(value),
        }
    }
    seen.filter(|s| !s.is_empty())
}

/// Set a CORS response header on whatever a CORS-evaluated request produced —
/// success or error alike.
///
/// Errors matter as much as successes: real S3 answers a 404 `NoSuchKey` for an
/// allowed origin *with* `Access-Control-Allow-Origin`, so `fetch(…, {mode:
/// 'cors'})` sees a 404 it can handle rather than an opaque CORS network error.
/// And a 404 is heuristically cacheable, so it needs `Vary` for the same reason
/// a 200 does.
fn set_cors_header(
    result: &mut Result<AwsResponse, AwsServiceError>,
    name: &'static str,
    value: &str,
) {
    match result {
        Ok(resp) => {
            if let Ok(v) = value.parse::<http::HeaderValue>() {
                // `Vary` is list-valued and combines, so append rather than
                // replace: a handler that set its own (say `Accept-Encoding`
                // on a range response) must not lose it just because the
                // request carried an `Origin`. Every other CORS header here is
                // single-valued and replaces.
                if name.eq_ignore_ascii_case("vary") {
                    // Deduped like the error path in `dispatch`: appending an
                    // identical value would emit the header twice, and the
                    // tests read only the first line so it would go unnoticed.
                    if !resp.headers.get_all(name).iter().any(|e| e == v) {
                        resp.headers.append(name, v);
                    }
                } else {
                    resp.headers.insert(name, v);
                }
            }
        }
        Err(AwsServiceError::AwsError { headers, .. }) => {
            // Same append-vs-replace split as above, so the two arms cannot
            // disagree if an error path ever sets its own `Vary`.
            if !name.eq_ignore_ascii_case("vary") {
                headers.retain(|(k, _)| !k.eq_ignore_ascii_case(name));
            }
            headers.push((name.to_string(), value.to_string()));
        }
        // Other variants render without per-error headers.
        Err(_) => {}
    }
}

pub struct S3Service {
    state: SharedS3State,
    delivery: Arc<DeliveryBus>,
    kms_state: Option<SharedKmsState>,
    pub(crate) kms_hook: Option<Arc<dyn fakecloud_core::delivery::KmsHook>>,
    store: Arc<dyn S3Store>,
    /// Resolves an access key ID to its secret + principal. Used only by
    /// POST Object (browser-form "POST Policy" upload) to verify the
    /// `x-amz-signature` form field cryptographically — that wire format
    /// carries no `Authorization` header, so dispatch's normal SigV4
    /// verification path never sees it. `None` when unwired (e.g. most
    /// unit tests): POST Policy signature checks then only accept the
    /// `test*` root-bypass access key, matching [`fakecloud_core::auth::is_root_bypass`]'s
    /// "always skip crypto" convention used everywhere else in the codebase.
    pub(crate) credential_resolver: Option<Arc<dyn fakecloud_core::auth::CredentialResolver>>,
    /// IAM enforcement mode, for authorization S3 performs itself rather
    /// than at dispatch: replication into another account's bucket needs
    /// that bucket's policy to allow the replication role (logged under
    /// `soft`, refused under `strict`).
    pub(crate) iam_mode: fakecloud_core::auth::IamMode,
    /// Service Quotas, for the bucket quota once the user switched its
    /// enforcement on. Without one nothing is enforced.
    quota_provider: Option<Arc<dyn fakecloud_core::quota::QuotaProvider>>,
    /// The delivery bus, KMS hook and IAM mode bundled for the side effects
    /// every new object version triggers, kept in step with the fields above
    /// and installed on the state so other services' writers share them.
    pub(crate) side_effects: Arc<crate::side_effects::S3SideEffects>,
}

/// Serialize a persistence snapshot, turning a failure into a 500 rather than an
/// empty document.
///
/// An empty file is worse than no file: the loader either skips it (losing the
/// configuration with no word, which the next Put then makes permanent) or, for
/// `acl.toml`, takes its presence to mean an explicit ACL and drops the default
/// owner grant.
pub(crate) fn toml_or_internal_error<T: serde::Serialize>(
    value: &T,
) -> Result<String, AwsServiceError> {
    toml::to_string(value).map_err(|e| {
        AwsServiceError::aws_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            format!("failed to serialize persisted state: {e}"),
        )
    })
}

/// Reject a request that names an ACL more than one way.
///
/// A canned header, the `x-amz-grant-*` headers and an `AccessControlPolicy`
/// body are mutually exclusive on S3, and letting one win silently discards what
/// the others asked for -- durably, since the result is persisted. Shared so the
/// bucket and object paths cannot drift on which pairs they reject.
pub(crate) fn reject_conflicting_acl_sources(
    canned: Option<&str>,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<(), AwsServiceError> {
    let grants = has_grant_headers(headers);
    let has_body = !body_is_blank(body);
    let conflict = match (canned.is_some(), grants, has_body) {
        (true, true, _) => Some("Specifying both Canned ACLs and Header Grants is not allowed"),
        (true, _, true) => {
            Some("Specifying both a Canned ACL and an AccessControlPolicy body is not allowed")
        }
        (_, true, true) => {
            Some("Specifying both Header Grants and an AccessControlPolicy body is not allowed")
        }
        _ => None,
    };
    match conflict {
        Some(message) => Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            message,
        )),
        None => Ok(()),
    }
}

/// The ACL an object write asks for: a canned value, or resolved grant headers,
/// or neither.
#[derive(Debug)]
pub(crate) struct WriteAclHeaders {
    pub(crate) canned: Option<String>,
    pub(crate) grants: Option<Vec<AclGrant>>,
}

impl WriteAclHeaders {
    /// The grants to store, given the owner to fall back to.
    pub(crate) fn grants_for(&self, owner_id: &str) -> Option<Vec<AclGrant>> {
        match (&self.grants, self.canned.as_deref()) {
            (Some(grants), _) => Some(grants.clone()),
            (None, Some(acl)) => Some(canned_acl_grants_for_object(acl, owner_id)),
            (None, None) => None,
        }
    }
}

/// Whether a request body carries nothing an ACL could be read from.
///
/// The mutual-exclusion checks and the "names no ACL at all" check have to agree
/// on this: testing raw emptiness in one and trimmed emptiness in the other made
/// a canned ACL plus a stray newline -- one ACL, named once -- fail as though it
/// named two.
pub(crate) fn body_is_blank(body: &[u8]) -> bool {
    // A body that does not decode is one no ACL can be read from, which is
    // exactly how the parse sites treat it (`from_utf8(..).unwrap_or("")`).
    // Calling it present would reject a canned ACL for conflicting with a body
    // the next line discards.
    std::str::from_utf8(body).map_or(true, |s| s.trim().is_empty())
}

impl S3Service {
    /// Validate and resolve the ACL headers of an object write.
    ///
    /// Every object-write path goes through here: PutObject,
    /// CreateMultipartUpload and CopyObject each used to keep their own copy of
    /// these four checks, and the ORDER matters -- the canned value is validated
    /// before the grant headers are resolved, and both before the
    /// "named two ways" rejection, so a request carrying an unresolvable grant
    /// answers InvalidArgument rather than InvalidRequest. The conformance probe
    /// populates the canned member and the grant members together, and only the
    /// first of those codes is in the S3 error allowlist, so a path that
    /// reordered its own copy would silently drop probe variants.
    ///
    /// Call it before any work the request would have to undo -- in particular
    /// before the body is spooled to disk, since nothing unlinks the spool file
    /// on an error path.
    pub(crate) fn resolve_write_acl_headers(
        &self,
        account_id: &str,
        bucket: &str,
        headers: &HeaderMap,
    ) -> Result<WriteAclHeaders, AwsServiceError> {
        let canned = headers
            .get("x-amz-acl")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        if let Some(acl) = canned.as_deref() {
            validate_object_canned_acl(acl)?;
        }
        let grants = if has_grant_headers(headers) {
            Some(resolved_grant_headers(headers)?)
        } else {
            None
        };
        if canned.is_some() && grants.is_some() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Specifying both Canned ACLs and Header Grants is not allowed",
            ));
        }
        // The bucket's ACL owner, which is who "bucket owner full control" means
        // -- not the caller. They coincide for a bucket this account created, and
        // diverge for one persisted by another account and hydrated into the
        // default account on restart, where comparing against the caller accepted
        // a grant to somebody who is not the owner and refused the grant to the
        // owner that AWS accepts.
        //
        // Looked up only when the request actually names an ACL: the overwhelming
        // majority of object writes carry none, and this takes the global read
        // lock. Resolved once and used for both the grants below and the exception
        // check, so the two cannot derive the owner differently.
        let bucket_owner = (canned.is_some() || grants.is_some())
            .then(|| self.bucket_acl_owner_id(account_id, bucket))
            .flatten();
        let owner_id = bucket_owner.as_deref().unwrap_or(account_id);

        // The grants the request resolves to, which is what the public-ACL check
        // below has to judge. Resolved against the bucket's owner, which is what
        // the write paths store them under.
        let requested = match (&grants, canned.as_deref()) {
            (Some(g), _) => Some(g.clone()),
            (None, Some(acl)) => Some(canned_acl_grants_for_object(acl, owner_id)),
            (None, None) => None,
        };

        // BucketOwnerEnforced disables object ACLs. The model is precise about
        // the one exception: such a bucket "only accept[s] PUT requests that
        // don't specify an ACL or PUT requests that specify bucket owner full
        // control ACLs, such as the bucket-owner-full-control canned ACL or an
        // equivalent form of this ACL expressed in the XML format".
        //
        // So the test is the ACL the request NAMES, not the grants it resolves
        // to. Judging by resolved shape would also accept `private`,
        // `bucket-owner-read` and `aws-exec-read`, all of which AWS refuses
        // here: `private` is owner-only by definition, and the other two
        // collapse onto an owner-only grant only because their real grantees
        // are not modeled. `private` in particular is neither "no ACL" nor
        // "bucket owner full control".
        let asks_for_owner_full_control = match (&grants, canned.as_deref()) {
            // "an equivalent form of this ACL": explicit grants that give the
            // owner full control and nobody anything. `resolved_grant_headers`
            // never yields an empty vector (it errors instead), so every grant
            // here is a real clause from the request.
            (Some(g), _) => g.iter().all(|grant| {
                grant.permission == "FULL_CONTROL"
                    && grant.grantee_type == "CanonicalUser"
                    && grant.grantee_id.as_deref() == Some(owner_id)
            }),
            (None, Some(acl)) => acl == "bucket-owner-full-control",
            (None, None) => false,
        };
        let specifies_an_acl = grants.is_some() || canned.is_some();
        if specifies_an_acl
            && !asks_for_owner_full_control
            && self.bucket_owner_enforced(account_id, bucket)
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AccessControlListNotSupported",
                "The bucket does not allow ACLs",
            ));
        }

        // BlockPublicAcls refuses a public grant at write time too. Only the
        // Put*Acl paths enforced it, so `put-object --acl public-read` stored an
        // AllUsers grant on a bucket that blocks exactly that while
        // `put-object-acl --acl public-read` was refused; AWS refuses both.
        if let Some(requested) = requested.as_deref() {
            if crate::service::config::grants_are_public(requested) {
                if let Some(flags) = self.pab_flags(account_id, bucket) {
                    if flags.block_public_acls {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::FORBIDDEN,
                            "AccessDenied",
                            "User is not authorized to perform: s3:PutObject. Reason: Public Access Block (BlockPublicAcls)",
                        ));
                    }
                }
            }
        }
        Ok(WriteAclHeaders { canned, grants })
    }

    /// Refuse a write that asks for SSE-C on a bucket whose default encryption
    /// blocks it (`BlockedEncryptionTypes` = SSE-C, the default for buckets
    /// created since April 2026). S3 rejects such PutObject, CopyObject,
    /// PostObject and multipart-upload requests with 403 AccessDenied; reads
    /// of existing SSE-C objects are unaffected. `sse_c_algorithm` is the
    /// request's `x-amz-server-side-encryption-customer-algorithm` (the
    /// destination one on a copy, never the copy-source one).
    pub(crate) fn reject_blocked_sse_c(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
        sse_c_algorithm: Option<&str>,
    ) -> Result<(), AwsServiceError> {
        if sse_c_algorithm.is_none_or(|v| v.trim().is_empty()) {
            return Ok(());
        }
        let blocked = {
            let accts = self.state.read();
            accts
                .get(account_id)
                .and_then(|s| s.buckets.get(bucket))
                .and_then(|b| b.encryption_config.as_deref())
                .is_some_and(crate::state::encryption_blocks_sse_c)
        };
        if !blocked {
            return Ok(());
        }
        let user = req
            .principal
            .as_ref()
            .map(|p| p.arn.clone())
            .unwrap_or_else(|| {
                Arn::global_in(&req.region, "iam", &req.account_id, "root").to_string()
            });
        let resource = Arn::s3_in(&req.region, &format!("{bucket}/{key}"));
        Err(AwsServiceError::aws_error(
            StatusCode::FORBIDDEN,
            "AccessDenied",
            format!(
                "User: {user} is not authorized to perform: s3:PutObject on resource: \
                 \"{resource}\" because this bucket has blocked upload requests that specify \
                 Server Side Encryption with Customer provided keys (SSE-C). Please specify a \
                 different server-side encryption type"
            ),
        ))
    }
}

/// The request's `x-amz-server-side-encryption-customer-algorithm`, the
/// header that makes an object write SSE-C.
pub(crate) fn sse_c_algorithm_header(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("x-amz-server-side-encryption-customer-algorithm")
        .and_then(|v| v.to_str().ok())
}

/// Map a [`StoreError`] from the persistence layer to a 500 InternalError
/// response. Invoked at every mutation site when the write-through persistence
/// call fails: the in-memory mutation has already happened, but we surface the
/// failure to the client so they know to retry (and so logs/metrics flag it).
pub(crate) fn persistence_error(err: StoreError) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "InternalError",
        format!("persistence store error: {err}"),
    )
}

/// Convert a filesystem IO error from a disk-backed body read into an
/// InternalError response.
pub(crate) fn io_to_aws(err: std::io::Error) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "InternalError",
        format!("failed to read object body from disk: {err}"),
    )
}

impl S3Service {
    /// The read authorization a copy request needs on its source object:
    /// `s3:GetObject` (or `s3:GetObjectVersion` when the source names a
    /// version) on `arn:...:s3:::src-bucket/src-key`. `None` for a request
    /// that is not a copy, or whose source is not a `bucket/key` path (an
    /// access-point ARN source is authorized through the access point).
    fn copy_source_read_action(
        &self,
        request: &AwsRequest,
    ) -> Option<fakecloud_core::auth::IamAction> {
        if request.method != Method::PUT
            || request_key(request).is_none()
            || fakecloud_core::protocol::is_s3_control_host(&request.headers)
        {
            return None;
        }
        let header = request.headers.get("x-amz-copy-source")?.to_str().ok()?;
        if header.trim_start_matches('/').starts_with("arn:") {
            return None;
        }
        let source = parse_copy_source(header)?;
        let (src_bucket, src_key) = (source.bucket.as_str(), source.key.as_str());
        if src_bucket.is_empty() || src_key.is_empty() {
            return None;
        }
        let action = if source.version_id.is_some() {
            "GetObjectVersion"
        } else {
            "GetObject"
        };
        let region = self
            .bucket_region(src_bucket)
            .unwrap_or_else(|| request.region.clone());
        Some(fakecloud_core::auth::IamAction {
            service: "s3",
            action,
            resource: Arn::s3_in(&region, &format!("{src_bucket}/{src_key}")).to_string(),
        })
    }

    /// The account that owns `bucket`, searched across accounts (bucket names
    /// are global). `None` when no account holds a bucket by that name.
    pub(crate) fn bucket_owner_account(&self, bucket: &str) -> Option<String> {
        self.state
            .read()
            .find_account(|s| s.buckets.contains_key(bucket))
            .map(|a| a.to_string())
    }

    /// The region of an existing bucket, searched across accounts (bucket
    /// names are global).
    fn bucket_region(&self, bucket: &str) -> Option<String> {
        let mas = self.state.read();
        let account = mas.find_account(|s| s.buckets.contains_key(bucket))?;
        mas.get(account)?
            .buckets
            .get(bucket)
            .map(|b| b.region.clone())
    }

    pub fn new(state: SharedS3State, delivery: Arc<DeliveryBus>) -> Self {
        Self::with_store(state, delivery, Arc::new(MemoryS3Store::new()))
    }

    pub fn with_store(
        state: SharedS3State,
        delivery: Arc<DeliveryBus>,
        store: Arc<dyn S3Store>,
    ) -> Self {
        let side_effects = Arc::new(crate::side_effects::S3SideEffects::new(
            delivery.clone(),
            None,
            fakecloud_core::auth::IamMode::Off,
        ));
        crate::side_effects::install(&state, &side_effects);
        Self {
            state,
            delivery,
            kms_state: None,
            kms_hook: None,
            store,
            credential_resolver: None,
            iam_mode: fakecloud_core::auth::IamMode::Off,
            quota_provider: None,
            side_effects,
        }
    }

    /// Rebuild the side-effect bundle after the delivery bus, KMS hook or
    /// IAM mode changed, and install it on the state again.
    fn refresh_side_effects(&mut self) {
        self.side_effects = Arc::new(crate::side_effects::S3SideEffects::new(
            self.delivery.clone(),
            self.kms_hook.clone(),
            self.iam_mode,
        ));
        crate::side_effects::install(&self.state, &self.side_effects);
    }

    /// Attach Service Quotas so the general purpose bucket quota is enforced
    /// once the user switches it on.
    pub fn with_quota_provider(
        mut self,
        provider: Option<Arc<dyn fakecloud_core::quota::QuotaProvider>>,
    ) -> Self {
        self.quota_provider = provider;
        self
    }

    pub fn with_iam_mode(mut self, mode: fakecloud_core::auth::IamMode) -> Self {
        self.iam_mode = mode;
        self.refresh_side_effects();
        self
    }

    pub fn with_kms(mut self, kms_state: SharedKmsState) -> Self {
        self.kms_state = Some(kms_state);
        self
    }

    pub fn with_kms_hook(mut self, hook: Arc<dyn fakecloud_core::delivery::KmsHook>) -> Self {
        self.kms_hook = Some(hook);
        self.refresh_side_effects();
        self
    }

    /// Wire a [`fakecloud_core::auth::CredentialResolver`] so POST Object
    /// (browser-form POST Policy upload) can verify the form-carried
    /// `x-amz-signature` against a non-root IAM access key's real secret.
    pub fn with_credential_resolver(
        mut self,
        resolver: Arc<dyn fakecloud_core::auth::CredentialResolver>,
    ) -> Self {
        self.credential_resolver = Some(resolver);
        self
    }

    /// The encryption a new object takes, as every object-creating request
    /// (PutObject, POST Object, CreateMultipartUpload) resolves it: the
    /// request's `x-amz-server-side-encryption*` headers, else the bucket's
    /// default encryption (`encryption_config`). An SSE-KMS key is recorded
    /// by its ARN, resolved in the bucket's region; with no key named, S3
    /// uses the account's AWS-managed `aws/s3` key and reports that key.
    pub(crate) fn object_encryption(
        &self,
        headers: &http::HeaderMap,
        encryption_config: Option<&str>,
        account_id: &str,
        bucket_region: &str,
    ) -> crate::side_effects::ObjectEncryption {
        let header = |name: &str| {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
        };
        let mut algorithm = header("x-amz-server-side-encryption");
        let mut kms_key_id = header("x-amz-server-side-encryption-aws-kms-key-id");
        let mut bucket_key_enabled = header("x-amz-server-side-encryption-bucket-key-enabled")
            .map(|v| v.eq_ignore_ascii_case("true"));
        if algorithm.is_none() {
            if let Some(default) =
                encryption_config.and_then(crate::side_effects::bucket_default_encryption)
            {
                if kms_key_id.is_none() {
                    kms_key_id = default.kms_key_id;
                }
                if bucket_key_enabled.is_none() {
                    bucket_key_enabled = default.bucket_key_enabled;
                }
                algorithm = Some(default.algorithm);
            }
        }
        if crate::side_effects::is_sse_kms(algorithm.as_deref()) {
            kms_key_id = self.resolve_object_kms_key(account_id, bucket_region, kms_key_id);
        }
        crate::side_effects::ObjectEncryption {
            algorithm,
            kms_key_id,
            bucket_key_enabled,
        }
    }

    /// The ARN of the SSE-KMS key `key_id` names (the AWS-managed `aws/s3`
    /// key when it names none). A key or alias ARN names its account
    /// (possibly the requester's); a bare id or alias resolves in the bucket
    /// owner's account, in the bucket's region. Through the hook an
    /// AWS-managed reference mints its key. A key that cannot be resolved is
    /// kept as given.
    fn resolve_object_kms_key(
        &self,
        account_id: &str,
        bucket_region: &str,
        key_id: Option<String>,
    ) -> Option<String> {
        let reference = key_id.clone().unwrap_or_else(|| "aws/s3".to_string());
        let key_account = kms_reference_account(&reference).unwrap_or(account_id);
        let resolved = self
            .kms_hook
            .as_ref()
            .and_then(|hook| {
                hook.resolve_key_arn(key_account, bucket_region, &reference, "s3.amazonaws.com")
                    .ok()
            })
            .or_else(|| {
                let kms = self.kms_state.as_ref()?;
                let kms_accounts = kms.read();
                kms_accounts
                    .get(key_account)
                    .unwrap_or(kms_accounts.default_ref())
                    .resolve_key_arn(bucket_region, &reference)
                    .map(str::to_string)
            });
        if resolved.is_none() {
            if let Some(key) = &key_id {
                tracing::debug!(key_id = %key, "KMS key not found in state, proceeding anyway");
            }
        }
        resolved.or(key_id)
    }

    /// Encrypt object body bytes for SSE-KMS storage. Returns ciphertext as
    /// raw bytes (a UTF-8 fakecloud-kms envelope) on success.
    ///
    /// Fail-closed: if the KMS hook reports an error (key denied, key not
    /// found, etc.), this returns `Err` so PutObject aborts with a 500
    /// rather than silently storing plaintext. AWS S3 has the same
    /// behavior — `KMS.NotFoundException` and friends surface as
    /// `AccessDenied` / `KMS.*` errors back to the caller. When no hook is
    /// wired (legacy / in-process tests with no KMS dependency), the
    /// plaintext is returned unchanged so existing tests keep working.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn encrypt_object_body(
        &self,
        req: &AwsRequest,
        account_id: &str,
        region: &str,
        bucket: &str,
        object_key: &str,
        bucket_key: bool,
        plaintext: &[u8],
        kms_key_id: Option<&str>,
    ) -> Result<bytes::Bytes, AwsServiceError> {
        if self.kms_hook.is_none() {
            return Ok(bytes::Bytes::copy_from_slice(plaintext));
        }
        // A key or alias ARN names its own account (it may be the
        // requester's, outside the bucket owner's); a bare key id or alias
        // resolves in the bucket owner's account. The KMS hook decides
        // whether the requester may use the key.
        crate::side_effects::kms_encrypt(
            self.kms_hook.as_ref(),
            account_id,
            region,
            bucket,
            object_key,
            bucket_key,
            plaintext,
            kms_key_id,
            req.principal.as_ref().map(|p| p.arn.as_str()),
        )
        .map_err(|err| {
            tracing::warn!(bucket = %bucket, error = %err, "SSE-KMS encrypt failed");
            kms_hook_error(&err)
        })
    }

    /// Decrypt object body bytes that were stored as a fakecloud-kms
    /// envelope. Caller is expected to gate this on
    /// `obj.sse_algorithm == Some("aws:kms")`. `kms_key_id` is the key the
    /// object records (normally its ARN), which names the account whose KMS
    /// holds the key; without one the key is the bucket owner's.
    ///
    /// Fail-closed: when a hook is wired and the bytes look like an
    /// envelope but don't decrypt (key revoked, malformed ciphertext),
    /// this returns `Err`. When no hook is wired, or the bytes aren't UTF-8
    /// (legacy snapshots from before the hook landed), the bytes are
    /// returned unchanged.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn decrypt_object_body(
        &self,
        req: &AwsRequest,
        account_id: &str,
        bucket: &str,
        object_key: &str,
        bucket_key: bool,
        ciphertext: &[u8],
        kms_key_id: Option<&str>,
    ) -> Result<bytes::Bytes, AwsServiceError> {
        let Some(hook) = &self.kms_hook else {
            return Ok(bytes::Bytes::copy_from_slice(ciphertext));
        };
        // Stored envelope is base64 ASCII; non-UTF-8 bytes are pre-hook
        // legacy snapshots, return as-is.
        let envelope = match std::str::from_utf8(ciphertext) {
            Ok(s) => s,
            Err(_) => return Ok(bytes::Bytes::copy_from_slice(ciphertext)),
        };
        let key = kms_key_id.filter(|k| !k.is_empty()).unwrap_or("aws/s3");
        let key_account = kms_reference_account(key).unwrap_or(account_id);
        let ctx = crate::side_effects::s3_encryption_context(bucket, object_key, bucket_key);
        // The object records its key's ARN, resolved in the bucket's region
        // when it was written (a replica's for a multi-Region key named by
        // id there), so that ARN's region is the one to decrypt in. Without
        // one (an object written before keys were recorded as ARNs, or the
        // AWS-managed default), the key the ciphertext names opens it, as
        // before; the request's region says nothing about the bucket's.
        let region = fakecloud_aws::arn::region_of(key).unwrap_or_default();
        hook.decrypt_in_region(
            key_account,
            region,
            envelope,
            fakecloud_core::delivery::KmsCaller::via_for(
                "s3.amazonaws.com",
                req.principal.as_ref().map(|p| p.arn.as_str()),
            ),
            ctx,
        )
        .map(bytes::Bytes::from)
        .map_err(|err| {
            tracing::warn!(bucket = %bucket, error = %err, "SSE-KMS decrypt failed");
            kms_hook_error(&err)
        })
    }
}

#[async_trait]
impl AwsService for S3Service {
    fn service_name(&self) -> &str {
        "s3"
    }

    async fn handle(&self, mut req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // PutObject / UploadPart enter dispatch via the streaming path
        // (`streams_request_body`) with `body = Bytes::new()` and the raw
        // HTTP body available on `req.body_stream`. Those handlers consume
        // the stream directly -- spooling chunks to disk while computing
        // MD5 + size in constant memory -- so a 1 GiB upload never
        // materializes into RAM. Every other request reads a small XML/JSON
        // body from `req.body`; a stream reaching one of them (a direct
        // call) is drained.
        // S3 Control (`s3-control` host) shares this handler; its PUTs
        // (CreateAccessPoint, ...) carry an XML body, never object data.
        let is_control = fakecloud_core::protocol::is_s3_control_host(&req.headers);
        // The same key IAM authorized the request for (`request_key`).
        let key = request_key(&req);
        // The operation, from the one classification authorization also uses
        // (`route::classify`), so the operation dispatch authorized is the one
        // routed below. Classified before the body is read, so a request S3
        // refuses outright (conflicting sub-resources, an unsupported method)
        // is refused without reading its body -- here too, for when IAM
        // enforcement is off. S3 Control (classified once its own routes are
        // ruled out), CORS preflights and paths with an empty bucket segment
        // are handled on their own below.
        let early_op =
            if is_control || req.method == Method::OPTIONS || req.raw_path.starts_with("//") {
                None
            } else {
                match classify_request(&req) {
                    Ok(op) => Some(op),
                    Err(refusal) => return Err(self.refuse(&req, refusal)),
                }
            };
        // Uploads (PutObject, UploadPart) read their body as a stream; every
        // other request's body is small and buffered.
        let is_put_object = early_op == Some(route::S3Op::PutObject);
        let is_upload_part = matches!(
            early_op,
            Some(route::S3Op::UploadPart | route::S3Op::UploadPartCopy)
        );
        if !is_put_object && !is_upload_part {
            if let Some(stream) = req.take_body_stream() {
                req.body = fakecloud_core::service::drain_request_stream(stream).await?;
            }
        }
        // Access point data plane: resolve alias -> bucket before routing.
        access_points::resolve_access_point(self, &mut req)?;

        let account_id = req.account_id.as_str();

        // S3 Control endpoint (access point management, resource tagging).
        if is_control {
            let v1 = req.path_segments.first().map(|s| s.as_str());
            let v2 = req.path_segments.get(1).map(|s| s.as_str());
            let v3 = req.path_segments.get(2).map(|s| s.as_str());
            if v1 == Some("v20180820") && v2 == Some("accesspoint") {
                if let Some(name) = v3 {
                    match req.method {
                        Method::PUT => return self.create_access_point(account_id, &req, name),
                        Method::GET => return self.get_access_point(account_id, &req, name),
                        Method::DELETE => return self.delete_access_point(account_id, &req, name),
                        _ => {}
                    }
                } else if req.method == Method::GET {
                    return self.list_access_points(account_id, &req);
                }
            }
            if v1 == Some("v20180820") && v2 == Some("tags") {
                if let Some(result) = self.handle_control_tags(account_id, &req) {
                    return result;
                }
            }
        }

        // S3 REST routing: method + path segments + query params
        //
        // Reject paths that start with `//` (an empty bucket segment). The
        // path-segment splitter further down filters empty segments out,
        // which would otherwise let a malformed URL like `PUT //my-key`
        // collapse to a valid `PUT /my-key` (CreateBucket) and silently
        // succeed. Real S3 rejects empty bucket segments with
        // InvalidBucketName, so mirror that.
        if req.raw_path.starts_with("//") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidBucketName",
                "The specified bucket is not valid: bucket name cannot be empty",
            ));
        }
        let bucket = req.path_segments.first().map(|s| s.as_str());
        let op = match early_op {
            Some(op) => Some(op),
            None if req.method == Method::OPTIONS => None,
            // An S3 Control request none of its routes took.
            None => match classify_request(&req) {
                Ok(op) => Some(op),
                Err(refusal) => return Err(self.refuse(&req, refusal)),
            },
        };

        // Bucket names are one global namespace, so a bucket-addressed request
        // is served in the account that OWNS the bucket, not the caller's: a
        // principal in account B reading account A's bucket (authorized by A's
        // bucket policy at dispatch, which resolves the same owner) must find
        // A's bucket and objects, and anything it writes lands in A's bucket.
        // A bucket no account holds keeps the caller's account, so the
        // handlers answer NoSuchBucket from there. CreateBucket and
        // ListBuckets stay in the caller's account (they are routed with
        // `caller_account` below), and the requester recorded on events is
        // always the caller.
        let caller_account = req.account_id.as_str();
        let owner_account = bucket.and_then(|b| self.bucket_owner_account(b));
        let account_id = owner_account.as_deref().unwrap_or(caller_account);

        // The payload's own refusals come once the bucket is known to exist
        // and the caller is authorized (dispatch has run), as recorded on S3:
        // a missing bucket is NoSuchBucket and an unauthorized caller
        // AccessDenied whatever the framing. A request to a missing bucket is
        // left to answer NoSuchBucket (CreateBucket creates its bucket).
        if !is_control
            && (bucket.is_none()
                || owner_account.is_some()
                || op == Some(route::S3Op::CreateBucket))
        {
            if let Some(error) = op.and_then(|op| streaming_payload_refused(&req, op)) {
                return Err(self.refuse(&req, error.into()));
            }
            // Dispatch unframed an aws-chunked body already
            // (`prepare_request`); one still framed here is broken, and this
            // returns its error. A direct call is unframed here.
            if req.body_stream.lock().is_none() {
                let mut body = std::mem::take(&mut req.body);
                let unframed = crate::checksum::deframe_buffered(&mut req.headers, &mut body);
                req.body = body;
                if let Err(error) = unframed {
                    return Err(self.refuse(&req, error.into()));
                }
            }
        } else if let Some(b) = bucket.filter(|_| {
            !is_control
                && req.body_stream.lock().is_none()
                && crate::checksum::still_framed(&req.headers)
        }) {
            // A body still framed to a missing bucket: NoSuchBucket, before
            // anything reads the body (recorded, PutObject and ?tagging).
            return Err(no_such_bucket(b));
        }

        if let Some(op) = op.filter(|_| !is_control && bucket.is_some()) {
            request_checks::check_request(&req, key.as_deref(), op)?;
            if owner_account.is_some() {
                request_checks::check_existing_bucket_request(&req, op)?;
            }
        }

        // Integrity of buffered request bodies (PutBucketCors, DeleteObjects,
        // PutObjectTagging, ...): S3 verifies a `Content-MD5` or
        // `x-amz-checksum-<algorithm>` header against the body it received.
        // Uploads verify their own payload (PutObject, UploadPart,
        // PutObjectAnnotation); CompleteMultipartUpload's checksum headers
        // describe the assembled object, not its XML body; a POST Object form
        // carries its fields in the body. A request to a bucket that does not
        // exist is left to answer NoSuchBucket.
        // CopyObject / UploadPartCopy carry no body; their checksum header
        // names the copy's algorithm and is validated by the handler.
        let verifies_own_payload = matches!(
            op,
            Some(
                route::S3Op::PutObject
                    | route::S3Op::UploadPart
                    | route::S3Op::PutObjectAnnotation
                    | route::S3Op::CompleteMultipartUpload
                    | route::S3Op::CopyObject
                    | route::S3Op::UploadPartCopy
                    | route::S3Op::PostObject
            )
        );
        if !is_control && !verifies_own_payload && (bucket.is_none() || owner_account.is_some()) {
            crate::checksum::verify_request_body(&req.headers, &req.body)?;
        }

        // Handle OPTIONS preflight requests (CORS)
        if req.method == Method::OPTIONS {
            if let Some(b_name) = bucket {
                // S3 validates the request before evaluating CORS at all: a
                // preflight with no `Origin` carries nothing to evaluate and is
                // a malformed request, distinct from the 403 a disallowed
                // origin gets. Empty counts as absent, as on every other path.
                // Every preflight response varies by `Origin` — absent is a
                // 400, present-and-allowed a 200, present-and-disallowed a 403
                // — so all three carry `Vary`. Without it a cache keying the
                // 400 from a bare health-check `OPTIONS` would replay it to a
                // real browser preflight for the same URL.
                let vary = || vec![("vary".to_string(), CORS_VARY.to_string())];

                // Only the missing-`Origin` case is a documented 400. A
                // preflight that carries `Origin` but no request-method is an
                // ordinary non-allowed preflight and falls through to the 403
                // below, whose message distinguishes an unconfigured bucket
                // from a non-matching rule.
                // Only a genuinely absent `Origin` gets the "needed" message. A
                // header that is present but unusable — blank, unreadable, or
                // sent on several lines — is not missing information, so it
                // falls through to the denial below rather than telling the
                // caller to add a header they already sent.
                let origin = single_cors_header(&req.headers, "origin");
                if origin.is_none() && req.headers.get("origin").is_none() {
                    return Err(AwsServiceError::aws_error_with_headers(
                        StatusCode::BAD_REQUEST,
                        "BadRequest",
                        "Insufficient information. Origin request header needed.",
                        vary(),
                    ));
                }
                let origin = origin.unwrap_or("");
                let cors_config = {
                    let accounts = self.state.read();
                    let _empty_s3 = crate::state::S3State::new(account_id, &req.region);
                    let state = accounts.get(account_id).unwrap_or(&_empty_s3);
                    state
                        .buckets
                        .get(b_name)
                        .and_then(|b| b.cors_config.clone())
                };
                if let Some(ref config) = cors_config {
                    // `origin` came from the guard above, so it is a single
                    // non-blank value. The request-method is read the same way:
                    // two lines are not one method to allow, and approving on
                    // the first would grant a method never evaluated.
                    let request_method =
                        single_cors_header(&req.headers, "access-control-request-method")
                            .unwrap_or("");
                    // Every header the preflight declares must be covered by
                    // the rule, or S3 denies it — the denial message, the
                    // `Vary` value and the docs all say this is evaluated.
                    // `get_all`: the field may be sent as several lines, and
                    // now that it gates approval, an unread second line would
                    // approve headers the rule never allowed. For the same
                    // reason a line that is not readable ASCII denies the
                    // preflight instead of being skipped — this is a deny gate,
                    // so an unreadable value must not fail open.
                    let requested_headers: Option<Vec<String>> = req
                        .headers
                        .get_all("access-control-request-headers")
                        .iter()
                        .map(|v| v.to_str().ok())
                        .collect::<Option<Vec<_>>>()
                        .map(|lines| {
                            lines
                                .iter()
                                .flat_map(|v| v.split(','))
                                .map(|h| h.trim().to_string())
                                .filter(|h| !h.is_empty())
                                // Deduped: this list is echoed back verbatim in
                                // `Access-Control-Allow-Headers`, and a client
                                // repeating a name should not produce
                                // `x-a, x-a` where S3 sends it once.
                                .fold(Vec::new(), |mut acc, h| {
                                    if !acc
                                        .iter()
                                        .any(|seen: &String| seen.eq_ignore_ascii_case(&h))
                                    {
                                        acc.push(h);
                                    }
                                    acc
                                })
                        });
                    let rules = parse_cors_config(config);
                    if let Some((rule, requested_headers)) =
                        requested_headers.as_ref().and_then(|rh| {
                            find_cors_rule(&rules, origin, request_method, rh).map(|r| (r, rh))
                        })
                    {
                        let mut headers = HeaderMap::new();
                        // `*` only when the entry that actually matched is `*`. A rule
                        // listing both a concrete origin and `*` matches the
                        // concrete one first, and echoing `*` there would drop
                        // allow-credentials for an origin the rule names.
                        let matched_origin = if rule
                            .allowed_origins
                            .iter()
                            .find(|o| origin_matches(origin, o))
                            .is_some_and(|o| o == "*")
                        {
                            "*"
                        } else {
                            origin
                        };
                        headers.insert(
                            "access-control-allow-origin",
                            matched_origin
                                .parse()
                                .unwrap_or_else(|_| http::HeaderValue::from_static("")),
                        );
                        headers.insert("vary", http::HeaderValue::from_static(CORS_VARY));
                        // A concrete allow-origin gets allow-credentials, as on
                        // S3. Without it the Fetch spec fails any
                        // `credentials: 'include'` request, so a preflight that
                        // otherwise passes still blocks the real request. It is
                        // never sent alongside `*`, which the spec forbids.
                        if matched_origin != "*" {
                            headers.insert(
                                "access-control-allow-credentials",
                                http::HeaderValue::from_static("true"),
                            );
                        }
                        // The matched rule's whole method list, as S3 returns:
                        // the browser caches one preflight per URL, so echoing
                        // only the requested method would force a fresh
                        // round-trip for every other method the rule allows.
                        headers.insert(
                            "access-control-allow-methods",
                            rule.allowed_methods
                                .join(", ")
                                .parse()
                                .unwrap_or_else(|_| http::HeaderValue::from_static("")),
                        );
                        // Echo the headers the preflight actually asked for —
                        // reaching here means every one of them is covered by
                        // the rule. Echoing `rule.allowed_headers` instead
                        // would emit patterns like `x-amz-*`, which browsers
                        // compare literally and would reject. With no
                        // `Access-Control-Request-Headers` there is nothing to
                        // allow, so the header is omitted, as on S3.
                        if !requested_headers.is_empty() {
                            headers.insert(
                                "access-control-allow-headers",
                                requested_headers
                                    .join(", ")
                                    .parse()
                                    .unwrap_or_else(|_| http::HeaderValue::from_static("")),
                            );
                        }
                        if let Some(max_age) = rule.max_age_seconds {
                            headers.insert(
                                "access-control-max-age",
                                max_age
                                    .to_string()
                                    .parse()
                                    .unwrap_or_else(|_| http::HeaderValue::from_static("")),
                            );
                        }
                        return Ok(AwsResponse {
                            status: StatusCode::OK,
                            content_type: String::new(),
                            body: Bytes::new().into(),
                            headers,
                        });
                    }
                }
                // S3 distinguishes the two denials, and so must this: a caller
                // otherwise cannot tell an unconfigured bucket from one whose
                // rules simply do not cover their request. Both carry `Vary` —
                // even on an unconfigured bucket this 403 differs from the 400
                // an `Origin`-less preflight gets, so the response still turns
                // on `Origin` and must not be cached across origins.
                let message = if cors_config.is_some() {
                    "CORSResponse: This CORS request is not allowed. This is usually because \
                     the evaluation of Origin, request method / Access-Control-Request-Method \
                     or Access-Control-Request-Headers are not whitelisted by the resource's \
                     CORS spec."
                } else {
                    "CORSResponse: CORS is not enabled for this bucket."
                };
                return Err(AwsServiceError::aws_error_with_headers(
                    StatusCode::FORBIDDEN,
                    "AccessForbidden",
                    message,
                    vary(),
                ));
            }
        }

        // A preflight with no bucket reaches no operation.
        let Some(op) = op else {
            return Err(AwsServiceError::aws_error(
                StatusCode::METHOD_NOT_ALLOWED,
                "MethodNotAllowed",
                "The specified method is not allowed against this resource",
            ));
        };
        // Every operation that addresses a bucket (or an object in one) was
        // classified from a path naming them, so these are only empty for the
        // service-level listings, which use neither.
        let b = bucket.unwrap_or_default();
        let k = key.as_deref().unwrap_or_default();
        let mut result = self
            .route_operation(op, &req, caller_account, account_id, b, k)
            .await;
        self.finish_response(
            &req,
            account_id,
            bucket,
            key.as_deref(),
            Some(op),
            &mut result,
        );
        result
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            // Buckets
            "ListBuckets",
            "CreateBucket",
            "DeleteBucket",
            "HeadBucket",
            "GetBucketLocation",
            // Objects
            "PutObject",
            "GetObject",
            "DeleteObject",
            "HeadObject",
            "CopyObject",
            "DeleteObjects",
            "ListObjectsV2",
            "ListObjects",
            "ListObjectVersions",
            "GetObjectAttributes",
            "RestoreObject",
            // Object annotations
            "PutObjectAnnotation",
            "GetObjectAnnotation",
            "DeleteObjectAnnotation",
            "ListObjectAnnotations",
            "UpdateBucketMetadataAnnotationTableConfiguration",
            // Object properties
            "PutObjectTagging",
            "GetObjectTagging",
            "DeleteObjectTagging",
            "PutObjectAcl",
            "GetObjectAcl",
            "PutObjectRetention",
            "GetObjectRetention",
            "PutObjectLegalHold",
            "GetObjectLegalHold",
            // Bucket configuration
            "PutBucketTagging",
            "GetBucketTagging",
            "DeleteBucketTagging",
            "PutBucketAcl",
            "GetBucketAcl",
            "PutBucketVersioning",
            "GetBucketVersioning",
            "PutBucketCors",
            "GetBucketCors",
            "DeleteBucketCors",
            "PutBucketNotificationConfiguration",
            "GetBucketNotificationConfiguration",
            "PutBucketWebsite",
            "GetBucketWebsite",
            "DeleteBucketWebsite",
            "PutBucketAccelerateConfiguration",
            "GetBucketAccelerateConfiguration",
            "PutPublicAccessBlock",
            "GetPublicAccessBlock",
            "DeletePublicAccessBlock",
            "PutBucketEncryption",
            "GetBucketEncryption",
            "DeleteBucketEncryption",
            "PutBucketLifecycleConfiguration",
            "GetBucketLifecycleConfiguration",
            "DeleteBucketLifecycle",
            "PutBucketLogging",
            "GetBucketLogging",
            "PutBucketPolicy",
            "GetBucketPolicy",
            "DeleteBucketPolicy",
            "PutObjectLockConfiguration",
            "GetObjectLockConfiguration",
            "PutBucketReplication",
            "GetBucketReplication",
            "DeleteBucketReplication",
            "PutBucketOwnershipControls",
            "GetBucketOwnershipControls",
            "DeleteBucketOwnershipControls",
            "PutBucketInventoryConfiguration",
            "GetBucketInventoryConfiguration",
            "DeleteBucketInventoryConfiguration",
            "ListBucketInventoryConfigurations",
            "PutBucketAnalyticsConfiguration",
            "GetBucketAnalyticsConfiguration",
            "DeleteBucketAnalyticsConfiguration",
            "ListBucketAnalyticsConfigurations",
            "PutBucketIntelligentTieringConfiguration",
            "GetBucketIntelligentTieringConfiguration",
            "DeleteBucketIntelligentTieringConfiguration",
            "ListBucketIntelligentTieringConfigurations",
            "PutBucketMetricsConfiguration",
            "GetBucketMetricsConfiguration",
            "DeleteBucketMetricsConfiguration",
            "ListBucketMetricsConfigurations",
            "PutBucketRequestPayment",
            "GetBucketRequestPayment",
            "PutBucketAbac",
            "GetBucketAbac",
            "GetBucketPolicyStatus",
            "CreateBucketMetadataConfiguration",
            "GetBucketMetadataConfiguration",
            "DeleteBucketMetadataConfiguration",
            "CreateBucketMetadataTableConfiguration",
            "GetBucketMetadataTableConfiguration",
            "DeleteBucketMetadataTableConfiguration",
            "UpdateBucketMetadataInventoryTableConfiguration",
            "UpdateBucketMetadataJournalTableConfiguration",
            "GetObjectTorrent",
            "RenameObject",
            "SelectObjectContent",
            "UpdateObjectEncryption",
            "WriteGetObjectResponse",
            "ListDirectoryBuckets",
            "CreateSession",
            // Multipart uploads
            "CreateMultipartUpload",
            "UploadPart",
            "UploadPartCopy",
            "CompleteMultipartUpload",
            "AbortMultipartUpload",
            "ListParts",
            "ListMultipartUploads",
        ]
    }

    fn iam_enforceable(&self) -> bool {
        true
    }

    /// The request's primary authorization: the operation's own required
    /// action (see [`AwsService::iam_actions_for`] for the full set).
    fn iam_action_for(&self, request: &AwsRequest) -> Option<fakecloud_core::auth::IamAction> {
        self.iam_actions_for(request).into_iter().next()
    }

    /// Every IAM authorization an S3 request needs, under S3's policy action
    /// names (not its operation names: `ListObjectsV2` is `s3:ListBucket`,
    /// `HeadObject` is `s3:GetObject`, ...; see [`iam_actions`]). Dispatch
    /// requires every one, so a request that also sets an ACL, tags, Object
    /// Lock settings or bypasses governance retention needs those permissions
    /// too, a copy needs the source read, and DeleteObjects needs the delete
    /// on each key.
    ///
    /// Resources are the bucket ARN (`arn:aws:s3:::bucket`) for bucket-level
    /// actions, the object ARN (`arn:aws:s3:::bucket/key`) for object-level
    /// ones and `*` for account-level ones (`s3:ListAllMyBuckets`), by the
    /// action's resource type in the service authorization reference. S3
    /// ARNs carry neither account nor region, since bucket names are global.
    fn iam_actions_for(&self, request: &AwsRequest) -> Vec<fakecloud_core::auth::IamAction> {
        if let Some(action) = control_iam_action(request) {
            return vec![action];
        }
        // S3 doesn't set `request.action` -- the handler dispatches on
        // method + path + query params directly -- so the operation is
        // re-derived here.
        let Some(op) = s3_operation(request) else {
            return Vec::new();
        };
        let bucket = request.path_segments.first().map(|s| s.as_str());
        let key = request_key(request);
        // Every ARN S3 returns for a bucket carries the bucket's own region's
        // partition, so policies written against those ARNs must match here.
        let bucket_region = bucket.and_then(|b| self.bucket_region(b));
        let region = bucket_region.as_deref().unwrap_or(&request.region);
        iam_actions::required(op, request)
            .into_iter()
            .filter_map(|need| match need {
                iam_actions::Need::Own(service, action) => {
                    let resource = if service == "s3express" && action == "CreateSession" {
                        match bucket {
                            Some(bucket) => self.directory_bucket_arn(request, region, bucket),
                            None => "*".to_string(),
                        }
                    } else {
                        s3_resource_for(region, action, bucket, key.as_deref())
                    };
                    Some(fakecloud_core::auth::IamAction {
                        service,
                        action,
                        resource,
                    })
                }
                iam_actions::Need::Key(action, object_key) => {
                    Some(fakecloud_core::auth::IamAction {
                        service: "s3",
                        action,
                        resource: s3_resource_for(region, action, bucket, Some(&object_key)),
                    })
                }
                // CopyObject and UploadPartCopy also read the source object --
                // evaluated against the source bucket's owner and bucket
                // policy, so copying out of another account's bucket needs
                // that bucket's grant.
                iam_actions::Need::CopySource => self.copy_source_read_action(request),
                iam_actions::Need::PassRole(role) => {
                    Some(fakecloud_core::auth::IamAction::pass_role(role))
                }
            })
            .collect()
    }

    /// CreateBucket creates in the caller's account: a name another account
    /// owns is authorized against the caller's own policies and then answered
    /// `BucketAlreadyExists`, not refused by that account's bucket policy.
    fn iam_resource_in_caller_account(&self, request: &AwsRequest) -> bool {
        s3_operation(request) == Some("CreateBucket") && control_iam_action(request).is_none()
    }

    /// DeleteObjects is authorized per key by its handler, which reports a
    /// denied key as an `AccessDenied` entry of its 200 response. The handler
    /// routes on the same classification, so a request deferred here is one
    /// it runs as DeleteObjects (`POST /b//?delete&uploads` addresses the key
    /// `/` and is refused as conflicting).
    fn iam_authorized_by_handler(&self, request: &AwsRequest) -> bool {
        !fakecloud_core::protocol::is_s3_control_host(&request.headers)
            && !request.raw_path.starts_with("//")
            && request.method != Method::OPTIONS
            && matches!(
                s3_operation(request),
                // A browser-form upload's key and signer are in its body.
                Some("DeleteObjects" | "PostObject")
            )
    }

    /// The requests S3 refuses before authorizing anything: conflicting
    /// sub-resource parameters, a sub-resource with a method it does not
    /// take, half an upload part's address (see [`route`]), and an upload
    /// part or part listing of an upload that does not exist (recorded:
    /// NoSuchUpload, even anonymously and whatever the part number). Every
    /// caller, anonymous included, gets these errors rather than an
    /// AccessDenied, answered as the handler answers them.
    /// S3 serves anonymous requests and judges them like any other: a
    /// missing bucket is NoSuchBucket and an unclassifiable request its 400
    /// before any AccessDenied (recorded).
    fn validates_anonymous_before_authorization(&self) -> bool {
        true
    }

    fn streams_request_body(&self, head: &fakecloud_core::service::RequestHead<'_>) -> bool {
        // Only an upload streams; every other request's body is small and
        // buffered, so what runs before the handler (IAM reading the tag set
        // of PutObjectTagging or CreateBucket, for one) sees it. Decided by
        // the classification the handler routes on, from the same bucket and
        // key, so the two cannot disagree about which requests stream.
        if fakecloud_core::protocol::is_s3_control_host(head.headers)
            || head.raw_path.starts_with("//")
        {
            return false;
        }
        let segments = fakecloud_core::path::split_path_segments(head.raw_path);
        let key = key_of_raw_path(head.raw_path);
        matches!(
            route::classify(
                head.method,
                segments.first().map(String::as_str),
                key.as_deref(),
                head.query_params,
                head.headers,
            ),
            Ok(route::S3Op::PutObject | route::S3Op::UploadPart)
        )
    }

    fn prepare_request(&self, request: &mut AwsRequest) -> Result<(), AwsServiceError> {
        if request.body_stream.lock().is_some() {
            return Ok(());
        }
        // A request refused for declaring a streaming payload keeps the
        // declaration it is refused for.
        if classify_request(request)
            .ok()
            .and_then(|op| streaming_payload_refused(request, op))
            .is_some()
        {
            return Ok(());
        }
        // A body that cannot be unframed stays framed (`still_framed`): S3
        // answers a missing bucket and an unauthorized caller before the
        // framing (recorded), so the handler refuses it after both, and
        // nothing reads it in between.
        let _ = crate::checksum::deframe_buffered(&mut request.headers, &mut request.body);
        Ok(())
    }

    fn validate_before_authorization(&self, request: &AwsRequest) -> Result<(), AwsServiceError> {
        // S3 Control TagResource: as for the S3 tagging writes below, a tag
        // set the handler cannot read is refused before its request tags are
        // evaluated.
        if control_tags::is_control_tags_request(request)
            && request.method == Method::POST
            && !request.body.is_empty()
        {
            let body = std::str::from_utf8(&request.body).unwrap_or("");
            tags::read_tag_set(body, "TagResourceRequest", "Tags")?;
        }
        if fakecloud_core::protocol::is_s3_control_host(&request.headers)
            || request.method == Method::OPTIONS
            || request.raw_path.starts_with("//")
        {
            return Ok(());
        }
        let op = classify_request(request).map_err(|refusal| self.refuse(request, refusal))?;
        if let Some(error) = self.missing_upload(request, op) {
            return Err(self.refuse(request, error.into()));
        }
        let bucket = request.path_segments.first();
        let bucket_exists = bucket.is_some_and(|b| self.bucket_owner_account(b).is_some());
        // An anonymous request to a bucket that does not exist is
        // NoSuchBucket, not AccessDenied (recorded, PutObject and ?tagging).
        // An access-point alias is resolved by the handler, not here.
        let via_access_point = request
            .headers
            .get("host")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|h| h.to_ascii_lowercase().contains("s3-accesspoint"));
        if let Some(b) = bucket.filter(|_| {
            !bucket_exists
                && !via_access_point
                && is_anonymous(request)
                && op != route::S3Op::CreateBucket
        }) {
            return Err(no_such_bucket(b));
        }
        if bucket_exists {
            if let Some(error) = streaming_payload_refused(request, op) {
                return Err(self.refuse(request, error.into()));
            }
        }
        // A body still framed could not be unframed (`prepare_request`): its
        // error comes after authorization, from the handler, and its tag set
        // is evaluated as none (the request fails and writes nothing).
        if crate::checksum::still_framed(&request.headers) {
            return Ok(());
        }
        // A tag set the handler cannot read is refused before its request
        // tags are evaluated, so IAM never judges a partial or misread set.
        if matches!(
            op,
            route::S3Op::PutObjectTagging | route::S3Op::PutBucketTagging
        ) && !request.body.is_empty()
        {
            let body = std::str::from_utf8(&request.body).unwrap_or("");
            if let Err(error) = tags::read_tag_set(body, "Tagging", "TagSet") {
                return Err(self.refuse(request, error.into()));
            }
        }
        if op == route::S3Op::CreateBucket {
            let body = std::str::from_utf8(&request.body).unwrap_or("");
            if let Err(error) = create_bucket_configuration_tags(body) {
                return Err(self.refuse(request, error.into()));
            }
        }
        Ok(())
    }

    fn iam_condition_keys_for(
        &self,
        request: &AwsRequest,
        action: &fakecloud_core::auth::IamAction,
    ) -> std::collections::BTreeMap<String, Vec<String>> {
        if action.is_pass_role() {
            let associated = self.iam_action_for(request).map(|a| a.resource);
            return fakecloud_core::auth::pass_role_condition_keys(
                "s3.amazonaws.com",
                associated.as_deref(),
            );
        }
        let mut keys = s3_condition_keys(action.action, &request.query_params, &request.headers);
        // `s3express:SessionMode`: the session a CreateSession asks for
        // (`x-amz-create-session-mode`, ReadWrite by default), and ReadWrite
        // for a Zonal write authorized as one (RenameObject).
        if action.service == "s3express" && action.action == "CreateSession" {
            let mode = if request.query_params.contains_key("session") {
                request
                    .headers
                    .get("x-amz-create-session-mode")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("ReadWrite")
            } else {
                "ReadWrite"
            };
            keys.insert("s3express:SessionMode".to_string(), vec![mode.to_string()]);
        }
        // S3 Control UntagResource: `aws:TagKeys` lists the keys being removed.
        if action.action == "UntagResource" && control_tags::is_control_tags_request(request) {
            keys.insert(
                "aws:TagKeys".to_string(),
                control_tags::tag_keys_from_query(&request.raw_query),
            );
        }
        keys
    }

    fn resource_tags_for(
        &self,
        resource_arn: &str,
    ) -> Option<std::collections::HashMap<String, String>> {
        s3_resource_tags(&self.state, resource_arn)
            .map(|m| m.into_iter().collect::<std::collections::HashMap<_, _>>())
    }

    /// `aws:RequestTag/*` comes from the request, not the action: AWS builds
    /// one request context for the whole request, so every action a request
    /// needs (CreateBucket's `s3:TagResource`, a tagged PutObject's
    /// `s3:PutObjectTagging`) sees the same tags.
    fn request_tags_from(
        &self,
        request: &AwsRequest,
        action: &str,
    ) -> Option<std::collections::HashMap<String, String>> {
        let op = if control_tags::is_control_tags_request(request) {
            action
        } else {
            s3_operation(request).unwrap_or(action)
        };
        s3_request_tags(request, op)
            .map(|m| m.into_iter().collect::<std::collections::HashMap<_, _>>())
    }
}

impl S3Service {
    /// Whether the caller of the request being served may also perform
    /// `s3:<action>` on `bucket` (or its object `key`, for an object action),
    /// as dispatch would authorize it. `true` when IAM enforcement does not
    /// apply (off, the `test*` root bypass, an in-process call), and under
    /// soft enforcement, which only logs. Must not be called while holding
    /// the state lock: the evaluation reads bucket policies and owners.
    pub(crate) fn caller_may(
        &self,
        req: &AwsRequest,
        action: &'static str,
        bucket: &str,
        key: Option<&str>,
    ) -> bool {
        let Some(authorizer) = fakecloud_core::auth::request_authorizer("s3") else {
            return true;
        };
        let region = self
            .bucket_region(bucket)
            .unwrap_or_else(|| req.region.clone());
        authorizer.authorize(&fakecloud_core::auth::IamAction {
            service: "s3",
            action,
            resource: s3_resource_for(&region, action, Some(bucket), key),
        })
    }

    /// [`Self::caller_may`] for a request the handler derived from the one it
    /// serves (a browser-form upload as the PutObject its fields make it,
    /// signed by the uploader's `access_key_id` or anonymous): `s3:<action>`
    /// on the object `key` of `bucket`, with that request's condition keys.
    pub(crate) fn caller_may_for(
        &self,
        derived: &AwsRequest,
        action: &'static str,
        bucket: &str,
        key: &str,
    ) -> bool {
        let Some(authorizer) = fakecloud_core::auth::request_authorizer("s3") else {
            return true;
        };
        let region = self
            .bucket_region(bucket)
            .unwrap_or_else(|| derived.region.clone());
        authorizer.authorize_for(
            derived,
            &fakecloud_core::auth::IamAction {
                service: "s3",
                action,
                resource: s3_resource_for(&region, action, Some(bucket), Some(key)),
            },
        )
    }

    /// A read of a key that does not exist answers 404 `NoSuchKey` only to a
    /// caller allowed `s3:ListBucket` on the bucket, and 403 `AccessDenied`
    /// to any other, so a caller who cannot list cannot probe which keys
    /// exist (GetObject / HeadObject / GetObjectAttributes, "Permissions").
    fn hide_missing_key(
        &self,
        req: &AwsRequest,
        bucket: &str,
        result: Result<AwsResponse, AwsServiceError>,
    ) -> Result<AwsResponse, AwsServiceError> {
        // HeadObject reports every missing target as `NotFound`: there it is
        // a missing key when the bucket exists and no version was named.
        let missing_key = |e: &AwsServiceError| {
            e.code() == "NoSuchKey"
                || (e.code() == "NotFound"
                    && !iam_actions::names_version(req)
                    && self.bucket_region(bucket).is_some())
        };
        match result {
            Err(e) if missing_key(&e) && !self.caller_may(req, "ListBucket", bucket, None) => Err(
                AwsServiceError::aws_error(StatusCode::FORBIDDEN, "AccessDenied", "Access Denied"),
            ),
            other => other,
        }
    }

    /// GetObject and HeadObject return an object's Object Lock retention
    /// (`x-amz-object-lock-mode`, `-retain-until-date`) only to a caller
    /// allowed `s3:GetObjectRetention`, and its legal hold
    /// (`x-amz-object-lock-legal-hold`) only to one allowed
    /// `s3:GetObjectLegalHold` (HeadObjectOutput: "This header is only
    /// returned if the requester has the ... permission").
    fn shape_lock_headers(
        &self,
        req: &AwsRequest,
        bucket: &str,
        key: &str,
        mut result: Result<AwsResponse, AwsServiceError>,
    ) -> Result<AwsResponse, AwsServiceError> {
        if let Ok(resp) = result.as_mut() {
            let retention = [
                "x-amz-object-lock-mode",
                "x-amz-object-lock-retain-until-date",
            ];
            if retention.iter().any(|h| resp.headers.contains_key(*h))
                && !self.caller_may(req, "GetObjectRetention", bucket, Some(key))
            {
                for h in retention {
                    resp.headers.remove(h);
                }
            }
            if resp.headers.contains_key("x-amz-object-lock-legal-hold")
                && !self.caller_may(req, "GetObjectLegalHold", bucket, Some(key))
            {
                resp.headers.remove("x-amz-object-lock-legal-hold");
            }
        }
        result
    }

    /// A directory bucket's ARN, in the `s3express` namespace and naming its
    /// region and owning account: `arn:aws:s3express:<region>:<account>:bucket/<name>`.
    fn directory_bucket_arn(&self, request: &AwsRequest, region: &str, bucket: &str) -> String {
        let owner = self
            .state
            .read()
            .find_account(|s| s.buckets.contains_key(bucket))
            .map_or_else(|| request.account_id.clone(), str::to_string);
        Arn::regional("s3express", region, &owner, &format!("bucket/{bucket}")).to_string()
    }
}

/// The object key a request addresses: the RAW path after the bucket
/// segment, decoded exactly once, `None` when empty. It keeps the leading,
/// trailing and repeated slashes `path_segments` drops (`/b//` addresses the
/// key `/`, `/b/k/` the key `k/`), and stays right when an access-point alias
/// in the first segment is rewritten to its bucket in `path_segments` but not
/// in `raw_path`. The one derivation both authorization and routing use, so
/// the operation dispatch authorizes is the operation the handler runs.
pub(crate) fn request_key(req: &AwsRequest) -> Option<String> {
    req.path_segments.first()?;
    key_of_raw_path(&req.raw_path)
}

/// A request that presents no credentials (and is no in-process service
/// principal).
fn is_anonymous(req: &AwsRequest) -> bool {
    req.access_key_id.is_none() && req.principal.is_none()
}

/// An anonymous request that declares a streaming payload
/// (`x-amz-content-sha256: STREAMING-...`) for an operation whose payload is
/// no streaming blob: 400 InvalidRequest, after the bucket's existence and
/// before authorization (recorded for an anonymous PutObjectTagging; an
/// anonymous PutObject with the same header goes on to AccessDenied, so the
/// upload operations take it). Signed requests are not judged here: nothing
/// recorded says how S3 treats one.
fn streaming_payload_refused(req: &AwsRequest, op: route::S3Op) -> Option<AwsServiceError> {
    let streaming = req
        .headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.trim().starts_with("STREAMING-"));
    let anonymous = is_anonymous(req);
    let takes_streaming_payload = matches!(
        op,
        route::S3Op::PutObject | route::S3Op::UploadPart | route::S3Op::PutObjectAnnotation
    );
    (streaming && anonymous && !takes_streaming_payload).then(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            "The value of x-amz-content-sha256 header is invalid.",
        )
    })
}

/// [`request_key`] from the raw path alone, for a request not yet built
/// (dispatch asking whether it streams).
fn key_of_raw_path(raw_path: &str) -> Option<String> {
    let rest = raw_path.strip_prefix('/').unwrap_or(raw_path);
    let (_, raw_key) = rest.split_once('/')?;
    (!raw_key.is_empty()).then(|| {
        percent_encoding::percent_decode_str(raw_key)
            .decode_utf8_lossy()
            .into_owned()
    })
}

/// The operation a request invokes, by the one classification the handler
/// routes on ([`route::classify`]); `Err` is how S3 refuses it before
/// authorizing anything.
fn classify_request(request: &AwsRequest) -> Result<route::S3Op, route::Refusal> {
    route::classify(
        &request.method,
        request.path_segments.first().map(String::as_str),
        request_key(request).as_deref(),
        &request.query_params,
        &request.headers,
    )
}

/// The name of the S3 API operation a request invokes; `None` for a request
/// S3 refuses before authorizing it.
fn s3_operation(request: &AwsRequest) -> Option<&'static str> {
    classify_request(request).ok().map(route::S3Op::name)
}

/// The authorization of an S3 Control request fakecloud serves: access-point
/// management and resource tagging. `None` for any other request.
fn control_iam_action(request: &AwsRequest) -> Option<fakecloud_core::auth::IamAction> {
    // S3 Control access-point management (host `s3-control...`, path
    // `/v20180820/accesspoint[/name]`) is authorized under the dedicated
    // s3:CreateAccessPoint / GetAccessPoint / DeleteAccessPoint /
    // ListAccessPoints actions — NOT the object-level GetObject/PutObject/
    // DeleteObject the generic method-based detection would pick, which made
    // access-point policies ineffective.
    let is_control = fakecloud_core::protocol::is_s3_control_host(&request.headers)
        && request.path_segments.first().map(|s| s.as_str()) == Some("v20180820");
    // S3 Control resource tagging is authorized as s3:TagResource /
    // UntagResource / ListTagsForResource on the tagged resource's ARN.
    if is_control && request.path_segments.get(1).map(|s| s.as_str()) == Some("tags") {
        let action = match request.method.as_str() {
            "GET" => Some("ListTagsForResource"),
            "POST" => Some("TagResource"),
            "DELETE" => Some("UntagResource"),
            _ => None,
        };
        if let Some(action) = action {
            let resource =
                control_tags::resource_arn_from_path(request).unwrap_or_else(|| "*".to_string());
            return Some(fakecloud_core::auth::IamAction {
                service: "s3",
                action,
                resource,
            });
        }
    }
    if is_control && request.path_segments.get(1).map(|s| s.as_str()) == Some("accesspoint") {
        let has_name = request.path_segments.get(2).is_some();
        let ap_action = match (request.method.as_str(), has_name) {
            ("PUT", true) => Some("CreateAccessPoint"),
            ("GET", true) => Some("GetAccessPoint"),
            ("DELETE", true) => Some("DeleteAccessPoint"),
            ("GET", false) => Some("ListAccessPoints"),
            _ => None,
        };
        if let Some(action) = ap_action {
            let resource = request
                .path_segments
                .get(2)
                .map(|name| {
                    Arn::s3_access_point(&request.region, &request.account_id, name).to_string()
                })
                .unwrap_or_else(|| "*".to_string());
            return Some(fakecloud_core::auth::IamAction {
                service: "s3",
                action,
                resource,
            });
        }
    }
    None
}

/// The top-level `<Role>` of a PutBucketReplication body: the role S3
/// assumes to replicate objects.
fn replication_role_arn(body: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(body).ok()?;
    let start = text.find("<Role>")? + "<Role>".len();
    let end = start + text[start..].find("</Role>")?;
    let role = text[start..end].trim();
    (!role.is_empty()).then(|| role.to_string())
}

/// Extract service-specific IAM condition keys from an S3 request.
///
/// `s3:ListBucket` / `s3:ListBucketVersions` expose `s3:prefix`, `s3:delimiter`
/// and `s3:max-keys` from their query params, a request naming a version
/// exposes `s3:versionid`, and any request carrying the ACL,
/// object-ownership or object-lock headers exposes the matching `s3:x-amz-*`
/// keys. An action with nothing to expose returns an empty map, so the
/// evaluator's safe-fail semantics treat a policy condition on an unknown key
/// as "doesn't apply".
fn s3_condition_keys(
    action: &str,
    query: &std::collections::HashMap<String, String>,
    headers: &HeaderMap,
) -> std::collections::BTreeMap<String, Vec<String>> {
    let mut out = std::collections::BTreeMap::new();
    // `s3:x-amz-acl` and the `s3:x-amz-grant-*` family are how a policy pins
    // down which ACL a write may ask for -- a guardrail like
    // `Deny s3:CreateBucket when s3:x-amz-acl != private` is useless while they
    // are never populated. AWS exposes them on every ACL-accepting write, so
    // they are emitted whenever the request carries them rather than being
    // gated on an action list that would drift.
    if let Some(acl) = headers
        .get("x-amz-acl")
        .and_then(|v| v.to_str().ok())
        // Blank is skipped for consistency with every other key family here (the
        // grant headers, object ownership): a present-but-empty header names no
        // value, and a key emitted as `""` matches nothing a policy can sensibly
        // write. Note this does NOT mean the request succeeds -- three of the four
        // canned-ACL paths answer 400 for a blank value; it means the policy
        // decision is not made on an empty string.
        .filter(|v| !v.trim().is_empty())
    {
        out.insert("s3:x-amz-acl".to_string(), vec![acl.to_string()]);
    }
    for (header, _) in &GRANT_HEADER_PERMISSIONS {
        // Blank values are skipped for the same reason `has_grant_headers`
        // treats them as absent: a present-but-empty header asks for no grant,
        // and emitting the key would make a `Null`-based guardrail read it as
        // present and deny the request.
        let values: Vec<String> = headers
            .get_all(*header)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .filter(|v| !v.trim().is_empty())
            .map(|v| v.to_string())
            .collect();
        if !values.is_empty() {
            out.insert(format!("s3:{header}"), values);
        }
    }
    // The ACL is not the only setting `iam_actions_for` gates: object ownership is
    // too, and `Deny CreateBucket unless s3:x-amz-object-ownership ==
    // BucketOwnerEnforced` is just as useless while its key is never populated.
    //
    // Object lock is deliberately NOT here. AWS defines no condition key for the
    // `x-amz-bucket-object-lock-enabled` header -- its object-lock keys are
    // `s3:object-lock-mode`, `-legal-hold`, `-remaining-retention-days` and
    // `-retain-until-date`, all about an object's retention rather than a bucket's
    // creation. Emitting `s3:x-amz-bucket-object-lock-enabled` would invert the
    // point of populating keys at all: a guardrail written against it would work
    // here and be a permanent no-op against AWS. The permission the lock header
    // requires is unaffected -- that comes from the model's Permissions text.
    if let Some(value) = headers
        .get("x-amz-object-ownership")
        .and_then(|v| v.to_str().ok())
        // Blank is skipped like the grant family: present but empty asks for
        // nothing, and emitting the key would make a `Null` check read it as set.
        .filter(|v| !v.trim().is_empty())
    {
        out.insert(
            "s3:x-amz-object-ownership".to_string(),
            vec![value.to_string()],
        );
    }
    // Request headers AWS exposes as `s3:` condition keys on the writes that
    // accept them. Encryption guardrails (`DenyIncorrectEncryptionHeader`:
    // `Deny PutObject when s3:x-amz-server-side-encryption != AES256`) and
    // storage-class / copy restrictions read these; since a negated operator
    // is true on an absent key, leaving them unpopulated would deny every
    // write, compliant ones included.
    for header in [
        "x-amz-server-side-encryption",
        "x-amz-server-side-encryption-aws-kms-key-id",
        "x-amz-server-side-encryption-customer-algorithm",
        "x-amz-storage-class",
        "x-amz-metadata-directive",
        "x-amz-copy-source",
    ] {
        if let Some(value) = headers
            .get(header)
            .and_then(|v| v.to_str().ok())
            .filter(|v| !v.trim().is_empty())
        {
            out.insert(format!("s3:{header}"), vec![value.to_string()]);
        }
    }
    // `s3:versionid` is defined on the `*Version*` actions a request naming a
    // version is authorized under.
    if let Some(version) = query.get("versionId").filter(|v| !v.trim().is_empty()) {
        out.insert("s3:versionid".to_string(), vec![version.clone()]);
    }
    // The listing keys of `s3:ListBucket` (ListObjects, ListObjectsV2) and
    // `s3:ListBucketVersions` (ListObjectVersions), which share the query
    // shape.
    if matches!(action, "ListBucket" | "ListBucketVersions") {
        if let Some(prefix) = query.get("prefix") {
            out.insert("s3:prefix".to_string(), vec![prefix.clone()]);
        }
        if let Some(delimiter) = query.get("delimiter") {
            out.insert("s3:delimiter".to_string(), vec![delimiter.clone()]);
        }
        if let Some(max_keys) = query.get("max-keys") {
            out.insert("s3:max-keys".to_string(), vec![max_keys.clone()]);
        }
    }
    out
}

/// Look up resource tags for an S3 ARN.
///
/// Bucket-level ARN (`arn:aws:s3:::bucket`) -> bucket tags.
/// Object-level ARN (`arn:aws:s3:::bucket/key`) -> object tags.
/// Access point ARN (`arn:aws:s3:<region>:<account>:accesspoint/<name>`) ->
/// access point tags.
/// `*` (ListBuckets) -> `Some(empty)` (no resource to tag).
fn s3_resource_tags(
    state: &SharedS3State,
    resource_arn: &str,
) -> Option<std::collections::BTreeMap<String, String>> {
    if resource_arn == "*" {
        return Some(std::collections::BTreeMap::new());
    }
    let resource = arn_resource(resource_arn, "s3")?;
    let mas = state.read();
    // Bucket or object ARN: arn:aws:s3:::bucket[/key]
    if let Some(after_prefix) = resource.strip_prefix("::") {
        return s3_bucket_or_object_tags(&mas, after_prefix);
    }
    // Access point ARN: arn:aws:s3:<region>:<account>:accesspoint/<name>,
    // taggable through S3 Control TagResource.
    let (_region, rest) = resource.split_once(':')?;
    let (account, rest) = rest.split_once(':')?;
    let name = rest.strip_prefix("accesspoint/")?;
    mas.get(account)?
        .access_points
        .get(name)
        .map(|ap| ap.tags.clone())
}

/// Tags of the bucket or object an S3 ARN resource (`bucket` or
/// `bucket/key`, after `arn:aws:s3:::`) names.
fn s3_bucket_or_object_tags(
    mas: &fakecloud_core::multi_account::MultiAccountState<crate::state::S3State>,
    after_prefix: &str,
) -> Option<std::collections::BTreeMap<String, String>> {
    // S3 bucket names are globally unique; scan all accounts to find the bucket
    let bucket_name = after_prefix.split('/').next().unwrap_or(after_prefix);
    let state = mas
        .find_account(|s| s.buckets.contains_key(bucket_name))
        .and_then(|id| mas.get(id))
        .or_else(|| Some(mas.default_ref()))?;
    if let Some(slash_pos) = after_prefix.find('/') {
        // Object-level: bucket/key
        let bucket_name = &after_prefix[..slash_pos];
        let key = &after_prefix[slash_pos + 1..];
        let bucket = state.buckets.get(bucket_name)?;
        // Try current objects first, then versioned objects (latest version)
        if let Some(obj) = bucket.objects.get(key) {
            Some(obj.tags.clone())
        } else if let Some(versions) = bucket.object_versions.get(key) {
            versions.last().map(|v| v.tags.clone())
        } else {
            // Object doesn't exist yet (e.g. PutObject creating it)
            Some(std::collections::BTreeMap::new())
        }
    } else {
        // Bucket-level
        let bucket = state.buckets.get(after_prefix)?;
        Some(bucket.tags.clone())
    }
}

/// Extract tags from an S3 request body/headers, by the operation `op` the
/// request invokes (or, for S3 Control tagging, its action).
///
/// S3 sends tags via:
/// - `x-amz-tagging` header (URL-encoded `key=value&...`) on PutObject,
///   CopyObject and CreateMultipartUpload
/// - XML body on PutBucketTagging / PutObjectTagging
/// - `CreateBucketConfiguration` on CreateBucket
fn s3_request_tags(
    request: &AwsRequest,
    op: &str,
) -> Option<std::collections::BTreeMap<String, String>> {
    match op {
        // A copy sets tags from the header only when it replaces the source's
        // tag set; under COPY (the default) the header is ignored.
        "CopyObject" if !iam_actions::copy_replaces_tags(&request.headers) => {
            Some(std::collections::BTreeMap::new())
        }
        "PutObject" | "CopyObject" | "CreateMultipartUpload" => {
            // Tags come via x-amz-tagging header
            if let Some(tagging) = request.headers.get("x-amz-tagging") {
                // An invalid header is refused by the handler before anything
                // is written.
                let tags =
                    tags::parse_tagging_header(tagging.to_str().unwrap_or("")).unwrap_or_default();
                Some(tags.into_iter().collect())
            } else {
                Some(std::collections::BTreeMap::new())
            }
        }
        // Tags come in the XML body, read by the parser the handler stores
        // them with (`tags::read_tag_set`). A body it cannot read is refused
        // before authorization (`validate_before_authorization`), so no
        // partial tag set is ever evaluated; a body whose aws-chunked framing
        // is broken has none, and is refused after authorization.
        "PutBucketTagging" | "PutObjectTagging" => {
            let body = std::str::from_utf8(&request.body).unwrap_or("");
            let tags = tags::read_tag_set(body, "Tagging", "TagSet").unwrap_or_default();
            Some(tags.into_iter().collect())
        }
        // S3 Control TagResource carries its tags in a `<TagResourceRequest>`
        // body.
        "TagResource" if control_tags::is_control_tags_request(request) => {
            let body = std::str::from_utf8(&request.body).unwrap_or("");
            let tags = tags::read_tag_set(body, "TagResourceRequest", "Tags").unwrap_or_default();
            Some(tags.into_iter().collect())
        }
        // UntagResource carries no tag values, only keys: AWS sets
        // `aws:TagKeys` (see `iam_condition_keys_for`) but no
        // `aws:RequestTag/*`. `None` keeps those unset, and lets the
        // `aws:TagKeys` lookup fall through to the condition keys.
        "UntagResource" if control_tags::is_control_tags_request(request) => None,
        // CreateBucket carries its tag set inside CreateBucketConfiguration, and
        // every authorization it requires alongside `s3:CreateBucket` is
        // evaluated against the same tags -- AWS builds one request context for
        // the whole request, so an `aws:RequestTag/*` guardrail that permits the
        // create must not then deny the ACL or object-lock action it implies.
        "CreateBucket" => {
            let body = std::str::from_utf8(&request.body).unwrap_or("");
            // Refused before authorization when it does not read
            // (`validate_before_authorization`).
            let tags = create_bucket_configuration_tags(body).unwrap_or_default();
            Some(tags.into_iter().collect())
        }
        _ => Some(std::collections::BTreeMap::new()),
    }
}

/// The name of the S3 API operation (not the IAM action: see
/// [`iam_actions`]) a request with this method, bucket, key and query
/// invokes, by the one classification ([`route::classify`]); `None` for a
/// request it refuses. Without the request's headers a PUT to a key is a
/// PutObject and an upload part an UploadPart ([`s3_operation`] tells the
/// copies apart).
#[cfg(test)]
fn s3_detect_action(
    method: &str,
    bucket: Option<&str>,
    key: Option<&str>,
    query: &std::collections::HashMap<String, String>,
) -> Option<&'static str> {
    let method: Method = method.parse().ok()?;
    route::classify(&method, bucket, key, query, &HeaderMap::new())
        .ok()
        .map(route::S3Op::name)
}

/// Build the resource ARN an S3 policy action is authorized on, by the
/// action's resource type in the service authorization reference: the object
/// ARN (`bucket/key`, `bucket/*` with no key) for object actions, `*` for
/// account actions (`s3:ListAllMyBuckets`) or a request naming no bucket, and
/// the bucket ARN otherwise.
fn s3_resource_for(region: &str, action: &str, bucket: Option<&str>, key: Option<&str>) -> String {
    use iam_reference::ResourceKind;
    let kind = iam_actions::resource_kind(action);
    let Some(bucket) = bucket.filter(|_| kind != Some(ResourceKind::Account)) else {
        return "*".to_string();
    };
    if kind == Some(ResourceKind::Object) {
        match key {
            Some(k) if !k.is_empty() => Arn::s3_in(region, &format!("{bucket}/{k}")).to_string(),
            _ => Arn::s3_in(region, &format!("{bucket}/*")).to_string(),
        }
    } else {
        Arn::s3_in(region, bucket).to_string()
    }
}

// Conditional request helpers

/// Truncate a DateTime to second-level precision (HTTP dates have no sub-second info).
pub(crate) fn truncate_to_seconds(dt: DateTime<Utc>) -> DateTime<Utc> {
    dt.with_nanosecond(0).unwrap_or(dt)
}

pub(crate) fn check_get_conditionals(
    req: &AwsRequest,
    obj: &S3Object,
) -> Result<(), AwsServiceError> {
    let obj_etag = format!("\"{}\"", obj.etag);
    let obj_time = truncate_to_seconds(obj.last_modified);

    // If-Match
    if let Some(if_match) = req.headers.get("if-match").and_then(|v| v.to_str().ok()) {
        if !etag_matches(if_match, &obj_etag) {
            return Err(precondition_failed("If-Match"));
        }
    }

    // If-None-Match
    if let Some(if_none_match) = req
        .headers
        .get("if-none-match")
        .and_then(|v| v.to_str().ok())
    {
        if etag_matches(if_none_match, &obj_etag) {
            return Err(not_modified_with_etag(&obj_etag));
        }
    }

    // If-Unmodified-Since — RFC 7232 §3.4: ignored when If-Match is present.
    if req.headers.get("if-match").is_none() {
        if let Some(since) = req
            .headers
            .get("if-unmodified-since")
            .and_then(|v| v.to_str().ok())
        {
            if let Some(dt) = parse_http_date(since) {
                if obj_time > dt {
                    return Err(precondition_failed("If-Unmodified-Since"));
                }
            }
        }
    }

    // If-Modified-Since — RFC 7232 §3.3: ignored when If-None-Match is present.
    if req.headers.get("if-none-match").is_none() {
        if let Some(since) = req
            .headers
            .get("if-modified-since")
            .and_then(|v| v.to_str().ok())
        {
            // A date in the future is not a valid condition (RFC 7232
            // §3.3) and S3 ignores it: GetObject with an If-Modified-Since
            // decades ahead answers 200 with the object (recorded).
            if let Some(dt) = parse_http_date(since).filter(|dt| *dt <= Utc::now()) {
                if obj_time <= dt {
                    return Err(not_modified());
                }
            }
        }
    }

    Ok(())
}

pub(crate) fn check_head_conditionals(
    req: &AwsRequest,
    obj: &S3Object,
) -> Result<(), AwsServiceError> {
    let obj_etag = format!("\"{}\"", obj.etag);
    let obj_time = truncate_to_seconds(obj.last_modified);

    // If-Match
    if let Some(if_match) = req.headers.get("if-match").and_then(|v| v.to_str().ok()) {
        if !etag_matches(if_match, &obj_etag) {
            return Err(precondition_failed("If-Match"));
        }
    }

    // If-None-Match
    if let Some(if_none_match) = req
        .headers
        .get("if-none-match")
        .and_then(|v| v.to_str().ok())
    {
        if etag_matches(if_none_match, &obj_etag) {
            return Err(not_modified_with_etag(&obj_etag));
        }
    }

    // If-Unmodified-Since — RFC 7232 §3.4: ignored when If-Match is present.
    if req.headers.get("if-match").is_none() {
        if let Some(since) = req
            .headers
            .get("if-unmodified-since")
            .and_then(|v| v.to_str().ok())
        {
            if let Some(dt) = parse_http_date(since) {
                if obj_time > dt {
                    return Err(precondition_failed("If-Unmodified-Since"));
                }
            }
        }
    }

    // If-Modified-Since — RFC 7232 §3.3: ignored when If-None-Match is present.
    if req.headers.get("if-none-match").is_none() {
        if let Some(since) = req
            .headers
            .get("if-modified-since")
            .and_then(|v| v.to_str().ok())
        {
            // A date in the future is not a valid condition (RFC 7232
            // §3.3) and S3 ignores it: GetObject with an If-Modified-Since
            // decades ahead answers 200 with the object (recorded).
            if let Some(dt) = parse_http_date(since).filter(|dt| *dt <= Utc::now()) {
                if obj_time <= dt {
                    return Err(not_modified());
                }
            }
        }
    }

    Ok(())
}

pub(crate) fn etag_matches(condition: &str, obj_etag: &str) -> bool {
    let condition = condition.trim();
    if condition == "*" {
        return true;
    }
    let clean_etag = obj_etag.replace('"', "");
    // Split on comma to handle multi-value If-Match / If-None-Match
    for part in condition.split(',') {
        let part = part.trim().replace('"', "");
        if part == clean_etag {
            return true;
        }
    }
    false
}

pub(crate) fn parse_http_date(s: &str) -> Option<DateTime<Utc>> {
    // Try RFC 2822 format: "Sat, 01 Jan 2000 00:00:00 GMT"
    if let Ok(dt) = DateTime::parse_from_rfc2822(s) {
        return Some(dt.with_timezone(&Utc));
    }
    // Try RFC 3339
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }
    // Try common HTTP date format: "%a, %d %b %Y %H:%M:%S GMT"
    if let Ok(dt) =
        chrono::NaiveDateTime::parse_from_str(s.trim_end_matches(" GMT"), "%a, %d %b %Y %H:%M:%S")
    {
        return Some(dt.and_utc());
    }
    // Try ISO 8601
    if let Ok(dt) = s.parse::<DateTime<Utc>>() {
        return Some(dt);
    }
    None
}

pub(crate) fn not_modified() -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::NOT_MODIFIED, "304", "Not Modified")
}

pub(crate) fn not_modified_with_etag(etag: &str) -> AwsServiceError {
    AwsServiceError::aws_error_with_headers(
        StatusCode::NOT_MODIFIED,
        "304",
        "Not Modified",
        vec![("etag".to_string(), etag.to_string())],
    )
}

pub(crate) fn precondition_failed(condition: &str) -> AwsServiceError {
    AwsServiceError::aws_error_with_fields(
        StatusCode::PRECONDITION_FAILED,
        "PreconditionFailed",
        "At least one of the pre-conditions you specified did not hold",
        vec![("Condition".to_string(), condition.to_string())],
    )
}

// ACL helpers

pub(crate) fn build_acl_xml(owner_id: &str, grants: &[AclGrant], _account_id: &str) -> String {
    let mut grants_xml = String::new();
    for g in grants {
        let grantee_xml = if g.grantee_type == "Group" {
            let uri = g.grantee_uri.as_deref().unwrap_or("");
            format!(
                "<Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\">\
                 <URI>{}</URI></Grantee>",
                xml_escape(uri),
            )
        } else if g.grantee_type == "AmazonCustomerByEmail" {
            let email = g.grantee_display_name.as_deref().unwrap_or("");
            format!(
                "<Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"AmazonCustomerByEmail\">\
                 <EmailAddress>{}</EmailAddress></Grantee>",
                xml_escape(email),
            )
        } else {
            let id = g.grantee_id.as_deref().unwrap_or("");
            format!(
                "<Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">\
                 <ID>{}</ID></Grantee>",
                xml_escape(id),
            )
        };
        grants_xml.push_str(&format!(
            "<Grant>{grantee_xml}<Permission>{}</Permission></Grant>",
            xml_escape(&g.permission),
        ));
    }

    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Owner><ID>{owner_id}</ID><DisplayName>{owner_id}</DisplayName></Owner>\
         <AccessControlList>{grants_xml}</AccessControlList>\
         </AccessControlPolicy>",
        owner_id = xml_escape(owner_id),
    )
}

/// The canned ACLs an object accepts, per `com.amazonaws.s3#ObjectCannedACL`.
/// `log-delivery-write` is bucket-only and is not among them.
pub(crate) const OBJECT_CANNED_ACLS: [&str; 7] = [
    "private",
    "public-read",
    "public-read-write",
    "authenticated-read",
    "aws-exec-read",
    "bucket-owner-read",
    "bucket-owner-full-control",
];

/// Reject a canned ACL an object does not accept. An unrecognized value used to
/// fall through `canned_acl_grants`' catch-all to owner-only, silently wiping
/// the object's grants with a 200.
pub(crate) fn validate_object_canned_acl(acl: &str) -> Result<(), AwsServiceError> {
    if OBJECT_CANNED_ACLS.contains(&acl) {
        return Ok(());
    }
    Err(AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidArgument",
        format!("Invalid x-amz-acl value: {acl}"),
    ))
}

/// The values `x-amz-object-ownership` accepts, per
/// `com.amazonaws.s3#ObjectOwnership`. Matched case-sensitively, as S3 does.
pub(crate) const OBJECT_OWNERSHIP_VALUES: [&str; 3] = [
    "BucketOwnerPreferred",
    "ObjectWriter",
    "BucketOwnerEnforced",
];

/// The canned ACLs a bucket accepts. `com.amazonaws.s3#BucketCannedACL` lists
/// only the first four; `log-delivery-write` is bucket-scoped in the S3
/// canned-ACL table (it is how a server-access-logging target is set up), so
/// the header takes it even though the modeled enum omits it.
///
/// `aws-exec-read` is bucket-scoped too (CloudFormation's
/// `AWS::S3::Bucket` `AccessControl: AwsExecRead` maps to it), and AWS answers
/// 200, so refusing it would turn a working call into a hard failure. Its grant
/// is a READ to the EC2 service's canonical user, which this build cannot
/// reproduce, so it resolves to owner-only -- [`acl_reaches_past_owner`] special-
/// cases it, since the request does ask for an ACL reaching past the owner. That
/// drives both the BucketOwnerEnforced conflict check and the `s3:PutBucketAcl`
/// authorization a create carrying it requires.
///
/// `bucket-owner-read` and `bucket-owner-full-control` are object-scoped, and
/// the canned-ACL table says S3 IGNORES them when they are given on a bucket
/// rather than refusing the call -- so they are accepted here and resolve to
/// the owner's FULL_CONTROL, which is what ignoring them produces. Rejecting
/// them would have turned a working call into a hard failure on a claim about
/// live S3 this build cannot check. A value S3 does not define at all is still
/// refused, since that is a typo silently wiping the bucket's grants.
pub(crate) const BUCKET_CANNED_ACLS: [&str; 8] = [
    "private",
    "public-read",
    "public-read-write",
    "authenticated-read",
    "aws-exec-read",
    "log-delivery-write",
    "bucket-owner-read",
    "bucket-owner-full-control",
];

/// Whether the ACL a request asks for reaches past the bucket owner.
///
/// Both the `BucketOwnerEnforced` conflict check and the `s3:PutBucketAcl`
/// authorization turn on this one question, so they share the answer rather
/// than each re-deriving it -- they disagreed once already, and the IAM side
/// silently authorized an ACL the conflict check treats as reaching outside the
/// owner.
///
/// `aws-exec-read` is the case that needs saying out loud: its READ grant to
/// the EC2 service's canonical user is not modeled, so [`canned_acl_grants`]
/// resolves it to owner-only -- but the request does ask for an ACL reaching
/// outside the owner.
///
/// NOT the same question as the object-write check in
/// [`S3Service::resolve_write_acl_headers`], which asks whether the request names
/// bucket-owner-full-control specifically. The two look alike and must not be
/// unified: a BucketOwnerEnforced BUCKET rejects an ACL that grants another
/// account anything, so `private` is fine there, while a BucketOwnerEnforced
/// bucket accepts an object PUT only when it specifies no ACL at all or
/// bucket-owner-full-control -- a value that is object-scoped in the model
/// (`com.amazonaws.s3#BucketCannedACL` omits it), so the bucket-side question
/// never has this answer. The header is still accepted on a bucket, resolving to
/// the owner's FULL_CONTROL, which is what S3 ignoring it produces.
/// Whether a bucket created with canned ACL `acl` needs an `acl.toml`.
///
/// `private` grants the owner FULL_CONTROL and nobody else, and
/// `aws-exec-read` resolves to the same here (its READ grant to the EC2
/// service's canonical user is not modeled): exactly the grant the loader
/// reconstructs for a bucket with no sidecar. Writing one would record an
/// explicit ACL the bucket does not have. Shared by CreateBucket and the
/// CloudFormation `AccessControl` property.
pub(crate) fn canned_acl_needs_sidecar(acl: &str) -> bool {
    !matches!(acl, "private" | "aws-exec-read")
}

pub(crate) fn acl_reaches_past_owner(acl: &str, owner_id: &str) -> bool {
    acl == "aws-exec-read"
        || !canned_acl_grants(acl, owner_id).iter().all(|g| {
            g.permission == "FULL_CONTROL"
                && g.grantee_type == "CanonicalUser"
                && g.grantee_id.as_deref() == Some(owner_id)
        })
}

pub(crate) fn canned_acl_grants(acl: &str, owner_id: &str) -> Vec<AclGrant> {
    let owner_grant = AclGrant {
        grantee_type: "CanonicalUser".to_string(),
        grantee_id: Some(owner_id.to_string()),
        grantee_display_name: Some(owner_id.to_string()),
        grantee_uri: None,
        permission: "FULL_CONTROL".to_string(),
    };
    match acl {
        "private" => vec![owner_grant],
        "public-read" => vec![
            owner_grant,
            AclGrant {
                grantee_type: "Group".to_string(),
                grantee_id: None,
                grantee_display_name: None,
                grantee_uri: Some("http://acs.amazonaws.com/groups/global/AllUsers".to_string()),
                permission: "READ".to_string(),
            },
        ],
        "public-read-write" => vec![
            owner_grant,
            AclGrant {
                grantee_type: "Group".to_string(),
                grantee_id: None,
                grantee_display_name: None,
                grantee_uri: Some("http://acs.amazonaws.com/groups/global/AllUsers".to_string()),
                permission: "READ".to_string(),
            },
            AclGrant {
                grantee_type: "Group".to_string(),
                grantee_id: None,
                grantee_display_name: None,
                grantee_uri: Some("http://acs.amazonaws.com/groups/global/AllUsers".to_string()),
                permission: "WRITE".to_string(),
            },
        ],
        "authenticated-read" => vec![
            owner_grant,
            AclGrant {
                grantee_type: "Group".to_string(),
                grantee_id: None,
                grantee_display_name: None,
                grantee_uri: Some(
                    "http://acs.amazonaws.com/groups/global/AuthenticatedUsers".to_string(),
                ),
                permission: "READ".to_string(),
            },
        ],
        // Grants the log-delivery group what it needs to write access logs into
        // the bucket, which is the entire point of this canned ACL.
        "log-delivery-write" => vec![
            owner_grant,
            AclGrant {
                grantee_type: "Group".to_string(),
                grantee_id: None,
                grantee_display_name: None,
                grantee_uri: Some("http://acs.amazonaws.com/groups/s3/LogDelivery".to_string()),
                permission: "WRITE".to_string(),
            },
            AclGrant {
                grantee_type: "Group".to_string(),
                grantee_id: None,
                grantee_display_name: None,
                grantee_uri: Some("http://acs.amazonaws.com/groups/s3/LogDelivery".to_string()),
                permission: "READ_ACP".to_string(),
            },
        ],
        // `bucket-owner-read` / `bucket-owner-full-control` on an object owned
        // by the caller come out as the owner's FULL_CONTROL.
        "bucket-owner-full-control" => vec![owner_grant],
        _ => vec![owner_grant],
    }
}

pub(crate) fn canned_acl_grants_for_object(acl: &str, owner_id: &str) -> Vec<AclGrant> {
    // For objects, canned ACLs work the same way
    canned_acl_grants(acl, owner_id)
}

/// The `x-amz-grant-*` headers and the ACL permission each one grants.
pub(crate) const GRANT_HEADER_PERMISSIONS: [(&str, &str); 5] = [
    ("x-amz-grant-read", "READ"),
    ("x-amz-grant-write", "WRITE"),
    ("x-amz-grant-read-acp", "READ_ACP"),
    ("x-amz-grant-write-acp", "WRITE_ACP"),
    ("x-amz-grant-full-control", "FULL_CONTROL"),
];

/// How many grantee clauses the `x-amz-grant-*` headers carry, counting every
/// comma-separated entry whether or not [`parse_grant_headers`] can turn it
/// into a grant.
///
/// That function skips a clause it cannot make a usable grantee of -- an `id=`
/// or `uri=` with nothing after it, or a key it does not recognize at all -- so
/// a caller about to STORE the parsed grants as the authoritative ACL compares
/// the two counts: fewer grants than clauses means the request asked for
/// something the stored ACL would not express.
pub(crate) fn grant_header_clause_count(headers: &HeaderMap) -> usize {
    let mut clauses = 0;
    for (header, _) in &GRANT_HEADER_PERMISSIONS {
        // `get_all`, not `get`: a repeated header carries more than one value,
        // and counting only the first would let the extras be dropped silently
        // -- the very thing the count exists to catch.
        for value in headers.get_all(*header) {
            match value.to_str() {
                Ok(value) => {
                    clauses += value
                        .split(',')
                        .filter(|part| !part.trim().is_empty())
                        .count();
                }
                // Header values are opaque bytes. `parse_grant_headers` cannot
                // read this one either, so counting it as zero would let the
                // counts agree and the grant disappear -- count it as a clause
                // that did not resolve.
                Err(_) => clauses += 1,
            }
        }
    }
    clauses
}

/// Whether the request carries any of the recognized `x-amz-grant-*` headers.
/// Matched by exact name, not by prefix, so an unknown `x-amz-grant-…` header
/// is ignored rather than turning a valid create into an error.
pub(crate) fn has_grant_headers(headers: &HeaderMap) -> bool {
    // A present-but-blank value is what a client sends for an unset config
    // field, and S3 treats it as absent. Testing presence alone turned that
    // into a hard rejection on every ACL-setting operation.
    GRANT_HEADER_PERMISSIONS.iter().any(|(header, _)| {
        headers
            .get_all(*header)
            .iter()
            .any(|v| v.to_str().map(|s| !s.trim().is_empty()).unwrap_or(true))
    })
}

/// Resolve the `x-amz-grant-*` headers into grants, refusing the request when
/// any grantee clause does not resolve into one.
///
/// [`parse_grant_headers`] skips a clause it cannot make a grantee of -- a key
/// with an empty value, or one it does not recognize -- so a caller that STORES
/// the result as the authoritative ACL would persist a permission set nobody
/// asked for, and a request where nothing resolves would store an ACL with not
/// even an owner entry. Every ACL-setting path goes through here so they all
/// agree. (`emailAddress=` IS resolved, into an AmazonCustomerByEmail grantee.)
pub(crate) fn resolved_grant_headers(
    headers: &HeaderMap,
) -> Result<Vec<AclGrant>, AwsServiceError> {
    let grants = parse_grant_headers(headers);
    if grants.is_empty() || grants.len() != grant_header_clause_count(headers) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
            "Argument format not recognized: grantees must be specified as id=<canonical-id> or uri=<group-uri>",
        ));
    }
    Ok(grants)
}

pub(crate) fn parse_grant_headers(headers: &HeaderMap) -> Vec<AclGrant> {
    let mut grants = Vec::new();
    let header_permission_map = GRANT_HEADER_PERMISSIONS;

    for (header, permission) in &header_permission_map {
        for value in headers.get_all(*header) {
            let Ok(value) = value.to_str() else { continue };
            // Parse "id=xxx" or "uri=xxx" or "emailAddress=xxx"
            for part in value.split(',') {
                let part = part.trim();
                if let Some((key, val)) = part.split_once('=') {
                    // Quotes first, then trim: trimming first leaves `"  "` as
                    // two spaces, which the emptiness checks below would accept
                    // as a grantee naming somebody.
                    let val = val.trim().trim_matches('"').trim();
                    let key = key.trim().to_lowercase();
                    // `id=` / `uri=` with nothing after it names no grantee.
                    // Emitting a grant with an empty id would store an ACL
                    // entry that can never match anyone; leaving it out lets
                    // the caller's clause count catch the request instead.
                    if val.is_empty() {
                        continue;
                    }
                    match key.as_str() {
                        "id" => {
                            grants.push(AclGrant {
                                grantee_type: "CanonicalUser".to_string(),
                                grantee_id: Some(val.to_string()),
                                grantee_display_name: Some(val.to_string()),
                                grantee_uri: None,
                                permission: permission.to_string(),
                            });
                        }
                        // S3 resolves an email to the account's canonical id.
                        // There is no directory to resolve against here, so the
                        // grant keeps the address as an AmazonCustomerByEmail
                        // grantee -- a real ACL grantee type -- rather than
                        // being dropped (which would silently lose a grant) or
                        // refused (which would fail a call AWS accepts).
                        //
                        // It is recorded, not enforced: every authorization path
                        // matches a Group URI or a canonical id, so this grant
                        // gives the named address no access. Say so rather than
                        // letting it look like working authorization.
                        "emailaddress" => {
                            tracing::warn!(
                                target: "fakecloud::s3",
                                email = %val,
                                permission = %permission,
                                "storing an email-addressed ACL grant that is not enforced: \
                                 email grantees are not resolved to a canonical user here, so \
                                 this grant conveys no access",
                            );
                            grants.push(AclGrant {
                                grantee_type: "AmazonCustomerByEmail".to_string(),
                                grantee_id: None,
                                grantee_display_name: Some(val.to_string()),
                                grantee_uri: None,
                                permission: permission.to_string(),
                            });
                        }
                        "uri" | "url" => {
                            grants.push(AclGrant {
                                grantee_type: "Group".to_string(),
                                grantee_id: None,
                                grantee_display_name: None,
                                grantee_uri: Some(val.to_string()),
                                permission: permission.to_string(),
                            });
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    grants
}

pub(crate) fn parse_acl_xml(xml: &str) -> Result<Vec<AclGrant>, AwsServiceError> {
    // Check for Owner presence
    if xml.contains("<AccessControlPolicy") && !xml.contains("<Owner>") {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "MalformedACLError",
            "The XML you provided was not well-formed or did not validate against our published schema",
        ));
    }

    let valid_permissions = ["READ", "WRITE", "READ_ACP", "WRITE_ACP", "FULL_CONTROL"];

    let mut grants = Vec::new();
    let mut remaining = xml;
    while let Some(start) = remaining.find("<Grant>") {
        let after = &remaining[start + 7..];
        if let Some(end) = after.find("</Grant>") {
            let grant_body = &after[..end];

            // Extract permission
            let permission = extract_xml_value(grant_body, "Permission").unwrap_or_default();
            if !valid_permissions.contains(&permission.as_str()) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "MalformedACLError",
                    "The XML you provided was not well-formed or did not validate against our published schema",
                ));
            }

            // Determine grantee type
            if grant_body.contains("xsi:type=\"Group\"") || grant_body.contains("<URI>") {
                // Trimmed: `<URI>\n  http://...\n</URI>` is what a formatted
                // policy carries, and the padding would otherwise be stored as
                // part of the group URI and never match.
                let uri = extract_xml_value(grant_body, "URI")
                    .map(|u| u.trim().to_string())
                    .unwrap_or_default();
                // Same rule as an empty <ID>: a grant naming no group can never
                // match anyone, and it would be stored as the bucket's ACL.
                if uri.is_empty() {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "MalformedACLError",
                        "The XML you provided was not well-formed or did not validate against our published schema",
                    ));
                }
                grants.push(AclGrant {
                    grantee_type: "Group".to_string(),
                    grantee_id: None,
                    grantee_display_name: None,
                    grantee_uri: Some(uri),
                    permission,
                });
            } else if grant_body.contains("AmazonCustomerByEmail")
                || grant_body.contains("<EmailAddress>")
            {
                // S3 allows exactly one grantee identifier. A Grantee carrying
                // both an ID and an EmailAddress used to silently drop the
                // canonical id in favor of the (unenforced) email grant, and the
                // wrong one was the persisted one.
                if grant_body.contains("<ID>") {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "MalformedACLError",
                        "The XML you provided was not well-formed or did not validate against our published schema",
                    ));
                }
                // An email grantee carries <EmailAddress>, never <ID>. Reading it
                // as a CanonicalUser would store a grant with an empty id --
                // an entry that can never match anyone, which is exactly what
                // the header path refuses. This is reachable by feeding
                // GetBucketAcl output straight back into PutBucketAcl.
                let email = extract_xml_value(grant_body, "EmailAddress")
                    .map(|e| e.trim().to_string())
                    .unwrap_or_default();
                if email.is_empty() {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "MalformedACLError",
                        "The XML you provided was not well-formed or did not validate against our published schema",
                    ));
                }
                grants.push(AclGrant {
                    grantee_type: "AmazonCustomerByEmail".to_string(),
                    grantee_id: None,
                    grantee_display_name: Some(email),
                    grantee_uri: None,
                    permission,
                });
            } else {
                // Trimmed for the same reason: a pretty-printed canonical id
                // would otherwise be stored with its padding and never match
                // the owner in any authorization comparison.
                let id = extract_xml_value(grant_body, "ID")
                    .map(|i| i.trim().to_string())
                    .unwrap_or_default();
                if id.is_empty() {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "MalformedACLError",
                        "The XML you provided was not well-formed or did not validate against our published schema",
                    ));
                }
                let display = extract_xml_value(grant_body, "DisplayName")
                    .map(|d| d.trim().to_string())
                    .filter(|d| !d.is_empty())
                    .unwrap_or_else(|| id.clone());
                grants.push(AclGrant {
                    grantee_type: "CanonicalUser".to_string(),
                    grantee_id: Some(id),
                    grantee_display_name: Some(display),
                    grantee_uri: None,
                    permission,
                });
            }

            remaining = &after[end + 8..];
        } else {
            break;
        }
    }
    Ok(grants)
}

// Range helpers

pub(crate) enum RangeResult {
    Satisfiable { start: usize, end: usize },
    NotSatisfiable,
    Ignored,
}

pub(crate) fn parse_range_header(range_str: &str, total_size: usize) -> Option<RangeResult> {
    let range_str = range_str.strip_prefix("bytes=")?;
    let (start_str, end_str) = range_str.split_once('-')?;
    if start_str.is_empty() {
        let suffix_len: usize = end_str.parse().ok()?;
        if suffix_len == 0 || total_size == 0 {
            return Some(RangeResult::NotSatisfiable);
        }
        let start = total_size.saturating_sub(suffix_len);
        Some(RangeResult::Satisfiable {
            start,
            end: total_size - 1,
        })
    } else {
        let start: usize = start_str.parse().ok()?;
        if start >= total_size {
            return Some(RangeResult::NotSatisfiable);
        }
        let end = if end_str.is_empty() {
            total_size - 1
        } else {
            let e: usize = end_str.parse().ok()?;
            if e < start {
                return Some(RangeResult::Ignored);
            }
            std::cmp::min(e, total_size - 1)
        };
        Some(RangeResult::Satisfiable { start, end })
    }
}

// Helpers

/// S3 XML response with `application/xml` content type (unlike Query protocol's `text/xml`).
pub(crate) fn s3_xml(status: StatusCode, body: impl Into<Bytes>) -> AwsResponse {
    AwsResponse {
        status,
        content_type: "application/xml".to_string(),
        body: body.into().into(),
        headers: HeaderMap::new(),
    }
}

pub(crate) fn empty_response(status: StatusCode) -> AwsResponse {
    AwsResponse {
        status,
        content_type: "application/xml".to_string(),
        body: Bytes::new().into(),
        headers: HeaderMap::new(),
    }
}

/// Returns true when the object is stored in a "cold" storage class (GLACIER, DEEP_ARCHIVE)
/// and has NOT been restored (or restore is still in progress).
pub(crate) fn is_frozen(obj: &S3Object) -> bool {
    matches!(obj.storage_class.as_str(), "GLACIER" | "DEEP_ARCHIVE")
        && obj.restore_ongoing != Some(false)
}

pub(crate) fn no_such_bucket(bucket: &str) -> AwsServiceError {
    AwsServiceError::aws_error_with_fields(
        StatusCode::NOT_FOUND,
        "NoSuchBucket",
        "The specified bucket does not exist",
        vec![("BucketName".to_string(), bucket.to_string())],
    )
}

pub(crate) fn no_such_key(key: &str) -> AwsServiceError {
    AwsServiceError::aws_error_with_fields(
        StatusCode::NOT_FOUND,
        "NoSuchKey",
        "The specified key does not exist.",
        vec![("Key".to_string(), key.to_string())],
    )
}

pub(crate) fn no_such_upload(upload_id: &str) -> AwsServiceError {
    AwsServiceError::aws_error_with_fields(
        StatusCode::NOT_FOUND,
        "NoSuchUpload",
        "The specified upload does not exist. The upload ID may be invalid, \
         or the upload may have been aborted or completed.",
        vec![("UploadId".to_string(), upload_id.to_string())],
    )
}

pub(crate) fn no_such_key_with_detail(key: &str) -> AwsServiceError {
    AwsServiceError::aws_error_with_fields(
        StatusCode::NOT_FOUND,
        "NoSuchKey",
        "The specified key does not exist.",
        vec![("Key".to_string(), key.to_string())],
    )
}

pub(crate) fn compute_md5(data: &[u8]) -> String {
    let digest = Md5::digest(data);
    format!("{:x}", digest)
}

pub(crate) fn url_encode_s3_key(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 2);
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(byte as char);
            }
            _ => {
                out.push_str(&format!("%{:02X}", byte));
            }
        }
    }
    out
}

pub(crate) use fakecloud_aws::xml::xml_escape;

/// The `x-amz-meta-*` user metadata on a request, keyed without the prefix.
/// Values are RFC 2047-decoded the way S3 does before storing them, so a
/// non-ASCII value is kept rather than dropped (see
/// [`fakecloud_core::rfc2047::decode`]).
pub(crate) fn extract_user_metadata(
    headers: &HeaderMap,
) -> std::collections::BTreeMap<String, String> {
    let mut meta = std::collections::BTreeMap::new();
    for (name, value) in headers {
        if let Some(key) = name.as_str().strip_prefix("x-amz-meta-") {
            meta.insert(
                key.to_string(),
                fakecloud_core::rfc2047::decode(value.as_bytes()),
            );
        }
    }
    meta
}

/// Emit stored user metadata as `x-amz-meta-*` response headers. A value
/// that is not pure US-ASCII is RFC 2047-encoded, as S3 returns it.
pub(crate) fn insert_user_metadata_headers(
    headers: &mut HeaderMap,
    metadata: &std::collections::BTreeMap<String, String>,
) {
    for (k, v) in metadata {
        if let (Ok(name), Ok(val)) = (
            format!("x-amz-meta-{k}").parse::<http::header::HeaderName>(),
            http::HeaderValue::from_str(&fakecloud_core::rfc2047::encode(v)),
        ) {
            headers.insert(name, val);
        }
    }
}

/// Every value of the Smithy `StorageClass` enum. Keep this in step with
/// `aws-models/s3.json` — a class AWS models but this list omits is rejected
/// with `InvalidStorageClass` on a request AWS accepts.
pub(crate) fn is_valid_storage_class(class: &str) -> bool {
    matches!(
        class,
        "STANDARD"
            | "REDUCED_REDUNDANCY"
            | "STANDARD_IA"
            | "ONEZONE_IA"
            | "INTELLIGENT_TIERING"
            | "GLACIER"
            | "DEEP_ARCHIVE"
            | "GLACIER_IR"
            | "OUTPOSTS"
            | "SNOW"
            | "EXPRESS_ONEZONE"
            | "FSX_OPENZFS"
            | "FSX_ONTAP"
            | "AWS_BACKUP_WARM"
            | "AWS_BACKUP_LOW_COST_WARM"
    )
}

pub(crate) fn is_valid_bucket_name(name: &str) -> bool {
    // General-purpose bucket naming rules (AWS S3):
    // https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
    if name.len() < 3 || name.len() > 63 {
        return false;
    }
    // Only lowercase letters, digits, hyphens, and dots. Underscores are NOT
    // permitted for general-purpose buckets.
    if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
    {
        return false;
    }
    // Must start and end with a letter or number.
    let bytes = name.as_bytes();
    if !bytes[0].is_ascii_alphanumeric() || !bytes[bytes.len() - 1].is_ascii_alphanumeric() {
        return false;
    }
    // Must not contain two adjacent periods.
    if name.contains("..") {
        return false;
    }
    // Must not be formatted as an IPv4 address (e.g. 192.168.5.4).
    if is_ipv4_format(name) {
        return false;
    }
    // Reserved prefixes / suffixes.
    if name.starts_with("xn--")
        || name.starts_with("sthree-")
        || name.starts_with("amzn-s3-demo-")
        || name.ends_with("-s3alias")
        || name.ends_with("--ol-s3")
        || name.ends_with(".mrap")
    {
        return false;
    }
    true
}

/// True when `name` is four dot-separated decimal octets (0-255), i.e. looks
/// like an IPv4 address, which S3 forbids as a bucket name.
fn is_ipv4_format(name: &str) -> bool {
    let parts: Vec<&str> = name.split('.').collect();
    parts.len() == 4
        && parts.iter().all(|p| {
            !p.is_empty()
                && p.len() <= 3
                && p.bytes().all(|b| b.is_ascii_digit())
                && p.parse::<u8>().is_ok()
        })
}

/// Every value of the Smithy `BucketLocationConstraint` enum in
/// `aws-models/s3.json` (kept in step by
/// `bucket_location_constraints_cover_the_model`), plus the partitions the
/// model omits but S3 serves (us-gov-east-2 and the isolated regions).
/// `us-east-1` is absent: it is the default and is rejected as an explicit
/// constraint before this check runs.
const BUCKET_LOCATION_CONSTRAINTS: &[&str] = &[
    "af-south-1",
    "ap-east-1",
    "ap-east-2",
    "ap-northeast-1",
    "ap-northeast-2",
    "ap-northeast-3",
    "ap-south-1",
    "ap-south-2",
    "ap-southeast-1",
    "ap-southeast-2",
    "ap-southeast-3",
    "ap-southeast-4",
    "ap-southeast-5",
    "ap-southeast-6",
    "ap-southeast-7",
    "ca-central-1",
    "ca-west-1",
    "cn-north-1",
    "cn-northwest-1",
    "EU",
    "eu-central-1",
    "eu-central-2",
    "eu-north-1",
    "eu-south-1",
    "eu-south-2",
    "eu-west-1",
    "eu-west-2",
    "eu-west-3",
    "il-central-1",
    "me-central-1",
    "me-south-1",
    "mx-central-1",
    "sa-east-1",
    "us-east-2",
    "us-gov-east-1",
    "us-gov-west-1",
    "us-west-1",
    "us-west-2",
    // Not in the model's enum, but real S3 partitions.
    "us-gov-east-2",
    "us-iso-east-1",
    "us-iso-west-1",
    "us-isob-east-1",
    "us-isof-south-1",
];

pub(crate) fn is_valid_region(region: &str) -> bool {
    region == "us-east-1" || BUCKET_LOCATION_CONSTRAINTS.contains(&region)
}

/// The region a location constraint places a bucket in. `EU` is the legacy
/// alias S3 still accepts for eu-west-1.
pub(crate) fn location_constraint_region(constraint: &str) -> &str {
    if constraint == "EU" {
        "eu-west-1"
    } else {
        constraint
    }
}

/// The account a KMS key reference names: the account field of a key or
/// alias ARN. `None` for a bare key id, alias name or `aws/<service>`
/// reference, which resolves in the account using it.
pub(crate) fn kms_reference_account(key: &str) -> Option<&str> {
    fakecloud_kms::parse_kms_arn(key)
        .map(|(_, account, _)| account)
        .filter(|a| !a.is_empty())
}

/// The S3 error for a failed KMS hook call. Recorded (PutObject with SSE-KMS):
/// a key policy refusal is 403 `AccessDenied` and a missing key 400
/// `KMS.NotFoundException`, each with KMS's own message; S3 relays the other
/// KMS failures as `KMS.<KMS error code>` (400), with KMS's message. A KMS
/// internal failure is a 500.
pub(crate) fn kms_hook_error(err: &fakecloud_core::delivery::KmsError) -> AwsServiceError {
    use fakecloud_core::delivery::KmsErrorKind;
    match err.kind {
        KmsErrorKind::AccessDenied => {
            AwsServiceError::aws_error(StatusCode::FORBIDDEN, "AccessDenied", err.message.clone())
        }
        KmsErrorKind::Internal => AwsServiceError::aws_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            err.message.clone(),
        ),
        kind => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            format!("KMS.{}", kind.code()),
            err.message.clone(),
        ),
    }
}

/// An `x-amz-copy-source` value, parsed once for both the copy handlers and
/// the copy's source authorization so they always name the same object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CopySource {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<String>,
}

/// Parse `[/]bucket/key[?versionId=v]`. The query starts at the first raw
/// `?`, split BEFORE percent-decoding, so a key containing `?` (sent, as S3
/// requires, URL-encoded as `%3F`) keeps it. `None` when the value has no
/// `bucket/key` shape.
pub(crate) fn parse_copy_source(header: &str) -> Option<CopySource> {
    let raw = header.strip_prefix('/').unwrap_or(header);
    let (raw_path, version_id) = match raw.split_once('?') {
        Some((path, query)) => (
            path,
            query
                .split('&')
                .find_map(|p| p.strip_prefix("versionId="))
                .map(|s| s.to_string()),
        ),
        None => (raw, None),
    };
    let decoded = percent_encoding::percent_decode_str(raw_path).decode_utf8_lossy();
    let (bucket, key) = decoded.split_once('/')?;
    Some(CopySource {
        bucket: bucket.to_string(),
        key: key.to_string(),
        version_id,
    })
}

pub(crate) fn resolve_object<'a>(
    b: &'a S3Bucket,
    key: &str,
    version_id: Option<&String>,
) -> Result<&'a S3Object, AwsServiceError> {
    if let Some(vid) = version_id {
        // "null" version ID refers to an object with no version_id (pre-versioning)
        if vid == "null" {
            let is_null =
                |o: &S3Object| o.version_id.is_none() || o.version_id.as_deref() == Some("null");
            // A live null object wins over a null delete marker: the marker is
            // only the null version until a write takes the slot back, and a
            // key with no version history at all keeps its null object in
            // `objects` alone. A marker is returned only when it is the only
            // null entry, where it correctly reports the key as deleted.
            if let Some(versions) = b.object_versions.get(key) {
                if let Some(obj) = versions.iter().find(|o| is_null(o) && !o.is_delete_marker) {
                    return Ok(obj);
                }
            }
            if let Some(obj) = b.objects.get(key) {
                if is_null(obj) && !obj.is_delete_marker {
                    return Ok(obj);
                }
            }
            if let Some(versions) = b.object_versions.get(key) {
                if let Some(obj) = versions.iter().find(|o| is_null(o)) {
                    return Ok(obj);
                }
            }
            if let Some(obj) = b.objects.get(key) {
                if is_null(obj) {
                    return Ok(obj);
                }
            }
        } else {
            // When a specific versionId is requested, check versions first
            if let Some(versions) = b.object_versions.get(key) {
                if let Some(obj) = versions
                    .iter()
                    .find(|o| o.version_id.as_deref() == Some(vid.as_str()))
                {
                    return Ok(obj);
                }
            }
            // Also check current object
            if let Some(obj) = b.objects.get(key) {
                if obj.version_id.as_deref() == Some(vid.as_str()) {
                    return Ok(obj);
                }
            }
        }
        // For versioned buckets, return NoSuchVersion; for non-versioned, return 400
        if b.versioning.is_some() {
            Err(AwsServiceError::aws_error_with_fields(
                StatusCode::NOT_FOUND,
                "NoSuchVersion",
                "The specified version does not exist.",
                vec![
                    ("Key".to_string(), key.to_string()),
                    ("VersionId".to_string(), vid.to_string()),
                ],
            ))
        } else {
            Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Invalid version id specified",
            ))
        }
    } else {
        b.objects.get(key).ok_or_else(|| no_such_key(key))
    }
}

pub(crate) fn make_delete_marker(key: &str, dm_id: &str) -> S3Object {
    S3Object {
        key: key.to_string(),
        last_modified: Utc::now(),
        storage_class: "STANDARD".to_string(),
        version_id: Some(dm_id.to_string()),
        is_delete_marker: true,
        ..Default::default()
    }
}

/// Represents an object to delete in a batch delete request.
pub(crate) struct DeleteObjectEntry {
    key: String,
    version_id: Option<String>,
}

pub(crate) fn parse_delete_objects_xml(xml: &str) -> Vec<DeleteObjectEntry> {
    let mut entries = Vec::new();
    let mut remaining = xml;
    while let Some(obj_start) = remaining.find("<Object>") {
        let after = &remaining[obj_start + 8..];
        if let Some(obj_end) = after.find("</Object>") {
            let obj_body = &after[..obj_end];
            let key = extract_xml_value(obj_body, "Key");
            let version_id = extract_xml_value(obj_body, "VersionId");
            if let Some(k) = key {
                entries.push(DeleteObjectEntry { key: k, version_id });
            }
            remaining = &after[obj_end + 9..];
        } else {
            break;
        }
    }
    entries
}

/// Returns true when the request body's top-level <Quiet> element is "true".
/// AWS docs: when Quiet=true, only failed deletes are emitted.
pub(crate) fn parse_delete_objects_quiet(xml: &str) -> bool {
    extract_xml_value(xml, "Quiet")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Extract the `<Tags>` tag set from a `CreateBucketConfiguration` body.
///
/// The tag set was added to `CreateBucketConfiguration` in the 2025 S3 API;
/// it is serialized as `<Tags><Tag><Key/><Value/></Tag></Tags>`, the same
/// `Tag` shape `PutBucketTagging` uses, read by the same reader
/// ([`tags::read_tag_set`]). A configuration that does not read (not
/// well-formed, a `Tag` missing its `Key` or `Value`) is 400 MalformedXML,
/// as for PutBucketTagging: creating the bucket untagged would drop tags the
/// caller asked for, and authorize the create without them. A body that is
/// no `CreateBucketConfiguration` at all names no tags, which the create
/// handler judges as it always has (by its location constraint).
pub(crate) fn create_bucket_configuration_tags(
    body: &str,
) -> Result<Vec<(String, String)>, AwsServiceError> {
    if !body.contains("CreateBucketConfiguration") {
        return Ok(Vec::new());
    }
    tags::read_tag_set(body, "CreateBucketConfiguration", "Tags")
}

pub(crate) fn validate_tags(tags: &[(String, String)]) -> Result<(), AwsServiceError> {
    // Check for duplicate keys
    let mut seen = std::collections::HashSet::new();
    for (k, _) in tags {
        if !seen.insert(k.as_str()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidTag",
                "Cannot provide multiple Tags with the same key",
            ));
        }
        // Check for aws: prefix
        if k.starts_with("aws:") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidTag",
                "System tags cannot be added/updated by requester",
            ));
        }
    }
    Ok(())
}

pub(crate) fn extract_xml_value(xml: &str, tag: &str) -> Option<String> {
    // Handle self-closing tags like <Value /> or <Value/>
    let self_closing1 = format!("<{tag} />");
    let self_closing2 = format!("<{tag}/>");
    if xml.contains(&self_closing1) || xml.contains(&self_closing2) {
        // Check if the self-closing tag appears before any open+close pair
        let self_pos = xml
            .find(&self_closing1)
            .or_else(|| xml.find(&self_closing2));
        let open = format!("<{tag}>");
        let open_pos = xml.find(&open);
        match (self_pos, open_pos) {
            (Some(sp), Some(op)) if sp < op => return Some(String::new()),
            (Some(_), None) => return Some(String::new()),
            _ => {}
        }
    }

    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    // Search for the closing tag AFTER the opening one so a malformed body
    // (closing tag before opening) can't make end < start and panic the
    // slice (bug-audit 2026-05-28, 2.2).
    let end = xml[start..].find(&close)? + start;
    // Decode XML entities in the text node. S3 request bodies arrive
    // XML-encoded — every SDK escapes `&`, `<`, `>` (and sometimes `"`/`'`
    // plus numeric refs) — so a key/value like `a&b` is on the wire as
    // `a&amp;b`. Without decoding, an object keyed `a&b` (the key arrives
    // URL-decoded from the request path) is never matched by a
    // DeleteObjects/PutObjectTagging body, and a tag value round-trips with
    // an extra layer of escaping on every GET.
    Some(decode_xml_entities(&xml[start..end]))
}

/// Decode the standard XML entities in a text node value using a single
/// left-to-right pass. A chain of `str::replace` calls would double-decode
/// (e.g. `&amp;lt;` — the escaped literal text `&lt;` — must decode to
/// `&lt;`, not to `<`), so the scan resolves each `&…;` exactly once.
/// Handles the named entities (`amp`, `lt`, `gt`, `quot`, `apos`) and both
/// decimal (`&#38;`) and hex (`&#x26;`) numeric character references; an
/// unrecognized `&…` sequence is emitted verbatim, matching how lenient XML
/// readers treat a bare ampersand.
pub(crate) fn decode_xml_entities(s: &str) -> String {
    if !s.contains('&') {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < s.len() {
        if bytes[i] == b'&' {
            // Bound the entity name so a stray '&' followed by a distant ';'
            // doesn't swallow a long span of text.
            if let Some(rel) = s[i + 1..].find(';') {
                if rel <= 10 {
                    let entity = &s[i + 1..i + 1 + rel];
                    let decoded = match entity {
                        "amp" => Some('&'),
                        "lt" => Some('<'),
                        "gt" => Some('>'),
                        "quot" => Some('"'),
                        "apos" => Some('\''),
                        _ => entity
                            .strip_prefix("#x")
                            .or_else(|| entity.strip_prefix("#X"))
                            .and_then(|hex| u32::from_str_radix(hex, 16).ok())
                            .or_else(|| {
                                entity.strip_prefix('#').and_then(|d| d.parse::<u32>().ok())
                            })
                            .and_then(char::from_u32),
                    };
                    if let Some(c) = decoded {
                        out.push(c);
                        i += 1 + rel + 1;
                        continue;
                    }
                }
            }
            out.push('&');
            i += 1;
        } else {
            let ch = s[i..].chars().next().unwrap();
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    out
}

/// Parse the CompleteMultipartUpload XML body into (part_number, etag) pairs.
/// Strip the surrounding quotes AWS clients wrap a part ETag in, so the value
/// matches the bare hex ETag fakecloud stores. The quote character arrives in
/// several encodings depending on the client's XML serializer: a literal `"`
/// (aws-cli/boto), the named entity `&quot;`, or the numeric character
/// references `&#34;` / `&#x22;` (aws-sdk-go-v2 — the terraform provider). All
/// must be removed, else a valid part is rejected with `InvalidPart` on
/// CompleteMultipartUpload. (2026-07-07: go-SDK multipart uploads of large
/// objects, e.g. the KinesisAnalyticsV2 tfacc app JAR, failed on this.)
pub(crate) fn strip_etag_quotes(s: &str) -> String {
    s.replace("&quot;", "")
        .replace("&#34;", "")
        .replace("&#x22;", "")
        .replace("&#X22;", "")
        .replace('"', "")
        .trim()
        .to_string()
}

/// One `<Part>` of a CompleteMultipartUpload request body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompletedPartXml {
    pub part_number: u32,
    pub etag: String,
    /// The part checksums the client listed (`<ChecksumCRC32>` ...).
    pub checksums: Vec<(crate::checksum::ChecksumAlgorithm, String)>,
}

pub(crate) fn parse_complete_multipart_xml(xml: &str) -> Vec<CompletedPartXml> {
    let mut parts = Vec::new();
    let mut remaining = xml;
    while let Some(part_start) = remaining.find("<Part>") {
        let after = &remaining[part_start + 6..];
        if let Some(part_end) = after.find("</Part>") {
            let part_body = &after[..part_end];
            let part_num =
                extract_xml_value(part_body, "PartNumber").and_then(|s| s.parse::<u32>().ok());
            let etag = extract_xml_value(part_body, "ETag").map(|s| strip_etag_quotes(&s));
            if let (Some(num), Some(e)) = (part_num, etag) {
                let checksums = crate::checksum::ChecksumAlgorithm::ALL
                    .into_iter()
                    .filter_map(|a| {
                        extract_xml_value(part_body, a.xml_element())
                            .map(|v| (a, v.trim().to_string()))
                    })
                    .collect();
                parts.push(CompletedPartXml {
                    part_number: num,
                    etag: e,
                    checksums,
                });
            }
            remaining = &after[part_end + 7..];
        } else {
            break;
        }
    }
    parts
}

pub(crate) fn parse_url_encoded_tags(s: &str) -> Vec<(String, String)> {
    let mut tags = Vec::new();
    for pair in s.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = match pair.find('=') {
            Some(pos) => (&pair[..pos], &pair[pos + 1..]),
            None => (pair, ""),
        };
        tags.push((
            percent_encoding::percent_decode_str(key)
                .decode_utf8_lossy()
                .to_string(),
            percent_encoding::percent_decode_str(value)
                .decode_utf8_lossy()
                .to_string(),
        ));
    }
    tags
}

/// Validate lifecycle configuration XML. Returns MalformedXML on invalid configs.
pub(crate) fn validate_lifecycle_xml(xml: &str) -> Result<(), AwsServiceError> {
    let malformed = || {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against our published schema",
        )
    };

    let mut remaining = xml;
    while let Some(rule_start) = remaining.find("<Rule>") {
        let after = &remaining[rule_start + 6..];
        if let Some(rule_end) = after.find("</Rule>") {
            let rule_body = &after[..rule_end];

            // Must have Filter or Prefix
            let has_filter = rule_body.contains("<Filter>")
                || rule_body.contains("<Filter/>")
                || rule_body.contains("<Filter />");

            // Check for <Prefix> at rule level (outside of <Filter>...</Filter>)
            let has_prefix_outside_filter = {
                if !rule_body.contains("<Prefix") {
                    false
                } else if !has_filter {
                    true // No filter means any Prefix is at rule level
                } else {
                    // Remove the Filter block and check if Prefix remains
                    let mut stripped = rule_body.to_string();
                    // Remove <Filter>...</Filter> or self-closing variants
                    // The closing tag is searched after the opening one, so
                    // a `</Filter>` ahead of it cannot splice the wrong span.
                    if let Some(fs) = stripped.find("<Filter") {
                        if let Some(fe) = stripped[fs..].find("</Filter>").map(|e| fs + e) {
                            stripped = format!("{}{}", &stripped[..fs], &stripped[fe + 9..]);
                        }
                    }
                    stripped.contains("<Prefix")
                }
            };

            if !has_filter && !has_prefix_outside_filter {
                return Err(malformed());
            }
            // Can't have both Filter and rule-level Prefix
            if has_filter && has_prefix_outside_filter {
                return Err(malformed());
            }

            // Expiration: if has ExpiredObjectDeleteMarker, cannot also have Days or Date
            // (only check within <Expiration> block)
            if let Some(exp_start) = rule_body.find("<Expiration>") {
                if let Some(exp_end) = rule_body[exp_start..].find("</Expiration>") {
                    let exp_body = &rule_body[exp_start..exp_start + exp_end];
                    if exp_body.contains("<ExpiredObjectDeleteMarker>")
                        && (exp_body.contains("<Days>") || exp_body.contains("<Date>"))
                    {
                        return Err(malformed());
                    }
                }
            }

            // Filter validation
            if has_filter {
                if let Some(fs) = rule_body.find("<Filter>") {
                    if let Some(fe) = rule_body.find("</Filter>") {
                        // `find` locates the two tags independently, so a body
                        // with `</Filter>` before `<Filter>` would slice with
                        // begin > end and panic (dropping the connection -- a
                        // reachable DoS). Reject as malformed instead.
                        if fe < fs + 8 {
                            return Err(malformed());
                        }
                        let filter_body = &rule_body[fs + 8..fe];
                        let has_prefix_in_filter = filter_body.contains("<Prefix");
                        let has_tag_in_filter = filter_body.contains("<Tag>");
                        let has_and_in_filter = filter_body.contains("<And>");
                        // Can't have both Prefix and Tag without And
                        if has_prefix_in_filter && has_tag_in_filter && !has_and_in_filter {
                            return Err(malformed());
                        }
                        // Can't have Tag and And simultaneously at the Filter level
                        if has_tag_in_filter && has_and_in_filter {
                            // Check if the <Tag> is outside <And>
                            let and_start = filter_body.find("<And>").unwrap_or(0);
                            let tag_pos = filter_body.find("<Tag>").unwrap_or(0);
                            if tag_pos < and_start {
                                return Err(malformed());
                            }
                        }
                    }
                }
            }

            // NoncurrentVersionTransition must have NoncurrentDays and StorageClass
            if rule_body.contains("<NoncurrentVersionTransition>") {
                let mut nvt_remaining = rule_body;
                while let Some(nvt_start) = nvt_remaining.find("<NoncurrentVersionTransition>") {
                    let nvt_after = &nvt_remaining[nvt_start + 29..];
                    if let Some(nvt_end) = nvt_after.find("</NoncurrentVersionTransition>") {
                        let nvt_body = &nvt_after[..nvt_end];
                        if !nvt_body.contains("<NoncurrentDays>") {
                            return Err(malformed());
                        }
                        if !nvt_body.contains("<StorageClass>") {
                            return Err(malformed());
                        }
                        nvt_remaining = &nvt_after[nvt_end + 30..];
                    } else {
                        break;
                    }
                }
            }

            remaining = &after[rule_end + 7..];
        } else {
            break;
        }
    }

    Ok(())
}

/// Parsed CORS rule from bucket configuration XML.
pub(crate) struct CorsRule {
    allowed_origins: Vec<String>,
    allowed_methods: Vec<String>,
    allowed_headers: Vec<String>,
    expose_headers: Vec<String>,
    max_age_seconds: Option<u32>,
}

/// Remove `<!-- ... -->` spans so tag scanning sees only live markup.
///
/// Every scan of a CORS body goes through this, so validation and request-time
/// parsing always see the same markup — otherwise a commented-out rule could
/// validate as absent and then be honored at request time. An unterminated
/// comment discards the rest of the body, so whatever it opened is treated as
/// absent by both sides alike.
pub(crate) fn strip_xml_comments(xml: &str) -> String {
    let mut out = String::with_capacity(xml.len());
    let mut rest = xml;
    while let Some(start) = rest.find("<!--") {
        out.push_str(&rest[..start]);
        match rest[start + 4..].find("-->") {
            Some(end) => rest = &rest[start + 4 + end + 3..],
            None => return out,
        }
    }
    out.push_str(rest);
    out
}

/// Parse CORS configuration XML into rules.
pub(crate) fn parse_cors_config(xml: &str) -> Vec<CorsRule> {
    // Comments are stripped here, not only in `validate_cors_xml`: the stored
    // body is the raw text, so if the two disagreed a `<CORSRule>` inside an
    // `<!-- ... -->` would pass validation as absent and then go live at
    // request time, handing out an allow-origin the caller never wrote.
    let stripped = strip_xml_comments(xml);
    let mut rules = Vec::new();
    let mut remaining = stripped.as_str();
    while let Some(start) = remaining.find("<CORSRule>") {
        let after = &remaining[start + 10..];
        if let Some(end) = after.find("</CORSRule>") {
            let block = &after[..end];
            // Values are trimmed: `extract_all_xml_values` keeps the raw text,
            // so a pretty-printed config stores "\n  GET\n". `put_bucket_cors`
            // trims before validating and therefore accepts it, and an
            // untrimmed value matches no method, no origin, and parses to no
            // header — the bucket would go silently CORS-dead.
            // Empty values are dropped, not kept as `""`: an empty entry
            // matches nothing meaningful, and an empty `<AllowedMethod/>`
            // alongside a real one would otherwise match the `""` that a
            // preflight with no `Access-Control-Request-Method` falls back to,
            // approving a request that must be denied. `validate_cors_xml`
            // still rejects a rule left with no usable value at all.
            let trimmed = |tag| {
                extract_all_xml_values(block, tag)
                    .into_iter()
                    .map(|v| v.trim().to_string())
                    .filter(|v| !v.is_empty())
                    .collect::<Vec<_>>()
            };
            let allowed_origins = trimmed("AllowedOrigin");
            // Not uppercased: `put_bucket_cors` validates `<AllowedMethod>`
            // case-sensitively against the canonical verbs and rejects anything
            // else, so a stored config only ever holds `GET`/`PUT`/... — and
            // `Access-Control-Allow-Methods` is echoed from this list, which
            // browsers compare case-sensitively.
            let allowed_methods = trimmed("AllowedMethod");
            let allowed_headers = trimmed("AllowedHeader");
            let expose_headers = trimmed("ExposeHeader");
            // Trimmed for the same reason as the lists above: an untrimmed
            // "\n  3600\n" fails to parse, and the preflight would silently
            // ship without `Access-Control-Max-Age`, re-preflighting forever.
            let max_age_seconds =
                extract_xml_value(block, "MaxAgeSeconds").and_then(|s| s.trim().parse().ok());
            rules.push(CorsRule {
                allowed_origins,
                allowed_methods,
                allowed_headers,
                expose_headers,
                max_age_seconds,
            });
            remaining = &after[end + 11..];
        } else {
            break;
        }
    }
    rules
}

/// Match a requested header name against an `AllowedHeader` pattern.
///
/// AWS permits one `*` per `AllowedHeader`, and `x-amz-*` is a common config.
/// Header names are case-insensitive.
pub(crate) fn header_matches(header: &str, pattern: &str) -> bool {
    wildcard_matches(header, pattern, false)
}

/// Match `value` against a `*`-wildcard pattern.
///
/// `PutBucketCors` rejects a value with more than one `*`, but a config stored
/// by an earlier build or restored from a snapshot never passes that check, so
/// the matcher handles any number rather than depending on an invariant it
/// cannot enforce. A leading/trailing `*` matches an empty run, and segments
/// must appear in order without overlapping.
fn wildcard_matches(value: &str, pattern: &str, case_sensitive: bool) -> bool {
    let eq = |a: &str, b: &str| {
        if case_sensitive {
            a == b
        } else {
            a.eq_ignore_ascii_case(b)
        }
    };
    let mut segments = pattern.split('*');
    let Some(prefix) = segments.next() else {
        return false;
    };
    // No wildcard at all: the whole value has to match.
    let Some(mut rest) = value.strip_prefix_matching(prefix, &eq) else {
        return false;
    };
    if !pattern.contains('*') {
        return rest.is_empty();
    }
    let segments: Vec<&str> = segments.collect();
    let (last, middles) = segments.split_last().expect("pattern contains a wildcard");
    for seg in middles {
        match rest.find_matching(seg, &eq) {
            Some(at) => rest = &rest[at + seg.len()..],
            None => return false,
        }
    }
    // The final segment anchors to the end, and must not overlap what the
    // earlier segments already consumed. Boundary-checked like the helpers
    // above, so no arm of this function depends on the ASCII contract.
    let at = rest.len().checked_sub(last.len());
    at.is_some_and(|at| rest.is_char_boundary(at) && eq(&rest[at..], last))
}

/// Helpers keeping [`wildcard_matches`] readable. Both operate on ASCII, which
/// every caller guarantees: header values come from `HeaderValue::to_str`.
trait AsciiMatch {
    fn strip_prefix_matching<'a>(
        &'a self,
        p: &str,
        eq: &dyn Fn(&str, &str) -> bool,
    ) -> Option<&'a str>;
    fn find_matching(&self, needle: &str, eq: &dyn Fn(&str, &str) -> bool) -> Option<usize>;
}

impl AsciiMatch for str {
    fn strip_prefix_matching<'a>(
        &'a self,
        p: &str,
        eq: &dyn Fn(&str, &str) -> bool,
    ) -> Option<&'a str> {
        // Boundary-checked rather than relying on the ASCII contract: callers
        // satisfy it today, but a future one passing XML or query text would
        // otherwise get a request-killing panic on a split multi-byte char.
        (self.len() >= p.len() && self.is_char_boundary(p.len()) && eq(&self[..p.len()], p))
            .then(|| &self[p.len()..])
    }

    fn find_matching(&self, needle: &str, eq: &dyn Fn(&str, &str) -> bool) -> Option<usize> {
        if needle.is_empty() {
            return Some(0);
        }
        // Must precede the range: `saturating_sub` would otherwise yield `0..=0`
        // for a needle longer than the haystack, and the slice below would run
        // off the end.
        if needle.len() > self.len() {
            return None;
        }
        (0..=self.len() - needle.len()).find(|&i| {
            self.is_char_boundary(i)
                && self.is_char_boundary(i + needle.len())
                && eq(&self[i..i + needle.len()], needle)
        })
    }
}

/// Match an origin against a CORS allowed origin pattern.
///
/// AWS permits one `*` anywhere in `AllowedOrigin`, and its own documentation
/// uses `https://*.example.com` — a pattern that a leading-`*`-only matcher
/// silently fails, leaving the bucket CORS-dead. Origins compare
/// case-sensitively, unlike header names.
pub(crate) fn origin_matches(origin: &str, pattern: &str) -> bool {
    wildcard_matches(origin, pattern, true)
}

/// Find the matching CORS rule for a given origin and HTTP method.
///
/// Methods compare exactly: `put_bucket_cors` accepts only the canonical
/// uppercase verbs, so a stored rule cannot hold anything else, and
/// [`parse_cors_config`] trims the pretty-printed whitespace that would
/// otherwise make an otherwise-valid value miss.
///
/// `requested_headers` are the `Access-Control-Request-Headers` of a preflight,
/// each of which must be covered by the rule's `AllowedHeader`. An actual
/// request passes an empty slice: only a preflight declares headers.
pub(crate) fn find_cors_rule<'a>(
    rules: &'a [CorsRule],
    origin: &str,
    method: &str,
    requested_headers: &[String],
) -> Option<&'a CorsRule> {
    // An absent or unusable `Origin` / `Access-Control-Request-Method` arrives
    // as `""`, which is neither an origin nor a method any rule can allow — and
    // `""` would otherwise satisfy a bare `*`. Guarded here as well as at parse
    // time so the deny does not depend on the stored config having no empty
    // entry.
    if origin.is_empty() || method.is_empty() {
        return None;
    }
    // A rule matches the request as a whole — origin, method, and every
    // requested header — and the first such rule wins, as on S3.
    //
    // Selecting on origin+method alone and then testing headers against that
    // one rule looks more conservative but is simply wrong: given a broad rule
    // followed by a narrower one that explicitly allows `content-type`, it
    // denies the request the bucket owner wrote that second rule to permit.
    // Refusing what the config plainly allows is not a safer failure, just a
    // broken one.
    rules.iter().find(|rule| {
        let origin_ok = rule
            .allowed_origins
            .iter()
            .any(|o| origin_matches(origin, o));
        let method_ok = rule.allowed_methods.iter().any(|am| am == method);
        let headers_ok = requested_headers
            .iter()
            .all(|h| rule.allowed_headers.iter().any(|ah| header_matches(h, ah)));
        origin_ok && method_ok && headers_ok
    })
}

/// Check if an object is locked (retention or legal hold) and should block mutation.
/// Returns an error string if locked, None if allowed.
pub(crate) fn check_object_lock_for_overwrite(
    obj: &S3Object,
    req: &AwsRequest,
) -> Option<&'static str> {
    // Legal hold blocks overwrite
    if obj.lock_legal_hold.as_deref() == Some("ON") {
        return Some("AccessDenied");
    }
    // Retention check
    if let (Some(mode), Some(until)) = (&obj.lock_mode, &obj.lock_retain_until) {
        if *until > Utc::now() {
            if mode == "COMPLIANCE" {
                return Some("AccessDenied");
            }
            if mode == "GOVERNANCE" && !governance_bypass_requested(req) {
                return Some("AccessDenied");
            }
        }
    }
    None
}

/// Whether the request carries `x-amz-bypass-governance-retention: true`,
/// which lets a caller override an unexpired GOVERNANCE retention.
pub(crate) fn governance_bypass_requested(req: &AwsRequest) -> bool {
    req.headers
        .get("x-amz-bypass-governance-retention")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|s| s.eq_ignore_ascii_case("true"))
}

/// Every operation [`s3_operation`] can return, which [`iam_actions`] must
/// map.
#[cfg(test)]
const IAM_MAPPED_OPERATIONS: &[&str] = &[
    "ListBuckets",
    "ListDirectoryBuckets",
    "WriteGetObjectResponse",
    "CreateMultipartUpload",
    "CompleteMultipartUpload",
    "UploadPart",
    "UploadPartCopy",
    "AbortMultipartUpload",
    "ListParts",
    "ListMultipartUploads",
    "GetObjectAnnotation",
    "ListObjectAnnotations",
    "PutObjectAnnotation",
    "DeleteObjectAnnotation",
    "GetObjectTagging",
    "PutObjectTagging",
    "DeleteObjectTagging",
    "GetObjectAcl",
    "PutObjectAcl",
    "GetObjectRetention",
    "PutObjectRetention",
    "GetObjectLegalHold",
    "PutObjectLegalHold",
    "GetObjectAttributes",
    "RestoreObject",
    "RenameObject",
    "UpdateObjectEncryption",
    "SelectObjectContent",
    "UpdateBucketMetadataAnnotationTableConfiguration",
    "GetBucketTagging",
    "PutBucketTagging",
    "DeleteBucketTagging",
    "GetBucketAcl",
    "PutBucketAcl",
    "GetBucketVersioning",
    "PutBucketVersioning",
    "GetBucketCors",
    "PutBucketCors",
    "DeleteBucketCors",
    "GetBucketPolicy",
    "PutBucketPolicy",
    "DeleteBucketPolicy",
    "GetBucketWebsite",
    "PutBucketWebsite",
    "DeleteBucketWebsite",
    "GetBucketLifecycleConfiguration",
    "PutBucketLifecycleConfiguration",
    "DeleteBucketLifecycle",
    "GetBucketEncryption",
    "PutBucketEncryption",
    "DeleteBucketEncryption",
    "GetBucketLogging",
    "PutBucketLogging",
    "GetBucketNotificationConfiguration",
    "PutBucketNotificationConfiguration",
    "GetBucketReplication",
    "PutBucketReplication",
    "DeleteBucketReplication",
    "GetBucketOwnershipControls",
    "PutBucketOwnershipControls",
    "DeleteBucketOwnershipControls",
    "GetPublicAccessBlock",
    "PutPublicAccessBlock",
    "DeletePublicAccessBlock",
    "GetBucketAccelerateConfiguration",
    "PutBucketAccelerateConfiguration",
    "GetBucketInventoryConfiguration",
    "PutBucketInventoryConfiguration",
    "DeleteBucketInventoryConfiguration",
    "GetBucketAnalyticsConfiguration",
    "ListBucketAnalyticsConfigurations",
    "PutBucketAnalyticsConfiguration",
    "DeleteBucketAnalyticsConfiguration",
    "GetBucketIntelligentTieringConfiguration",
    "ListBucketIntelligentTieringConfigurations",
    "PutBucketIntelligentTieringConfiguration",
    "DeleteBucketIntelligentTieringConfiguration",
    "GetBucketMetricsConfiguration",
    "ListBucketMetricsConfigurations",
    "PutBucketMetricsConfiguration",
    "DeleteBucketMetricsConfiguration",
    "GetBucketRequestPayment",
    "PutBucketRequestPayment",
    "GetBucketPolicyStatus",
    "GetBucketMetadataConfiguration",
    "CreateBucketMetadataConfiguration",
    "DeleteBucketMetadataConfiguration",
    "GetBucketMetadataTableConfiguration",
    "CreateBucketMetadataTableConfiguration",
    "DeleteBucketMetadataTableConfiguration",
    "UpdateBucketMetadataInventoryTableConfiguration",
    "UpdateBucketMetadataJournalTableConfiguration",
    "PutBucketAbac",
    "GetBucketAbac",
    "CreateSession",
    "GetObjectLockConfiguration",
    "PutObjectLockConfiguration",
    "GetBucketLocation",
    "DeleteObjects",
    "ListObjectVersions",
    "GetObjectTorrent",
    "GetObject",
    "PutObject",
    "CopyObject",
    "DeleteObject",
    "HeadObject",
    "ListObjectsV2",
    "ListObjects",
    "CreateBucket",
    "DeleteBucket",
    "HeadBucket",
];

#[cfg(test)]
mod tests;

#[cfg(test)]
mod partition_tests {
    use super::*;
    use parking_lot::RwLock;
    use std::collections::HashMap;

    fn request(method: Method, region: &str, path: &str) -> AwsRequest {
        AwsRequest {
            service: "s3".to_string(),
            action: String::new(),
            region: region.to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test-req".to_string(),
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: path
                .split('/')
                .filter(|s| !s.is_empty())
                .map(|a| a.to_string())
                .collect(),
            raw_path: path.to_string(),
            raw_query: String::new(),
            method,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    #[test]
    fn iam_resource_arns_follow_request_partition() {
        assert_eq!(
            s3_resource_for("cn-north-1", "GetObject", Some("b"), Some("k")),
            "arn:aws-cn:s3:::b/k"
        );
        assert_eq!(
            s3_resource_for("us-east-1", "ListBucket", Some("b"), None),
            "arn:aws:s3:::b"
        );
        assert_eq!(
            s3_resource_for("us-east-1", "ListAllMyBuckets", None, None),
            "*"
        );
    }

    /// UpdateObjectEncryption acts on an object, so it is authorized on
    /// `bucket/key`, not the bucket; GetObjectTorrent is `s3:GetObject` on
    /// the object.
    #[test]
    fn object_level_operations_are_authorized_on_the_object() {
        assert_eq!(
            s3_resource_for("us-east-1", "UpdateObjectEncryption", Some("b"), Some("k")),
            "arn:aws:s3:::b/k",
        );
        let query = HashMap::from([("torrent".to_string(), String::new())]);
        let torrent = s3_detect_action("GET", Some("b"), Some("k"), &query);
        assert_eq!(torrent, Some("GetObjectTorrent"));
        // ListParts and AbortMultipartUpload act on the upload's object;
        // ListMultipartUploads on the bucket.
        assert_eq!(
            s3_resource_for(
                "us-east-1",
                "ListMultipartUploadParts",
                Some("b"),
                Some("k")
            ),
            "arn:aws:s3:::b/k",
        );
        assert_eq!(
            s3_resource_for("us-east-1", "ListBucketMultipartUploads", Some("b"), None),
            "arn:aws:s3:::b",
        );
    }

    #[test]
    fn access_point_iam_resource_is_regional() {
        let svc = S3Service::new(
            Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            Arc::new(DeliveryBus::new()),
        );
        for (region, want) in [
            (
                "us-east-1",
                "arn:aws:s3:us-east-1:123456789012:accesspoint/ap1",
            ),
            (
                "cn-north-1",
                "arn:aws-cn:s3:cn-north-1:123456789012:accesspoint/ap1",
            ),
        ] {
            let mut r = request(Method::GET, region, "/v20180820/accesspoint/ap1");
            r.headers
                .insert("host", "123456789012.s3-control.localhost".parse().unwrap());
            let action = svc.iam_action_for(&r).unwrap();
            assert_eq!(action.action, "GetAccessPoint");
            assert_eq!(action.resource, want);
        }
    }

    #[test]
    fn control_tagging_iam_action_targets_resource_arn() {
        let svc = S3Service::new(
            Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            Arc::new(DeliveryBus::new()),
        );
        for (method, want_action, path, want_resource) in [
            (
                Method::GET,
                "ListTagsForResource",
                "/v20180820/tags/arn%3Aaws%3As3%3A%3A%3Amy-bucket",
                "arn:aws:s3:::my-bucket",
            ),
            (
                Method::POST,
                "TagResource",
                "/v20180820/tags/arn:aws:s3:us-east-1:123456789012:accesspoint/ap1",
                "arn:aws:s3:us-east-1:123456789012:accesspoint/ap1",
            ),
            (
                Method::DELETE,
                "UntagResource",
                "/v20180820/tags/arn:aws:s3:::my-bucket",
                "arn:aws:s3:::my-bucket",
            ),
        ] {
            let mut r = request(method, "us-east-1", path);
            r.headers
                .insert("host", "123456789012.s3-control.localhost".parse().unwrap());
            let action = svc.iam_action_for(&r).unwrap();
            assert_eq!(action.action, want_action);
            assert_eq!(action.resource, want_resource);
        }
    }

    #[test]
    fn iam_resource_uses_bucket_region_partition() {
        let state: SharedS3State = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        state.write().default_mut().buckets.insert(
            "cn-bucket".to_string(),
            S3Bucket::new("cn-bucket", "cn-north-1", "123456789012"),
        );
        let svc = S3Service::new(state, Arc::new(DeliveryBus::new()));
        let action = svc
            .iam_action_for(&request(Method::GET, "us-east-1", "/cn-bucket/k"))
            .unwrap();
        assert_eq!(action.resource, "arn:aws-cn:s3:::cn-bucket/k");
        let action = svc
            .iam_action_for(&request(Method::GET, "cn-north-1", "/missing/k"))
            .unwrap();
        assert_eq!(action.resource, "arn:aws-cn:s3:::missing/k");
    }

    #[tokio::test]
    async fn china_bucket_arn_round_trips() {
        let state: SharedS3State = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let svc = S3Service::new(state.clone(), Arc::new(DeliveryBus::new()));
        let mut create = request(Method::PUT, "cn-north-1", "/cn-bucket");
        create.body = Bytes::from_static(
            b"<CreateBucketConfiguration><LocationConstraint>cn-north-1</LocationConstraint></CreateBucketConfiguration>",
        );
        let created = svc.handle(create).await.unwrap();
        let arn = created.headers["x-amz-bucket-arn"]
            .to_str()
            .unwrap()
            .to_string();
        assert_eq!(arn, "arn:aws-cn:s3:::cn-bucket");

        let listed = svc
            .handle(request(Method::GET, "cn-north-1", "/"))
            .await
            .unwrap();
        let xml = std::str::from_utf8(listed.body.expect_bytes()).unwrap();
        assert!(
            xml.contains("<BucketArn>arn:aws-cn:s3:::cn-bucket</BucketArn>"),
            "{xml}"
        );

        state
            .write()
            .default_mut()
            .buckets
            .get_mut("cn-bucket")
            .unwrap()
            .tags
            .insert("env".to_string(), "prod".to_string());
        let tags = s3_resource_tags(&state, &arn).unwrap();
        assert_eq!(tags.get("env").map(String::as_str), Some("prod"));
    }

    fn control_request(method: Method, path: &str, raw_query: &str, body: &str) -> AwsRequest {
        let mut r = request(method, "us-east-1", path);
        r.headers
            .insert("host", "123456789012.s3-control.localhost".parse().unwrap());
        r.raw_query = raw_query.to_string();
        r.body = Bytes::from(body.to_string());
        r
    }

    #[test]
    fn control_tag_resource_exposes_request_tags() {
        let r = control_request(
            Method::POST,
            "/v20180820/tags/arn:aws:s3:::my-bucket",
            "",
            "<TagResourceRequest xmlns=\"http://awss3control.amazonaws.com/doc/2018-08-20/\">\
             <Tags><Tag><Key>env</Key><Value>prod</Value></Tag></Tags></TagResourceRequest>",
        );
        let tags = s3_request_tags(&r, "TagResource").unwrap();
        assert_eq!(tags.get("env").map(String::as_str), Some("prod"));
        assert_eq!(tags.len(), 1);

        // CreateBucket's own s3:TagResource authorization still reads the
        // tags from its CreateBucketConfiguration.
        let mut create = request(Method::PUT, "us-east-1", "/new-bucket");
        create.body = Bytes::from_static(
            b"<CreateBucketConfiguration><Tags><Tag><Key>team</Key>\
              <Value>core</Value></Tag></Tags></CreateBucketConfiguration>",
        );
        let tags = s3_request_tags(&create, "CreateBucket").unwrap();
        assert_eq!(tags.get("team").map(String::as_str), Some("core"));
    }

    #[test]
    fn control_untag_resource_sets_tag_keys_without_request_tags() {
        let svc = S3Service::new(
            Arc::new(RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            Arc::new(DeliveryBus::new()),
        );
        let r = control_request(
            Method::DELETE,
            "/v20180820/tags/arn:aws:s3:::my-bucket",
            "tagKeys=env&tagKeys=team",
            "",
        );
        assert_eq!(s3_request_tags(&r, "UntagResource"), None);
        let action = svc.iam_action_for(&r).unwrap();
        assert_eq!(action.action, "UntagResource");
        let keys = svc.iam_condition_keys_for(&r, &action);
        assert_eq!(
            keys.get("aws:TagKeys"),
            Some(&vec!["env".to_string(), "team".to_string()])
        );

        let ctx = fakecloud_core::auth::ConditionContext {
            request_tags: svc.request_tags_from(&r, "UntagResource"),
            service_keys: keys,
            ..Default::default()
        };
        assert_eq!(
            ctx.lookup("aws:TagKeys"),
            Some(vec!["env".to_string(), "team".to_string()])
        );
        assert_eq!(ctx.lookup("aws:RequestTag/env"), None);
    }

    #[test]
    fn access_point_arn_resolves_resource_tags() {
        let state: SharedS3State = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        {
            let mut mas = state.write();
            let st = mas.get_or_create("123456789012");
            st.buckets.insert(
                "ap-bucket".to_string(),
                S3Bucket::new("ap-bucket", "us-east-1", "123456789012"),
            );
            st.access_points.insert(
                "ap1".to_string(),
                crate::state::S3AccessPoint {
                    name: "ap1".to_string(),
                    bucket: "ap-bucket".to_string(),
                    account_id: "123456789012".to_string(),
                    network_origin: "Internet".to_string(),
                    vpc_configuration: None,
                    creation_date: Utc::now(),
                    public_access_block: None,
                    bucket_account_id: Some("123456789012".to_string()),
                    tags: [("env".to_string(), "prod".to_string())].into(),
                },
            );
        }
        let tags =
            s3_resource_tags(&state, "arn:aws:s3:us-east-1:123456789012:accesspoint/ap1").unwrap();
        assert_eq!(tags.get("env").map(String::as_str), Some("prod"));
        assert_eq!(
            s3_resource_tags(
                &state,
                "arn:aws:s3:us-east-1:123456789012:accesspoint/missing"
            ),
            None
        );
        assert_eq!(
            s3_resource_tags(&state, "arn:aws:s3:us-east-1:999999999999:accesspoint/ap1"),
            None
        );
    }
}

#[cfg(test)]
mod s3_detect_action_tests {
    //! Auth-mapping regressions for bug-hunt 2026-06-15 §5.3: ops that
    //! were unmapped (-> enforcement skipped) or mapped to the wrong
    //! action (-> the policy targeting them never applied).
    use super::s3_detect_action;
    use std::collections::HashMap;

    fn q(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn select_object_content_maps_to_get_object() {
        // POST /bucket/key?select&select-type=2 reads object data.
        let action = s3_detect_action(
            "POST",
            Some("bucket"),
            Some("key"),
            &q(&[("select", ""), ("select-type", "2")]),
        );
        assert_eq!(action, Some("SelectObjectContent"));
    }

    #[test]
    fn annotation_reads_split_on_the_annotation_name_wire_key() {
        // GET /bucket/key?annotation&annotationName=n reads one annotation;
        // without `annotationName` the same URI lists them. The member-name
        // spelling `AnnotationName` is not the wire key and must not select
        // the single-annotation read.
        let get = s3_detect_action(
            "GET",
            Some("bucket"),
            Some("key"),
            &q(&[
                ("annotation", ""),
                (super::annotations::ANNOTATION_NAME_QUERY, "n"),
            ]),
        );
        assert_eq!(get, Some("GetObjectAnnotation"));
        let list = s3_detect_action(
            "GET",
            Some("bucket"),
            Some("key"),
            &q(&[("annotation", ""), ("AnnotationName", "n")]),
        );
        assert_eq!(list, Some("ListObjectAnnotations"));
        let delete = s3_detect_action(
            "DELETE",
            Some("bucket"),
            Some("key"),
            &q(&[
                ("annotation", ""),
                (super::annotations::ANNOTATION_NAME_QUERY, "n"),
            ]),
        );
        assert_eq!(delete, Some("DeleteObjectAnnotation"));
    }

    #[test]
    fn write_get_object_response_is_detected() {
        // POST /WriteGetObjectResponse (no key) — Object Lambda data plane.
        let action = s3_detect_action("POST", Some("WriteGetObjectResponse"), None, &q(&[]));
        assert_eq!(action, Some("WriteGetObjectResponse"));
    }

    #[test]
    fn list_directory_buckets_is_distinct_from_list_buckets() {
        // GET /?x-id=ListDirectoryBuckets is its own operation (authorized as
        // s3express:ListAllMyDirectoryBuckets), not ListBuckets.
        let dir = s3_detect_action("GET", None, None, &q(&[("x-id", "ListDirectoryBuckets")]));
        assert_eq!(dir, Some("ListDirectoryBuckets"));
        // Plain GET / is still ListBuckets.
        let plain = s3_detect_action("GET", None, None, &q(&[]));
        assert_eq!(plain, Some("ListBuckets"));
    }

    #[test]
    fn rename_object_is_detected_with_key() {
        // PUT /bucket/newkey?renameObject carries a key — previously fell
        // through to PutObject so s3:RenameObject denies were ineffective.
        let action = s3_detect_action(
            "PUT",
            Some("bucket"),
            Some("newkey"),
            &q(&[("renameObject", "")]),
        );
        assert_eq!(action, Some("RenameObject"));
    }

    #[test]
    fn update_object_encryption_maps_to_its_own_action() {
        // PUT /bucket/key?encryption changes object SSE, authorized as
        // s3:UpdateObjectEncryption (the API reference's required
        // permission), not s3:PutObject; the bucket-level ?encryption PUT
        // stays PutBucketEncryption.
        let action = s3_detect_action(
            "PUT",
            Some("bucket"),
            Some("key"),
            &q(&[("encryption", "")]),
        );
        assert_eq!(action, Some("UpdateObjectEncryption"));
        let bucket_level = s3_detect_action("PUT", Some("bucket"), None, &q(&[("encryption", "")]));
        assert_eq!(bucket_level, Some("PutBucketEncryption"));
    }

    #[test]
    fn create_session_not_misdetected_as_list_objects() {
        // GET /bucket?session — previously fell through to ListObjects.
        let action = s3_detect_action("GET", Some("bucket"), None, &q(&[("session", "")]));
        assert_eq!(action, Some("CreateSession"));
    }

    #[test]
    fn get_bucket_abac_not_misdetected_as_list_objects() {
        // GET /bucket?abac — previously fell through to ListObjects.
        let action = s3_detect_action("GET", Some("bucket"), None, &q(&[("abac", "")]));
        assert_eq!(action, Some("GetBucketAbac"));
    }

    #[test]
    fn put_bucket_abac_still_detected() {
        let action = s3_detect_action("PUT", Some("bucket"), None, &q(&[("abac", "")]));
        assert_eq!(action, Some("PutBucketAbac"));
    }
}

#[cfg(test)]
mod s3_iam_service_prefix_tests {
    //! §5.3: CreateSession and WriteGetObjectResponse are authorized under
    //! sibling IAM service prefixes (s3express / s3-object-lambda), not s3.
    use super::*;
    use parking_lot::RwLock;
    use std::sync::Arc;

    fn make_service() -> S3Service {
        let state: SharedS3State = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        S3Service::new(state, Arc::new(DeliveryBus::new()))
    }

    fn req(method: Method, path: &str, query: &[(&str, &str)]) -> AwsRequest {
        let path_segments: Vec<String> = path
            .split('/')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        AwsRequest {
            service: "s3".to_string(),
            action: String::new(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "rid".to_string(),
            headers: http::HeaderMap::new(),
            query_params: query
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            body: bytes::Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments,
            raw_path: path.to_string(),
            raw_query: String::new(),
            method,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    #[test]
    fn create_session_uses_s3express_prefix() {
        let svc = make_service();
        let action = svc
            .iam_action_for(&req(Method::GET, "/mybucket", &[("session", "")]))
            .expect("must be mapped");
        assert_eq!(action.service, "s3express");
        assert_eq!(action.action, "CreateSession");
        assert_eq!(action.action_string(), "s3express:CreateSession");
    }

    #[test]
    fn write_get_object_response_uses_object_lambda_prefix() {
        let svc = make_service();
        let action = svc
            .iam_action_for(&req(Method::POST, "/WriteGetObjectResponse", &[]))
            .expect("must be mapped");
        assert_eq!(action.service, "s3-object-lambda");
        assert_eq!(action.action, "WriteGetObjectResponse");
        assert_eq!(
            action.action_string(),
            "s3-object-lambda:WriteGetObjectResponse"
        );
    }

    /// The key comes from the raw path, as the handler routes it: repeated,
    /// leading and trailing slashes are part of it.
    #[test]
    fn keys_are_derived_from_the_raw_path() {
        let svc = make_service();
        for (path, key) in [
            ("/b", None),
            ("/b/", None),
            ("/b//", Some("/")),
            ("/b/k/", Some("k/")),
            ("/b/a//c", Some("a//c")),
            ("/b/a%2Fb", Some("a/b")),
        ] {
            assert_eq!(
                request_key(&req(Method::GET, path, &[])).as_deref(),
                key,
                "{path}"
            );
        }
        // `/b//` addresses the key `/`: a GET of it is GetObject on that key,
        // not a listing of the bucket.
        let get = svc.iam_action_for(&req(Method::GET, "/b//", &[])).unwrap();
        assert_eq!(get.action_string(), "s3:GetObject");
        assert_eq!(get.resource, "arn:aws:s3:::b//");
        // `?delete` on a key is not DeleteObjects, so it is not left to the
        // handler: POST /b//?delete&uploads names two resource types and is
        // refused before authorization; ?delete alone on a key is not a POST
        // the key takes.
        let sneaky = req(Method::POST, "/b//", &[("delete", ""), ("uploads", "")]);
        assert!(!svc.iam_authorized_by_handler(&sneaky));
        let refused = svc.validate_before_authorization(&sneaky).unwrap_err();
        assert_eq!(
            refused.message(),
            "Conflicting query string parameters: delete, uploads"
        );
        assert!(svc.iam_action_for(&sneaky).is_none());
        let on_key = req(Method::POST, "/b//", &[("delete", "")]);
        assert_eq!(
            svc.validate_before_authorization(&on_key)
                .unwrap_err()
                .status(),
            StatusCode::METHOD_NOT_ALLOWED
        );
        assert!(svc.iam_authorized_by_handler(&req(Method::POST, "/b", &[("delete", "")])));
        assert!(!svc.iam_authorized_by_handler(&req(
            Method::POST,
            "/b",
            &[("delete", ""), ("metadataConfiguration", "")]
        )));
    }

    /// The full authorization set of requests whose operation and policy
    /// action names differ, with each action's resource.
    #[test]
    fn requests_are_authorized_under_policy_action_names() {
        let svc = make_service();
        let pairs = |r: &AwsRequest| -> Vec<(String, String)> {
            svc.iam_actions_for(r)
                .into_iter()
                .map(|a| (a.action_string(), a.resource))
                .collect()
        };
        let one = |action: &str, resource: &str| vec![(action.to_string(), resource.to_string())];
        assert_eq!(
            pairs(&req(Method::GET, "/", &[])),
            one("s3:ListAllMyBuckets", "*")
        );
        assert_eq!(
            pairs(&req(Method::GET, "/b", &[("list-type", "2")])),
            one("s3:ListBucket", "arn:aws:s3:::b")
        );
        assert_eq!(
            pairs(&req(Method::HEAD, "/b", &[])),
            one("s3:ListBucket", "arn:aws:s3:::b")
        );
        assert_eq!(
            pairs(&req(Method::GET, "/b", &[("versions", "")])),
            one("s3:ListBucketVersions", "arn:aws:s3:::b")
        );
        assert_eq!(
            pairs(&req(Method::GET, "/b", &[("uploads", "")])),
            one("s3:ListBucketMultipartUploads", "arn:aws:s3:::b")
        );
        assert_eq!(
            pairs(&req(Method::GET, "/b/k", &[("uploadId", "u")])),
            one("s3:ListMultipartUploadParts", "arn:aws:s3:::b/k")
        );
        assert_eq!(
            pairs(&req(Method::HEAD, "/b/dir/k", &[])),
            one("s3:GetObject", "arn:aws:s3:::b/dir/k")
        );
        assert_eq!(
            pairs(&req(Method::GET, "/b/k", &[("versionId", "v1")])),
            one("s3:GetObjectVersion", "arn:aws:s3:::b/k")
        );
        assert_eq!(
            pairs(&req(Method::DELETE, "/b", &[("lifecycle", "")])),
            one("s3:PutLifecycleConfiguration", "arn:aws:s3:::b")
        );
        assert_eq!(
            pairs(&req(Method::POST, "/b/k", &[("uploads", "")])),
            one("s3:PutObject", "arn:aws:s3:::b/k")
        );

        // CopyObject: the destination write, then the source read on the
        // source object (its version's read for a versioned source).
        let mut copy = req(Method::PUT, "/dst/new", &[]);
        copy.headers
            .insert("x-amz-copy-source", "/src/old%20key".parse().unwrap());
        copy.headers
            .insert("x-amz-tagging", "team=a".parse().unwrap());
        // Under the default COPY tagging directive the header is ignored.
        assert!(svc
            .request_tags_from(&copy, "PutObject")
            .unwrap()
            .is_empty());
        copy.headers
            .insert("x-amz-tagging-directive", "REPLACE".parse().unwrap());
        assert_eq!(
            pairs(&copy),
            vec![
                (
                    "s3:PutObject".to_string(),
                    "arn:aws:s3:::dst/new".to_string()
                ),
                (
                    "s3:GetObject".to_string(),
                    "arn:aws:s3:::src/old key".to_string()
                ),
                (
                    "s3:PutObjectTagging".to_string(),
                    "arn:aws:s3:::dst/new".to_string()
                ),
            ]
        );
        // The tagged copy's request tags reach every action it needs.
        assert_eq!(
            svc.request_tags_from(&copy, "PutObjectTagging")
                .unwrap()
                .get("team")
                .map(String::as_str),
            Some("a")
        );
        let mut part_copy = req(
            Method::PUT,
            "/dst/new",
            &[("partNumber", "1"), ("uploadId", "u")],
        );
        part_copy
            .headers
            .insert("x-amz-copy-source", "src/k?versionId=v9".parse().unwrap());
        assert_eq!(
            pairs(&part_copy),
            vec![
                (
                    "s3:PutObject".to_string(),
                    "arn:aws:s3:::dst/new".to_string()
                ),
                (
                    "s3:GetObjectVersion".to_string(),
                    "arn:aws:s3:::src/k".to_string()
                ),
            ]
        );

        // A PutObjectTagging body is not read as request tags of a PutObject.
        let mut put = req(Method::PUT, "/b/k", &[]);
        put.body = bytes::Bytes::from_static(
            b"<Tagging><TagSet><Tag><Key>x</Key><Value>y</Value></Tag></TagSet></Tagging>",
        );
        assert!(svc.request_tags_from(&put, "PutObject").unwrap().is_empty());
    }

    #[test]
    fn get_object_torrent_is_authorized_as_get_object() {
        // "Required permissions for Amazon S3 API operations": GetObjectTorrent
        // requires s3:GetObject (on the object).
        let svc = make_service();
        let action = svc
            .iam_action_for(&req(Method::GET, "/mybucket/key.txt", &[("torrent", "")]))
            .expect("must be mapped");
        assert_eq!(action.action_string(), "s3:GetObject");
        assert_eq!(action.resource, "arn:aws:s3:::mybucket/key.txt");
    }

    /// RenameObject (a directory-bucket Zonal operation) runs on a
    /// CreateSession session, so it is authorized as `s3express:CreateSession`
    /// on the bucket's `s3express` ARN, for a ReadWrite session; CreateSession
    /// itself is authorized on that ARN with the mode it asks for.
    #[test]
    fn rename_object_is_authorized_as_a_read_write_session() {
        let svc = make_service();
        let mut rename = req(
            Method::PUT,
            "/b--use1-az4--x-s3/new",
            &[("renameObject", "")],
        );
        rename.headers.insert(
            "x-amz-rename-source",
            "b--use1-az4--x-s3/old".parse().unwrap(),
        );
        let action = svc.iam_action_for(&rename).expect("must be mapped");
        assert_eq!(action.action_string(), "s3express:CreateSession");
        assert_eq!(
            action.resource,
            "arn:aws:s3express:us-east-1:123456789012:bucket/b--use1-az4--x-s3"
        );
        let keys = svc.iam_condition_keys_for(&rename, &action);
        assert_eq!(
            keys.get("s3express:SessionMode"),
            Some(&vec!["ReadWrite".to_string()])
        );

        let mut session = req(Method::GET, "/b--use1-az4--x-s3", &[("session", "")]);
        session
            .headers
            .insert("x-amz-create-session-mode", "ReadOnly".parse().unwrap());
        let action = svc.iam_action_for(&session).expect("must be mapped");
        assert_eq!(
            action.resource,
            "arn:aws:s3express:us-east-1:123456789012:bucket/b--use1-az4--x-s3"
        );
        let keys = svc.iam_condition_keys_for(&session, &action);
        assert_eq!(
            keys.get("s3express:SessionMode"),
            Some(&vec!["ReadOnly".to_string()])
        );
    }
}

#[cfg(test)]
mod extract_xml_value_tests {
    use super::{decode_xml_entities, extract_xml_value};

    #[test]
    fn returns_inner_value() {
        assert_eq!(
            extract_xml_value("<Root><Key>value</Key></Root>", "Key"),
            Some("value".to_string())
        );
    }

    // The wire form of a key `a&b` is `<Key>a&amp;b</Key>`; the parsed value
    // must decode back to `a&b` so it matches the URL-decoded key that
    // arrives via the request path (otherwise DeleteObjects/Tagging silently
    // miss the object).
    #[test]
    fn decodes_named_and_numeric_entities() {
        assert_eq!(
            extract_xml_value("<Key>a&amp;b</Key>", "Key"),
            Some("a&b".to_string())
        );
        assert_eq!(
            extract_xml_value("<Key>&lt;x&gt; &quot;y&quot; &apos;z&apos;</Key>", "Key"),
            Some("<x> \"y\" 'z'".to_string())
        );
        assert_eq!(
            extract_xml_value("<Key>a&#38;b&#x26;c</Key>", "Key"),
            Some("a&b&c".to_string())
        );
    }

    // A single left-to-right pass must resolve each entity exactly once:
    // `&amp;lt;` is the escaped literal text `&lt;`, so it decodes to `&lt;`,
    // NOT to `<` (which a chain of `str::replace` calls would wrongly produce).
    #[test]
    fn does_not_double_decode() {
        assert_eq!(decode_xml_entities("&amp;lt;"), "&lt;");
        assert_eq!(decode_xml_entities("&amp;amp;"), "&amp;");
    }

    #[test]
    fn leaves_bare_ampersand_and_unknown_entities_verbatim() {
        assert_eq!(decode_xml_entities("a & b"), "a & b");
        assert_eq!(decode_xml_entities("50% &bogus; done"), "50% &bogus; done");
        assert_eq!(decode_xml_entities("no entities here"), "no entities here");
    }

    #[test]
    fn missing_tag_is_none() {
        assert_eq!(extract_xml_value("<Root></Root>", "Key"), None);
    }

    // bug-audit 2026-05-28, 2.2: a closing tag before the opening one used to
    // slice with end < start and panic; must return None instead.
    #[test]
    fn close_before_open_does_not_panic() {
        assert_eq!(extract_xml_value("</Key>oops<Key>value", "Key"), None);
    }

    #[test]
    fn open_without_close_is_none() {
        assert_eq!(extract_xml_value("<Key>value", "Key"), None);
    }
}

#[cfg(test)]
mod complete_multipart_parse_tests {
    use super::parse_complete_multipart_xml;

    // 2026-07-07: aws-sdk-go-v2 (the terraform AWS provider) serializes the
    // part ETag with the quote char as the numeric reference `&#34;`, e.g.
    // <ETag>&#34;<hex>&#34;</ETag>. The parser must decode that to the bare hex
    // so it matches the stored part ETag; otherwise every large go-SDK
    // multipart upload fails CompleteMultipartUpload with InvalidPart.
    #[test]
    fn parses_go_sdk_numeric_quote_entity_etag() {
        let xml = "<CompleteMultipartUpload>\
            <Part><PartNumber>1</PartNumber><ETag>&#34;100bffa249c1cbde1a66242835cdab03&#34;</ETag></Part>\
            <Part><PartNumber>2</PartNumber><ETag>&#x22;5df1aff8ceab081b20d677b2d4f7eab1&#x22;</ETag></Part>\
            </CompleteMultipartUpload>";
        let parts: Vec<(u32, String)> = parse_complete_multipart_xml(xml)
            .into_iter()
            .map(|p| (p.part_number, p.etag))
            .collect();
        assert_eq!(
            parts,
            vec![
                (1, "100bffa249c1cbde1a66242835cdab03".to_string()),
                (2, "5df1aff8ceab081b20d677b2d4f7eab1".to_string()),
            ]
        );
    }

    // aws-cli / boto send the quote as a literal `"` or the named entity
    // `&quot;` — both must still parse to the same bare hex.
    #[test]
    fn parses_cli_literal_and_named_quote_etag() {
        let xml = "<CompleteMultipartUpload>\
            <Part><PartNumber>1</PartNumber><ETag>\"abc123\"</ETag></Part>\
            <Part><PartNumber>2</PartNumber><ETag>&quot;def456&quot;</ETag></Part>\
            </CompleteMultipartUpload>";
        let parts: Vec<(u32, String)> = parse_complete_multipart_xml(xml)
            .into_iter()
            .map(|p| (p.part_number, p.etag))
            .collect();
        assert_eq!(
            parts,
            vec![(1, "abc123".to_string()), (2, "def456".to_string())]
        );
    }

    #[test]
    fn parses_part_checksums() {
        use crate::checksum::ChecksumAlgorithm;
        let xml = "<CompleteMultipartUpload>\
            <Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag>\
            <ChecksumXXHASH128>340J6T+HSQCpm4d1zBW2xw==</ChecksumXXHASH128></Part>\
            <Part><ETag>\"b\"</ETag><ChecksumCRC32>DUoRhQ==</ChecksumCRC32>\
            <PartNumber>2</PartNumber></Part>\
            </CompleteMultipartUpload>";
        let parts = parse_complete_multipart_xml(xml);
        assert_eq!(
            parts[0].checksums,
            vec![(
                ChecksumAlgorithm::XxHash128,
                "340J6T+HSQCpm4d1zBW2xw==".to_string()
            )]
        );
        assert_eq!(
            parts[1].checksums,
            vec![(ChecksumAlgorithm::Crc32, "DUoRhQ==".to_string())]
        );
    }
}
