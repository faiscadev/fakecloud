//! What S3 does around every new object version, whichever path wrote it.
//!
//! PutObject, POST Object, CopyObject, CompleteMultipartUpload and the objects
//! other services deliver into a bucket (Firehose batches, CloudWatch Logs
//! exports, ELB access logs, Athena results, server access logs, inventory
//! reports, ...) are all ordinary new object versions on AWS. Each one:
//!
//! - takes the bucket's default encryption when the writer asked for none,
//! - is replicated by the bucket's replication configuration, and
//! - fires the bucket's `s3:ObjectCreated:*` event notifications.
//!
//! The pieces live here so the write paths call one implementation instead of
//! each carrying its own copy.

use std::sync::Arc;

use bytes::Bytes;
use fakecloud_core::auth::IamMode;
use fakecloud_core::delivery::{DeliveryBus, KmsHook};
use fakecloud_core::multi_account::MultiAccountState;
use fakecloud_persistence::{BodyRef, ObjectMeta, S3Store, StoreError, StoreResult};

use crate::state::{S3Object, S3State, SharedS3State};

/// The collaborators S3 needs for the side effects of a write: the delivery
/// bus event notifications go out on, the KMS hook that encrypts SSE-KMS
/// bodies (and re-encrypts replicas), and the IAM mode that governs
/// replication into another account's bucket.
///
/// The S3 service installs it on its state (see [`install`]), so writers that
/// only hold the shared state -- the producers in other services -- get the
/// same side effects as a PutObject.
pub struct S3SideEffects {
    pub(crate) delivery: Arc<DeliveryBus>,
    pub(crate) kms_hook: Option<Arc<dyn KmsHook>>,
    pub(crate) iam_mode: IamMode,
}

impl S3SideEffects {
    pub fn new(
        delivery: Arc<DeliveryBus>,
        kms_hook: Option<Arc<dyn KmsHook>>,
        iam_mode: IamMode,
    ) -> Self {
        Self {
            delivery,
            kms_hook,
            iam_mode,
        }
    }
}

/// Install `effects` on every account in `state`. Accounts created later
/// inherit it from the default account (`AccountState::inherit_from`).
pub(crate) fn install(state: &SharedS3State, effects: &Arc<S3SideEffects>) {
    let mut accounts = state.write();
    for (_, account) in accounts.iter_mut() {
        account.side_effects = Some(effects.clone());
    }
}

/// The side effects installed on `account_id`'s state, falling back to the
/// default account's (an account only appears once something is written to
/// it, and it inherits them then).
pub(crate) fn installed(
    accounts: &MultiAccountState<S3State>,
    account_id: &str,
) -> Option<Arc<S3SideEffects>> {
    accounts
        .get(account_id)
        .and_then(|s| s.side_effects.clone())
        .or_else(|| accounts.default_ref().side_effects.clone())
}

/// The server-side encryption a new object version is stored under.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ObjectEncryption {
    /// `AES256`, `aws:kms`, `aws:kms:dsse`; `None` when neither the request
    /// nor the bucket names one.
    pub algorithm: Option<String>,
    /// The SSE-KMS key, as its ARN once resolved.
    pub kms_key_id: Option<String>,
    pub bucket_key_enabled: Option<bool>,
}

impl ObjectEncryption {
    /// The `x-amz-server-side-encryption*` response headers S3 returns on
    /// every request that writes object data (PutObject, the multipart
    /// calls). Every object is encrypted at rest on S3, so one with no other
    /// encryption reports SSE-S3 (`AES256`).
    pub(crate) fn insert_response_headers(&self, headers: &mut http::HeaderMap) {
        let algorithm = self.algorithm.as_deref().unwrap_or("AES256");
        if let Ok(v) = algorithm.parse() {
            headers.insert("x-amz-server-side-encryption", v);
        }
        if let Some(Ok(v)) = self.kms_key_id.as_deref().map(str::parse) {
            headers.insert("x-amz-server-side-encryption-aws-kms-key-id", v);
        }
        if self.bucket_key_enabled == Some(true) {
            headers.insert(
                "x-amz-server-side-encryption-bucket-key-enabled",
                http::HeaderValue::from_static("true"),
            );
        }
    }
}

/// A bucket's default encryption, from its `ServerSideEncryptionConfiguration`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DefaultEncryption {
    pub algorithm: String,
    pub kms_key_id: Option<String>,
    pub bucket_key_enabled: Option<bool>,
}

/// Parse the default encryption out of a bucket's encryption configuration.
/// `None` when the configuration names no algorithm.
pub(crate) fn bucket_default_encryption(config: &str) -> Option<DefaultEncryption> {
    let algorithm = crate::service::extract_xml_value(config, "SSEAlgorithm")?;
    if algorithm.is_empty() {
        return None;
    }
    let kms_key_id = if algorithm.starts_with("aws:kms") {
        crate::service::extract_xml_value(config, "KMSMasterKeyID").filter(|k| !k.is_empty())
    } else {
        None
    };
    let bucket_key_enabled = crate::service::extract_xml_value(config, "BucketKeyEnabled")
        .map(|v| v.trim().eq_ignore_ascii_case("true"));
    Some(DefaultEncryption {
        algorithm,
        kms_key_id,
        bucket_key_enabled,
    })
}

/// Whether `algorithm` is the SSE-KMS fakecloud stores as a KMS envelope.
/// Exactly `aws:kms`, on every write, read and copy path alike:
/// `aws:kms:dsse` (dual-layer) is recorded as requested but its body is not
/// enveloped, so it reads back as stored.
pub fn is_sse_kms(algorithm: Option<&str>) -> bool {
    algorithm == Some("aws:kms")
}

/// Encrypt `plaintext` for SSE-KMS storage in `bucket` under `kms_key_id`
/// (the AWS-managed `aws/s3` key when none is named), in `account_id`'s KMS
/// unless the key is an ARN naming another account. Returns the stored
/// envelope. With no hook wired the plaintext is returned unchanged, as on
/// the PutObject path.
#[allow(clippy::too_many_arguments)]
pub(crate) fn kms_encrypt(
    hook: Option<&Arc<dyn KmsHook>>,
    account_id: &str,
    region: &str,
    bucket: &str,
    object_key: &str,
    bucket_key: bool,
    plaintext: &[u8],
    kms_key_id: Option<&str>,
    caller_arn: Option<&str>,
) -> Result<Bytes, fakecloud_core::delivery::KmsError> {
    let Some(hook) = hook else {
        return Ok(Bytes::copy_from_slice(plaintext));
    };
    let key = kms_key_id.filter(|k| !k.is_empty()).unwrap_or("aws/s3");
    let key_account = crate::service::kms_reference_account(key).unwrap_or(account_id);
    hook.encrypt(
        key_account,
        region,
        key,
        plaintext,
        fakecloud_core::delivery::KmsCaller::via_for("s3.amazonaws.com", caller_arn),
        s3_encryption_context(bucket, object_key, bucket_key),
    )
    .map(|envelope| Bytes::from(envelope.into_bytes()))
}

/// Decrypt an SSE-KMS envelope stored in `bucket` (see [`kms_encrypt`]).
/// Bytes that are not an envelope (no hook wired, or a pre-hook body) are
/// returned unchanged.
#[allow(clippy::too_many_arguments)]
pub(crate) fn kms_decrypt(
    hook: Option<&Arc<dyn KmsHook>>,
    account_id: &str,
    bucket: &str,
    object_key: &str,
    bucket_key: bool,
    stored: &[u8],
    kms_key_id: Option<&str>,
    caller_arn: Option<&str>,
) -> Result<Bytes, fakecloud_core::delivery::KmsError> {
    let Some(hook) = hook else {
        return Ok(Bytes::copy_from_slice(stored));
    };
    let Ok(envelope) = std::str::from_utf8(stored) else {
        return Ok(Bytes::copy_from_slice(stored));
    };
    let key = kms_key_id.filter(|k| !k.is_empty()).unwrap_or("aws/s3");
    let key_account = crate::service::kms_reference_account(key).unwrap_or(account_id);
    // The object records its key's ARN, resolved in the bucket's region when
    // it was written, so that ARN names the region to decrypt in; without
    // one (a legacy object, the AWS-managed default) the key the ciphertext
    // names opens it.
    hook.decrypt_in_region(
        key_account,
        fakecloud_aws::arn::region_of(key).unwrap_or_default(),
        envelope,
        fakecloud_core::delivery::KmsCaller::via_for("s3.amazonaws.com", caller_arn),
        s3_encryption_context(bucket, object_key, bucket_key),
    )
    .map(Bytes::from)
}

/// The encryption context S3 binds an SSE-KMS data key to (S3 user guide,
/// "Encryption context"): `aws:s3:arn` is the object's ARN, or the bucket's
/// ARN when the object uses an S3 Bucket Key.
pub(crate) fn s3_encryption_context(
    bucket: &str,
    object_key: &str,
    bucket_key: bool,
) -> std::collections::HashMap<String, String> {
    let arn = if bucket_key {
        fakecloud_aws::arn::Arn::s3(bucket).to_string()
    } else {
        fakecloud_aws::arn::Arn::s3(&format!("{bucket}/{object_key}")).to_string()
    };
    [("aws:s3:arn".to_string(), arn)].into()
}

/// Resolve an SSE-KMS key reference to the key's ARN through the hook (which
/// provisions the AWS-managed `aws/s3` key on first use), as S3 records it on
/// the object. `None` when it cannot be resolved; the caller keeps the
/// reference it was given.
pub(crate) fn resolve_kms_key_arn(
    hook: Option<&Arc<dyn KmsHook>>,
    account_id: &str,
    region: &str,
    key_id: &str,
) -> Option<String> {
    let key_account = crate::service::kms_reference_account(key_id).unwrap_or(account_id);
    hook?
        .resolve_key_arn(key_account, region, key_id, "s3.amazonaws.com")
        .ok()
}

/// A new object version committed to a bucket, with what the caller needs to
/// fire its event notifications once the S3 lock is released.
pub(crate) struct CommittedObject {
    /// The version id to report to the writer: `Some` only on a
    /// versioning-enabled bucket.
    pub version_id: Option<String>,
    pub notification_config: Option<String>,
    pub region: String,
}

impl CommittedObject {
    /// Fire the bucket's notifications for the new object. Call it with the
    /// S3 lock released: delivery reads bucket state, and a Lambda target is
    /// invoked asynchronously.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn notify(
        &self,
        delivery: &Arc<DeliveryBus>,
        state: &SharedS3State,
        event_name: &str,
        requester_account: &str,
        bucket: &str,
        key: &str,
        size: u64,
        etag: &str,
    ) {
        let Some(config) = self.notification_config.as_deref() else {
            return;
        };
        crate::service::notifications::deliver_notifications(
            delivery,
            config,
            &crate::service::notifications::ObjectEvent {
                event_name,
                bucket_name: bucket,
                requester_account,
                key,
                size,
                etag,
                region: &self.region,
                version_id: self.version_id.as_deref(),
            },
            Some(state),
        );
    }
}

/// Commit `obj` as the newest version of its key in `bucket` (owned by
/// `account_id`) and replicate it, the way every S3 write records a new
/// object: [`crate::service::commit_object_version_with`] decides its version
/// from the bucket's versioning state and persists the body through
/// `write_body`, then the bucket's replication configuration copies it to its
/// destinations (under the same guard, so the source's replication status and
/// the replicas land together).
///
/// The caller holds the S3 write guard (`accounts`) and has already checked
/// that the bucket exists under it; a bucket missing here is an internal
/// error. Notifications are NOT sent here: the returned [`CommittedObject`]
/// fires them once the caller drops the guard.
pub(crate) fn commit_new_object<F>(
    accounts: &mut MultiAccountState<S3State>,
    account_id: &str,
    bucket: &str,
    store: Option<&dyn S3Store>,
    effects: Option<&S3SideEffects>,
    obj: S3Object,
    write_body: F,
) -> StoreResult<CommittedObject>
where
    F: FnOnce(&dyn S3Store, &S3Object, &ObjectMeta) -> StoreResult<BodyRef>,
{
    let key = obj.key.clone();
    let state = accounts
        .get_mut(account_id)
        .ok_or_else(|| missing(bucket))?;
    let region = state.region.clone();
    let b = state
        .buckets
        .get_mut(bucket)
        .ok_or_else(|| missing(bucket))?;
    let notification_config = b.notification_config.clone();
    let version_id = crate::service::commit_object_version_with(b, bucket, store, obj, write_body)?;
    // The new version is committed: a replication that fails from here on
    // marks it FAILED instead of failing the write.
    crate::service::replication::replicate_through_store(
        accounts, account_id, store, bucket, &key, effects,
    );
    Ok(CommittedObject {
        version_id,
        notification_config,
        region,
    })
}

fn missing(bucket: &str) -> StoreError {
    StoreError::Other(format!("bucket {bucket} disappeared during the write"))
}
