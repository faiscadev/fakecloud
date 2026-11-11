use std::sync::Arc;

use bytes::Bytes;
use chrono::Utc;
use fakecloud_core::delivery::S3Delivery;
use fakecloud_persistence::S3Store;
use md5::{Digest, Md5};
use parking_lot::RwLock;

use crate::state::{memory_body, AclGrant, S3Object, SharedS3State};

/// Lets non-S3 services write objects to buckets without taking a
/// direct dep on this crate. Used by CloudWatch Logs export tasks,
/// CloudWatch Logs deliveries, Firehose S3 destinations, ELB access
/// logs, and similar producers.
pub struct S3DeliveryImpl {
    state: SharedS3State,
    /// Durable S3 backing store. S3 durability is write-through to this store
    /// (there is no `s3_state` snapshot; on boot S3 is rebuilt from the store),
    /// so an object written only into `state` is lost on restart. Behind a lock
    /// so callers built before the S3 store exists during startup can wire it up
    /// later via [`Self::set_s3_store`]. `None` => memory mode (no store).
    store: RwLock<Option<Arc<dyn S3Store>>>,
}

impl S3DeliveryImpl {
    pub fn new(state: SharedS3State) -> Self {
        Self {
            state,
            store: RwLock::new(None),
        }
    }

    /// Wire the durable S3 store after construction, so delivered objects are
    /// written through to disk and survive a restart. Several producers (the
    /// CloudWatch-Logs delivery bus, ELB access logs) build this impl before the
    /// S3 store exists during server startup, so the store is set later.
    pub fn set_s3_store(&self, store: Arc<dyn S3Store>) {
        *self.store.write() = Some(store);
    }
}

/// Write `body` as `bucket/key` in `account_id`, persisting through `store`
/// when one is configured (disk mode). Shared by [`S3DeliveryImpl`] and
/// services that write result objects (Athena query results). See
/// [`put_delivered_object`] for the write semantics.
pub fn put_object_via_store(
    state: &SharedS3State,
    store: Option<&dyn S3Store>,
    account_id: &str,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
    content_type: Option<&str>,
) -> Result<(), String> {
    put_object_via_store_with_encryption(
        state,
        store,
        account_id,
        bucket,
        key,
        body,
        content_type,
        DeliveredEncryption::AsDelivered,
    )
}

/// [`put_object_via_store`] with the encryption the producer asked for (see
/// [`DeliveredEncryption`]); `body` is the plaintext.
#[allow(clippy::too_many_arguments)]
pub fn put_object_via_store_with_encryption(
    state: &SharedS3State,
    store: Option<&dyn S3Store>,
    account_id: &str,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
    content_type: Option<&str>,
    encryption: DeliveredEncryption,
) -> Result<(), String> {
    put_object_via_store_with_options(
        state,
        store,
        DeliveryObjectRequest {
            account: account_id,
            bucket,
            key,
            body: Bytes::from(body),
            options: DeliveryObjectOptions {
                content_type: content_type.map(str::to_owned),
                content_encoding: None,
                encryption,
            },
        },
    )
}

/// Write an object another service produced (a log export, a delivery
/// stream batch, an access log, a query result, ...) into `bucket` in
/// `account_id`. `obj` carries the object's metadata (key, content type,
/// ETag, size, encryption); `body` is the payload, in plaintext unless the
/// producer encrypted it itself (`obj.sse_algorithm` set to `aws:kms`).
///
/// On AWS such an object is an ordinary new object version, so it gets
/// everything a PutObject gets:
///
/// - the bucket's versioning decides its version id and history, and on an
///   Object Lock bucket the default retention applies;
/// - the bucket's default encryption applies when the producer set none
///   (SSE-KMS encrypts the body under the configured key, or the AWS-managed
///   `aws/s3` key);
/// - the bucket's replication configuration replicates it ("If Amazon S3
///   delivers logs to a bucket that has replication enabled, it replicates
///   the log objects", S3 User Guide, Requirements and considerations for
///   replication);
/// - the bucket's event notifications fire `s3:ObjectCreated:Put`, the event
///   for the PUT the producing service makes, with the receiving account as
///   the requester.
///
/// The notifications, replication and SSE-KMS encryption use the side
/// effects the S3 service installed on `state`; without them (a state no S3
/// service was built on) only versioning, retention and SSE-S3 apply.
///
/// The body is persisted through `store` when one is configured, so the
/// object (and every version it displaces) survives a restart. A missing
/// bucket, a failed encryption or a failed store write is an error, and
/// leaves the bucket's in-memory state unchanged.
///
/// The payload is encrypted and staged next to the store with no S3 lock
/// held; the global S3 write lock is taken only for the rename-into-place,
/// the in-memory commit and the replication, and notifications go out after
/// it is released. Returns the new version id on a versioning-enabled bucket.
pub fn put_delivered_object(
    state: &SharedS3State,
    store: Option<&dyn S3Store>,
    account_id: &str,
    bucket: &str,
    obj: S3Object,
    body: Bytes,
) -> Result<Option<String>, String> {
    put_delivered_object_with_encryption(
        state,
        store,
        account_id,
        bucket,
        obj,
        body,
        DeliveredEncryption::AsDelivered,
    )
}

/// The server-side encryption a producing service asks for on an object it
/// delivers, the way its own API names it (a DynamoDB export's
/// `S3SseAlgorithm` / `S3SseKmsKeyId`, for one). An explicit choice
/// overrides the bucket's default encryption, as SSE headers on a PutObject
/// do.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum DeliveredEncryption {
    /// The object's own `sse_*` fields describe the body as given (already
    /// encrypted by the producer); with none, the bucket default applies.
    #[default]
    AsDelivered,
    /// SSE-S3 (`AES256`).
    SseS3,
    /// SSE-KMS under `key_id`, or the AWS-managed `aws/s3` key when `None`;
    /// the body is encrypted on the way in.
    SseKms { key_id: Option<String> },
}

/// Object metadata a producing service sets on a delivered object.
#[derive(Clone, Debug, Default)]
pub struct DeliveryObjectOptions {
    /// `Content-Type`; `application/octet-stream` when `None`.
    pub content_type: Option<String>,
    /// `Content-Encoding` (e.g. `gzip` for a compressed Firehose batch).
    pub content_encoding: Option<String>,
    /// Server-side encryption; [`DeliveredEncryption::AsDelivered`] applies
    /// the bucket's default encryption.
    pub encryption: DeliveredEncryption,
}

/// One object to write with [`put_object_via_store_with_options`]; `body` is
/// the plaintext.
pub struct DeliveryObjectRequest<'a> {
    pub account: &'a str,
    pub bucket: &'a str,
    pub key: &'a str,
    pub body: Bytes,
    pub options: DeliveryObjectOptions,
}

/// Metadata-aware form of [`put_object_via_store`], retaining the same
/// versioning, Object Lock, and durable-write semantics: the unguarded
/// [`put_object_via_store_guarded`], which every `put_object_via_store*`
/// variant goes through.
pub fn put_object_via_store_with_options(
    state: &SharedS3State,
    store: Option<&dyn S3Store>,
    request: DeliveryObjectRequest<'_>,
) -> Result<(), String> {
    put_object_via_store_guarded(state, store, request, || Some(())).map(|_| ())
}

/// Metadata-aware delivery that stages outside the producer/S3 guards, then
/// pins the producer's configuration through the versioned durable commit and
/// in-memory publication, with the same versioning, Object Lock, encryption,
/// and durable-write semantics as [`put_object_via_store`].
///
/// Lock order: `before_commit` runs, and its guard is taken, before the S3
/// write lock; both are released before notifications are dispatched. Callers
/// must never take the guard's lock while holding S3 state, and returning
/// `None` cancels the write with nothing published.
pub fn put_object_via_store_guarded<G>(
    state: &SharedS3State,
    store: Option<&dyn S3Store>,
    request: DeliveryObjectRequest<'_>,
    before_commit: impl FnOnce() -> Option<G>,
) -> Result<DeliveryWriteOutcome, String> {
    let DeliveryObjectRequest {
        account,
        bucket,
        key,
        body,
        options,
    } = request;
    let obj = S3Object {
        key: key.to_string(),
        content_type: options
            .content_type
            .unwrap_or_else(|| "application/octet-stream".to_string()),
        content_encoding: options.content_encoding,
        etag: format!("{:x}", Md5::digest(&body)),
        size: body.len() as u64,
        ..Default::default()
    };
    put_delivered_object_guarded(
        state,
        store,
        account,
        bucket,
        obj,
        body,
        options.encryption,
        before_commit,
    )
    .map(|(outcome, _)| outcome)
}

/// [`put_delivered_object`] with the encryption the producer asked for. See
/// [`DeliveredEncryption`].
pub fn put_delivered_object_with_encryption(
    state: &SharedS3State,
    store: Option<&dyn S3Store>,
    account_id: &str,
    bucket: &str,
    obj: S3Object,
    body: Bytes,
    encryption: DeliveredEncryption,
) -> Result<Option<String>, String> {
    put_delivered_object_guarded(
        state,
        store,
        account_id,
        bucket,
        obj,
        body,
        encryption,
        || Some(()),
    )
    .map(|(_, version)| version)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryWriteOutcome {
    Committed,
    Cancelled,
}

fn missing_bucket(account_id: &str, bucket: &str) -> String {
    format!("bucket {bucket} not found in account {account_id}")
}

/// Cheap destination existence check under a read guard. Producers can call it
/// before expensive body encoding; the write path repeats it, and the commit
/// re-resolves the bucket under the write guard.
pub fn ensure_delivery_bucket(
    state: &SharedS3State,
    account_id: &str,
    bucket: &str,
) -> Result<(), String> {
    let exists = state
        .read()
        .get(account_id)
        .is_some_and(|s| s.buckets.contains_key(bucket));
    if exists {
        Ok(())
    } else {
        Err(missing_bucket(account_id, bucket))
    }
}

#[allow(clippy::too_many_arguments)]
fn put_delivered_object_guarded<G>(
    state: &SharedS3State,
    store: Option<&dyn S3Store>,
    account_id: &str,
    bucket: &str,
    mut obj: S3Object,
    body: Bytes,
    encryption: DeliveredEncryption,
    before_commit: impl FnOnce() -> Option<G>,
) -> Result<(DeliveryWriteOutcome, Option<String>), String> {
    let key = obj.key.clone();
    let missing = || missing_bucket(account_id, bucket);

    // Cheap existence check under a read guard, so a delivery to a missing
    // bucket never touches the disk; it also reads what the write needs from
    // the bucket.
    let (default_encryption, bucket_region, effects) = {
        let accounts = state.read();
        let b = accounts
            .get(account_id)
            .and_then(|s| s.buckets.get(bucket))
            .ok_or_else(missing)?;
        (
            b.encryption_config
                .as_deref()
                .and_then(crate::side_effects::bucket_default_encryption),
            b.region.clone(),
            crate::side_effects::installed(&accounts, account_id),
        )
    };
    let kms_hook = effects.as_ref().and_then(|e| e.kms_hook.as_ref());

    // Every new object carries an additional checksum, as on S3: the one the
    // producer set (it must, for an encrypted payload, since the checksum
    // covers the plaintext), else S3's default CRC64NVME full-object checksum
    // of the body, the one PutObject stores for an upload that names none.
    if obj.checksum_value.is_none()
        && !crate::side_effects::is_sse_kms(obj.sse_algorithm.as_deref())
    {
        let algorithm = obj
            .checksum_algorithm
            .unwrap_or(crate::checksum::DEFAULT_ALGORITHM);
        obj.checksum_value = Some(crate::checksum::checksum(algorithm, &body));
        obj.checksum_algorithm = Some(algorithm);
        obj.checksum_type = Some(crate::checksum::ChecksumType::FullObject);
    }

    // The encryption the producer asked for, else (for an object it did not
    // encrypt itself) the bucket's default encryption, as PutObject applies
    // it to a request with no SSE headers.
    let mut body = body;
    let apply = match encryption {
        DeliveredEncryption::AsDelivered => {
            default_encryption.filter(|_| obj.sse_algorithm.is_none())
        }
        DeliveredEncryption::SseS3 => Some(crate::side_effects::DefaultEncryption {
            algorithm: "AES256".to_string(),
            kms_key_id: None,
            bucket_key_enabled: None,
        }),
        DeliveredEncryption::SseKms { key_id } => Some(crate::side_effects::DefaultEncryption {
            algorithm: "aws:kms".to_string(),
            kms_key_id: key_id,
            bucket_key_enabled: None,
        }),
    };
    if let Some(chosen) = apply {
        // Only SSE-KMS proper is stored as an envelope, as on the PutObject
        // path (`aws:kms:dsse` is recorded, not enveloped).
        if crate::side_effects::is_sse_kms(Some(&chosen.algorithm)) {
            // An object marked SSE-KMS must really be encrypted: without a
            // KMS to encrypt it the write fails, as S3 fails a write it
            // cannot encrypt, rather than storing plaintext labelled
            // `aws:kms` (which no reader could then decrypt).
            if kms_hook.is_none() {
                return Err(format!(
                    "cannot store {key} in bucket {bucket}: SSE-KMS encryption is required but \
                     no KMS is available"
                ));
            }
            let key_id = chosen.kms_key_id.clone();
            body = crate::side_effects::kms_encrypt(
                kms_hook,
                account_id,
                &bucket_region,
                bucket,
                &key,
                chosen.bucket_key_enabled == Some(true),
                &body,
                key_id.as_deref(),
                // The producing service writes with its own access.
                None,
            )
            .map_err(|e| format!("failed to encrypt object {key} for bucket {bucket}: {e}"))?;
            // The object records the key's ARN, as S3 does (the
            // AWS-managed `aws/s3` key when the default names none).
            let resolve = |reference: &str| {
                crate::side_effects::resolve_kms_key_arn(
                    kms_hook,
                    account_id,
                    &bucket_region,
                    reference,
                )
            };
            obj.sse_kms_key_id = match key_id {
                Some(configured) => Some(resolve(&configured).unwrap_or(configured)),
                None => resolve("aws/s3"),
            };
        }
        obj.sse_algorithm = Some(chosen.algorithm);
        obj.bucket_key_enabled = chosen.bucket_key_enabled;
    }

    // Stage the payload with no S3 lock held: the global S3 lock is shared by
    // every bucket in every account, so a large export or log delivery written
    // to disk under it would stall all of S3.
    let mut spooled =
        crate::service::run_blocking_io(|| crate::body_io::SpooledBody::spool(store, body.clone()))
            .map_err(|e| format!("failed to stage object {key} for bucket {bucket}: {e}"))?;

    let Some(commit_guard) = before_commit() else {
        return Ok((DeliveryWriteOutcome::Cancelled, None));
    };
    obj.body = memory_body(body);
    if obj.storage_class.is_empty() {
        obj.storage_class = "STANDARD".to_string();
    }
    let now = Utc::now();
    obj.last_modified = now;
    let size = obj.size;
    let etag = obj.etag.clone();

    let committed = {
        let mut accounts = state.write();
        let acct = accounts.get_mut(account_id).ok_or_else(missing)?;
        let bucket_ref = acct.buckets.get_mut(bucket).ok_or_else(missing)?;
        // The bucket owner owns a delivered object, with the default private ACL.
        if obj.acl_owner_id.is_none() {
            obj.acl_owner_id = Some(bucket_ref.acl_owner_id.clone());
            obj.acl_grants = vec![AclGrant {
                grantee_type: "CanonicalUser".to_string(),
                grantee_id: Some(bucket_ref.acl_owner_id.clone()),
                grantee_display_name: Some(bucket_ref.acl_owner_id.clone()),
                grantee_uri: None,
                permission: "FULL_CONTROL".to_string(),
            }];
        }
        // Commit under the same guard as the existence check, so the on-disk
        // body and the in-memory metadata of concurrent writers to one key
        // stay in step. The payload is already staged next to the store, so
        // the store write is a rename, not a body copy. A failed store write
        // is returned to the caller with the bucket left exactly as it was, so
        // the caller's failure handling (a failed export job, an error
        // response) sees it and memory never claims an object the disk lacks.
        let source = spooled.source();
        crate::side_effects::commit_new_object(
            &mut accounts,
            account_id,
            bucket,
            store,
            effects.as_deref(),
            obj,
            |store, obj, meta| {
                store.put_object(bucket, &obj.key, meta.version_id.as_deref(), source, meta)
            },
        )
        .map_err(|e| format!("failed to persist object {key} to bucket {bucket}: {e}"))?
    };

    // Notification fanout may re-enter the producer. Neither its configuration
    // guard nor the S3 write lock may be held across those callbacks.
    drop(commit_guard);
    // Notifications go out with the S3 lock released, as for PutObject.
    if let Some(effects) = &effects {
        committed.notify(
            &effects.delivery,
            state,
            "ObjectCreated:Put",
            account_id,
            bucket,
            &key,
            size,
            &etag,
        );
    }
    Ok((DeliveryWriteOutcome::Committed, committed.version_id))
}

impl S3Delivery for S3DeliveryImpl {
    fn put_object(
        &self,
        account_id: &str,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<(), String> {
        // Snapshot the durable store handle before taking the S3 state lock so
        // the two locks are never held nested.
        let store = self.store.read().clone();
        put_object_via_store(
            &self.state,
            store.as_deref(),
            account_id,
            bucket,
            key,
            body,
            content_type,
        )
    }

    fn get_object(&self, account_id: &str, bucket: &str, key: &str) -> Result<Vec<u8>, String> {
        // Read as GetObject returns it (an SSE-KMS object decrypts), with the
        // body read after the S3 guard is released.
        match crate::body_io::read_object(&self.state, account_id, bucket, key) {
            Some(Ok(body)) => Ok(body.to_vec()),
            Some(Err(e)) => Err(format!("failed to read body: {e}")),
            None => {
                let exists = self
                    .state
                    .read()
                    .get(account_id)
                    .is_some_and(|s| s.buckets.contains_key(bucket));
                Err(if exists {
                    format!("key {key} not found in bucket {bucket}")
                } else {
                    format!("bucket {bucket} not found in account {account_id}")
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{S3Bucket, S3State};
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;
    use std::sync::Arc;

    const ACCOUNT: &str = "123456789012";

    fn shared_state() -> SharedS3State {
        let mut mas: MultiAccountState<S3State> =
            MultiAccountState::new(ACCOUNT, "us-east-1", "http://localhost");
        let st = mas.get_or_create(ACCOUNT);
        st.buckets.insert(
            "logs-bucket".to_string(),
            S3Bucket::new("logs-bucket", "us-east-1", "owner-id"),
        );
        Arc::new(RwLock::new(mas))
    }

    fn disk_store(tmp: &tempfile::TempDir) -> fakecloud_persistence::s3::DiskS3Store {
        let disk = fakecloud_persistence::s3::DiskS3Store::new(
            tmp.path().join("s3"),
            Arc::new(fakecloud_persistence::cache::BodyCache::new(1024)),
        );
        disk.put_bucket_meta(
            "logs-bucket",
            &fakecloud_persistence::BucketMeta {
                name: "logs-bucket".into(),
                region: "us-east-1".into(),
                ..Default::default()
            },
        )
        .unwrap();
        disk
    }

    fn guarded_request() -> DeliveryObjectRequest<'static> {
        DeliveryObjectRequest {
            account: ACCOUNT,
            bucket: "logs-bucket",
            key: "events.gz",
            body: Bytes::from_static(b"encoded payload"),
            options: DeliveryObjectOptions {
                content_type: Some("application/octet-stream".into()),
                content_encoding: Some("gzip".into()),
                ..Default::default()
            },
        }
    }

    fn staged_path(disk: &dyn S3Store) -> std::path::PathBuf {
        let paths: Vec<_> = std::fs::read_dir(disk.spool_dir().unwrap())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        assert_eq!(paths.len(), 1, "callback must run after real staging");
        assert_eq!(std::fs::read(&paths[0]).unwrap(), b"encoded payload");
        paths[0].clone()
    }

    #[test]
    fn guarded_write_carries_encoding_and_durability() {
        let tmp = tempfile::TempDir::new().unwrap();
        let disk = disk_store(&tmp);
        let state = shared_state();
        let outcome = put_object_via_store_guarded(&state, Some(&disk), guarded_request(), || {
            assert!(state.try_write().is_some(), "staging holds no S3 guard");
            staged_path(&disk);
            Some(())
        })
        .unwrap();
        assert_eq!(outcome, DeliveryWriteOutcome::Committed);
        let accounts = state.read();
        let object = &accounts.get(ACCOUNT).unwrap().buckets["logs-bucket"].objects["events.gz"];
        assert!(matches!(
            object.body,
            fakecloud_persistence::BodyRef::Disk { .. }
        ));
        assert_eq!(object.content_encoding.as_deref(), Some("gzip"));
        assert_eq!(object.size, 15);
        assert_eq!(
            disk.open_object_body(&object.body).unwrap(),
            b"encoded payload"[..]
        );
        drop(accounts);
        let reloaded = disk_store(&tmp).load().unwrap();
        let object = &reloaded.buckets["logs-bucket"].objects["events.gz"];
        assert_eq!(object.meta.content_encoding.as_deref(), Some("gzip"));
        assert_eq!(object.meta.content_type, "application/octet-stream");
        assert_eq!(object.meta.storage_class, "STANDARD");
        assert_eq!(
            disk.open_object_body(&object.body).unwrap(),
            b"encoded payload"[..]
        );
    }

    #[test]
    fn cancelled_commit_publishes_nothing() {
        let tmp = tempfile::TempDir::new().unwrap();
        let disk = disk_store(&tmp);
        let state = shared_state();
        let sqs = Arc::new(SqsRecorder::default());
        let _svc = with_s3_service(&state, sqs.clone(), None);
        set_bucket(&state, |bucket| {
            bucket.versioning = Some("Enabled".to_string());
            bucket.notification_config = Some(
                "<NotificationConfiguration><QueueConfiguration>\
                 <Queue>arn:aws:sqs:us-east-1:123456789012:q</Queue>\
                 <Event>s3:ObjectCreated:*</Event></QueueConfiguration>\
                 </NotificationConfiguration>"
                    .to_string(),
            );
            bucket.replication_config = Some(
                "<ReplicationConfiguration><Rule><Priority>1</Priority><Status>Enabled</Status>\
                 <Filter><Prefix></Prefix></Filter>\
                 <Destination><Bucket>arn:aws:s3:::replica-bucket</Bucket></Destination>\
                 </Rule></ReplicationConfiguration>"
                    .to_string(),
            );
        });
        let mut replica = S3Bucket::new("replica-bucket", "us-east-1", "owner-id");
        replica.versioning = Some("Enabled".to_string());
        state
            .write()
            .get_mut(ACCOUNT)
            .unwrap()
            .buckets
            .insert("replica-bucket".to_string(), replica);
        let mut staged = None;
        let outcome = put_object_via_store_guarded(&state, Some(&disk), guarded_request(), || {
            staged = Some(staged_path(&disk));
            assert!(state.try_write().is_some());
            None::<()>
        })
        .unwrap();
        assert_eq!(outcome, DeliveryWriteOutcome::Cancelled);
        assert!(!staged.unwrap().exists());
        assert!(state.read().get(ACCOUNT).unwrap().buckets["logs-bucket"]
            .objects
            .is_empty());
        assert!(disk.load().unwrap().buckets["logs-bucket"]
            .objects
            .is_empty());
        let accounts = state.read();
        for name in ["logs-bucket", "replica-bucket"] {
            let bucket = &accounts.get(ACCOUNT).unwrap().buckets[name];
            assert!(bucket.objects.is_empty());
            assert!(bucket.object_versions.is_empty());
        }
        assert!(
            sqs.0.lock().unwrap().is_empty(),
            "cancelled writes emit no event"
        );
    }

    #[test]
    fn account_removed_after_staging_fails_without_recreating_it() {
        const OTHER: &str = "210987654321";
        let tmp = tempfile::TempDir::new().unwrap();
        let disk = disk_store(&tmp);
        let mut accounts: MultiAccountState<S3State> =
            MultiAccountState::new(ACCOUNT, "us-east-1", "http://localhost");
        accounts.get_or_create(OTHER).buckets.insert(
            "logs-bucket".to_string(),
            S3Bucket::new("logs-bucket", "us-east-1", "owner-id"),
        );
        let state: SharedS3State = Arc::new(RwLock::new(accounts));
        let mut staged = None;
        let request = DeliveryObjectRequest {
            account: OTHER,
            ..guarded_request()
        };
        let error = put_object_via_store_guarded(&state, Some(&disk), request, || {
            staged = Some(staged_path(&disk));
            // A reset between staging and commit drops every non-default account.
            state.write().reset();
            Some(())
        })
        .unwrap_err();
        assert!(error.contains(OTHER), "{error}");
        assert!(
            state.read().get(OTHER).is_none(),
            "the commit must not recreate a removed account"
        );
        assert!(!staged.unwrap().exists(), "staged body is cleaned up");
    }

    #[test]
    fn failed_store_does_not_publish_memory_success() {
        let tmp = tempfile::TempDir::new().unwrap();
        let disk = disk_store(&tmp);
        let state = shared_state();
        let mut staged = None;
        let result = put_object_via_store_guarded(&state, Some(&disk), guarded_request(), || {
            staged = Some(staged_path(&disk));
            // Staging succeeds; the actual disk commit cannot create its object directory.
            std::fs::write(
                tmp.path().join("s3/buckets/logs-bucket/objects"),
                b"blocked",
            )
            .unwrap();
            Some(())
        });
        assert!(result.unwrap_err().contains("failed to persist"));
        assert!(!staged.unwrap().exists());
        assert!(state.read().get(ACCOUNT).unwrap().buckets["logs-bucket"]
            .objects
            .is_empty());
        std::fs::remove_file(tmp.path().join("s3/buckets/logs-bucket/objects")).unwrap();
        assert!(disk.load().unwrap().buckets["logs-bucket"]
            .objects
            .is_empty());
    }

    #[test]
    fn bucket_removed_after_staging_cleans_up_without_committing() {
        let tmp = tempfile::TempDir::new().unwrap();
        let disk = disk_store(&tmp);
        let state = shared_state();
        let mut staged = None;
        let result = put_object_via_store_guarded(&state, Some(&disk), guarded_request(), || {
            staged = Some(staged_path(&disk));
            state.write().get_or_create(ACCOUNT).buckets.clear();
            Some(())
        });
        assert!(result.unwrap_err().contains("not found"));
        assert!(!staged.unwrap().exists());
        assert!(disk.load().unwrap().buckets["logs-bucket"]
            .objects
            .is_empty());
    }

    #[test]
    fn returned_guard_lives_through_memory_publication() {
        struct PublicationGuard(SharedS3State);
        impl Drop for PublicationGuard {
            fn drop(&mut self) {
                let state = self
                    .0
                    .try_read()
                    .expect("producer guard dropped before S3 publication finished");
                assert!(state.get(ACCOUNT).unwrap().buckets["logs-bucket"]
                    .objects
                    .contains_key("events.gz"));
            }
        }
        let state = shared_state();
        assert_eq!(
            put_object_via_store_guarded(&state, None, guarded_request(), || Some(
                PublicationGuard(state.clone())
            ),)
            .unwrap(),
            DeliveryWriteOutcome::Committed
        );
    }

    #[test]
    fn options_write_persists_encoding_and_canonical_body() {
        use fakecloud_persistence::{s3::DiskS3Store, BodyRef, BucketMeta};

        let tmp = tempfile::TempDir::new().unwrap();
        let disk = DiskS3Store::new(
            tmp.path().join("s3"),
            Arc::new(fakecloud_persistence::cache::BodyCache::new(1024)),
        );
        disk.put_bucket_meta(
            "logs-bucket",
            &BucketMeta {
                name: "logs-bucket".to_owned(),
                region: "us-east-1".to_owned(),
                ..Default::default()
            },
        )
        .unwrap();
        let state = shared_state();
        let body = Bytes::from_static(b"opaque encoded bytes");
        put_object_via_store_with_options(
            &state,
            Some(&disk),
            DeliveryObjectRequest {
                account: ACCOUNT,
                bucket: "logs-bucket",
                key: "events.gz",
                body: body.clone(),
                options: DeliveryObjectOptions {
                    content_type: Some("application/octet-stream".to_owned()),
                    content_encoding: Some("gzip".to_owned()),
                    ..Default::default()
                },
            },
        )
        .unwrap();
        let accounts = state.read();
        let object = &accounts.get(ACCOUNT).unwrap().buckets["logs-bucket"].objects["events.gz"];
        assert!(matches!(object.body, BodyRef::Disk { .. }));
        assert_eq!(object.size, body.len() as u64);
        assert_eq!(object.etag, format!("{:x}", Md5::digest(&body)));
        assert_eq!(object.content_encoding.as_deref(), Some("gzip"));
        assert_eq!(object.storage_class, "STANDARD");
        assert_eq!(disk.open_object_body(&object.body).unwrap(), body);
        drop(accounts);

        let loaded = disk.load().unwrap();
        let object = &loaded.buckets["logs-bucket"].objects["events.gz"];
        assert_eq!(object.meta.content_type, "application/octet-stream");
        assert_eq!(object.meta.content_encoding.as_deref(), Some("gzip"));
        assert_eq!(object.meta.storage_class, "STANDARD");
        assert_eq!(object.meta.etag, format!("{:x}", Md5::digest(&body)));
        assert_eq!(disk.open_object_body(&object.body).unwrap(), body);
    }

    #[test]
    fn options_write_preserves_versioning_and_default_retention() {
        let state = shared_state();
        set_bucket(&state, |bucket| {
            bucket.versioning = Some("Enabled".to_string());
            bucket.object_lock_config = Some(
                "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>\
                 <Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>1</Days>\
                 </DefaultRetention></Rule></ObjectLockConfiguration>"
                    .to_string(),
            );
        });
        for body in [b"first".as_slice(), b"second".as_slice()] {
            put_object_via_store_with_options(
                &state,
                None,
                DeliveryObjectRequest {
                    account: ACCOUNT,
                    bucket: "logs-bucket",
                    key: "events.gz",
                    body: Bytes::copy_from_slice(body),
                    options: DeliveryObjectOptions {
                        content_type: None,
                        content_encoding: Some("gzip".to_string()),
                        ..Default::default()
                    },
                },
            )
            .unwrap();
        }
        let bucket = bucket(&state);
        let versions = &bucket.object_versions["events.gz"];
        assert_eq!(versions.len(), 2);
        assert_ne!(versions[0].version_id, versions[1].version_id);
        for (object, body) in versions
            .iter()
            .zip([b"first".as_slice(), b"second".as_slice()])
        {
            assert_eq!(object.content_encoding.as_deref(), Some("gzip"));
            assert_eq!(object.lock_mode.as_deref(), Some("GOVERNANCE"));
            assert!(object.lock_retain_until.is_some());
            assert_eq!(read(&state, &object.body), body);
        }
        assert_eq!(
            bucket.objects["events.gz"].version_id,
            versions[1].version_id
        );
    }

    #[test]
    fn options_write_failure_does_not_publish_memory_success() {
        use fakecloud_persistence::s3::DiskS3Store;

        let tmp = tempfile::TempDir::new().unwrap();
        let blocked = tmp.path().join("not-a-directory");
        std::fs::write(&blocked, b"blocks staging").unwrap();
        let disk = DiskS3Store::new(
            blocked,
            Arc::new(fakecloud_persistence::cache::BodyCache::new(1024)),
        );
        let state = shared_state();
        let result = put_object_via_store_with_options(
            &state,
            Some(&disk),
            DeliveryObjectRequest {
                account: ACCOUNT,
                bucket: "logs-bucket",
                key: "events.gz",
                body: Bytes::from_static(b"encoded bytes"),
                options: DeliveryObjectOptions {
                    content_type: None,
                    content_encoding: Some("gzip".to_owned()),
                    ..Default::default()
                },
            },
        );
        assert!(result.is_err());
        assert!(state.read().get(ACCOUNT).unwrap().buckets["logs-bucket"]
            .objects
            .is_empty());
    }

    #[test]
    fn put_object_writes_to_bucket() {
        let state = shared_state();
        let delivery = S3DeliveryImpl::new(state.clone());
        delivery
            .put_object(
                ACCOUNT,
                "logs-bucket",
                "exports/run-1.txt",
                b"hello world".to_vec(),
                Some("text/plain"),
            )
            .expect("put should succeed");
        let accounts = state.read();
        let st = accounts.get(ACCOUNT).unwrap();
        let bucket = st.buckets.get("logs-bucket").unwrap();
        let obj = bucket.objects.get("exports/run-1.txt").unwrap();
        assert_eq!(obj.size, 11);
        assert_eq!(obj.content_type, "text/plain");
        assert_eq!(obj.content_encoding, None);
        assert_eq!(obj.storage_class, "STANDARD");
        assert!(!obj.etag.is_empty());
    }

    #[test]
    fn put_object_rejects_missing_bucket() {
        let state = shared_state();
        let delivery = S3DeliveryImpl::new(state);
        let err = delivery
            .put_object(ACCOUNT, "ghost", "k", b"x".to_vec(), None)
            .expect_err("missing bucket");
        assert!(err.contains("not found"));
    }

    #[test]
    fn ensure_delivery_bucket_matches_write_path_error() {
        let state = shared_state();
        assert_eq!(
            ensure_delivery_bucket(&state, ACCOUNT, "logs-bucket"),
            Ok(())
        );
        let expected = format!("bucket ghost not found in account {ACCOUNT}");
        assert_eq!(
            ensure_delivery_bucket(&state, ACCOUNT, "ghost"),
            Err(expected.clone())
        );
        assert_eq!(
            ensure_delivery_bucket(&state, "999999999999", "logs-bucket"),
            Err("bucket logs-bucket not found in account 999999999999".to_string())
        );
        let mut request = guarded_request();
        request.bucket = "ghost";
        assert_eq!(
            put_object_via_store_guarded(&state, None, request, || -> Option<()> {
                panic!("a missing bucket must fail before authorization")
            }),
            Err(expected)
        );
    }

    /// Regression (bug-hunt Tier 0 side-channel-persistence): a delivered object
    /// must be written through the durable S3 store, else it is lost on restart
    /// (S3 has no state snapshot; it is rebuilt from the store on boot). With a
    /// store wired, the in-memory body becomes the store's disk-backed ref and
    /// the object reappears after a store reload.
    #[test]
    fn put_object_writes_through_to_durable_store() {
        use fakecloud_persistence::s3::DiskS3Store;
        use fakecloud_persistence::{BodyRef, BucketMeta, S3Store};

        let tmp = tempfile::TempDir::new().unwrap();
        let cache = Arc::new(fakecloud_persistence::cache::BodyCache::new(1024 * 1024));
        let disk: Arc<dyn S3Store> = Arc::new(DiskS3Store::new(tmp.path().join("s3"), cache));
        disk.put_bucket_meta(
            "logs-bucket",
            &BucketMeta {
                name: "logs-bucket".to_string(),
                region: "us-east-1".to_string(),
                ..Default::default()
            },
        )
        .unwrap();

        let state = shared_state();
        let delivery = S3DeliveryImpl::new(state.clone());
        delivery.set_s3_store(disk.clone());
        delivery
            .put_object(
                ACCOUNT,
                "logs-bucket",
                "exports/run-1.txt",
                b"durable-bytes".to_vec(),
                Some("text/plain"),
            )
            .expect("put");

        // In-memory body is now the store's disk-backed ref, and the file exists.
        {
            let accounts = state.read();
            let obj = accounts
                .get(ACCOUNT)
                .unwrap()
                .buckets
                .get("logs-bucket")
                .unwrap()
                .objects
                .get("exports/run-1.txt")
                .unwrap();
            match &obj.body {
                BodyRef::Disk { path, .. } => assert!(path.exists()),
                BodyRef::Memory(_) => panic!("delivery was not written through the store"),
            }
        }

        // Reloading the store from disk (as boot does) surfaces the object.
        let reloaded = disk.load().unwrap();
        assert!(reloaded
            .buckets
            .get("logs-bucket")
            .expect("bucket survives reload")
            .objects
            .contains_key("exports/run-1.txt"));
    }

    fn deliver(state: &SharedS3State, store: Option<&dyn S3Store>, key: &str, body: &[u8]) {
        put_object_via_store(
            state,
            store,
            ACCOUNT,
            "logs-bucket",
            key,
            body.to_vec(),
            Some("text/plain"),
        )
        .expect("delivery");
    }

    fn set_bucket(state: &SharedS3State, f: impl FnOnce(&mut S3Bucket)) {
        let mut accounts = state.write();
        f(accounts
            .get_mut(ACCOUNT)
            .unwrap()
            .buckets
            .get_mut("logs-bucket")
            .unwrap());
    }

    fn bucket(state: &SharedS3State) -> S3Bucket {
        state
            .read()
            .get(ACCOUNT)
            .unwrap()
            .buckets
            .get("logs-bucket")
            .unwrap()
            .clone()
    }

    fn read(state: &SharedS3State, body: &fakecloud_persistence::BodyRef) -> Vec<u8> {
        state
            .read()
            .get(ACCOUNT)
            .unwrap()
            .read_body(body)
            .unwrap()
            .to_vec()
    }

    #[test]
    fn delivery_to_unversioned_bucket_overwrites_without_history() {
        let state = shared_state();
        deliver(&state, None, "k", b"one");
        deliver(&state, None, "k", b"two");
        let b = bucket(&state);
        let obj = &b.objects["k"];
        assert_eq!(obj.version_id, None);
        assert_eq!(obj.storage_class, "STANDARD");
        assert_eq!(obj.acl_owner_id.as_deref(), Some("owner-id"));
        assert_eq!(read(&state, &obj.body), b"two");
        assert!(b.object_versions.is_empty());
        assert_eq!(obj.lock_mode, None);
    }

    /// A delivered object carries S3's default CRC64NVME full-object
    /// checksum of its body, like a PutObject that names no algorithm.
    #[test]
    fn delivery_stores_the_default_full_object_checksum() {
        use crate::checksum::{ChecksumAlgorithm, ChecksumType};
        let state = shared_state();
        deliver(&state, None, "k", b"payload");
        let obj = &bucket(&state).objects["k"];
        assert_eq!(obj.checksum_algorithm, Some(ChecksumAlgorithm::Crc64Nvme));
        assert_eq!(obj.checksum_type, Some(ChecksumType::FullObject));
        assert_eq!(
            obj.checksum_value.as_deref(),
            Some(crate::checksum::checksum(ChecksumAlgorithm::Crc64Nvme, b"payload").as_str())
        );
        assert_eq!(obj.part_checksums, None);
    }

    /// A producer-supplied checksum (one over the plaintext of an encrypted
    /// payload) is kept, and an encrypted body is never hashed for a default.
    #[test]
    fn delivery_keeps_a_supplied_checksum_and_skips_encrypted_bodies() {
        use crate::checksum::{ChecksumAlgorithm, ChecksumType};
        let state = shared_state();
        let supplied = S3Object {
            key: "with".to_string(),
            sse_algorithm: Some("aws:kms".to_string()),
            checksum_algorithm: Some(ChecksumAlgorithm::Crc64Nvme),
            checksum_value: Some("plaintext-sum".to_string()),
            checksum_type: Some(ChecksumType::FullObject),
            ..Default::default()
        };
        put_delivered_object(
            &state,
            None,
            ACCOUNT,
            "logs-bucket",
            supplied,
            Bytes::from_static(b"cipher"),
        )
        .unwrap();
        let encrypted = S3Object {
            key: "without".to_string(),
            sse_algorithm: Some("aws:kms".to_string()),
            ..Default::default()
        };
        put_delivered_object(
            &state,
            None,
            ACCOUNT,
            "logs-bucket",
            encrypted,
            Bytes::from_static(b"cipher"),
        )
        .unwrap();
        let b = bucket(&state);
        assert_eq!(
            b.objects["with"].checksum_value.as_deref(),
            Some("plaintext-sum")
        );
        assert_eq!(b.objects["without"].checksum_value, None);
        assert_eq!(b.objects["without"].checksum_algorithm, None);
    }

    #[test]
    fn delivery_to_versioned_bucket_adds_a_version_each_write() {
        let state = shared_state();
        // Written before versioning: becomes the "null" version.
        deliver(&state, None, "k", b"pre");
        set_bucket(&state, |b| b.versioning = Some("Enabled".to_string()));
        deliver(&state, None, "k", b"one");
        deliver(&state, None, "k", b"two");

        let b = bucket(&state);
        let versions = &b.object_versions["k"];
        assert_eq!(versions.len(), 3, "{versions:?}");
        assert_eq!(versions[0].version_id.as_deref(), Some("null"));
        assert_eq!(read(&state, &versions[0].body), b"pre");
        let v1 = versions[1].version_id.clone().unwrap();
        let v2 = versions[2].version_id.clone().unwrap();
        assert_ne!(v1, "null");
        assert_ne!(v1, v2);
        assert_eq!(read(&state, &versions[1].body), b"one");
        assert_eq!(read(&state, &versions[2].body), b"two");
        let current = &b.objects["k"];
        assert_eq!(current.version_id.as_deref(), Some(v2.as_str()));
        assert_eq!(read(&state, &current.body), b"two");
    }

    #[test]
    fn delivery_reports_the_new_version_id() {
        let state = shared_state();
        set_bucket(&state, |b| b.versioning = Some("Enabled".to_string()));
        let obj = S3Object {
            key: "k".to_string(),
            ..Default::default()
        };
        let vid = put_delivered_object(&state, None, ACCOUNT, "logs-bucket", obj, Bytes::new())
            .unwrap()
            .expect("a version id on a versioned bucket");
        assert_eq!(
            bucket(&state).objects["k"].version_id.as_deref(),
            Some(vid.as_str())
        );
    }

    #[test]
    fn delivery_to_suspended_bucket_takes_over_the_null_version() {
        let state = shared_state();
        set_bucket(&state, |b| b.versioning = Some("Enabled".to_string()));
        deliver(&state, None, "k", b"versioned");
        set_bucket(&state, |b| b.versioning = Some("Suspended".to_string()));
        deliver(&state, None, "k", b"null-1");
        deliver(&state, None, "k", b"null-2");

        let b = bucket(&state);
        let current = &b.objects["k"];
        assert_eq!(current.version_id.as_deref(), Some("null"));
        assert_eq!(read(&state, &current.body), b"null-2");
        let versions = &b.object_versions["k"];
        // The enabled-era version survives; the null slot holds only the
        // latest suspended write.
        assert_eq!(versions.len(), 2, "{versions:?}");
        assert_ne!(versions[0].version_id.as_deref(), Some("null"));
        assert_eq!(read(&state, &versions[0].body), b"versioned");
        assert_eq!(versions[1].version_id.as_deref(), Some("null"));
        assert_eq!(read(&state, &versions[1].body), b"null-2");
    }

    #[test]
    fn delivery_to_lock_bucket_applies_default_retention() {
        let state = shared_state();
        set_bucket(&state, |b| {
            b.versioning = Some("Enabled".to_string());
            b.object_lock_config = Some(
                "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>\
                 <Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days> 3 </Days>\
                 </DefaultRetention></Rule></ObjectLockConfiguration>"
                    .to_string(),
            );
        });
        let before = Utc::now();
        deliver(&state, None, "k", b"locked");
        let after = Utc::now();

        let b = bucket(&state);
        let obj = &b.objects["k"];
        assert_eq!(obj.lock_mode.as_deref(), Some("GOVERNANCE"));
        let until = obj.lock_retain_until.expect("retain-until");
        assert!(until >= before + chrono::Duration::days(3));
        assert!(until <= after + chrono::Duration::days(3));
        assert_eq!(obj.lock_legal_hold, None);
        // The version entry carries the same lock.
        let version = b.object_versions["k"].last().unwrap();
        assert_eq!(version.lock_mode.as_deref(), Some("GOVERNANCE"));
        assert_eq!(version.lock_retain_until, Some(until));
    }

    #[test]
    fn delivery_default_retention_in_years_uses_calendar_years() {
        let state = shared_state();
        set_bucket(&state, |b| {
            b.versioning = Some("Enabled".to_string());
            b.object_lock_config = Some(
                "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>\
                 <Rule><DefaultRetention><Mode>COMPLIANCE</Mode><Years>1</Years>\
                 </DefaultRetention></Rule></ObjectLockConfiguration>"
                    .to_string(),
            );
        });
        let before = Utc::now();
        deliver(&state, None, "k", b"locked");
        let after = Utc::now();
        let obj = &bucket(&state).objects["k"];
        assert_eq!(obj.lock_mode.as_deref(), Some("COMPLIANCE"));
        let until = obj.lock_retain_until.unwrap();
        assert!(until >= before.checked_add_months(chrono::Months::new(12)).unwrap());
        assert!(until <= after.checked_add_months(chrono::Months::new(12)).unwrap());
    }

    #[test]
    fn delivery_keeps_an_explicit_lock_over_the_bucket_default() {
        let state = shared_state();
        set_bucket(&state, |b| {
            b.versioning = Some("Enabled".to_string());
            b.object_lock_config = Some(
                "<ObjectLockConfiguration><Rule><DefaultRetention><Mode>GOVERNANCE</Mode>\
                 <Days>1</Days></DefaultRetention></Rule></ObjectLockConfiguration>"
                    .to_string(),
            );
        });
        let until = Utc::now() + chrono::Duration::days(30);
        let obj = S3Object {
            key: "k".to_string(),
            lock_mode: Some("COMPLIANCE".to_string()),
            lock_retain_until: Some(until),
            ..Default::default()
        };
        put_delivered_object(&state, None, ACCOUNT, "logs-bucket", obj, Bytes::new()).unwrap();
        let obj = &bucket(&state).objects["k"];
        assert_eq!(obj.lock_mode.as_deref(), Some("COMPLIANCE"));
        assert_eq!(obj.lock_retain_until, Some(until));
    }

    /// Versioned deliveries persist like PutObject's: every version (and the
    /// preserved pre-versioning "null" one) survives a reload of the store,
    /// with its own body and lock.
    #[test]
    fn versioned_delivery_persists_every_version() {
        use fakecloud_persistence::s3::DiskS3Store;
        use fakecloud_persistence::BucketMeta;

        let tmp = tempfile::TempDir::new().unwrap();
        let cache = Arc::new(fakecloud_persistence::cache::BodyCache::new(1024 * 1024));
        let disk: Arc<dyn S3Store> = Arc::new(DiskS3Store::new(tmp.path().join("s3"), cache));
        disk.put_bucket_meta(
            "logs-bucket",
            &BucketMeta {
                name: "logs-bucket".to_string(),
                region: "us-east-1".to_string(),
                ..Default::default()
            },
        )
        .unwrap();

        let state = shared_state();
        deliver(&state, Some(disk.as_ref()), "k", b"pre");
        set_bucket(&state, |b| {
            b.versioning = Some("Enabled".to_string());
            b.object_lock_config = Some(
                "<ObjectLockConfiguration><Rule><DefaultRetention><Mode>GOVERNANCE</Mode>\
                 <Days>1</Days></DefaultRetention></Rule></ObjectLockConfiguration>"
                    .to_string(),
            );
        });
        deliver(&state, Some(disk.as_ref()), "k", b"one");
        deliver(&state, Some(disk.as_ref()), "k", b"two");
        let live = bucket(&state);

        let reloaded = disk.load().unwrap();
        let snap = reloaded.buckets.get("logs-bucket").unwrap();
        let mut ids: Vec<Option<String>> = snap.object_versions["k"]
            .iter()
            .map(|o| o.meta.version_id.clone())
            .collect();
        let mut live_ids: Vec<Option<String>> = live.object_versions["k"]
            .iter()
            .map(|o| o.version_id.clone())
            .collect();
        ids.sort();
        live_ids.sort();
        assert_eq!(ids, live_ids);
        let current = &snap.objects["k"];
        assert_eq!(current.meta.version_id, live.objects["k"].version_id);
        assert_eq!(current.meta.lock_mode.as_deref(), Some("GOVERNANCE"));
        match &current.body {
            fakecloud_persistence::BodyRef::Disk { path, .. } => {
                assert_eq!(std::fs::read(path).unwrap(), b"two");
            }
            fakecloud_persistence::BodyRef::Memory(_) => panic!("expected a disk body"),
        }
    }

    /// A store whose object writes fail, standing in for a disk error: every
    /// write, or (with `only_bucket`) the writes into that one bucket.
    struct FailingStore {
        inner: fakecloud_persistence::MemoryS3Store,
        fail_meta: bool,
        only_bucket: Option<&'static str>,
    }

    fn disk_full() -> fakecloud_persistence::StoreError {
        fakecloud_persistence::StoreError::Other("disk full".to_string())
    }

    impl S3Store for FailingStore {
        fn load(
            &self,
        ) -> fakecloud_persistence::StoreResult<fakecloud_persistence::S3StateSnapshot> {
            self.inner.load()
        }
        fn put_bucket_meta(
            &self,
            bucket: &str,
            meta: &fakecloud_persistence::BucketMeta,
        ) -> fakecloud_persistence::StoreResult<()> {
            self.inner.put_bucket_meta(bucket, meta)
        }
        fn put_bucket_subresource(
            &self,
            bucket: &str,
            kind: fakecloud_persistence::BucketSubresource,
            payload: &str,
        ) -> fakecloud_persistence::StoreResult<()> {
            self.inner.put_bucket_subresource(bucket, kind, payload)
        }
        fn delete_bucket_subresource(
            &self,
            bucket: &str,
            kind: fakecloud_persistence::BucketSubresource,
        ) -> fakecloud_persistence::StoreResult<()> {
            self.inner.delete_bucket_subresource(bucket, kind)
        }
        fn delete_bucket(&self, bucket: &str) -> fakecloud_persistence::StoreResult<()> {
            self.inner.delete_bucket(bucket)
        }
        fn put_object(
            &self,
            bucket: &str,
            key: &str,
            version: Option<&str>,
            body: fakecloud_persistence::BodySource,
            meta: &fakecloud_persistence::ObjectMeta,
        ) -> fakecloud_persistence::StoreResult<fakecloud_persistence::BodyRef> {
            if self.only_bucket.is_none_or(|b| b == bucket) {
                return Err(disk_full());
            }
            self.inner.put_object(bucket, key, version, body, meta)
        }
        fn put_object_meta(
            &self,
            bucket: &str,
            key: &str,
            version: Option<&str>,
            meta: &fakecloud_persistence::ObjectMeta,
        ) -> fakecloud_persistence::StoreResult<()> {
            if self.fail_meta {
                return Err(disk_full());
            }
            self.inner.put_object_meta(bucket, key, version, meta)
        }
        fn delete_object(
            &self,
            bucket: &str,
            key: &str,
            version: Option<&str>,
        ) -> fakecloud_persistence::StoreResult<()> {
            self.inner.delete_object(bucket, key, version)
        }
        fn open_object_body(
            &self,
            body: &fakecloud_persistence::BodyRef,
        ) -> fakecloud_persistence::StoreResult<Bytes> {
            self.inner.open_object_body(body)
        }
        fn mpu_create(
            &self,
            bucket: &str,
            upload_id: &str,
            init: &fakecloud_persistence::MpuInit,
        ) -> fakecloud_persistence::StoreResult<()> {
            self.inner.mpu_create(bucket, upload_id, init)
        }
        fn mpu_put_part(
            &self,
            bucket: &str,
            upload_id: &str,
            part_number: u32,
            body: fakecloud_persistence::BodySource,
            etag: &str,
            checksum: Option<fakecloud_persistence::PartChecksum<'_>>,
        ) -> fakecloud_persistence::StoreResult<fakecloud_persistence::BodyRef> {
            self.inner
                .mpu_put_part(bucket, upload_id, part_number, body, etag, checksum)
        }
        fn mpu_abort(
            &self,
            bucket: &str,
            upload_id: &str,
        ) -> fakecloud_persistence::StoreResult<()> {
            self.inner.mpu_abort(bucket, upload_id)
        }
        fn mpu_complete(
            &self,
            bucket: &str,
            upload_id: &str,
            final_key: &str,
            version: Option<&str>,
            meta: &fakecloud_persistence::ObjectMeta,
        ) -> fakecloud_persistence::StoreResult<fakecloud_persistence::BodyRef> {
            self.inner
                .mpu_complete(bucket, upload_id, final_key, version, meta)
        }
    }

    /// A failed store write is returned to the caller and records no new
    /// object or version in memory, and memory keeps agreeing with disk: when
    /// only the body write fails, the pre-versioning object's sidecar was
    /// already relabelled the "null" version, so memory records it as the null
    /// version too; when that relabel itself fails, nothing changes.
    #[test]
    fn store_failure_is_returned_and_leaves_memory_unchanged() {
        for fail_meta in [false, true] {
            let store = FailingStore {
                inner: fakecloud_persistence::MemoryS3Store::new(),
                fail_meta,
                only_bucket: None,
            };
            let state = shared_state();
            // An unversioned write that fails stores nothing.
            let err = put_object_via_store(
                &state,
                Some(&store),
                ACCOUNT,
                "logs-bucket",
                "k",
                b"lost".to_vec(),
                None,
            )
            .expect_err("store failure must be returned");
            assert!(err.contains("disk full"), "{err}");
            assert!(bucket(&state).objects.is_empty());

            // A versioned write over a pre-versioning object leaves both the
            // current object and the (empty) history as they were.
            deliver(&state, None, "k", b"pre");
            set_bucket(&state, |b| b.versioning = Some("Enabled".to_string()));
            let before = bucket(&state);
            put_object_via_store(
                &state,
                Some(&store),
                ACCOUNT,
                "logs-bucket",
                "k",
                b"one".to_vec(),
                None,
            )
            .expect_err("store failure must be returned");
            let after = bucket(&state);
            let current = &after.objects["k"];
            assert_eq!(read(&state, &current.body), b"pre");
            assert_eq!(before.objects["k"].version_id, None);
            if fail_meta {
                // The relabel failed: disk and memory both still hold the
                // untagged pre-versioning object.
                assert!(after.object_versions.is_empty());
                assert_eq!(current.version_id, None);
            } else {
                // The relabel reached disk, so memory records the object as
                // the null version, and no new version exists.
                assert_eq!(current.version_id.as_deref(), Some("null"));
                let versions = &after.object_versions["k"];
                assert_eq!(versions.len(), 1, "{versions:?}");
                assert_eq!(versions[0].version_id.as_deref(), Some("null"));
                assert_eq!(read(&state, &versions[0].body), b"pre");
            }
        }
    }

    #[test]
    fn get_object_round_trips_put_object() {
        let state = shared_state();
        let delivery = S3DeliveryImpl::new(state);
        delivery
            .put_object(
                ACCOUNT,
                "logs-bucket",
                "dump.sql",
                b"-- pg dump --".to_vec(),
                Some("application/sql"),
            )
            .expect("put");
        let body = delivery
            .get_object(ACCOUNT, "logs-bucket", "dump.sql")
            .expect("get");
        assert_eq!(body, b"-- pg dump --".to_vec());
    }

    #[test]
    fn get_object_rejects_missing_key() {
        let state = shared_state();
        let delivery = S3DeliveryImpl::new(state);
        let err = delivery
            .get_object(ACCOUNT, "logs-bucket", "ghost")
            .expect_err("missing key");
        assert!(err.contains("not found"));
    }

    /// Records every SQS message an S3 notification sends.
    #[derive(Default)]
    struct SqsRecorder(std::sync::Mutex<Vec<(String, String)>>);

    impl fakecloud_core::delivery::SqsDelivery for SqsRecorder {
        fn deliver_to_queue(
            &self,
            _sender: fakecloud_core::delivery::DeliverySender<'_>,
            queue_arn: &str,
            message_body: &str,
            _attributes: &std::collections::HashMap<String, String>,
        ) {
            self.0
                .lock()
                .unwrap()
                .push((queue_arn.to_string(), message_body.to_string()));
        }
    }

    /// A KMS hook whose "envelope" is readable: `enc:<key>:<plaintext>`.
    struct ReadableKms;

    impl fakecloud_core::delivery::KmsHook for ReadableKms {
        fn is_ciphertext(&self, _account_id: &str, stored: &str) -> bool {
            stored.starts_with("enc:")
        }

        fn encrypt(
            &self,
            _account_id: &str,
            _region: &str,
            key_id: &str,
            plaintext: &[u8],
            _caller: fakecloud_core::delivery::KmsCaller<'_>,
            _encryption_context: std::collections::HashMap<String, String>,
        ) -> Result<String, fakecloud_core::delivery::KmsError> {
            Ok(format!(
                "enc:{key_id}:{}",
                String::from_utf8_lossy(plaintext)
            ))
        }

        fn decrypt(
            &self,
            _account_id: &str,
            ciphertext_b64: &str,
            _caller: fakecloud_core::delivery::KmsCaller<'_>,
            _encryption_context: std::collections::HashMap<String, String>,
        ) -> Result<Vec<u8>, fakecloud_core::delivery::KmsError> {
            let rest = ciphertext_b64.strip_prefix("enc:").ok_or_else(|| {
                fakecloud_core::delivery::KmsError::new(
                    fakecloud_core::delivery::KmsErrorKind::InvalidCiphertext,
                    "not an envelope",
                )
            })?;
            let (_, plaintext) = rest.split_once(':').ok_or_else(|| {
                fakecloud_core::delivery::KmsError::new(
                    fakecloud_core::delivery::KmsErrorKind::InvalidCiphertext,
                    "not an envelope",
                )
            })?;
            Ok(plaintext.as_bytes().to_vec())
        }

        fn resolve_key_arn(
            &self,
            account_id: &str,
            region: &str,
            key_id: &str,
            _service_principal: &str,
        ) -> Result<String, String> {
            Ok(format!("arn:aws:kms:{region}:{account_id}:key/{key_id}"))
        }
    }

    /// Wire an S3 service onto `state`, the way the server does, so the
    /// side effects of a write are installed.
    fn with_s3_service(
        state: &SharedS3State,
        sqs: Arc<SqsRecorder>,
        kms: Option<Arc<dyn fakecloud_core::delivery::KmsHook>>,
    ) -> crate::service::S3Service {
        let bus = Arc::new(fakecloud_core::delivery::DeliveryBus::new().with_sqs(sqs));
        let svc = crate::service::S3Service::new(state.clone(), bus);
        match kms {
            Some(hook) => svc.with_kms_hook(hook),
            None => svc,
        }
    }

    /// Notification callbacks may reconfigure the producer or write to S3.
    /// Neither guard may remain held when the callback runs.
    #[test]
    fn guarded_write_releases_producer_and_s3_locks_before_notification() {
        struct NotificationLockProbe {
            producer_configuration: Arc<RwLock<()>>,
            state: SharedS3State,
            fired: std::sync::atomic::AtomicBool,
        }

        impl fakecloud_core::delivery::SqsDelivery for NotificationLockProbe {
            fn deliver_to_queue(
                &self,
                _sender: fakecloud_core::delivery::DeliverySender<'_>,
                queue_arn: &str,
                message_body: &str,
                _attributes: &std::collections::HashMap<String, String>,
            ) {
                let producer_released = self.producer_configuration.try_write().is_some();
                let s3_released = self.state.try_write().is_some();
                assert!(s3_released, "notification holds the S3 state guard");
                assert!(
                    producer_released,
                    "notification holds the producer configuration guard"
                );
                assert_eq!(queue_arn, "arn:aws:sqs:us-east-1:123456789012:q");
                let event: serde_json::Value = serde_json::from_str(message_body).unwrap();
                assert_eq!(event["Records"][0]["eventName"], "ObjectCreated:Put");
                assert_eq!(event["Records"][0]["s3"]["object"]["key"], "events.gz");
                assert!(
                    self.state.read().get(ACCOUNT).unwrap().buckets["logs-bucket"]
                        .objects
                        .contains_key("events.gz")
                );
                self.fired.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }

        let state = shared_state();
        let producer_configuration = Arc::new(RwLock::new(()));
        let probe = Arc::new(NotificationLockProbe {
            producer_configuration: producer_configuration.clone(),
            state: state.clone(),
            fired: std::sync::atomic::AtomicBool::new(false),
        });
        let bus = Arc::new(fakecloud_core::delivery::DeliveryBus::new().with_sqs(probe.clone()));
        let _svc = crate::service::S3Service::new(state.clone(), bus);
        set_bucket(&state, |bucket| {
            bucket.notification_config = Some(
                "<NotificationConfiguration><QueueConfiguration>\
                 <Queue>arn:aws:sqs:us-east-1:123456789012:q</Queue>\
                 <Event>s3:ObjectCreated:*</Event></QueueConfiguration>\
                 </NotificationConfiguration>"
                    .to_string(),
            );
        });

        let outcome = put_object_via_store_guarded(&state, None, guarded_request(), || {
            Some(producer_configuration.read())
        })
        .unwrap();
        assert_eq!(outcome, DeliveryWriteOutcome::Committed);
        assert!(probe.fired.load(std::sync::atomic::Ordering::SeqCst));
    }

    /// A delivered object fires the bucket's `s3:ObjectCreated:Put`
    /// notification, like a PutObject.
    #[test]
    fn delivered_object_fires_object_created_put_notification() {
        let state = shared_state();
        let sqs = Arc::new(SqsRecorder::default());
        let _svc = with_s3_service(&state, sqs.clone(), None);
        set_bucket(&state, |b| {
            b.notification_config = Some(
                "<NotificationConfiguration><QueueConfiguration><Id>logs</Id>\
                 <Queue>arn:aws:sqs:us-east-1:123456789012:q</Queue>\
                 <Event>s3:ObjectCreated:*</Event></QueueConfiguration>\
                 </NotificationConfiguration>"
                    .to_string(),
            );
        });
        deliver(&state, None, "firehose/batch-1", b"records");

        let sent = sqs.0.lock().unwrap();
        assert_eq!(sent.len(), 1, "{sent:?}");
        assert_eq!(sent[0].0, "arn:aws:sqs:us-east-1:123456789012:q");
        let event: serde_json::Value = serde_json::from_str(&sent[0].1).unwrap();
        let record = &event["Records"][0];
        assert_eq!(record["eventName"], "ObjectCreated:Put");
        assert_eq!(record["s3"]["object"]["key"], "firehose/batch-1");
        assert_eq!(record["s3"]["object"]["size"], 7);
        assert_eq!(record["s3"]["configurationId"], "logs");
    }

    /// A delivered object is replicated by the bucket's replication
    /// configuration, keeping its version id; the source version reports
    /// COMPLETED and the replica REPLICA.
    #[test]
    fn delivered_object_is_replicated() {
        let state = shared_state();
        let _svc = with_s3_service(&state, Arc::new(SqsRecorder::default()), None);
        set_bucket(&state, |b| {
            b.versioning = Some("Enabled".to_string());
            b.replication_config = Some(
                "<ReplicationConfiguration><Rule><ID>all</ID><Status>Enabled</Status>\
                 <Priority>1</Priority><Filter><Prefix></Prefix></Filter>\
                 <DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication>\
                 <Destination><Bucket>arn:aws:s3:::replica-bucket</Bucket></Destination>\
                 </Rule></ReplicationConfiguration>"
                    .to_string(),
            );
        });
        {
            let mut accounts = state.write();
            let mut dest = S3Bucket::new("replica-bucket", "us-east-1", "owner-id");
            dest.versioning = Some("Enabled".to_string());
            accounts
                .get_mut(ACCOUNT)
                .unwrap()
                .buckets
                .insert("replica-bucket".to_string(), dest);
        }
        let version = put_object_via_store_versioned(&state, "logs/2026/10/17/x.gz", b"log");

        let accounts = state.read();
        let st = accounts.get(ACCOUNT).unwrap();
        let source = &st.buckets["logs-bucket"].objects["logs/2026/10/17/x.gz"];
        assert_eq!(source.replication_status.as_deref(), Some("COMPLETED"));
        let replica = &st.buckets["replica-bucket"].objects["logs/2026/10/17/x.gz"];
        assert_eq!(replica.replication_status.as_deref(), Some("REPLICA"));
        assert_eq!(replica.version_id, version);
        assert_eq!(st.read_body(&replica.body).unwrap().as_ref(), b"log");
    }

    fn put_object_via_store_versioned(
        state: &SharedS3State,
        key: &str,
        body: &[u8],
    ) -> Option<String> {
        let obj = S3Object {
            key: key.to_string(),
            content_type: "application/octet-stream".to_string(),
            etag: format!("{:x}", Md5::digest(body)),
            size: body.len() as u64,
            ..Default::default()
        };
        let version = put_delivered_object(
            state,
            None,
            ACCOUNT,
            "logs-bucket",
            obj,
            Bytes::copy_from_slice(body),
        )
        .unwrap();
        assert!(version.is_some(), "a versioned bucket reports the version");
        version
    }

    /// A delivered object with no encryption of its own takes the bucket's
    /// default SSE-S3 encryption, bucket key setting included.
    #[test]
    fn delivered_object_takes_default_sse_s3() {
        let state = shared_state();
        set_bucket(&state, |b| {
            b.encryption_config = Some(
                "<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault>\
                 <SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault>\
                 <BucketKeyEnabled>true</BucketKeyEnabled></Rule>\
                 </ServerSideEncryptionConfiguration>"
                    .to_string(),
            );
        });
        deliver(&state, None, "k", b"plain");
        let obj = &bucket(&state).objects["k"];
        assert_eq!(obj.sse_algorithm.as_deref(), Some("AES256"));
        assert_eq!(obj.sse_kms_key_id, None);
        assert_eq!(obj.bucket_key_enabled, Some(true));
        assert_eq!(read(&state, &obj.body), b"plain");
    }

    /// A bucket whose default is SSE-KMS stores a delivered object encrypted
    /// under the configured key (recorded by ARN), with the checksum and size
    /// of the plaintext; with no key configured the AWS-managed `aws/s3` key
    /// is used. A producer that encrypted the object itself is left alone.
    #[test]
    fn delivered_object_takes_default_sse_kms() {
        for (configured, expected_key) in [
            (
                Some("my-key"),
                "arn:aws:kms:us-east-1:123456789012:key/my-key",
            ),
            (None, "arn:aws:kms:us-east-1:123456789012:key/aws/s3"),
        ] {
            let state = shared_state();
            let _svc = with_s3_service(
                &state,
                Arc::new(SqsRecorder::default()),
                Some(Arc::new(ReadableKms)),
            );
            let key_xml = configured
                .map(|k| format!("<KMSMasterKeyID>{k}</KMSMasterKeyID>"))
                .unwrap_or_default();
            set_bucket(&state, |b| {
                b.encryption_config = Some(format!(
                    "<ServerSideEncryptionConfiguration><Rule>\
                     <ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm>\
                     {key_xml}</ApplyServerSideEncryptionByDefault></Rule>\
                     </ServerSideEncryptionConfiguration>"
                ));
            });
            deliver(&state, None, "k", b"secret");
            let obj = &bucket(&state).objects["k"];
            assert_eq!(obj.sse_algorithm.as_deref(), Some("aws:kms"));
            assert_eq!(obj.sse_kms_key_id.as_deref(), Some(expected_key));
            let stored = read(&state, &obj.body);
            assert_eq!(
                stored,
                format!("enc:{}:secret", configured.unwrap_or("aws/s3")).as_bytes()
            );
            assert_eq!(obj.size, 6);
            assert_eq!(
                obj.checksum_value.as_deref(),
                Some(
                    crate::checksum::checksum(crate::checksum::DEFAULT_ALGORITHM, b"secret")
                        .as_str()
                )
            );

            // Already encrypted by the producer: stored as given.
            let own = S3Object {
                key: "own".to_string(),
                sse_algorithm: Some("aws:kms".to_string()),
                sse_kms_key_id: Some("producer-key".to_string()),
                ..Default::default()
            };
            put_delivered_object(
                &state,
                None,
                ACCOUNT,
                "logs-bucket",
                own,
                Bytes::from_static(b"cipher"),
            )
            .unwrap();
            let obj = &bucket(&state).objects["own"];
            assert_eq!(obj.sse_kms_key_id.as_deref(), Some("producer-key"));
            assert_eq!(read(&state, &obj.body), b"cipher");
        }
    }

    /// An encryption the producer asks for overrides the bucket default:
    /// SSE-KMS under the named key encrypts the body, SSE-S3 leaves it as is.
    #[test]
    fn requested_encryption_overrides_the_bucket_default() {
        let state = shared_state();
        let _svc = with_s3_service(
            &state,
            Arc::new(SqsRecorder::default()),
            Some(Arc::new(ReadableKms)),
        );
        set_bucket(&state, |b| {
            b.encryption_config = Some(
                "<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault>\
                 <SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>bucket-key</KMSMasterKeyID>\
                 </ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"
                    .to_string(),
            );
        });
        for (key, encryption, sse, kms_key, stored) in [
            (
                "kms",
                DeliveredEncryption::SseKms {
                    key_id: Some("export-key".to_string()),
                },
                "aws:kms",
                Some("arn:aws:kms:us-east-1:123456789012:key/export-key"),
                &b"enc:export-key:rows"[..],
            ),
            (
                "s3",
                DeliveredEncryption::SseS3,
                "AES256",
                None,
                &b"rows"[..],
            ),
            (
                "default",
                DeliveredEncryption::AsDelivered,
                "aws:kms",
                Some("arn:aws:kms:us-east-1:123456789012:key/bucket-key"),
                &b"enc:bucket-key:rows"[..],
            ),
        ] {
            put_object_via_store_with_encryption(
                &state,
                None,
                ACCOUNT,
                "logs-bucket",
                key,
                b"rows".to_vec(),
                None,
                encryption,
            )
            .unwrap();
            let obj = &bucket(&state).objects[key];
            assert_eq!(obj.sse_algorithm.as_deref(), Some(sse), "{key}");
            assert_eq!(obj.sse_kms_key_id.as_deref(), kms_key, "{key}");
            assert_eq!(read(&state, &obj.body), stored, "{key}");
        }
    }

    /// The options form carries SSE-KMS alongside the content encoding: the
    /// body is encrypted under the requested key and the gzip metadata kept.
    #[test]
    fn options_write_applies_requested_sse_kms_with_content_encoding() {
        let state = shared_state();
        let _svc = with_s3_service(
            &state,
            Arc::new(SqsRecorder::default()),
            Some(Arc::new(ReadableKms)),
        );
        put_object_via_store_with_options(
            &state,
            None,
            DeliveryObjectRequest {
                account: ACCOUNT,
                bucket: "logs-bucket",
                key: "events.gz",
                body: Bytes::from_static(b"gz"),
                options: DeliveryObjectOptions {
                    content_type: None,
                    content_encoding: Some("gzip".to_owned()),
                    encryption: DeliveredEncryption::SseKms {
                        key_id: Some("stream-key".to_owned()),
                    },
                },
            },
        )
        .unwrap();
        let obj = &bucket(&state).objects["events.gz"];
        assert_eq!(obj.sse_algorithm.as_deref(), Some("aws:kms"));
        assert_eq!(
            obj.sse_kms_key_id.as_deref(),
            Some("arn:aws:kms:us-east-1:123456789012:key/stream-key")
        );
        assert_eq!(obj.content_encoding.as_deref(), Some("gzip"));
        assert_eq!(obj.content_type, "application/octet-stream");
        assert_eq!(obj.size, 2);
        assert_eq!(read(&state, &obj.body), b"enc:stream-key:gz");
    }

    /// The delivered object is committed before it is replicated, so a
    /// replica the destination cannot store fails that replication (the
    /// source reports FAILED) without failing the delivery.
    #[test]
    fn replica_store_failure_marks_the_source_failed_not_the_write() {
        let state = shared_state();
        let _svc = with_s3_service(&state, Arc::new(SqsRecorder::default()), None);
        set_bucket(&state, |b| {
            b.versioning = Some("Enabled".to_string());
            b.replication_config = Some(
                "<ReplicationConfiguration><Rule><Priority>1</Priority><Status>Enabled</Status>\
                 <Filter><Prefix></Prefix></Filter>\
                 <Destination><Bucket>arn:aws:s3:::replica-bucket</Bucket></Destination>\
                 </Rule></ReplicationConfiguration>"
                    .to_string(),
            );
        });
        {
            let mut accounts = state.write();
            let mut dest = S3Bucket::new("replica-bucket", "us-east-1", "owner-id");
            dest.versioning = Some("Enabled".to_string());
            accounts
                .get_mut(ACCOUNT)
                .unwrap()
                .buckets
                .insert("replica-bucket".to_string(), dest);
        }
        let store = FailingStore {
            inner: fakecloud_persistence::MemoryS3Store::new(),
            fail_meta: false,
            only_bucket: Some("replica-bucket"),
        };
        put_object_via_store(
            &state,
            Some(&store),
            ACCOUNT,
            "logs-bucket",
            "k",
            b"data".to_vec(),
            None,
        )
        .expect("the delivery itself succeeds");
        let accounts = state.read();
        let st = accounts.get(ACCOUNT).unwrap();
        assert_eq!(
            st.buckets["logs-bucket"].objects["k"]
                .replication_status
                .as_deref(),
            Some("FAILED")
        );
        assert!(st.buckets["replica-bucket"].objects.is_empty());
    }

    /// SSE-KMS (the bucket default or an explicit request) with no KMS to
    /// encrypt under fails the delivery instead of storing plaintext marked
    /// `aws:kms`. `aws:kms:dsse` is recorded and stored as delivered, as on
    /// the PutObject path, so it reads back.
    #[test]
    fn sse_kms_without_kms_fails_and_dsse_is_stored_readable() {
        let state = shared_state();
        set_bucket(&state, |b| {
            b.encryption_config = Some(
                "<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault>\
                 <SSEAlgorithm>aws:kms</SSEAlgorithm></ApplyServerSideEncryptionByDefault>\
                 </Rule></ServerSideEncryptionConfiguration>"
                    .to_string(),
            );
        });
        let err = put_object_via_store(
            &state,
            None,
            ACCOUNT,
            "logs-bucket",
            "k",
            b"x".to_vec(),
            None,
        )
        .expect_err("no KMS to encrypt with");
        assert!(err.contains("SSE-KMS"), "{err}");
        assert!(bucket(&state).objects.is_empty());

        let _svc = with_s3_service(
            &state,
            Arc::new(SqsRecorder::default()),
            Some(Arc::new(ReadableKms)),
        );
        set_bucket(&state, |b| {
            b.encryption_config = Some(
                "<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault>\
                 <SSEAlgorithm>aws:kms:dsse</SSEAlgorithm><KMSMasterKeyID>k1</KMSMasterKeyID>\
                 </ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"
                    .to_string(),
            );
        });
        deliver(&state, None, "dsse", b"dual-layer");
        let obj = &bucket(&state).objects["dsse"];
        assert_eq!(obj.sse_algorithm.as_deref(), Some("aws:kms:dsse"));
        assert_eq!(read(&state, &obj.body), b"dual-layer");
        let delivery = S3DeliveryImpl::new(state.clone());
        assert_eq!(
            delivery.get_object(ACCOUNT, "logs-bucket", "dsse").unwrap(),
            b"dual-layer".to_vec()
        );
    }

    /// A KMS whose keys encrypt but refuse to decrypt (a disabled key, a key
    /// policy that denies the reader).
    struct DenyingKms;

    impl fakecloud_core::delivery::KmsHook for DenyingKms {
        fn is_ciphertext(&self, _account_id: &str, stored: &str) -> bool {
            stored.starts_with("envelope-under-")
        }

        fn encrypt(
            &self,
            _account_id: &str,
            _region: &str,
            key_id: &str,
            _plaintext: &[u8],
            _caller: fakecloud_core::delivery::KmsCaller<'_>,
            _encryption_context: std::collections::HashMap<String, String>,
        ) -> Result<String, fakecloud_core::delivery::KmsError> {
            Ok(format!("envelope-under-{key_id}"))
        }

        fn decrypt(
            &self,
            _account_id: &str,
            _ciphertext_b64: &str,
            _caller: fakecloud_core::delivery::KmsCaller<'_>,
            _encryption_context: std::collections::HashMap<String, String>,
        ) -> Result<Vec<u8>, fakecloud_core::delivery::KmsError> {
            Err(fakecloud_core::delivery::KmsError::new(
                fakecloud_core::delivery::KmsErrorKind::Disabled,
                "kms key is disabled",
            ))
        }
    }

    /// Install an S3 service on `s3` whose KMS refuses every decrypt, and
    /// store `body` as an SSE-KMS object at `bucket/key` in `account`.
    fn put_undecryptable(
        s3: &crate::SharedS3State,
        account: &str,
        bucket: &str,
        key: &str,
        body: &[u8],
    ) -> crate::S3Service {
        let svc = crate::S3Service::new(
            s3.clone(),
            std::sync::Arc::new(fakecloud_core::delivery::DeliveryBus::new()),
        )
        .with_kms_hook(std::sync::Arc::new(DenyingKms));
        crate::delivery::put_object_via_store_with_encryption(
            s3,
            None,
            account,
            bucket,
            key,
            body.to_vec(),
            None,
            crate::delivery::DeliveredEncryption::SseKms { key_id: None },
        )
        .unwrap();
        svc
    }

    /// A read that cannot decrypt an SSE-KMS object fails with the KMS
    /// access failure, distinct from a missing object, and never returns the
    /// stored envelope.
    #[test]
    fn undecryptable_reads_fail_as_kms_access_failures() {
        let state = shared_state();
        let _svc = put_undecryptable(&state, ACCOUNT, "logs-bucket", "k", b"secret");
        let err = crate::body_io::read_object(&state, ACCOUNT, "logs-bucket", "k")
            .expect("the object exists")
            .expect_err("it cannot be decrypted");
        assert!(err.is_kms());
        assert!(err.to_string().contains("logs-bucket/k"), "{err}");
        assert!(crate::body_io::read_object(&state, ACCOUNT, "logs-bucket", "missing").is_none());
        let delivery = S3DeliveryImpl::new(state.clone());
        let err = delivery
            .get_object(ACCOUNT, "logs-bucket", "k")
            .expect_err("no plaintext");
        assert!(err.contains("kms key is disabled"), "{err}");
    }

    /// Reads through the delivery bus return what GetObject returns: an
    /// SSE-KMS object's plaintext, not its stored envelope.
    #[test]
    fn get_object_decrypts_sse_kms_objects() {
        let state = shared_state();
        let _svc = with_s3_service(
            &state,
            Arc::new(SqsRecorder::default()),
            Some(Arc::new(ReadableKms)),
        );
        put_object_via_store_with_encryption(
            &state,
            None,
            ACCOUNT,
            "logs-bucket",
            "k",
            b"secret".to_vec(),
            None,
            DeliveredEncryption::SseKms { key_id: None },
        )
        .unwrap();
        assert_eq!(
            read(&state, &bucket(&state).objects["k"].body),
            b"enc:aws/s3:secret"
        );
        let delivery = S3DeliveryImpl::new(state.clone());
        assert_eq!(
            delivery.get_object(ACCOUNT, "logs-bucket", "k").unwrap(),
            b"secret".to_vec()
        );
    }

    /// The side effects survive an account that did not exist when the S3
    /// service was built: a new account inherits them.
    #[test]
    fn side_effects_reach_accounts_created_later() {
        let state = shared_state();
        let sqs = Arc::new(SqsRecorder::default());
        let _svc = with_s3_service(&state, sqs.clone(), None);
        {
            let mut accounts = state.write();
            let mut b = S3Bucket::new("other-bucket", "us-east-1", "222222222222");
            b.notification_config = Some(
                "<NotificationConfiguration><QueueConfiguration>\
                 <Queue>arn:aws:sqs:us-east-1:222222222222:q</Queue>\
                 <Event>s3:ObjectCreated:Put</Event></QueueConfiguration>\
                 </NotificationConfiguration>"
                    .to_string(),
            );
            accounts
                .get_or_create("222222222222")
                .buckets
                .insert("other-bucket".to_string(), b);
        }
        put_object_via_store(
            &state,
            None,
            "222222222222",
            "other-bucket",
            "k",
            b"x".to_vec(),
            None,
        )
        .unwrap();
        assert_eq!(sqs.0.lock().unwrap().len(), 1);
    }
}
