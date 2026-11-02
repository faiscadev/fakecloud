use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use fakecloud_persistence::S3Store;
use fakecloud_s3::delivery::{
    ensure_delivery_bucket, put_object_via_store_guarded, DeliveryObjectOptions,
    DeliveryObjectRequest, DeliveryWriteOutcome,
};
use fakecloud_s3::SharedS3State;
use parking_lot::RwLock;

use crate::output::encode_records;
use crate::{DeliveryConfig, SharedFirehoseState, StreamKey};

/// An immutable destination/version snapshot and an already chosen, retry-stable key.
#[derive(Clone, Debug)]
pub struct DeliveryBatch {
    pub stream: StreamKey,
    pub version_id: String,
    pub destination: DeliveryConfig,
    pub records: Vec<Bytes>,
    pub oldest_arrival: DateTime<Utc>,
    pub delivery_time: DateTime<Utc>,
    pub object_key: String,
}

pub trait BatchWriter: Send + Sync {
    fn write(&self, batch: &DeliveryBatch) -> Result<DeliveryWriteOutcome, String>;
}

pub struct S3BatchWriter {
    state: SharedFirehoseState,
    s3: SharedS3State,
    store: Arc<RwLock<Option<Arc<dyn S3Store>>>>,
}

impl S3BatchWriter {
    pub fn new(
        state: SharedFirehoseState,
        s3: SharedS3State,
        store: Arc<RwLock<Option<Arc<dyn S3Store>>>>,
    ) -> Self {
        Self { state, s3, store }
    }
}

impl BatchWriter for S3BatchWriter {
    fn write(&self, batch: &DeliveryBatch) -> Result<DeliveryWriteOutcome, String> {
        // A missing bucket is the common persistent failure; detect it before
        // spending a full encode (up to ~132 MiB) on every retry.
        ensure_delivery_bucket(
            &self.s3,
            &batch.stream.account_id,
            &batch.destination.bucket,
        )?;
        let compression = batch.destination.output.compression;
        let body = encode_records(&batch.records, &batch.destination.output)
            .map_err(|e| format!("failed to encode Firehose batch {}: {e}", batch.object_key))?;
        // Snapshot the backend before either configuration or S3 state is locked.
        let store = self.store.read().clone();
        put_object_via_store_guarded(
            &self.s3,
            store.as_deref(),
            DeliveryObjectRequest {
                account: &batch.stream.account_id,
                bucket: &batch.destination.bucket,
                key: &batch.object_key,
                body,
                options: DeliveryObjectOptions {
                    content_type: Some("application/octet-stream".into()),
                    content_encoding: compression.content_encoding().map(str::to_owned),
                    encryption: batch.destination.encryption.clone(),
                },
            },
            || {
                // Configuration changes preserve the instance ID, so old batches keep
                // their captured destination. Delete/recreate and reset invalidate it.
                let config = self.state.read();
                let current = config
                    .get(&batch.stream.account_id)?
                    .streams(&batch.stream.region)?
                    .get(&batch.stream.name)?;
                if current.instance_id != batch.stream.instance_id {
                    return None;
                }
                Some(config)
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::io::Read;
    use std::path::PathBuf;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    use fakecloud_core::multi_account::MultiAccountState;
    use fakecloud_persistence::s3::{DiskS3Store, StoreResult};
    use fakecloud_persistence::{
        BodyRef, BodySource, BucketMeta, BucketSubresource, MpuInit, ObjectMeta,
    };
    use fakecloud_s3::delivery::DeliveredEncryption;
    use fakecloud_s3::{S3Bucket, S3State};
    use uuid::Uuid;

    use crate::{BufferingPolicy, Compression, DeliveryStream, FirehoseAccounts, OutputConfig};

    const ACCOUNT: &str = "123456789012";
    const REGION: &str = "us-east-1";

    // Only the staging/commit IO boundary is controlled; all disk operations are real.
    struct BoundaryStore {
        disk: DiskS3Store,
        staging: Option<Box<dyn Fn() + Send + Sync>>,
        commit: Option<Box<dyn Fn() -> StoreResult<()> + Send + Sync>>,
    }

    macro_rules! forward {
        ($($name:ident($($arg:ident: $ty:ty),*) -> $result:ty;)*) => {
            $(fn $name(&self, $($arg: $ty),*) -> $result {
                self.disk.$name($($arg),*)
            })*
        };
    }

    impl S3Store for BoundaryStore {
        forward! {
            load() -> StoreResult<fakecloud_persistence::s3::S3State>;
            put_bucket_meta(bucket: &str, meta: &BucketMeta) -> StoreResult<()>;
            put_bucket_subresource(bucket: &str, kind: BucketSubresource, payload: &str) -> StoreResult<()>;
            delete_bucket_subresource(bucket: &str, kind: BucketSubresource) -> StoreResult<()>;
            delete_bucket(bucket: &str) -> StoreResult<()>;
            put_object_meta(bucket: &str, key: &str, version: Option<&str>, meta: &ObjectMeta) -> StoreResult<()>;
            delete_object(bucket: &str, key: &str, version: Option<&str>) -> StoreResult<()>;
            open_object_body(body: &BodyRef) -> StoreResult<Bytes>;
            mpu_create(bucket: &str, upload_id: &str, init: &MpuInit) -> StoreResult<()>;
            mpu_put_part(bucket: &str, upload_id: &str, part_number: u32, body: BodySource, etag: &str, checksum: Option<fakecloud_persistence::PartChecksum<'_>>) -> StoreResult<BodyRef>;
            mpu_abort(bucket: &str, upload_id: &str) -> StoreResult<()>;
            mpu_complete(bucket: &str, upload_id: &str, final_key: &str, version: Option<&str>, meta: &ObjectMeta) -> StoreResult<BodyRef>;
        }

        fn spool_dir(&self) -> Option<PathBuf> {
            if let Some(hook) = &self.staging {
                hook();
            }
            self.disk.spool_dir()
        }

        fn put_object(
            &self,
            bucket: &str,
            key: &str,
            version: Option<&str>,
            body: BodySource,
            meta: &ObjectMeta,
        ) -> StoreResult<BodyRef> {
            if let Some(hook) = &self.commit {
                hook()?;
            }
            self.disk.put_object(bucket, key, version, body, meta)
        }
    }

    fn fixture() -> (SharedFirehoseState, SharedS3State, DeliveryBatch) {
        let now = Utc::now();
        let stream = DeliveryStream {
            instance_id: Uuid::new_v4(),
            name: "events".into(),
            arn: String::new(),
            status: "ACTIVE".into(),
            stream_type: "DirectPut".into(),
            created_at: now,
            last_update: now,
            version_id: "1".into(),
            destination: None,
            tags: BTreeMap::new(),
            encryption: None,
            extra_destinations: BTreeMap::new(),
            kinesis_source: None,
        };
        let batch = DeliveryBatch {
            stream: StreamKey {
                account_id: ACCOUNT.into(),
                region: REGION.into(),
                name: stream.name.clone(),
                instance_id: stream.instance_id,
            },
            version_id: "1".into(),
            destination: DeliveryConfig {
                bucket: "logs-bucket".into(),
                output: OutputConfig {
                    prefix: "captured/".into(),
                    file_extension: None,
                    compression: Compression::Gzip,
                    append_delimiter: false,
                },
                buffering: BufferingPolicy {
                    size_bytes: 1024,
                    interval: Duration::from_secs(60),
                },
                encryption: DeliveredEncryption::AsDelivered,
            },
            records: vec![
                Bytes::from_static(b"first\n"),
                Bytes::from_static(b"second\x00\xff"),
            ],
            oldest_arrival: now,
            delivery_time: now,
            object_key: "captured/events-1.gz".into(),
        };
        let mut accounts = FirehoseAccounts::new();
        accounts
            .get_or_create(ACCOUNT, REGION)
            .streams_mut(REGION)
            .insert(stream.name.clone(), stream);
        let mut s3 = MultiAccountState::<S3State>::new(ACCOUNT, REGION, "http://localhost");
        s3.get_or_create(ACCOUNT).buckets.insert(
            "logs-bucket".into(),
            S3Bucket::new("logs-bucket", REGION, "owner"),
        );
        (
            Arc::new(RwLock::new(accounts)),
            Arc::new(RwLock::new(s3)),
            batch,
        )
    }

    fn disk(tmp: &tempfile::TempDir) -> DiskS3Store {
        let disk = DiskS3Store::new(
            tmp.path().join("s3"),
            Arc::new(fakecloud_persistence::cache::BodyCache::new(1024)),
        );
        disk.put_bucket_meta(
            "logs-bucket",
            &BucketMeta {
                name: "logs-bucket".into(),
                region: REGION.into(),
                ..Default::default()
            },
        )
        .unwrap();
        disk
    }

    fn writer(
        state: &SharedFirehoseState,
        s3: &SharedS3State,
        store: Option<Arc<dyn S3Store>>,
    ) -> S3BatchWriter {
        S3BatchWriter::new(state.clone(), s3.clone(), Arc::new(RwLock::new(store)))
    }

    fn wait_for_staged_payload(dir: &std::path::Path) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries {
                    if let Ok(bytes) = std::fs::read(entry.unwrap().path()) {
                        let mut decoded = Vec::new();
                        if flate2::read::GzDecoder::new(bytes.as_slice())
                            .read_to_end(&mut decoded)
                            .is_ok()
                            && decoded == b"first\nsecond\x00\xff"
                        {
                            return;
                        }
                    }
                }
            }
            assert!(Instant::now() < deadline, "real staging did not complete");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn cancelled_after_staging(mutate: impl FnOnce(&mut FirehoseAccounts, &DeliveryBatch)) {
        let tmp = tempfile::TempDir::new().unwrap();
        let (state, s3, batch) = fixture();
        let disk = disk(&tmp);
        let spool = disk.spool_dir().unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let resume_rx = std::sync::Mutex::new(resume_rx);
        let fh = state.clone();
        let s3_check = s3.clone();
        let store: Arc<dyn S3Store> = Arc::new(BoundaryStore {
            disk,
            staging: Some(Box::new(move || {
                assert!(
                    fh.try_write().is_some(),
                    "hash/staging holds Firehose guard"
                );
                assert!(
                    s3_check.try_write().is_some(),
                    "hash/staging holds S3 guard"
                );
                entered_tx.send(()).unwrap();
                resume_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
            })),
            commit: Some(Box::new(|| {
                panic!("cancelled generation reached disk commit")
            })),
        });
        let writer = writer(&state, &s3, Some(store.clone()));
        std::thread::scope(|scope| {
            let writing = scope.spawn(|| writer.write(&batch));
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let mut config = state.write();
            resume_tx.send(()).unwrap();
            wait_for_staged_payload(&spool);
            // Invalidate while staged bytes exist, before the commit read guard can be acquired.
            mutate(&mut config, &batch);
            drop(config);
            assert_eq!(
                writing.join().unwrap().unwrap(),
                DeliveryWriteOutcome::Cancelled
            );
        });
        assert_eq!(std::fs::read_dir(&spool).unwrap().count(), 0);
        assert!(s3.read().get(ACCOUNT).unwrap().buckets["logs-bucket"]
            .objects
            .is_empty());
        assert!(store.load().unwrap().buckets["logs-bucket"]
            .objects
            .is_empty());
    }

    #[test]
    fn stale_generation_cancels_after_staging() {
        cancelled_after_staging(|state, batch| {
            state
                .get_or_create(ACCOUNT, REGION)
                .streams_mut(REGION)
                .get_mut(&batch.stream.name)
                .unwrap()
                .instance_id = Uuid::new_v4();
        });
    }

    #[test]
    fn delete_recreate_cancels_after_staging() {
        cancelled_after_staging(|state, batch| {
            let streams = state.get_or_create(ACCOUNT, REGION).streams_mut(REGION);
            let mut recreated = streams.remove(&batch.stream.name).unwrap();
            recreated.instance_id = Uuid::new_v4();
            streams.insert(recreated.name.clone(), recreated);
        });
    }

    #[test]
    fn stream_region_and_account_removal_cancel_after_staging() {
        cancelled_after_staging(|state, batch| {
            state
                .get_or_create(ACCOUNT, REGION)
                .streams_mut(REGION)
                .remove(&batch.stream.name);
        });
        cancelled_after_staging(|state, _| {
            state
                .get_or_create(ACCOUNT, REGION)
                .streams_by_region
                .remove(REGION);
        });
        cancelled_after_staging(|state, _| {
            state.accounts.remove(ACCOUNT);
        });
    }

    #[test]
    fn successful_encoding_and_disk_reload_use_captured_destination() {
        let tmp = tempfile::TempDir::new().unwrap();
        let (state, s3, batch) = fixture();
        let store: Arc<dyn S3Store> = Arc::new(disk(&tmp));
        s3.write().get_or_create(ACCOUNT).buckets.insert(
            "updated-bucket".into(),
            S3Bucket::new("updated-bucket", REGION, "owner"),
        );
        store
            .put_bucket_meta(
                "updated-bucket",
                &BucketMeta {
                    name: "updated-bucket".into(),
                    region: REGION.into(),
                    ..Default::default()
                },
            )
            .unwrap();
        let live_destination: crate::S3Destination = serde_json::from_value(serde_json::json!({
            "destination_id": "destinationId-000000000001",
            "role_arn": "arn:aws:iam::123456789012:role/firehose-delivery",
            "bucket_arn": "arn:aws:s3:::updated-bucket",
            "prefix": "updated/",
            "file_extension": ".updated-snappy",
            "compression_format": "Snappy",
            "buffering_size_mb": 1,
            "buffering_interval_seconds": 60
        }))
        .unwrap();
        crate::delivery_config(&live_destination).unwrap();
        // In-place updates keep the incarnation but replace every live output setting.
        {
            let mut config = state.write();
            let stream = config
                .get_or_create(ACCOUNT, REGION)
                .streams_mut(REGION)
                .get_mut("events")
                .unwrap();
            stream.version_id = "2".into();
            stream.destination = Some(live_destination);
        }
        assert_eq!(
            writer(&state, &s3, Some(store.clone()))
                .write(&batch)
                .unwrap(),
            DeliveryWriteOutcome::Committed
        );
        let accounts = s3.read();
        let buckets = &accounts.get(ACCOUNT).unwrap().buckets;
        assert_eq!(
            buckets["logs-bucket"]
                .objects
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec!["captured/events-1.gz"]
        );
        assert!(buckets["updated-bucket"].objects.is_empty());
        let object = &buckets["logs-bucket"].objects["captured/events-1.gz"];
        assert!(matches!(object.body, BodyRef::Disk { .. }));
        assert_eq!(object.content_encoding.as_deref(), Some("gzip"));
        let bytes = store.open_object_body(&object.body).unwrap();
        let mut decoded = Vec::new();
        flate2::read::GzDecoder::new(bytes.as_ref())
            .read_to_end(&mut decoded)
            .unwrap();
        assert_eq!(decoded, b"first\nsecond\x00\xff");
        assert_eq!(object.size, bytes.len() as u64);
        drop(accounts);
        let reloaded = disk(&tmp).load().unwrap();
        assert_eq!(
            reloaded.buckets["logs-bucket"]
                .objects
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec!["captured/events-1.gz"]
        );
        assert!(reloaded.buckets["updated-bucket"].objects.is_empty());
        let object = &reloaded.buckets["logs-bucket"].objects["captured/events-1.gz"];
        assert_eq!(object.meta.content_type, "application/octet-stream");
        assert_eq!(object.meta.content_encoding.as_deref(), Some("gzip"));
        let reloaded_bytes = store.open_object_body(&object.body).unwrap();
        assert_eq!(reloaded_bytes, bytes);
        let mut reloaded_decoded = Vec::new();
        flate2::read::GzDecoder::new(reloaded_bytes.as_ref())
            .read_to_end(&mut reloaded_decoded)
            .unwrap();
        assert_eq!(reloaded_decoded, b"first\nsecond\x00\xff");
    }

    #[test]
    fn commit_guard_blocks_configuration_mutation_until_memory_publication() {
        let tmp = tempfile::TempDir::new().unwrap();
        let (state, s3, batch) = fixture();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let resume_rx = std::sync::Mutex::new(resume_rx);
        let fh = state.clone();
        let s3_check = s3.clone();
        let store: Arc<dyn S3Store> = Arc::new(BoundaryStore {
            disk: disk(&tmp),
            staging: None,
            commit: Some(Box::new(move || {
                assert!(
                    fh.try_write().is_none(),
                    "durable commit must hold Firehose guard"
                );
                assert!(
                    s3_check.try_read().is_none(),
                    "durable commit must hold S3 write guard"
                );
                entered_tx.send(()).unwrap();
                resume_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
                Ok(())
            })),
        });
        let writer = writer(&state, &s3, Some(store));
        let (mutating_tx, mutating_rx) = mpsc::channel();
        std::thread::scope(|scope| {
            let writing = scope.spawn(|| writer.write(&batch));
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            assert!(state.try_write().is_none());
            let mutation = scope.spawn(|| {
                mutating_tx.send(()).unwrap();
                let mut config = state.write();
                let s3 = s3.try_read().expect(
                    "configuration mutation acquired its guard before S3 publication finished",
                );
                assert!(
                    s3.get(ACCOUNT).unwrap().buckets["logs-bucket"]
                        .objects
                        .contains_key(&batch.object_key),
                    "configuration lock released before publication"
                );
                config.accounts.clear();
            });
            mutating_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            // Resume only once the mutation is parked on the configuration lock:
            // parking_lot refuses new readers as soon as a writer is waiting, so
            // this cannot pass while the mutation thread is merely scheduled.
            let deadline = Instant::now() + Duration::from_secs(5);
            while state.try_read().is_some() {
                assert!(
                    Instant::now() < deadline,
                    "mutation never waited for the commit guard"
                );
                std::thread::yield_now();
            }
            resume_tx.send(()).unwrap();
            assert_eq!(
                writing.join().unwrap().unwrap(),
                DeliveryWriteOutcome::Committed
            );
            mutation.join().unwrap();
        });
        assert!(state.read().accounts.is_empty());
    }

    #[test]
    fn missing_bucket_fails_before_staging_or_commit() {
        let tmp = tempfile::TempDir::new().unwrap();
        let (state, s3, mut batch) = fixture();
        batch.destination.bucket = "ghost-bucket".into();
        let disk = disk(&tmp);
        let spool = disk.spool_dir().unwrap();
        let store: Arc<dyn S3Store> = Arc::new(BoundaryStore {
            disk,
            staging: Some(Box::new(|| panic!("missing bucket reached staging"))),
            commit: Some(Box::new(|| panic!("missing bucket reached commit"))),
        });
        // Staging precedes authorization and commit, so neither hook may run.
        assert_eq!(
            writer(&state, &s3, Some(store)).write(&batch),
            Err(format!(
                "bucket ghost-bucket not found in account {ACCOUNT}"
            ))
        );
        assert_eq!(std::fs::read_dir(spool).unwrap().count(), 0);
    }

    const KEY_ARN: &str = "arn:aws:kms:us-east-1:123456789012:key/stream-key";

    /// Records every SSE-KMS encryption S3 performs.
    #[derive(Default)]
    struct RecordingKms {
        encrypted: std::sync::Mutex<Vec<(String, Vec<u8>)>>,
    }

    impl fakecloud_core::delivery::KmsHook for RecordingKms {
        fn encrypt(
            &self,
            _account_id: &str,
            _region: &str,
            key_id: &str,
            plaintext: &[u8],
            _service_principal: &str,
            _encryption_context: std::collections::HashMap<String, String>,
        ) -> Result<String, String> {
            self.encrypted
                .lock()
                .unwrap()
                .push((key_id.to_owned(), plaintext.to_vec()));
            Ok(format!("envelope:{key_id}"))
        }

        fn decrypt(
            &self,
            _account_id: &str,
            _ciphertext_b64: &str,
            _service_principal: &str,
            _encryption_context: std::collections::HashMap<String, String>,
        ) -> Result<Vec<u8>, String> {
            Err("not used".into())
        }

        fn resolve_key_arn(
            &self,
            _account_id: &str,
            _region: &str,
            key_id: &str,
            _service_principal: &str,
        ) -> Result<String, String> {
            Ok(key_id.to_owned())
        }
    }

    /// Wire `kms` into S3's write side effects, as the server does.
    fn install_kms(s3: &SharedS3State, kms: Arc<RecordingKms>) {
        s3.write().get_or_create(ACCOUNT).side_effects =
            Some(Arc::new(fakecloud_s3::side_effects::S3SideEffects::new(
                Arc::new(fakecloud_core::delivery::DeliveryBus::new()),
                Some(kms),
                fakecloud_core::auth::IamMode::Off,
            )));
    }

    fn gunzip(body: &[u8]) -> Vec<u8> {
        let mut decoded = Vec::new();
        flate2::read::GzDecoder::new(body)
            .read_to_end(&mut decoded)
            .unwrap();
        decoded
    }

    #[test]
    fn captured_kms_encryption_reaches_the_s3_write() {
        let (state, s3, mut batch) = fixture();
        let kms = Arc::new(RecordingKms::default());
        install_kms(&s3, kms.clone());
        batch.destination.encryption = DeliveredEncryption::SseKms {
            key_id: Some(KEY_ARN.into()),
        };
        // The live stream has no encryption configured: only the batch's
        // captured setting may decide how its object is encrypted.
        assert_eq!(
            writer(&state, &s3, None).write(&batch).unwrap(),
            DeliveryWriteOutcome::Committed
        );
        let encrypted = kms.encrypted.lock().unwrap().clone();
        assert_eq!(encrypted.len(), 1);
        assert_eq!(encrypted[0].0, KEY_ARN);
        assert_eq!(gunzip(&encrypted[0].1), b"first\nsecond\x00\xff");
        let accounts = s3.read();
        let object =
            &accounts.get(ACCOUNT).unwrap().buckets["logs-bucket"].objects[&batch.object_key];
        assert_eq!(object.sse_algorithm.as_deref(), Some("aws:kms"));
        assert_eq!(object.sse_kms_key_id.as_deref(), Some(KEY_ARN));
        assert_eq!(object.content_encoding.as_deref(), Some("gzip"));
        assert!(
            matches!(&object.body, BodyRef::Memory(body) if body.as_ref() == format!("envelope:{KEY_ARN}").as_bytes())
        );

        // A batch captured without encryption is not encrypted.
        let (state, s3, batch) = fixture();
        let kms = Arc::new(RecordingKms::default());
        install_kms(&s3, kms.clone());
        assert_eq!(
            writer(&state, &s3, None).write(&batch).unwrap(),
            DeliveryWriteOutcome::Committed
        );
        assert!(kms.encrypted.lock().unwrap().is_empty());
        let accounts = s3.read();
        let object =
            &accounts.get(ACCOUNT).unwrap().buckets["logs-bucket"].objects[&batch.object_key];
        assert_eq!(object.sse_algorithm, None);
        assert_eq!(object.sse_kms_key_id, None);
    }

    #[test]
    fn memory_writer_uses_raw_record_concatenation() {
        let (state, s3, mut batch) = fixture();
        batch.destination.output.compression = Compression::Uncompressed;
        assert_eq!(
            writer(&state, &s3, None).write(&batch).unwrap(),
            DeliveryWriteOutcome::Committed
        );
        let accounts = s3.read();
        let object =
            &accounts.get(ACCOUNT).unwrap().buckets["logs-bucket"].objects[&batch.object_key];
        assert_eq!(object.content_encoding, None);
        assert!(
            matches!(&object.body, BodyRef::Memory(bytes) if bytes.as_ref() == b"first\nsecond\x00\xff")
        );
    }

    #[test]
    fn captured_append_delimiter_terminates_records_before_compression() {
        for compression in [
            Compression::Uncompressed,
            Compression::Gzip,
            Compression::Snappy,
        ] {
            let (state, s3, mut batch) = fixture();
            batch.destination.output.compression = compression;
            batch.destination.output.append_delimiter = true;
            batch.records.insert(1, Bytes::new());
            assert_eq!(
                writer(&state, &s3, None).write(&batch).unwrap(),
                DeliveryWriteOutcome::Committed
            );
            let accounts = s3.read();
            let object =
                &accounts.get(ACCOUNT).unwrap().buckets["logs-bucket"].objects[&batch.object_key];
            assert_eq!(
                object.content_encoding.as_deref(),
                compression.content_encoding()
            );
            let BodyRef::Memory(body) = &object.body else {
                panic!("memory writer")
            };
            assert_eq!(object.size, body.len() as u64);
            let decoded = match compression {
                Compression::Uncompressed => body.to_vec(),
                Compression::Gzip => {
                    let mut decoded = Vec::new();
                    flate2::read::GzDecoder::new(body.as_ref())
                        .read_to_end(&mut decoded)
                        .unwrap();
                    decoded
                }
                Compression::Snappy => crate::test_xerial_decoder::decode(body).unwrap().concat(),
            };
            // Producer newlines are kept and an empty record still gets one.
            assert_eq!(decoded, b"first\n\n\nsecond\x00\xff\n");
        }
    }
}
