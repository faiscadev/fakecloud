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
use crate::search::{self, SearchDelivery, SearchTarget};
use crate::topology::{SearchDomain, SharedTopology};
use crate::{DeliveryConfig, FirehoseAccounts, SharedFirehoseState, StreamKey};

/// An immutable destination/version snapshot and an already chosen, retry-stable key.
#[derive(Clone, Debug)]
pub struct DeliveryBatch {
    pub stream: StreamKey,
    pub version_id: String,
    pub destination: DeliveryConfig,
    pub records: Vec<Bytes>,
    /// Source-derived document IDs, one per record; empty for Direct PUT.
    pub document_ids: Vec<String>,
    /// Arrival time of each record; may be empty in tests.
    pub arrivals: Vec<DateTime<Utc>>,
    pub oldest_arrival: DateTime<Utc>,
    pub delivery_time: DateTime<Utc>,
    pub object_key: String,
}

impl DeliveryBatch {
    fn live_records(&self) -> Vec<&Bytes> {
        self.records
            .iter()
            .enumerate()
            .filter(|(position, _)| !self.destination.expired.contains(position))
            .map(|(_, record)| record)
            .collect()
    }
}

pub trait BatchWriter: Send + Sync {
    fn write(&self, batch: &DeliveryBatch) -> Result<DeliveryWriteOutcome, String>;
}

pub struct S3BatchWriter {
    state: SharedFirehoseState,
    s3: SharedS3State,
    store: Arc<RwLock<Option<Arc<dyn S3Store>>>>,
    topology: SharedTopology,
}

impl S3BatchWriter {
    pub fn new(
        state: SharedFirehoseState,
        s3: SharedS3State,
        store: Arc<RwLock<Option<Arc<dyn S3Store>>>>,
    ) -> Self {
        Self {
            state,
            s3,
            store,
            topology: Arc::default(),
        }
    }

    pub(crate) fn with_topology(mut self, topology: SharedTopology) -> Self {
        self.topology = topology;
        self
    }
}

impl S3BatchWriter {
    fn put(
        &self,
        batch: &DeliveryBatch,
        key: &str,
        records: &[impl AsRef<[u8]>],
    ) -> Result<DeliveryWriteOutcome, String> {
        // A missing bucket is the common persistent failure; detect it before
        // spending a full encode (up to ~132 MiB) on every retry.
        ensure_delivery_bucket(
            &self.s3,
            &batch.stream.account_id,
            &batch.destination.bucket,
        )?;
        let compression = batch.destination.output.compression;
        let body = encode_records(records, &batch.destination.output)
            .map_err(|e| format!("failed to encode Firehose batch {key}: {e}"))?;
        // Snapshot the backend before either configuration or S3 state is locked.
        let store = self.store.read().clone();
        put_object_via_store_guarded(
            &self.s3,
            store.as_deref(),
            DeliveryObjectRequest {
                account: &batch.stream.account_id,
                bucket: &batch.destination.bucket,
                key,
                body,
                options: DeliveryObjectOptions {
                    content_type: Some("application/octet-stream".into()),
                    content_encoding: compression.content_encoding().map(str::to_owned),
                    encryption: batch.destination.encryption.clone(),
                },
            },
            || self.live_stream(batch),
        )
    }

    /// Configuration changes preserve the instance ID, so old batches keep their
    /// captured destination. Delete/recreate and reset invalidate it.
    fn live_stream(
        &self,
        batch: &DeliveryBatch,
    ) -> Option<parking_lot::RwLockReadGuard<'_, FirehoseAccounts>> {
        let config = self.state.read();
        let current = config
            .get(&batch.stream.account_id)?
            .streams(&batch.stream.region)?
            .get(&batch.stream.name)?;
        (current.instance_id == batch.stream.instance_id).then_some(config)
    }

    /// The cluster to send to now. A domain resolves on every attempt, so its
    /// failure retries like any other until the retry duration ends.
    fn bulk_endpoint(&self, search: &SearchDelivery) -> Result<String, search::BulkError> {
        let family = search.family;
        let domain = match &search.target {
            SearchTarget::Cluster(endpoint) => return Ok(endpoint.clone()),
            SearchTarget::Domain(arn) => arn,
        };
        let lookup = self.topology.read().clone();
        match lookup.map(|topology| topology.search_domain(domain)) {
            Some(SearchDomain::Present {
                reachable_url: Some(url),
            }) => Ok(url.trim_end_matches('/').to_owned()),
            // Recorded for a domain deleted with records buffered; a domain nothing
            // can reach is the same to a client.
            _ => Err(family.connection_error()),
        }
    }

    /// One bulk attempt. `Err` asks for another; `Ok` means no record is left to
    /// send.
    fn index_pending(
        &self,
        batch: &DeliveryBatch,
        search: &SearchDelivery,
        progress: &mut search::BulkProgress,
    ) -> Result<(), String> {
        let first_attempt = search.attempts.begin();
        let result = self.bulk_endpoint(search).and_then(|endpoint| {
            search::post_bulk(
                search,
                &endpoint,
                batch,
                &progress.pending,
                &mut progress.first_bad_request,
            )
        });
        let elapsed = (Utc::now() - first_attempt).to_std().unwrap_or_default();
        let retrying = elapsed < search.retry;
        match result {
            Ok(items) => {
                progress.any_indexed |= items.len() < progress.pending.len();
                let (retriable, finished): (Vec<_>, Vec<_>) =
                    items.into_iter().partition(|failure| failure.retriable);
                progress.failed.extend(finished);
                if retrying && !retriable.is_empty() {
                    let message = format!(
                        "{} documents failed with a retriable status, first: {} {}",
                        retriable.len(),
                        retriable[0].code,
                        retriable[0].message
                    );
                    progress.pending = retriable.iter().map(|failure| failure.index).collect();
                    return Err(message);
                }
                progress.failed.extend(retriable);
            }
            Err(error) => {
                if retrying {
                    return Err(error.message);
                }
                progress
                    .failed
                    .extend(progress.pending.iter().map(|&index| search::Failure {
                        index,
                        code: error.code.clone(),
                        message: error.message.clone(),
                        retriable: false,
                        bad_request_item: false,
                    }));
            }
        }
        progress.pending.clear();
        Ok(())
    }

    /// The object of the records refused before sending, if any are still live.
    fn put_rejected(
        &self,
        batch: &DeliveryBatch,
        search: &SearchDelivery,
        progress: &search::BulkProgress,
    ) -> Result<DeliveryWriteOutcome, String> {
        let lines: Vec<String> = progress
            .rejected
            .iter()
            .filter(|failure| !batch.destination.expired.contains(&failure.index))
            .map(|failure| search::rejected_record(batch, failure))
            .collect();
        if lines.is_empty() {
            return Ok(DeliveryWriteOutcome::Committed);
        }
        let key = search::rejected_object_key(batch, search);
        self.put(batch, &key, &[lines.join("\n")])
    }

    fn write_search(
        &self,
        batch: &DeliveryBatch,
        search: &SearchDelivery,
    ) -> Result<DeliveryWriteOutcome, String> {
        // No lock is held across the request, so this cannot cover one in flight.
        if self.live_stream(batch).is_none() {
            return Ok(DeliveryWriteOutcome::Cancelled);
        }
        search.attempts.set_bulk_retry_due(false);
        let mut progress = search.attempts.progress(batch);
        progress
            .pending
            .retain(|index| !batch.destination.expired.contains(index));
        let live = |failure: &&search::Failure| !batch.destination.expired.contains(&failure.index);
        let mut outcome = DeliveryWriteOutcome::Committed;
        // Written when first seen, as recorded, but a failed S3 write must not hold
        // back the valid records: indexing goes on, and it is tried again below.
        let mut rejected_failure = None;
        if !progress.rejected_written {
            match self.put_rejected(batch, search, &progress) {
                Ok(DeliveryWriteOutcome::Cancelled) => {
                    return Ok(DeliveryWriteOutcome::Cancelled);
                }
                Ok(written) => {
                    outcome = written;
                    progress.rejected_written = true;
                }
                Err(error) => rejected_failure = Some(error),
            }
        }
        if !progress.pending.is_empty() {
            if let Err(message) = self.index_pending(batch, search, &mut progress) {
                search.attempts.save(progress);
                search.attempts.set_bulk_retry_due(true);
                return Err(message);
            }
        }
        // Also when the last pending records expired and nothing was sent.
        search::apply_parse_exception(search.family, &mut progress);
        progress.failed.sort_by_key(|failure| failure.index);
        // A failed S3 write below retries the write, not the indexing.
        search.attempts.save(progress.clone());
        if let Some(error) = rejected_failure {
            return Err(error);
        }
        if search.backup_all {
            outcome = self.put(batch, &batch.object_key, &batch.live_records())?;
        }
        let lines: Vec<String> = progress
            .failed
            .iter()
            .filter(live)
            .map(|failure| search::failed_record(batch, search, failure))
            .collect();
        if !lines.is_empty() {
            let name = batch.object_key.rsplit('/').next().unwrap_or_default();
            outcome = self.put(
                batch,
                &format!("{}{name}", search.error_prefix),
                &[lines.join("\n")],
            )?;
        }
        Ok(outcome)
    }
}

impl BatchWriter for S3BatchWriter {
    fn write(&self, batch: &DeliveryBatch) -> Result<DeliveryWriteOutcome, String> {
        if let Some(search) = &batch.destination.search {
            return self.write_search(batch, search);
        }
        self.put(batch, &batch.object_key, &batch.live_records())
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
        fn replace_object_body(
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
            self.disk
                .replace_object_body(bucket, key, version, body, meta)
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
                search: None,
                expired: Default::default(),
            },
            records: vec![
                Bytes::from_static(b"first\n"),
                Bytes::from_static(b"second\x00\xff"),
            ],
            document_ids: Vec::new(),
            arrivals: Vec::new(),
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

    fn search_batch(batch: &mut DeliveryBatch, retry: Duration) -> Arc<search::BulkAttempts> {
        let attempts = Arc::<search::BulkAttempts>::default();
        batch.destination.output.compression = Compression::Uncompressed;
        batch.records = vec![
            Bytes::from_static(b"{\"n\":0}"),
            Bytes::from_static(b"{\"n\":1}"),
        ];
        batch.destination.search = Some(SearchDelivery {
            family: search::SearchFamily::OpenSearch,
            // Nothing listens on port 1, so the request is refused.
            target: SearchTarget::Cluster("http://127.0.0.1:1".into()),
            index_name: "activity".into(),
            rotation: "NoRotation".into(),
            type_name: None,
            document_ids: true,
            backup_all: false,
            retry,
            error_prefix: "failed/".into(),
            attempts: attempts.clone(),
        });
        attempts
    }

    /// One response per bulk request, recording each request's document count.
    async fn bulk_stub(
        responses: Vec<serde_json::Value>,
    ) -> (String, Arc<parking_lot::Mutex<Vec<usize>>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let sent = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let seen = sent.clone();
        tokio::spawn(async move {
            for reply in responses {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut buffer = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    let n = sock.read(&mut chunk).await.unwrap();
                    buffer.extend_from_slice(&chunk[..n]);
                    let text = String::from_utf8_lossy(&buffer).to_lowercase();
                    let Some(end) = text.find("\r\n\r\n") else {
                        continue;
                    };
                    let length: usize = text
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length:"))
                        .map_or(0, |value| value.trim().parse().unwrap());
                    if buffer.len() >= end + 4 + length {
                        seen.lock().push(text[end + 4..].lines().count() / 2);
                        break;
                    }
                }
                let reply = reply.to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{reply}",
                    reply.len()
                );
                sock.write_all(response.as_bytes()).await.unwrap();
            }
        });
        (endpoint, sent)
    }

    fn failure_lines(s3: &SharedS3State, key: &str) -> Vec<serde_json::Value> {
        let suffix = &key["captured/".len()..];
        let accounts = s3.read();
        let object = &accounts.get(ACCOUNT).unwrap().buckets["logs-bucket"].objects
            [&format!("failed/{suffix}")];
        let BodyRef::Memory(body) = &object.body else {
            panic!("memory writer")
        };
        std::str::from_utf8(body)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn failure_objects(s3: &SharedS3State) -> Vec<(String, Vec<serde_json::Value>)> {
        let accounts = s3.read();
        accounts.get(ACCOUNT).unwrap().buckets["logs-bucket"]
            .objects
            .iter()
            .filter(|(key, _)| key.starts_with("failed/"))
            .map(|(key, object)| {
                let BodyRef::Memory(body) = &object.body else {
                    panic!("memory writer")
                };
                let lines = std::str::from_utf8(body)
                    .unwrap()
                    .lines()
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect();
                (key.clone(), lines)
            })
            .collect()
    }

    /// Records refused before sending land in their own object with their own
    /// shape; the others are sent, and a sent one that fails lands in another.
    #[tokio::test(flavor = "multi_thread")]
    async fn refused_and_sent_failures_land_in_separate_objects() {
        let (state, s3, mut batch) = fixture();
        search_batch(&mut batch, Duration::from_secs(300));
        batch.records = vec![
            Bytes::from_static(b"{\"ok\":1}\n"),
            Bytes::new(),
            Bytes::from_static(b"not json"),
            Bytes::from_static(b"\xff\xfe"),
        ];
        let (endpoint, sent) = bulk_stub(vec![serde_json::json!({
            "errors": true,
            "items": [
                {"index": {"status": 201}},
                {"index": {"status": 400, "error": {"type": "mapper_parsing_exception"}}},
            ],
        })])
        .await;
        batch.destination.search.as_mut().unwrap().target = SearchTarget::Cluster(endpoint);
        let writer = Arc::new(writer(&state, &s3, None));
        tokio::task::spawn_blocking(move || writer.write(&batch))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(*sent.lock(), vec![2]);
        let objects = failure_objects(&s3);
        assert_eq!(objects.len(), 2, "{objects:?}");
        let (refused, attempted): (Vec<_>, Vec<_>) = objects
            .iter()
            .flat_map(|(_, lines)| lines)
            .partition(|line| line["attemptsMade"] == 0);
        assert_eq!(refused.len(), 2);
        assert!(refused.iter().all(|l| l.get("osDocumentId").is_none()));
        assert_eq!(attempted.len(), 1);
        assert_eq!(attempted[0]["errorCode"], "OS.ServiceException");
        assert_eq!(attempted[0]["attemptsMade"], 1);
        assert_eq!(attempted[0]["osIndexName"], "activity");
    }

    /// A failure object's lines are separated, with nothing after the last.
    #[tokio::test(flavor = "multi_thread")]
    async fn failure_objects_end_without_a_newline() {
        let (state, s3, mut batch) = fixture();
        search_batch(&mut batch, Duration::ZERO);
        let key = batch.object_key.clone();
        let writer = Arc::new(writer(&state, &s3, None));
        tokio::task::spawn_blocking(move || writer.write(&batch))
            .await
            .unwrap()
            .unwrap();
        let accounts = s3.read();
        let suffix = &key["captured/".len()..];
        let object = &accounts.get(ACCOUNT).unwrap().buckets["logs-bucket"].objects
            [&format!("failed/{suffix}")];
        let BodyRef::Memory(body) = &object.body else {
            panic!("memory writer")
        };
        assert_eq!(body.iter().filter(|b| **b == b'\n').count(), 1);
        assert_ne!(body.last(), Some(&b'\n'));
    }

    struct FixedTopology(SearchDomain);

    impl crate::FirehoseTopology for FixedTopology {
        fn kinesis_stream_exists(&self, _stream_arn: &str) -> bool {
            true
        }
        fn search_domain(&self, _domain_arn: &str) -> SearchDomain {
            self.0.clone()
        }
    }

    fn domain_writer(
        state: &SharedFirehoseState,
        s3: &SharedS3State,
        domain: SearchDomain,
    ) -> Arc<S3BatchWriter> {
        let topology: Arc<dyn crate::FirehoseTopology> = Arc::new(FixedTopology(domain));
        Arc::new(writer(state, s3, None).with_topology(Arc::new(RwLock::new(Some(topology)))))
    }

    /// A `DomainARN` destination delivers to the URL its domain resolves to.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_domain_arn_destination_delivers_to_the_resolved_url() {
        let (state, s3, mut batch) = fixture();
        search_batch(&mut batch, Duration::from_secs(300));
        batch.destination.search.as_mut().unwrap().target =
            SearchTarget::Domain("arn:aws:es:us-east-1:123456789012:domain/d".into());
        let (endpoint, sent) = bulk_stub(vec![serde_json::json!({
            "errors": false,
            "items": [{"index": {"status": 201}}, {"index": {"status": 201}}],
        })])
        .await;
        let writer = domain_writer(
            &state,
            &s3,
            SearchDomain::Present {
                reachable_url: Some(format!("{endpoint}/")),
            },
        );
        tokio::task::spawn_blocking(move || writer.write(&batch))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(*sent.lock(), vec![2]);
        assert!(failure_objects(&s3).is_empty());
    }

    /// A domain nothing can reach, or one gone since, retries like any failed
    /// request and then fails into S3 with the code naming why.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_unavailable_domain_retries_then_fails_into_s3() {
        for (domain, code) in [
            (
                SearchDomain::Present {
                    reachable_url: None,
                },
                "OS.ConnectionReset",
            ),
            (SearchDomain::Absent, "OS.ConnectionReset"),
        ] {
            let (state, s3, mut batch) = fixture();
            let attempts = search_batch(&mut batch, Duration::from_secs(300));
            batch.destination.search.as_mut().unwrap().target =
                SearchTarget::Domain("arn:aws:es:us-east-1:123456789012:domain/d".into());
            let writer = domain_writer(&state, &s3, domain);
            let retried = batch.clone();
            let retrying = writer.clone();
            assert!(
                tokio::task::spawn_blocking(move || retrying.write(&retried))
                    .await
                    .unwrap()
                    .is_err()
            );
            assert_eq!(attempts.made(), 1);
            assert!(failure_objects(&s3).is_empty(), "still retrying");

            batch.destination.search.as_mut().unwrap().retry = Duration::ZERO;
            tokio::task::spawn_blocking(move || writer.write(&batch))
                .await
                .unwrap()
                .unwrap();
            let objects = failure_objects(&s3);
            let lines = &objects[0].1;
            assert_eq!(lines.len(), 2);
            assert_eq!(lines[0]["errorCode"], code);
            assert_eq!(lines[0]["attemptsMade"], 2);
        }
    }

    /// A failed S3 write of refused records does not hold back the valid ones: they
    /// are indexed, and only the write is tried again, not the indexing.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_refused_write_does_not_stop_indexing() {
        let (state, s3, mut batch) = fixture();
        search_batch(&mut batch, Duration::from_secs(300));
        batch.records = vec![Bytes::from_static(b"{\"ok\":1}"), Bytes::new()];
        let (endpoint, sent) = bulk_stub(vec![serde_json::json!({
            "errors": false, "items": [{"index": {"status": 201}}],
        })])
        .await;
        batch.destination.search.as_mut().unwrap().target = SearchTarget::Cluster(endpoint);
        batch.destination.bucket = "missing-for-now".into();
        let writer = Arc::new(writer(&state, &s3, None));
        let (first_writer, first_batch) = (writer.clone(), batch.clone());
        let error = tokio::task::spawn_blocking(move || first_writer.write(&first_batch))
            .await
            .unwrap()
            .unwrap_err();
        assert!(error.contains("missing-for-now"), "{error}");
        assert_eq!(*sent.lock(), vec![1], "indexed despite the failed write");
        assert_eq!(
            batch
                .destination
                .search
                .as_ref()
                .unwrap()
                .attempts
                .retry_delay(Duration::from_secs(300)),
            None,
            "retries on S3's interval, not the bulk schedule"
        );

        s3.write().get_or_create(ACCOUNT).buckets.insert(
            "missing-for-now".into(),
            S3Bucket::new("missing-for-now", REGION, "owner"),
        );
        tokio::task::spawn_blocking(move || writer.write(&batch))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(*sent.lock(), vec![1], "not indexed a second time");
        let accounts = s3.read();
        let objects = &accounts.get(ACCOUNT).unwrap().buckets["missing-for-now"].objects;
        assert_eq!(objects.len(), 1);
    }

    /// Failures collected before the last pending records expired are still
    /// finished: sorted, with the code the batch's outcome gives them.
    #[tokio::test(flavor = "multi_thread")]
    async fn failures_are_finished_when_the_last_pending_records_expire() {
        let (state, s3, mut batch) = fixture();
        search_batch(&mut batch, Duration::from_secs(300));
        batch.records = vec![
            Bytes::from_static(b"{\"n\":0}"),
            Bytes::from_static(b"{\"n\":1}"),
            Bytes::from_static(b"{\"n\":2}"),
            Bytes::from_static(b"{\"n\":3}"),
        ];
        let search = batch.destination.search.clone().unwrap();
        let bad = |index| search::Failure {
            index,
            code: "OS.ServiceException".into(),
            message: "m".into(),
            retriable: false,
            bad_request_item: true,
        };
        search.attempts.save(search::BulkProgress {
            failed: vec![bad(2), bad(0)],
            pending: vec![3],
            rejected_written: true,
            any_indexed: true,
            ..Default::default()
        });
        batch.destination.expired.insert(3);
        let key = batch.object_key.clone();
        let writer = Arc::new(writer(&state, &s3, None));
        tokio::task::spawn_blocking(move || writer.write(&batch))
            .await
            .unwrap()
            .unwrap();
        let lines = failure_lines(&s3, &key);
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0]["rawData"], "eyJuIjowfQ==", "sorted by position");
        assert_eq!(
            lines[0]["errorCode"],
            "OS.AmazonOpenSearchServiceParseException"
        );
    }

    /// A record that aged out while its batch retried is not delivered, while the
    /// rest of the batch is, at positions the retry state still recognises.
    #[tokio::test(flavor = "multi_thread")]
    async fn expired_records_are_left_out_of_delivery() {
        let (state, s3, mut batch) = fixture();
        search_batch(&mut batch, Duration::from_secs(300));
        batch.destination.expired.insert(0);
        let (endpoint, sent) = bulk_stub(vec![serde_json::json!({
            "errors": false,
            "items": [{"index": {"status": 201}}],
        })])
        .await;
        batch.destination.search.as_mut().unwrap().target = SearchTarget::Cluster(endpoint);
        let writer = Arc::new(writer(&state, &s3, None));
        tokio::task::spawn_blocking(move || writer.write(&batch))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(*sent.lock(), vec![1]);

        let (state, s3, mut batch) = fixture();
        batch.destination.output.compression = Compression::Uncompressed;
        batch.destination.expired.insert(0);
        super::BatchWriter::write(&self::tests::writer(&state, &s3, None), &batch).unwrap();
        let accounts = s3.read();
        let object =
            &accounts.get(ACCOUNT).unwrap().buckets["logs-bucket"].objects[&batch.object_key];
        assert!(
            matches!(&object.body, BodyRef::Memory(bytes) if bytes.as_ref() == b"second\x00\xff")
        );
    }

    /// Retriable item statuses are resent alone until the retry duration ends;
    /// a final rejection is recorded with the attempts really made.
    #[tokio::test(flavor = "multi_thread")]
    async fn retriable_items_are_resent_alone() {
        let (state, s3, mut batch) = fixture();
        let attempts = search_batch(&mut batch, Duration::from_secs(300));
        batch.records = vec![
            Bytes::from_static(b"{\"n\":0}"),
            Bytes::from_static(b"{\"n\":1}"),
            Bytes::from_static(b"{\"n\":2}"),
        ];
        let (endpoint, sent) = bulk_stub(vec![
            serde_json::json!({"errors": true, "items": [
                {"index": {"status": 201}},
                {"index": {"status": 429, "error": {"type": "es_rejected_execution_exception"}}},
                {"index": {"status": 400, "error": {"type": "mapper_parsing_exception"}}},
            ]}),
            serde_json::json!({"errors": false, "items": [{"index": {"status": 201}}]}),
        ])
        .await;
        batch.destination.search.as_mut().unwrap().target = SearchTarget::Cluster(endpoint);
        let writer = Arc::new(writer(&state, &s3, None));
        let key = batch.object_key.clone();
        let write = |writer: Arc<S3BatchWriter>, batch: DeliveryBatch| {
            tokio::task::spawn_blocking(move || writer.write(&batch))
        };
        assert!(write(writer.clone(), batch.clone()).await.unwrap().is_err());
        write(writer, batch).await.unwrap().unwrap();
        assert_eq!(*sent.lock(), vec![3, 1]);
        assert_eq!(attempts.made(), 2);
        let lines = failure_lines(&s3, &key);
        assert_eq!(lines.len(), 1, "only the rejected document failed");
        assert_eq!(
            lines[0]["errorCode"], "OS.AmazonOpenSearchServiceParseException",
            "part of the batch was indexed and nothing was refused"
        );
        assert_eq!(lines[0]["attemptsMade"], 2);
    }

    /// A batch of a deleted or recreated stream is not posted to the cluster.
    #[tokio::test(flavor = "multi_thread")]
    async fn search_batch_of_a_replaced_stream_is_cancelled_before_posting() {
        let (state, s3, mut batch) = fixture();
        let attempts = search_batch(&mut batch, Duration::from_secs(300));
        batch.stream.instance_id = uuid::Uuid::new_v4();
        let writer = Arc::new(writer(&state, &s3, None));
        let outcome = tokio::task::spawn_blocking(move || writer.write(&batch))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outcome, DeliveryWriteOutcome::Cancelled);
        assert_eq!(attempts.made(), 0);
    }

    /// A batch that waited behind a failing one still gets its retry duration,
    /// which starts at its own first request and not when it detached.
    #[tokio::test(flavor = "multi_thread")]
    async fn retry_duration_starts_at_the_first_bulk_request() {
        let (state, s3, mut batch) = fixture();
        batch.delivery_time = Utc::now() - chrono::Duration::hours(1);
        let attempts = search_batch(&mut batch, Duration::from_secs(300));
        let writer = Arc::new(writer(&state, &s3, None));
        let write = |writer: Arc<S3BatchWriter>, batch: DeliveryBatch| {
            tokio::task::spawn_blocking(move || writer.write(&batch))
        };
        assert!(write(writer.clone(), batch.clone()).await.unwrap().is_err());
        assert_eq!(attempts.made(), 1);
        assert!(s3.read().get(ACCOUNT).unwrap().buckets["logs-bucket"]
            .objects
            .is_empty());
    }

    /// Once the retry duration is spent, every record goes to the failure
    /// location with the code of the failure and the real attempt count.
    #[tokio::test(flavor = "multi_thread")]
    async fn exhausted_retries_write_failure_records() {
        let (state, s3, mut batch) = fixture();
        let attempts = search_batch(&mut batch, Duration::ZERO);
        let writer = Arc::new(writer(&state, &s3, None));
        let key = batch.object_key.clone();
        tokio::task::spawn_blocking(move || writer.write(&batch))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(attempts.made(), 1);
        let suffix = &key["captured/".len()..];
        let accounts = s3.read();
        let object = &accounts.get(ACCOUNT).unwrap().buckets["logs-bucket"].objects
            [&format!("failed/{suffix}")];
        let BodyRef::Memory(body) = &object.body else {
            panic!("memory writer")
        };
        let lines: Vec<serde_json::Value> = std::str::from_utf8(body)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0]["errorCode"], "OS.ConnectionReset");
        assert_eq!(lines[0]["attemptsMade"], 1);
    }

    const KEY_ARN: &str = "arn:aws:kms:us-east-1:123456789012:key/stream-key";

    /// Records every SSE-KMS encryption S3 performs.
    #[derive(Default)]
    struct RecordingKms {
        encrypted: std::sync::Mutex<Vec<(String, Vec<u8>)>>,
    }

    impl fakecloud_core::delivery::KmsHook for RecordingKms {
        fn is_ciphertext(&self, _account_id: &str, stored: &str) -> bool {
            stored.starts_with("envelope:")
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
            _caller: fakecloud_core::delivery::KmsCaller<'_>,
            _encryption_context: std::collections::HashMap<String, String>,
        ) -> Result<Vec<u8>, fakecloud_core::delivery::KmsError> {
            Err(fakecloud_core::delivery::KmsError::new(
                fakecloud_core::delivery::KmsErrorKind::AccessDenied,
                "not used",
            ))
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
