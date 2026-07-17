use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use base64::Engine;
use fakecloud_core::delivery::KinesisDelivery;

use crate::service::{append_record, select_shard_mut};
use crate::state::SharedKinesisState;

/// Kinesis delivery implementation for cross-service integrations.
pub struct KinesisDeliveryImpl {
    state: SharedKinesisState,
    /// Set whenever a cross-service delivery appends a record. The
    /// `KinesisDelivery` trait is synchronous and cannot reach the async
    /// snapshot writer the service handler uses, so DynamoDB-streaming /
    /// Logs-subscription / EventBridge-target records used to vanish on
    /// restart while direct PutRecord survived. A background flusher in the
    /// server polls this flag and persists. bug-audit 2026-06-15, 4.8.
    dirty: Option<Arc<AtomicBool>>,
}

impl KinesisDeliveryImpl {
    pub fn new(state: SharedKinesisState) -> Arc<Self> {
        Arc::new(Self { state, dirty: None })
    }

    /// Construct with a dirty flag a background flusher watches to persist
    /// cross-service deliveries. bug-audit 4.8.
    pub fn with_dirty_flag(state: SharedKinesisState, dirty: Arc<AtomicBool>) -> Arc<Self> {
        Arc::new(Self {
            state,
            dirty: Some(dirty),
        })
    }

    fn mark_dirty(&self) {
        if let Some(flag) = &self.dirty {
            flag.store(true, Ordering::Release);
        }
    }
}

impl KinesisDelivery for KinesisDeliveryImpl {
    fn put_record(&self, stream_arn: &str, data: &str, partition_key: &str) {
        // Extract stream name from ARN: arn:aws:kinesis:region:account:stream/StreamName
        let stream_name = if let Some(name_part) = stream_arn.rsplit('/').next() {
            // Handles both arn:aws:kinesis:region:account:stream/Name and plain name
            name_part
        } else {
            stream_arn
        };

        let default_id = self.state.read().default_account_id().to_string();
        let target_account = stream_arn
            .split(':')
            .nth(4)
            .filter(|s| !s.is_empty())
            .unwrap_or(&default_id);
        let mut delivered = false;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(target_account);
        if let Some(stream) = state.streams.get_mut(stream_name) {
            // Route by the MD5 hash-key range over OPEN shards, exactly like the
            // direct PutRecord path. The old hand-rolled `sum(bytes) % len`
            // included CLOSED parent shards (post-split/merge) and ignored hash
            // ranges, so a fanned-in record could land on a drained closed shard
            // whose iterator is null -> the record was silently lost.
            if !stream.shards.is_empty() {
                match select_shard_mut(stream, partition_key, None) {
                    Ok(shard) => {
                        let data_bytes = base64::engine::general_purpose::STANDARD
                            .decode(data)
                            .unwrap_or_else(|_| data.as_bytes().to_vec());
                        let sequence_number = append_record(shard, partition_key, data_bytes);

                        tracing::debug!(
                            stream_name = %stream_name,
                            partition_key = %partition_key,
                            sequence_number = %sequence_number,
                            "Delivered record to Kinesis stream"
                        );
                        delivered = true;
                    }
                    Err(err) => {
                        tracing::warn!(
                            stream_name = %stream_name,
                            ?err,
                            "Kinesis delivery shard selection failed"
                        );
                    }
                }
            }
        } else {
            tracing::warn!(
                stream_arn = %stream_arn,
                stream_name = %stream_name,
                "Stream not found for Kinesis delivery"
            );
        }
        drop(accounts);
        if delivered {
            self.mark_dirty();
        }
    }
}

/// Persist `state` as a Kinesis snapshot. Shared by the background flusher and
/// exposed for tests so the durability of cross-service deliveries can be
/// verified with a real save+load round-trip. bug-audit 2026-06-15, 4.8.
pub fn persist_kinesis_state(
    state: &SharedKinesisState,
    store: &Arc<dyn fakecloud_persistence::SnapshotStore>,
) -> std::io::Result<()> {
    let snapshot = crate::state::KinesisSnapshot {
        schema_version: crate::state::KINESIS_SNAPSHOT_SCHEMA_VERSION,
        accounts: Some(state.read().clone()),
        state: None,
    };
    let bytes = serde_json::to_vec(&snapshot)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    store.save(&bytes)
}

/// Background loop that persists cross-service Kinesis deliveries.
///
/// The `KinesisDelivery` trait is synchronous and cannot reach the async
/// snapshot writer the request handler uses, so DynamoDB-streaming /
/// Logs-subscription / EventBridge-target records were never checkpointed.
/// This loop watches the shared `dirty` flag the delivery impl sets and
/// persists whenever it flips, bounding the data-loss window to one poll
/// interval. bug-audit 2026-06-15, 4.8.
pub async fn run_delivery_flusher(
    state: SharedKinesisState,
    store: Arc<dyn fakecloud_persistence::SnapshotStore>,
    dirty: Arc<AtomicBool>,
    interval: std::time::Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        if dirty.swap(false, Ordering::AcqRel) {
            let state = state.clone();
            let store = store.clone();
            let dirty = dirty.clone();
            let join =
                tokio::task::spawn_blocking(move || persist_kinesis_state(&state, &store)).await;
            match join {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    tracing::warn!(%err, "kinesis delivery flush failed; will retry");
                    dirty.store(true, Ordering::Release);
                }
                Err(err) => {
                    tracing::warn!(%err, "kinesis delivery flush task panicked");
                    dirty.store(true, Ordering::Release);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{KinesisShard, KinesisState, KinesisStream, SharedKinesisState};
    use chrono::Utc;
    use fakecloud_aws::arn::Arn;
    use parking_lot::RwLock;
    use std::collections::BTreeMap;

    fn make_stream(name: &str, shard_count: usize) -> KinesisStream {
        let shards = (0..shard_count)
            .map(|i| KinesisShard {
                shard_id: format!("shardId-{i:012}"),
                starting_hash_key: "0".to_string(),
                ending_hash_key: "340282366920938463463374607431768211455".to_string(),
                parent_shard_id: None,
                adjacent_parent_shard_id: None,
                is_open: true,
                next_sequence_number: 0,
                records: Vec::new(),
            })
            .collect();
        KinesisStream {
            stream_name: name.to_string(),
            stream_arn: Arn::new(
                "kinesis",
                "us-east-1",
                "123456789012",
                &format!("stream/{name}"),
            )
            .to_string(),
            stream_status: "ACTIVE".to_string(),
            stream_creation_timestamp: Utc::now(),
            retention_period_hours: 24,
            stream_mode: "PROVISIONED".to_string(),
            encryption_type: "NONE".to_string(),
            key_id: None,
            shard_count: shard_count as i32,
            open_shard_count: shard_count as i32,
            tags: BTreeMap::new(),
            shards,
            next_shard_index: shard_count as i32,
            enhanced_metrics: Vec::new(),
            warm_throughput_mibps: None,
            max_record_size_kib: None,
        }
    }

    fn make_state(stream: KinesisStream) -> SharedKinesisState {
        let mut mas: fakecloud_core::multi_account::MultiAccountState<KinesisState> =
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", "");
        mas.get_or_create("123456789012")
            .streams
            .insert(stream.stream_name.clone(), stream);
        Arc::new(RwLock::new(mas))
    }

    #[test]
    fn put_record_delivers_to_shard() {
        let stream = make_stream("my-stream", 1);
        let state = make_state(stream);
        let delivery = KinesisDeliveryImpl::new(state.clone());
        let encoded = base64::engine::general_purpose::STANDARD.encode(b"hello");
        delivery.put_record(
            "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream",
            &encoded,
            "pk-1",
        );
        let mas = state.read();
        let guard = mas.default_ref();
        let stream = guard.streams.get("my-stream").unwrap();
        assert_eq!(stream.shards[0].records.len(), 1);
        let rec = &stream.shards[0].records[0];
        assert_eq!(rec.data, b"hello");
        assert_eq!(rec.partition_key, "pk-1");
    }

    // bug-audit 2026-06-15, 4.8: a cross-service Kinesis delivery (e.g. a
    // DynamoDB streaming record) must survive a save+load round-trip. Before
    // the dirty-flag + flusher, only the service handler persisted, so these
    // records vanished on restart while direct PutRecord survived.
    #[test]
    fn delivered_record_survives_save_and_load() {
        use fakecloud_persistence::SnapshotStore;
        use std::sync::atomic::{AtomicBool, Ordering};

        let stream = make_stream("durable", 1);
        let arn = stream.stream_arn.clone();
        let state = make_state(stream);
        let dirty = Arc::new(AtomicBool::new(false));
        let delivery = KinesisDeliveryImpl::with_dirty_flag(state.clone(), dirty.clone());
        let encoded = base64::engine::general_purpose::STANDARD.encode(b"ddb-change");
        delivery.put_record(&arn, &encoded, "pk");
        assert!(
            dirty.load(Ordering::Acquire),
            "delivery must mark state dirty for the flusher"
        );

        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn SnapshotStore> = Arc::new(
            fakecloud_persistence::DiskSnapshotStore::new(dir.path().join("snapshot.json")),
        );
        super::persist_kinesis_state(&state, &store).unwrap();

        let bytes = store.load().unwrap().expect("snapshot written");
        let snapshot: crate::state::KinesisSnapshot = serde_json::from_slice(&bytes).unwrap();
        let accounts = snapshot.accounts.expect("multi-account snapshot");
        let restored = accounts.default_ref();
        let s = restored
            .streams
            .get("durable")
            .expect("stream after reload");
        let total: usize = s.shards.iter().map(|sh| sh.records.len()).sum();
        assert_eq!(total, 1, "delivered record must survive restart");
        assert_eq!(
            s.shards
                .iter()
                .flat_map(|sh| &sh.records)
                .next()
                .unwrap()
                .data,
            b"ddb-change"
        );
    }

    // bug-hunt restart-dataloss: the EventBridge Scheduler and EventBridge-Pipes
    // delivery buses must build their Kinesis sender with `with_dirty_flag`, not
    // `::new`. `::new` leaves `dirty: None`, so `mark_dirty()` is a no-op and a
    // record delivered to a Kinesis stream target by a schedule/pipe lands in
    // memory but never flips the shared flag the flusher persists on -> the
    // record vanishes on restart. This locks in the `::new` vs `with_dirty_flag`
    // contract those wiring sites depend on.
    #[test]
    fn new_does_not_mark_dirty_but_with_dirty_flag_does() {
        let stream = make_stream("contract", 1);
        let arn = stream.stream_arn.clone();
        let encoded = base64::engine::general_purpose::STANDARD.encode(b"evt");

        // `::new` (the buggy scheduler/pipes wiring) delivers but never signals
        // the flusher, so the record would be lost on restart.
        let state_new = make_state(make_stream("contract", 1));
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let via_new = KinesisDeliveryImpl::new(state_new.clone());
        via_new.put_record(&arn, &encoded, "pk");
        assert!(
            !flag.load(Ordering::Acquire),
            "::new must not touch any external dirty flag"
        );

        // `with_dirty_flag` (the fixed wiring) flips the shared flag so the
        // background flusher persists the delivery.
        let state_dirty = make_state(stream);
        let via_flag = KinesisDeliveryImpl::with_dirty_flag(state_dirty.clone(), flag.clone());
        via_flag.put_record(&arn, &encoded, "pk");
        assert!(
            flag.load(Ordering::Acquire),
            "with_dirty_flag must flip the shared flag so the flusher persists"
        );
    }

    // bug-audit 2026-06-26, 4.1: after a split/merge the modulo router could
    // land a fanned-in record on a CLOSED parent shard (drained, null iterator)
    // -> silent loss. Delivery must route by hash range over OPEN shards only.
    #[test]
    fn delivery_skips_closed_shards() {
        let mut stream = make_stream("split", 2);
        // shard 0 closed (the drained parent), shard 1 open (the child) and
        // owning the entire hash range.
        stream.shards[0].is_open = false;
        stream.shards[0].ending_hash_key = "0".to_string();
        stream.shards[1].is_open = true;
        stream.shards[1].starting_hash_key = "0".to_string();
        stream.shards[1].ending_hash_key = "340282366920938463463374607431768211455".to_string();
        let arn = stream.stream_arn.clone();
        let state = make_state(stream);
        let delivery = KinesisDeliveryImpl::new(state.clone());
        let encoded = base64::engine::general_purpose::STANDARD.encode(b"x");
        // Try several partition keys; none may land on the closed shard.
        for pk in ["a", "b", "c", "d", "e"] {
            delivery.put_record(&arn, &encoded, pk);
        }
        let mas = state.read();
        let s = mas.default_ref().streams.get("split").unwrap().clone();
        assert_eq!(
            s.shards[0].records.len(),
            0,
            "closed shard must get nothing"
        );
        assert_eq!(s.shards[1].records.len(), 5, "open shard gets all records");
    }

    #[test]
    fn put_record_stores_raw_bytes_when_not_base64() {
        let stream = make_stream("s", 1);
        let state = make_state(stream);
        let delivery = KinesisDeliveryImpl::new(state.clone());
        delivery.put_record(
            "arn:aws:kinesis:us-east-1:123456789012:stream/s",
            "not-base64!",
            "p",
        );
        let mas = state.read();
        let guard = mas.default_ref();
        let rec = &guard.streams.get("s").unwrap().shards[0].records[0];
        assert_eq!(rec.data, b"not-base64!");
    }

    #[test]
    fn put_record_distributes_across_shards_by_partition_key() {
        let stream = make_stream("s", 4);
        let state = make_state(stream);
        let delivery = KinesisDeliveryImpl::new(state.clone());
        delivery.put_record(
            "arn:aws:kinesis:us-east-1:123456789012:stream/s",
            &base64::engine::general_purpose::STANDARD.encode(b"a"),
            "A",
        );
        delivery.put_record(
            "arn:aws:kinesis:us-east-1:123456789012:stream/s",
            &base64::engine::general_purpose::STANDARD.encode(b"b"),
            "B",
        );
        let mas = state.read();
        let guard = mas.default_ref();
        let stream = guard.streams.get("s").unwrap();
        let total: usize = stream.shards.iter().map(|s| s.records.len()).sum();
        assert_eq!(total, 2);
    }

    #[test]
    fn put_record_unknown_stream_is_noop() {
        let stream = make_stream("s", 1);
        let state = make_state(stream);
        let delivery = KinesisDeliveryImpl::new(state.clone());
        delivery.put_record(
            "arn:aws:kinesis:us-east-1:123456789012:stream/other",
            "AAA=",
            "p",
        );
        let mas = state.read();
        let guard = mas.default_ref();
        assert!(guard.streams.get("s").unwrap().shards[0].records.is_empty());
    }

    #[test]
    fn put_record_handles_plain_stream_name_arg() {
        let stream = make_stream("plain", 1);
        let state = make_state(stream);
        let delivery = KinesisDeliveryImpl::new(state.clone());
        delivery.put_record("plain", "AAA=", "p");
        let mas = state.read();
        let guard = mas.default_ref();
        assert_eq!(
            guard.streams.get("plain").unwrap().shards[0].records.len(),
            1
        );
    }
}
