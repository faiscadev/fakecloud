use crate::state::{AttributeValue, DynamoDbStreamRecord, DynamoTable, StreamRecord};
use chrono::Utc;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use uuid::Uuid;

/// Mint a strictly-monotonic, unique DynamoDB stream sequence number.
///
/// Sequence numbers were previously the wall-clock nanosecond timestamp, which
/// collides under batch writes in the same nanosecond and can go backwards on
/// a coarse clock or NTP step. Since `AT_SEQUENCE_NUMBER`/`AFTER_SEQUENCE_NUMBER`
/// iterators locate a record by exact match, a duplicate seq makes them replay
/// or skip records. An atomic counter guarantees every record gets a unique,
/// monotonically increasing number (per-stream ordering is a monotonic subset
/// of the global one).
///
/// The counter is *seeded once* from the current wall-clock nanoseconds rather
/// than from 1. Stream records persist across restart (4.5), so a counter that
/// reset to 1 would re-mint sequence numbers that collide with already-persisted
/// records. The clock seed handles the common forward-moving case; because a
/// clock can also step *backwards* (NTP), `observe_stream_sequence` additionally
/// raises the floor above every persisted record loaded on restart, so the next
/// minted number is guaranteed greater. The atomic increment preserves
/// uniqueness + monotonicity within a run; zero-padding to a fixed width keeps
/// the decimal strings lexicographically ordered. bug-audit 2026-05-28, 4.4.
static STREAM_SEQUENCE: OnceLock<AtomicU64> = OnceLock::new();

fn stream_sequence_counter() -> &'static AtomicU64 {
    STREAM_SEQUENCE.get_or_init(|| {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(1)
            .max(1);
        AtomicU64::new(seed)
    })
}

pub fn next_stream_sequence() -> String {
    format!(
        "{:021}",
        stream_sequence_counter().fetch_add(1, Ordering::Relaxed)
    )
}

/// Raise the sequence-number floor so the next minted number exceeds an
/// already-existing one. Called for every persisted stream record loaded on
/// restart: the wall-clock seed alone is not restart-safe (the clock can step
/// backwards via NTP and reissue a persisted number), so we also bump the
/// counter above the maximum sequence number we have ever seen. bug-audit
/// 2026-05-28, 4.4.
pub fn observe_stream_sequence(sequence_number: &str) {
    if let Ok(seq) = sequence_number.parse::<u64>() {
        stream_sequence_counter().fetch_max(seq.saturating_add(1), Ordering::Relaxed);
    }
}

/// Generate a stream record for a table mutation.
/// This should be called after the mutation is applied.
pub fn generate_stream_record(
    table: &DynamoTable,
    event_name: &str, // INSERT, MODIFY, REMOVE
    keys: HashMap<String, AttributeValue>,
    old_image: Option<HashMap<String, AttributeValue>>,
    new_image: Option<HashMap<String, AttributeValue>>,
    region: &str,
) -> Option<StreamRecord> {
    if !table.stream_enabled {
        return None;
    }

    let stream_view_type = table.stream_view_type.as_ref()?;

    // Filter images based on stream view type
    let (filtered_old, filtered_new) = match stream_view_type.as_str() {
        "KEYS_ONLY" => (None, None),
        "NEW_IMAGE" => (None, new_image),
        "OLD_IMAGE" => (old_image, None),
        "NEW_AND_OLD_IMAGES" => (old_image, new_image),
        _ => (None, None),
    };

    // Calculate size
    let size_bytes = serde_json::to_vec(&keys).ok()?.len() as i64
        + filtered_old
            .as_ref()
            .and_then(|img| serde_json::to_vec(img).ok())
            .map(|v| v.len() as i64)
            .unwrap_or(0)
        + filtered_new
            .as_ref()
            .and_then(|img| serde_json::to_vec(img).ok())
            .map(|v| v.len() as i64)
            .unwrap_or(0);

    let event_id = Uuid::new_v4().to_string();
    let sequence_number = next_stream_sequence();

    Some(StreamRecord {
        event_id,
        event_name: event_name.to_string(),
        event_version: "1.1".to_string(),
        event_source: "aws:dynamodb".to_string(),
        aws_region: region.to_string(),
        dynamodb: DynamoDbStreamRecord {
            keys,
            new_image: filtered_new,
            old_image: filtered_old,
            sequence_number,
            size_bytes,
            stream_view_type: stream_view_type.clone(),
        },
        event_source_arn: table.stream_arn.clone().unwrap_or_default(),
        timestamp: Utc::now(),
        user_identity: None,
    })
}

/// Add a stream record to the table's stream.
/// Records are retained for 24 hours.
pub fn add_stream_record(table: &mut DynamoTable, record: StreamRecord) {
    let mut records = table.stream_records.write();
    records.push(record);

    // Clean up records older than 24 hours
    let cutoff = Utc::now() - chrono::Duration::hours(24);
    records.retain(|r| r.timestamp > cutoff);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{KeySchemaElement, ProvisionedThroughput};
    use parking_lot::RwLock;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    #[test]
    fn stream_sequence_numbers_are_unique_and_monotonic() {
        // bug-audit 4.4: even when minted in a tight loop (formerly all the
        // same nanosecond timestamp), every sequence number must be distinct
        // and strictly increasing — otherwise AT/AFTER_SEQUENCE_NUMBER
        // iterators replay or skip records.
        let seqs: Vec<String> = (0..10_000).map(|_| next_stream_sequence()).collect();
        let unique: std::collections::HashSet<&String> = seqs.iter().collect();
        assert_eq!(unique.len(), seqs.len(), "sequence numbers must be unique");
        for w in seqs.windows(2) {
            // Fixed-width zero-padding makes lexicographic order == numeric.
            assert!(w[0] < w[1], "sequence numbers must strictly increase");
            assert_eq!(w[0].len(), w[1].len(), "fixed-width padding");
        }
    }

    #[test]
    fn stream_sequence_numbers_seeded_above_low_persisted_values() {
        // bug-audit 4.4 (Cubic): the counter is wall-clock seeded, not reset to
        // 1, so post-restart numbers never collide with low sequence numbers
        // already persisted from a prior run.
        let seq = next_stream_sequence();
        let n: u128 = seq.parse().unwrap();
        // Nanoseconds since the 2017 epoch are well above 1e18; certainly above
        // any handful of records a prior run would have minted from a low seed.
        assert!(
            n > 1_000_000_000_000_000_000,
            "sequence not clock-seeded: {seq}"
        );
    }

    #[test]
    fn observe_stream_sequence_raises_floor_above_persisted() {
        // bug-audit 4.4 (Cubic): after observing a persisted sequence number,
        // the next minted number must exceed it — even if it is far above the
        // wall-clock seed (simulating a clock that went backwards on restart).
        // A high-but-u64-valid value (real sequence numbers are u64), well
        // above the ~1.7e18 nanosecond seed, zero-padded to the 21-char width.
        let high = format!("{:021}", 9_000_000_000_000_000_000u64);
        observe_stream_sequence(&high);
        let next: u64 = next_stream_sequence().parse().unwrap();
        assert!(
            next > 9_000_000_000_000_000_000,
            "floor not strictly above observed: {next}"
        );
    }

    #[test]
    fn stream_sequence_numbers_unique_under_concurrency() {
        // Mint from many threads at once: the atomic counter must still hand
        // out distinct numbers (no same-instant collision).
        use std::thread;
        let handles: Vec<_> = (0..8)
            .map(|_| {
                thread::spawn(|| {
                    (0..1000)
                        .map(|_| next_stream_sequence())
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let mut all = std::collections::HashSet::new();
        for h in handles {
            for s in h.join().unwrap() {
                assert!(all.insert(s), "duplicate sequence number across threads");
            }
        }
        assert_eq!(all.len(), 8 * 1000);
    }

    fn make_stream_table() -> DynamoTable {
        DynamoTable {
            name: "test-table".to_string(),
            arn: "arn:aws:dynamodb:eu-west-1:999999999999:table/test-table".to_string(),
            table_id: "test-table-id".to_string(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: "HASH".to_string(),
            }],
            attribute_definitions: vec![],
            provisioned_throughput: ProvisionedThroughput {
                read_capacity_units: 5,
                write_capacity_units: 5,
            },
            items: vec![],
            gsi: vec![],
            lsi: vec![],
            tags: BTreeMap::new(),
            created_at: Utc::now(),
            status: "ACTIVE".to_string(),
            item_count: 0,
            size_bytes: 0,
            billing_mode: "PROVISIONED".to_string(),
            ttl_attribute: None,
            ttl_enabled: false,
            resource_policy: None,
            pitr_enabled: false,
            kinesis_destinations: vec![],
            contributor_insights_status: "DISABLED".to_string(),
            contributor_insights_counters: BTreeMap::new(),
            stream_enabled: true,
            stream_view_type: Some("NEW_AND_OLD_IMAGES".to_string()),
            stream_arn: Some(
                "arn:aws:dynamodb:eu-west-1:999999999999:table/test-table/stream/123".to_string(),
            ),
            stream_records: Arc::new(RwLock::new(Vec::new())),
            sse_type: None,
            sse_kms_key_arn: None,

            deletion_protection_enabled: false,
            on_demand_throughput: None,
            table_class: "STANDARD".to_string(),
            vector_indexes: Vec::new(),
        }
    }

    #[test]
    fn stream_record_uses_configured_region() {
        let table = make_stream_table();
        let mut keys = HashMap::new();
        keys.insert("pk".to_string(), json!({"S": "user1"}));

        let record = generate_stream_record(
            &table,
            "INSERT",
            keys,
            None,
            Some(HashMap::from([("pk".to_string(), json!({"S": "user1"}))])),
            "eu-west-1",
        )
        .expect("stream record should be generated");

        assert_eq!(
            record.aws_region, "eu-west-1",
            "stream record must use the configured region, not a hardcoded value"
        );
    }

    // bug-audit 2026-05-28, 4.5: un-consumed stream change records must survive a
    // serialize/deserialize (snapshot restart) cycle; they used to be
    // #[serde(skip)] and silently vanished.
    #[test]
    fn stream_records_persist_across_serde() {
        let table = make_stream_table();
        let mut keys = HashMap::new();
        keys.insert("pk".to_string(), json!({"S": "user1"}));
        let record = generate_stream_record(
            &table,
            "INSERT",
            keys,
            None,
            Some(HashMap::from([("pk".to_string(), json!({"S": "user1"}))])),
            "eu-west-1",
        )
        .expect("stream record should be generated");
        let want_event_id = record.event_id.clone();
        table.stream_records.write().push(record);

        let json = serde_json::to_string(&table).unwrap();
        let restored: DynamoTable = serde_json::from_str(&json).unwrap();
        let recs = restored.stream_records.read();
        assert_eq!(recs.len(), 1, "pending stream record must survive restart");
        assert_eq!(recs[0].event_id, want_event_id);
        assert_eq!(recs[0].event_name, "INSERT");
    }
}
