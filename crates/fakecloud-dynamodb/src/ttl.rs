use std::collections::HashMap;
use std::sync::Arc;

use fakecloud_core::delivery::DeliveryBus;

use crate::state::{AttributeValue, SharedDynamoDbState, StreamUserIdentity};

/// A Kinesis change record collected during a TTL sweep and dispatched after
/// the state write lock is released: (target, keys, old_image).
type PendingTtlKinesis = (
    crate::service::KinesisDeliveryTarget,
    HashMap<String, AttributeValue>,
    HashMap<String, AttributeValue>,
);

/// Process TTL expirations across all tables.
///
/// Iterates every table with TTL enabled, checks each item for the configured
/// TTL attribute, and deletes items whose TTL value (epoch seconds as a Number)
/// is less than the current time. Each expiry emits a `REMOVE` stream record
/// (and Kinesis delivery, when `delivery` is supplied) carrying the
/// `dynamodb.amazonaws.com` userIdentity marker AWS attaches to TTL deletions.
///
/// Returns the total number of expired (deleted) items.
pub fn process_ttl_expirations(state: &SharedDynamoDbState) -> usize {
    process_ttl_expirations_with(state, None)
}

/// Like [`process_ttl_expirations`] but with a Kinesis [`DeliveryBus`] so
/// TTL-expiry REMOVE records reach a table's active Kinesis streaming
/// destinations.
pub fn process_ttl_expirations_with(
    state: &SharedDynamoDbState,
    delivery: Option<&Arc<DeliveryBus>>,
) -> usize {
    let now = chrono::Utc::now().timestamp();
    process_ttl_expirations_at_with(state, now, delivery)
}

/// Same as [`process_ttl_expirations`] but accepts an explicit "now" timestamp,
/// making it easy to test without time manipulation.
pub fn process_ttl_expirations_at(state: &SharedDynamoDbState, now_epoch: i64) -> usize {
    process_ttl_expirations_at_with(state, now_epoch, None)
}

/// Core TTL sweep with an explicit clock and an optional Kinesis delivery bus.
pub fn process_ttl_expirations_at_with(
    state: &SharedDynamoDbState,
    now_epoch: i64,
    delivery: Option<&Arc<DeliveryBus>>,
) -> usize {
    let mut total_expired = 0;
    // Kinesis deliveries collected under the lock and fired after it is
    // released, mirroring the put/delete paths (the delivery bus may take its
    // own locks, so we never hold the DynamoDB write lock across delivery).
    let mut pending_kinesis: Vec<PendingTtlKinesis> = Vec::new();
    {
        let mut mas = state.write();

        // Process TTL across all accounts
        for (_, acct_state) in mas.iter_mut() {
            let region = acct_state.region.clone();
            for table in acct_state.tables.values_mut() {
                if !table.ttl_enabled {
                    continue;
                }

                let ttl_attr = match &table.ttl_attribute {
                    Some(attr) => attr.clone(),
                    None => continue,
                };

                // Partition into kept vs. expired so we can emit a REMOVE
                // record per expired item rather than silently dropping it.
                let mut kept: Vec<HashMap<String, AttributeValue>> =
                    Vec::with_capacity(table.items.len());
                let mut expired: Vec<HashMap<String, AttributeValue>> = Vec::new();
                for item in std::mem::take(&mut table.items) {
                    if is_expired(&item, &ttl_attr, now_epoch) {
                        expired.push(item);
                    } else {
                        kept.push(item);
                    }
                }
                table.items = kept;

                if expired.is_empty() {
                    continue;
                }
                total_expired += expired.len();
                table.recalculate_stats();

                let kinesis_target = crate::service::kinesis_target_for(table);
                for item in expired {
                    let keys = extract_key_by_schema(table, &item);

                    if let Some(record) = crate::streams::generate_stream_record(
                        table,
                        "REMOVE",
                        keys.clone(),
                        Some(item.clone()),
                        None,
                        &region,
                    ) {
                        let mut record = record;
                        record.user_identity = Some(StreamUserIdentity::ttl());
                        crate::streams::add_stream_record(table, record);
                    }

                    if delivery.is_some() {
                        if let Some(target) = kinesis_target.clone() {
                            pending_kinesis.push((target, keys, item));
                        }
                    }
                }
            }
        } // end per-account loop
    } // write lock released here

    if let Some(delivery) = delivery {
        let ttl_identity = StreamUserIdentity::ttl();
        for (target, keys, old_image) in pending_kinesis {
            crate::service::deliver_kinesis_change(
                delivery,
                &target,
                "REMOVE",
                &keys,
                Some(&old_image),
                None,
                Some(&ttl_identity),
            );
        }
    }

    total_expired
}

/// Whether `item` is past its TTL. Items lacking the attribute, with a
/// non-Number type, or with a non-numeric value are never expired (kept).
fn is_expired(item: &HashMap<String, AttributeValue>, ttl_attr: &str, now_epoch: i64) -> bool {
    let Some(av) = item.get(ttl_attr) else {
        return false;
    };
    let epoch = av
        .as_object()
        .and_then(|obj| obj.get("N"))
        .and_then(|n| n.as_str())
        .and_then(|s| s.parse::<i64>().ok());
    match epoch {
        Some(e) => e < now_epoch,
        None => false,
    }
}

/// Extract the table's primary key (hash + optional range) from an item.
fn extract_key_by_schema(
    table: &crate::state::DynamoTable,
    item: &HashMap<String, AttributeValue>,
) -> HashMap<String, AttributeValue> {
    let mut key = HashMap::new();
    let hash = table.hash_key_name();
    if let Some(v) = item.get(hash) {
        key.insert(hash.to_string(), v.clone());
    }
    if let Some(range) = table.range_key_name() {
        if let Some(v) = item.get(range) {
            key.insert(range.to_string(), v.clone());
        }
    }
    key
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::*;
    use fakecloud_aws::arn::Arn;
    use parking_lot::RwLock;
    use serde_json::json;
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;

    fn make_state() -> SharedDynamoDbState {
        Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ))
    }

    fn make_table(name: &str, ttl_enabled: bool, ttl_attribute: Option<&str>) -> DynamoTable {
        DynamoTable {
            name: name.to_string(),
            arn: Arn::new(
                "dynamodb",
                "us-east-1",
                "123456789012",
                &format!("table/{name}"),
            )
            .to_string(),
            table_id: format!("{name}-id"),
            key_schema: vec![KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: "HASH".to_string(),
            }],
            attribute_definitions: vec![AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: "S".to_string(),
            }],
            provisioned_throughput: ProvisionedThroughput {
                read_capacity_units: 5,
                write_capacity_units: 5,
            },
            items: vec![],
            gsi: vec![],
            lsi: vec![],
            tags: BTreeMap::new(),
            created_at: chrono::Utc::now(),
            status: "ACTIVE".to_string(),
            item_count: 0,
            size_bytes: 0,
            billing_mode: "PROVISIONED".to_string(),
            ttl_attribute: ttl_attribute.map(|s| s.to_string()),
            ttl_enabled,
            resource_policy: None,
            pitr_enabled: false,
            kinesis_destinations: vec![],
            contributor_insights_status: "DISABLED".to_string(),
            contributor_insights_counters: BTreeMap::new(),
            stream_enabled: false,
            stream_view_type: None,
            stream_arn: None,
            stream_records: Arc::new(RwLock::new(Vec::new())),
            sse_type: None,
            sse_kms_key_arn: None,

            deletion_protection_enabled: false,
            on_demand_throughput: None,
            table_class: "STANDARD".to_string(),
            vector_indexes: Vec::new(),
        }
    }

    fn make_item(
        pk: &str,
        ttl_val: Option<serde_json::Value>,
    ) -> HashMap<String, serde_json::Value> {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), json!({"S": pk}));
        if let Some(ttl) = ttl_val {
            item.insert("ttl".to_string(), ttl);
        }
        item
    }

    #[test]
    fn expired_item_is_deleted() {
        let state = make_state();
        let now = 1_000_000;

        let mut table = make_table("t1", true, Some("ttl"));
        // Item with TTL in the past
        table
            .items
            .push(make_item("a", Some(json!({"N": "999999"}))));
        state
            .write()
            .default_mut()
            .tables
            .insert("t1".to_string(), table);

        let count = process_ttl_expirations_at(&state, now);
        assert_eq!(count, 1);
        assert_eq!(state.read().default_ref().tables["t1"].items.len(), 0);
    }

    #[test]
    fn future_item_is_kept() {
        let state = make_state();
        let now = 1_000_000;

        let mut table = make_table("t1", true, Some("ttl"));
        // Item with TTL in the future
        table
            .items
            .push(make_item("a", Some(json!({"N": "2000000"}))));
        state
            .write()
            .default_mut()
            .tables
            .insert("t1".to_string(), table);

        let count = process_ttl_expirations_at(&state, now);
        assert_eq!(count, 0);
        assert_eq!(state.read().default_ref().tables["t1"].items.len(), 1);
    }

    #[test]
    fn ttl_disabled_table_untouched() {
        let state = make_state();
        let now = 1_000_000;

        let mut table = make_table("t1", false, Some("ttl"));
        table
            .items
            .push(make_item("a", Some(json!({"N": "999999"}))));
        state
            .write()
            .default_mut()
            .tables
            .insert("t1".to_string(), table);

        let count = process_ttl_expirations_at(&state, now);
        assert_eq!(count, 0);
        assert_eq!(state.read().default_ref().tables["t1"].items.len(), 1);
    }

    #[test]
    fn item_without_ttl_attribute_kept() {
        let state = make_state();
        let now = 1_000_000;

        let mut table = make_table("t1", true, Some("ttl"));
        // Item without the TTL attribute at all
        table.items.push(make_item("a", None));
        state
            .write()
            .default_mut()
            .tables
            .insert("t1".to_string(), table);

        let count = process_ttl_expirations_at(&state, now);
        assert_eq!(count, 0);
        assert_eq!(state.read().default_ref().tables["t1"].items.len(), 1);
    }

    #[test]
    fn non_numeric_ttl_attribute_kept() {
        let state = make_state();
        let now = 1_000_000;

        let mut table = make_table("t1", true, Some("ttl"));
        // TTL attribute is a String, not a Number
        table
            .items
            .push(make_item("a", Some(json!({"S": "not-a-number"}))));
        state
            .write()
            .default_mut()
            .tables
            .insert("t1".to_string(), table);

        let count = process_ttl_expirations_at(&state, now);
        assert_eq!(count, 0);
        assert_eq!(state.read().default_ref().tables["t1"].items.len(), 1);
    }

    #[test]
    fn mixed_items_only_expired_deleted() {
        let state = make_state();
        let now = 1_000_000;

        let mut table = make_table("t1", true, Some("ttl"));
        table
            .items
            .push(make_item("expired1", Some(json!({"N": "500000"}))));
        table
            .items
            .push(make_item("future1", Some(json!({"N": "2000000"}))));
        table
            .items
            .push(make_item("expired2", Some(json!({"N": "999999"}))));
        table.items.push(make_item("no-ttl", None));
        table
            .items
            .push(make_item("string-ttl", Some(json!({"S": "oops"}))));
        state
            .write()
            .default_mut()
            .tables
            .insert("t1".to_string(), table);

        let count = process_ttl_expirations_at(&state, now);
        assert_eq!(count, 2);
        assert_eq!(state.read().default_ref().tables["t1"].items.len(), 3);
    }

    // bug-audit 2026-06-28: a TTL expiry must publish a REMOVE stream record
    // (carrying the dynamodb.amazonaws.com userIdentity marker) just like any
    // other delete path, not silently drop the item.
    #[test]
    fn ttl_expiry_emits_remove_stream_record() {
        let state = make_state();
        let now = 1_000_000;

        let mut table = make_table("t1", true, Some("ttl"));
        table.stream_enabled = true;
        table.stream_view_type = Some("NEW_AND_OLD_IMAGES".to_string());
        table.stream_arn = Some(format!("{}/stream/2026-06-28", table.arn));
        table
            .items
            .push(make_item("a", Some(json!({"N": "999999"}))));
        state
            .write()
            .default_mut()
            .tables
            .insert("t1".to_string(), table);

        let count = process_ttl_expirations_at(&state, now);
        assert_eq!(count, 1);

        let s = state.read();
        let records = s.default_ref().tables["t1"].stream_records.read();
        assert_eq!(records.len(), 1, "one REMOVE stream record emitted");
        let rec = &records[0];
        assert_eq!(rec.event_name, "REMOVE");
        let ui = rec
            .user_identity
            .as_ref()
            .expect("TTL record carries userIdentity");
        assert_eq!(ui.principal_id, "dynamodb.amazonaws.com");
        assert_eq!(ui.identity_type, "Service");
        // The expiring item is captured as the OldImage and the key is present.
        assert_eq!(
            rec.dynamodb.old_image.as_ref().unwrap()["pk"],
            json!({"S": "a"})
        );
        assert_eq!(rec.dynamodb.keys["pk"], json!({"S": "a"}));
    }

    #[test]
    fn stats_recalculated_after_expiration() {
        let state = make_state();
        let now = 1_000_000;

        let mut table = make_table("t1", true, Some("ttl"));
        table
            .items
            .push(make_item("a", Some(json!({"N": "500000"}))));
        table
            .items
            .push(make_item("b", Some(json!({"N": "2000000"}))));
        table.item_count = 2;
        table.size_bytes = 100;
        state
            .write()
            .default_mut()
            .tables
            .insert("t1".to_string(), table);

        process_ttl_expirations_at(&state, now);
        let s = state.read();
        assert_eq!(s.default_ref().tables["t1"].item_count, 1);
    }
}
