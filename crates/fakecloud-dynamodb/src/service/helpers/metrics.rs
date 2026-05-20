//! dynamodb helpers `metrics` concerns (audit-2026-05-19).

use super::*;

/// Build a `ConsumedCapacity` JSON object matching AWS shape.
/// Mode is one of `"TOTAL" | "INDEXES"`. Anything else returns `Value::Null`.
/// `read_units` and `write_units` are the consumed CU; either may be 0.
/// `INDEXES` mode also emits an empty `Table`/`GlobalSecondaryIndexes`/
/// `LocalSecondaryIndexes` breakdown so SDKs that deserialize the index
/// map round-trip.
pub(crate) fn build_consumed_capacity(
    mode: &str,
    table_name: &str,
    read_units: f64,
    write_units: f64,
) -> Value {
    if mode != "TOTAL" && mode != "INDEXES" {
        return Value::Null;
    }
    let total = read_units + write_units;
    let mut cc = json!({
        "TableName": table_name,
        "CapacityUnits": total,
    });
    if read_units > 0.0 {
        cc["ReadCapacityUnits"] = json!(read_units);
    }
    if write_units > 0.0 {
        cc["WriteCapacityUnits"] = json!(write_units);
    }
    if mode == "INDEXES" {
        let mut table_breakdown = json!({ "CapacityUnits": total });
        if read_units > 0.0 {
            table_breakdown["ReadCapacityUnits"] = json!(read_units);
        }
        if write_units > 0.0 {
            table_breakdown["WriteCapacityUnits"] = json!(write_units);
        }
        cc["Table"] = table_breakdown;
        cc["GlobalSecondaryIndexes"] = json!({});
        cc["LocalSecondaryIndexes"] = json!({});
    }
    cc
}

/// Read the request body's `ReturnConsumedCapacity` value with default
/// `"NONE"`, returning the canonical mode string.
pub(crate) fn return_consumed_mode(body: &Value) -> &str {
    body["ReturnConsumedCapacity"].as_str().unwrap_or("NONE")
}

/// Read the request body's `ReturnItemCollectionMetrics` value with
/// default `"NONE"`.
pub(crate) fn return_icm_mode(body: &Value) -> &str {
    body["ReturnItemCollectionMetrics"]
        .as_str()
        .unwrap_or("NONE")
}

/// Build the per-write `ItemCollectionMetrics` document AWS emits when the
/// table has at least one local secondary index and the caller asked for
/// `ReturnItemCollectionMetrics=SIZE`. Returns `Value::Null` whenever the
/// document should be omitted.
///
/// `ItemCollectionKey` is the partition-key attribute of the affected item
/// — we look up the key-schema's HASH element and copy that attribute from
/// `key`. `SizeEstimateRangeGB` reports a coarse [lower, upper] estimate;
/// real DynamoDB returns 0..1 GB for small collections, so we use the same
/// range as a stand-in until item-collection sizing is tracked precisely.
pub(crate) fn build_item_collection_metrics(
    mode: &str,
    table: &crate::state::DynamoTable,
    key: &std::collections::HashMap<String, crate::state::AttributeValue>,
) -> Value {
    if mode != "SIZE" || table.lsi.is_empty() {
        return Value::Null;
    }
    let partition_key = table
        .key_schema
        .iter()
        .find(|k| k.key_type == "HASH")
        .map(|k| k.attribute_name.as_str());
    let mut item_collection_key = serde_json::Map::new();
    if let Some(pk_name) = partition_key {
        if let Some(pk_value) = key.get(pk_name) {
            if let Ok(serialized) = serde_json::to_value(pk_value) {
                item_collection_key.insert(pk_name.to_string(), serialized);
            }
        }
    }
    json!({
        "ItemCollectionKey": Value::Object(item_collection_key),
        "SizeEstimateRangeGB": [0.0, 1.0],
    })
}
