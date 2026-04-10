use crate::state::SharedDynamoDbState;

/// Process TTL expirations across all tables.
///
/// Iterates every table with TTL enabled, checks each item for the configured
/// TTL attribute, and deletes items whose TTL value (epoch seconds as a Number)
/// is less than the current time.
///
/// Returns the total number of expired (deleted) items.
pub fn process_ttl_expirations(state: &SharedDynamoDbState) -> usize {
    let now = chrono::Utc::now().timestamp();
    process_ttl_expirations_at(state, now)
}

/// Same as [`process_ttl_expirations`] but accepts an explicit "now" timestamp,
/// making it easy to test without time manipulation.
pub fn process_ttl_expirations_at(state: &SharedDynamoDbState, now_epoch: i64) -> usize {
    let mut total_expired = 0;
    let mut state = state.write();

    for table in state.tables.values_mut() {
        if !table.ttl_enabled {
            continue;
        }

        let ttl_attr = match &table.ttl_attribute {
            Some(attr) => attr.clone(),
            None => continue,
        };

        let before = table.items.len();
        table.items.retain(|item| {
            let av = match item.get(&ttl_attr) {
                Some(v) => v,
                None => return true, // no TTL attribute → keep
            };

            // TTL attribute must be a Number ({"N": "..."})
            let epoch = match av.as_object().and_then(|obj| obj.get("N")) {
                Some(n) => match n.as_str().and_then(|s| s.parse::<i64>().ok()) {
                    Some(v) => v,
                    None => return true, // non-numeric → keep
                },
                None => return true, // not a Number type → keep
            };

            // Keep if TTL is in the future (or exactly now)
            epoch >= now_epoch
        });
        let removed = before - table.items.len();
        if removed > 0 {
            table.recalculate_stats();
        }
        total_expired += removed;
    }

    total_expired
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::*;
    use parking_lot::RwLock;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_state() -> SharedDynamoDbState {
        Arc::new(RwLock::new(DynamoDbState::new("123456789012", "us-east-1")))
    }

    fn make_table(name: &str, ttl_enabled: bool, ttl_attribute: Option<&str>) -> DynamoTable {
        DynamoTable {
            name: name.to_string(),
            arn: format!("arn:aws:dynamodb:us-east-1:123456789012:table/{}", name),
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
            tags: HashMap::new(),
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
            contributor_insights_counters: HashMap::new(),
            stream_enabled: false,
            stream_view_type: None,
            stream_arn: None,
            stream_records: Arc::new(RwLock::new(Vec::new())),
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
        state.write().tables.insert("t1".to_string(), table);

        let count = process_ttl_expirations_at(&state, now);
        assert_eq!(count, 1);
        assert_eq!(state.read().tables["t1"].items.len(), 0);
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
        state.write().tables.insert("t1".to_string(), table);

        let count = process_ttl_expirations_at(&state, now);
        assert_eq!(count, 0);
        assert_eq!(state.read().tables["t1"].items.len(), 1);
    }

    #[test]
    fn ttl_disabled_table_untouched() {
        let state = make_state();
        let now = 1_000_000;

        let mut table = make_table("t1", false, Some("ttl"));
        table
            .items
            .push(make_item("a", Some(json!({"N": "999999"}))));
        state.write().tables.insert("t1".to_string(), table);

        let count = process_ttl_expirations_at(&state, now);
        assert_eq!(count, 0);
        assert_eq!(state.read().tables["t1"].items.len(), 1);
    }

    #[test]
    fn item_without_ttl_attribute_kept() {
        let state = make_state();
        let now = 1_000_000;

        let mut table = make_table("t1", true, Some("ttl"));
        // Item without the TTL attribute at all
        table.items.push(make_item("a", None));
        state.write().tables.insert("t1".to_string(), table);

        let count = process_ttl_expirations_at(&state, now);
        assert_eq!(count, 0);
        assert_eq!(state.read().tables["t1"].items.len(), 1);
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
        state.write().tables.insert("t1".to_string(), table);

        let count = process_ttl_expirations_at(&state, now);
        assert_eq!(count, 0);
        assert_eq!(state.read().tables["t1"].items.len(), 1);
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
        state.write().tables.insert("t1".to_string(), table);

        let count = process_ttl_expirations_at(&state, now);
        assert_eq!(count, 2);
        assert_eq!(state.read().tables["t1"].items.len(), 3);
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
        state.write().tables.insert("t1".to_string(), table);

        process_ttl_expirations_at(&state, now);
        let s = state.read();
        assert_eq!(s.tables["t1"].item_count, 1);
    }
}
