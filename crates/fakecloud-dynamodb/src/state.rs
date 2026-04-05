use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// A single DynamoDB attribute value (tagged union matching the AWS wire format).
/// AWS sends attribute values as `{"S": "hello"}`, `{"N": "42"}`, etc.
pub type AttributeValue = Value;

/// Extract the "typed" inner value for comparison purposes.
/// Returns (type_tag, inner_value) e.g. ("S", "hello") or ("N", "42").
pub fn attribute_type_and_value(av: &Value) -> Option<(&str, &Value)> {
    let obj = av.as_object()?;
    if obj.len() != 1 {
        return None;
    }
    let (k, v) = obj.iter().next()?;
    Some((k.as_str(), v))
}

#[derive(Debug, Clone)]
pub struct KeySchemaElement {
    pub attribute_name: String,
    pub key_type: String, // HASH or RANGE
}

#[derive(Debug, Clone)]
pub struct AttributeDefinition {
    pub attribute_name: String,
    pub attribute_type: String, // S, N, B
}

#[derive(Debug, Clone)]
pub struct ProvisionedThroughput {
    pub read_capacity_units: i64,
    pub write_capacity_units: i64,
}

#[derive(Debug, Clone)]
pub struct GlobalSecondaryIndex {
    pub index_name: String,
    pub key_schema: Vec<KeySchemaElement>,
    pub projection: Projection,
    pub provisioned_throughput: Option<ProvisionedThroughput>,
}

#[derive(Debug, Clone)]
pub struct LocalSecondaryIndex {
    pub index_name: String,
    pub key_schema: Vec<KeySchemaElement>,
    pub projection: Projection,
}

#[derive(Debug, Clone)]
pub struct Projection {
    pub projection_type: String, // ALL, KEYS_ONLY, INCLUDE
    pub non_key_attributes: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct DynamoTable {
    pub name: String,
    pub arn: String,
    pub key_schema: Vec<KeySchemaElement>,
    pub attribute_definitions: Vec<AttributeDefinition>,
    pub provisioned_throughput: ProvisionedThroughput,
    pub items: Vec<HashMap<String, AttributeValue>>,
    pub gsi: Vec<GlobalSecondaryIndex>,
    pub lsi: Vec<LocalSecondaryIndex>,
    pub tags: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
    pub status: String,
    pub item_count: i64,
    pub size_bytes: i64,
    pub billing_mode: String, // PROVISIONED or PAY_PER_REQUEST
}

impl DynamoTable {
    /// Get the hash key attribute name from the key schema.
    pub fn hash_key_name(&self) -> &str {
        self.key_schema
            .iter()
            .find(|k| k.key_type == "HASH")
            .map(|k| k.attribute_name.as_str())
            .unwrap_or("")
    }

    /// Get the range key attribute name from the key schema (if any).
    pub fn range_key_name(&self) -> Option<&str> {
        self.key_schema
            .iter()
            .find(|k| k.key_type == "RANGE")
            .map(|k| k.attribute_name.as_str())
    }

    /// Find an item index by its primary key.
    pub fn find_item_index(&self, key: &HashMap<String, AttributeValue>) -> Option<usize> {
        let hash_key = self.hash_key_name();
        let range_key = self.range_key_name();

        self.items.iter().position(|item| {
            let hash_match = match (item.get(hash_key), key.get(hash_key)) {
                (Some(a), Some(b)) => a == b,
                _ => false,
            };
            if !hash_match {
                return false;
            }
            match range_key {
                Some(rk) => match (item.get(rk), key.get(rk)) {
                    (Some(a), Some(b)) => a == b,
                    (None, None) => true,
                    _ => false,
                },
                None => true,
            }
        })
    }

    /// Estimate item size in bytes (rough approximation).
    fn estimate_item_size(item: &HashMap<String, AttributeValue>) -> i64 {
        let mut size: i64 = 0;
        for (k, v) in item {
            size += k.len() as i64;
            size += Self::estimate_value_size(v);
        }
        size
    }

    fn estimate_value_size(v: &Value) -> i64 {
        match v {
            Value::Object(obj) => {
                if let Some(s) = obj.get("S").and_then(|v| v.as_str()) {
                    s.len() as i64
                } else if let Some(n) = obj.get("N").and_then(|v| v.as_str()) {
                    n.len() as i64
                } else if obj.contains_key("BOOL") || obj.contains_key("NULL") {
                    1
                } else if let Some(l) = obj.get("L").and_then(|v| v.as_array()) {
                    3 + l.iter().map(Self::estimate_value_size).sum::<i64>()
                } else if let Some(m) = obj.get("M").and_then(|v| v.as_object()) {
                    3 + m
                        .iter()
                        .map(|(k, v)| k.len() as i64 + Self::estimate_value_size(v))
                        .sum::<i64>()
                } else if let Some(ss) = obj.get("SS").and_then(|v| v.as_array()) {
                    ss.iter()
                        .filter_map(|v| v.as_str())
                        .map(|s| s.len() as i64)
                        .sum()
                } else if let Some(ns) = obj.get("NS").and_then(|v| v.as_array()) {
                    ns.iter()
                        .filter_map(|v| v.as_str())
                        .map(|s| s.len() as i64)
                        .sum()
                } else if let Some(b) = obj.get("B").and_then(|v| v.as_str()) {
                    // Base64-encoded binary
                    (b.len() as i64 * 3) / 4
                } else {
                    v.to_string().len() as i64
                }
            }
            _ => v.to_string().len() as i64,
        }
    }

    /// Recalculate item_count and size_bytes from the items vec.
    pub fn recalculate_stats(&mut self) {
        self.item_count = self.items.len() as i64;
        self.size_bytes = self.items.iter().map(Self::estimate_item_size).sum::<i64>();
    }
}

pub struct DynamoDbState {
    pub account_id: String,
    pub region: String,
    pub tables: HashMap<String, DynamoTable>,
}

impl DynamoDbState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            tables: HashMap::new(),
        }
    }

    pub fn reset(&mut self) {
        self.tables.clear();
    }
}

pub type SharedDynamoDbState = Arc<RwLock<DynamoDbState>>;
