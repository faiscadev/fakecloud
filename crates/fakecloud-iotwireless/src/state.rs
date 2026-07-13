//! Account-partitioned, serializable state for the AWS IoT Wireless control
//! plane.
//!
//! Like the sibling `fakecloud-iot` crate, the registry is deliberately
//! schema-light: every named resource family (destinations, device profiles,
//! service profiles, FUOTA tasks, multicast groups, wireless devices, wireless
//! gateways, network-analyzer configurations, task definitions, position
//! configurations, ...) is stored in one uniform two-level map,
//! `resources[resource_type][id]`, where the stored value is the resource's
//! JSON record (its persisted attributes plus any minted ARN / id /
//! timestamps). Keeping every key a plain `String` means the snapshot never
//! depends on the tuple-key serde adapter and new resource families need no
//! new struct fields.
//!
//! Alongside the resource map are:
//! * `tags` — resource tags keyed by ARN.
//! * `singletons` — account-scoped singleton configurations (log levels,
//!   event configuration by resource type, metric configuration, ...), keyed
//!   by a stable string.
//! * `relations` — many-to-many relationship sets keyed by a stable string
//!   (e.g. `multicast-devices:<group>` -> wireless device ids,
//!   `fuota-devices:<task>` -> wireless device ids). Stored as ordered vectors
//!   so list operations are deterministic.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const IOTWIRELESS_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Per-account IoT Wireless control-plane state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IotWirelessData {
    /// `resource_type -> id -> record`. The record is the resource's persisted
    /// JSON attributes plus any minted ARN / id / timestamps.
    #[serde(default)]
    pub resources: BTreeMap<String, BTreeMap<String, Value>>,
    /// Resource tags keyed by ARN.
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
    /// Account-scoped singleton configurations keyed by a stable string.
    #[serde(default)]
    pub singletons: BTreeMap<String, Value>,
    /// Raw `@httpPayload` blobs (e.g. resource GeoJSON positions) keyed by a
    /// stable string. Stored base64-free as UTF-8 lossy strings.
    #[serde(default)]
    pub blobs: BTreeMap<String, String>,
    /// Many-to-many relationship sets keyed by a stable string.
    #[serde(default)]
    pub relations: BTreeMap<String, Vec<String>>,
    /// Monotonic counter used to mint unique ids within an account.
    #[serde(default)]
    pub seq: u64,
}

impl IotWirelessData {
    /// Fetch a resource record by type + id.
    pub fn get_resource(&self, rtype: &str, id: &str) -> Option<&Value> {
        self.resources.get(rtype).and_then(|m| m.get(id))
    }

    /// Insert / replace a resource record.
    pub fn put_resource(&mut self, rtype: &str, id: &str, record: Value) {
        self.resources
            .entry(rtype.to_string())
            .or_default()
            .insert(id.to_string(), record);
    }

    /// Remove a resource record, returning it if present.
    pub fn remove_resource(&mut self, rtype: &str, id: &str) -> Option<Value> {
        let removed = self.resources.get_mut(rtype).and_then(|m| m.remove(id));
        if let Some(m) = self.resources.get(rtype) {
            if m.is_empty() {
                self.resources.remove(rtype);
            }
        }
        removed
    }

    /// All records of a type, ordered by id.
    pub fn list_resources(&self, rtype: &str) -> Vec<Value> {
        self.resources
            .get(rtype)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default()
    }

    /// All records of a type as `(id, record)` pairs, ordered by id.
    pub fn list_resource_entries(&self, rtype: &str) -> Vec<(String, Value)> {
        self.resources
            .get(rtype)
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default()
    }

    /// Next unique sequence value for id minting.
    pub fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    // ---- account-scoped singleton configurations ----

    /// Read a singleton configuration by its stable key.
    pub fn get_singleton(&self, key: &str) -> Option<&Value> {
        self.singletons.get(key)
    }

    /// Insert / replace a singleton configuration.
    pub fn put_singleton(&mut self, key: &str, value: Value) {
        self.singletons.insert(key.to_string(), value);
    }

    // ---- many-to-many relationship edges ----

    /// Add `member` to the ordered relationship set `key` (idempotent — a member
    /// already present is not duplicated).
    pub fn add_relation(&mut self, key: &str, member: &str) {
        let set = self.relations.entry(key.to_string()).or_default();
        if !set.iter().any(|m| m == member) {
            set.push(member.to_string());
        }
    }

    /// Remove `member` from the relationship set `key`; drops the empty set.
    pub fn remove_relation(&mut self, key: &str, member: &str) {
        if let Some(set) = self.relations.get_mut(key) {
            set.retain(|m| m != member);
            if set.is_empty() {
                self.relations.remove(key);
            }
        }
    }

    /// The members of relationship set `key`, in insertion order.
    pub fn list_relation(&self, key: &str) -> Vec<String> {
        self.relations.get(key).cloned().unwrap_or_default()
    }
}

impl AccountState for IotWirelessData {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedIotWirelessState = Arc<RwLock<MultiAccountState<IotWirelessData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct IotWirelessSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<IotWirelessData>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn new_account_is_empty() {
        let data = IotWirelessData::new_for_account("000000000000", "us-east-1", "");
        assert!(data.resources.is_empty());
        assert!(data.tags.is_empty());
        assert!(data.singletons.is_empty());
    }

    #[test]
    fn resource_round_trips() {
        let mut d = IotWirelessData::default();
        d.put_resource("destinations", "dest-1", json!({"Name": "dest-1"}));
        assert_eq!(
            d.get_resource("destinations", "dest-1").unwrap()["Name"],
            "dest-1"
        );
        assert_eq!(d.list_resources("destinations").len(), 1);
        assert!(d.remove_resource("destinations", "dest-1").is_some());
        assert!(d.get_resource("destinations", "dest-1").is_none());
        assert!(!d.resources.contains_key("destinations"));
    }

    #[test]
    fn singleton_round_trips() {
        let mut d = IotWirelessData::default();
        assert!(d.get_singleton("metric-configuration").is_none());
        d.put_singleton("metric-configuration", json!({"SummaryMetric": {"Status": "Enabled"}}));
        assert_eq!(
            d.get_singleton("metric-configuration").unwrap()["SummaryMetric"]["Status"],
            "Enabled"
        );
    }

    #[test]
    fn relation_edges_round_trip() {
        let mut d = IotWirelessData::default();
        d.add_relation("fuota-multicast:task-1", "mc-1");
        d.add_relation("fuota-multicast:task-1", "mc-2");
        // idempotent
        d.add_relation("fuota-multicast:task-1", "mc-1");
        assert_eq!(d.list_relation("fuota-multicast:task-1"), vec!["mc-1", "mc-2"]);
        d.remove_relation("fuota-multicast:task-1", "mc-1");
        assert_eq!(d.list_relation("fuota-multicast:task-1"), vec!["mc-2"]);
        d.remove_relation("fuota-multicast:task-1", "mc-2");
        assert!(d.list_relation("fuota-multicast:task-1").is_empty());
        assert!(!d.relations.contains_key("fuota-multicast:task-1"));
    }
}
