//! Account-partitioned, serializable state for the AWS IoT Core control plane.
//!
//! The registry is deliberately schema-light: every named resource family
//! (things, policies, certificates, jobs, topic rules, security profiles, ...)
//! is stored in one uniform two-level map, `resources[resource_type][id]`,
//! where the stored value is the resource's JSON record (its persisted
//! attributes plus any minted ARN / id / timestamps). Keeping every key a
//! plain `String` means the snapshot never depends on the tuple-key serde
//! adapter and new resource families need no new struct fields.
//!
//! Alongside the resource map are:
//! * `tags` — resource tags keyed by ARN.
//! * `singletons` — account-scoped singleton configurations (indexing config,
//!   event configurations, V2 logging options, audit configuration, default
//!   authorizer, encryption configuration, package configuration, the CA
//!   registration code, ...), keyed by a stable string.
//! * `relations` — many-to-many relationship sets keyed by a stable string
//!   (e.g. `thing-principals:<thing>` -> principal ARNs,
//!   `principal-policies:<principal>` -> policy names,
//!   `group-things:<group>` -> thing names). Stored as ordered vectors so
//!   list operations are deterministic.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const IOT_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Per-account IoT Core control-plane state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IotData {
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
    /// Many-to-many relationship sets keyed by a stable string.
    #[serde(default)]
    pub relations: BTreeMap<String, Vec<String>>,
    /// Monotonic counter used to mint unique ids within an account.
    #[serde(default)]
    pub seq: u64,
}

impl IotData {
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

    /// Next unique sequence value for id minting.
    pub fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }
}

impl AccountState for IotData {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedIotState = Arc<RwLock<MultiAccountState<IotData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct IotSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<IotData>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn new_account_is_empty() {
        let data = IotData::new_for_account("000000000000", "us-east-1", "");
        assert!(data.resources.is_empty());
        assert!(data.tags.is_empty());
        assert!(data.singletons.is_empty());
    }

    #[test]
    fn resource_round_trips() {
        let mut d = IotData::default();
        d.put_resource("things", "sensor-1", json!({"thingName": "sensor-1"}));
        assert_eq!(
            d.get_resource("things", "sensor-1").unwrap()["thingName"],
            "sensor-1"
        );
        assert_eq!(d.list_resources("things").len(), 1);
        assert!(d.remove_resource("things", "sensor-1").is_some());
        assert!(d.get_resource("things", "sensor-1").is_none());
        assert!(!d.resources.contains_key("things"));
    }
}
