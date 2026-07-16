//! Account-partitioned, serializable state for the Amazon SageMaker control plane.
//!
//! The registry is deliberately schema-light: every named resource family
//! (models, endpoints, endpoint configs, training jobs, notebook instances,
//! pipelines, ...) is stored in one uniform two-level map,
//! `resources[family][id]`, where the stored value is the resource's JSON
//! record (its persisted attributes plus any minted ARN / id / timestamps).
//! Keeping every key a plain `String` means the snapshot never depends on the
//! tuple-key serde adapter and new resource families need no new struct fields.
//!
//! Alongside the resource map are:
//! * `tags` — resource tags keyed by ARN.
//! * `singletons` — account-scoped singleton values keyed by a stable string
//!   (e.g. the Service Catalog portfolio status).

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const SAGEMAKER_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Per-account SageMaker control-plane state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SageMakerData {
    /// `family -> id -> record`. The record is the resource's persisted JSON
    /// attributes plus any minted ARN / id / timestamps.
    #[serde(default)]
    pub resources: BTreeMap<String, BTreeMap<String, Value>>,
    /// Resource tags keyed by ARN.
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
    /// Account-scoped singleton values keyed by a stable string.
    #[serde(default)]
    pub singletons: BTreeMap<String, Value>,
    /// Monotonic counter used to mint unique ids within an account.
    #[serde(default)]
    pub seq: u64,
}

impl SageMakerData {
    /// Fetch a resource record by family + id.
    pub fn get_resource(&self, family: &str, id: &str) -> Option<&Value> {
        self.resources.get(family).and_then(|m| m.get(id))
    }

    /// Fetch a mutable resource record by family + id.
    pub fn get_resource_mut(&mut self, family: &str, id: &str) -> Option<&mut Value> {
        self.resources.get_mut(family).and_then(|m| m.get_mut(id))
    }

    /// Insert / replace a resource record.
    pub fn put_resource(&mut self, family: &str, id: &str, record: Value) {
        self.resources
            .entry(family.to_string())
            .or_default()
            .insert(id.to_string(), record);
    }

    /// Remove a resource record, returning it if present.
    pub fn remove_resource(&mut self, family: &str, id: &str) -> Option<Value> {
        let removed = self.resources.get_mut(family).and_then(|m| m.remove(id));
        if let Some(m) = self.resources.get(family) {
            if m.is_empty() {
                self.resources.remove(family);
            }
        }
        removed
    }

    /// Resolve a caller-supplied identifier value to a stored key within a
    /// family. Matches the direct storage key first, then falls back to a record
    /// whose *canonical* identifier member — `{Family}Name`, `{Family}Id` or
    /// `{Family}Arn` — equals the value (so a resource keyed by its Name can
    /// still be described by the minted Id or ARN the create returned).
    ///
    /// The fallback is restricted to the family's own canonical members rather
    /// than any suffix-matching `*Name` / `*Id` / `*Arn` member, so an unrelated
    /// member that happens to carry an equal value (e.g. a shared `RoleArn`, or a
    /// cross-referenced `SourceArn`) on a sibling record cannot mis-resolve to
    /// the wrong record.
    pub fn resolve_key(&self, family: &str, value: &str) -> Option<String> {
        let m = self.resources.get(family)?;
        if m.contains_key(value) {
            return Some(value.to_string());
        }
        let canonical = [
            format!("{family}Name"),
            format!("{family}Id"),
            format!("{family}Arn"),
        ];
        for (k, rec) in m {
            if let Some(obj) = rec.as_object() {
                if canonical
                    .iter()
                    .any(|cand| obj.get(cand).and_then(Value::as_str) == Some(value))
                {
                    return Some(k.clone());
                }
            }
        }
        None
    }

    /// All records of a family as `(id, record)` pairs, ordered by id.
    pub fn list_resource_entries(&self, family: &str) -> Vec<(String, Value)> {
        self.resources
            .get(family)
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default()
    }

    /// Next unique sequence value for id minting.
    pub fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }
}

impl AccountState for SageMakerData {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedSageMakerState = Arc<RwLock<MultiAccountState<SageMakerData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct SageMakerSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<SageMakerData>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn new_account_is_empty() {
        let data = SageMakerData::new_for_account("000000000000", "us-east-1", "");
        assert!(data.resources.is_empty());
        assert!(data.tags.is_empty());
    }

    #[test]
    fn resource_round_trips() {
        let mut d = SageMakerData::default();
        d.put_resource(
            "Model",
            "m1",
            json!({"ModelName": "m1", "ModelArn": "arn:aws:sagemaker:us-east-1:0:model/m1"}),
        );
        assert_eq!(d.get_resource("Model", "m1").unwrap()["ModelName"], "m1");
        assert_eq!(d.resolve_key("Model", "m1").as_deref(), Some("m1"));
        // resolve by the minted ARN
        assert_eq!(
            d.resolve_key("Model", "arn:aws:sagemaker:us-east-1:0:model/m1")
                .as_deref(),
            Some("m1")
        );
        assert!(d.remove_resource("Model", "m1").is_some());
        assert!(d.get_resource("Model", "m1").is_none());
    }

    #[test]
    fn resolve_key_ignores_noncanonical_sibling_member() {
        let mut d = SageMakerData::default();
        let shared = "arn:aws:iam::0:role/shared";
        // A record keyed by its name, carrying an unrelated RoleArn member.
        d.put_resource("Model", "m1", json!({"ModelName": "m1", "RoleArn": shared}));
        // Resolving by the shared RoleArn must NOT mis-match m1: RoleArn is not a
        // canonical Model identifier member.
        assert_eq!(d.resolve_key("Model", shared), None);
        // The canonical ModelName still resolves.
        assert_eq!(d.resolve_key("Model", "m1").as_deref(), Some("m1"));
    }
}
