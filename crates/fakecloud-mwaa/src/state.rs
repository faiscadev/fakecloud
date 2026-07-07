//! Account-partitioned, serializable state for Amazon MWAA's (`mwaa`) control
//! plane.
//!
//! Each environment is stored as its already-output-valid wire JSON object
//! (`serde_json::Value`, PascalCase members matching the restJson1 member names
//! -- MWAA declares no `jsonName` overrides) keyed by its `Name`. Storing the
//! wire shape directly keeps nested configuration (network config, logging
//! config, tags) round-tripping verbatim and guarantees `GetEnvironment` echoes
//! exactly what `CreateEnvironment` / `UpdateEnvironment` persisted. The async
//! environment lifecycle mutates the stored object's `Status` field on the next
//! read.
//!
//! All map keys are plain `String`s (environment name, resource ARN) so the
//! snapshot never depends on the tuple-key serde adapter that has silently
//! broken snapshot serialization on other services.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const MWAA_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Per-account Amazon MWAA state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MwaaData {
    /// Environments keyed by `Name`, stored as their `GetEnvironment`-shaped
    /// `Environment` wire object (PascalCase members). Tags live inside the
    /// object's `Tags` member so they round-trip on `GetEnvironment`.
    #[serde(default)]
    pub environments: BTreeMap<String, Value>,
}

impl MwaaData {
    /// Settle any in-flight environment transition so an interrupted lifecycle
    /// never wedges: an environment left `CREATING` or `UPDATING` becomes
    /// `AVAILABLE` (an `UPDATING` one also stamps its `LastUpdate` `SUCCESS`),
    /// and one left `DELETING` is removed. Used both on the next read and on
    /// restart. Returns `true` if anything changed.
    pub fn reconcile_environments(&mut self) -> bool {
        let mut changed = false;
        let names: Vec<String> = self.environments.keys().cloned().collect();
        for name in names {
            let st = self
                .environments
                .get(&name)
                .and_then(|e| e.get("Status"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            match st.as_str() {
                "CREATING" => {
                    if let Some(obj) = self
                        .environments
                        .get_mut(&name)
                        .and_then(Value::as_object_mut)
                    {
                        obj.insert("Status".into(), Value::String("AVAILABLE".into()));
                    }
                    changed = true;
                }
                "UPDATING" => {
                    if let Some(obj) = self
                        .environments
                        .get_mut(&name)
                        .and_then(Value::as_object_mut)
                    {
                        obj.insert("Status".into(), Value::String("AVAILABLE".into()));
                        if let Some(lu) = obj.get_mut("LastUpdate").and_then(Value::as_object_mut) {
                            lu.insert("Status".into(), Value::String("SUCCESS".into()));
                        }
                    }
                    changed = true;
                }
                "DELETING" => {
                    self.environments.remove(&name);
                    changed = true;
                }
                _ => {}
            }
        }
        changed
    }
}

impl AccountState for MwaaData {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedMwaaState = Arc<RwLock<MultiAccountState<MwaaData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct MwaaSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<MwaaData>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn data_with_env(status: &str) -> MwaaData {
        let mut d = MwaaData::default();
        d.environments.insert(
            "e1".to_string(),
            json!({ "Name": "e1", "Status": status, "LastUpdate": { "Status": "PENDING" } }),
        );
        d
    }

    #[test]
    fn reconcile_settles_creating_to_available() {
        let mut d = data_with_env("CREATING");
        assert!(d.reconcile_environments());
        assert_eq!(d.environments["e1"]["Status"], json!("AVAILABLE"));
    }

    #[test]
    fn reconcile_settles_updating_and_stamps_last_update() {
        let mut d = data_with_env("UPDATING");
        assert!(d.reconcile_environments());
        assert_eq!(d.environments["e1"]["Status"], json!("AVAILABLE"));
        assert_eq!(
            d.environments["e1"]["LastUpdate"]["Status"],
            json!("SUCCESS")
        );
    }

    #[test]
    fn reconcile_removes_deleting() {
        let mut d = data_with_env("DELETING");
        assert!(d.reconcile_environments());
        assert!(d.environments.is_empty());
    }

    #[test]
    fn reconcile_available_is_a_noop() {
        let mut d = data_with_env("AVAILABLE");
        assert!(!d.reconcile_environments());
    }
}
