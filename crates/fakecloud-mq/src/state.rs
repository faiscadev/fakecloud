//! Account-partitioned, serializable state for Amazon MQ's (`mq`) control plane.
//!
//! Each broker and configuration is stored as its already-output-valid wire
//! JSON object (`serde_json::Value`, camelCase members matching the restJson1
//! `jsonName`s) keyed by its identifier. Storing the wire shape directly keeps
//! nested configuration round-tripping verbatim and guarantees the `Describe`
//! responses echo exactly what was persisted. The async broker lifecycle
//! mutates the stored object's `brokerState` field on the next describe.
//!
//! All map keys are plain `String`s (broker id, configuration id, resource ARN,
//! `brokerId/username`) so the snapshot never depends on the tuple-key serde
//! adapter that has silently broken snapshot serialization on other services.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const MQ_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Per-account Amazon MQ state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MqData {
    /// Brokers keyed by `BrokerId` (`b-<uuid>`), stored as their
    /// `DescribeBroker`-shaped wire object (without the derived
    /// `brokerInstances` / `users` lists, which are computed at describe time).
    #[serde(default)]
    pub brokers: BTreeMap<String, Value>,
    /// Per-broker users keyed by `BrokerId` -> `Username` -> user wire object
    /// (`{password, consoleAccess, groups, replicationUser, pendingChange}`).
    #[serde(default)]
    pub users: BTreeMap<String, BTreeMap<String, Value>>,
    /// `creatorRequestId` -> `BrokerId` for CreateBroker idempotency.
    #[serde(default)]
    pub broker_creator_ids: BTreeMap<String, String>,
    /// Configurations keyed by `ConfigurationId` (`c-<uuid>`), stored as their
    /// `Configuration` wire object (with `latestRevision`).
    #[serde(default)]
    pub configurations: BTreeMap<String, Value>,
    /// Configuration revisions keyed by `ConfigurationId` -> ordered list of
    /// `{revision, created, description, data}` (data is base64).
    #[serde(default)]
    pub configuration_revisions: BTreeMap<String, Vec<Value>>,
    /// Resource tags keyed by resource ARN (broker or configuration) -> tag map.
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
}

impl MqData {
    /// Settle any in-flight broker transition so an interrupted lifecycle never
    /// wedges: a broker left `CREATION_IN_PROGRESS` or `REBOOT_IN_PROGRESS`
    /// becomes `RUNNING` (applying any staged pending change), and one left
    /// `DELETION_IN_PROGRESS` is removed. Used both on the next describe and on
    /// restart. Returns `true` if anything changed.
    pub fn reconcile_brokers(&mut self) -> bool {
        let mut changed = false;
        let ids: Vec<String> = self.brokers.keys().cloned().collect();
        for id in ids {
            let st = self
                .brokers
                .get(&id)
                .and_then(|b| b.get("brokerState"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            match st.as_str() {
                "CREATION_IN_PROGRESS" => {
                    if let Some(obj) = self.brokers.get_mut(&id).and_then(Value::as_object_mut) {
                        obj.insert("brokerState".into(), Value::String("RUNNING".into()));
                    }
                    changed = true;
                }
                "REBOOT_IN_PROGRESS" => {
                    self.apply_pending(&id);
                    changed = true;
                }
                "DELETION_IN_PROGRESS" => {
                    self.brokers.remove(&id);
                    self.users.remove(&id);
                    // Drop the idempotency mapping so reusing the same
                    // creatorRequestId after a delete creates a fresh broker
                    // rather than returning the deleted id with an empty ARN.
                    self.broker_creator_ids.retain(|_, v| v != &id);
                    changed = true;
                }
                _ => {}
            }
        }
        changed
    }

    /// Apply a broker's staged pending changes (as a reboot does): move
    /// `pending*` broker fields to their live fields, apply pending
    /// configuration (pending -> current, push old current to history), apply
    /// each user's pending change (CREATE/UPDATE settle, DELETE removes), and
    /// return the broker to `RUNNING`.
    pub fn apply_pending(&mut self, broker_id: &str) {
        if let Some(obj) = self
            .brokers
            .get_mut(broker_id)
            .and_then(Value::as_object_mut)
        {
            for (pending, live) in [
                ("pendingEngineVersion", "engineVersion"),
                ("pendingHostInstanceType", "hostInstanceType"),
                ("pendingAuthenticationStrategy", "authenticationStrategy"),
            ] {
                if let Some(v) = obj.remove(pending) {
                    obj.insert(live.into(), v);
                }
            }
            if let Some(v) = obj.remove("pendingSecurityGroups") {
                obj.insert("securityGroups".into(), v);
            }
            // Pending logs settle into the live logs summary.
            if let Some(pending) = obj.get("logs").and_then(|l| l.get("pending")).cloned() {
                if let Some(logs) = obj.get_mut("logs").and_then(Value::as_object_mut) {
                    if let Some(a) = pending.get("audit") {
                        logs.insert("audit".into(), a.clone());
                    }
                    if let Some(g) = pending.get("general") {
                        logs.insert("general".into(), g.clone());
                    }
                    logs.remove("pending");
                }
            }
            // Pending configuration settles: pending -> current, old current -> history.
            if let Some(configs) = obj.get_mut("configurations").and_then(Value::as_object_mut) {
                if let Some(pending) = configs.remove("pending") {
                    if let Some(old_current) = configs.get("current").cloned() {
                        let hist = configs
                            .entry("history")
                            .or_insert_with(|| Value::Array(vec![]));
                        if let Some(arr) = hist.as_array_mut() {
                            arr.push(old_current);
                        }
                    }
                    configs.insert("current".into(), pending);
                }
            }
            obj.insert("brokerState".into(), Value::String("RUNNING".into()));
        }
        // Apply each user's pending change.
        if let Some(users) = self.users.get_mut(broker_id) {
            let names: Vec<String> = users.keys().cloned().collect();
            for name in names {
                let change = users
                    .get(&name)
                    .and_then(|u| u.get("pendingChange"))
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                if change == "DELETE" {
                    users.remove(&name);
                } else if let Some(obj) = users.get_mut(&name).and_then(Value::as_object_mut) {
                    obj.remove("pendingChange");
                }
            }
        }
    }
}

impl AccountState for MqData {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedMqState = Arc<RwLock<MultiAccountState<MqData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct MqSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<MqData>,
}
