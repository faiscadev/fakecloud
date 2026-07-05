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

/// The live data-plane binding for a broker whose backing engine container
/// (ActiveMQ/RabbitMQ) is running. Recorded when the container becomes
/// reachable and used to project the REAL, connectable host + mapped ports
/// into `DescribeBroker`'s `brokerInstances` (replacing the cosmetic
/// `*.amazonaws.com` synthesis). Persisted so a restart can tell which
/// brokers had a backing container and reconcile them (respawn if the
/// container is gone, #1338), and so the mapped ports survive a snapshot.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BrokerDataPlane {
    /// The backing container id.
    pub container_id: String,
    /// Host clients reach the published ports at: `127.0.0.1` on the host,
    /// or `host.docker.internal` / `host.containers.internal` when fakecloud
    /// is itself containerized (issue #1539).
    pub host: String,
    /// Wire-protocol label (`openwire`/`amqp`/`stomp`/`mqtt`/`ws`/`console`)
    /// -> the host port the container's corresponding port is published on.
    #[serde(default)]
    pub ports: BTreeMap<String, u16>,
}

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
    /// Live data-plane binding keyed by `BrokerId`, present only while a
    /// backing engine container is running. Kept OUT of the broker wire
    /// object so `DescribeBroker` never leaks a non-AWS field; the describe
    /// path reads it to render REAL endpoints and the restart path reads it
    /// to reconcile the backing container.
    #[serde(default)]
    pub data_plane: BTreeMap<String, BrokerDataPlane>,
}

impl MqData {
    /// Settle any in-flight broker transition so an interrupted lifecycle never
    /// wedges: a broker left `CREATION_IN_PROGRESS` or `REBOOT_IN_PROGRESS`
    /// becomes `RUNNING` (applying any staged pending change), and one left
    /// `DELETION_IN_PROGRESS` is removed. Used both on the next describe and on
    /// restart. Returns `true` if anything changed.
    ///
    /// `auto_settle` controls who owns the transitions. It is `true` in the
    /// in-memory / degraded path (no container runtime): the lifecycle is a
    /// pure state-machine that settles here on the next describe, exactly as
    /// the control-plane-only broker always did. It is `false` when a real
    /// engine-container runtime is attached: the runtime's background tasks
    /// own CREATION/REBOOT/DELETION (a broker is only `RUNNING` once its
    /// container actually accepts connections), so this becomes a no-op and
    /// never races those tasks by prematurely flipping state.
    pub fn reconcile_brokers(&mut self, auto_settle: bool) -> bool {
        if !auto_settle {
            return false;
        }
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
                    self.data_plane.remove(&id);
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn data_with_broker(state: &str) -> MqData {
        let mut d = MqData::default();
        d.brokers.insert(
            "b-1".to_string(),
            json!({ "brokerId": "b-1", "brokerState": state }),
        );
        d
    }

    #[test]
    fn reconcile_auto_settle_false_is_a_noop() {
        // With a container runtime attached, the runtime's background tasks own
        // the transitions; reconcile must never flip state itself.
        let mut d = data_with_broker("CREATION_IN_PROGRESS");
        assert!(!d.reconcile_brokers(false));
        assert_eq!(
            d.brokers["b-1"]["brokerState"],
            json!("CREATION_IN_PROGRESS"),
            "runtime-managed brokers are not settled by reconcile",
        );
    }

    #[test]
    fn reconcile_auto_settle_true_settles_creation() {
        let mut d = data_with_broker("CREATION_IN_PROGRESS");
        assert!(d.reconcile_brokers(true));
        assert_eq!(d.brokers["b-1"]["brokerState"], json!("RUNNING"));
    }

    #[test]
    fn reconcile_deletion_clears_data_plane_binding() {
        let mut d = data_with_broker("DELETION_IN_PROGRESS");
        d.data_plane.insert(
            "b-1".to_string(),
            BrokerDataPlane {
                container_id: "c1".to_string(),
                host: "127.0.0.1".to_string(),
                ports: BTreeMap::new(),
            },
        );
        assert!(d.reconcile_brokers(true));
        assert!(d.brokers.is_empty());
        assert!(
            d.data_plane.is_empty(),
            "the data-plane binding must be freed with the broker",
        );
    }
}
