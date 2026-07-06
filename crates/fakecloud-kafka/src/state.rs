//! Account-partitioned, serializable state for Amazon MSK's (`kafka`) control
//! plane.
//!
//! Each cluster, configuration, cluster-operation, replicator, and VPC
//! connection is stored as its already-output-valid wire JSON object
//! (`serde_json::Value`, camelCase `jsonName` members matching the restJson1
//! shapes) keyed by its ARN. Storing the wire shape directly keeps nested shapes
//! round-tripping verbatim and guarantees the `Describe` responses echo exactly
//! what was persisted. The async cluster lifecycle mutates the stored object's
//! `state` field on the next describe.
//!
//! All map keys are plain `String`s (resource ARN, `clusterArn/topicName`) so
//! the snapshot never depends on the tuple-key serde adapter that has silently
//! broken snapshot serialization on other services.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const KAFKA_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Per-account Amazon MSK state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct KafkaData {
    /// Clusters keyed by `clusterArn`, stored as their `ClusterInfo`-shaped wire
    /// object (provisioned) plus internal `_`-prefixed helper fields
    /// (`_clusterType`, `_serverless`) that never leak into a describe response.
    #[serde(default)]
    pub clusters: BTreeMap<String, Value>,
    /// Cluster operations keyed by `OperationArn`, stored as their
    /// `ClusterOperationInfo` wire object.
    #[serde(default)]
    pub operations: BTreeMap<String, Value>,
    /// Configurations keyed by configuration `Arn`, stored as their
    /// `Configuration` wire object (with `LatestRevision`).
    #[serde(default)]
    pub configurations: BTreeMap<String, Value>,
    /// Configuration revisions keyed by configuration `Arn` -> ordered list of
    /// `{Revision, CreationTime, Description, ServerProperties}` (b64).
    #[serde(default)]
    pub configuration_revisions: BTreeMap<String, Vec<Value>>,
    /// Topics keyed by `ClusterArn` -> `TopicName` -> topic wire object.
    #[serde(default)]
    pub topics: BTreeMap<String, BTreeMap<String, Value>>,
    /// Replicators keyed by `ReplicatorArn`.
    #[serde(default)]
    pub replicators: BTreeMap<String, Value>,
    /// VPC connections keyed by `VpcConnectionArn`.
    #[serde(default)]
    pub vpc_connections: BTreeMap<String, Value>,
    /// SCRAM secret ARNs keyed by `ClusterArn`.
    #[serde(default)]
    pub scram_secrets: BTreeMap<String, Vec<String>>,
    /// Cluster resource policies keyed by `ClusterArn` -> `{Policy, Version}`.
    #[serde(default)]
    pub policies: BTreeMap<String, Value>,
    /// Resource tags keyed by resource ARN -> tag map.
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
    /// Monotonic counter for the `-N` ARN suffix and cluster `CurrentVersion`s.
    #[serde(default)]
    pub seq: u64,
}

impl KafkaData {
    /// Next monotonic sequence number (for ARN `-N` suffixes and versions).
    pub fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    /// Settle any in-flight lifecycle transition so an interrupted lifecycle
    /// never wedges: a `CREATING`/`UPDATING`/`REBOOTING_BROKER` cluster becomes
    /// `ACTIVE`, a `DELETING` cluster is removed, a `CREATING` VPC connection
    /// becomes `AVAILABLE`, a `CREATING`/`UPDATING` replicator becomes
    /// `RUNNING`, a `CREATING`/`UPDATING` topic becomes `ACTIVE`, and a pending
    /// cluster operation settles to `UPDATE_COMPLETE`. Used both on the next
    /// read and on restart. Returns `true` if anything changed.
    ///
    /// Batch 1 has no backing Kafka runtime, so this always auto-settles (the
    /// pure state-machine the control-plane models). // TODO(batch2): gate on a
    /// runtime the way Amazon MQ does once a real broker backs the data plane.
    pub fn reconcile(&mut self) -> bool {
        let mut changed = false;

        // Clusters.
        let cluster_arns: Vec<String> = self.clusters.keys().cloned().collect();
        for arn in cluster_arns {
            let state = self
                .clusters
                .get(&arn)
                .and_then(|c| c.get("state"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            match state.as_str() {
                "CREATING" | "UPDATING" | "REBOOTING_BROKER" | "HEALING" | "MAINTENANCE" => {
                    if let Some(obj) = self.clusters.get_mut(&arn).and_then(Value::as_object_mut) {
                        obj.insert("state".into(), Value::String("ACTIVE".into()));
                        if let Some(si) = obj.get_mut("stateInfo").and_then(Value::as_object_mut) {
                            si.insert("code".into(), Value::String("NONE".into()));
                            si.insert("message".into(), Value::String(String::new()));
                        }
                        obj.remove("activeOperationArn");
                    }
                    changed = true;
                }
                "DELETING" => {
                    self.clusters.remove(&arn);
                    self.topics.remove(&arn);
                    self.scram_secrets.remove(&arn);
                    self.policies.remove(&arn);
                    changed = true;
                }
                _ => {}
            }
        }

        // Cluster operations settle to UPDATE_COMPLETE.
        let op_arns: Vec<String> = self.operations.keys().cloned().collect();
        for arn in op_arns {
            let st = self
                .operations
                .get(&arn)
                .and_then(|o| o.get("operationState"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            if st == "PENDING" || st == "UPDATE_IN_PROGRESS" {
                if let Some(obj) = self.operations.get_mut(&arn).and_then(Value::as_object_mut) {
                    obj.insert(
                        "operationState".into(),
                        Value::String("UPDATE_COMPLETE".into()),
                    );
                    obj.insert("endTime".into(), Value::String(crate::shared::now_iso()));
                }
                changed = true;
            }
        }

        // VPC connections settle CREATING -> AVAILABLE, and are removed when
        // left DELETING/REJECTING.
        let vpc_arns: Vec<String> = self.vpc_connections.keys().cloned().collect();
        for arn in vpc_arns {
            let st = self
                .vpc_connections
                .get(&arn)
                .and_then(|v| v.get("state"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            match st.as_str() {
                "CREATING" => {
                    if let Some(obj) = self
                        .vpc_connections
                        .get_mut(&arn)
                        .and_then(Value::as_object_mut)
                    {
                        obj.insert("state".into(), Value::String("AVAILABLE".into()));
                    }
                    changed = true;
                }
                "DELETING" | "REJECTING" => {
                    self.vpc_connections.remove(&arn);
                    changed = true;
                }
                _ => {}
            }
        }

        // Replicators settle CREATING/UPDATING -> RUNNING; DELETING removed.
        let rep_arns: Vec<String> = self.replicators.keys().cloned().collect();
        for arn in rep_arns {
            let st = self
                .replicators
                .get(&arn)
                .and_then(|r| r.get("replicatorState"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            match st.as_str() {
                "CREATING" | "UPDATING" => {
                    if let Some(obj) = self
                        .replicators
                        .get_mut(&arn)
                        .and_then(Value::as_object_mut)
                    {
                        obj.insert("replicatorState".into(), Value::String("RUNNING".into()));
                    }
                    changed = true;
                }
                "DELETING" => {
                    self.replicators.remove(&arn);
                    changed = true;
                }
                _ => {}
            }
        }

        // Topics settle CREATING/UPDATING -> ACTIVE; DELETING removed.
        let topic_clusters: Vec<String> = self.topics.keys().cloned().collect();
        for carn in topic_clusters {
            if let Some(map) = self.topics.get_mut(&carn) {
                let names: Vec<String> = map.keys().cloned().collect();
                for name in names {
                    let st = map
                        .get(&name)
                        .and_then(|t| t.get("status"))
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    match st.as_str() {
                        "CREATING" | "UPDATING" => {
                            if let Some(obj) = map.get_mut(&name).and_then(Value::as_object_mut) {
                                obj.insert("status".into(), Value::String("ACTIVE".into()));
                            }
                            changed = true;
                        }
                        "DELETING" => {
                            map.remove(&name);
                            changed = true;
                        }
                        _ => {}
                    }
                }
            }
        }

        changed
    }
}

impl AccountState for KafkaData {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedKafkaState = Arc<RwLock<MultiAccountState<KafkaData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct KafkaSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<KafkaData>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn data_with_cluster(state: &str) -> KafkaData {
        let mut d = KafkaData::default();
        d.clusters.insert(
            "arn:aws:kafka:us-east-1:000000000000:cluster/c/uuid-1".to_string(),
            json!({ "clusterArn": "arn:aws:kafka:us-east-1:000000000000:cluster/c/uuid-1", "state": state }),
        );
        d
    }

    #[test]
    fn reconcile_settles_creating_to_active() {
        let mut d = data_with_cluster("CREATING");
        assert!(d.reconcile());
        let c = d.clusters.values().next().unwrap();
        assert_eq!(c["state"], json!("ACTIVE"));
    }

    #[test]
    fn reconcile_removes_deleting_cluster() {
        let mut d = data_with_cluster("DELETING");
        assert!(d.reconcile());
        assert!(d.clusters.is_empty());
    }

    #[test]
    fn reconcile_stable_cluster_is_noop() {
        let mut d = data_with_cluster("ACTIVE");
        assert!(!d.reconcile());
    }

    #[test]
    fn seq_is_monotonic() {
        let mut d = KafkaData::default();
        assert_eq!(d.next_seq(), 1);
        assert_eq!(d.next_seq(), 2);
    }
}
