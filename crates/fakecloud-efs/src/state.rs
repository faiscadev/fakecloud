//! Account-partitioned, serializable state for Amazon EFS's
//! (`elasticfilesystem`) control plane.
//!
//! Each file system, mount target, and access point is stored as its
//! already-output-valid `Description` JSON object (`serde_json::Value`) keyed by
//! its identifier. Storing the wire shape directly keeps nested configuration
//! (POSIX user, root directory, size breakdown) round-tripping verbatim and
//! guarantees the `Describe` responses echo exactly what was persisted. The
//! async `creating` -> `available` lifecycle mutates the stored object's
//! `LifeCycleState` field on the next describe.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const EFS_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Per-account EFS state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EfsData {
    /// File systems keyed by `FileSystemId` (`fs-...`), stored as their
    /// `FileSystemDescription` object.
    #[serde(default)]
    pub file_systems: BTreeMap<String, Value>,
    /// Mount targets keyed by `MountTargetId` (`fsmt-...`), stored as their
    /// `MountTargetDescription` object.
    #[serde(default)]
    pub mount_targets: BTreeMap<String, Value>,
    /// Security groups per mount target, keyed by `MountTargetId`.
    #[serde(default)]
    pub mount_target_security_groups: BTreeMap<String, Vec<String>>,
    /// Access points keyed by `AccessPointId` (`fsap-...`), stored as their
    /// `AccessPointDescription` object.
    #[serde(default)]
    pub access_points: BTreeMap<String, Value>,
    /// Lifecycle configurations keyed by `FileSystemId` -> `LifecyclePolicies`.
    #[serde(default)]
    pub lifecycle_configs: BTreeMap<String, Value>,
    /// Backup policy status keyed by `FileSystemId` (`ENABLED`/`DISABLED`/...).
    #[serde(default)]
    pub backup_policies: BTreeMap<String, String>,
    /// File-system resource policies keyed by `FileSystemId` -> policy JSON.
    #[serde(default)]
    pub file_system_policies: BTreeMap<String, String>,
    /// Replication configurations keyed by source `FileSystemId`, stored as the
    /// `ReplicationConfigurationDescription` object.
    #[serde(default)]
    pub replications: BTreeMap<String, Value>,
    /// Resource tags keyed by resource id (`fs-...` or `fsap-...`) -> tag map.
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
    /// Account-level resource-id-type preference (`LONG_ID` / `SHORT_ID`).
    #[serde(default)]
    pub resource_id_preference: Option<String>,
}

impl EfsData {
    /// Settle any in-flight lifecycle transition: a resource left in `creating`
    /// becomes `available`. Used both on the next describe and on restart so an
    /// interrupted transition never wedges. Returns `true` if anything changed.
    pub fn reconcile_lifecycle(&mut self) -> bool {
        let mut changed = false;
        for map in [
            &mut self.file_systems,
            &mut self.mount_targets,
            &mut self.access_points,
        ] {
            for v in map.values_mut() {
                if let Some(obj) = v.as_object_mut() {
                    if obj.get("LifeCycleState").and_then(Value::as_str) == Some("creating") {
                        obj.insert("LifeCycleState".into(), Value::String("available".into()));
                        changed = true;
                    }
                }
            }
        }
        changed
    }
}

impl AccountState for EfsData {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedEfsState = Arc<RwLock<MultiAccountState<EfsData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct EfsSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<EfsData>,
}
