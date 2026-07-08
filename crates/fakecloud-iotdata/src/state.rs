//! Account-partitioned, serializable state for the AWS IoT Data Plane
//! (`iotdata`).
//!
//! Two stores per account:
//!
//! * `shadows` — device shadows, keyed by `thingName`. Each thing owns a map
//!   of `shadowName` -> shadow document, where the empty string `""` is the
//!   classic (unnamed) shadow and any other key is a named shadow. Every key
//!   is a plain `String`, so the snapshot never depends on the tuple-key serde
//!   adapter.
//! * `retained` — retained MQTT messages, keyed by `topic`. The stored value
//!   carries the base64 payload plus `qos` / `lastModifiedTime`.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const IOTDATA_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// The classic (unnamed) shadow is stored under this reserved shadow-name key.
pub const CLASSIC_SHADOW_KEY: &str = "";

/// Per-account IoT Data Plane state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IotDataData {
    /// Device shadows keyed by `thingName`, then by `shadowName` (with the
    /// empty string denoting the classic shadow). The stored value is the full
    /// internal shadow document (`state` / `metadata` / `version` /
    /// `timestamp`).
    #[serde(default)]
    pub shadows: BTreeMap<String, BTreeMap<String, Value>>,
    /// Retained MQTT messages keyed by `topic`. The stored value carries the
    /// base64 `payload`, `qos`, and `lastModifiedTime` (epoch millis).
    #[serde(default)]
    pub retained: BTreeMap<String, Value>,
}

impl AccountState for IotDataData {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedIotDataState = Arc<RwLock<MultiAccountState<IotDataData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct IotDataSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<IotDataData>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_account_is_empty() {
        let data = IotDataData::new_for_account("000000000000", "us-east-1", "");
        assert!(data.shadows.is_empty());
        assert!(data.retained.is_empty());
    }
}
