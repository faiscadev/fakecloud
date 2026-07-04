//! Account-partitioned, serializable state for AWS CloudTrail's
//! (`cloudtrail`) control plane.
//!
//! Every CloudTrail resource is stored as an already-output-valid JSON object
//! (`serde_json::Value`) keyed by its name or ARN. Storing the wire shape
//! directly keeps nested configuration objects (event selectors, destinations,
//! import sources) round-tripping verbatim and guarantees the `Get`/`Describe`
//! responses echo exactly what was persisted.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const CLOUDTRAIL_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Per-account CloudTrail state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CloudTrailData {
    /// Trails keyed by trail `Name`, holding the trail's describe object.
    #[serde(default)]
    pub trails: BTreeMap<String, Value>,
    /// Per-trail logging flag keyed by trail `Name`. `CreateTrail` leaves this
    /// `false` until `StartLogging` (AWS semantics).
    #[serde(default)]
    pub trail_logging: BTreeMap<String, bool>,
    /// Per-trail status timestamps keyed by trail `Name` (start/stop logging
    /// times populated by `StartLogging`/`StopLogging`).
    #[serde(default)]
    pub trail_status: BTreeMap<String, Value>,
    /// Per-trail event selectors keyed by trail `Name`
    /// (`{EventSelectors, AdvancedEventSelectors}`).
    #[serde(default)]
    pub event_selectors: BTreeMap<String, Value>,
    /// Per-trail insight selectors keyed by trail `Name`.
    #[serde(default)]
    pub insight_selectors: BTreeMap<String, Value>,
    /// Event data stores keyed by `EventDataStoreArn`.
    #[serde(default)]
    pub event_data_stores: BTreeMap<String, Value>,
    /// Channels keyed by `ChannelArn`.
    #[serde(default)]
    pub channels: BTreeMap<String, Value>,
    /// Imports keyed by `ImportId`.
    #[serde(default)]
    pub imports: BTreeMap<String, Value>,
    /// CloudTrail Lake queries keyed by `QueryId`.
    #[serde(default)]
    pub queries: BTreeMap<String, Value>,
    /// Dashboards keyed by `DashboardArn`.
    #[serde(default)]
    pub dashboards: BTreeMap<String, Value>,
    /// Resource-based policies keyed by `ResourceArn`.
    #[serde(default)]
    pub resource_policies: BTreeMap<String, String>,
    /// Organization delegated admins keyed by account id.
    #[serde(default)]
    pub delegated_admins: BTreeMap<String, Value>,
    /// Event configuration keyed by trail/EDS ARN.
    #[serde(default)]
    pub event_configurations: BTreeMap<String, Value>,
    /// Resource tags keyed by resource id/ARN -> (key -> value).
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
}

impl AccountState for CloudTrailData {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedCloudTrailState = Arc<RwLock<MultiAccountState<CloudTrailData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct CloudTrailSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<CloudTrailData>,
}
