//! Account-partitioned, serializable state for AWS Cost Explorer (`ce`).
//!
//! Every stored Cost Explorer resource is kept as an already-output-valid JSON
//! object (`serde_json::Value`) keyed by its ARN, name, or id. Storing the wire
//! shape directly keeps nested configuration (cost-category rules, monitor
//! specifications, subscription expressions) round-tripping verbatim and
//! guarantees the `Get`/`Describe` responses echo exactly what was persisted.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const CE_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Per-account Cost Explorer state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CeData {
    /// Anomaly monitors keyed by `MonitorArn`, holding the `AnomalyMonitor`
    /// object.
    #[serde(default)]
    pub anomaly_monitors: BTreeMap<String, Value>,
    /// Anomaly subscriptions keyed by `SubscriptionArn`, holding the
    /// `AnomalySubscription` object.
    #[serde(default)]
    pub anomaly_subscriptions: BTreeMap<String, Value>,
    /// Detected anomalies keyed by `AnomalyId` (feedback target).
    #[serde(default)]
    pub anomalies: BTreeMap<String, Value>,
    /// Cost category definitions keyed by `CostCategoryArn`, holding the
    /// `CostCategory` object.
    #[serde(default)]
    pub cost_categories: BTreeMap<String, Value>,
    /// Cost-category resource associations keyed by `CostCategoryArn`.
    #[serde(default)]
    pub cost_category_associations: BTreeMap<String, Vec<Value>>,
    /// Cost allocation tags keyed by `TagKey`, holding the `CostAllocationTag`
    /// object (status/type/timestamps).
    #[serde(default)]
    pub cost_allocation_tags: BTreeMap<String, Value>,
    /// Cost-allocation-tag backfill requests, newest last.
    #[serde(default)]
    pub backfill_requests: Vec<Value>,
    /// Commitment-purchase analyses keyed by `AnalysisId`.
    #[serde(default)]
    pub commitment_analyses: BTreeMap<String, Value>,
    /// Savings-plans purchase recommendation generations keyed by
    /// `RecommendationId`.
    #[serde(default)]
    pub sp_generations: BTreeMap<String, Value>,
    /// Resource tags keyed by resource ARN -> (key -> value).
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
}

impl AccountState for CeData {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedCeState = Arc<RwLock<MultiAccountState<CeData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct CeSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<CeData>,
}
