//! Account-partitioned, serializable state for the Resource Groups Tagging API.
//!
//! Two things live here:
//!  * `api_tags` — tags applied directly through `TagResources`/`UntagResources`
//!    to arbitrary ARNs. AWS lets the tagging API tag any resource, including
//!    ARNs no modelled service owns; those tags are stored here and merged onto
//!    aggregated results at read time.
//!  * `report` — the async tag-report state driven by
//!    `StartReportCreation`/`DescribeReportCreation`.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const RESOURCE_GROUPS_TAGGING_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// State of the most recent `StartReportCreation` request.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReportState {
    /// `RUNNING` | `SUCCEEDED` | `FAILED`, or `None` when no report was ever
    /// requested.
    pub status: Option<String>,
    pub s3_location: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceGroupsTaggingState {
    /// ARN -> (key -> value) for tags applied through the tagging API directly.
    pub api_tags: BTreeMap<String, BTreeMap<String, String>>,
    pub report: ReportState,
}

impl AccountState for ResourceGroupsTaggingState {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedResourceGroupsTaggingState =
    Arc<RwLock<MultiAccountState<ResourceGroupsTaggingState>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct ResourceGroupsTaggingSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<ResourceGroupsTaggingState>,
}
