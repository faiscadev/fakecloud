//! Account-partitioned, serializable state for AWS Elemental MediaConvert
//! (`mediaconvert`).
//!
//! Every resource is stored as its already-output-valid wire JSON object so
//! reads echo exactly what writes persisted. All map keys are plain `String`s
//! (queue/preset/job-template name, job id, resource ARN), so the snapshot never
//! depends on the tuple-key serde adapter that has silently broken snapshot
//! serialization on other services.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

use crate::shared;

pub const MEDIACONVERT_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Per-account AWS Elemental MediaConvert state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MediaConvertData {
    /// Queues keyed by `name`, stored as their `Queue` wire object.
    #[serde(default)]
    pub queues: BTreeMap<String, Value>,
    /// Presets keyed by `name`, stored as their `Preset` wire object.
    #[serde(default)]
    pub presets: BTreeMap<String, Value>,
    /// Job templates keyed by `name`, stored as their `JobTemplate` wire object.
    #[serde(default)]
    pub job_templates: BTreeMap<String, Value>,
    /// Jobs keyed by `id`, stored as their `Job` wire object.
    #[serde(default)]
    pub jobs: BTreeMap<String, Value>,
    /// The account resource policy (`Policy`), if one has been set.
    #[serde(default)]
    pub policy: Option<Value>,
    /// ACM certificate ARNs associated with this account.
    #[serde(default)]
    pub certificates: Vec<String>,
    /// Jobs-query records keyed by query id, stored as their result wire object.
    #[serde(default)]
    pub jobs_queries: BTreeMap<String, Value>,
    /// Tags keyed by resource ARN.
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
}

/// Build the seeded `Default` on-demand queue every MediaConvert account owns.
/// It is a `SYSTEM` queue in the `ACTIVE` state and cannot be deleted.
fn default_queue(region: &str, account: &str) -> Value {
    let now = shared::now_epoch();
    json!({
        "arn": shared::queue_arn(region, account, "Default"),
        "name": "Default",
        "type": "SYSTEM",
        "status": "ACTIVE",
        "pricingPlan": "ON_DEMAND",
        "description": "The queue MediaConvert uses when you don't specify a queue.",
        "createdAt": now,
        "lastUpdated": now,
        "progressingJobsCount": 0,
        "submittedJobsCount": 0,
    })
}

impl AccountState for MediaConvertData {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        let mut data = Self::default();
        data.queues
            .insert("Default".to_string(), default_queue(region, account_id));
        data
    }
}

pub type SharedMediaConvertState = Arc<RwLock<MultiAccountState<MediaConvertData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct MediaConvertSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<MediaConvertData>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_account_seeds_default_queue() {
        let data = MediaConvertData::new_for_account("000000000000", "us-east-1", "");
        assert!(data.queues.contains_key("Default"));
        assert_eq!(data.queues["Default"]["type"], "SYSTEM");
        assert!(data.presets.is_empty());
        assert!(data.tags.is_empty());
    }
}
