//! Account-partitioned, serializable state for AWS X-Ray (`xray`).
//!
//! Control-plane resources (groups, sampling rules, the encryption config,
//! resource policies, the indexing rule, the trace-segment destination) are
//! stored as their already-output-valid wire JSON objects so reads echo exactly
//! what writes persisted. The data plane stores ingested [`StoredSegment`]s
//! keyed by trace id. Tags are keyed by resource ARN in a single map covering
//! every taggable resource (groups + sampling rules).
//!
//! Every map key is a plain `String` (name, ARN, trace id), so the snapshot
//! never depends on the tuple-key serde adapter that has silently broken
//! snapshot serialization on other services.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

use crate::segment::StoredSegment;

pub const XRAY_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// The name of the built-in sampling rule X-Ray always provides.
pub const DEFAULT_SAMPLING_RULE: &str = "Default";

/// Per-account AWS X-Ray state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XrayData {
    /// Groups keyed by `GroupName`, stored as their `Group` wire object.
    #[serde(default)]
    pub groups: BTreeMap<String, Value>,
    /// Sampling rules keyed by `RuleName`, stored as their `SamplingRuleRecord`
    /// wire object.
    #[serde(default)]
    pub sampling_rules: BTreeMap<String, Value>,
    /// The account `EncryptionConfig` wire object, once `PutEncryptionConfig`
    /// has set it; `None` means the default (`Type: NONE`).
    #[serde(default)]
    pub encryption_config: Option<Value>,
    /// Resource policies keyed by `PolicyName`, stored as their `ResourcePolicy`
    /// wire object.
    #[serde(default)]
    pub resource_policies: BTreeMap<String, Value>,
    /// The trace-segment destination (`XRay` or `CloudWatchLogs`).
    #[serde(default = "default_destination")]
    pub trace_segment_destination: String,
    /// Ingested trace segments keyed by trace id.
    #[serde(default)]
    pub traces: BTreeMap<String, Vec<StoredSegment>>,
    /// Active trace retrievals (Transaction Search) keyed by retrieval token,
    /// each `{ "TraceIds": [...], "StartTime": .., "EndTime": .. }`.
    #[serde(default)]
    pub retrievals: BTreeMap<String, Value>,
    /// Tags keyed by resource ARN (groups + sampling rules).
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
}

fn default_destination() -> String {
    "XRay".to_string()
}

impl Default for XrayData {
    fn default() -> Self {
        Self {
            groups: BTreeMap::new(),
            sampling_rules: BTreeMap::new(),
            encryption_config: None,
            resource_policies: BTreeMap::new(),
            trace_segment_destination: default_destination(),
            traces: BTreeMap::new(),
            retrievals: BTreeMap::new(),
            tags: BTreeMap::new(),
        }
    }
}

impl XrayData {
    /// Seed the built-in, undeletable `Default` sampling rule that every X-Ray
    /// account carries (matched last, 1 req/s reservoir + 5% of the rest).
    fn seed_default_rule(&mut self, region: &str, account: &str) {
        if self.sampling_rules.contains_key(DEFAULT_SAMPLING_RULE) {
            return;
        }
        let arn = format!("arn:aws:xray:{region}:{account}:sampling-rule/{DEFAULT_SAMPLING_RULE}");
        let now = now_epoch();
        let record = json!({
            "SamplingRule": {
                "RuleName": DEFAULT_SAMPLING_RULE,
                "RuleARN": arn,
                "ResourceARN": "*",
                "Priority": 10000,
                "FixedRate": 0.05,
                "ReservoirSize": 1,
                "ServiceName": "*",
                "ServiceType": "*",
                "Host": "*",
                "HTTPMethod": "*",
                "URLPath": "*",
                "Version": 1,
                "Attributes": {},
            },
            "CreatedAt": now,
            "ModifiedAt": now,
        });
        self.sampling_rules
            .insert(DEFAULT_SAMPLING_RULE.to_string(), record);
    }
}

/// Current time as restJson1 epoch-seconds (a floating-point number). X-Ray's
/// timestamp members carry no `@timestampFormat`, so restJson1's default
/// epoch-seconds applies.
pub fn now_epoch() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

impl AccountState for XrayData {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        let mut data = Self::default();
        data.seed_default_rule(region, account_id);
        data
    }
}

pub type SharedXrayState = Arc<RwLock<MultiAccountState<XrayData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct XraySnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<XrayData>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_account_has_default_sampling_rule() {
        let data = XrayData::new_for_account("000000000000", "us-east-1", "");
        assert!(data.sampling_rules.contains_key(DEFAULT_SAMPLING_RULE));
        let rule = &data.sampling_rules[DEFAULT_SAMPLING_RULE]["SamplingRule"];
        assert_eq!(rule["ResourceARN"], json!("*"));
        assert_eq!(rule["Priority"], json!(10000));
    }

    #[test]
    fn default_destination_is_xray() {
        let data = XrayData::default();
        assert_eq!(data.trace_segment_destination, "XRay");
    }
}
