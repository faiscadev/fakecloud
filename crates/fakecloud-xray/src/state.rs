//! Account- and region-partitioned, serializable state for AWS X-Ray
//! (`xray`). Every X-Ray resource (groups, sampling rules, the encryption
//! config, resource policies, traces, retrievals) is regional.
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

use fakecloud_core::multi_account::{AccountState, MultiAccountState, MultiRegionState};

use crate::segment::StoredSegment;

/// v2: state partitioned by (account, region). v1 kept one state per account
/// with every region's resources in it.
pub const XRAY_SNAPSHOT_SCHEMA_VERSION: u32 = 2;

/// The name of the built-in sampling rule X-Ray always provides.
pub const DEFAULT_SAMPLING_RULE: &str = "Default";

/// The name of the single built-in Transaction Search indexing rule.
pub const DEFAULT_INDEXING_RULE: &str = "Default";

/// One account's AWS X-Ray state in one region.
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
    /// Transaction Search indexing rules keyed by rule `Name`, stored as their
    /// `IndexingRule` wire object. X-Ray provides exactly one, `Default`,
    /// seeded by [`XrayData::seed_default_rule`]; `UpdateIndexingRule`
    /// rewrites it in place.
    #[serde(default)]
    pub indexing_rules: BTreeMap<String, Value>,
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
            indexing_rules: BTreeMap::new(),
        }
    }
}

impl XrayData {
    /// Seed what X-Ray provides in every region of every account: the
    /// built-in, undeletable `Default` sampling rule (matched last, 1 req/s
    /// reservoir + 5% of the rest), with that region's ARN, and the built-in
    /// `Default` Transaction Search indexing rule.
    pub(crate) fn seed_default_rule(&mut self, region: &str, account: &str) {
        self.indexing_rules
            .entry(DEFAULT_INDEXING_RULE.to_string())
            .or_insert_with(|| {
                json!({
                    "Name": DEFAULT_INDEXING_RULE,
                    "ModifiedAt": now_epoch(),
                    "Rule": { "Probabilistic": {
                        "DesiredSamplingPercentage": 1.0,
                        "ActualSamplingPercentage": 1.0,
                    } },
                })
            });
        if self.sampling_rules.contains_key(DEFAULT_SAMPLING_RULE) {
            return;
        }
        let want = fakecloud_aws::arn::Arn::regional(
            "xray",
            region,
            account,
            &format!("sampling-rule/{DEFAULT_SAMPLING_RULE}"),
        )
        .to_string();
        let now = now_epoch();
        let record = json!({
            "SamplingRule": {
                "RuleName": DEFAULT_SAMPLING_RULE,
                "RuleARN": want,
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

pub type SharedXrayState = Arc<RwLock<MultiRegionState<XrayData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct XraySnapshot {
    pub schema_version: u32,
    pub accounts: MultiRegionState<XrayData>,
}

/// The shape v1 snapshots stored: one state per account.
#[derive(Debug, Deserialize)]
pub(crate) struct LegacyXraySnapshot {
    pub(crate) accounts: MultiAccountState<XrayData>,
}

/// The region a stored X-Ray wire object's ARN member names.
fn region_of_member<'a>(value: &'a Value, path: &[&str]) -> Option<&'a str> {
    let mut v = value;
    for key in path {
        v = v.get(*key)?;
    }
    fakecloud_aws::arn::region_of(v.as_str()?)
}

impl fakecloud_core::multi_account::SplitByRegion for XrayData {
    /// Groups, sampling rules and resource-tag entries go to the region their
    /// ARN names. Traces, retrievals, resource policies, the encryption config
    /// and the trace-segment destination carry no region, so they stay in the
    /// server's default region, where an account-wide v1 state served them.
    /// Every region gets its own built-in `Default` rule.
    fn split_by_region(self, into: &mut fakecloud_core::multi_account::RegionalState<Self>) {
        let home = into.region_or_default_mut(None);
        home.encryption_config = self.encryption_config;
        home.resource_policies = self.resource_policies;
        home.trace_segment_destination = self.trace_segment_destination;
        home.indexing_rules = self.indexing_rules;
        home.traces = self.traces;
        home.retrievals = self.retrievals;
        for (name, group) in self.groups {
            let region = region_of_member(&group, &["GroupARN"]).map(str::to_string);
            into.region_or_default_mut(region.as_deref())
                .groups
                .insert(name, group);
        }
        // The built-in `Default` rule goes where its ARN points like any
        // other (keeping its edits there); other regions get a fresh seed.
        for (name, rule) in self.sampling_rules {
            let region = region_of_member(&rule, &["SamplingRule", "RuleARN"]).map(str::to_string);
            into.region_or_default_mut(region.as_deref())
                .sampling_rules
                .insert(name, rule);
        }
        for (arn, tags) in self.tags {
            let region = fakecloud_aws::arn::region_of(&arn).map(str::to_string);
            into.region_or_default_mut(region.as_deref())
                .tags
                .insert(arn, tags);
        }
    }
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
