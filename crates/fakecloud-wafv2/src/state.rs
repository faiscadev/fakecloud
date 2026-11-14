//! In-memory state for WAF v2.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type SharedWafv2State = Arc<RwLock<Wafv2Accounts>>;

/// WAFv2 state, partitioned by account and then by region.
///
/// REGIONAL-scope resources (web ACLs, rule groups, IP sets, regex pattern
/// sets, API keys, managed rule sets, plus the logging configurations,
/// permission policies and associations attached to them) live in the region
/// they were created in. CLOUDFRONT-scope resources are global: AWS only
/// accepts CLOUDFRONT-scope calls in the partition's global region
/// (`us-east-1` for `aws`) and mints their ARNs there, so they live in that
/// region's state.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Wafv2Accounts {
    /// account id -> region -> that account's state in that region.
    pub accounts: BTreeMap<String, BTreeMap<String, AccountState>>,
}

impl Wafv2Accounts {
    pub fn new() -> Self {
        Self::default()
    }

    /// The state of `account_id` in `region`, `None` when nothing has
    /// touched it. Never creates anything.
    pub fn region(&self, account_id: &str, region: &str) -> Option<&AccountState> {
        self.accounts.get(account_id).and_then(|r| r.get(region))
    }

    /// The state of `account_id` in `region`, created empty on first use.
    pub fn region_mut(&mut self, account_id: &str, region: &str) -> &mut AccountState {
        self.accounts
            .entry(account_id.to_string())
            .or_default()
            .entry(region.to_string())
            .or_default()
    }

    /// Every (account, region, state) triple.
    pub fn iter_regional(&self) -> impl Iterator<Item = (&str, &str, &AccountState)> {
        self.accounts.iter().flat_map(|(account, regions)| {
            regions
                .iter()
                .map(move |(region, s)| (account.as_str(), region.as_str(), s))
        })
    }

    /// The states that can hold a WAF association for `resource_arn`: the
    /// ARN's region (a region-less ARN, such as a CloudFront distribution's,
    /// resolves to its partition's global region, where CLOUDFRONT-scope web
    /// ACLs live) and the ARN's account, or every account when the ARN names
    /// none (API Gateway stage ARNs carry no account).
    pub fn states_for_resource<'a>(
        &'a self,
        resource_arn: &'a str,
    ) -> impl Iterator<Item = &'a AccountState> + 'a {
        let region = fakecloud_aws::arn::region_of(resource_arn).unwrap_or_else(|| {
            fakecloud_aws::arn::implicit_global_region(fakecloud_aws::arn::partition_of(
                resource_arn,
            ))
        });
        let account = fakecloud_aws::arn::account_of(resource_arn);
        self.accounts
            .iter()
            .filter(move |(id, _)| account.is_none_or(|a| a == id.as_str()))
            .filter_map(move |(_, regions)| regions.get(region))
    }
}

/// On-disk snapshot envelope for WAFv2 state. Versioned so format changes fail
/// loudly on upgrade rather than silently mis-parsing.
#[derive(Clone, Serialize, Deserialize)]
pub struct Wafv2Snapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<Wafv2Accounts>,
}

/// Bumped to 2 when the state was split by region: v1 kept one account-wide
/// state, migrated on load by [`parse_wafv2_snapshot`].
pub const WAFV2_SNAPSHOT_SCHEMA_VERSION: u32 = 2;

#[derive(Deserialize)]
struct SnapshotVersion {
    schema_version: u32,
}

/// The v1 snapshot shape: one account-wide state per account.
#[derive(Deserialize)]
struct LegacyWafv2Snapshot {
    #[serde(default)]
    accounts: Option<LegacyWafv2Accounts>,
}

#[derive(Deserialize)]
struct LegacyWafv2Accounts {
    #[serde(default)]
    accounts: BTreeMap<String, AccountState>,
}

/// Parse an on-disk WAFv2 snapshot, migrating the pre-region (v1) shape.
///
/// A snapshot newer than this binary understands is returned with its
/// version and no state so the caller can refuse it. Legacy state is split
/// by region: every resource goes to the region its ARN names (CLOUDFRONT
/// scope ARNs name the global region), logging configurations and
/// permission policies follow the ARN they are keyed by, an association
/// follows its web ACL, a tag set follows its resource ARN, and records that
/// name no region (API keys, managed rule sets) go to `default_region`, or
/// the partition's global region for CLOUDFRONT scope.
pub fn parse_wafv2_snapshot(
    bytes: &[u8],
    default_region: &str,
) -> Result<Wafv2Snapshot, serde_json::Error> {
    let SnapshotVersion { schema_version } = serde_json::from_slice(bytes)?;
    if schema_version >= WAFV2_SNAPSHOT_SCHEMA_VERSION {
        if schema_version > WAFV2_SNAPSHOT_SCHEMA_VERSION {
            return Ok(Wafv2Snapshot {
                schema_version,
                accounts: None,
            });
        }
        return serde_json::from_slice(bytes);
    }
    let legacy: LegacyWafv2Snapshot = serde_json::from_slice(bytes)?;
    let accounts = legacy.accounts.map(|legacy| {
        let mut out = Wafv2Accounts::new();
        for (account_id, state) in legacy.accounts {
            split_legacy_account(&mut out, &account_id, state, default_region);
        }
        out
    });
    Ok(Wafv2Snapshot {
        schema_version: WAFV2_SNAPSHOT_SCHEMA_VERSION,
        accounts,
    })
}

/// The region a WAFv2 resource of `scope` lives in when its record names no
/// ARN: CLOUDFRONT scope lives in the partition's global region.
pub(crate) fn scope_region<'a>(scope: &str, region: &'a str) -> &'a str {
    if scope == "CLOUDFRONT" {
        fakecloud_aws::arn::implicit_global_region(fakecloud_aws::arn::partition_for(region))
    } else {
        region
    }
}

fn split_legacy_account(
    out: &mut Wafv2Accounts,
    account_id: &str,
    legacy: AccountState,
    default_region: &str,
) {
    let region_of = |arn: &str| -> String {
        fakecloud_aws::arn::region_of(arn)
            .unwrap_or(default_region)
            .to_string()
    };
    let AccountState {
        web_acls,
        rule_groups,
        ip_sets,
        regex_pattern_sets,
        api_keys,
        logging_configs,
        permission_policies,
        associations,
        tags,
        managed_rule_sets,
    } = legacy;
    // Ensure the account keeps a (possibly empty) state in the default
    // region, matching what a fresh request would create.
    out.region_mut(account_id, default_region);
    for (key, acl) in web_acls {
        out.region_mut(account_id, &region_of(&acl.arn))
            .web_acls
            .insert(key, acl);
    }
    for (key, group) in rule_groups {
        out.region_mut(account_id, &region_of(&group.arn))
            .rule_groups
            .insert(key, group);
    }
    for (key, set) in ip_sets {
        out.region_mut(account_id, &region_of(&set.arn))
            .ip_sets
            .insert(key, set);
    }
    for (key, set) in regex_pattern_sets {
        out.region_mut(account_id, &region_of(&set.arn))
            .regex_pattern_sets
            .insert(key, set);
    }
    for (token, key) in api_keys {
        let region = scope_region(&key.scope, default_region).to_string();
        out.region_mut(account_id, &region)
            .api_keys
            .insert(token, key);
    }
    for (key, set) in managed_rule_sets {
        let region = scope_region(&set.scope, default_region).to_string();
        out.region_mut(account_id, &region)
            .managed_rule_sets
            .insert(key, set);
    }
    for (arn, config) in logging_configs {
        out.region_mut(account_id, &region_of(&arn))
            .logging_configs
            .insert(arn, config);
    }
    for (arn, policy) in permission_policies {
        out.region_mut(account_id, &region_of(&arn))
            .permission_policies
            .insert(arn, policy);
    }
    for (resource, acl_arn) in associations {
        // The association lives with its web ACL (which must share the
        // resource's region).
        out.region_mut(account_id, &region_of(&acl_arn))
            .associations
            .insert(resource, acl_arn);
    }
    for (arn, set) in tags {
        out.region_mut(account_id, &region_of(&arn))
            .tags
            .insert(arn, set);
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AccountState {
    /// Keyed by (scope, name).
    #[serde(with = "scoped_map_serde")]
    pub web_acls: BTreeMap<ScopedKey, WebAcl>,
    /// Keyed by (scope, name).
    #[serde(with = "scoped_map_serde")]
    pub rule_groups: BTreeMap<ScopedKey, RuleGroup>,
    /// Keyed by (scope, name).
    #[serde(with = "scoped_map_serde")]
    pub ip_sets: BTreeMap<ScopedKey, IpSet>,
    /// Keyed by (scope, name).
    #[serde(with = "scoped_map_serde")]
    pub regex_pattern_sets: BTreeMap<ScopedKey, RegexPatternSet>,
    /// API key tokens keyed by token string.
    pub api_keys: BTreeMap<String, ApiKey>,
    /// LoggingConfiguration keyed by ResourceArn (WebACL ARN).
    pub logging_configs: BTreeMap<String, Value>,
    /// IAM-style permission policies keyed by RuleGroup ARN.
    pub permission_policies: BTreeMap<String, String>,
    /// WebACL ARN keyed by associated ResourceArn (ALB / APIGW / Cognito UP / etc).
    pub associations: BTreeMap<String, String>,
    /// Tags keyed by ARN.
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
    /// Vendor-published managed rule sets keyed by (scope, name), set via
    /// PutManagedRuleSetVersions and read by ListManagedRuleSets /
    /// ListAvailableManagedRuleGroupVersions.
    #[serde(default, with = "scoped_map_serde")]
    pub managed_rule_sets: BTreeMap<ScopedKey, ManagedRuleSet>,
}

pub type ScopedKey = (String, String);

/// (De)serialize a `(scope, name) -> V` map as a sequence of `(scope, name, V)`
/// triples. JSON object keys must be strings, so a tuple-keyed map cannot be
/// serialized directly — without this, whole-state snapshot writes fail.
mod scoped_map_serde {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::ScopedKey;

    pub fn serialize<S, V>(map: &BTreeMap<ScopedKey, V>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        V: Serialize,
    {
        let entries: Vec<(&String, &String, &V)> =
            map.iter().map(|((a, b), v)| (a, b, v)).collect();
        entries.serialize(serializer)
    }

    pub fn deserialize<'de, D, V>(deserializer: D) -> Result<BTreeMap<ScopedKey, V>, D::Error>
    where
        D: Deserializer<'de>,
        V: Deserialize<'de>,
    {
        let entries: Vec<(String, String, V)> = Vec::deserialize(deserializer)?;
        Ok(entries.into_iter().map(|(a, b, v)| ((a, b), v)).collect())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagedRuleSet {
    pub id: String,
    pub name: String,
    pub scope: String,
    pub description: Option<String>,
    pub lock_token: String,
    pub label_namespace: String,
    pub recommended_version: Option<String>,
    /// Published version names (e.g. "Version_1.0").
    pub published_versions: Vec<String>,
    /// Per-version detail (AssociatedRuleGroupArn / Capacity / lifetime /
    /// timestamps) keyed by version name, as published via
    /// PutManagedRuleSetVersions and read back by GetManagedRuleSet.
    #[serde(default)]
    pub published_version_details: BTreeMap<String, Value>,
    pub created_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebAcl {
    pub id: String,
    pub name: String,
    pub arn: String,
    pub scope: String,
    pub default_action: Value,
    pub description: Option<String>,
    pub rules: Vec<Value>,
    pub visibility_config: Value,
    pub capacity: i64,
    pub lock_token: String,
    pub label_namespace: String,
    pub custom_response_bodies: BTreeMap<String, Value>,
    pub captcha_config: Option<Value>,
    pub challenge_config: Option<Value>,
    pub token_domains: Vec<String>,
    pub association_config: Option<Value>,
    pub data_protection_config: Option<Value>,
    pub on_source_d_do_s_protection_config: Option<Value>,
    pub application_config: Option<Value>,
    /// `MonetizationConfig` (AI bot pay-per-crawl settlement settings).
    #[serde(default)]
    pub monetization_config: Option<Value>,
    pub retrofitted_by_firewall_manager: bool,
    pub pre_process_firewall_manager_rule_groups: Vec<Value>,
    pub post_process_firewall_manager_rule_groups: Vec<Value>,
    pub managed_by_firewall_manager: bool,
    pub created_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleGroup {
    pub id: String,
    pub name: String,
    pub arn: String,
    pub scope: String,
    pub capacity: i64,
    pub description: Option<String>,
    pub rules: Vec<Value>,
    pub visibility_config: Value,
    pub lock_token: String,
    pub label_namespace: String,
    pub custom_response_bodies: BTreeMap<String, Value>,
    pub available_labels: Vec<Value>,
    pub consumed_labels: Vec<Value>,
    pub created_time: DateTime<Utc>,
    /// `MonetizationConfig` (AI bot pay-per-crawl settlement settings).
    #[serde(default)]
    pub monetization_config: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpSet {
    pub id: String,
    pub name: String,
    pub arn: String,
    pub scope: String,
    pub description: Option<String>,
    pub ip_address_version: String,
    pub addresses: Vec<String>,
    pub lock_token: String,
    pub created_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegexPatternSet {
    pub id: String,
    pub name: String,
    pub arn: String,
    pub scope: String,
    pub description: Option<String>,
    pub regular_expressions: Vec<Value>,
    pub lock_token: String,
    pub created_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKey {
    pub api_key: String,
    pub scope: String,
    pub token_domains: Vec<String>,
    pub version: i32,
    pub creation_timestamp: DateTime<Utc>,
}
