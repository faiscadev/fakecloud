//! In-memory state for WAF v2.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type SharedWafv2State = Arc<RwLock<Wafv2Accounts>>;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Wafv2Accounts {
    pub accounts: BTreeMap<String, AccountState>,
}

impl Wafv2Accounts {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct AccountState {
    /// Keyed by (scope, name).
    pub web_acls: BTreeMap<ScopedKey, WebAcl>,
    /// Keyed by (scope, name).
    pub rule_groups: BTreeMap<ScopedKey, RuleGroup>,
    /// Keyed by (scope, name).
    pub ip_sets: BTreeMap<ScopedKey, IpSet>,
    /// Keyed by (scope, name).
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
    #[serde(default)]
    pub managed_rule_sets: BTreeMap<ScopedKey, ManagedRuleSet>,
}

pub type ScopedKey = (String, String);

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
