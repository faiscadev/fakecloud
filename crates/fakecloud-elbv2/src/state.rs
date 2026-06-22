use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

pub type SharedElbv2State = Arc<RwLock<Elbv2Accounts>>;

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct Elbv2Accounts {
    accounts: BTreeMap<String, Elbv2State>,
}

/// On-disk snapshot envelope for ELBv2 state. Versioned so format changes fail
/// loudly on upgrade rather than silently mis-parsing. Target health is not
/// persisted -- the prober re-derives it after a restart.
#[derive(Clone, Serialize, Deserialize)]
pub struct Elbv2Snapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<Elbv2Accounts>,
}

pub const ELBV2_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

impl Elbv2Accounts {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, account_id: &str) -> Option<&Elbv2State> {
        self.accounts.get(account_id)
    }

    pub fn get_mut(&mut self, account_id: &str) -> Option<&mut Elbv2State> {
        self.accounts.get_mut(account_id)
    }

    pub fn get_or_create(&mut self, account_id: &str) -> &mut Elbv2State {
        self.accounts
            .entry(account_id.to_string())
            .or_insert_with(|| Elbv2State::new(account_id))
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Elbv2State)> {
        self.accounts.iter()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Elbv2State {
    pub account_id: String,
    pub load_balancers: BTreeMap<String, LoadBalancer>,
    pub target_groups: BTreeMap<String, TargetGroup>,
    pub listeners: BTreeMap<String, Listener>,
    pub rules: BTreeMap<String, Rule>,
    pub trust_stores: BTreeMap<String, TrustStore>,
    pub resource_policies: BTreeMap<String, String>,
}

impl Elbv2State {
    pub fn new(account_id: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            load_balancers: BTreeMap::new(),
            target_groups: BTreeMap::new(),
            listeners: BTreeMap::new(),
            rules: BTreeMap::new(),
            trust_stores: BTreeMap::new(),
            resource_policies: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadBalancer {
    pub arn: String,
    pub name: String,
    pub dns_name: String,
    pub canonical_hosted_zone_id: String,
    pub created_time: DateTime<Utc>,
    pub scheme: String,
    pub vpc_id: String,
    pub state_code: String,
    pub state_reason: Option<String>,
    pub lb_type: String,
    pub availability_zones: Vec<AvailabilityZone>,
    pub security_groups: Vec<String>,
    pub ip_address_type: String,
    pub customer_owned_ipv4_pool: Option<String>,
    pub enforce_security_group_inbound_rules_on_private_link_traffic: Option<String>,
    pub enable_prefix_for_ipv6_source_nat: Option<String>,
    pub ipv4_ipam_pool_id: Option<String>,
    pub tags: Vec<Tag>,
    pub attributes: BTreeMap<String, String>,
    pub minimum_capacity_units: Option<i32>,
    /// In-process data plane TCP port assigned by the OS once the
    /// data plane supervisor binds a listener for this LB. `None`
    /// until the supervisor has reconciled this LB on the current
    /// run. Skipped on (de)serialize because the value is runtime-
    /// only — restarts get fresh ports.
    #[serde(skip)]
    pub bound_port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AvailabilityZone {
    pub zone_name: String,
    pub subnet_id: String,
    pub outpost_id: Option<String>,
    pub load_balancer_addresses: Vec<LoadBalancerAddress>,
    pub source_nat_ipv6_prefixes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadBalancerAddress {
    pub ip_address: Option<String>,
    pub allocation_id: Option<String>,
    pub private_ipv4_address: Option<String>,
    pub ipv6_address: Option<String>,
    pub ipv4_prefix: Option<String>,
    pub ipv6_prefix: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tag {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetGroup {
    pub arn: String,
    pub name: String,
    pub protocol: Option<String>,
    pub port: Option<i32>,
    pub vpc_id: Option<String>,
    pub target_type: String,
    pub ip_address_type: String,
    pub protocol_version: Option<String>,
    pub health_check_protocol: Option<String>,
    pub health_check_port: Option<String>,
    pub health_check_enabled: bool,
    pub health_check_path: Option<String>,
    pub health_check_interval_seconds: i32,
    pub health_check_timeout_seconds: i32,
    pub healthy_threshold_count: i32,
    pub unhealthy_threshold_count: i32,
    pub matcher_http_code: Option<String>,
    pub matcher_grpc_code: Option<String>,
    pub load_balancer_arns: Vec<String>,
    pub targets: Vec<TargetDescription>,
    pub tags: Vec<Tag>,
    pub attributes: BTreeMap<String, String>,
    pub created_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetDescription {
    pub id: String,
    pub port: Option<i32>,
    pub availability_zone: Option<String>,
    pub health: TargetHealth,
    #[serde(default)]
    pub consecutive_success: u32,
    #[serde(default)]
    pub consecutive_failure: u32,
    #[serde(default)]
    pub last_probe_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetHealth {
    pub state: String,
    pub reason: Option<String>,
    pub description: Option<String>,
}

impl Default for TargetHealth {
    fn default() -> Self {
        Self {
            state: "healthy".to_string(),
            reason: None,
            description: None,
        }
    }
}

impl TargetHealth {
    pub fn initial() -> Self {
        Self {
            state: "initial".to_string(),
            reason: Some("Elb.RegistrationInProgress".to_string()),
            description: Some("Target registration is in progress".to_string()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Listener {
    pub arn: String,
    pub load_balancer_arn: String,
    pub port: Option<i32>,
    pub protocol: Option<String>,
    pub certificates: Vec<Certificate>,
    pub ssl_policy: Option<String>,
    pub default_actions: Vec<Action>,
    pub alpn_policy: Vec<String>,
    pub mutual_authentication: Option<MutualAuthentication>,
    pub tags: Vec<Tag>,
    pub attributes: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Certificate {
    pub certificate_arn: String,
    pub is_default: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutualAuthentication {
    pub mode: Option<String>,
    pub trust_store_arn: Option<String>,
    pub ignore_client_certificate_expiry: Option<bool>,
    pub trust_store_association_status: Option<String>,
    pub advertise_trust_store_ca_names: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Action {
    pub action_type: String,
    pub target_group_arn: Option<String>,
    pub order: Option<i32>,
    pub redirect: Option<RedirectConfig>,
    pub fixed_response: Option<FixedResponseConfig>,
    pub forward: Option<ForwardConfig>,
    pub authenticate_cognito: Option<serde_json::Value>,
    pub authenticate_oidc: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedirectConfig {
    pub protocol: Option<String>,
    pub port: Option<String>,
    pub host: Option<String>,
    pub path: Option<String>,
    pub query: Option<String>,
    pub status_code: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixedResponseConfig {
    pub message_body: Option<String>,
    pub status_code: String,
    pub content_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardConfig {
    pub target_groups: Vec<TargetGroupTuple>,
    pub stickiness: Option<TargetGroupStickinessConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetGroupTuple {
    pub target_group_arn: String,
    pub weight: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetGroupStickinessConfig {
    pub enabled: Option<bool>,
    pub duration_seconds: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rule {
    pub arn: String,
    pub listener_arn: String,
    pub priority: String,
    pub conditions: Vec<RuleCondition>,
    pub actions: Vec<Action>,
    pub is_default: bool,
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleCondition {
    pub field: String,
    pub values: Vec<String>,
    pub host_header_values: Vec<String>,
    pub path_pattern_values: Vec<String>,
    pub http_header_name: Option<String>,
    pub http_header_values: Vec<String>,
    pub query_string_values: Vec<QueryStringKeyValuePair>,
    pub http_request_method_values: Vec<String>,
    pub source_ip_values: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryStringKeyValuePair {
    pub key: Option<String>,
    pub value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrustStore {
    pub arn: String,
    pub name: String,
    pub status: String,
    pub number_of_ca_certificates: i32,
    pub total_revoked_entries: i64,
    pub created_time: DateTime<Utc>,
    pub ca_certificates_bundle: Option<Vec<u8>>,
    pub revocations: BTreeMap<i64, TrustStoreRevocation>,
    pub next_revocation_id: i64,
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrustStoreRevocation {
    pub revocation_id: i64,
    pub revocation_type: String,
    pub number_of_revoked_entries: i64,
    pub content: Vec<u8>,
}
