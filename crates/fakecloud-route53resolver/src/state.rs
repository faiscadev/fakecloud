//! In-memory state for Amazon Route 53 Resolver (`route53resolver`).
//!
//! State is partitioned per account. Each account owns the full set of Resolver
//! resources: resolver endpoints (with their IP addresses), resolver rules and
//! their VPC associations, query-log configurations and their associations,
//! DNS Firewall rule groups / domain lists / rules / associations, plus the
//! per-VPC firewall / resolver / DNSSEC configuration singletons, Outpost
//! resolvers, resource-based policies and tags.
//!
//! Every map is keyed by `String` (resource id or ARN) so serde round-trips
//! cleanly with no tuple-key `KeyMustBeAString` trap. The typed resource structs
//! double as the awsJson wire shapes: they derive `Serialize` with PascalCase
//! field names and skip `None` optionals, matching what AWS returns.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

pub type SharedRoute53ResolverState = Arc<RwLock<Route53ResolverAccounts>>;

pub const R53R_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Route53ResolverAccounts {
    pub accounts: BTreeMap<String, AccountState>,
}

impl Route53ResolverAccounts {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn account_mut(&mut self, account: &str) -> &mut AccountState {
        self.accounts.entry(account.to_string()).or_default()
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AccountState {
    /// Resolver endpoints keyed by endpoint id (`rslvr-in-*` / `rslvr-out-*`).
    pub endpoints: BTreeMap<String, EndpointRecord>,
    /// Resolver rules keyed by rule id (`rslvr-rr-*`).
    pub rules: BTreeMap<String, ResolverRule>,
    /// Resolver rule -> VPC associations keyed by association id
    /// (`rslvr-rrassoc-*`).
    pub rule_associations: BTreeMap<String, ResolverRuleAssociation>,
    /// Query-log configurations keyed by config id (`rslvr-qlc-*`).
    pub query_log_configs: BTreeMap<String, ResolverQueryLogConfig>,
    /// Query-log config -> resource associations keyed by association id
    /// (`rslvr-qlcassoc-*`).
    pub query_log_associations: BTreeMap<String, ResolverQueryLogConfigAssociation>,
    /// DNS Firewall rule groups keyed by group id (`rslvr-frg-*`).
    pub firewall_rule_groups: BTreeMap<String, FirewallRuleGroup>,
    /// Firewall rules keyed by owning rule-group id.
    pub firewall_rules: BTreeMap<String, Vec<FirewallRule>>,
    /// Firewall domain lists keyed by list id (`rslvr-fdl-*`).
    pub firewall_domain_lists: BTreeMap<String, FirewallDomainList>,
    /// Domains contained in each firewall domain list, keyed by list id.
    pub firewall_domains: BTreeMap<String, Vec<String>>,
    /// Firewall rule-group -> VPC associations keyed by association id
    /// (`rslvr-frgassoc-*`).
    pub firewall_rule_group_associations: BTreeMap<String, FirewallRuleGroupAssociation>,
    /// Per-VPC firewall configuration, keyed by VPC (resource) id.
    pub firewall_configs: BTreeMap<String, FirewallConfig>,
    /// Per-VPC resolver configuration, keyed by VPC (resource) id.
    pub resolver_configs: BTreeMap<String, ResolverConfig>,
    /// Per-VPC DNSSEC configuration, keyed by VPC (resource) id.
    pub dnssec_configs: BTreeMap<String, ResolverDnssecConfig>,
    /// Outpost resolvers keyed by id (`rslvr-op-*`).
    pub outpost_resolvers: BTreeMap<String, OutpostResolver>,
    /// Resource-based policies keyed by resource ARN.
    pub firewall_rule_group_policies: BTreeMap<String, String>,
    pub query_log_config_policies: BTreeMap<String, String>,
    pub resolver_rule_policies: BTreeMap<String, String>,
    /// Tags keyed by resource ARN.
    pub tags: BTreeMap<String, Vec<Tag>>,
}

/// A resolver endpoint plus its associated IP addresses (which the endpoint's
/// own `IpAddressCount` summarizes but which `ListResolverEndpointIpAddresses`
/// returns in full).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointRecord {
    pub endpoint: ResolverEndpoint,
    pub ip_addresses: Vec<IpAddressResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ResolverEndpoint {
    pub id: String,
    pub creator_request_id: String,
    pub arn: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub security_group_ids: Vec<String>,
    pub direction: String,
    pub ip_address_count: i64,
    #[serde(rename = "HostVPCId")]
    pub host_vpc_id: String,
    pub status: String,
    pub status_message: String,
    pub creation_time: String,
    pub modification_time: String,
    pub resolver_endpoint_type: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub protocols: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct IpAddressResponse {
    pub ip_id: String,
    pub subnet_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ipv6: Option<String>,
    pub status: String,
    pub status_message: String,
    pub creation_time: String,
    pub modification_time: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ResolverRule {
    pub id: String,
    pub creator_request_id: String,
    pub arn: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain_name: Option<String>,
    pub status: String,
    pub status_message: String,
    pub rule_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub target_ips: Vec<TargetAddress>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolver_endpoint_id: Option<String>,
    pub owner_id: String,
    pub share_status: String,
    pub creation_time: String,
    pub modification_time: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct TargetAddress {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ipv6: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
    #[serde(
        rename = "ServerNameIndication",
        skip_serializing_if = "Option::is_none"
    )]
    pub server_name_indication: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ResolverRuleAssociation {
    pub id: String,
    pub resolver_rule_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(rename = "VPCId")]
    pub vpc_id: String,
    pub status: String,
    pub status_message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ResolverQueryLogConfig {
    pub id: String,
    pub owner_id: String,
    pub status: String,
    pub share_status: String,
    pub association_count: i64,
    pub arn: String,
    pub name: String,
    pub destination_arn: String,
    pub creator_request_id: String,
    pub creation_time: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ResolverQueryLogConfigAssociation {
    pub id: String,
    pub resolver_query_log_config_id: String,
    pub resource_id: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    pub creation_time: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct FirewallRuleGroup {
    pub id: String,
    pub arn: String,
    pub name: String,
    pub rule_count: i64,
    pub status: String,
    pub status_message: String,
    pub owner_id: String,
    pub creator_request_id: String,
    pub share_status: String,
    pub creation_time: String,
    pub modification_time: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct FirewallRule {
    pub firewall_rule_group_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub firewall_domain_list_id: Option<String>,
    pub name: String,
    pub priority: i64,
    pub action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_response: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_override_domain: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_override_dns_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_override_ttl: Option<i64>,
    pub creator_request_id: String,
    pub creation_time: String,
    pub modification_time: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub firewall_domain_redirection_action: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qtype: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct FirewallDomainList {
    pub id: String,
    pub arn: String,
    pub name: String,
    pub domain_count: i64,
    pub status: String,
    pub status_message: String,
    pub creator_request_id: String,
    pub creation_time: String,
    pub modification_time: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct FirewallRuleGroupAssociation {
    pub id: String,
    pub arn: String,
    pub firewall_rule_group_id: String,
    pub vpc_id: String,
    pub name: String,
    pub priority: i64,
    pub mutation_protection: String,
    pub status: String,
    pub status_message: String,
    pub creator_request_id: String,
    pub creation_time: String,
    pub modification_time: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct FirewallConfig {
    pub id: String,
    pub resource_id: String,
    pub owner_id: String,
    pub firewall_fail_open: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ResolverConfig {
    pub id: String,
    pub resource_id: String,
    pub owner_id: String,
    pub autodefined_reverse: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ResolverDnssecConfig {
    pub id: String,
    pub owner_id: String,
    pub resource_id: String,
    pub validation_status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct OutpostResolver {
    pub arn: String,
    pub creation_time: String,
    pub modification_time: String,
    pub creator_request_id: String,
    pub id: String,
    pub instance_count: i64,
    pub preferred_instance_type: String,
    pub name: String,
    pub status: String,
    pub status_message: String,
    pub outpost_arn: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Tag {
    pub key: String,
    pub value: String,
}

/// On-disk snapshot envelope. Versioned so format changes fail loudly on
/// upgrade rather than silently mis-parsing.
#[derive(Clone, Serialize, Deserialize)]
pub struct Route53ResolverSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<Route53ResolverAccounts>,
}
