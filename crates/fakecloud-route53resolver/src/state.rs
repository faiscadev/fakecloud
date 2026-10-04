//! In-memory state for Amazon Route 53 Resolver (`route53resolver`).
//!
//! State is partitioned per account and region: every Resolver resource is
//! regional, so each (account, region) owns its own full set of Resolver
//! resources: resolver endpoints (with their IP addresses), resolver rules and
//! their VPC associations, query-log configurations and their associations,
//! DNS Firewall rule groups / domain lists / rules / associations, plus the
//! per-VPC firewall / resolver / DNSSEC configuration singletons (keyed by VPC
//! id within the region), Outpost resolvers, resource-based policies and tags.
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

/// v2: state partitioned by account, then region. v1 kept one state per
/// account for every region.
pub const R53R_SNAPSHOT_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Route53ResolverAccounts {
    /// Account id -> region -> that account's Resolver state in the region.
    pub accounts: BTreeMap<String, BTreeMap<String, AccountState>>,
}

impl Route53ResolverAccounts {
    pub fn new() -> Self {
        Self::default()
    }

    /// The account's state in `region`, created empty on first use.
    pub fn region_mut(&mut self, account: &str, region: &str) -> &mut AccountState {
        self.accounts
            .entry(account.to_string())
            .or_default()
            .entry(region.to_string())
            .or_default()
    }

    /// The account's state in `region`, `None` when nothing has touched it.
    pub fn region(&self, account: &str, region: &str) -> Option<&AccountState> {
        self.accounts.get(account).and_then(|r| r.get(region))
    }

    /// The account's state in `region` without creating it.
    pub fn region_get_mut(&mut self, account: &str, region: &str) -> Option<&mut AccountState> {
        self.accounts.get_mut(account).and_then(|r| r.get_mut(region))
    }

    /// Every (account, region, state) triple.
    pub fn iter_regional(&self) -> impl Iterator<Item = (&str, &str, &AccountState)> {
        self.accounts.iter().flat_map(|(account, regions)| {
            regions
                .iter()
                .map(move |(region, st)| (account.as_str(), region.as_str(), st))
        })
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outpost_arn: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preferred_instance_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dns64_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ipv6_internet_access_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rni_enhanced_metrics_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_name_server_metrics_enabled: Option<bool>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delegation_record: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dns_threat_protection: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence_threshold: Option<String>,
    /// The (structured) rule-type configuration echoed back verbatim. Stored as
    /// a JSON value because the `FirewallRuleType` union carries several
    /// alternative nested configs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub firewall_rule_type: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub firewall_threat_protection_id: Option<String>,
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

/// The container v1 snapshots stored: one state per account.
#[derive(Deserialize)]
struct LegacySnapshot {
    #[serde(default)]
    accounts: Option<LegacyAccounts>,
}

#[derive(Deserialize)]
struct LegacyAccounts {
    accounts: BTreeMap<String, AccountState>,
}

#[derive(Deserialize)]
struct SnapshotVersion {
    schema_version: u32,
}

impl AccountState {
    /// Split a v1 account-wide state into regions. Resources with an ARN go
    /// to the region it names; records without one follow the resource they
    /// belong to (an IP address its endpoint, a rule association its rule, a
    /// query-log association its config, firewall rules / domains their group
    /// / list, a firewall association its group); the per-VPC configs name no
    /// region and land in `default_region`.
    fn split_by_region(self, default_region: &str) -> BTreeMap<String, AccountState> {
        let mut out: BTreeMap<String, AccountState> = BTreeMap::new();
        let region_of = |arn: &str| {
            fakecloud_aws::arn::region_of(arn)
                .unwrap_or(default_region)
                .to_string()
        };
        // Every region a resource id was placed in, for children to follow.
        let mut placed: BTreeMap<String, String> = BTreeMap::new();
        macro_rules! by_arn {
            ($field:ident, $item:ident => $arn:expr) => {
                for (id, $item) in self.$field {
                    let region = region_of(&$arn);
                    placed.insert(id.clone(), region.clone());
                    out.entry(region).or_default().$field.insert(id, $item);
                }
            };
        }
        by_arn!(endpoints, r => r.endpoint.arn);
        by_arn!(rules, r => r.arn);
        by_arn!(query_log_configs, r => r.arn);
        by_arn!(firewall_rule_groups, r => r.arn);
        by_arn!(firewall_domain_lists, r => r.arn);
        by_arn!(firewall_rule_group_associations, r => r.arn);
        by_arn!(outpost_resolvers, r => r.arn);
        let follow = |parent: &str| {
            placed
                .get(parent)
                .cloned()
                .unwrap_or_else(|| default_region.to_string())
        };
        for (id, a) in self.rule_associations {
            let region = follow(&a.resolver_rule_id);
            out.entry(region).or_default().rule_associations.insert(id, a);
        }
        for (id, a) in self.query_log_associations {
            let region = follow(&a.resolver_query_log_config_id);
            out.entry(region)
                .or_default()
                .query_log_associations
                .insert(id, a);
        }
        for (group, rules) in self.firewall_rules {
            let region = follow(&group);
            out.entry(region).or_default().firewall_rules.insert(group, rules);
        }
        for (list, domains) in self.firewall_domains {
            let region = follow(&list);
            out.entry(region)
                .or_default()
                .firewall_domains
                .insert(list, domains);
        }
        let home = out.entry(default_region.to_string()).or_default();
        home.firewall_configs = self.firewall_configs;
        home.resolver_configs = self.resolver_configs;
        home.dnssec_configs = self.dnssec_configs;
        for (map, target) in [
            (self.firewall_rule_group_policies, 0),
            (self.query_log_config_policies, 1),
            (self.resolver_rule_policies, 2),
        ] {
            for (arn, policy) in map {
                let st = out.entry(region_of(&arn)).or_default();
                match target {
                    0 => st.firewall_rule_group_policies.insert(arn, policy),
                    1 => st.query_log_config_policies.insert(arn, policy),
                    _ => st.resolver_rule_policies.insert(arn, policy),
                };
            }
        }
        for (arn, tags) in self.tags {
            out.entry(region_of(&arn)).or_default().tags.insert(arn, tags);
        }
        out
    }
}

/// Parse a persisted snapshot, migrating a v1 one (one state per account)
/// by moving each resource into its region (see
/// [`AccountState::split_by_region`]); `default_region` receives what names
/// no region. A snapshot newer than this build comes back with its on-disk
/// `schema_version` and no state, for the caller to refuse.
pub fn parse_route53resolver_snapshot(
    bytes: &[u8],
    default_region: &str,
) -> Result<Route53ResolverSnapshot, serde_json::Error> {
    let SnapshotVersion { schema_version } = serde_json::from_slice(bytes)?;
    if schema_version > R53R_SNAPSHOT_SCHEMA_VERSION {
        return Ok(Route53ResolverSnapshot {
            schema_version,
            accounts: None,
        });
    }
    if schema_version == R53R_SNAPSHOT_SCHEMA_VERSION {
        return serde_json::from_slice(bytes);
    }
    let legacy: LegacySnapshot = serde_json::from_slice(bytes)?;
    Ok(Route53ResolverSnapshot {
        schema_version: R53R_SNAPSHOT_SCHEMA_VERSION,
        accounts: legacy.accounts.map(|l| Route53ResolverAccounts {
            accounts: l
                .accounts
                .into_iter()
                .map(|(account, st)| (account, st.split_by_region(default_region)))
                .collect(),
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn group(id: &str, region: &str) -> FirewallRuleGroup {
        FirewallRuleGroup {
            id: id.to_string(),
            arn: format!("arn:aws:route53resolver:{region}:123456789012:firewall-rule-group/{id}"),
            name: "g".to_string(),
            rule_count: 0,
            status: "COMPLETE".to_string(),
            status_message: String::new(),
            owner_id: "123456789012".to_string(),
            creator_request_id: id.to_string(),
            share_status: "NOT_SHARED".to_string(),
            creation_time: String::new(),
            modification_time: String::new(),
        }
    }

    #[test]
    fn v1_snapshot_splits_resources_by_region() {
        let mut legacy = AccountState::default();
        legacy
            .firewall_rule_groups
            .insert("rslvr-frg-east".into(), group("rslvr-frg-east", "us-east-1"));
        legacy
            .firewall_rule_groups
            .insert("rslvr-frg-west".into(), group("rslvr-frg-west", "eu-west-1"));
        // Rules follow their group; per-VPC configs land in the default region.
        legacy
            .firewall_rules
            .insert("rslvr-frg-west".into(), Vec::new());
        legacy.firewall_configs.insert(
            "vpc-1".into(),
            FirewallConfig {
                id: "rslvr-fc-1".into(),
                resource_id: "vpc-1".into(),
                owner_id: "123456789012".into(),
                firewall_fail_open: "ENABLED".into(),
            },
        );
        legacy.tags.insert(
            "arn:aws:route53resolver:eu-west-1:123456789012:firewall-rule-group/rslvr-frg-west"
                .into(),
            vec![Tag {
                key: "k".into(),
                value: "v".into(),
            }],
        );
        let v1 = serde_json::json!({
            "schema_version": 1,
            "accounts": { "accounts": { "123456789012": legacy } },
        });
        let snap =
            parse_route53resolver_snapshot(&serde_json::to_vec(&v1).unwrap(), "us-east-1").unwrap();
        assert_eq!(snap.schema_version, R53R_SNAPSHOT_SCHEMA_VERSION);
        let accounts = snap.accounts.unwrap();
        let east = accounts.region("123456789012", "us-east-1").unwrap();
        let west = accounts.region("123456789012", "eu-west-1").unwrap();
        assert!(east.firewall_rule_groups.contains_key("rslvr-frg-east"));
        assert!(!east.firewall_rule_groups.contains_key("rslvr-frg-west"));
        assert!(west.firewall_rule_groups.contains_key("rslvr-frg-west"));
        assert!(west.firewall_rules.contains_key("rslvr-frg-west"));
        assert_eq!(west.tags.len(), 1);
        assert!(east.firewall_configs.contains_key("vpc-1"));
        assert!(west.firewall_configs.is_empty());

        // The migrated snapshot round-trips as the current schema.
        let current = serde_json::to_vec(&Route53ResolverSnapshot {
            schema_version: R53R_SNAPSHOT_SCHEMA_VERSION,
            accounts: Some(accounts),
        })
        .unwrap();
        let again = parse_route53resolver_snapshot(&current, "us-east-1")
            .unwrap()
            .accounts
            .unwrap();
        assert!(again.region("123456789012", "eu-west-1").is_some());
    }

    #[test]
    fn newer_snapshot_is_reported_not_parsed() {
        let snap = parse_route53resolver_snapshot(br#"{"schema_version": 99}"#, "us-east-1").unwrap();
        assert_eq!(snap.schema_version, 99);
        assert!(snap.accounts.is_none());
    }
}
