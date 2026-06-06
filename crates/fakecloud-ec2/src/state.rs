//! EC2 service state.
//!
//! Partitioned per account+region via [`fakecloud_core::multi_account`]. The
//! `tags` map is keyed by EC2 resource id (e.g. `vpc-…`, `i-…`, `sg-…`) and is
//! the backing store for `CreateTags`/`DeleteTags`/`DescribeTags` plus the
//! `tag:`/`tag-key` describe filters shared across every resource family.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Shared, account-partitioned EC2 state handle.
pub type SharedEc2State = Arc<RwLock<fakecloud_core::multi_account::MultiAccountState<Ec2State>>>;

impl fakecloud_core::multi_account::AccountState for Ec2State {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }
}

/// A single EC2 resource tag.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tag {
    pub key: String,
    pub value: String,
}

/// A secondary CIDR-block association on a VPC.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VpcCidrAssoc {
    pub association_id: String,
    pub cidr_block: String,
    /// `associated` | `disassociated`.
    pub state: String,
}

/// A Virtual Private Cloud.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Vpc {
    pub vpc_id: String,
    pub cidr_block: String,
    /// `pending` | `available`.
    pub state: String,
    pub dhcp_options_id: String,
    /// `default` | `dedicated` | `host`.
    pub instance_tenancy: String,
    pub is_default: bool,
    pub enable_dns_support: bool,
    pub enable_dns_hostnames: bool,
    #[serde(default)]
    pub cidr_associations: Vec<VpcCidrAssoc>,
}

/// One `key -> values` entry in a DHCP options set.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DhcpConfig {
    pub key: String,
    pub values: Vec<String>,
}

/// A DHCP options set.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DhcpOptions {
    pub dhcp_options_id: String,
    pub configurations: Vec<DhcpConfig>,
}

/// A subnet within a VPC.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Subnet {
    pub subnet_id: String,
    pub vpc_id: String,
    pub cidr_block: String,
    pub availability_zone: String,
    pub availability_zone_id: String,
    /// `pending` | `available`.
    pub state: String,
    pub available_ip_address_count: i32,
    pub default_for_az: bool,
    pub map_public_ip_on_launch: bool,
    pub assign_ipv6_address_on_creation: bool,
    pub map_customer_owned_ip_on_launch: bool,
    pub enable_dns64: bool,
    /// `ip-name` | `resource-name`.
    pub private_dns_hostname_type: String,
}

/// A CIDR reservation within a subnet.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SubnetCidrReservation {
    pub subnet_cidr_reservation_id: String,
    pub subnet_id: String,
    pub cidr: String,
    /// `prefix` | `explicit`.
    pub reservation_type: String,
    pub description: String,
}

/// A security-group rule (ingress or egress), stored flat.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SecurityGroupRule {
    pub rule_id: String,
    pub group_id: String,
    pub is_egress: bool,
    pub ip_protocol: String,
    pub from_port: i64,
    pub to_port: i64,
    pub cidr_ipv4: Option<String>,
    pub cidr_ipv6: Option<String>,
    pub prefix_list_id: Option<String>,
    pub referenced_group_id: Option<String>,
    pub description: String,
}

/// A security group.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SecurityGroup {
    pub group_id: String,
    pub group_name: String,
    pub description: String,
    pub vpc_id: String,
    #[serde(default)]
    pub rules: Vec<SecurityGroupRule>,
}

/// A route within a route table.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Route {
    pub destination_cidr_block: Option<String>,
    pub destination_ipv6_cidr_block: Option<String>,
    pub destination_prefix_list_id: Option<String>,
    pub gateway_id: Option<String>,
    pub nat_gateway_id: Option<String>,
    pub network_interface_id: Option<String>,
    pub instance_id: Option<String>,
    pub vpc_peering_connection_id: Option<String>,
    pub transit_gateway_id: Option<String>,
    pub egress_only_internet_gateway_id: Option<String>,
    /// `active` | `blackhole`.
    pub state: String,
    /// `CreateRouteTable` | `CreateRoute`.
    pub origin: String,
}

/// A route-table association (to a subnet or gateway, or the VPC main table).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RouteTableAssociation {
    pub association_id: String,
    pub route_table_id: String,
    pub subnet_id: Option<String>,
    pub gateway_id: Option<String>,
    pub main: bool,
}

/// A route table.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RouteTable {
    pub route_table_id: String,
    pub vpc_id: String,
    #[serde(default)]
    pub routes: Vec<Route>,
    #[serde(default)]
    pub associations: Vec<RouteTableAssociation>,
}

/// An internet gateway (or egress-only IGW) with its VPC attachments.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InternetGateway {
    pub internet_gateway_id: String,
    /// (vpc_id, state) pairs.
    #[serde(default)]
    pub attachments: Vec<(String, String)>,
}

/// A NAT gateway.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NatGateway {
    pub nat_gateway_id: String,
    pub subnet_id: String,
    pub vpc_id: String,
    /// `pending` | `available` | `deleting` | `deleted`.
    pub state: String,
    /// `public` | `private`.
    pub connectivity_type: String,
    pub allocation_id: Option<String>,
}

/// An Elastic IP allocation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ElasticIp {
    pub allocation_id: String,
    pub public_ip: String,
    /// `vpc` | `standard`.
    pub domain: String,
    pub association_id: Option<String>,
    pub instance_id: Option<String>,
    pub network_interface_id: Option<String>,
    pub private_ip_address: Option<String>,
}

/// An EC2 key pair (public-key metadata only).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyPair {
    pub key_pair_id: String,
    pub key_name: String,
    /// `rsa` | `ed25519`.
    pub key_type: String,
    pub key_fingerprint: String,
}

/// A placement group.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlacementGroup {
    pub group_id: String,
    pub group_name: String,
    /// `cluster` | `spread` | `partition`.
    pub strategy: String,
    /// `available`.
    pub state: String,
    pub partition_count: Option<i64>,
    pub spread_level: Option<String>,
}

/// Per-account, per-region EC2 state. Resource families are added to this
/// struct as their batches land.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Ec2State {
    pub account_id: String,
    pub region: String,
    /// resource-id -> tags. Shared by every Describe* `tag:` filter.
    #[serde(default)]
    pub tags: HashMap<String, Vec<Tag>>,
    #[serde(default)]
    pub vpcs: HashMap<String, Vpc>,
    #[serde(default)]
    pub dhcp_options: HashMap<String, DhcpOptions>,
    #[serde(default)]
    pub subnets: HashMap<String, Subnet>,
    #[serde(default)]
    pub subnet_cidr_reservations: HashMap<String, SubnetCidrReservation>,
    #[serde(default)]
    pub security_groups: HashMap<String, SecurityGroup>,
    #[serde(default)]
    pub route_tables: HashMap<String, RouteTable>,
    #[serde(default)]
    pub internet_gateways: HashMap<String, InternetGateway>,
    #[serde(default)]
    pub egress_only_igws: HashMap<String, InternetGateway>,
    #[serde(default)]
    pub nat_gateways: HashMap<String, NatGateway>,
    /// keyed by allocation id.
    #[serde(default)]
    pub elastic_ips: HashMap<String, ElasticIp>,
    /// keyed by key name.
    #[serde(default)]
    pub key_pairs: HashMap<String, KeyPair>,
    /// keyed by group name.
    #[serde(default)]
    pub placement_groups: HashMap<String, PlacementGroup>,
}

impl Ec2State {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            ..Default::default()
        }
    }

    /// Replace the tag set for `resource_id` with `tags` merged over any
    /// existing tags (CreateTags is upsert-by-key, matching AWS).
    pub fn upsert_tags(&mut self, resource_id: &str, new_tags: &[Tag]) {
        let entry = self.tags.entry(resource_id.to_string()).or_default();
        for t in new_tags {
            if let Some(existing) = entry.iter_mut().find(|e| e.key == t.key) {
                existing.value = t.value.clone();
            } else {
                entry.push(t.clone());
            }
        }
    }

    /// Remove tags for `resource_id`. When a tag's value is `None`, the key is
    /// removed regardless of value; when `Some`, only a key+value match is
    /// removed (AWS DeleteTags semantics).
    pub fn remove_tags(&mut self, resource_id: &str, to_remove: &[(String, Option<String>)]) {
        if let Some(entry) = self.tags.get_mut(resource_id) {
            for (key, value) in to_remove {
                entry.retain(|e| {
                    if &e.key != key {
                        return true;
                    }
                    match value {
                        Some(v) => &e.value != v,
                        None => false,
                    }
                });
            }
            if entry.is_empty() {
                self.tags.remove(resource_id);
            }
        }
    }

    /// Tags for `resource_id`, or an empty slice when none.
    pub fn tags_for(&self, resource_id: &str) -> &[Tag] {
        self.tags.get(resource_id).map(Vec::as_slice).unwrap_or(&[])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tag(k: &str, v: &str) -> Tag {
        Tag {
            key: k.to_string(),
            value: v.to_string(),
        }
    }

    #[test]
    fn upsert_tags_inserts_then_overwrites_by_key() {
        let mut s = Ec2State::new("123456789012", "us-east-1");
        s.upsert_tags("vpc-1", &[tag("Name", "a"), tag("env", "dev")]);
        s.upsert_tags("vpc-1", &[tag("Name", "b")]);
        let tags = s.tags_for("vpc-1");
        assert_eq!(tags.len(), 2);
        assert_eq!(tags.iter().find(|t| t.key == "Name").unwrap().value, "b");
    }

    #[test]
    fn remove_tags_by_key_and_by_key_value() {
        let mut s = Ec2State::new("123456789012", "us-east-1");
        s.upsert_tags(
            "i-1",
            &[tag("Name", "x"), tag("env", "prod"), tag("team", "a")],
        );
        // key-only removal
        s.remove_tags("i-1", &[("Name".to_string(), None)]);
        // key+value removal that does NOT match -> kept
        s.remove_tags("i-1", &[("env".to_string(), Some("dev".to_string()))]);
        // key+value removal that matches -> removed
        s.remove_tags("i-1", &[("team".to_string(), Some("a".to_string()))]);
        let tags = s.tags_for("i-1");
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0].key, "env");
    }

    #[test]
    fn empty_tag_set_drops_resource_entry() {
        let mut s = Ec2State::new("123456789012", "us-east-1");
        s.upsert_tags("sg-1", &[tag("Name", "x")]);
        s.remove_tags("sg-1", &[("Name".to_string(), None)]);
        assert!(!s.tags.contains_key("sg-1"));
    }
}
