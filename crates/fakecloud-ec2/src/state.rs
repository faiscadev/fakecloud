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

/// An ENI attachment.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EniAttachment {
    pub attachment_id: String,
    pub instance_id: String,
    pub device_index: i64,
    /// `attaching` | `attached` | `detaching` | `detached`.
    pub status: String,
}

/// An elastic network interface.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkInterface {
    pub network_interface_id: String,
    pub subnet_id: String,
    pub vpc_id: String,
    pub availability_zone: String,
    pub description: String,
    pub mac_address: String,
    pub private_ip_address: String,
    /// `available` | `in-use`.
    pub status: String,
    pub interface_type: String,
    pub source_dest_check: bool,
    #[serde(default)]
    pub group_ids: Vec<String>,
    #[serde(default)]
    pub private_ips: Vec<String>,
    #[serde(default)]
    pub ipv6_addresses: Vec<String>,
    pub attachment: Option<EniAttachment>,
}

/// A network-interface permission grant.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkInterfacePermission {
    pub permission_id: String,
    pub network_interface_id: String,
    pub aws_account_id: String,
    /// `INSTANCE-ATTACH` | `EIP-ASSOCIATE`.
    pub permission: String,
}

/// An EC2 instance (metadata-faithful; a Docker-backed runtime layers on top).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Instance {
    pub instance_id: String,
    pub image_id: String,
    pub instance_type: String,
    /// EC2 state code: 0 pending, 16 running, 32 shutting-down, 48 terminated,
    /// 64 stopping, 80 stopped.
    pub state_code: i64,
    pub state_name: String,
    pub private_ip: String,
    pub public_ip: Option<String>,
    pub subnet_id: Option<String>,
    pub vpc_id: Option<String>,
    pub key_name: Option<String>,
    #[serde(default)]
    pub security_group_ids: Vec<String>,
    pub reservation_id: String,
    pub ami_launch_index: i64,
    pub monitoring: bool,
    pub az: String,
    pub launch_time: String,
}

/// An EBS volume attachment.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VolumeAttachment {
    pub volume_id: String,
    pub instance_id: String,
    pub device: String,
    /// `attaching` | `attached` | `detaching` | `detached`.
    pub status: String,
    pub delete_on_termination: bool,
}

/// An EBS volume.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Volume {
    pub volume_id: String,
    pub size: i64,
    pub snapshot_id: Option<String>,
    pub availability_zone: String,
    /// `creating` | `available` | `in-use` | `deleting` | `deleted`.
    pub state: String,
    pub volume_type: String,
    pub iops: Option<i64>,
    pub throughput: Option<i64>,
    pub encrypted: bool,
    pub kms_key_id: Option<String>,
    pub multi_attach_enabled: bool,
    pub auto_enable_io: bool,
    #[serde(default)]
    pub attachments: Vec<VolumeAttachment>,
    #[serde(default)]
    pub in_recycle_bin: bool,
}

/// An EBS snapshot.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub snapshot_id: String,
    pub volume_id: String,
    /// `pending` | `completed` | `error`.
    pub state: String,
    pub volume_size: i64,
    pub description: String,
    pub encrypted: bool,
    /// `standard` | `archive`.
    pub storage_tier: String,
    #[serde(default)]
    pub in_recycle_bin: bool,
    #[serde(default)]
    pub locked: bool,
    pub lock_mode: Option<String>,
}

/// An AMI (machine image).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Image {
    pub image_id: String,
    pub name: String,
    pub description: String,
    /// `pending` | `available` | `disabled` | `deregistered`.
    pub state: String,
    pub architecture: String,
    pub public: bool,
    pub source_instance_id: Option<String>,
    #[serde(default)]
    pub in_recycle_bin: bool,
    pub deprecation_time: Option<String>,
    #[serde(default)]
    pub deregistration_protection: bool,
}

/// A network ACL entry (rule).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkAclEntry {
    pub rule_number: i64,
    pub protocol: String,
    /// `allow` | `deny`.
    pub rule_action: String,
    pub egress: bool,
    pub cidr_block: Option<String>,
    pub ipv6_cidr_block: Option<String>,
    /// TCP/UDP port range (from, to).
    pub port_range: Option<(i64, i64)>,
    /// ICMP (type, code).
    pub icmp_type_code: Option<(i64, i64)>,
}

/// A network ACL <-> subnet association.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkAclAssoc {
    pub association_id: String,
    pub subnet_id: String,
}

/// A network ACL.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkAcl {
    pub network_acl_id: String,
    pub vpc_id: String,
    pub is_default: bool,
    #[serde(default)]
    pub entries: Vec<NetworkAclEntry>,
    #[serde(default)]
    pub associations: Vec<NetworkAclAssoc>,
}

/// A VPC peering connection.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VpcPeering {
    pub id: String,
    pub requester_vpc_id: String,
    pub accepter_vpc_id: String,
    /// `pending-acceptance` | `active` | `rejected` | `deleted`.
    pub status: String,
    /// Requester-side DNS-resolution-from-remote-VPC option.
    #[serde(default)]
    pub requester_allow_dns: bool,
    /// Accepter-side DNS-resolution-from-remote-VPC option.
    #[serde(default)]
    pub accepter_allow_dns: bool,
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
    #[serde(default)]
    pub network_interfaces: HashMap<String, NetworkInterface>,
    /// keyed by permission id.
    #[serde(default)]
    pub eni_permissions: HashMap<String, NetworkInterfacePermission>,
    #[serde(default)]
    pub instances: HashMap<String, Instance>,
    #[serde(default)]
    pub volumes: HashMap<String, Volume>,
    /// Account-level EBS default encryption toggle.
    #[serde(default)]
    pub ebs_encryption_default: bool,
    /// Account-level EBS default KMS key (None = `alias/aws/ebs`).
    #[serde(default)]
    pub ebs_default_kms_key_id: Option<String>,
    #[serde(default)]
    pub snapshots: HashMap<String, Snapshot>,
    /// Account-level snapshot block-public-access state.
    #[serde(default)]
    pub snapshot_block_public_access: String,
    #[serde(default)]
    pub images: HashMap<String, Image>,
    /// Account-level image block-public-access state.
    #[serde(default)]
    pub image_block_public_access: String,
    /// Account-level allowed-images settings state.
    #[serde(default)]
    pub allowed_images_settings: String,
    #[serde(default)]
    pub network_acls: HashMap<String, NetworkAcl>,
    #[serde(default)]
    pub vpc_peerings: HashMap<String, VpcPeering>,
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
