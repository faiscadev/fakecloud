//! EC2 service state.
//!
//! Partitioned per account+region via [`fakecloud_core::multi_account`]. The
//! `tags` map is keyed by EC2 resource id (e.g. `vpc-…`, `i-…`, `sg-…`) and is
//! the backing store for `CreateTags`/`DeleteTags`/`DescribeTags` plus the
//! `tag:`/`tag-key` describe filters shared across every resource family.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Shared, account-partitioned EC2 state handle.
pub type SharedEc2State = Arc<RwLock<fakecloud_core::multi_account::MultiAccountState<Ec2State>>>;

/// On-disk snapshot envelope for EC2 state. Versioned so format changes fail
/// loudly on upgrade rather than silently mis-parsing. Backing containers are
/// not serialized -- on restore the server reconciles them via
/// `Ec2Service::recover_persisted_containers`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ec2Snapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<fakecloud_core::multi_account::MultiAccountState<Ec2State>>,
}

pub const EC2_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

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
    /// Amazon-provided IPv6 /56 CIDR, set when the VPC was created (or updated)
    /// with `AmazonProvidedIpv6CidrBlock=true`. Reported in the
    /// `Ipv6CidrBlockAssociationSet`; the `aws_vpc` resource reads
    /// `ipv6_cidr_block` / `assign_generated_ipv6_cidr_block` from it.
    #[serde(default)]
    pub ipv6_cidr_block: Option<String>,
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
    /// IPv6 /64 associated with the subnet (via CreateSubnet `Ipv6CidrBlock` or
    /// AssociateSubnetCidrBlock). Reported in the `ipv6CidrBlockAssociationSet`;
    /// the `aws_subnet` resource waits for this association.
    #[serde(default)]
    pub ipv6_cidr_block: Option<String>,
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
    /// `publicIpDnsNameOptions.publicHostnameType` set by
    /// `ModifyPublicIpDnsNameOptions`. AWS exposes no Describe that returns this
    /// setting, so it is persisted (and the ENI's existence enforced) but not
    /// reflected back through a Describe.
    #[serde(default)]
    pub public_ip_dns_hostname_type: Option<String>,
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
    /// Id of the backing container/Pod, when this instance is backed by a
    /// real container runtime. `None` in metadata-only mode.
    #[serde(default)]
    pub container_id: Option<String>,
    // ---- modifiable instance attributes (ModifyInstanceAttribute et al.) ----
    /// `disableApiTermination` — when true, TerminateInstances is rejected.
    #[serde(default)]
    pub disable_api_termination: bool,
    /// `disableApiStop` — when true, StopInstances is rejected.
    #[serde(default)]
    pub disable_api_stop: bool,
    /// `sourceDestCheck` — defaults to true on AWS.
    #[serde(default = "default_true")]
    pub source_dest_check: bool,
    /// `ebsOptimized`.
    #[serde(default)]
    pub ebs_optimized: bool,
    /// `instanceInitiatedShutdownBehavior` — `stop` (default) | `terminate`.
    #[serde(default = "default_shutdown_behavior")]
    pub instance_initiated_shutdown_behavior: String,
    /// `userData` — base64-encoded, as supplied at launch / via Modify.
    #[serde(default)]
    pub user_data: Option<String>,
    // ---- Modify*Options round-trip state ----
    /// `metadataOptions` (`ModifyInstanceMetadataOptions`).
    #[serde(default)]
    pub metadata_options: MetadataOptions,
    /// `cpuOptions` (`ModifyInstanceCpuOptions`).
    #[serde(default)]
    pub cpu_options: Option<CpuOptions>,
    /// `bandwidthWeighting` (`ModifyInstanceNetworkPerformanceOptions`).
    #[serde(default)]
    pub bandwidth_weighting: Option<String>,
    /// `maintenanceOptions` (`ModifyInstanceMaintenanceOptions`).
    #[serde(default)]
    pub maintenance_options: MaintenanceOptions,
    /// `placement` tenancy/affinity overrides (`ModifyInstancePlacement`).
    #[serde(default)]
    pub placement_tenancy: Option<String>,
    #[serde(default)]
    pub placement_affinity: Option<String>,
    #[serde(default)]
    pub placement_group_name: Option<String>,
    // ---- ModifyPrivateDnsNameOptions round-trip state ----
    /// `privateDnsNameOptions.hostnameType` — `ip-name` | `resource-name`.
    /// `None` reports the AWS default `ip-name`.
    #[serde(default)]
    pub private_dns_hostname_type: Option<String>,
    /// `privateDnsNameOptions.enableResourceNameDnsARecord`.
    #[serde(default)]
    pub enable_resource_name_dns_a_record: bool,
    /// `privateDnsNameOptions.enableResourceNameDnsAAAARecord`.
    #[serde(default)]
    pub enable_resource_name_dns_aaaa_record: bool,
}

fn default_true() -> bool {
    true
}

fn default_shutdown_behavior() -> String {
    "stop".to_string()
}

/// IMDS (instance-metadata service) options, round-tripped by
/// `ModifyInstanceMetadataOptions` and reflected in DescribeInstances.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetadataOptions {
    /// `optional` | `required`.
    pub http_tokens: String,
    /// `disabled` | `enabled`.
    pub http_endpoint: String,
    pub http_put_response_hop_limit: i64,
    /// `disabled` | `enabled`.
    pub http_protocol_ipv6: String,
    /// `disabled` | `enabled`.
    pub instance_metadata_tags: String,
}

impl Default for MetadataOptions {
    fn default() -> Self {
        Self {
            http_tokens: "optional".to_string(),
            http_endpoint: "enabled".to_string(),
            http_put_response_hop_limit: 1,
            http_protocol_ipv6: "disabled".to_string(),
            instance_metadata_tags: "disabled".to_string(),
        }
    }
}

/// CPU options round-tripped by `ModifyInstanceCpuOptions`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CpuOptions {
    pub core_count: i64,
    pub threads_per_core: i64,
}

/// Maintenance options round-tripped by `ModifyInstanceMaintenanceOptions`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MaintenanceOptions {
    /// `disabled` | `default`.
    pub auto_recovery: String,
    /// `disabled` | `default`.
    pub reboot_migration: String,
}

impl Default for MaintenanceOptions {
    fn default() -> Self {
        Self {
            auto_recovery: "default".to_string(),
            reboot_migration: "default".to_string(),
        }
    }
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
    /// `launchPermission` — AWS account ids the AMI is explicitly shared with
    /// (cross-account share via `ModifyImageAttribute`).
    #[serde(default)]
    pub launch_permission_users: Vec<String>,
    /// `launchPermission` groups — only `all` is valid in AWS (public share).
    #[serde(default)]
    pub launch_permission_groups: Vec<String>,
    /// `bootMode` — `legacy-bios` | `uefi` | `uefi-preferred`. `None` reports
    /// the default `uefi`; settable via `ModifyImageAttribute`.
    #[serde(default)]
    pub boot_mode: Option<String>,
    /// `imageOwnerId` — the AWS account that owns the AMI. `None` reports the
    /// requesting account (a user-registered AMI is owned by its creator); the
    /// seeded public AMIs set this to the real Amazon/Canonical/etc. owner so
    /// `aws_ami` data sources filtering by `owners`/`owner-id` resolve them.
    #[serde(default)]
    pub owner_id: Option<String>,
    /// `imageOwnerAlias` — `amazon` | `aws-marketplace` | `self` etc. Set on the
    /// seeded public AMIs so `owner-alias` filters and `owners = ["amazon"]`
    /// resolve them; `None` for user-registered AMIs.
    #[serde(default)]
    pub owner_alias: Option<String>,
    /// `creationDate`. `None` reports the fixed fallback; the seeded catalogue
    /// sets distinct dates so Terraform's `most_recent = true` ordering is
    /// deterministic.
    #[serde(default)]
    pub creation_date: Option<String>,
    /// `rootDeviceName` (e.g. `/dev/xvda` for Linux, `/dev/sda1` for Windows).
    /// `None` reports the Linux default.
    #[serde(default)]
    pub root_device_name: Option<String>,
    /// `platformDetails` / Windows `platform`. `None` = Linux/UNIX.
    #[serde(default)]
    pub platform: Option<String>,
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

/// A VPC endpoint.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VpcEndpoint {
    pub id: String,
    /// `Interface` | `Gateway` | `GatewayLoadBalancer` | ...
    pub endpoint_type: String,
    pub vpc_id: String,
    pub service_name: String,
    pub state: String,
    #[serde(default)]
    pub subnet_ids: Vec<String>,
    #[serde(default)]
    pub route_table_ids: Vec<String>,
    #[serde(default)]
    pub private_dns_enabled: bool,
}

/// A VPC endpoint service configuration (PrivateLink provider side).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EndpointService {
    pub service_id: String,
    pub service_name: String,
    pub state: String,
    pub acceptance_required: bool,
    pub payer_responsibility: String,
    #[serde(default)]
    pub nlb_arns: Vec<String>,
}

/// A VPC endpoint connection notification.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectionNotification {
    pub id: String,
    pub arn: String,
    pub service_id: Option<String>,
    #[serde(default)]
    pub events: Vec<String>,
}

/// A VPC flow log.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FlowLog {
    pub id: String,
    pub resource_id: String,
    pub traffic_type: String,
    pub log_destination_type: String,
    pub log_group_name: Option<String>,
    /// Destination ARN for `s3` / `kinesis-data-firehose` deliveries.
    pub log_destination: Option<String>,
    /// IAM role ARN used to deliver logs to CloudWatch Logs
    /// (`iam_role_arn` on the Terraform resource).
    #[serde(default)]
    pub deliver_logs_permission_arn: Option<String>,
    /// Max log aggregation interval in seconds. AWS accepts 60 or 600 and
    /// defaults to 600 when unspecified.
    #[serde(default = "default_max_aggregation_interval")]
    pub max_aggregation_interval: i64,
}

fn default_max_aggregation_interval() -> i64 {
    600
}

/// A launch template (versions tracked as monotonic counters).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LaunchTemplate {
    pub id: String,
    pub name: String,
    pub default_version: i64,
    pub latest_version: i64,
}

/// A Spot instance request.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpotRequest {
    pub id: String,
    /// `open` | `active` | `cancelled` | `closed`.
    pub state: String,
    pub request_type: String,
    pub spot_price: String,
}

/// A Spot fleet request.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpotFleet {
    pub id: String,
    pub state: String,
}

/// An EC2 fleet.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Fleet {
    pub id: String,
    pub state: String,
    pub fleet_type: String,
}

/// An on-demand capacity reservation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CapacityReservation {
    pub id: String,
    pub instance_type: String,
    pub instance_platform: String,
    pub availability_zone: String,
    pub tenancy: String,
    pub total_instance_count: i64,
    pub available_instance_count: i64,
    /// `active` | `expired` | `cancelled` | `pending` | `failed`.
    pub state: String,
    pub end_date_type: String,
    pub instance_match_criteria: String,
}

/// A Reserved Instance purchase.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReservedInstances {
    pub id: String,
    pub instance_type: String,
    pub availability_zone: String,
    pub instance_count: i64,
    pub product_description: String,
    pub state: String,
    pub duration: i64,
    pub fixed_price: String,
    pub usage_price: String,
}

/// A Reserved Instances listing in the Reserved Instance Marketplace.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReservedInstancesListing {
    pub listing_id: String,
    pub reserved_instances_id: String,
    pub instance_count: i64,
    pub client_token: String,
    /// `active` | `cancelled` | `closed`.
    pub status: String,
    pub status_message: String,
}

/// A Reserved Instances modification request.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReservedInstancesModification {
    pub modification_id: String,
    pub reserved_instances_ids: Vec<String>,
    /// `processing` | `fulfilled` | `failed`.
    pub status: String,
    pub client_token: String,
}

/// A Dedicated Host.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DedicatedHost {
    pub id: String,
    pub auto_placement: String,
    pub availability_zone: String,
    pub instance_type: String,
    pub state: String,
    pub host_recovery: String,
    pub host_maintenance: String,
}

/// A Transit Gateway.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransitGateway {
    pub id: String,
    pub description: String,
    /// `pending` | `available` | `modifying` | `deleting` | `deleted`.
    #[serde(default = "tgw_default_state")]
    pub state: String,
}

fn tgw_default_state() -> String {
    "available".to_string()
}

/// A Transit Gateway attachment (VPC and others).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TgwAttachment {
    pub id: String,
    pub tgw_id: String,
    pub resource_id: String,
    pub resource_type: String,
    #[serde(default)]
    pub subnet_ids: Vec<String>,
    pub state: String,
}

/// A Transit Gateway route table.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TgwRouteTable {
    pub id: String,
    pub tgw_id: String,
}

/// A static Transit Gateway route within a route table.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TgwRoute {
    pub cidr: String,
    pub attachment_id: String,
    pub state: String,
}

/// A Transit Gateway multicast domain.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TgwMulticastDomain {
    pub id: String,
    pub tgw_id: String,
}

/// A Transit Gateway metering policy.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TgwMeteringPolicy {
    pub id: String,
    pub tgw_id: String,
}

/// A customer gateway (on-prem side of a VPN).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CustomerGateway {
    pub id: String,
    pub state: String,
    pub ip_address: String,
    pub bgp_asn: String,
}

/// A virtual private gateway.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VpnGateway {
    pub id: String,
    pub state: String,
    #[serde(default)]
    pub attachments: Vec<String>,
}

/// A Site-to-Site VPN connection.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VpnConnection {
    pub id: String,
    pub state: String,
    pub customer_gateway_id: String,
    pub vpn_gateway_id: Option<String>,
    #[serde(default)]
    pub routes: Vec<String>,
}

/// A VPN concentrator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VpnConcentrator {
    pub id: String,
    pub state: String,
}

/// An IPAM (IP Address Manager).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Ipam {
    pub id: String,
    pub public_scope_id: String,
    pub private_scope_id: String,
    pub tier: String,
    #[serde(default)]
    pub description: String,
}

/// An IPAM scope.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IpamScope {
    pub id: String,
    pub ipam_id: String,
    /// "public" or "private".
    #[serde(default)]
    pub scope_type: String,
    #[serde(default)]
    pub description: String,
}

/// An IPAM pool.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IpamPool {
    pub id: String,
    pub scope_id: String,
    pub address_family: String,
    #[serde(default)]
    pub description: String,
}

/// An IPAM resource discovery.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IpamResourceDiscovery {
    pub id: String,
    #[serde(default)]
    pub description: String,
}

/// An IPAM policy.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IpamPolicy {
    pub id: String,
    pub ipam_id: String,
}

/// An IPAM prefix-list resolver.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IpamPrefixListResolver {
    pub id: String,
    pub ipam_id: String,
    pub address_family: String,
    #[serde(default)]
    pub description: String,
}

/// An IPAM prefix-list resolver target.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IpamPrefixListResolverTarget {
    pub id: String,
    pub resolver_id: String,
    pub prefix_list_id: String,
    pub prefix_list_region: String,
    #[serde(default)]
    pub track_latest_version: bool,
}

/// A Verified Access instance.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VerifiedAccessInstance {
    pub id: String,
    pub description: String,
    #[serde(default)]
    pub trust_providers: Vec<String>,
}

/// A Verified Access trust provider.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VerifiedAccessTrustProvider {
    pub id: String,
    pub trust_provider_type: String,
    pub policy_reference_name: String,
    pub description: String,
}

/// A Verified Access group.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VerifiedAccessGroup {
    pub id: String,
    pub instance_id: String,
    pub description: String,
}

/// A Verified Access endpoint.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VerifiedAccessEndpoint {
    pub id: String,
    pub group_id: String,
    pub instance_id: String,
    pub endpoint_type: String,
    pub attachment_type: String,
}

/// A Network Insights reachability path.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkInsightsPath {
    pub id: String,
    pub source: String,
    pub destination: String,
    pub protocol: String,
}

/// A Network Insights path analysis.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkInsightsAnalysis {
    pub id: String,
    pub path_id: String,
}

/// A Network Insights access scope.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkInsightsAccessScope {
    pub id: String,
}

/// A Network Insights access-scope analysis.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkInsightsAccessScopeAnalysis {
    pub id: String,
    pub scope_id: String,
}

/// A carrier gateway (Wavelength).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CarrierGateway {
    pub id: String,
    pub vpc_id: String,
}

/// An EC2 Instance Connect endpoint.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstanceConnectEndpoint {
    pub id: String,
    pub subnet_id: String,
}

/// A customer-owned IP (CoIP) pool.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CoipPool {
    pub id: String,
    pub route_table_id: String,
}

/// A local-gateway route table.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LocalGatewayRouteTable {
    pub id: String,
    pub local_gateway_id: String,
    pub mode: String,
}

/// A local-gateway route-table <-> VPC association.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LocalGatewayRouteTableVpcAssoc {
    pub id: String,
    pub route_table_id: String,
    pub vpc_id: String,
}

/// A local-gateway virtual interface.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LocalGatewayVif {
    pub id: String,
    pub group_id: String,
    pub vlan: String,
    pub local_address: String,
    pub peer_address: String,
}

/// A local-gateway virtual-interface group.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LocalGatewayVifGroup {
    pub id: String,
    pub local_gateway_id: String,
}

/// A local-gateway route-table <-> virtual-interface-group association.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LocalGatewayRouteTableVifgAssoc {
    pub id: String,
    pub route_table_id: String,
    pub vif_group_id: String,
}

/// A Client VPN endpoint.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientVpnEndpoint {
    pub id: String,
    pub description: String,
    pub status: String,
    pub server_cert_arn: String,
    pub transport_protocol: String,
    pub client_cidr: String,
    #[serde(default)]
    pub routes: Vec<String>,
    /// (association id, subnet id) for each associated target network.
    #[serde(default)]
    pub target_networks: Vec<(String, String)>,
    /// Ingress authorization rule target CIDRs.
    #[serde(default)]
    pub auth_rules: Vec<String>,
}

/// A Transit Gateway peering attachment.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TgwPeering {
    pub id: String,
    pub tgw_id: String,
    pub peer_tgw_id: String,
    pub peer_account: String,
    pub peer_region: String,
    pub state: String,
}

/// One entry in a customer-managed prefix list.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrefixListEntry {
    pub cidr: String,
    #[serde(default)]
    pub description: Option<String>,
}

/// A customer-managed prefix list (`CreateManagedPrefixList`). Versions are a
/// monotonic counter; each entry-mutating Modify bumps `version` and snapshots
/// the prior entries into `version_history` so `RestoreManagedPrefixListVersion`
/// and `GetManagedPrefixListEntries(TargetVersion)` round-trip.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ManagedPrefixList {
    pub prefix_list_id: String,
    pub prefix_list_name: String,
    pub address_family: String,
    pub max_entries: i64,
    pub version: i64,
    /// `create-complete` | `modify-complete`.
    pub state: String,
    #[serde(default)]
    pub entries: Vec<PrefixListEntry>,
    /// version -> entries snapshot at that version.
    #[serde(default)]
    pub version_history: BTreeMap<i64, Vec<PrefixListEntry>>,
}

/// A weekly time range within an instance event window.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventWindowTimeRange {
    pub start_week_day: String,
    pub start_hour: i64,
    pub end_week_day: String,
    pub end_hour: i64,
}

/// An instance event window (`CreateInstanceEventWindow`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstanceEventWindow {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub cron_expression: Option<String>,
    #[serde(default)]
    pub time_ranges: Vec<EventWindowTimeRange>,
    /// `creating` | `active` | `deleting` | `deleted`.
    pub state: String,
    /// Association target — instance ids, dedicated-host ids, or tags
    /// (`AssociateInstanceEventWindow` / `DisassociateInstanceEventWindow`).
    #[serde(default)]
    pub assoc_instance_ids: Vec<String>,
    #[serde(default)]
    pub assoc_dedicated_host_ids: Vec<String>,
    #[serde(default)]
    pub assoc_tags: Vec<Tag>,
}

/// A traffic-mirror target (`CreateTrafficMirrorTarget`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TrafficMirrorTarget {
    pub id: String,
    pub network_interface_id: Option<String>,
    pub network_load_balancer_arn: Option<String>,
    pub gateway_lb_endpoint_id: Option<String>,
    /// `network-interface` | `network-load-balancer` | `gateway-load-balancer-endpoint`.
    pub target_type: String,
    pub description: Option<String>,
}

/// A traffic-mirror filter (`CreateTrafficMirrorFilter`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TrafficMirrorFilter {
    pub id: String,
    pub description: Option<String>,
    #[serde(default)]
    pub network_services: Vec<String>,
}

/// A traffic-mirror filter rule (`CreateTrafficMirrorFilterRule`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TrafficMirrorFilterRule {
    pub id: String,
    pub filter_id: String,
    pub traffic_direction: String,
    pub rule_number: i64,
    pub rule_action: String,
    pub protocol: Option<i64>,
    pub destination_cidr_block: Option<String>,
    pub source_cidr_block: Option<String>,
    /// (from, to) port ranges.
    pub destination_port_range: Option<(i64, i64)>,
    pub source_port_range: Option<(i64, i64)>,
    pub description: Option<String>,
}

/// A traffic-mirror session (`CreateTrafficMirrorSession`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TrafficMirrorSession {
    pub id: String,
    pub target_id: String,
    pub filter_id: String,
    pub network_interface_id: String,
    pub packet_length: Option<i64>,
    pub session_number: i64,
    pub virtual_network_id: Option<i64>,
    pub description: Option<String>,
}

/// A route server (`CreateRouteServer`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RouteServer {
    pub id: String,
    pub amazon_side_asn: i64,
    /// `available` (lowercase per the AWS state enum is uppercase; we store the
    /// wire value).
    pub state: String,
    /// `ENABLED` | `DISABLED` | `RESETTING` | ...
    pub persist_routes_state: String,
    pub persist_routes_duration: Option<i64>,
    pub sns_notifications_enabled: bool,
}

/// A VPC encryption control (`CreateVpcEncryptionControl`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VpcEncryptionControl {
    pub id: String,
    pub vpc_id: String,
    /// `monitor` | `enforce`.
    pub mode: String,
    /// `available` | `monitor_in_progress` | `enforce_in_progress` | ...
    pub state: String,
    /// Per-resource-type exclusion state: resource -> `enabled` | `disabled`.
    #[serde(default)]
    pub exclusions: BTreeMap<String, String>,
}

/// A VPC block-public-access exclusion (`CreateVpcBlockPublicAccessExclusion`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VpcBpaExclusion {
    pub id: String,
    /// `allow-bidirectional` | `allow-egress`.
    pub internet_gateway_exclusion_mode: String,
    pub resource_arn: Option<String>,
    /// `create-complete` | `update-complete` | `delete-complete`.
    pub state: String,
}

/// An Amazon FPGA image (`CreateFpgaImage` / `CopyFpgaImage`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FpgaImage {
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub description: String,
    /// `loadPermission` users the image is shared with.
    #[serde(default)]
    pub load_permission_users: Vec<String>,
    /// `loadPermission` groups (`all` for public).
    #[serde(default)]
    pub load_permission_groups: Vec<String>,
}

/// Per-account, per-region EC2 state. Resource families are added to this
/// struct as their batches land.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Ec2State {
    pub account_id: String,
    pub region: String,
    /// resource-id -> tags. Shared by every Describe* `tag:` filter.
    #[serde(default)]
    pub tags: BTreeMap<String, Vec<Tag>>,
    #[serde(default)]
    pub vpcs: BTreeMap<String, Vpc>,
    #[serde(default)]
    pub dhcp_options: BTreeMap<String, DhcpOptions>,
    #[serde(default)]
    pub subnets: BTreeMap<String, Subnet>,
    #[serde(default)]
    pub subnet_cidr_reservations: BTreeMap<String, SubnetCidrReservation>,
    #[serde(default)]
    pub security_groups: BTreeMap<String, SecurityGroup>,
    #[serde(default)]
    pub route_tables: BTreeMap<String, RouteTable>,
    #[serde(default)]
    pub internet_gateways: BTreeMap<String, InternetGateway>,
    #[serde(default)]
    pub egress_only_igws: BTreeMap<String, InternetGateway>,
    #[serde(default)]
    pub nat_gateways: BTreeMap<String, NatGateway>,
    /// keyed by allocation id.
    #[serde(default)]
    pub elastic_ips: BTreeMap<String, ElasticIp>,
    /// keyed by key name.
    #[serde(default)]
    pub key_pairs: BTreeMap<String, KeyPair>,
    /// keyed by group name.
    #[serde(default)]
    pub placement_groups: BTreeMap<String, PlacementGroup>,
    #[serde(default)]
    pub network_interfaces: BTreeMap<String, NetworkInterface>,
    /// keyed by permission id.
    #[serde(default)]
    pub eni_permissions: BTreeMap<String, NetworkInterfacePermission>,
    #[serde(default)]
    pub instances: BTreeMap<String, Instance>,
    #[serde(default)]
    pub volumes: BTreeMap<String, Volume>,
    /// Account-level EBS default encryption toggle.
    #[serde(default)]
    pub ebs_encryption_default: bool,
    /// Account-level EBS default KMS key (None = `alias/aws/ebs`).
    #[serde(default)]
    pub ebs_default_kms_key_id: Option<String>,
    #[serde(default)]
    pub snapshots: BTreeMap<String, Snapshot>,
    /// Account-level snapshot block-public-access state.
    #[serde(default)]
    pub snapshot_block_public_access: String,
    #[serde(default)]
    pub images: BTreeMap<String, Image>,
    /// Watermarks attached to AMIs: image_id -> watermark_key -> watermark_name.
    #[serde(default)]
    pub image_watermarks: BTreeMap<String, BTreeMap<String, String>>,
    /// Account-level image block-public-access state.
    #[serde(default)]
    pub image_block_public_access: String,
    /// Account-level allowed-images settings state.
    #[serde(default)]
    pub allowed_images_settings: String,
    /// Allowed-images `imageCriterionSet`: each criterion is its list of
    /// `ImageProvider`s, persisted by ReplaceImageCriteriaInAllowedImagesSettings
    /// and reported by GetAllowedImagesSettings.
    #[serde(default)]
    pub allowed_image_criteria: Vec<Vec<String>>,
    #[serde(default)]
    pub network_acls: BTreeMap<String, NetworkAcl>,
    #[serde(default)]
    pub vpc_peerings: BTreeMap<String, VpcPeering>,
    #[serde(default)]
    pub vpc_endpoints: BTreeMap<String, VpcEndpoint>,
    #[serde(default)]
    pub endpoint_services: BTreeMap<String, EndpointService>,
    #[serde(default)]
    pub connection_notifications: BTreeMap<String, ConnectionNotification>,
    #[serde(default)]
    pub flow_logs: BTreeMap<String, FlowLog>,
    #[serde(default)]
    pub launch_templates: BTreeMap<String, LaunchTemplate>,
    #[serde(default)]
    pub spot_requests: BTreeMap<String, SpotRequest>,
    #[serde(default)]
    pub spot_fleets: BTreeMap<String, SpotFleet>,
    #[serde(default)]
    pub fleets: BTreeMap<String, Fleet>,
    /// Account-level spot datafeed subscription (bucket, prefix).
    #[serde(default)]
    pub spot_datafeed: Option<(String, String)>,
    #[serde(default)]
    pub capacity_reservations: BTreeMap<String, CapacityReservation>,
    /// Capacity reservation fleet ids (metadata-only).
    #[serde(default)]
    pub capacity_reservation_fleets: BTreeMap<String, String>,
    #[serde(default)]
    pub reserved_instances: BTreeMap<String, ReservedInstances>,
    #[serde(default)]
    pub reserved_instances_listings: BTreeMap<String, ReservedInstancesListing>,
    #[serde(default)]
    pub reserved_instances_modifications: BTreeMap<String, ReservedInstancesModification>,
    #[serde(default)]
    pub dedicated_hosts: BTreeMap<String, DedicatedHost>,
    #[serde(default)]
    pub transit_gateways: BTreeMap<String, TransitGateway>,
    #[serde(default)]
    pub tgw_attachments: BTreeMap<String, TgwAttachment>,
    #[serde(default)]
    pub tgw_route_tables: BTreeMap<String, TgwRouteTable>,
    /// route-table-id -> static routes.
    #[serde(default)]
    pub tgw_routes: BTreeMap<String, Vec<TgwRoute>>,
    /// route-table-id -> associated attachment ids.
    #[serde(default)]
    pub tgw_rt_associations: BTreeMap<String, Vec<String>>,
    /// route-table-id -> propagated attachment ids.
    #[serde(default)]
    pub tgw_rt_propagations: BTreeMap<String, Vec<String>>,
    /// route-table-id -> prefix-list ids referenced.
    #[serde(default)]
    pub tgw_prefix_list_refs: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub tgw_peerings: BTreeMap<String, TgwPeering>,
    /// connect-attachment-id -> (transport attachment id, tgw id).
    #[serde(default)]
    pub tgw_connects: BTreeMap<String, (String, String)>,
    /// connect-peer-id -> attachment id.
    #[serde(default)]
    pub tgw_connect_peers: BTreeMap<String, String>,
    /// policy-table-id -> tgw id.
    #[serde(default)]
    pub tgw_policy_tables: BTreeMap<String, String>,
    /// policy-table-id -> associated attachment ids.
    #[serde(default)]
    pub tgw_policy_table_associations: BTreeMap<String, Vec<String>>,
    /// announcement-id -> (route-table id, peering-attachment id).
    #[serde(default)]
    pub tgw_announcements: BTreeMap<String, (String, String)>,
    #[serde(default)]
    pub tgw_multicast_domains: BTreeMap<String, TgwMulticastDomain>,
    #[serde(default)]
    pub tgw_metering_policies: BTreeMap<String, TgwMeteringPolicy>,
    #[serde(default)]
    pub customer_gateways: BTreeMap<String, CustomerGateway>,
    #[serde(default)]
    pub vpn_gateways: BTreeMap<String, VpnGateway>,
    #[serde(default)]
    pub vpn_connections: BTreeMap<String, VpnConnection>,
    #[serde(default)]
    pub vpn_concentrators: BTreeMap<String, VpnConcentrator>,
    #[serde(default)]
    pub client_vpn_endpoints: BTreeMap<String, ClientVpnEndpoint>,
    #[serde(default)]
    pub ipams: BTreeMap<String, Ipam>,
    #[serde(default)]
    pub ipam_scopes: BTreeMap<String, IpamScope>,
    #[serde(default)]
    pub ipam_pools: BTreeMap<String, IpamPool>,
    /// pool-id -> provisioned (cidr, cidr-id).
    #[serde(default)]
    pub ipam_pool_cidrs: BTreeMap<String, Vec<(String, String)>>,
    /// pool-id -> allocations (cidr, allocation-id).
    #[serde(default)]
    pub ipam_pool_allocations: BTreeMap<String, Vec<(String, String)>>,
    #[serde(default)]
    pub ipam_resource_discoveries: BTreeMap<String, IpamResourceDiscovery>,
    /// association-id -> (discovery-id, ipam-id).
    #[serde(default)]
    pub ipam_rd_associations: BTreeMap<String, (String, String)>,
    /// asn -> associated cidr.
    #[serde(default)]
    pub ipam_byoasns: BTreeMap<String, String>,
    /// external-token-id -> ipam-id.
    #[serde(default)]
    pub ipam_ext_tokens: BTreeMap<String, String>,
    #[serde(default)]
    pub ipam_policies: BTreeMap<String, IpamPolicy>,
    #[serde(default)]
    pub ipam_pl_resolvers: BTreeMap<String, IpamPrefixListResolver>,
    #[serde(default)]
    pub ipam_pl_resolver_targets: BTreeMap<String, IpamPrefixListResolverTarget>,
    /// policy-id -> (locale, resource-type) allocation-rule documents.
    #[serde(default)]
    pub ipam_policy_alloc_rules: BTreeMap<String, Vec<(String, String)>>,
    /// The single enabled IPAM policy id, if any.
    #[serde(default)]
    pub ipam_enabled_policy: Option<String>,
    #[serde(default)]
    pub va_instances: BTreeMap<String, VerifiedAccessInstance>,
    #[serde(default)]
    pub va_trust_providers: BTreeMap<String, VerifiedAccessTrustProvider>,
    #[serde(default)]
    pub va_groups: BTreeMap<String, VerifiedAccessGroup>,
    #[serde(default)]
    pub va_endpoints: BTreeMap<String, VerifiedAccessEndpoint>,
    /// group-id -> policy document.
    #[serde(default)]
    pub va_group_policies: BTreeMap<String, String>,
    /// endpoint-id -> policy document.
    #[serde(default)]
    pub va_endpoint_policies: BTreeMap<String, String>,
    #[serde(default)]
    pub ni_paths: BTreeMap<String, NetworkInsightsPath>,
    #[serde(default)]
    pub ni_analyses: BTreeMap<String, NetworkInsightsAnalysis>,
    #[serde(default)]
    pub ni_access_scopes: BTreeMap<String, NetworkInsightsAccessScope>,
    #[serde(default)]
    pub ni_scope_analyses: BTreeMap<String, NetworkInsightsAccessScopeAnalysis>,
    #[serde(default)]
    pub carrier_gateways: BTreeMap<String, CarrierGateway>,
    #[serde(default)]
    pub coip_pools: BTreeMap<String, CoipPool>,
    /// coip-pool-id -> CIDRs.
    #[serde(default)]
    pub coip_pool_cidrs: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub lg_route_tables: BTreeMap<String, LocalGatewayRouteTable>,
    /// route-table-id -> destination CIDRs.
    #[serde(default)]
    pub lg_routes: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub lg_rt_vpc_assocs: BTreeMap<String, LocalGatewayRouteTableVpcAssoc>,
    #[serde(default)]
    pub lg_virtual_interfaces: BTreeMap<String, LocalGatewayVif>,
    #[serde(default)]
    pub lg_vif_groups: BTreeMap<String, LocalGatewayVifGroup>,
    #[serde(default)]
    pub lg_rt_vifg_assocs: BTreeMap<String, LocalGatewayRouteTableVifgAssoc>,
    #[serde(default)]
    pub instance_connect_endpoints: BTreeMap<String, InstanceConnectEndpoint>,
    /// Image ids with fast-launch enabled.
    #[serde(default)]
    pub fast_launch_images: std::collections::HashSet<String>,
    #[serde(default)]
    pub serial_console_access: bool,
    // ---- account/region-scoped id-format settings (ModifyIdFormat) ----
    /// resource type -> use-long-ids (account default).
    #[serde(default)]
    pub id_format: BTreeMap<String, bool>,
    /// principal ARN -> (resource type -> use-long-ids).
    #[serde(default)]
    pub identity_id_format: BTreeMap<String, BTreeMap<String, bool>>,
    /// burstable instance family -> CpuCredits (`standard` | `unlimited`),
    /// set by ModifyDefaultCreditSpecification, read by GetDefaultCreditSpecification.
    #[serde(default)]
    pub default_credit_specs: BTreeMap<String, String>,
    /// VPC block-public-access `InternetGatewayBlockMode` (account/region
    /// singleton). `None` reports the default `off`.
    #[serde(default)]
    pub vpc_bpa_internet_gateway_block_mode: Option<String>,
    /// Managed-resource `DefaultVisibility` (`hidden` | `visible`). `None`
    /// reports the default `visible`.
    #[serde(default)]
    pub managed_resource_default_visibility: Option<String>,
    /// Availability-zone group -> opt-in status (`opted-in` | `not-opted-in`),
    /// set by ModifyAvailabilityZoneGroup, reflected in DescribeAvailabilityZones.
    #[serde(default)]
    pub az_group_optin: BTreeMap<String, String>,
    #[serde(default)]
    pub managed_prefix_lists: BTreeMap<String, ManagedPrefixList>,
    #[serde(default)]
    pub instance_event_windows: BTreeMap<String, InstanceEventWindow>,
    #[serde(default)]
    pub traffic_mirror_targets: BTreeMap<String, TrafficMirrorTarget>,
    #[serde(default)]
    pub traffic_mirror_filters: BTreeMap<String, TrafficMirrorFilter>,
    #[serde(default)]
    pub traffic_mirror_filter_rules: BTreeMap<String, TrafficMirrorFilterRule>,
    #[serde(default)]
    pub traffic_mirror_sessions: BTreeMap<String, TrafficMirrorSession>,
    #[serde(default)]
    pub route_servers: BTreeMap<String, RouteServer>,
    #[serde(default)]
    pub vpc_encryption_controls: BTreeMap<String, VpcEncryptionControl>,
    #[serde(default)]
    pub vpc_bpa_exclusions: BTreeMap<String, VpcBpaExclusion>,
    #[serde(default)]
    pub fpga_images: BTreeMap<String, FpgaImage>,
    /// IPAM pool allocation id -> description (ModifyIpamPoolAllocation), read
    /// back by DescribeIpamPoolAllocations.
    #[serde(default)]
    pub ipam_allocation_descriptions: BTreeMap<String, String>,
}

impl Ec2State {
    pub fn new(account_id: &str, region: &str) -> Self {
        let mut state = Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            ..Default::default()
        };
        // Seed the default VPC topology (VPC, IGW, subnets, route table,
        // security group, NACL) the way every AWS account+region ships one, so
        // callers that never touch the VPC APIs still launch into a real,
        // isolatable network. Ids are deterministic, so the throwaway empty
        // states the read paths build report the same ids as this one.
        crate::defaults::bootstrap_default_network(&mut state);
        // Seed the public AMI catalogue (Amazon Linux, Ubuntu, Windows) so
        // `aws_ami` data sources resolve, matching how every real account sees
        // Amazon/Canonical-owned public images.
        crate::defaults::seed_public_images(&mut state);
        state
    }

    /// Idempotently (re)seed the public AMI catalogue into this account. Used on
    /// snapshot restore so accounts persisted by a binary that predated the
    /// catalogue (#1964) still get it after an upgrade+restart — without it,
    /// `aws_ami { owners=["amazon"] }` returns empty for legacy accounts. Seeds
    /// have deterministic ids, so re-seeding an already-seeded account is a no-op.
    pub fn ensure_public_images_seeded(&mut self) {
        crate::defaults::seed_public_images(self);
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

    fn sample_instance() -> Instance {
        Instance {
            instance_id: "i-1".to_string(),
            image_id: "ami-1".to_string(),
            instance_type: "t3.micro".to_string(),
            state_code: 16,
            state_name: "running".to_string(),
            private_ip: "10.0.0.1".to_string(),
            public_ip: Some("52.0.0.1".to_string()),
            subnet_id: Some("subnet-1".to_string()),
            vpc_id: Some("vpc-1".to_string()),
            key_name: None,
            security_group_ids: vec!["sg-1".to_string()],
            reservation_id: "r-1".to_string(),
            ami_launch_index: 0,
            monitoring: false,
            az: "us-east-1a".to_string(),
            launch_time: "2024-01-01T00:00:00.000Z".to_string(),
            container_id: Some("abc".to_string()),
            disable_api_termination: true,
            disable_api_stop: true,
            source_dest_check: false,
            ebs_optimized: true,
            instance_initiated_shutdown_behavior: "terminate".to_string(),
            user_data: Some("ZWNobyBoaQ==".to_string()),
            metadata_options: MetadataOptions {
                http_tokens: "required".to_string(),
                ..MetadataOptions::default()
            },
            cpu_options: Some(CpuOptions {
                core_count: 4,
                threads_per_core: 2,
            }),
            bandwidth_weighting: Some("vpc-1".to_string()),
            maintenance_options: MaintenanceOptions::default(),
            placement_tenancy: Some("dedicated".to_string()),
            placement_affinity: None,
            placement_group_name: Some("cluster-1".to_string()),
            private_dns_hostname_type: Some("resource-name".to_string()),
            enable_resource_name_dns_a_record: true,
            enable_resource_name_dns_aaaa_record: false,
        }
    }

    #[test]
    fn instance_attributes_round_trip_through_serde() {
        let inst = sample_instance();
        let json = serde_json::to_string(&inst).unwrap();
        let back: Instance = serde_json::from_str(&json).unwrap();
        assert!(back.disable_api_termination);
        assert!(back.disable_api_stop);
        assert!(!back.source_dest_check);
        assert!(back.ebs_optimized);
        assert_eq!(back.instance_initiated_shutdown_behavior, "terminate");
        assert_eq!(back.user_data.as_deref(), Some("ZWNobyBoaQ=="));
        assert_eq!(back.metadata_options.http_tokens, "required");
        assert_eq!(back.cpu_options.as_ref().unwrap().core_count, 4);
        assert_eq!(back.bandwidth_weighting.as_deref(), Some("vpc-1"));
        assert_eq!(back.placement_tenancy.as_deref(), Some("dedicated"));
        assert_eq!(back.placement_group_name.as_deref(), Some("cluster-1"));
    }

    #[test]
    fn instance_attribute_defaults_load_from_legacy_snapshot() {
        // A snapshot written before the attribute fields existed (only the
        // pre-existing members) must deserialize, with AWS defaults filled in.
        let legacy = r#"{
            "instance_id":"i-1","image_id":"ami-1","instance_type":"t3.micro",
            "state_code":16,"state_name":"running","private_ip":"10.0.0.1",
            "public_ip":null,"subnet_id":null,"vpc_id":null,"key_name":null,
            "reservation_id":"r-1","ami_launch_index":0,"monitoring":false,
            "az":"us-east-1a","launch_time":"2024-01-01T00:00:00.000Z"
        }"#;
        let inst: Instance = serde_json::from_str(legacy).unwrap();
        assert!(!inst.disable_api_termination);
        assert!(inst.source_dest_check, "sourceDestCheck defaults to true");
        assert_eq!(inst.instance_initiated_shutdown_behavior, "stop");
        assert_eq!(inst.metadata_options.http_tokens, "optional");
        assert!(inst.cpu_options.is_none());
    }
}
