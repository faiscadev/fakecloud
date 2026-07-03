//! In-memory, per-account state for Amazon Redshift.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

pub type SharedRedshiftState = Arc<RwLock<RedshiftAccounts>>;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RedshiftAccounts {
    pub accounts: BTreeMap<String, RedshiftState>,
}

impl RedshiftAccounts {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn account(&mut self, account_id: &str) -> &mut RedshiftState {
        self.accounts
            .entry(account_id.to_string())
            .or_insert_with(|| RedshiftState::new_for_account(account_id))
    }
}

/// On-disk snapshot envelope for Redshift state. Versioned so format changes
/// fail loudly on upgrade rather than silently mis-parsing.
#[derive(Clone, Serialize, Deserialize)]
pub struct RedshiftSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<RedshiftAccounts>,
}

pub const REDSHIFT_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// All Redshift control-plane resources for a single AWS account.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RedshiftState {
    #[serde(default)]
    pub account_id: String,
    #[serde(default)]
    pub clusters: BTreeMap<String, Cluster>,
    #[serde(default)]
    pub parameter_groups: BTreeMap<String, ClusterParameterGroup>,
    #[serde(default)]
    pub security_groups: BTreeMap<String, ClusterSecurityGroup>,
    #[serde(default)]
    pub subnet_groups: BTreeMap<String, ClusterSubnetGroup>,
    #[serde(default)]
    pub snapshots: BTreeMap<String, Snapshot>,
    #[serde(default)]
    pub event_subscriptions: BTreeMap<String, EventSubscription>,
    #[serde(default)]
    pub hsm_client_certificates: BTreeMap<String, HsmClientCertificate>,
    #[serde(default)]
    pub hsm_configurations: BTreeMap<String, HsmConfiguration>,
    #[serde(default)]
    pub snapshot_copy_grants: BTreeMap<String, SnapshotCopyGrant>,
    #[serde(default)]
    pub snapshot_schedules: BTreeMap<String, SnapshotSchedule>,
    #[serde(default)]
    pub scheduled_actions: BTreeMap<String, ScheduledAction>,
    #[serde(default)]
    pub usage_limits: BTreeMap<String, UsageLimit>,
    #[serde(default)]
    pub endpoint_access: BTreeMap<String, EndpointAccess>,
    #[serde(default)]
    pub endpoint_authorizations: BTreeMap<String, EndpointAuthorization>,
    #[serde(default)]
    pub authentication_profiles: BTreeMap<String, AuthenticationProfile>,
    #[serde(default)]
    pub datashares: BTreeMap<String, DataShare>,
    #[serde(default)]
    pub partners: BTreeMap<String, Partner>,
    #[serde(default)]
    pub reserved_nodes: BTreeMap<String, ReservedNode>,
    #[serde(default)]
    pub integrations: BTreeMap<String, Integration>,
    #[serde(default)]
    pub idc_applications: BTreeMap<String, RedshiftIdcApplication>,
    #[serde(default)]
    pub custom_domains: BTreeMap<String, CustomDomainAssociation>,
    #[serde(default)]
    pub table_restore_status: BTreeMap<String, TableRestoreStatus>,
    /// Cluster-logging status keyed by cluster identifier.
    #[serde(default)]
    pub logging: BTreeMap<String, LoggingStatus>,
    /// IAM resource policies keyed by resource ARN.
    #[serde(default)]
    pub resource_policies: BTreeMap<String, String>,
    /// Cross-region snapshot copy config keyed by cluster identifier.
    #[serde(default)]
    pub registered_namespaces: BTreeMap<String, String>,
}

impl RedshiftState {
    pub fn new_for_account(account_id: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tag {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cluster {
    pub cluster_identifier: String,
    pub node_type: String,
    pub cluster_status: String,
    pub cluster_availability_status: String,
    pub master_username: String,
    pub db_name: String,
    pub endpoint_address: String,
    pub endpoint_port: i32,
    pub cluster_create_time: DateTime<Utc>,
    pub automated_snapshot_retention_period: i32,
    pub manual_snapshot_retention_period: i32,
    pub cluster_security_groups: Vec<String>,
    pub vpc_security_group_ids: Vec<String>,
    pub cluster_parameter_group_name: String,
    pub cluster_subnet_group_name: Option<String>,
    pub vpc_id: Option<String>,
    pub availability_zone: String,
    pub preferred_maintenance_window: String,
    pub cluster_version: String,
    pub allow_version_upgrade: bool,
    pub number_of_nodes: i32,
    pub publicly_accessible: bool,
    pub encrypted: bool,
    pub multi_az: bool,
    pub cluster_type: String,
    pub kms_key_id: Option<String>,
    pub enhanced_vpc_routing: bool,
    pub maintenance_track_name: String,
    pub elastic_ip: Option<String>,
    pub availability_zone_relocation_status: String,
    pub aqua_configuration_status: String,
    pub default_iam_role_arn: Option<String>,
    pub iam_roles: Vec<String>,
    pub snapshot_schedule_identifier: Option<String>,
    pub total_storage_capacity_in_mega_bytes: i64,
    pub next_maintenance_window_start_time: Option<DateTime<Utc>>,
    pub snapshot_copy: Option<SnapshotCopyStatus>,
    pub tags: Vec<Tag>,
}

/// Cross-region snapshot-copy configuration set via `EnableSnapshotCopy`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotCopyStatus {
    pub destination_region: String,
    pub retention_period: i64,
    pub manual_snapshot_retention_period: i32,
    pub snapshot_copy_grant_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Parameter {
    pub parameter_name: String,
    pub parameter_value: String,
    pub description: String,
    pub source: String,
    pub data_type: String,
    pub allowed_values: String,
    pub apply_type: String,
    pub is_modifiable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterParameterGroup {
    pub parameter_group_name: String,
    pub parameter_group_family: String,
    pub description: String,
    pub parameters: Vec<Parameter>,
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ec2SecurityGroup {
    pub status: String,
    pub ec2_security_group_name: String,
    pub ec2_security_group_owner_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpRange {
    pub status: String,
    pub cidrip: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSecurityGroup {
    pub cluster_security_group_name: String,
    pub description: String,
    pub ec2_security_groups: Vec<Ec2SecurityGroup>,
    pub ip_ranges: Vec<IpRange>,
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subnet {
    pub subnet_identifier: String,
    pub subnet_availability_zone: String,
    pub subnet_status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSubnetGroup {
    pub cluster_subnet_group_name: String,
    pub description: String,
    pub vpc_id: String,
    pub subnet_group_status: String,
    pub subnets: Vec<Subnet>,
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub snapshot_identifier: String,
    pub snapshot_arn: String,
    pub cluster_identifier: String,
    pub snapshot_create_time: DateTime<Utc>,
    pub status: String,
    pub port: i32,
    pub availability_zone: String,
    pub cluster_create_time: DateTime<Utc>,
    pub master_username: String,
    pub cluster_version: String,
    pub snapshot_type: String,
    pub node_type: String,
    pub number_of_nodes: i32,
    pub db_name: String,
    pub vpc_id: Option<String>,
    pub encrypted: bool,
    pub kms_key_id: Option<String>,
    pub manual_snapshot_retention_period: i32,
    pub total_backup_size_in_mega_bytes: f64,
    pub actual_incremental_backup_size_in_mega_bytes: f64,
    pub accounts_with_restore_access: Vec<String>,
    pub owner_account: String,
    pub source_region: Option<String>,
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventSubscription {
    pub subscription_name: String,
    pub customer_aws_id: String,
    pub sns_topic_arn: String,
    pub status: String,
    pub subscription_creation_time: DateTime<Utc>,
    pub source_type: Option<String>,
    pub source_ids: Vec<String>,
    pub event_categories: Vec<String>,
    pub severity: Option<String>,
    pub enabled: bool,
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HsmClientCertificate {
    pub hsm_client_certificate_identifier: String,
    pub hsm_client_certificate_public_key: String,
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HsmConfiguration {
    pub hsm_configuration_identifier: String,
    pub description: String,
    pub hsm_ip_address: String,
    pub hsm_partition_name: String,
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotCopyGrant {
    pub snapshot_copy_grant_name: String,
    pub kms_key_id: String,
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotSchedule {
    pub schedule_identifier: String,
    pub schedule_description: String,
    pub schedule_definitions: Vec<String>,
    pub tags: Vec<Tag>,
    pub associated_cluster_count: i32,
    pub associated_clusters: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledAction {
    pub scheduled_action_name: String,
    pub target_action: Option<String>,
    pub schedule: String,
    pub iam_role: String,
    pub scheduled_action_description: String,
    pub state: String,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub enable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageLimit {
    pub usage_limit_id: String,
    pub cluster_identifier: String,
    pub feature_type: String,
    pub limit_type: String,
    pub amount: i64,
    pub period: String,
    pub breach_action: String,
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointAccess {
    pub endpoint_name: String,
    pub cluster_identifier: String,
    pub subnet_group_name: String,
    pub endpoint_status: String,
    pub endpoint_create_time: DateTime<Utc>,
    pub resource_owner: String,
    pub port: i32,
    pub address: String,
    pub vpc_security_group_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointAuthorization {
    pub grantor: String,
    pub grantee: String,
    pub cluster_identifier: String,
    pub authorize_time: DateTime<Utc>,
    pub cluster_status: String,
    pub status: String,
    pub allowed_all_vpcs: bool,
    pub allowed_vpcs: Vec<String>,
    pub endpoint_count: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthenticationProfile {
    pub authentication_profile_name: String,
    pub authentication_profile_content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataShare {
    pub data_share_arn: String,
    pub producer_arn: String,
    pub allow_publicly_accessible_consumers: bool,
    pub managed_by: Option<String>,
    pub data_share_type: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Partner {
    pub database_name: String,
    pub partner_name: String,
    pub cluster_identifier: String,
    pub account_id: String,
    pub status: String,
    pub status_message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReservedNode {
    pub reserved_node_id: String,
    pub reserved_node_offering_id: String,
    pub node_type: String,
    pub start_time: DateTime<Utc>,
    pub duration: i32,
    pub fixed_price: f64,
    pub usage_price: f64,
    pub currency_code: String,
    pub node_count: i32,
    pub state: String,
    pub offering_type: String,
    pub reserved_node_offering_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Integration {
    pub integration_arn: String,
    pub integration_name: String,
    pub source_arn: String,
    pub target_arn: String,
    pub status: String,
    pub create_time: DateTime<Utc>,
    pub description: Option<String>,
    pub kms_key_id: Option<String>,
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedshiftIdcApplication {
    pub redshift_idc_application_name: String,
    pub redshift_idc_application_arn: String,
    pub identity_namespace: Option<String>,
    pub idc_instance_arn: String,
    pub idc_display_name: String,
    pub iam_role_arn: String,
    pub idc_managed_application_arn: String,
    pub authorized_token_issuer_list: Vec<String>,
    pub service_integrations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomDomainAssociation {
    pub custom_domain_name: String,
    pub custom_domain_certificate_arn: String,
    pub cluster_identifier: String,
    pub custom_domain_certificate_expiry_date: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableRestoreStatus {
    pub table_restore_request_id: String,
    pub status: String,
    pub cluster_identifier: String,
    pub snapshot_identifier: String,
    pub source_database_name: String,
    pub source_table_name: String,
    pub new_table_name: String,
    pub target_database_name: String,
    pub request_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingStatus {
    pub logging_enabled: bool,
    pub bucket_name: Option<String>,
    pub s3_key_prefix: Option<String>,
    pub log_destination_type: Option<String>,
    pub log_exports: Vec<String>,
    pub last_successful_delivery_time: Option<DateTime<Utc>>,
}
