use fakecloud_core::data_volume::DataVolumeBinding;
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use fakecloud_aws::arn::Arn;
use parking_lot::RwLock;
use uuid::Uuid;

/// An RDS ARN (`arn:<partition>:rds:<region>:<account>:<kind>:<id>`) in the
/// region's partition. Every RDS resource kind (`db`, `cluster`, `snapshot`,
/// `cluster-snapshot`, `pg`, `cluster-pg`, `subgrp`, `og`, `es`, `db-proxy`,
/// ...) is built through here so the API and CloudFormation agree.
pub fn rds_arn(region: &str, account_id: &str, kind: &str, id: &str) -> String {
    Arn::regional("rds", region, account_id, &format!("{kind}:{id}")).to_string()
}

/// The per-account, per-region DNS label RDS puts in every endpoint host
/// (`c9akciq32.us-east-1.rds.amazonaws.com`): 12 lowercase alphanumerics,
/// stable for an account and region so every instance and cluster in it
/// shares one, as on AWS.
pub fn endpoint_hash(account_id: &str, region: &str) -> String {
    // FNV-1a over "<account>:<region>", rendered in base 36.
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in account_id
        .bytes()
        .chain(std::iter::once(b':'))
        .chain(region.bytes())
    {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut out = String::with_capacity(12);
    for i in 0..12 {
        // The first character is a letter, like AWS's.
        let radix = if i == 0 { 26 } else { 36 };
        out.push(ALPHABET[(h % radix) as usize] as char);
        h /= radix;
        if h == 0 {
            h = 0x9e37_79b9_7f4a_7c15 ^ (i as u64);
        }
    }
    out
}

/// A DB instance's endpoint host: `<id>.<hash>.<region>.rds.<dns suffix>`.
pub fn instance_endpoint(id: &str, account_id: &str, region: &str) -> String {
    format!(
        "{id}.{}.{region}.rds.{}",
        endpoint_hash(account_id, region),
        fakecloud_aws::endpoint::dns_suffix_for_region(region)
    )
}

/// A DB cluster's writer endpoint host:
/// `<id>.cluster-<hash>.<region>.rds.<dns suffix>`.
pub fn cluster_endpoint(id: &str, account_id: &str, region: &str) -> String {
    format!(
        "{id}.cluster-{}.{region}.rds.{}",
        endpoint_hash(account_id, region),
        fakecloud_aws::endpoint::dns_suffix_for_region(region)
    )
}

/// A DB cluster's reader endpoint host:
/// `<id>.cluster-ro-<hash>.<region>.rds.<dns suffix>`.
pub fn cluster_reader_endpoint(id: &str, account_id: &str, region: &str) -> String {
    format!(
        "{id}.cluster-ro-{}.{region}.rds.{}",
        endpoint_hash(account_id, region),
        fakecloud_aws::endpoint::dns_suffix_for_region(region)
    )
}

/// A global cluster's ARN: no region field, in the partition of `region`.
pub fn global_cluster_arn(region: &str, account_id: &str, id: &str) -> String {
    Arn::global_in(region, "rds", account_id, &format!("global-cluster:{id}")).to_string()
}

pub type SharedRdsState = Arc<RwLock<fakecloud_core::multi_account::MultiAccountState<RdsState>>>;

impl fakecloud_core::multi_account::AccountState for RdsState {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }
}

/// Representative catalog of current DB instance classes used to
/// enumerate `DescribeOrderableDBInstanceOptions`. This is NOT an
/// allowlist — `CreateDBInstance`/`ModifyDBInstance` accept ANY
/// well-formed AWS instance class (see
/// `service_helpers::validate_db_instance_class`), because the class is
/// stored purely as metadata and never reaches the container runtime.
/// The list below just gives `DescribeOrderableDBInstanceOptions` a
/// realistic, non-empty set of common current-generation classes to
/// return across the burstable (t*), general-purpose (m*), and
/// memory-optimized (r*) families.
pub const SUPPORTED_INSTANCE_CLASSES: &[&str] = &[
    // Burstable — t2 / t3 / t4g
    "db.t2.micro",
    "db.t2.small",
    "db.t2.medium",
    "db.t2.large",
    "db.t3.micro",
    "db.t3.small",
    "db.t3.medium",
    "db.t3.large",
    "db.t3.xlarge",
    "db.t3.2xlarge",
    "db.t4g.micro",
    "db.t4g.small",
    "db.t4g.medium",
    "db.t4g.large",
    "db.t4g.xlarge",
    "db.t4g.2xlarge",
    // General purpose — m5 / m6i / m6g / m7g
    "db.m5.large",
    "db.m5.xlarge",
    "db.m5.2xlarge",
    "db.m5.4xlarge",
    "db.m5.8xlarge",
    "db.m5.12xlarge",
    "db.m5.16xlarge",
    "db.m5.24xlarge",
    "db.m6i.large",
    "db.m6i.xlarge",
    "db.m6i.2xlarge",
    "db.m6i.4xlarge",
    "db.m6i.8xlarge",
    "db.m6i.12xlarge",
    "db.m6i.16xlarge",
    "db.m6i.24xlarge",
    "db.m6i.32xlarge",
    "db.m6g.large",
    "db.m6g.xlarge",
    "db.m6g.2xlarge",
    "db.m6g.4xlarge",
    "db.m6g.8xlarge",
    "db.m6g.12xlarge",
    "db.m6g.16xlarge",
    "db.m7g.large",
    "db.m7g.xlarge",
    "db.m7g.2xlarge",
    "db.m7g.4xlarge",
    "db.m7g.8xlarge",
    "db.m7g.12xlarge",
    "db.m7g.16xlarge",
    // Memory optimized — r5 / r6g / r7g
    "db.r5.large",
    "db.r5.xlarge",
    "db.r5.2xlarge",
    "db.r5.4xlarge",
    "db.r5.8xlarge",
    "db.r5.12xlarge",
    "db.r5.16xlarge",
    "db.r5.24xlarge",
    "db.r6g.large",
    "db.r6g.xlarge",
    "db.r6g.2xlarge",
    "db.r6g.4xlarge",
    "db.r6g.8xlarge",
    "db.r6g.12xlarge",
    "db.r6g.16xlarge",
    "db.r7g.large",
    "db.r7g.xlarge",
    "db.r7g.2xlarge",
    "db.r7g.4xlarge",
    "db.r7g.8xlarge",
    "db.r7g.12xlarge",
    "db.r7g.16xlarge",
];

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct DbInstance {
    pub db_instance_identifier: String,
    pub db_instance_arn: String,
    /// IAM roles associated through `AddRoleToDBInstance`, reported as
    /// `AssociatedRoles`.
    #[serde(default)]
    pub associated_roles: Vec<DbRole>,
    pub db_instance_class: String,
    pub engine: String,
    pub engine_version: String,
    pub db_instance_status: String,
    pub master_username: String,
    pub db_name: Option<String>,
    pub endpoint_address: String,
    pub port: i32,
    pub allocated_storage: i32,
    pub publicly_accessible: bool,
    pub deletion_protection: bool,
    pub created_at: DateTime<Utc>,
    pub dbi_resource_id: String,
    pub master_user_password: String,
    pub container_id: String,
    pub host_port: u16,
    /// Which data volume the instance mounts (see
    /// `fakecloud_core::data_volume` and [`DbInstance::data_volume_name`]).
    /// Instances created by this build are bound to their data-dir scoped
    /// volume; `None` only for an instance persisted before volumes were
    /// scoped (or never resolved since), which
    /// [`RdsState::resolve_data_volumes`] binds against the daemon's volumes.
    #[serde(default)]
    pub data_volume: Option<DataVolumeBinding>,
    pub tags: Vec<RdsTag>,
    pub read_replica_source_db_instance_identifier: Option<String>,
    pub read_replica_db_instance_identifiers: Vec<String>,
    pub vpc_security_group_ids: Vec<String>,
    pub db_parameter_group_name: Option<String>,
    pub backup_retention_period: i32,
    pub preferred_backup_window: String,
    #[serde(default)]
    pub preferred_maintenance_window: Option<String>,
    pub latest_restorable_time: Option<DateTime<Utc>>,
    pub option_group_name: Option<String>,
    pub multi_az: bool,
    pub pending_modified_values: Option<PendingModifiedValues>,
    /// DB subnet group the instance was placed in. AWS echoes the whole
    /// group under `<DBSubnetGroup>` in Describe/Create/Modify responses;
    /// we keep the name here and resolve the group from `subnet_groups`
    /// at render time so a later ModifyDBSubnetGroup is reflected.
    #[serde(default)]
    pub db_subnet_group_name: Option<String>,
    /// Read from input on Create/Modify; defaults preserve existing
    /// behaviour (non-encrypted, gp2, single AZ, no IAM auth).
    #[serde(default)]
    pub availability_zone: Option<String>,
    #[serde(default)]
    pub storage_type: Option<String>,
    #[serde(default)]
    pub storage_encrypted: bool,
    #[serde(default)]
    pub kms_key_id: Option<String>,
    #[serde(default)]
    pub iam_database_authentication_enabled: bool,
    #[serde(default)]
    pub iops: Option<i32>,
    #[serde(default)]
    pub monitoring_interval: Option<i32>,
    #[serde(default)]
    pub monitoring_role_arn: Option<String>,
    #[serde(default)]
    pub performance_insights_enabled: bool,
    #[serde(default)]
    pub performance_insights_kms_key_id: Option<String>,
    #[serde(default)]
    pub performance_insights_retention_period: Option<i32>,
    #[serde(default)]
    pub enabled_cloudwatch_logs_exports: Vec<String>,
    #[serde(default)]
    pub ca_certificate_identifier: Option<String>,
    #[serde(default)]
    pub network_type: Option<String>,
    #[serde(default)]
    pub character_set_name: Option<String>,
    #[serde(default)]
    pub auto_minor_version_upgrade: Option<bool>,
    #[serde(default)]
    pub copy_tags_to_snapshot: Option<bool>,
    #[serde(default)]
    pub master_user_secret_arn: Option<String>,
    #[serde(default)]
    pub master_user_secret_kms_key_id: Option<String>,
    /// Settable via Modify; AWS reports the engine-derived default until
    /// the caller overrides. We honor explicit overrides but fall back to
    /// `license_model_for_engine` in XML when this is `None`.
    #[serde(default)]
    pub license_model: Option<String>,
    #[serde(default)]
    pub max_allocated_storage: Option<i32>,
    #[serde(default)]
    pub multi_tenant: Option<bool>,
    #[serde(default)]
    pub storage_throughput: Option<i32>,
    #[serde(default)]
    pub tde_credential_arn: Option<String>,
    #[serde(default)]
    pub delete_automated_backups: Option<bool>,
    #[serde(default)]
    pub db_security_groups: Vec<String>,
    /// Active Directory domain membership. AWS exposes these via
    /// `<DomainMemberships><DomainMembership>...` in describe responses.
    #[serde(default)]
    pub domain: Option<String>,
    #[serde(default)]
    pub domain_fqdn: Option<String>,
    #[serde(default)]
    pub domain_ou: Option<String>,
    #[serde(default)]
    pub domain_iam_role_name: Option<String>,
    #[serde(default)]
    pub domain_auth_secret_arn: Option<String>,
    #[serde(default)]
    pub domain_dns_ips: Vec<String>,
    /// Aurora cluster the instance is a member of, when set. Mirrors
    /// `DBClusterIdentifier` on CreateDBInstance / RestoreDB* requests so
    /// snapshot/restore paths can find the writer for a given cluster.
    #[serde(default)]
    pub db_cluster_identifier: Option<String>,
    /// Database Activity Stream configuration, written by
    /// `StartActivityStream` / `ModifyActivityStream` and cleared to
    /// `stopped` by `StopActivityStream`. `None` reads back as a stopped
    /// stream in describe responses.
    #[serde(default)]
    pub activity_stream: Option<ActivityStreamConfig>,
    /// The DB capacity reservation this instance belongs to
    /// (`DBCapacityReservationIdentifier`). A stopped instance keeps it
    /// (unless stopped with `DetachFromDBCapacityReservation`) but holds no
    /// unit until it starts again.
    #[serde(default)]
    pub capacity_reservation: Option<CapacityReservationUse>,
}

/// Database Activity Stream state for a DB instance. Persisted so that
/// `StartActivityStream` / `StopActivityStream` / `ModifyActivityStream`
/// round-trip through `DescribeDBInstances` instead of always reporting
/// `stopped`.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ActivityStreamConfig {
    /// One of `starting` | `started` | `stopping` | `stopped`.
    pub status: String,
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default)]
    pub kms_key_id: Option<String>,
    #[serde(default)]
    pub kinesis_stream_name: Option<String>,
}

#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct PendingModifiedValues {
    pub db_instance_class: Option<String>,
    pub allocated_storage: Option<i32>,
    pub backup_retention_period: Option<i32>,
    pub multi_az: Option<bool>,
    pub engine_version: Option<String>,
    pub master_user_password: Option<String>,
    #[serde(default)]
    pub preferred_backup_window: Option<String>,
    #[serde(default)]
    pub preferred_maintenance_window: Option<String>,
    #[serde(default)]
    pub db_parameter_group_name: Option<String>,
    #[serde(default)]
    pub iops: Option<i32>,
    #[serde(default)]
    pub storage_type: Option<String>,
    #[serde(default)]
    pub monitoring_interval: Option<i32>,
    #[serde(default)]
    pub performance_insights_enabled: Option<bool>,
    #[serde(default)]
    pub enabled_cloudwatch_logs_exports: Option<Vec<String>>,
    #[serde(default)]
    pub storage_throughput: Option<i32>,
    #[serde(default)]
    pub license_model: Option<String>,
    #[serde(default)]
    pub multi_tenant: Option<bool>,
    #[serde(default)]
    pub publicly_accessible: Option<bool>,
    #[serde(default)]
    pub tde_credential_arn: Option<String>,
    #[serde(default)]
    pub port: Option<i32>,
    #[serde(default)]
    pub ca_certificate_identifier: Option<String>,
    /// The reservation the instance moves onto when the pending change
    /// applies. It already holds a unit there, as on AWS.
    #[serde(default)]
    pub capacity_reservation: Option<CapacityReservationUse>,
}

impl fmt::Debug for PendingModifiedValues {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PendingModifiedValues")
            .field("db_instance_class", &self.db_instance_class)
            .field("allocated_storage", &self.allocated_storage)
            .field("backup_retention_period", &self.backup_retention_period)
            .field("multi_az", &self.multi_az)
            .field("engine_version", &self.engine_version)
            .field(
                "master_user_password",
                &self.master_user_password.as_ref().map(|_| "<redacted>"),
            )
            .field("preferred_backup_window", &self.preferred_backup_window)
            .field(
                "preferred_maintenance_window",
                &self.preferred_maintenance_window,
            )
            .field("db_parameter_group_name", &self.db_parameter_group_name)
            .field("iops", &self.iops)
            .field("storage_type", &self.storage_type)
            .field("monitoring_interval", &self.monitoring_interval)
            .field(
                "performance_insights_enabled",
                &self.performance_insights_enabled,
            )
            .field(
                "enabled_cloudwatch_logs_exports",
                &self.enabled_cloudwatch_logs_exports,
            )
            .field("storage_throughput", &self.storage_throughput)
            .field("license_model", &self.license_model)
            .field("multi_tenant", &self.multi_tenant)
            .field("publicly_accessible", &self.publicly_accessible)
            .field("tde_credential_arn", &self.tde_credential_arn)
            .field("port", &self.port)
            .field("ca_certificate_identifier", &self.ca_certificate_identifier)
            .field("capacity_reservation", &self.capacity_reservation)
            .finish()
    }
}

impl fmt::Debug for DbInstance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DbInstance")
            .field("db_instance_identifier", &self.db_instance_identifier)
            .field("db_instance_arn", &self.db_instance_arn)
            .field("db_instance_class", &self.db_instance_class)
            .field("engine", &self.engine)
            .field("engine_version", &self.engine_version)
            .field("db_instance_status", &self.db_instance_status)
            .field("master_username", &self.master_username)
            .field("db_name", &self.db_name)
            .field("endpoint_address", &self.endpoint_address)
            .field("port", &self.port)
            .field("allocated_storage", &self.allocated_storage)
            .field("publicly_accessible", &self.publicly_accessible)
            .field("deletion_protection", &self.deletion_protection)
            .field("created_at", &self.created_at)
            .field("dbi_resource_id", &self.dbi_resource_id)
            .field("master_user_password", &"<redacted>")
            .field("container_id", &self.container_id)
            .field("host_port", &self.host_port)
            .field("tags", &self.tags)
            .field(
                "read_replica_source_db_instance_identifier",
                &self.read_replica_source_db_instance_identifier,
            )
            .field(
                "read_replica_db_instance_identifiers",
                &self.read_replica_db_instance_identifiers,
            )
            .field("vpc_security_group_ids", &self.vpc_security_group_ids)
            .field("db_parameter_group_name", &self.db_parameter_group_name)
            .field("backup_retention_period", &self.backup_retention_period)
            .field("preferred_backup_window", &self.preferred_backup_window)
            .field("latest_restorable_time", &self.latest_restorable_time)
            .field("option_group_name", &self.option_group_name)
            .field("multi_az", &self.multi_az)
            .field("pending_modified_values", &self.pending_modified_values)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RdsTag {
    pub key: String,
    pub value: String,
}

/// An IAM role associated with a DB instance or cluster.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct DbRole {
    pub role_arn: String,
    pub feature_name: String,
    /// AWS reports `ACTIVE` once the association is usable; there is
    /// nothing to wait for here.
    pub status: String,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct DbSnapshot {
    /// For a final snapshot taken on DeleteDBInstance: the deleted instance's
    /// data volume, which the snapshot's deferred teardown removes. `None`
    /// for every other snapshot.
    #[serde(default)]
    pub source_data_volume: Option<String>,
    pub db_snapshot_identifier: String,
    pub db_snapshot_arn: String,
    /// The ARN of the snapshot this one was copied from, when it is a
    /// copy. AWS reports it as `SourceDBSnapshotIdentifier`.
    ///
    /// A copy's own ARN names the COPIER's account and region, while the
    /// instance it records still belongs to the original owner -- so the
    /// source ARN is the only thing that says where that instance lives,
    /// and `db-instance-id` needs it to match a cross-account or
    /// cross-region copy by ARN.
    #[serde(default)]
    pub source_db_snapshot_arn: Option<String>,
    pub db_instance_identifier: String,
    pub snapshot_create_time: DateTime<Utc>,
    pub engine: String,
    pub engine_version: String,
    pub allocated_storage: i32,
    pub status: String,
    pub port: i32,
    pub master_username: String,
    pub db_name: Option<String>,
    pub dbi_resource_id: String,
    pub snapshot_type: String,
    pub master_user_password: String,
    pub tags: Vec<RdsTag>,
    pub dump_data: Vec<u8>,
    #[serde(default)]
    pub availability_zone: Option<String>,
    #[serde(default)]
    pub vpc_id: Option<String>,
    #[serde(default)]
    pub instance_create_time: Option<DateTime<Utc>>,
    #[serde(default)]
    pub license_model: Option<String>,
    #[serde(default)]
    pub iops: Option<i32>,
    #[serde(default)]
    pub option_group_name: Option<String>,
    #[serde(default)]
    pub percent_progress: Option<i32>,
    #[serde(default)]
    pub storage_type: Option<String>,
    #[serde(default)]
    pub encrypted: bool,
    #[serde(default)]
    pub kms_key_id: Option<String>,
    #[serde(default)]
    pub iam_database_authentication_enabled: bool,
    #[serde(default)]
    pub timezone: Option<String>,
    #[serde(default)]
    pub storage_throughput: Option<i32>,
    /// Snapshot share attributes keyed by attribute name (currently only
    /// `restore`), written by `ModifyDBSnapshotAttribute` and surfaced by
    /// `DescribeDBSnapshotAttributes`. The `restore` list holds the AWS
    /// account ids the snapshot is shared with (or the literal `all` for a
    /// public snapshot).
    #[serde(default)]
    pub snapshot_attributes: BTreeMap<String, Vec<String>>,
}

impl fmt::Debug for DbSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DbSnapshot")
            .field("db_snapshot_identifier", &self.db_snapshot_identifier)
            .field("db_snapshot_arn", &self.db_snapshot_arn)
            .field("db_instance_identifier", &self.db_instance_identifier)
            .field("snapshot_create_time", &self.snapshot_create_time)
            .field("engine", &self.engine)
            .field("engine_version", &self.engine_version)
            .field("allocated_storage", &self.allocated_storage)
            .field("status", &self.status)
            .field("port", &self.port)
            .field("master_username", &self.master_username)
            .field("db_name", &self.db_name)
            .field("dbi_resource_id", &self.dbi_resource_id)
            .field("snapshot_type", &self.snapshot_type)
            .field("master_user_password", &"<redacted>")
            .field("tags", &self.tags)
            .field("dump_data", &format!("<{} bytes>", self.dump_data.len()))
            .finish()
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RdsState {
    pub account_id: String,
    pub region: String,
    pub instances: BTreeMap<String, DbInstance>,
    pub in_progress_instance_ids: HashSet<String>,
    pub snapshots: BTreeMap<String, DbSnapshot>,
    pub subnet_groups: BTreeMap<String, DbSubnetGroup>,
    pub parameter_groups: BTreeMap<String, DbParameterGroup>,
    /// Generic stores keyed by category (clusters, cluster_snapshots,
    /// cluster_param_groups, proxies, proxy_endpoints, security_groups,
    /// option_groups, event_subscriptions, global_clusters, integrations,
    /// blue_green, shard_groups, custom_engine_versions, tenant_dbs,
    /// export_tasks, etc.) so the extras handlers can persist state
    /// without proliferating per-category fields.
    #[serde(default)]
    pub extras: BTreeMap<String, BTreeMap<String, serde_json::Value>>,
    /// In-memory ring of RDS events emitted by the service, used by
    /// `DescribeEvents`. Capped at the most recent ~14 days of events
    /// (matching real RDS retention) by [`Self::push_event`].
    #[serde(default)]
    pub events: Vec<RdsEventRecord>,
    /// Account-level default CA certificate identifier set by
    /// `ModifyCertificates`. Returned by `DescribeCertificates` so
    /// callers see their override on subsequent reads.
    #[serde(default)]
    pub default_certificate_identifier: Option<String>,
    /// DB capacity reservations keyed by their `dbcr-` identifier.
    #[serde(default)]
    pub capacity_reservations: BTreeMap<String, DbCapacityReservation>,
}

/// A DB capacity reservation: `CapacityPerAvailabilityZone` units of one
/// DB instance class + engine in each of its Availability Zones.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DbCapacityReservation {
    /// `dbcr-` + 32 lowercase alphanumerics.
    pub identifier: String,
    pub name: String,
    pub arn: String,
    pub db_instance_class: String,
    pub engine: String,
    #[serde(default)]
    pub license_model: Option<String>,
    pub capacity_per_az: i32,
    pub availability_zones: Vec<String>,
    /// Set only for a future-dated reservation.
    #[serde(default)]
    pub start_date: Option<DateTime<Utc>>,
    #[serde(default)]
    pub end_date: Option<DateTime<Utc>>,
    /// The capacity commitment of a future-dated reservation.
    #[serde(default)]
    pub committed_capacity_per_az: Option<i32>,
    #[serde(default)]
    pub commitment_end_date: Option<DateTime<Utc>>,
    #[serde(default)]
    pub cancelled: bool,
    pub create_date: DateTime<Utc>,
    #[serde(default)]
    pub tags: Vec<RdsTag>,
}

impl DbCapacityReservation {
    /// The reservation's `State` at `now`. Time-driven transitions
    /// (a future-dated reservation reaching its start date, any
    /// reservation passing its end date) are derived rather than stored.
    pub fn state_at(&self, now: DateTime<Utc>) -> &'static str {
        if self.cancelled {
            "cancelled"
        } else if self.end_date.is_some_and(|end| end <= now) {
            "expired"
        } else if self.start_date.is_some_and(|start| start > now) {
            "scheduled"
        } else {
            "active"
        }
    }

    /// Inside the capacity commitment of a future-dated reservation.
    pub fn in_commitment(&self, now: DateTime<Utc>) -> bool {
        self.commitment_end_date.is_some_and(|end| now < end)
    }
}

/// The reserved capacity a DB instance holds: the reservation and the
/// Availability Zones it holds one unit in each (the primary first; a
/// Multi-AZ instance holds two).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CapacityReservationUse {
    pub identifier: String,
    pub availability_zones: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RdsEventRecord {
    pub source_identifier: String,
    pub source_type: String,
    pub source_arn: String,
    pub event_id: String,
    pub event_categories: Vec<String>,
    pub message: String,
    pub date: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineVersionInfo {
    pub engine: String,
    pub engine_version: String,
    pub db_parameter_group_family: String,
    pub db_engine_description: String,
    pub db_engine_version_description: String,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderableDbInstanceOption {
    pub engine: String,
    pub engine_version: String,
    pub db_instance_class: String,
    pub license_model: String,
    pub storage_type: String,
    pub min_storage_size: i32,
    pub max_storage_size: i32,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DbSubnetGroup {
    pub db_subnet_group_name: String,
    pub db_subnet_group_arn: String,
    pub db_subnet_group_description: String,
    pub vpc_id: String,
    pub subnet_ids: Vec<String>,
    pub subnet_availability_zones: Vec<String>,
    pub tags: Vec<RdsTag>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DbParameterGroup {
    pub db_parameter_group_name: String,
    pub db_parameter_group_arn: String,
    pub db_parameter_group_family: String,
    pub description: String,
    pub parameters: BTreeMap<String, String>,
    /// Per-parameter `ApplyMethod` (`immediate` | `pending-reboot`),
    /// keyed by parameter name. Defaulted for snapshots written before
    /// this field existed; a missing entry reads back as `immediate`.
    #[serde(default)]
    pub parameter_apply_methods: BTreeMap<String, String>,
    pub tags: Vec<RdsTag>,
}

/// Static metadata for an engine-default parameter, used by
/// `DescribeDBParameters`/`DescribeDBClusterParameters`/`DescribeEngineDefaultParameters`
/// to surface a baseline set of parameters when no user override exists.
///
/// The seed below is intentionally small (a handful of common knobs per
/// engine family). Real RDS exposes hundreds of parameters per family;
/// callers needing comprehensive coverage should add entries to
/// [`engine_default_parameters`] as needs arise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineDefaultParameter {
    pub name: &'static str,
    pub value: &'static str,
    pub apply_type: &'static str,
    pub data_type: &'static str,
    pub allowed_values: &'static str,
    pub is_modifiable: bool,
}

/// Return a small, representative set of engine-default parameters for the
/// given parameter group family (e.g. `postgres16`, `mysql8.0`,
/// `aurora-postgresql15`). The list is not comprehensive — real RDS
/// exposes hundreds of parameters; we ship just enough to make callers
/// that round-trip `DescribeDBParameters` with `Source=engine-default`
/// see meaningful entries. Unknown families fall through to an empty list.
pub fn engine_default_parameters(family: &str) -> &'static [EngineDefaultParameter] {
    if family.starts_with("postgres") || family.starts_with("aurora-postgresql") {
        POSTGRES_DEFAULT_PARAMETERS
    } else if family.starts_with("mysql") || family.starts_with("aurora-mysql") {
        MYSQL_DEFAULT_PARAMETERS
    } else if family.starts_with("mariadb") {
        MARIADB_DEFAULT_PARAMETERS
    } else {
        &[]
    }
}

const POSTGRES_DEFAULT_PARAMETERS: &[EngineDefaultParameter] = &[
    EngineDefaultParameter {
        name: "max_connections",
        value: "LEAST({DBInstanceClassMemory/9531392},5000)",
        apply_type: "static",
        data_type: "integer",
        allowed_values: "6-8388607",
        is_modifiable: true,
    },
    EngineDefaultParameter {
        name: "shared_buffers",
        value: "{DBInstanceClassMemory/32768}",
        apply_type: "static",
        data_type: "integer",
        allowed_values: "16-1073741823",
        is_modifiable: true,
    },
    EngineDefaultParameter {
        name: "work_mem",
        value: "4096",
        apply_type: "dynamic",
        data_type: "integer",
        allowed_values: "64-2147483647",
        is_modifiable: true,
    },
    EngineDefaultParameter {
        name: "maintenance_work_mem",
        value: "GREATEST({DBInstanceClassMemory/63963136*1024},65536)",
        apply_type: "dynamic",
        data_type: "integer",
        allowed_values: "1024-2147483647",
        is_modifiable: true,
    },
    EngineDefaultParameter {
        name: "effective_cache_size",
        value: "{DBInstanceClassMemory/16384}",
        apply_type: "dynamic",
        data_type: "integer",
        allowed_values: "1-2147483647",
        is_modifiable: true,
    },
];

const MYSQL_DEFAULT_PARAMETERS: &[EngineDefaultParameter] = &[
    EngineDefaultParameter {
        name: "max_connections",
        value: "{DBInstanceClassMemory/12582880}",
        apply_type: "dynamic",
        data_type: "integer",
        allowed_values: "1-100000",
        is_modifiable: true,
    },
    EngineDefaultParameter {
        name: "innodb_buffer_pool_size",
        value: "{DBInstanceClassMemory*3/4}",
        apply_type: "static",
        data_type: "integer",
        allowed_values: "5242880-2147483648",
        is_modifiable: true,
    },
    EngineDefaultParameter {
        name: "max_allowed_packet",
        value: "67108864",
        apply_type: "dynamic",
        data_type: "integer",
        allowed_values: "1024-1073741824",
        is_modifiable: true,
    },
    EngineDefaultParameter {
        name: "character_set_server",
        value: "utf8mb4",
        apply_type: "dynamic",
        data_type: "string",
        allowed_values: "utf8,utf8mb4,latin1",
        is_modifiable: true,
    },
];

const MARIADB_DEFAULT_PARAMETERS: &[EngineDefaultParameter] = &[
    EngineDefaultParameter {
        name: "max_connections",
        value: "{DBInstanceClassMemory/12582880}",
        apply_type: "dynamic",
        data_type: "integer",
        allowed_values: "1-100000",
        is_modifiable: true,
    },
    EngineDefaultParameter {
        name: "innodb_buffer_pool_size",
        value: "{DBInstanceClassMemory*3/4}",
        apply_type: "static",
        data_type: "integer",
        allowed_values: "5242880-2147483648",
        is_modifiable: true,
    },
    EngineDefaultParameter {
        name: "max_allowed_packet",
        value: "67108864",
        apply_type: "dynamic",
        data_type: "integer",
        allowed_values: "1024-1073741824",
        is_modifiable: true,
    },
];

impl DbInstance {
    /// The data volume this instance mounts: an adopted legacy volume, or the
    /// one scoped to the data dir and keyed by the immutable `DbiResourceId`
    /// (not the identifier), so a `NewDBInstanceIdentifier` rename keeps its
    /// data and a new instance reusing the old identifier never inherits it.
    pub fn data_volume_name(&self, scope_tag: &str, account_id: &str) -> String {
        match &self.data_volume {
            Some(DataVolumeBinding::Legacy(name)) => name.clone(),
            _ => crate::runtime::scoped_data_volume_name(
                scope_tag,
                account_id,
                &self.dbi_resource_id,
            ),
        }
    }
}

impl RdsState {
    /// The instance row of incarnation `dbi_resource_id` (its immutable
    /// `DbiResourceId`), whatever identifier it goes by now. A background
    /// start task finds its instance this way: by identifier it could hit a
    /// replacement created after a delete, or miss its own after a rename.
    pub fn instance_by_incarnation_mut(
        &mut self,
        dbi_resource_id: &str,
    ) -> Option<&mut DbInstance> {
        self.instances
            .values_mut()
            .find(|i| i.dbi_resource_id == dbi_resource_id)
    }

    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            instances: BTreeMap::new(),
            in_progress_instance_ids: HashSet::new(),
            snapshots: BTreeMap::new(),
            subnet_groups: BTreeMap::new(),
            parameter_groups: default_parameter_groups(account_id, region),
            extras: BTreeMap::new(),
            events: Vec::new(),
            default_certificate_identifier: None,
            capacity_reservations: BTreeMap::new(),
        }
    }

    /// Fix up state loaded from an older persistence snapshot, given the
    /// schema version the file declared.
    ///
    /// Final snapshots (`FinalDBSnapshotIdentifier` on DeleteDBInstance)
    /// were recorded as `automated` up to schema v2; AWS types them
    /// `manual`, because they outlive the instance, unlike automated
    /// backups. Left alone such a row would silently disappear from
    /// `DescribeDBSnapshots --snapshot-type manual` now that SnapshotType
    /// actually narrows the result.
    ///
    /// Version-gated on purpose: this rewrite is only sound while nothing
    /// produces genuine `automated` snapshots. Once automated backups
    /// become real they are written at a newer schema version and are
    /// left untouched here.
    pub fn migrate_loaded(&mut self, from_schema_version: u32) {
        if from_schema_version > RDS_FINAL_SNAPSHOT_AUTOMATED_SCHEMA {
            return;
        }
        for snapshot in self.snapshots.values_mut() {
            if snapshot.snapshot_type == "automated" {
                snapshot.snapshot_type = "manual".to_string();
            }
        }
    }

    /// Bind every instance persisted without a data-volume binding (state
    /// written before volumes were scoped to the data dir) against the
    /// daemon's volumes (`existing`): its scoped volume if that exists, else
    /// the legacy (unscoped) volume its identifier named, else a new scoped
    /// one. Instances created by this build are bound at creation, so a
    /// fresh data dir never adopts another one's legacy volume (#2630).
    /// Returns whether any instance was bound.
    pub fn resolve_data_volumes(&mut self, scope_tag: &str, existing: &HashSet<String>) -> bool {
        let mut changed = false;
        for (id, inst) in self.instances.iter_mut() {
            if inst.data_volume.is_none() {
                inst.data_volume = Some(fakecloud_core::data_volume::resolve_binding(
                    &crate::runtime::scoped_data_volume_name(
                        scope_tag,
                        &self.account_id,
                        &inst.dbi_resource_id,
                    ),
                    &crate::runtime::legacy_data_volume_name(&self.account_id, id),
                    existing,
                ));
                changed = true;
            }
        }
        changed
    }

    pub fn reset(&mut self) {
        self.instances.clear();
        self.in_progress_instance_ids.clear();
        self.snapshots.clear();
        self.subnet_groups.clear();
        self.parameter_groups = default_parameter_groups(&self.account_id, &self.region);
        self.extras.clear();
        self.events.clear();
        self.default_certificate_identifier = None;
        self.capacity_reservations.clear();
    }

    /// Append an event row to the in-memory ring, dropping the oldest
    /// entries beyond a 14-day window (matching real RDS retention).
    pub fn push_event(&mut self, event: RdsEventRecord) {
        const RETENTION_DAYS: i64 = 14;
        let cutoff = chrono::Utc::now() - chrono::Duration::days(RETENTION_DAYS);
        self.events.retain(|e| e.date >= cutoff);
        self.events.push(event);
    }

    // ARN carries the request's credential-scope region (req.region), not the
    // frozen server default. Storage keying is by account/identifier, unchanged.
    pub fn db_instance_arn(&self, region: &str, db_instance_identifier: &str) -> String {
        rds_arn(region, &self.account_id, "db", db_instance_identifier)
    }

    pub fn db_snapshot_arn(&self, region: &str, db_snapshot_identifier: &str) -> String {
        rds_arn(region, &self.account_id, "snapshot", db_snapshot_identifier)
    }

    pub fn db_subnet_group_arn(&self, region: &str, db_subnet_group_name: &str) -> String {
        rds_arn(region, &self.account_id, "subgrp", db_subnet_group_name)
    }

    pub fn db_parameter_group_arn(&self, region: &str, db_parameter_group_name: &str) -> String {
        rds_arn(region, &self.account_id, "pg", db_parameter_group_name)
    }

    pub fn next_dbi_resource_id(&self) -> String {
        format!("db-{}", Uuid::new_v4().simple())
    }

    pub fn begin_instance_creation(&mut self, db_instance_identifier: &str) -> bool {
        if self.instances.contains_key(db_instance_identifier)
            || self
                .in_progress_instance_ids
                .contains(db_instance_identifier)
        {
            return false;
        }

        self.in_progress_instance_ids
            .insert(db_instance_identifier.to_string());
        true
    }

    pub fn finish_instance_creation(&mut self, instance: DbInstance) {
        self.in_progress_instance_ids
            .remove(&instance.db_instance_identifier);
        self.instances
            .insert(instance.db_instance_identifier.clone(), instance);
    }

    pub fn cancel_instance_creation(&mut self, db_instance_identifier: &str) {
        self.in_progress_instance_ids.remove(db_instance_identifier);
    }
}

pub fn default_engine_versions() -> Vec<EngineVersionInfo> {
    let mut versions = vec![
        // PostgreSQL versions. The first entry per engine is what
        // `DescribeDBEngineVersions` with `DefaultOnly=true` returns (and what
        // Terraform's `aws_rds_engine_version` data source resolves to), so the
        // established GA default (17.x / 8.0.x) stays first; newer majors that
        // AWS also accepts (18, 8.4) are appended as non-default options.
        EngineVersionInfo {
            engine: "postgres".to_string(),
            engine_version: "17.4".to_string(),
            db_parameter_group_family: "postgres17".to_string(),
            db_engine_description: "PostgreSQL".to_string(),
            db_engine_version_description: "PostgreSQL 17.4".to_string(),
            status: "available".to_string(),
        },
        EngineVersionInfo {
            engine: "postgres".to_string(),
            engine_version: "16.3".to_string(),
            db_parameter_group_family: "postgres16".to_string(),
            db_engine_description: "PostgreSQL".to_string(),
            db_engine_version_description: "PostgreSQL 16.3".to_string(),
            status: "available".to_string(),
        },
        EngineVersionInfo {
            engine: "postgres".to_string(),
            engine_version: "15.5".to_string(),
            db_parameter_group_family: "postgres15".to_string(),
            db_engine_description: "PostgreSQL".to_string(),
            db_engine_version_description: "PostgreSQL 15.5".to_string(),
            status: "available".to_string(),
        },
        EngineVersionInfo {
            engine: "postgres".to_string(),
            engine_version: "14.10".to_string(),
            db_parameter_group_family: "postgres14".to_string(),
            db_engine_description: "PostgreSQL".to_string(),
            db_engine_version_description: "PostgreSQL 14.10".to_string(),
            status: "available".to_string(),
        },
        EngineVersionInfo {
            engine: "postgres".to_string(),
            engine_version: "13.13".to_string(),
            db_parameter_group_family: "postgres13".to_string(),
            db_engine_description: "PostgreSQL".to_string(),
            db_engine_version_description: "PostgreSQL 13.13".to_string(),
            status: "available".to_string(),
        },
        EngineVersionInfo {
            engine: "postgres".to_string(),
            engine_version: "18.0".to_string(),
            db_parameter_group_family: "postgres18".to_string(),
            db_engine_description: "PostgreSQL".to_string(),
            db_engine_version_description: "PostgreSQL 18.0".to_string(),
            status: "available".to_string(),
        },
        // MySQL versions
        EngineVersionInfo {
            engine: "mysql".to_string(),
            engine_version: "8.0.35".to_string(),
            db_parameter_group_family: "mysql8.0".to_string(),
            db_engine_description: "MySQL Community Edition".to_string(),
            db_engine_version_description: "MySQL 8.0.35".to_string(),
            status: "available".to_string(),
        },
        EngineVersionInfo {
            engine: "mysql".to_string(),
            engine_version: "8.0.28".to_string(),
            db_parameter_group_family: "mysql8.0".to_string(),
            db_engine_description: "MySQL Community Edition".to_string(),
            db_engine_version_description: "MySQL 8.0.28".to_string(),
            status: "available".to_string(),
        },
        EngineVersionInfo {
            engine: "mysql".to_string(),
            engine_version: "5.7.44".to_string(),
            db_parameter_group_family: "mysql5.7".to_string(),
            db_engine_description: "MySQL Community Edition".to_string(),
            db_engine_version_description: "MySQL 5.7.44".to_string(),
            status: "available".to_string(),
        },
        EngineVersionInfo {
            engine: "mysql".to_string(),
            engine_version: "8.4.0".to_string(),
            db_parameter_group_family: "mysql8.4".to_string(),
            db_engine_description: "MySQL Community Edition".to_string(),
            db_engine_version_description: "MySQL 8.4.0".to_string(),
            status: "available".to_string(),
        },
        // MariaDB versions
        EngineVersionInfo {
            engine: "mariadb".to_string(),
            engine_version: "11.4.5".to_string(),
            db_parameter_group_family: "mariadb11.4".to_string(),
            db_engine_description: "MariaDB Community Edition".to_string(),
            db_engine_version_description: "MariaDB 11.4.5".to_string(),
            status: "available".to_string(),
        },
        EngineVersionInfo {
            engine: "mariadb".to_string(),
            engine_version: "10.11.6".to_string(),
            db_parameter_group_family: "mariadb10.11".to_string(),
            db_engine_description: "MariaDB Community Edition".to_string(),
            db_engine_version_description: "MariaDB 10.11.6".to_string(),
            status: "available".to_string(),
        },
        EngineVersionInfo {
            engine: "mariadb".to_string(),
            engine_version: "10.6.16".to_string(),
            db_parameter_group_family: "mariadb10.6".to_string(),
            db_engine_description: "MariaDB Community Edition".to_string(),
            db_engine_version_description: "MariaDB 10.6.16".to_string(),
            status: "available".to_string(),
        },
    ];
    // Versions appended after the per-engine defaults above, so `DefaultOnly`
    // keeps resolving to the established release: AWS's build-suffixed MySQL
    // (extended support), MariaDB 11.8, and the commercial engines in the
    // version grammar RDS really reports for them.
    let appended: &[(&str, &str, &str, &str, &str)] = &[
        (
            "mysql",
            "5.7.44-rds.20250103",
            "mysql5.7",
            "MySQL Community Edition",
            "MySQL 5.7.44-rds.20250103",
        ),
        (
            "mariadb",
            "11.8.3",
            "mariadb11.8",
            "MariaDB Community Edition",
            "MariaDB 11.8.3",
        ),
    ];
    for (engine, version, family, description, version_description) in appended {
        versions.push(engine_version_info(
            engine,
            version,
            family,
            description,
            version_description,
        ));
    }
    const ORACLE_19: &str = "19.0.0.0.ru-2025-07.rur-2025-07.r1";
    const ORACLE_21: &str = "21.0.0.0.ru-2025-07.rur-2025-07.r1";
    for (engine, description, majors) in [
        (
            "oracle-ee",
            "Oracle Database Enterprise Edition",
            &[("19", ORACLE_19)][..],
        ),
        (
            "oracle-se2",
            "Oracle Database Standard Edition Two",
            &[("19", ORACLE_19)][..],
        ),
        (
            "oracle-ee-cdb",
            "Oracle Database Enterprise Edition (CDB)",
            &[("19", ORACLE_19), ("21", ORACLE_21)][..],
        ),
        (
            "oracle-se2-cdb",
            "Oracle Database Standard Edition Two (CDB)",
            &[("19", ORACLE_19), ("21", ORACLE_21)][..],
        ),
    ] {
        for (major, version) in majors {
            versions.push(engine_version_info(
                engine,
                version,
                &format!("{engine}-{major}"),
                description,
                &format!("Oracle {version}"),
            ));
        }
    }
    for (engine, description) in [
        ("sqlserver-ee", "Microsoft SQL Server Enterprise Edition"),
        ("sqlserver-se", "Microsoft SQL Server Standard Edition"),
        ("sqlserver-ex", "Microsoft SQL Server Express Edition"),
        ("sqlserver-web", "Microsoft SQL Server Web Edition"),
    ] {
        for (major, version, release) in [
            ("16", "16.00.4195.2.v1", "SQL Server 2022"),
            ("15", "15.00.4430.1.v1", "SQL Server 2019"),
        ] {
            versions.push(engine_version_info(
                engine,
                version,
                &format!("{engine}-{major}"),
                description,
                &format!("{release} {version}"),
            ));
        }
    }
    for (engine, description) in [
        ("db2-se", "IBM Db2 Standard Edition"),
        ("db2-ae", "IBM Db2 Advanced Edition"),
    ] {
        versions.push(engine_version_info(
            engine,
            "11.5.9.0.sb00000000.r1",
            &format!("{engine}-11.5"),
            description,
            "Db2 11.5.9.0.sb00000000.r1",
        ));
    }
    versions
}

fn engine_version_info(
    engine: &str,
    version: &str,
    family: &str,
    description: &str,
    version_description: &str,
) -> EngineVersionInfo {
    EngineVersionInfo {
        engine: engine.to_string(),
        engine_version: version.to_string(),
        db_parameter_group_family: family.to_string(),
        db_engine_description: description.to_string(),
        db_engine_version_description: version_description.to_string(),
        status: "available".to_string(),
    }
}

pub fn default_orderable_options() -> Vec<OrderableDbInstanceOption> {
    // One option per engine version and class, in `default_engine_versions`
    // order, so the default (first-per-engine) version stays the established
    // GA release for `DefaultOnly` / `aws_rds_orderable_db_instance`.
    let mut options = Vec::new();
    for version in default_engine_versions() {
        for class in SUPPORTED_INSTANCE_CLASSES {
            options.push(OrderableDbInstanceOption {
                engine: version.engine.clone(),
                engine_version: version.engine_version.clone(),
                db_instance_class: class.to_string(),
                license_model: crate::service::service_helpers::license_model_for_engine(
                    &version.engine,
                )
                .to_string(),
                storage_type: "gp2".to_string(),
                min_storage_size: 20,
                max_storage_size: 16384,
            });
        }
    }
    options
}

pub fn default_parameter_groups(
    account_id: &str,
    region: &str,
) -> BTreeMap<String, DbParameterGroup> {
    let mut groups = BTreeMap::new();

    let families = vec![
        ("postgres18", "Default parameter group for postgres18"),
        ("postgres17", "Default parameter group for postgres17"),
        ("postgres16", "Default parameter group for postgres16"),
        ("postgres15", "Default parameter group for postgres15"),
        ("postgres14", "Default parameter group for postgres14"),
        ("postgres13", "Default parameter group for postgres13"),
        ("mysql8.4", "Default parameter group for mysql8.4"),
        ("mysql8.0", "Default parameter group for mysql8.0"),
        ("mysql5.7", "Default parameter group for mysql5.7"),
        ("mariadb11.8", "Default parameter group for mariadb11.8"),
        ("mariadb11.4", "Default parameter group for mariadb11.4"),
        ("mariadb10.11", "Default parameter group for mariadb10.11"),
        ("mariadb10.6", "Default parameter group for mariadb10.6"),
        // Heavy-engine families. The names match what
        // `service::default_parameter_group` returns so callers that
        // omit `DBParameterGroupName` get a hit instead of a
        // `DBParameterGroupNotFound`.
        ("oracle-ee-23", "Default parameter group for oracle-ee-23"),
        ("oracle-ee-21", "Default parameter group for oracle-ee-21"),
        ("oracle-ee-19", "Default parameter group for oracle-ee-19"),
        ("oracle-se2-23", "Default parameter group for oracle-se2-23"),
        ("oracle-se2-21", "Default parameter group for oracle-se2-21"),
        ("oracle-se2-19", "Default parameter group for oracle-se2-19"),
        (
            "oracle-ee-cdb-23",
            "Default parameter group for oracle-ee-cdb-23",
        ),
        (
            "oracle-se2-cdb-23",
            "Default parameter group for oracle-se2-cdb-23",
        ),
        (
            "oracle-ee-cdb-21",
            "Default parameter group for oracle-ee-cdb-21",
        ),
        (
            "oracle-ee-cdb-19",
            "Default parameter group for oracle-ee-cdb-19",
        ),
        (
            "oracle-se2-cdb-21",
            "Default parameter group for oracle-se2-cdb-21",
        ),
        (
            "oracle-se2-cdb-19",
            "Default parameter group for oracle-se2-cdb-19",
        ),
        // Aurora instances take their family's default DB parameter group.
        (
            "aurora-postgresql17",
            "Default parameter group for aurora-postgresql17",
        ),
        (
            "aurora-postgresql16",
            "Default parameter group for aurora-postgresql16",
        ),
        (
            "aurora-postgresql15",
            "Default parameter group for aurora-postgresql15",
        ),
        (
            "aurora-postgresql14",
            "Default parameter group for aurora-postgresql14",
        ),
        (
            "aurora-postgresql13",
            "Default parameter group for aurora-postgresql13",
        ),
        (
            "aurora-mysql8.0",
            "Default parameter group for aurora-mysql8.0",
        ),
        (
            "aurora-mysql5.7",
            "Default parameter group for aurora-mysql5.7",
        ),
        (
            "sqlserver-ee-16",
            "Default parameter group for sqlserver-ee-16",
        ),
        (
            "sqlserver-ee-15",
            "Default parameter group for sqlserver-ee-15",
        ),
        (
            "sqlserver-se-16",
            "Default parameter group for sqlserver-se-16",
        ),
        (
            "sqlserver-se-15",
            "Default parameter group for sqlserver-se-15",
        ),
        (
            "sqlserver-ex-16",
            "Default parameter group for sqlserver-ex-16",
        ),
        (
            "sqlserver-ex-15",
            "Default parameter group for sqlserver-ex-15",
        ),
        (
            "sqlserver-web-16",
            "Default parameter group for sqlserver-web-16",
        ),
        (
            "sqlserver-web-15",
            "Default parameter group for sqlserver-web-15",
        ),
        ("db2-se-11.5", "Default parameter group for db2-se-11.5"),
        ("db2-ae-11.5", "Default parameter group for db2-ae-11.5"),
    ];

    for (family, description) in families {
        let group_name = format!("default.{}", family);
        let group = DbParameterGroup {
            db_parameter_group_name: group_name.clone(),
            db_parameter_group_arn: rds_arn(region, account_id, "pg", &group_name),
            db_parameter_group_family: family.to_string(),
            description: description.to_string(),
            parameters: BTreeMap::new(),
            parameter_apply_methods: BTreeMap::new(),
            tags: Vec::new(),
        };
        groups.insert(group_name, group);
    }

    groups
}

/// v3 retyped final snapshots from `automated` to `manual`; a v2 file
/// still carries the old type and is migrated on load.
pub const RDS_SNAPSHOT_SCHEMA_VERSION: u32 = 3;

/// Last schema version whose final snapshots were persisted as
/// `automated`. Files at or below this need [`RdsState::migrate_loaded`].
pub const RDS_FINAL_SNAPSHOT_AUTOMATED_SCHEMA: u32 = 2;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RdsSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<fakecloud_core::multi_account::MultiAccountState<RdsState>>,
    #[serde(default)]
    pub state: Option<RdsState>,
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::{
        default_engine_versions, default_orderable_options, default_parameter_groups, Arn,
        DataVolumeBinding, DbInstance, RdsState, SUPPORTED_INSTANCE_CLASSES,
    };

    #[test]
    fn new_initializes_account_and_region() {
        let state = RdsState::new("123456789012", "us-east-1");

        assert_eq!(state.account_id, "123456789012");
        assert_eq!(state.region, "us-east-1");
        assert!(state.instances.is_empty());
        assert!(state.in_progress_instance_ids.is_empty());
    }

    #[test]
    fn reset_clears_instances() {
        let mut state = RdsState::new("123456789012", "us-east-1");
        let created_at = Utc::now();
        state.instances.insert(
            "db-1".to_string(),
            DbInstance {
                db_instance_identifier: "db-1".to_string(),
                db_instance_arn: "arn:aws:rds:us-east-1:123456789012:db:db-1".to_string(),
                associated_roles: Vec::new(),
                db_instance_class: "db.t3.micro".to_string(),
                engine: "postgres".to_string(),
                engine_version: "16.3".to_string(),
                db_instance_status: "available".to_string(),
                master_username: "admin".to_string(),
                db_name: Some("postgres".to_string()),
                endpoint_address: "127.0.0.1".to_string(),
                port: 5432,
                allocated_storage: 20,
                publicly_accessible: true,
                deletion_protection: false,
                created_at,
                dbi_resource_id: "db-test".to_string(),
                master_user_password: "secret123".to_string(),
                container_id: "container-id".to_string(),
                host_port: 15432,
                data_volume: None,
                tags: Vec::new(),
                read_replica_source_db_instance_identifier: None,
                read_replica_db_instance_identifiers: Vec::new(),
                vpc_security_group_ids: Vec::new(),
                db_parameter_group_name: None,
                backup_retention_period: 1,
                preferred_backup_window: "03:00-04:00".to_string(),
                preferred_maintenance_window: None,
                latest_restorable_time: Some(created_at),
                option_group_name: None,
                multi_az: false,
                pending_modified_values: None,
                db_subnet_group_name: None,
                availability_zone: None,
                storage_type: None,
                storage_encrypted: false,
                kms_key_id: None,
                iam_database_authentication_enabled: false,
                iops: None,
                monitoring_interval: None,
                monitoring_role_arn: None,
                performance_insights_enabled: false,
                performance_insights_kms_key_id: None,
                performance_insights_retention_period: None,
                enabled_cloudwatch_logs_exports: Vec::new(),
                ca_certificate_identifier: None,
                network_type: None,
                character_set_name: None,
                auto_minor_version_upgrade: None,
                copy_tags_to_snapshot: None,
                master_user_secret_arn: None,
                master_user_secret_kms_key_id: None,
                license_model: None,
                max_allocated_storage: None,
                multi_tenant: None,
                storage_throughput: None,
                tde_credential_arn: None,
                delete_automated_backups: None,
                db_security_groups: Vec::new(),
                domain: None,
                domain_fqdn: None,
                domain_ou: None,
                domain_iam_role_name: None,
                domain_auth_secret_arn: None,
                domain_dns_ips: Vec::new(),
                db_cluster_identifier: None,
                activity_stream: None,
                capacity_reservation: None,
            },
        );

        state.reset();

        assert!(state.instances.is_empty());
        assert!(state.in_progress_instance_ids.is_empty());
    }

    #[test]
    fn default_engine_versions_are_postgres_metadata() {
        let versions = default_engine_versions();

        // 6 postgres + 5 mysql + 4 mariadb + 6 oracle + 8 sqlserver + 2 db2
        assert_eq!(versions.len(), 31);
        // The first postgres entry is the DefaultOnly/default version (17.4);
        // 18.0 is present but appended as a non-default option.
        assert_eq!(versions[0].engine, "postgres");
        assert_eq!(versions[0].engine_version, "17.4");
        assert_eq!(versions[0].db_parameter_group_family, "postgres17");
        assert!(versions
            .iter()
            .any(|v| v.engine == "postgres" && v.engine_version == "18.0"));
        // The first mysql entry is the default (8.0.35); 8.4.0 is appended.
        let first_mysql = versions.iter().find(|v| v.engine == "mysql").unwrap();
        assert_eq!(first_mysql.engine_version, "8.0.35");
        assert!(versions
            .iter()
            .any(|v| v.engine == "mysql" && v.engine_version == "8.4.0"));
    }

    #[test]
    fn default_orderable_options_match_engine_versions() {
        let versions = default_engine_versions();
        let options = default_orderable_options();

        // Every engine version * every representative instance class.
        assert_eq!(
            options.len(),
            versions.len() * SUPPORTED_INSTANCE_CLASSES.len()
        );
        // Verify all engines and versions have orderable options
        for version in &versions {
            assert!(options.iter().any(|opt| {
                opt.engine == version.engine && opt.engine_version == version.engine_version
            }));
        }
    }

    #[test]
    fn begin_instance_creation_rejects_duplicate_identifiers() {
        let mut state = RdsState::new("123456789012", "us-east-1");

        assert!(state.begin_instance_creation("db-1"));
        assert!(!state.begin_instance_creation("db-1"));

        state.cancel_instance_creation("db-1");
        assert!(state.begin_instance_creation("db-1"));
    }

    #[test]
    fn arn_helpers_format_correctly() {
        let state = RdsState::new("123456789012", "eu-west-1");
        assert!(state
            .db_instance_arn(&state.region, "mydb")
            .contains(":db:mydb"));
        assert!(state
            .db_snapshot_arn(&state.region, "snap1")
            .contains(":snapshot:snap1"));
        assert!(state
            .db_subnet_group_arn(&state.region, "sng")
            .contains("sng"));
        assert!(state
            .db_parameter_group_arn(&state.region, "pg")
            .contains("pg"));
    }

    #[test]
    fn next_dbi_resource_id_format() {
        let state = RdsState::new("123456789012", "us-east-1");
        let id = state.next_dbi_resource_id();
        assert!(id.starts_with("db-"));
        assert!(id.len() > 3);
    }

    #[test]
    fn default_engine_versions_list_not_empty() {
        let versions = default_engine_versions();
        assert!(!versions.is_empty());
    }

    #[test]
    fn default_orderable_options_list_not_empty() {
        let opts = default_orderable_options();
        assert!(!opts.is_empty());
    }

    #[test]
    fn default_parameter_groups_returned_per_family() {
        let groups = default_parameter_groups("123456789012", "us-east-1");
        assert!(!groups.is_empty());
    }

    fn make_instance(id: &str) -> DbInstance {
        let created_at = Utc::now();
        DbInstance {
            db_instance_identifier: id.to_string(),
            db_instance_arn: Arn::new("rds", "us-east-1", "123", &format!("db:{id}")).to_string(),
            associated_roles: Vec::new(),
            db_instance_class: "db.t3.micro".to_string(),
            engine: "postgres".to_string(),
            engine_version: "16.3".to_string(),
            db_instance_status: "available".to_string(),
            master_username: "admin".to_string(),
            db_name: None,
            endpoint_address: "x".to_string(),
            port: 5432,
            allocated_storage: 20,
            publicly_accessible: false,
            deletion_protection: false,
            created_at,
            dbi_resource_id: "d".to_string(),
            master_user_password: "p".to_string(),
            container_id: "c".to_string(),
            host_port: 0,
            data_volume: None,
            tags: Vec::new(),
            read_replica_source_db_instance_identifier: None,
            read_replica_db_instance_identifiers: Vec::new(),
            vpc_security_group_ids: Vec::new(),
            db_parameter_group_name: None,
            backup_retention_period: 0,
            preferred_backup_window: String::new(),
            preferred_maintenance_window: None,
            latest_restorable_time: None,
            option_group_name: None,
            multi_az: false,
            pending_modified_values: None,
            db_subnet_group_name: None,
            availability_zone: None,
            storage_type: None,
            storage_encrypted: false,
            kms_key_id: None,
            iam_database_authentication_enabled: false,
            iops: None,
            monitoring_interval: None,
            monitoring_role_arn: None,
            performance_insights_enabled: false,
            performance_insights_kms_key_id: None,
            performance_insights_retention_period: None,
            enabled_cloudwatch_logs_exports: Vec::new(),
            ca_certificate_identifier: None,
            network_type: None,
            character_set_name: None,
            auto_minor_version_upgrade: None,
            copy_tags_to_snapshot: None,
            master_user_secret_arn: None,
            master_user_secret_kms_key_id: None,
            license_model: None,
            max_allocated_storage: None,
            multi_tenant: None,
            storage_throughput: None,
            tde_credential_arn: None,
            delete_automated_backups: None,
            db_security_groups: Vec::new(),
            domain: None,
            domain_fqdn: None,
            domain_ou: None,
            domain_iam_role_name: None,
            domain_auth_secret_arn: None,
            domain_dns_ips: Vec::new(),
            db_cluster_identifier: None,
            activity_stream: None,
            capacity_reservation: None,
        }
    }

    /// A start task finds its instance by incarnation: after a rename it
    /// still finds it, and after a delete + recreate under the same
    /// identifier it finds nothing (and so reaps its own container).
    #[test]
    fn instance_by_incarnation_follows_renames_not_identifiers() {
        let mut state = RdsState::new("123", "us-east-1");
        let mut old = make_instance("db");
        old.dbi_resource_id = "db-OLD".to_string();
        state.instances.insert("db".to_string(), old);
        // Rename db -> db2: same incarnation.
        let mut moved = state.instances.remove("db").unwrap();
        moved.db_instance_identifier = "db2".to_string();
        state.instances.insert("db2".to_string(), moved);
        assert_eq!(
            state
                .instance_by_incarnation_mut("db-OLD")
                .map(|i| i.db_instance_identifier.clone()),
            Some("db2".to_string())
        );
        // Delete db2 and recreate under the old identifier: a new incarnation.
        state.instances.remove("db2");
        let mut new = make_instance("db");
        new.dbi_resource_id = "db-NEW".to_string();
        state.instances.insert("db".to_string(), new);
        assert!(state.instance_by_incarnation_mut("db-OLD").is_none());
        assert!(state.instance_by_incarnation_mut("db-NEW").is_some());
    }

    #[test]
    fn unbound_instances_resolve_scoped_first_then_legacy() {
        let scoped =
            |id: &str| crate::runtime::scoped_data_volume_name("dtag", "123", &format!("dbi-{id}"));
        let mut existing = std::collections::HashSet::new();
        existing.insert("fakecloud-rds-data-123-old-db".to_string());
        existing.insert("fakecloud-rds-data-123-used-db".to_string());
        existing.insert(scoped("used-db"));
        existing.insert("fakecloud-rds-data-123-new-db".to_string());

        let mut state = RdsState::new("123", "us-east-1");
        let with_dbi = |id: &str| {
            let mut inst = make_instance(id);
            inst.dbi_resource_id = format!("dbi-{id}");
            inst
        };
        for id in ["old-db", "no-vol", "used-db"] {
            state.instances.insert(id.to_string(), with_dbi(id));
        }
        // Created by this build: bound at creation, never re-resolved.
        let mut new_db = with_dbi("new-db");
        new_db.data_volume = Some(DataVolumeBinding::Scoped);
        state.instances.insert("new-db".to_string(), new_db);

        // State written before the field existed deserializes unbound.
        let mut json = serde_json::to_value(&state).unwrap();
        json["instances"]["old-db"]
            .as_object_mut()
            .unwrap()
            .remove("data_volume");
        let mut loaded: RdsState = serde_json::from_value(json).unwrap();
        assert_eq!(loaded.instances["old-db"].data_volume, None);

        assert!(loaded.resolve_data_volumes("dtag", &existing));
        // Its legacy volume exists: kept.
        assert_eq!(
            loaded.instances["old-db"].data_volume,
            Some(DataVolumeBinding::Legacy(
                "fakecloud-rds-data-123-old-db".to_string()
            ))
        );
        // Nothing to keep: scoped.
        assert_eq!(
            loaded.instances["no-vol"].data_volume,
            Some(DataVolumeBinding::Scoped)
        );
        // Already writing to its scoped volume: stays there.
        assert_eq!(
            loaded.instances["used-db"].data_volume,
            Some(DataVolumeBinding::Scoped)
        );
        // Bound at creation: a same-named legacy volume is ignored.
        assert_eq!(
            loaded.instances["new-db"].data_volume,
            Some(DataVolumeBinding::Scoped)
        );
        // Resolution is idempotent.
        assert!(!loaded.resolve_data_volumes("dtag", &existing));
        // Named after the resource id, or the adopted legacy volume.
        assert_eq!(
            loaded.instances["used-db"].data_volume_name("dtag", "123"),
            scoped("used-db")
        );
        assert_eq!(
            loaded.instances["old-db"].data_volume_name("dtag", "123"),
            "fakecloud-rds-data-123-old-db"
        );
    }

    #[test]
    fn finish_instance_creation_moves_from_pending_to_instances() {
        let mut state = RdsState::new("123456789012", "us-east-1");
        assert!(state.begin_instance_creation("db-x"));
        assert!(state.in_progress_instance_ids.contains("db-x"));
        state.finish_instance_creation(make_instance("db-x"));
        assert!(!state.in_progress_instance_ids.contains("db-x"));
        assert!(state.instances.contains_key("db-x"));
    }

    #[test]
    fn cancel_instance_creation_drops_pending() {
        let mut state = RdsState::new("123456789012", "us-east-1");
        state.begin_instance_creation("db-y");
        state.cancel_instance_creation("db-y");
        assert!(!state.in_progress_instance_ids.contains("db-y"));
    }

    #[test]
    fn begin_instance_creation_rejects_when_already_created() {
        let mut state = RdsState::new("123456789012", "us-east-1");
        state
            .instances
            .insert("db-z".to_string(), make_instance("db-z"));
        assert!(!state.begin_instance_creation("db-z"));
    }

    #[test]
    fn reset_restores_default_parameter_groups() {
        let mut state = RdsState::new("123456789012", "us-east-1");
        state.parameter_groups.clear();
        state.reset();
        assert!(!state.parameter_groups.is_empty());
    }

    #[test]
    fn arn_helpers_include_region_and_account() {
        let state = RdsState::new("111122223333", "ap-southeast-2");
        let arn = state.db_instance_arn(&state.region, "my-db");
        assert!(arn.contains("111122223333"));
        assert!(arn.contains("ap-southeast-2"));
        let snap = state.db_snapshot_arn(&state.region, "snap");
        assert!(snap.contains("snapshot:snap"));
    }

    #[test]
    fn next_dbi_resource_id_unique_across_calls() {
        let state = RdsState::new("123", "us-east-1");
        let a = state.next_dbi_resource_id();
        let b = state.next_dbi_resource_id();
        assert_ne!(a, b);
    }
}

#[cfg(test)]
mod endpoint_host_tests {
    use super::*;

    #[test]
    fn endpoint_hosts_share_a_stable_per_account_region_label() {
        let h = endpoint_hash("123456789012", "us-east-1");
        assert_eq!(h.len(), 12);
        assert!(h.chars().next().unwrap().is_ascii_lowercase());
        assert!(h
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()));
        assert_eq!(h, endpoint_hash("123456789012", "us-east-1"));
        assert_ne!(h, endpoint_hash("123456789012", "eu-west-1"));
        assert_ne!(h, endpoint_hash("210987654321", "us-east-1"));
        assert_eq!(
            instance_endpoint("db1", "123456789012", "us-east-1"),
            format!("db1.{h}.us-east-1.rds.amazonaws.com")
        );
        assert_eq!(
            cluster_endpoint("c1", "123456789012", "us-east-1"),
            format!("c1.cluster-{h}.us-east-1.rds.amazonaws.com")
        );
        assert_eq!(
            cluster_reader_endpoint("c1", "123456789012", "us-east-1"),
            format!("c1.cluster-ro-{h}.us-east-1.rds.amazonaws.com")
        );
        let cn = endpoint_hash("123456789012", "cn-north-1");
        assert_eq!(
            instance_endpoint("db1", "123456789012", "cn-north-1"),
            format!("db1.{cn}.cn-north-1.rds.amazonaws.com.cn")
        );
    }
}
