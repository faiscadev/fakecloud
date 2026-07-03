//! Account-partitioned, serializable state for AWS Database Migration
//! Service's (`dms`) control plane.
//!
//! Every DMS resource is stored as an already-output-valid JSON object
//! (`serde_json::Value`) keyed by its ARN or identifier. Storing the wire
//! shape directly keeps per-engine endpoint settings and other nested
//! configuration objects round-tripping verbatim and guarantees the
//! `Describe` responses echo exactly what was persisted.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const DMS_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Per-account DMS state. Each map is keyed by the resource's ARN (or, for
/// resources without an ARN in their describe shape, their identifier) and
/// holds the resource's already-output-valid JSON object.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DmsData {
    /// Replication instances keyed by `ReplicationInstanceArn`.
    #[serde(default)]
    pub replication_instances: BTreeMap<String, Value>,
    /// Endpoints keyed by `EndpointArn`.
    #[serde(default)]
    pub endpoints: BTreeMap<String, Value>,
    /// Replication tasks keyed by `ReplicationTaskArn`.
    #[serde(default)]
    pub replication_tasks: BTreeMap<String, Value>,
    /// Replication subnet groups keyed by `ReplicationSubnetGroupIdentifier`.
    #[serde(default)]
    pub subnet_groups: BTreeMap<String, Value>,
    /// Event subscriptions keyed by `CustSubscriptionId` (subscription name).
    #[serde(default)]
    pub event_subscriptions: BTreeMap<String, Value>,
    /// Certificates keyed by `CertificateArn`.
    #[serde(default)]
    pub certificates: BTreeMap<String, Value>,
    /// Serverless replication configs keyed by `ReplicationConfigArn`.
    #[serde(default)]
    pub replication_configs: BTreeMap<String, Value>,
    /// Serverless replications keyed by `ReplicationConfigArn`.
    #[serde(default)]
    pub replications: BTreeMap<String, Value>,
    /// Data providers keyed by `DataProviderArn`.
    #[serde(default)]
    pub data_providers: BTreeMap<String, Value>,
    /// Instance profiles keyed by `InstanceProfileArn`.
    #[serde(default)]
    pub instance_profiles: BTreeMap<String, Value>,
    /// Migration projects keyed by `MigrationProjectArn`.
    #[serde(default)]
    pub migration_projects: BTreeMap<String, Value>,
    /// Data migrations keyed by `DataMigrationArn`.
    #[serde(default)]
    pub data_migrations: BTreeMap<String, Value>,
    /// Replication-task assessment runs keyed by
    /// `ReplicationTaskAssessmentRunArn`.
    #[serde(default)]
    pub assessment_runs: BTreeMap<String, Value>,
    /// Endpoint/replication-instance connections keyed by
    /// `"{endpoint_arn}|{instance_arn}"`.
    #[serde(default)]
    pub connections: BTreeMap<String, Value>,
    /// Fleet Advisor collectors keyed by `CollectorReferencedId`.
    #[serde(default)]
    pub fleet_advisor_collectors: BTreeMap<String, Value>,
    /// Schema-conversion / metadata-model requests keyed by `RequestIdentifier`.
    #[serde(default)]
    pub schema_conversion_requests: BTreeMap<String, Value>,
    /// Fleet-advisor / target recommendations keyed by `DatabaseId`.
    #[serde(default)]
    pub recommendations: BTreeMap<String, Value>,
    /// Per-migration-project schema-conversion configuration JSON, keyed by
    /// `MigrationProjectArn`.
    #[serde(default)]
    pub conversion_configs: BTreeMap<String, String>,
    /// Refresh-schemas status keyed by `EndpointArn`.
    #[serde(default)]
    pub refresh_schemas_status: BTreeMap<String, Value>,
    /// Resource tags keyed by `ResourceArn` -> (key -> value).
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
}

impl AccountState for DmsData {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedDmsState = Arc<RwLock<MultiAccountState<DmsData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct DmsSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<DmsData>,
}
