//! In-memory state for AWS Elastic Beanstalk.
//!
//! Account-partitioned. Elastic Beanstalk is an orchestration facade: an
//! Application groups ApplicationVersions (source bundles in S3) and
//! Environments. An Environment carries a real modeled lifecycle
//! (`Launching -> Ready`, `Updating`, `Terminating -> Terminated`) plus a
//! health rollup (`Green/Yellow/Red/Grey`) derived from that state. Every
//! transition emits an Event.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

pub type SharedEbState = Arc<RwLock<EbAccounts>>;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct EbAccounts {
    pub accounts: BTreeMap<String, AccountState>,
}

impl EbAccounts {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_or_create(&mut self, account_id: &str) -> &mut AccountState {
        self.accounts.entry(account_id.to_string()).or_default()
    }

    pub fn account_count(&self) -> usize {
        self.accounts.len()
    }
}

/// Versioned on-disk persistence snapshot for Elastic Beanstalk.
#[derive(Debug, Serialize, Deserialize)]
pub struct ElasticBeanstalkSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<EbAccounts>,
}

pub const ELASTICBEANSTALK_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Serialize/deserialize `BTreeMap<(String, String), V>` as `Vec<(String, String, V)>`.
///
/// `serde_json` cannot serialize a map with a non-string key (a tuple key fails
/// with `KeyMustBeAString`), so the tuple-keyed maps are stored as a list of
/// triples on disk. Mirrors the sibling crates (bedrock/route53/athena/logs).
mod tuple2_map_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::BTreeMap;

    pub(crate) fn serialize<V: Serialize, S: Serializer>(
        map: &BTreeMap<(String, String), V>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        let entries: Vec<(&String, &String, &V)> =
            map.iter().map(|((a, b), v)| (a, b, v)).collect();
        entries.serialize(s)
    }

    pub(crate) fn deserialize<'de, V: Deserialize<'de>, D: Deserializer<'de>>(
        d: D,
    ) -> Result<BTreeMap<(String, String), V>, D::Error> {
        let entries: Vec<(String, String, V)> = Vec::deserialize(d)?;
        Ok(entries.into_iter().map(|(a, b, v)| ((a, b), v)).collect())
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AccountState {
    /// Applications keyed by name.
    pub applications: BTreeMap<String, Application>,
    /// Application versions keyed by `(application_name, version_label)`.
    #[serde(with = "tuple2_map_serde")]
    pub versions: BTreeMap<(String, String), ApplicationVersion>,
    /// Environments keyed by environment id (`e-xxxxxxxxxx`).
    pub environments: BTreeMap<String, Environment>,
    /// Configuration templates keyed by `(application_name, template_name)`.
    #[serde(with = "tuple2_map_serde")]
    pub templates: BTreeMap<(String, String), ConfigurationTemplate>,
    /// Events, newest first.
    pub events: Vec<Event>,
    /// The account's default managed S3 storage bucket, once created.
    #[serde(default)]
    pub storage_bucket: Option<String>,
    /// Resource tags keyed by resource ARN (applications and environments).
    #[serde(default)]
    pub tags: BTreeMap<String, Vec<ResourceTag>>,
    /// Custom platform versions keyed by platform ARN (CreatePlatformVersion).
    #[serde(default)]
    pub platforms: BTreeMap<String, CustomPlatform>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomPlatform {
    pub arn: String,
    pub name: String,
    pub version: String,
    pub owner: String,
    pub status: String,
    pub category: String,
    pub date_created: DateTime<Utc>,
    pub date_updated: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceTag {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Application {
    pub name: String,
    pub arn: String,
    #[serde(default)]
    pub description: Option<String>,
    pub date_created: DateTime<Utc>,
    pub date_updated: DateTime<Utc>,
    #[serde(default)]
    pub resource_lifecycle_config: ResourceLifecycleConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceLifecycleConfig {
    #[serde(default)]
    pub service_role: Option<String>,
    #[serde(default)]
    pub max_count_rule: Option<MaxCountRule>,
    #[serde(default)]
    pub max_age_rule: Option<MaxAgeRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaxCountRule {
    pub enabled: bool,
    #[serde(default)]
    pub max_count: Option<i64>,
    #[serde(default)]
    pub delete_source_from_s3: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaxAgeRule {
    pub enabled: bool,
    #[serde(default)]
    pub max_age_in_days: Option<i64>,
    #[serde(default)]
    pub delete_source_from_s3: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplicationVersion {
    pub application_name: String,
    pub version_label: String,
    pub arn: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub source_bundle_bucket: Option<String>,
    #[serde(default)]
    pub source_bundle_key: Option<String>,
    #[serde(default)]
    pub source_build_information: Option<SourceBuildInformation>,
    #[serde(default)]
    pub build_arn: Option<String>,
    pub date_created: DateTime<Utc>,
    pub date_updated: DateTime<Utc>,
    /// `Processed` | `Unprocessed` | `Failed` | `Processing` | `Building`.
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceBuildInformation {
    pub source_type: String,
    pub source_repository: String,
    pub source_location: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Environment {
    pub name: String,
    pub id: String,
    pub arn: String,
    pub application_name: String,
    #[serde(default)]
    pub version_label: Option<String>,
    #[serde(default)]
    pub solution_stack_name: Option<String>,
    #[serde(default)]
    pub platform_arn: Option<String>,
    #[serde(default)]
    pub template_name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    pub cname: String,
    pub endpoint_url: String,
    pub date_created: DateTime<Utc>,
    pub date_updated: DateTime<Utc>,
    /// One of [`crate::state::EnvironmentStatus`] string values.
    pub status: String,
    #[serde(default)]
    pub abortable_operation_in_progress: bool,
    /// `Green` | `Yellow` | `Red` | `Grey`.
    pub health: String,
    /// `Ok` | `Warning` | `Degraded` | `Severe` | `Info` | `Pending` |
    /// `Unknown` | `NoData` | `Suspended`.
    pub health_status: String,
    pub tier_name: String,
    pub tier_type: String,
    pub tier_version: String,
    #[serde(default)]
    pub operations_role: Option<String>,
    #[serde(default)]
    pub group_name: Option<String>,
    /// Effective configuration option settings (namespace/name/value).
    #[serde(default)]
    pub option_settings: Vec<OptionSetting>,
    /// Monotonic transition epoch. Every lifecycle-mutating operation bumps
    /// this and captures the value; a spawned settle task applies its terminal
    /// transition only if the generation still matches (else it is stale, e.g.
    /// a Terminate landed on top of an in-flight Update). This makes
    /// concurrent Terminate-vs-Update deterministic.
    #[serde(default)]
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OptionSetting {
    #[serde(default)]
    pub resource_name: Option<String>,
    pub namespace: String,
    pub option_name: String,
    #[serde(default)]
    pub value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigurationTemplate {
    pub application_name: String,
    pub template_name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub solution_stack_name: Option<String>,
    #[serde(default)]
    pub platform_arn: Option<String>,
    #[serde(default)]
    pub environment_name: Option<String>,
    /// `deployed` | `pending` | `failed`.
    pub deployment_status: String,
    pub date_created: DateTime<Utc>,
    pub date_updated: DateTime<Utc>,
    #[serde(default)]
    pub option_settings: Vec<OptionSetting>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub event_date: DateTime<Utc>,
    pub message: String,
    #[serde(default)]
    pub application_name: Option<String>,
    #[serde(default)]
    pub version_label: Option<String>,
    #[serde(default)]
    pub template_name: Option<String>,
    #[serde(default)]
    pub environment_name: Option<String>,
    #[serde(default)]
    pub platform_arn: Option<String>,
    #[serde(default)]
    pub request_id: Option<String>,
    /// `TRACE` | `DEBUG` | `INFO` | `WARN` | `ERROR` | `FATAL`.
    pub severity: String,
}

/// Canonical `EnvironmentStatus` enum values (Smithy `EnvironmentStatus`).
pub mod environment_status {
    pub const LAUNCHING: &str = "Launching";
    pub const UPDATING: &str = "Updating";
    pub const READY: &str = "Ready";
    pub const TERMINATING: &str = "Terminating";
    pub const TERMINATED: &str = "Terminated";
}
