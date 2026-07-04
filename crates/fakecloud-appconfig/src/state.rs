//! Account-partitioned, serializable state for AWS AppConfig.
//!
//! Models the AppConfig control plane (applications with their environments and
//! configuration profiles, hosted configuration versions storing raw bytes,
//! deployment strategies, deployments, extensions and their associations,
//! experiment definitions and runs, account settings, tags) plus the AppConfig
//! Data plane (configuration sessions resolved to deployed content).
//!
//! Complex request members (validators, monitors, extension action maps,
//! experiment treatments) are stored as the JSON objects the caller sent so
//! Get / List round-trip faithfully, the same store-the-`Value` pattern the
//! Backup and EKS handlers use.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const APPCONFIG_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

pub type TagMap = BTreeMap<String, String>;

/// A hosted configuration version: raw configuration bytes plus metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostedVersionRecord {
    pub version_number: i64,
    pub description: Option<String>,
    pub content: Vec<u8>,
    pub content_type: String,
    pub version_label: Option<String>,
}

/// A configuration profile, nested under an application.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileRecord {
    pub id: String,
    pub application_id: String,
    pub name: String,
    pub description: Option<String>,
    pub location_uri: String,
    pub retrieval_role_arn: Option<String>,
    pub validators: Value,
    pub profile_type: String,
    pub kms_key_arn: Option<String>,
    pub kms_key_identifier: Option<String>,
    #[serde(default)]
    pub hosted_versions: BTreeMap<i64, HostedVersionRecord>,
    #[serde(default)]
    pub next_version_number: i64,
}

/// A deployment, nested under an environment (keyed by DeploymentNumber).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentRecord {
    pub deployment_number: i64,
    /// The full `Deployment` output object, settled to a terminal state.
    pub deployment: Value,
}

/// An environment, nested under an application.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentRecord {
    pub id: String,
    pub application_id: String,
    pub name: String,
    pub description: Option<String>,
    pub state: String,
    pub monitors: Value,
    #[serde(default)]
    pub deployments: BTreeMap<i64, DeploymentRecord>,
    #[serde(default)]
    pub next_deployment_number: i64,
}

/// An application with its nested environments, profiles and experiments.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplicationRecord {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    #[serde(default)]
    pub environments: BTreeMap<String, EnvironmentRecord>,
    #[serde(default)]
    pub profiles: BTreeMap<String, ProfileRecord>,
    /// Experiment definitions keyed by id, each storing its runs.
    #[serde(default)]
    pub experiments: BTreeMap<String, ExperimentRecord>,
}

/// An experiment definition plus its runs (stored as JSON for round-trip).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExperimentRecord {
    pub id: String,
    /// The `ExperimentDefinition` output object.
    pub definition: Value,
    /// Runs keyed by run number; each value is an `ExperimentRun` object.
    #[serde(default)]
    pub runs: BTreeMap<i64, Value>,
    #[serde(default)]
    pub next_run_number: i64,
}

/// An AppConfig Data configuration session (control-plane token binding).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRecord {
    pub token: String,
    pub application_id: String,
    pub environment_id: String,
    pub configuration_profile_id: String,
}

/// The account-scoped AppConfig state for one AWS account.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AppConfigState {
    #[serde(default)]
    pub applications: BTreeMap<String, ApplicationRecord>,
    /// Customer-created deployment strategies (predefined ones are synthesized).
    #[serde(default)]
    pub deployment_strategies: BTreeMap<String, Value>,
    /// Extensions keyed by id; each value is the `Extension` object.
    #[serde(default)]
    pub extensions: BTreeMap<String, Value>,
    /// Extension associations keyed by id; each value is the object.
    #[serde(default)]
    pub extension_associations: BTreeMap<String, Value>,
    /// Extensions referenced by an association's `ExtensionIdentifier` that
    /// were not created locally (e.g. AWS-authored extensions). Keyed by the
    /// identifier used on the association so `GetExtension` can resolve them.
    #[serde(default)]
    pub referenced_extensions: BTreeMap<String, Value>,
    /// Account settings (deletion protection + vended metrics), applied lazily.
    #[serde(default)]
    pub account_settings: Option<Value>,
    /// AppConfig Data sessions keyed by their token.
    #[serde(default)]
    pub sessions: BTreeMap<String, SessionRecord>,
    /// Tags keyed by resource ARN.
    #[serde(default)]
    pub tags: BTreeMap<String, TagMap>,
}

impl AccountState for AppConfigState {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedAppConfigState = Arc<RwLock<MultiAccountState<AppConfigState>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct AppConfigSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<AppConfigState>,
}

// ---------------------------------------------------------------------------
// ARN builders
// ---------------------------------------------------------------------------

pub fn application_arn(region: &str, account_id: &str, id: &str) -> String {
    format!("arn:aws:appconfig:{region}:{account_id}:application/{id}")
}

pub fn environment_arn(region: &str, account_id: &str, app: &str, env: &str) -> String {
    format!("arn:aws:appconfig:{region}:{account_id}:application/{app}/environment/{env}")
}

pub fn profile_arn(region: &str, account_id: &str, app: &str, profile: &str) -> String {
    format!(
        "arn:aws:appconfig:{region}:{account_id}:application/{app}/configurationprofile/{profile}"
    )
}

pub fn deployment_strategy_arn(region: &str, account_id: &str, id: &str) -> String {
    format!("arn:aws:appconfig:{region}:{account_id}:deploymentstrategy/{id}")
}

pub fn extension_arn(region: &str, account_id: &str, id: &str, version: i64) -> String {
    format!("arn:aws:appconfig:{region}:{account_id}:extension/{id}/{version}")
}

pub fn extension_association_arn(region: &str, account_id: &str, id: &str) -> String {
    format!("arn:aws:appconfig:{region}:{account_id}:extensionassociation/{id}")
}
