//! Account-partitioned, serializable state for AWS CodeDeploy.
//!
//! Resources are stored as assembled response-shaped JSON values so a read
//! returns exactly what was written (round-trip fidelity). Deployments carry a
//! separate `deployment_settle` step counter so the first reads after
//! `CreateDeployment` can advance them from `Created` through `InProgress` to a
//! terminal `Succeeded` state deterministically, without leaking an internal
//! counter into the response shape.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const CODEDEPLOY_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// The account-scoped CodeDeploy state for one AWS account.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CodeDeployState {
    /// Applications keyed by application name; value is an `ApplicationInfo` JSON.
    #[serde(default)]
    pub applications: BTreeMap<String, Value>,
    /// Insertion order of application names (most-recent last).
    #[serde(default)]
    pub application_order: Vec<String>,
    /// Registered application revisions keyed by application name; value is an
    /// ordered list of `RevisionInfo` JSON objects.
    #[serde(default)]
    pub app_revisions: BTreeMap<String, Vec<Value>>,
    /// Deployment groups keyed by application name then group name; value is a
    /// `DeploymentGroupInfo` JSON.
    #[serde(default)]
    pub deployment_groups: BTreeMap<String, BTreeMap<String, Value>>,
    /// User-created deployment configurations keyed by name; value is a
    /// `DeploymentConfigInfo` JSON. The predefined `CodeDeployDefault.*`
    /// configs are synthesized on read and never stored here.
    #[serde(default)]
    pub deployment_configs: BTreeMap<String, Value>,
    /// Insertion order of user deployment-config names.
    #[serde(default)]
    pub deployment_config_order: Vec<String>,
    /// Deployments keyed by deployment id (`d-XXXXXXXXX`); value is a
    /// `DeploymentInfo` JSON.
    #[serde(default)]
    pub deployments: BTreeMap<String, Value>,
    /// Insertion order of deployment ids (most-recent last).
    #[serde(default)]
    pub deployment_order: Vec<String>,
    /// Per-deployment settle step, used to advance a deployment from
    /// `Created` -> `InProgress` -> `Succeeded` across successive reads.
    #[serde(default)]
    pub deployment_settle: BTreeMap<String, u8>,
    /// On-premises instances keyed by instance name; value is an `InstanceInfo` JSON.
    #[serde(default)]
    pub on_premises_instances: BTreeMap<String, Value>,
    /// Insertion order of on-premises instance names.
    #[serde(default)]
    pub on_premises_order: Vec<String>,
    /// Resource tags keyed by resource ARN; value is a `TagList` (list of
    /// `{Key,Value}` objects).
    #[serde(default)]
    pub tags: BTreeMap<String, Vec<Value>>,
}

impl AccountState for CodeDeployState {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedCodeDeployState = Arc<RwLock<MultiAccountState<CodeDeployState>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct CodeDeploySnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<CodeDeployState>,
}
