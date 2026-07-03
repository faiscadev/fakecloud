//! Account-partitioned, serializable state for AWS Backup.
//!
//! Models the Backup control plane: backup plans (with their versions and
//! backup selections), backup vaults (standard, logically-air-gapped, and
//! restore-access) plus their notifications / access policies / lock config,
//! recovery points, backup / copy / restore / scan jobs, frameworks, report
//! plans and report jobs, legal holds, restore-testing plans and selections,
//! tiering configurations, and the account-scoped global / region settings.
//!
//! Complex request members (a `BackupPlan` rule set, a `BackupSelection`, a
//! `RestoreTestingPlan`, ...) are stored as the JSON objects the caller sent so
//! Get / Describe / List round-trip faithfully, following the same
//! store-the-`Value` pattern the EKS handler uses for its nested members.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const BACKUP_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

pub type TagMap = BTreeMap<String, String>;

/// A single backup selection attached to a plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectionRecord {
    pub selection_id: String,
    pub creation_date: DateTime<Utc>,
    pub creator_request_id: Option<String>,
    /// The `BackupSelection` object as sent, echoed back on Get.
    pub selection: Value,
}

/// A backup plan, keyed by its generated `BackupPlanId`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanRecord {
    pub id: String,
    pub arn: String,
    pub version_id: String,
    pub creation_date: DateTime<Utc>,
    pub deletion_date: Option<DateTime<Utc>>,
    pub last_execution_date: Option<DateTime<Utc>>,
    pub creator_request_id: Option<String>,
    /// The `BackupPlan` object (rules, advanced settings) as sent.
    pub plan: Value,
    pub advanced_backup_settings: Value,
    pub selections: BTreeMap<String, SelectionRecord>,
    /// Prior version metadata for `ListBackupPlanVersions`, newest last.
    pub versions: Vec<PlanVersion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanVersion {
    pub version_id: String,
    pub creation_date: DateTime<Utc>,
    pub deletion_date: Option<DateTime<Utc>>,
    pub plan_name: String,
}

/// A backup vault (standard / logically-air-gapped / restore-access).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VaultRecord {
    pub name: String,
    pub arn: String,
    pub vault_type: String,
    pub vault_state: String,
    pub encryption_key_arn: Option<String>,
    pub creation_date: DateTime<Utc>,
    pub creator_request_id: Option<String>,
    pub min_retention_days: Option<i64>,
    pub max_retention_days: Option<i64>,
    pub locked: bool,
    pub lock_date: Option<DateTime<Utc>>,
    pub changeable_for_days: Option<i64>,
    pub source_backup_vault_arn: Option<String>,
    pub access_policy: Option<String>,
    /// `{ SNSTopicArn, BackupVaultEvents }` when notifications are configured.
    pub notifications: Option<Value>,
    /// Recovery points keyed by RecoveryPointArn.
    pub recovery_points: BTreeMap<String, Value>,
}

/// A restore-testing plan with its selections.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreTestingPlanRecord {
    pub name: String,
    pub arn: String,
    pub creation_time: DateTime<Utc>,
    pub update_time: Option<DateTime<Utc>>,
    pub last_execution_time: Option<DateTime<Utc>>,
    pub creator_request_id: Option<String>,
    /// The `RestoreTestingPlanForCreate` object as sent.
    pub plan: Value,
    pub selections: BTreeMap<String, RestoreTestingSelectionRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreTestingSelectionRecord {
    pub name: String,
    pub creation_time: DateTime<Utc>,
    pub update_time: Option<DateTime<Utc>>,
    pub creator_request_id: Option<String>,
    /// The `RestoreTestingSelectionForCreate` object as sent.
    pub selection: Value,
}

/// The account-scoped Backup state for one AWS account.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BackupState {
    #[serde(default)]
    pub plans: BTreeMap<String, PlanRecord>,
    #[serde(default)]
    pub vaults: BTreeMap<String, VaultRecord>,
    #[serde(default)]
    pub frameworks: BTreeMap<String, Value>,
    #[serde(default)]
    pub report_plans: BTreeMap<String, Value>,
    #[serde(default)]
    pub report_jobs: BTreeMap<String, Value>,
    #[serde(default)]
    pub legal_holds: BTreeMap<String, Value>,
    #[serde(default)]
    pub restore_testing_plans: BTreeMap<String, RestoreTestingPlanRecord>,
    #[serde(default)]
    pub tiering_configs: BTreeMap<String, Value>,
    #[serde(default)]
    pub backup_jobs: BTreeMap<String, Value>,
    #[serde(default)]
    pub copy_jobs: BTreeMap<String, Value>,
    #[serde(default)]
    pub restore_jobs: BTreeMap<String, Value>,
    #[serde(default)]
    pub scan_jobs: BTreeMap<String, Value>,
    /// A flat map of every recovery point across vaults for resource-scoped
    /// lookups (`ListRecoveryPointsByResource`, `DescribeProtectedResource`):
    /// resourceArn -> list of recoveryPointArn (in creation order).
    #[serde(default)]
    pub resource_recovery_points: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub global_settings: BTreeMap<String, String>,
    /// Per-resource-type opt-in preference (defaults applied on read).
    #[serde(default)]
    pub region_optin: BTreeMap<String, bool>,
    #[serde(default)]
    pub region_mgmt: BTreeMap<String, bool>,
    /// Tags keyed by resource ARN.
    #[serde(default)]
    pub tags: BTreeMap<String, TagMap>,
}

impl AccountState for BackupState {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedBackupState = Arc<RwLock<MultiAccountState<BackupState>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct BackupSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<BackupState>,
}

// ---------------------------------------------------------------------------
// ARN builders
// ---------------------------------------------------------------------------

pub fn vault_arn(region: &str, account_id: &str, name: &str) -> String {
    format!("arn:aws:backup:{region}:{account_id}:backup-vault:{name}")
}

pub fn plan_arn(region: &str, account_id: &str, id: &str) -> String {
    format!("arn:aws:backup:{region}:{account_id}:backup-plan:{id}")
}

pub fn recovery_point_arn(region: &str, account_id: &str, id: &str) -> String {
    format!("arn:aws:backup:{region}:{account_id}:recovery-point:{id}")
}

pub fn framework_arn(region: &str, account_id: &str, name: &str, id: &str) -> String {
    format!("arn:aws:backup:{region}:{account_id}:framework:{name}-{id}")
}

pub fn report_plan_arn(region: &str, account_id: &str, name: &str, id: &str) -> String {
    format!("arn:aws:backup:{region}:{account_id}:report-plan:{name}-{id}")
}

pub fn legal_hold_arn(region: &str, account_id: &str, id: &str) -> String {
    format!("arn:aws:backup:{region}:{account_id}:legal-hold:{id}")
}

pub fn restore_testing_plan_arn(region: &str, account_id: &str, name: &str) -> String {
    format!("arn:aws:backup:{region}:{account_id}:restore-testing-plan:{name}")
}

pub fn tiering_configuration_arn(region: &str, account_id: &str, name: &str) -> String {
    format!("arn:aws:backup:{region}:{account_id}:tiering-configuration:{name}")
}
