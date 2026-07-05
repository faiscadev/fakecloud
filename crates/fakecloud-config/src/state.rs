//! In-memory state for AWS Config (`config`).
//!
//! State is partitioned per account. Every map uses `String` keys so serde
//! round-trips cleanly (no tuple-key `KeyMustBeAString` trap): where a logical
//! key is a pair (e.g. a configuration item is identified by resource type +
//! resource id), the two halves are joined with a `\u{1}` separator via
//! [`resource_key`].

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

pub type SharedConfigState = Arc<RwLock<ConfigAccounts>>;

/// Join a resource type + id into a single stable map key.
pub fn resource_key(resource_type: &str, resource_id: &str) -> String {
    format!("{resource_type}\u{1}{resource_id}")
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ConfigAccounts {
    pub accounts: BTreeMap<String, AccountState>,
}

impl ConfigAccounts {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn account_mut(&mut self, account_id: &str) -> &mut AccountState {
        self.accounts.entry(account_id.to_string()).or_default()
    }

    pub fn account(&self, account_id: &str) -> Option<&AccountState> {
        self.accounts.get(account_id)
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AccountState {
    /// Configuration recorders keyed by recorder name.
    pub recorders: BTreeMap<String, ConfigurationRecorder>,
    /// Delivery channels keyed by channel name.
    pub delivery_channels: BTreeMap<String, DeliveryChannel>,
    /// Config rules keyed by rule name.
    pub rules: BTreeMap<String, ConfigRule>,
    /// Configuration-item history keyed by `resource_key(type, id)`; each value
    /// is an ordered (oldest-first) list of recorded items.
    pub config_items: BTreeMap<String, Vec<ConfigurationItem>>,
    /// Evaluation results keyed by rule name; each value is keyed by
    /// `resource_key(type, id)`.
    pub evaluations: BTreeMap<String, BTreeMap<String, EvaluationResult>>,
    /// Conformance packs keyed by pack name.
    pub conformance_packs: BTreeMap<String, ConformancePack>,
    /// Organization config rules keyed by rule name.
    pub org_rules: BTreeMap<String, OrganizationConfigRule>,
    /// Organization conformance packs keyed by pack name.
    pub org_conformance_packs: BTreeMap<String, OrganizationConformancePack>,
    /// Configuration aggregators keyed by aggregator name.
    pub aggregators: BTreeMap<String, ConfigurationAggregator>,
    /// Aggregation authorizations keyed by `account\u{1}region`.
    pub aggregation_authorizations: BTreeMap<String, AggregationAuthorization>,
    /// Retention configurations keyed by name (AWS allows exactly one, named
    /// `default`).
    pub retention_configurations: BTreeMap<String, RetentionConfiguration>,
    /// Stored queries keyed by query name.
    pub stored_queries: BTreeMap<String, StoredQuery>,
    /// Remediation configurations keyed by config-rule name.
    pub remediation_configs: BTreeMap<String, RemediationConfiguration>,
    /// Remediation exceptions keyed by `rule\u{1}resourceType\u{1}resourceId`.
    pub remediation_exceptions: BTreeMap<String, RemediationException>,
    /// Resource evaluations keyed by evaluation id.
    pub resource_evaluations: BTreeMap<String, ResourceEvaluation>,
    /// Tags per resource ARN.
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
}

// ─── Configuration recorder ──────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigurationRecorder {
    pub name: String,
    pub role_arn: String,
    /// The `recordingGroup` echoed back verbatim.
    #[serde(default)]
    pub recording_group: Option<serde_json::Value>,
    #[serde(default)]
    pub recording_mode: Option<serde_json::Value>,
    /// `arn:aws:config:...:configuration-recorder/<name>` — only populated for
    /// service-linked recorders.
    #[serde(default)]
    pub arn: Option<String>,
    #[serde(default)]
    pub service_principal: Option<String>,
    /// Whether the recorder is currently running.
    pub recording: bool,
    pub last_start_time: Option<DateTime<Utc>>,
    pub last_stop_time: Option<DateTime<Utc>>,
    pub last_status: String,
    pub last_status_change_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryChannel {
    pub name: String,
    pub s3_bucket_name: Option<String>,
    pub s3_key_prefix: Option<String>,
    pub s3_kms_key_arn: Option<String>,
    pub sns_topic_arn: Option<String>,
    #[serde(default)]
    pub config_snapshot_delivery_properties: Option<serde_json::Value>,
    pub last_status: String,
}

// ─── Configuration items ─────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigurationItem {
    pub version: String,
    pub account_id: String,
    pub configuration_item_capture_time: DateTime<Utc>,
    /// `OK`, `ResourceDiscovered`, `ResourceNotRecorded`, `ResourceDeleted`,
    /// `ResourceDeletedNotRecorded`.
    pub configuration_item_status: String,
    pub configuration_state_id: String,
    pub arn: String,
    pub resource_type: String,
    pub resource_id: String,
    #[serde(default)]
    pub resource_name: Option<String>,
    pub aws_region: String,
    pub availability_zone: String,
    pub resource_creation_time: Option<DateTime<Utc>>,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    /// The resource configuration as a JSON string (Config stores this as an
    /// escaped JSON document).
    pub configuration: String,
    #[serde(default)]
    pub supplementary_configuration: BTreeMap<String, String>,
}

// ─── Config rules + evaluations ──────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigRule {
    pub name: String,
    pub arn: String,
    pub rule_id: String,
    #[serde(default)]
    pub description: Option<String>,
    /// The `Scope` object echoed back verbatim.
    #[serde(default)]
    pub scope: Option<serde_json::Value>,
    /// The `Source` object: Owner (AWS/CUSTOM_LAMBDA/CUSTOM_POLICY),
    /// SourceIdentifier, SourceDetails, CustomPolicyDetails.
    pub source: serde_json::Value,
    #[serde(default)]
    pub input_parameters: Option<String>,
    #[serde(default)]
    pub maximum_execution_frequency: Option<String>,
    /// `ACTIVE`, `DELETING`, `EVALUATING`.
    pub state: String,
    #[serde(default)]
    pub created_by: Option<String>,
    #[serde(default)]
    pub evaluation_modes: Option<serde_json::Value>,
    pub last_updated: DateTime<Utc>,
    // Evaluation status bookkeeping.
    pub first_activated_time: Option<DateTime<Utc>>,
    pub last_successful_evaluation_time: Option<DateTime<Utc>>,
    pub last_successful_invocation_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvaluationResult {
    pub resource_type: String,
    pub resource_id: String,
    pub rule_name: String,
    /// `COMPLIANT`, `NON_COMPLIANT`, `NOT_APPLICABLE`, `INSUFFICIENT_DATA`.
    pub compliance_type: String,
    pub annotation: Option<String>,
    pub result_recorded_time: DateTime<Utc>,
    pub config_rule_invoked_time: DateTime<Utc>,
    pub ordering_timestamp: DateTime<Utc>,
}

// ─── Conformance packs ───────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConformancePack {
    pub name: String,
    pub arn: String,
    pub id: String,
    pub delivery_s3_bucket: Option<String>,
    pub delivery_s3_key_prefix: Option<String>,
    #[serde(default)]
    pub input_parameters: Vec<serde_json::Value>,
    pub template_body: Option<String>,
    pub template_s3_uri: Option<String>,
    #[serde(default)]
    pub template_ssm_document_details: Option<serde_json::Value>,
    pub last_update_requested_time: DateTime<Utc>,
    pub created_by: Option<String>,
    /// Rule names created by this pack.
    #[serde(default)]
    pub rule_names: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrganizationConfigRule {
    pub name: String,
    pub arn: String,
    #[serde(default)]
    pub managed_rule_metadata: Option<serde_json::Value>,
    #[serde(default)]
    pub custom_rule_metadata: Option<serde_json::Value>,
    #[serde(default)]
    pub custom_policy_rule_metadata: Option<serde_json::Value>,
    #[serde(default)]
    pub excluded_accounts: Vec<String>,
    pub last_update_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrganizationConformancePack {
    pub name: String,
    pub arn: String,
    pub delivery_s3_bucket: Option<String>,
    pub delivery_s3_key_prefix: Option<String>,
    #[serde(default)]
    pub input_parameters: Vec<serde_json::Value>,
    pub template_body: Option<String>,
    pub template_s3_uri: Option<String>,
    #[serde(default)]
    pub excluded_accounts: Vec<String>,
    pub last_update_time: DateTime<Utc>,
}

// ─── Aggregators ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigurationAggregator {
    pub name: String,
    pub arn: String,
    #[serde(default)]
    pub account_aggregation_sources: Vec<serde_json::Value>,
    #[serde(default)]
    pub organization_aggregation_source: Option<serde_json::Value>,
    pub creation_time: DateTime<Utc>,
    pub last_updated_time: DateTime<Utc>,
    pub created_by: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationAuthorization {
    pub arn: String,
    pub authorized_account_id: String,
    pub authorized_aws_region: String,
    pub creation_time: DateTime<Utc>,
}

// ─── Retention / stored queries / remediation / resource-eval ────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionConfiguration {
    pub name: String,
    pub retention_period_in_days: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredQuery {
    pub id: String,
    pub name: String,
    pub arn: String,
    pub description: Option<String>,
    pub expression: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemediationConfiguration {
    pub config_rule_name: String,
    pub arn: String,
    pub target_type: String,
    pub target_id: String,
    #[serde(default)]
    pub target_version: Option<String>,
    #[serde(default)]
    pub parameters: Option<serde_json::Value>,
    #[serde(default)]
    pub resource_type: Option<String>,
    #[serde(default)]
    pub automatic: bool,
    #[serde(default)]
    pub execution_controls: Option<serde_json::Value>,
    #[serde(default)]
    pub maximum_automatic_attempts: Option<i64>,
    #[serde(default)]
    pub retry_attempt_seconds: Option<i64>,
    pub created_by_service: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemediationException {
    pub config_rule_name: String,
    pub resource_type: String,
    pub resource_id: String,
    pub message: Option<String>,
    pub expiration_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceEvaluation {
    pub resource_evaluation_id: String,
    pub evaluation_mode: String,
    pub time_stamp: DateTime<Utc>,
    /// `IN_PROGRESS`, `SUCCEEDED`, `FAILED`.
    pub status: String,
    #[serde(default)]
    pub resource_details: Option<serde_json::Value>,
    #[serde(default)]
    pub evaluation_context: Option<serde_json::Value>,
    #[serde(default)]
    pub evaluation_results: Vec<EvaluationResult>,
}

/// On-disk snapshot envelope. Versioned so format changes fail loudly on
/// upgrade rather than silently mis-parsing.
#[derive(Clone, Serialize, Deserialize)]
pub struct ConfigSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<ConfigAccounts>,
}

pub const CONFIG_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
