use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SsmParameter {
    pub name: String,
    pub value: String,
    pub param_type: String, // String, StringList, SecureString
    pub version: i64,
    pub arn: String,
    pub last_modified: DateTime<Utc>,
    pub history: Vec<SsmParameterVersion>,
    pub tags: BTreeMap<String, String>,
    pub labels: BTreeMap<i64, Vec<String>>, // version -> labels
    pub description: Option<String>,
    pub allowed_pattern: Option<String>,
    pub key_id: Option<String>,
    pub data_type: String, // "text" or "aws:ec2:image"
    pub tier: String,      // "Standard", "Advanced", "Intelligent-Tiering"
    pub policies: Option<String>,
    /// Whether the `ExpirationNotification` event has already been
    /// emitted for the current Policies list. Reset whenever the
    /// parameter is overwritten so updated policies fire fresh
    /// notifications. Snapshots from before this field existed
    /// deserialize as `false`.
    #[serde(default)]
    pub expiration_notified: bool,
    /// Whether the `NoChangeNotification` event has already been
    /// emitted for the current value. Reset whenever the parameter is
    /// overwritten so the inactivity window restarts on each update.
    #[serde(default)]
    pub no_change_notified: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SsmParameterVersion {
    pub value: String,
    pub version: i64,
    pub last_modified: DateTime<Utc>,
    pub param_type: String,
    pub description: Option<String>,
    pub key_id: Option<String>,
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SsmDocument {
    pub name: String,
    pub content: String,
    pub document_type: String,
    pub document_format: String,
    pub target_type: Option<String>,
    pub version_name: Option<String>,
    pub tags: BTreeMap<String, String>,
    pub versions: Vec<SsmDocumentVersion>,
    pub default_version: String,
    pub latest_version: String,
    pub created_date: DateTime<Utc>,
    pub owner: String,
    pub status: String,
    pub permissions: BTreeMap<String, Vec<String>>, // permission_type -> account_ids
    #[serde(default)]
    pub reviews: Vec<DocumentReview>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DocumentReview {
    pub reviewer: String,
    pub action: String, // SendForReview / Approve / Reject
    pub comment: Vec<DocumentReviewComment>,
    pub created_time: DateTime<Utc>,
    pub updated_time: DateTime<Utc>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DocumentReviewComment {
    pub comment_type: String, // Comment
    pub content: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SsmDocumentVersion {
    pub content: String,
    pub document_version: String,
    pub version_name: Option<String>,
    pub created_date: DateTime<Utc>,
    pub status: String,
    pub document_format: String,
    pub is_default_version: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SsmCommand {
    pub command_id: String,
    pub document_name: String,
    pub instance_ids: Vec<String>,
    pub parameters: BTreeMap<String, Vec<String>>,
    pub status: String,
    pub requested_date_time: DateTime<Utc>,
    /// When the command's results stop being readable. Defaults to
    /// `requested_date_time + 1h` for snapshots written before this
    /// field existed so old data still deserializes cleanly.
    #[serde(default = "default_command_expiry")]
    pub expires_after: DateTime<Utc>,
    pub comment: Option<String>,
    pub output_s3_bucket_name: Option<String>,
    pub output_s3_key_prefix: Option<String>,
    pub output_s3_region: Option<String>,
    pub timeout_seconds: Option<i64>,
    pub service_role_arn: Option<String>,
    pub notification_config: Option<serde_json::Value>,
    pub targets: Vec<serde_json::Value>,
    pub document_hash: Option<String>,
    pub document_hash_type: Option<String>,
    /// Per-instance invocation state. One entry per `InstanceIds`
    /// member; updated independently by the async transition task or
    /// by the admin force-fail endpoint.
    #[serde(default)]
    pub invocations: Vec<SsmCommandInvocation>,
}

fn default_command_expiry() -> DateTime<Utc> {
    chrono::Utc::now() + chrono::Duration::seconds(3600)
}

/// One execution of a command on a single managed instance. The
/// real SSM API exposes this via `GetCommandInvocation` and
/// `ListCommandInvocations`; per-invocation status diverges from the
/// parent command status when only some instances fail.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SsmCommandInvocation {
    pub instance_id: String,
    pub status: String,
    pub status_details: String,
    pub standard_output_content: String,
    pub standard_error_content: String,
    pub response_code: i64,
    pub requested_date_time: DateTime<Utc>,
    pub last_update_at: DateTime<Utc>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MaintenanceWindowTarget {
    pub window_target_id: String,
    pub window_id: String,
    pub resource_type: String,
    pub targets: Vec<serde_json::Value>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub owner_information: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MaintenanceWindowTask {
    pub window_task_id: String,
    pub window_id: String,
    pub task_arn: String,
    pub task_type: String,
    pub targets: Vec<serde_json::Value>,
    pub max_concurrency: Option<String>,
    pub max_errors: Option<String>,
    pub priority: i64,
    pub service_role_arn: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MaintenanceWindow {
    pub id: String,
    pub name: String,
    pub schedule: String,
    pub duration: i64,
    pub cutoff: i64,
    pub allow_unassociated_targets: bool,
    pub enabled: bool,
    pub description: Option<String>,
    pub tags: BTreeMap<String, String>,
    pub targets: Vec<MaintenanceWindowTarget>,
    pub tasks: Vec<MaintenanceWindowTask>,
    pub schedule_timezone: Option<String>,
    pub schedule_offset: Option<i64>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub client_token: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PatchBaseline {
    pub id: String,
    pub name: String,
    pub operating_system: String,
    pub description: Option<String>,
    pub approval_rules: Option<serde_json::Value>,
    pub approved_patches: Vec<String>,
    pub rejected_patches: Vec<String>,
    pub tags: BTreeMap<String, String>,
    pub approved_patches_compliance_level: String,
    pub rejected_patches_action: String,
    pub global_filters: Option<serde_json::Value>,
    pub sources: Vec<serde_json::Value>,
    pub approved_patches_enable_non_security: bool,
    pub available_security_updates_compliance_status: Option<String>,
    pub client_token: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PatchGroup {
    pub baseline_id: String,
    pub patch_group: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SsmAssociation {
    pub association_id: String,
    pub name: String, // document name
    pub targets: Vec<serde_json::Value>,
    pub schedule_expression: Option<String>,
    pub parameters: BTreeMap<String, Vec<String>>,
    pub association_name: Option<String>,
    pub document_version: Option<String>,
    pub output_location: Option<serde_json::Value>,
    pub automation_target_parameter_name: Option<String>,
    pub max_errors: Option<String>,
    pub max_concurrency: Option<String>,
    pub compliance_severity: Option<String>,
    pub sync_compliance: Option<String>,
    pub apply_only_at_cron_interval: bool,
    pub calendar_names: Vec<String>,
    pub target_locations: Vec<serde_json::Value>,
    pub schedule_offset: Option<i64>,
    pub target_maps: Vec<serde_json::Value>,
    pub tags: BTreeMap<String, String>,
    pub status: String,
    pub status_date: DateTime<Utc>,
    pub overview: serde_json::Value,
    pub created_date: DateTime<Utc>,
    pub last_update_association_date: DateTime<Utc>,
    pub last_execution_date: Option<DateTime<Utc>>,
    pub instance_id: Option<String>,
    pub versions: Vec<SsmAssociationVersion>,
    /// Recorded executions (StartAssociationsOnce / scheduled applies). Empty
    /// until the association runs; surfaced by DescribeAssociationExecutions
    /// and DescribeAssociationExecutionTargets (bug-audit 2026-05-28, 1.15).
    #[serde(default)]
    pub executions: Vec<AssociationExecution>,
}

/// One recorded run of an SSM State Manager association. fakecloud applies
/// associations synchronously and always succeeds, so every execution is a
/// `Success` over the association's resolved targets.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AssociationExecution {
    pub execution_id: String,
    pub status: String,
    pub detailed_status: String,
    pub created_time: DateTime<Utc>,
    pub resource_count: usize,
    /// Resolved target resource ids covered by this execution.
    pub resource_ids: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SsmAssociationVersion {
    pub version: i64,
    pub name: String,
    pub targets: Vec<serde_json::Value>,
    pub schedule_expression: Option<String>,
    pub parameters: BTreeMap<String, Vec<String>>,
    pub document_version: Option<String>,
    pub created_date: DateTime<Utc>,
    pub association_name: Option<String>,
    pub max_errors: Option<String>,
    pub max_concurrency: Option<String>,
    pub compliance_severity: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SsmOpsItem {
    pub ops_item_id: String,
    pub title: String,
    pub description: Option<String>,
    pub source: String,
    pub status: String,
    pub priority: Option<i64>,
    pub severity: Option<String>,
    pub category: Option<String>,
    pub operational_data: BTreeMap<String, serde_json::Value>,
    pub notifications: Vec<serde_json::Value>,
    pub related_ops_items: Vec<serde_json::Value>,
    pub tags: BTreeMap<String, String>,
    pub created_time: DateTime<Utc>,
    pub last_modified_time: DateTime<Utc>,
    pub created_by: String,
    pub last_modified_by: String,
    pub ops_item_type: Option<String>,
    pub planned_start_time: Option<DateTime<Utc>>,
    pub planned_end_time: Option<DateTime<Utc>>,
    pub actual_start_time: Option<DateTime<Utc>>,
    pub actual_end_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SsmResourcePolicy {
    pub policy_id: String,
    pub policy_hash: String,
    pub policy: String,
    pub resource_arn: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SsmServiceSetting {
    pub setting_id: String,
    pub setting_value: String,
    pub last_modified_date: DateTime<Utc>,
    pub last_modified_user: String,
    pub status: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OpsItemRelatedItem {
    pub association_id: String,
    pub ops_item_id: String,
    pub association_type: String,
    pub resource_type: String,
    pub resource_uri: String,
    pub created_time: DateTime<Utc>,
    pub created_by: String,
    pub last_modified_time: DateTime<Utc>,
    pub last_modified_by: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OpsItemEvent {
    pub ops_item_id: String,
    pub event_id: String,
    pub source: String,
    pub detail_type: String,
    pub created_time: DateTime<Utc>,
    pub created_by: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OpsMetadataEntry {
    pub ops_metadata_arn: String,
    pub resource_id: String,
    pub metadata: BTreeMap<String, serde_json::Value>,
    pub creation_date: DateTime<Utc>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AutomationExecution {
    pub automation_execution_id: String,
    pub document_name: String,
    pub document_version: Option<String>,
    pub automation_execution_status: String,
    pub execution_start_time: DateTime<Utc>,
    pub execution_end_time: Option<DateTime<Utc>>,
    pub parameters: BTreeMap<String, Vec<String>>,
    pub outputs: BTreeMap<String, Vec<String>>,
    pub mode: String,
    pub target: Option<String>,
    pub targets: Vec<serde_json::Value>,
    pub max_concurrency: Option<String>,
    pub max_errors: Option<String>,
    pub executed_by: String,
    pub step_executions: Vec<AutomationStepExecution>,
    pub automation_subtype: Option<String>,
    pub runbooks: Vec<serde_json::Value>,
    pub change_request_name: Option<String>,
    pub scheduled_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AutomationStepExecution {
    pub step_name: String,
    pub action: String,
    pub step_status: String,
    pub execution_start_time: Option<DateTime<Utc>>,
    pub execution_end_time: Option<DateTime<Utc>>,
    pub inputs: BTreeMap<String, String>,
    pub outputs: BTreeMap<String, Vec<String>>,
    pub step_execution_id: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SsmSession {
    pub session_id: String,
    pub target: String,
    pub status: String,
    pub start_date: DateTime<Utc>,
    pub end_date: Option<DateTime<Utc>>,
    pub owner: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SsmActivation {
    pub activation_id: String,
    pub iam_role: String,
    pub registration_limit: i64,
    pub registrations_count: i64,
    pub expiration_date: Option<DateTime<Utc>>,
    pub description: Option<String>,
    pub default_instance_name: Option<String>,
    pub created_date: DateTime<Utc>,
    pub expired: bool,
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ManagedInstance {
    pub instance_id: String,
    pub activation_id: Option<String>,
    pub iam_role: String,
    pub ping_status: String,
    pub platform_type: String,
    pub platform_name: String,
    pub platform_version: String,
    pub agent_version: String,
    pub last_ping_date_time: DateTime<Utc>,
    pub registration_date: DateTime<Utc>,
    pub resource_type: String,
    pub computer_name: String,
    pub ip_address: String,
    pub is_latest_version: bool,
    pub association_status: Option<String>,
    pub source_id: Option<String>,
    pub source_type: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExecutionPreview {
    pub execution_preview_id: String,
    pub document_name: String,
    pub status: String,
    pub created_time: DateTime<Utc>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SsmState {
    pub account_id: String,
    pub region: String,
    pub parameters: BTreeMap<String, SsmParameter>, // name -> param (BTreeMap for path queries)
    pub documents: BTreeMap<String, SsmDocument>,
    pub commands: Vec<SsmCommand>,
    pub maintenance_windows: BTreeMap<String, MaintenanceWindow>,
    pub patch_baselines: BTreeMap<String, PatchBaseline>,
    pub patch_groups: Vec<PatchGroup>,
    pub associations: BTreeMap<String, SsmAssociation>,
    pub ops_items: BTreeMap<String, SsmOpsItem>,
    pub resource_policies: Vec<SsmResourcePolicy>,
    pub service_settings: BTreeMap<String, SsmServiceSetting>,
    pub default_patch_baseline_id: Option<String>,
    pub ops_item_counter: u64,
    pub maintenance_window_executions: Vec<MaintenanceWindowExecution>,
    pub inventory_entries: BTreeMap<String, InventoryEntry>, // instance_id -> entry
    pub inventory_deletions: Vec<InventoryDeletion>,
    pub compliance_items: Vec<ComplianceItem>,
    pub resource_data_syncs: BTreeMap<String, ResourceDataSync>,
    pub mw_execution_counter: u64,
    pub inventory_deletion_counter: u64,
    pub ops_item_related_items: Vec<OpsItemRelatedItem>,
    pub ops_item_related_item_counter: u64,
    pub ops_item_events: Vec<OpsItemEvent>,
    pub ops_metadata: BTreeMap<String, OpsMetadataEntry>,
    pub automation_executions: BTreeMap<String, AutomationExecution>,
    pub automation_execution_counter: u64,
    pub sessions: BTreeMap<String, SsmSession>,
    pub session_counter: u64,
    pub activations: BTreeMap<String, SsmActivation>,
    pub activation_counter: u64,
    pub managed_instances: BTreeMap<String, ManagedInstance>,
    pub execution_previews: BTreeMap<String, ExecutionPreview>,
    pub execution_preview_counter: u64,
    /// Local log of parameter-policy notification events. Real AWS sends
    /// these to EventBridge; we record them in-memory so tests can
    /// inspect notification fan-out via the admin endpoint. Defaults to
    /// empty when deserializing snapshots from before this field
    /// existed.
    #[serde(default)]
    pub parameter_policy_events: Vec<ParameterPolicyEvent>,
}

/// One emission of a parameter-policy notification (Expiration/
/// ExpirationNotification/NoChangeNotification). Captured at PutParameter
/// time and at read time when an Expiration ages out a parameter.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ParameterPolicyEvent {
    pub parameter_name: String,
    pub parameter_arn: String,
    pub event_type: String,
    pub message: String,
    pub created_at: DateTime<Utc>,
}

impl SsmState {
    pub fn new(account_id: &str, region: &str) -> Self {
        let mut state = Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            parameters: BTreeMap::new(),
            documents: BTreeMap::new(),
            commands: Vec::new(),
            maintenance_windows: BTreeMap::new(),
            patch_baselines: BTreeMap::new(),
            patch_groups: Vec::new(),
            associations: BTreeMap::new(),
            ops_items: BTreeMap::new(),
            resource_policies: Vec::new(),
            service_settings: BTreeMap::new(),
            default_patch_baseline_id: None,
            ops_item_counter: 0,
            maintenance_window_executions: Vec::new(),
            inventory_entries: BTreeMap::new(),
            inventory_deletions: Vec::new(),
            compliance_items: Vec::new(),
            resource_data_syncs: BTreeMap::new(),
            mw_execution_counter: 0,
            inventory_deletion_counter: 0,
            ops_item_related_items: Vec::new(),
            ops_item_related_item_counter: 0,
            ops_item_events: Vec::new(),
            ops_metadata: BTreeMap::new(),
            automation_executions: BTreeMap::new(),
            automation_execution_counter: 0,
            sessions: BTreeMap::new(),
            session_counter: 0,
            activations: BTreeMap::new(),
            activation_counter: 0,
            managed_instances: BTreeMap::new(),
            execution_previews: BTreeMap::new(),
            execution_preview_counter: 0,
            parameter_policy_events: Vec::new(),
        };
        state.seed_defaults();
        state
    }

    pub fn reset(&mut self) {
        self.parameters.clear();
        self.documents.clear();
        self.commands.clear();
        self.maintenance_windows.clear();
        self.patch_baselines.clear();
        self.patch_groups.clear();
        self.associations.clear();
        self.ops_items.clear();
        self.resource_policies.clear();
        self.service_settings.clear();
        self.default_patch_baseline_id = None;
        self.ops_item_counter = 0;
        self.maintenance_window_executions.clear();
        self.inventory_entries.clear();
        self.inventory_deletions.clear();
        self.compliance_items.clear();
        self.resource_data_syncs.clear();
        self.mw_execution_counter = 0;
        self.inventory_deletion_counter = 0;
        self.ops_item_related_items.clear();
        self.ops_item_related_item_counter = 0;
        self.ops_item_events.clear();
        self.ops_metadata.clear();
        self.automation_executions.clear();
        self.automation_execution_counter = 0;
        self.sessions.clear();
        self.session_counter = 0;
        self.activations.clear();
        self.activation_counter = 0;
        self.managed_instances.clear();
        self.execution_previews.clear();
        self.execution_preview_counter = 0;
        self.parameter_policy_events.clear();
        self.seed_defaults();
    }

    fn seed_defaults(&mut self) {
        let now = chrono::Utc::now();

        // Seed region parameters
        let regions: &[(&str, &str)] = &[
            ("af-south-1", "Africa (Cape Town)"),
            ("ap-east-1", "Asia Pacific (Hong Kong)"),
            ("ap-northeast-1", "Asia Pacific (Tokyo)"),
            ("ap-northeast-2", "Asia Pacific (Seoul)"),
            ("ap-northeast-3", "Asia Pacific (Osaka)"),
            ("ap-south-1", "Asia Pacific (Mumbai)"),
            ("ap-south-2", "Asia Pacific (Hyderabad)"),
            ("ap-southeast-1", "Asia Pacific (Singapore)"),
            ("ap-southeast-2", "Asia Pacific (Sydney)"),
            ("ap-southeast-3", "Asia Pacific (Jakarta)"),
            ("ca-central-1", "Canada (Central)"),
            ("eu-central-1", "Europe (Frankfurt)"),
            ("eu-central-2", "Europe (Zurich)"),
            ("eu-north-1", "Europe (Stockholm)"),
            ("eu-south-1", "Europe (Milan)"),
            ("eu-south-2", "Europe (Spain)"),
            ("eu-west-1", "Europe (Ireland)"),
            ("eu-west-2", "Europe (London)"),
            ("eu-west-3", "Europe (Paris)"),
            ("me-central-1", "Middle East (UAE)"),
            ("me-south-1", "Middle East (Bahrain)"),
            ("sa-east-1", "South America (Sao Paulo)"),
            ("us-east-1", "US East (N. Virginia)"),
            ("us-east-2", "US East (Ohio)"),
            ("us-west-1", "US West (N. California)"),
            ("us-west-2", "US West (Oregon)"),
        ];

        for (region_code, long_name) in regions {
            let base_path = format!("/aws/service/global-infrastructure/regions/{region_code}");
            self.insert_default_param(&base_path, region_code, now);
            self.insert_default_param(&format!("{base_path}/longName"), long_name, now);
            self.insert_default_param(&format!("{base_path}/domain"), "amazonaws.com", now);
            self.insert_default_param(&format!("{base_path}/geolocationRegion"), region_code, now);
            let country = match region_code.split('-').next().unwrap_or("") {
                "us" => "US",
                "eu" => "DE",
                "ap" => "JP",
                "sa" => "BR",
                "ca" => "CA",
                "me" => "BH",
                "af" => "ZA",
                "il" => "IL",
                _ => "US",
            };
            self.insert_default_param(&format!("{base_path}/geolocationCountry"), country, now);
            self.insert_default_param(&format!("{base_path}/partition"), "aws", now);
        }

        // Seed service parameters
        let services = [
            "acm",
            "apigateway",
            "autoscaling",
            "cloudformation",
            "cloudfront",
            "cloudwatch",
            "codebuild",
            "codecommit",
            "codedeploy",
            "dynamodb",
            "ec2",
            "ecr",
            "ecs",
            "eks",
            "elasticache",
            "elasticbeanstalk",
            "elasticloadbalancing",
            "es",
            "events",
            "firehose",
            "iam",
            "kinesis",
            "kms",
            "lambda",
            "logs",
            "rds",
            "redshift",
            "route53",
            "s3",
            "ses",
            "sns",
            "sqs",
            "ssm",
            "sts",
        ];
        for svc in &services {
            let name = format!("/aws/service/global-infrastructure/services/{svc}");
            self.insert_default_param(&name, svc, now);
        }

        // Seed AMI parameters (10 entries per region)
        let ami_names = [
            "al2023-ami-kernel-default-x86_64",
            "al2023-ami-kernel-default-arm64",
            "al2023-ami-minimal-kernel-default-x86_64",
            "al2023-ami-minimal-kernel-default-arm64",
            "amzn2-ami-hvm-x86_64-gp2",
            "amzn2-ami-hvm-arm64-gp2",
            "amzn2-ami-kernel-5.10-hvm-x86_64-gp2",
            "amzn2-ami-kernel-5.10-hvm-arm64-gp2",
            "amzn2-ami-minimal-hvm-x86_64-ebs",
            "amzn2-ami-minimal-hvm-arm64-ebs",
        ];

        // Generate region-specific AMI IDs using a simple hash
        for (i, ami_name) in ami_names.iter().enumerate() {
            let name = format!("/aws/service/ami-amazon-linux-latest/{ami_name}");
            let ami_id = format!(
                "ami-{:017x}",
                // Simple region-specific hash
                {
                    let mut h: u64 = 0xcbf29ce484222325;
                    for b in self.region.as_bytes() {
                        h ^= *b as u64;
                        h = h.wrapping_mul(0x100000001b3);
                    }
                    for b in ami_name.as_bytes() {
                        h ^= *b as u64;
                        h = h.wrapping_mul(0x100000001b3);
                    }
                    h.wrapping_add(i as u64)
                }
            );
            self.insert_default_param(&name, &ami_id, now);
        }
    }

    fn insert_default_param(&mut self, name: &str, value: &str, now: DateTime<Utc>) {
        let arn = if name.starts_with('/') {
            format!(
                "arn:aws:ssm:{}:{}:parameter{}",
                self.region, self.account_id, name
            )
        } else {
            format!(
                "arn:aws:ssm:{}:{}:parameter/{}",
                self.region, self.account_id, name
            )
        };
        self.parameters.insert(
            name.to_string(),
            SsmParameter {
                name: name.to_string(),
                value: value.to_string(),
                param_type: "String".to_string(),
                version: 1,
                arn,
                last_modified: now,
                history: Vec::new(),
                tags: BTreeMap::new(),
                labels: BTreeMap::new(),
                description: None,
                allowed_pattern: None,
                key_id: None,
                data_type: "text".to_string(),
                tier: "Standard".to_string(),
                policies: None,
                expiration_notified: false,
                no_change_notified: false,
            },
        );
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MaintenanceWindowExecution {
    pub window_execution_id: String,
    pub window_id: String,
    pub status: String,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub tasks: Vec<MaintenanceWindowExecutionTask>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MaintenanceWindowExecutionTask {
    pub task_execution_id: String,
    pub window_execution_id: String,
    pub task_arn: String,
    pub task_type: String,
    pub status: String,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub invocations: Vec<MaintenanceWindowExecutionTaskInvocation>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MaintenanceWindowExecutionTaskInvocation {
    pub invocation_id: String,
    pub task_execution_id: String,
    pub window_execution_id: String,
    pub execution_id: Option<String>,
    pub status: String,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub parameters: Option<String>,
    pub owner_information: Option<String>,
    pub window_target_id: Option<String>,
    pub status_details: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct InventoryItem {
    pub type_name: String,
    pub schema_version: String,
    pub capture_time: String,
    pub content: Vec<BTreeMap<String, String>>,
    pub content_hash: Option<String>,
    pub context: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct InventoryEntry {
    pub instance_id: String,
    pub items: Vec<InventoryItem>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct InventoryDeletion {
    pub deletion_id: String,
    pub type_name: String,
    pub deletion_start_time: DateTime<Utc>,
    pub last_status: String,
    pub last_status_message: String,
    pub deletion_summary: serde_json::Value,
    pub last_status_update_time: DateTime<Utc>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ComplianceItem {
    pub resource_id: String,
    pub resource_type: String,
    pub compliance_type: String,
    pub severity: String,
    pub status: String,
    pub title: Option<String>,
    pub id: Option<String>,
    pub details: BTreeMap<String, String>,
    pub execution_summary: serde_json::Value,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ResourceDataSync {
    pub sync_name: String,
    pub sync_type: Option<String>,
    pub sync_source: Option<serde_json::Value>,
    pub s3_destination: Option<serde_json::Value>,
    pub created_date: DateTime<Utc>,
    pub last_sync_time: Option<DateTime<Utc>>,
    pub last_successful_sync_time: Option<DateTime<Utc>>,
    pub last_status: String,
    pub sync_last_modified_time: DateTime<Utc>,
}

pub type SharedSsmState = Arc<RwLock<fakecloud_core::multi_account::MultiAccountState<SsmState>>>;

impl fakecloud_core::multi_account::AccountState for SsmState {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }
}

/// On-disk snapshot envelope for SSM state. Versioned so format
/// changes fail loudly on upgrade.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SsmSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<fakecloud_core::multi_account::MultiAccountState<SsmState>>,
    #[serde(default)]
    pub state: Option<SsmState>,
}

pub const SSM_SNAPSHOT_SCHEMA_VERSION: u32 = 2;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_initializes() {
        let state = SsmState::new("123456789012", "us-east-1");
        assert_eq!(state.account_id, "123456789012");
        assert_eq!(state.region, "us-east-1");
    }

    #[test]
    fn new_seeds_default_region_parameters() {
        let state = SsmState::new("123456789012", "us-east-1");
        let region_key = "/aws/service/global-infrastructure/regions/us-east-1";
        assert!(state.parameters.contains_key(region_key));
        let long_key = format!("{region_key}/longName");
        assert!(state.parameters.contains_key(&long_key));
    }

    #[test]
    fn new_seeds_default_service_parameters() {
        let state = SsmState::new("123456789012", "us-east-1");
        let key = "/aws/service/global-infrastructure/services/lambda";
        assert!(state.parameters.contains_key(key));
    }

    #[test]
    fn reset_reseeds_defaults() {
        let mut state = SsmState::new("123456789012", "us-east-1");
        state.parameters.clear();
        state.documents.clear();
        state.ops_item_counter = 42;
        state.reset();
        // Defaults re-seeded
        let key = "/aws/service/global-infrastructure/services/s3";
        assert!(state.parameters.contains_key(key));
        assert_eq!(state.ops_item_counter, 0);
    }

    #[test]
    fn reset_clears_ephemeral_counters() {
        let mut state = SsmState::new("123456789012", "us-east-1");
        state.mw_execution_counter = 7;
        state.automation_execution_counter = 3;
        state.session_counter = 9;
        state.activation_counter = 2;
        state.execution_preview_counter = 5;
        state.reset();
        assert_eq!(state.mw_execution_counter, 0);
        assert_eq!(state.automation_execution_counter, 0);
        assert_eq!(state.session_counter, 0);
        assert_eq!(state.activation_counter, 0);
        assert_eq!(state.execution_preview_counter, 0);
    }
}
