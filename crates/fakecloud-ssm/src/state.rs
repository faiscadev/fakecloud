use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct SsmParameter {
    pub name: String,
    pub value: String,
    pub param_type: String, // String, StringList, SecureString
    pub version: i64,
    pub arn: String,
    pub last_modified: DateTime<Utc>,
    pub history: Vec<SsmParameterVersion>,
    pub tags: HashMap<String, String>,
    pub labels: HashMap<i64, Vec<String>>, // version -> labels
    pub description: Option<String>,
    pub allowed_pattern: Option<String>,
    pub key_id: Option<String>,
    pub data_type: String, // "text" or "aws:ec2:image"
    pub tier: String,      // "Standard", "Advanced", "Intelligent-Tiering"
    pub policies: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SsmParameterVersion {
    pub value: String,
    pub version: i64,
    pub last_modified: DateTime<Utc>,
    pub param_type: String,
    pub description: Option<String>,
    pub key_id: Option<String>,
    pub labels: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SsmDocument {
    pub name: String,
    pub content: String,
    pub document_type: String,
    pub document_format: String,
    pub target_type: Option<String>,
    pub version_name: Option<String>,
    pub tags: HashMap<String, String>,
    pub versions: Vec<SsmDocumentVersion>,
    pub default_version: String,
    pub latest_version: String,
    pub created_date: DateTime<Utc>,
    pub owner: String,
    pub status: String,
    pub permissions: HashMap<String, Vec<String>>, // permission_type -> account_ids
}

#[derive(Debug, Clone)]
pub struct SsmDocumentVersion {
    pub content: String,
    pub document_version: String,
    pub version_name: Option<String>,
    pub created_date: DateTime<Utc>,
    pub status: String,
    pub document_format: String,
    pub is_default_version: bool,
}

#[derive(Debug, Clone)]
pub struct SsmCommand {
    pub command_id: String,
    pub document_name: String,
    pub instance_ids: Vec<String>,
    pub parameters: HashMap<String, Vec<String>>,
    pub status: String,
    pub requested_date_time: DateTime<Utc>,
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
}

#[derive(Debug, Clone)]
pub struct MaintenanceWindowTarget {
    pub window_target_id: String,
    pub window_id: String,
    pub resource_type: String,
    pub targets: Vec<serde_json::Value>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub owner_information: Option<String>,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct MaintenanceWindow {
    pub id: String,
    pub name: String,
    pub schedule: String,
    pub duration: i64,
    pub cutoff: i64,
    pub allow_unassociated_targets: bool,
    pub enabled: bool,
    pub description: Option<String>,
    pub tags: HashMap<String, String>,
    pub targets: Vec<MaintenanceWindowTarget>,
    pub tasks: Vec<MaintenanceWindowTask>,
    pub schedule_timezone: Option<String>,
    pub schedule_offset: Option<i64>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub client_token: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PatchBaseline {
    pub id: String,
    pub name: String,
    pub operating_system: String,
    pub description: Option<String>,
    pub approval_rules: Option<serde_json::Value>,
    pub approved_patches: Vec<String>,
    pub rejected_patches: Vec<String>,
    pub tags: HashMap<String, String>,
    pub approved_patches_compliance_level: String,
    pub rejected_patches_action: String,
    pub global_filters: Option<serde_json::Value>,
    pub sources: Vec<serde_json::Value>,
    pub approved_patches_enable_non_security: bool,
    pub available_security_updates_compliance_status: Option<String>,
    pub client_token: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PatchGroup {
    pub baseline_id: String,
    pub patch_group: String,
}

#[derive(Debug, Clone)]
pub struct SsmAssociation {
    pub association_id: String,
    pub name: String, // document name
    pub targets: Vec<serde_json::Value>,
    pub schedule_expression: Option<String>,
    pub parameters: HashMap<String, Vec<String>>,
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
    pub tags: HashMap<String, String>,
    pub status: String,
    pub status_date: DateTime<Utc>,
    pub overview: serde_json::Value,
    pub created_date: DateTime<Utc>,
    pub last_update_association_date: DateTime<Utc>,
    pub last_execution_date: Option<DateTime<Utc>>,
    pub instance_id: Option<String>,
    pub versions: Vec<SsmAssociationVersion>,
}

#[derive(Debug, Clone)]
pub struct SsmAssociationVersion {
    pub version: i64,
    pub name: String,
    pub targets: Vec<serde_json::Value>,
    pub schedule_expression: Option<String>,
    pub parameters: HashMap<String, Vec<String>>,
    pub document_version: Option<String>,
    pub created_date: DateTime<Utc>,
    pub association_name: Option<String>,
    pub max_errors: Option<String>,
    pub max_concurrency: Option<String>,
    pub compliance_severity: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SsmOpsItem {
    pub ops_item_id: String,
    pub title: String,
    pub description: Option<String>,
    pub source: String,
    pub status: String,
    pub priority: Option<i64>,
    pub severity: Option<String>,
    pub category: Option<String>,
    pub operational_data: HashMap<String, serde_json::Value>,
    pub notifications: Vec<serde_json::Value>,
    pub related_ops_items: Vec<serde_json::Value>,
    pub tags: HashMap<String, String>,
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

#[derive(Debug, Clone)]
pub struct SsmResourcePolicy {
    pub policy_id: String,
    pub policy_hash: String,
    pub policy: String,
    pub resource_arn: String,
}

#[derive(Debug, Clone)]
pub struct SsmServiceSetting {
    pub setting_id: String,
    pub setting_value: String,
    pub last_modified_date: DateTime<Utc>,
    pub last_modified_user: String,
    pub status: String,
}

pub struct SsmState {
    pub account_id: String,
    pub region: String,
    pub parameters: BTreeMap<String, SsmParameter>, // name -> param (BTreeMap for path queries)
    pub documents: BTreeMap<String, SsmDocument>,
    pub commands: Vec<SsmCommand>,
    pub maintenance_windows: HashMap<String, MaintenanceWindow>,
    pub patch_baselines: HashMap<String, PatchBaseline>,
    pub patch_groups: Vec<PatchGroup>,
    pub associations: HashMap<String, SsmAssociation>,
    pub ops_items: HashMap<String, SsmOpsItem>,
    pub resource_policies: Vec<SsmResourcePolicy>,
    pub service_settings: HashMap<String, SsmServiceSetting>,
    pub default_patch_baseline_id: Option<String>,
    pub ops_item_counter: u64,
    pub maintenance_window_executions: Vec<MaintenanceWindowExecution>,
    pub inventory_entries: HashMap<String, InventoryEntry>, // instance_id -> entry
    pub inventory_deletions: Vec<InventoryDeletion>,
    pub compliance_items: Vec<ComplianceItem>,
    pub resource_data_syncs: HashMap<String, ResourceDataSync>,
    pub mw_execution_counter: u64,
    pub inventory_deletion_counter: u64,
}

impl SsmState {
    pub fn new(account_id: &str, region: &str) -> Self {
        let mut state = Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            parameters: BTreeMap::new(),
            documents: BTreeMap::new(),
            commands: Vec::new(),
            maintenance_windows: HashMap::new(),
            patch_baselines: HashMap::new(),
            patch_groups: Vec::new(),
            associations: HashMap::new(),
            ops_items: HashMap::new(),
            resource_policies: Vec::new(),
            service_settings: HashMap::new(),
            default_patch_baseline_id: None,
            ops_item_counter: 0,
            maintenance_window_executions: Vec::new(),
            inventory_entries: HashMap::new(),
            inventory_deletions: Vec::new(),
            compliance_items: Vec::new(),
            resource_data_syncs: HashMap::new(),
            mw_execution_counter: 0,
            inventory_deletion_counter: 0,
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
                tags: HashMap::new(),
                labels: HashMap::new(),
                description: None,
                allowed_pattern: None,
                key_id: None,
                data_type: "text".to_string(),
                tier: "Standard".to_string(),
                policies: None,
            },
        );
    }
}

#[derive(Debug, Clone)]
pub struct MaintenanceWindowExecution {
    pub window_execution_id: String,
    pub window_id: String,
    pub status: String,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub tasks: Vec<MaintenanceWindowExecutionTask>,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct InventoryItem {
    pub type_name: String,
    pub schema_version: String,
    pub capture_time: String,
    pub content: Vec<HashMap<String, String>>,
    pub content_hash: Option<String>,
    pub context: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone)]
pub struct InventoryEntry {
    pub instance_id: String,
    pub items: Vec<InventoryItem>,
}

#[derive(Debug, Clone)]
pub struct InventoryDeletion {
    pub deletion_id: String,
    pub type_name: String,
    pub deletion_start_time: DateTime<Utc>,
    pub last_status: String,
    pub last_status_message: String,
    pub deletion_summary: serde_json::Value,
    pub last_status_update_time: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ComplianceItem {
    pub resource_id: String,
    pub resource_type: String,
    pub compliance_type: String,
    pub severity: String,
    pub status: String,
    pub title: Option<String>,
    pub id: Option<String>,
    pub details: HashMap<String, String>,
    pub execution_summary: serde_json::Value,
}

#[derive(Debug, Clone)]
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

pub type SharedSsmState = Arc<RwLock<SsmState>>;
