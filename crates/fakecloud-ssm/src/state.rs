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

pub struct SsmState {
    pub account_id: String,
    pub region: String,
    pub parameters: BTreeMap<String, SsmParameter>, // name -> param (BTreeMap for path queries)
    pub documents: BTreeMap<String, SsmDocument>,
    pub commands: Vec<SsmCommand>,
    pub maintenance_windows: HashMap<String, MaintenanceWindow>,
    pub patch_baselines: HashMap<String, PatchBaseline>,
    pub patch_groups: Vec<PatchGroup>,
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

pub type SharedSsmState = Arc<RwLock<SsmState>>;
