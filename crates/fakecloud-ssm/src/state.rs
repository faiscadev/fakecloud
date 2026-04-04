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
    pub timeout_seconds: Option<i64>,
    pub service_role_arn: Option<String>,
    pub notification_config: Option<serde_json::Value>,
    pub targets: Vec<serde_json::Value>,
}

pub struct SsmState {
    pub account_id: String,
    pub region: String,
    pub parameters: BTreeMap<String, SsmParameter>, // name -> param (BTreeMap for path queries)
    pub documents: BTreeMap<String, SsmDocument>,
    pub commands: Vec<SsmCommand>,
}

impl SsmState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            parameters: BTreeMap::new(),
            documents: BTreeMap::new(),
            commands: Vec::new(),
        }
    }

    pub fn reset(&mut self) {
        self.parameters.clear();
    }
}

pub type SharedSsmState = Arc<RwLock<SsmState>>;
