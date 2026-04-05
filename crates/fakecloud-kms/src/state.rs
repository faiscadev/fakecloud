use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

pub type SharedKmsState = Arc<RwLock<KmsState>>;

pub struct KmsState {
    pub account_id: String,
    pub region: String,
    pub keys: HashMap<String, KmsKey>,
    pub aliases: HashMap<String, KmsAlias>,
    pub grants: Vec<KmsGrant>,
}

impl KmsState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            keys: HashMap::new(),
            aliases: HashMap::new(),
            grants: Vec::new(),
        }
    }

    pub fn reset(&mut self) {
        self.keys.clear();
        self.aliases.clear();
        self.grants.clear();
    }
}

pub struct KmsKey {
    pub key_id: String,
    pub arn: String,
    pub creation_date: f64,
    pub description: String,
    pub enabled: bool,
    pub key_usage: String,
    pub key_spec: String,
    pub key_manager: String,
    pub key_state: String,
    pub deletion_date: Option<f64>,
    pub tags: HashMap<String, String>,
    pub policy: String,
    pub key_rotation_enabled: bool,
    pub origin: String,
    pub multi_region: bool,
    pub rotations: Vec<KeyRotation>,
    pub signing_algorithms: Option<Vec<String>>,
    pub encryption_algorithms: Option<Vec<String>>,
    pub mac_algorithms: Option<Vec<String>>,
}

pub struct KmsAlias {
    pub alias_name: String,
    pub alias_arn: String,
    pub target_key_id: String,
    pub creation_date: f64,
}

pub struct KmsGrant {
    pub grant_id: String,
    pub grant_token: String,
    pub key_id: String,
    pub grantee_principal: String,
    pub retiring_principal: Option<String>,
    pub operations: Vec<String>,
    pub constraints: Option<serde_json::Value>,
    pub name: Option<String>,
    pub creation_date: f64,
}

pub struct KeyRotation {
    pub key_id: String,
    pub rotation_date: f64,
    pub rotation_type: String,
}
