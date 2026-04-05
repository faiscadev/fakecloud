use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

pub type SharedKmsState = Arc<RwLock<KmsState>>;

pub struct KmsState {
    pub account_id: String,
    pub region: String,
    pub keys: HashMap<String, KmsKey>,
    pub aliases: HashMap<String, KmsAlias>,
}

impl KmsState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            keys: HashMap::new(),
            aliases: HashMap::new(),
        }
    }

    pub fn reset(&mut self) {
        self.keys.clear();
        self.aliases.clear();
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
}

pub struct KmsAlias {
    pub alias_name: String,
    pub alias_arn: String,
    pub target_key_id: String,
    pub creation_date: f64,
}
