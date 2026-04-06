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
    pub custom_key_stores: HashMap<String, CustomKeyStore>,
}

impl KmsState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            keys: HashMap::new(),
            aliases: HashMap::new(),
            grants: Vec::new(),
            custom_key_stores: HashMap::new(),
        }
    }

    pub fn reset(&mut self) {
        self.keys.clear();
        self.aliases.clear();
        self.grants.clear();
        self.custom_key_stores.clear();
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
    pub custom_key_store_id: Option<String>,
    pub imported_key_material: bool,
    /// Raw bytes of imported key material (used as AES key for encrypt/decrypt).
    pub imported_material_bytes: Option<Vec<u8>>,
    /// Deterministic seed for the key (used for DeriveSharedSecret).
    pub private_key_seed: Vec<u8>,
    pub primary_region: Option<String>,
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

pub struct CustomKeyStore {
    pub custom_key_store_id: String,
    pub custom_key_store_name: String,
    pub custom_key_store_type: String,
    pub cloud_hsm_cluster_id: Option<String>,
    pub trust_anchor_certificate: Option<String>,
    pub connection_state: String,
    pub creation_date: f64,
    pub xks_proxy_uri_endpoint: Option<String>,
    pub xks_proxy_uri_path: Option<String>,
    pub xks_proxy_vpc_endpoint_service_name: Option<String>,
    pub xks_proxy_connectivity: Option<String>,
}
