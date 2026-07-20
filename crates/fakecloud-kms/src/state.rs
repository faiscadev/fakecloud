use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;

pub type SharedKmsState = Arc<RwLock<fakecloud_core::multi_account::MultiAccountState<KmsState>>>;

impl fakecloud_core::multi_account::AccountState for KmsState {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct KmsState {
    pub account_id: String,
    pub region: String,
    pub keys: BTreeMap<String, KmsKey>,
    pub aliases: BTreeMap<String, KmsAlias>,
    pub grants: Vec<KmsGrant>,
    pub custom_key_stores: BTreeMap<String, CustomKeyStore>,
    /// Per-account master key bytes (32 bytes for AES-256-GCM) used to
    /// wrap plaintext in the AWS-shaped ciphertext blob format. Generated
    /// lazily on first encrypt and persisted alongside the rest of the
    /// state so that ciphertexts produced before a server restart still
    /// decrypt afterwards.
    #[serde(default = "default_master_key_bytes_on_load")]
    pub master_key_bytes: Vec<u8>,
    /// In-flight RSA wrapping keypairs handed out by GetParametersForImport
    /// and consumed by ImportKeyMaterial to RSA-OAEP-unwrap the encrypted
    /// key material. Keyed by the import token bytes returned to the
    /// caller; entries are removed after a successful import.
    #[serde(default)]
    pub import_wrapping_keys: BTreeMap<String, ImportWrapEntry>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ImportWrapEntry {
    /// PKCS#8 DER-encoded RSA-2048 private key half of the wrapping
    /// keypair. The corresponding SubjectPublicKeyInfo DER was returned
    /// to the caller in `GetParametersForImport.PublicKey` so they can
    /// RSA-OAEP-encrypt the key material under it.
    pub private_key_der: Vec<u8>,
    /// CMK key id this token is bound to. ImportKeyMaterial rejects the
    /// token when it doesn't match the CMK in the request.
    pub key_id: String,
}

fn default_master_key_bytes() -> Vec<u8> {
    use aes_gcm::aead::rand_core::RngCore;
    use aes_gcm::aead::OsRng;
    let mut bytes = vec![0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    bytes
}

/// serde `default` for [`KmsState::master_key_bytes`], invoked ONLY when a
/// persisted snapshot is deserialized WITHOUT the field — e.g. a state file
/// written by a fakecloud build predating per-account master keys. Silently
/// regenerating the key would make every ciphertext blob produced by that
/// older build undecryptable (`Decrypt` -> `InvalidCiphertextException`) with
/// no indication why, so we log a loud warning to surface the data-integrity
/// break. Fresh accounts go through [`KmsState::new`], which calls
/// [`default_master_key_bytes`] directly and stays quiet — only the on-load
/// default path warns.
fn default_master_key_bytes_on_load() -> Vec<u8> {
    tracing::warn!(
        "KMS state snapshot was missing the per-account master key; generating \
         a fresh one. Ciphertext produced before this upgrade can NO LONGER be \
         decrypted (Decrypt will fail with InvalidCiphertextException)."
    );
    default_master_key_bytes()
}

impl KmsState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            keys: BTreeMap::new(),
            aliases: BTreeMap::new(),
            grants: Vec::new(),
            custom_key_stores: BTreeMap::new(),
            master_key_bytes: default_master_key_bytes(),
            import_wrapping_keys: BTreeMap::new(),
        }
    }

    pub fn reset(&mut self) {
        self.keys.clear();
        self.aliases.clear();
        self.grants.clear();
        self.custom_key_stores.clear();
        // Keep the master key across resets so ciphertexts still decrypt.
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
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
    pub tags: BTreeMap<String, String>,
    pub policy: String,
    pub key_rotation_enabled: bool,
    /// Customer-specified rotation cadence from EnableKeyRotation
    /// (`RotationPeriodInDays`, 90..2560). `None` means AWS's 365-day
    /// default. Echoed by GetKeyRotationStatus.
    #[serde(default)]
    pub rotation_period_in_days: Option<i32>,
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
    /// PKCS#8 DER-encoded private key, populated for asymmetric specs
    /// (RSA_2048/3072/4096, ECC_*) at CreateKey time. None for
    /// symmetric / HMAC specs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub asymmetric_private_key_der: Option<Vec<u8>>,
    /// SubjectPublicKeyInfo DER-encoded public key. Returned by
    /// GetPublicKey verbatim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub asymmetric_public_key_der: Option<Vec<u8>>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct KmsAlias {
    pub alias_name: String,
    pub alias_arn: String,
    pub target_key_id: String,
    pub creation_date: f64,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct KeyRotation {
    pub key_id: String,
    pub rotation_date: f64,
    pub rotation_type: String,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
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

/// On-disk snapshot envelope for KMS state. Versioned so format
/// changes fail loudly on upgrade.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct KmsSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<fakecloud_core::multi_account::MultiAccountState<KmsState>>,
    #[serde(default)]
    pub state: Option<KmsState>,
}

pub const KMS_SNAPSHOT_SCHEMA_VERSION: u32 = 2;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_has_empty_collections() {
        let state = KmsState::new("123456789012", "us-east-1");
        assert_eq!(state.account_id, "123456789012");
        assert_eq!(state.region, "us-east-1");
        assert!(state.keys.is_empty());
        assert!(state.aliases.is_empty());
        assert!(state.grants.is_empty());
        assert!(state.custom_key_stores.is_empty());
    }

    #[test]
    fn reset_clears_collections() {
        let mut state = KmsState::new("123456789012", "us-east-1");
        state.aliases.insert(
            "alias/test".to_string(),
            KmsAlias {
                alias_name: "alias/test".to_string(),
                alias_arn: "arn".to_string(),
                target_key_id: "k".to_string(),
                creation_date: 0.0,
            },
        );
        assert!(!state.aliases.is_empty());
        state.reset();
        assert!(state.aliases.is_empty());
    }

    #[test]
    fn snapshot_with_master_key_preserves_it() {
        // A snapshot that carries the master key must round-trip it byte-for-byte
        // so ciphertext produced before a restart still decrypts.
        let mut original = KmsState::new("123456789012", "us-east-1");
        original.master_key_bytes = vec![7u8; 32];
        let json = serde_json::to_string(&original).unwrap();
        let loaded: KmsState = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.master_key_bytes, vec![7u8; 32]);
    }

    #[test]
    fn snapshot_missing_master_key_is_defaulted_on_load() {
        // An older snapshot with no `master_key_bytes` (predating per-account
        // master keys) must still deserialize, with a freshly generated 32-byte
        // key via the on-load default (which also logs a loud warning that the
        // prior ciphertext is now undecryptable).
        let json = r#"{
            "account_id": "123456789012",
            "region": "us-east-1",
            "keys": {},
            "aliases": {},
            "grants": [],
            "custom_key_stores": {}
        }"#;
        let loaded: KmsState = serde_json::from_str(json).unwrap();
        assert_eq!(loaded.master_key_bytes.len(), 32);
        assert!(loaded.import_wrapping_keys.is_empty());
    }
}
