use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Secret {
    pub name: String,
    pub arn: String,
    pub description: Option<String>,
    pub kms_key_id: Option<String>,
    pub versions: BTreeMap<String, SecretVersion>,
    pub current_version_id: Option<String>,
    pub tags: Vec<(String, String)>,
    pub tags_ever_set: bool,
    pub deleted: bool,
    pub deletion_date: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub last_changed_at: DateTime<Utc>,
    pub last_accessed_at: Option<DateTime<Utc>>,
    pub rotation_enabled: Option<bool>,
    pub rotation_lambda_arn: Option<String>,
    pub rotation_rules: Option<RotationRules>,
    pub last_rotated_at: Option<DateTime<Utc>>,
    pub resource_policy: Option<String>,
    /// Replica regions added via ReplicateSecretToRegions. Reflected in
    /// ReplicationStatus on describe/replicate responses.
    #[serde(default)]
    pub replica_regions: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RotationRules {
    pub automatically_after_days: Option<i64>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SecretVersion {
    pub version_id: String,
    pub secret_string: Option<String>,
    pub secret_binary: Option<Vec<u8>>,
    pub stages: Vec<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SecretsManagerState {
    pub account_id: String,
    pub region: String,
    pub secrets: BTreeMap<String, Secret>,
}

impl SecretsManagerState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            secrets: BTreeMap::new(),
        }
    }

    pub fn reset(&mut self) {
        self.secrets.clear();
    }
}

pub type SharedSecretsManagerState =
    Arc<RwLock<fakecloud_core::multi_account::MultiAccountState<SecretsManagerState>>>;

impl fakecloud_core::multi_account::AccountState for SecretsManagerState {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }
}

/// On-disk snapshot envelope for Secrets Manager state. Versioned so
/// format changes fail loudly on upgrade.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SecretsManagerSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<fakecloud_core::multi_account::MultiAccountState<SecretsManagerState>>,
    #[serde(default)]
    pub state: Option<SecretsManagerState>,
}

pub const SECRETSMANAGER_SNAPSHOT_SCHEMA_VERSION: u32 = 2;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_initializes_empty() {
        let state = SecretsManagerState::new("123456789012", "us-east-1");
        assert_eq!(state.account_id, "123456789012");
        assert_eq!(state.region, "us-east-1");
        assert!(state.secrets.is_empty());
    }

    #[test]
    fn reset_clears_secrets() {
        let mut state = SecretsManagerState::new("123456789012", "us-east-1");
        state.secrets.insert(
            "s1".to_string(),
            Secret {
                name: "s1".to_string(),
                arn: "arn".to_string(),
                description: None,
                kms_key_id: None,
                versions: BTreeMap::new(),
                current_version_id: None,
                tags: vec![],
                tags_ever_set: false,
                deleted: false,
                deletion_date: None,
                created_at: Utc::now(),
                last_changed_at: Utc::now(),
                last_accessed_at: None,
                rotation_enabled: None,
                rotation_lambda_arn: None,
                rotation_rules: None,
                last_rotated_at: None,
                resource_policy: None,
                replica_regions: Vec::new(),
            },
        );
        state.reset();
        assert!(state.secrets.is_empty());
    }
}
