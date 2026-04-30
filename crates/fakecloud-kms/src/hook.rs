//! Cross-service KMS hook.
//!
//! Services that accept a `KmsKeyId` (Secrets Manager, SSM
//! `SecureString`, S3 SSE-KMS, SQS, SNS, DynamoDB) call into this
//! module so that:
//!
//! 1. The supplied key is resolved (alias `aws/<service>` and bare
//!    aliases included), auto-provisioning AWS-managed keys on first
//!    use to match real AWS.
//! 2. Each call is recorded in [`KmsUsageState`] so test code can
//!    assert through `/_fakecloud/kms/usage` that the right service
//!    triggered the right operation on the right key.
//! 3. The returned ciphertext is a real envelope decryptable by the
//!    public KMS `Decrypt` API (uses the same `fakecloud-kms:`
//!    envelope as the existing service-side encrypt path).
//!
//! Encryption context, key policy enforcement, and KMS-managed key
//! rotation come in follow-up PRs.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;

use base64::Engine;

use crate::state::{KmsKey, KmsState, SharedKmsState};

/// One recorded KMS hook call. Returned by the introspection endpoint
/// so test code can assert `kms:GenerateDataKey` / `kms:Decrypt` ran
/// on the expected key + service principal.
#[derive(Clone, serde::Serialize)]
pub struct KmsUsageRecord {
    pub timestamp: DateTime<Utc>,
    pub operation: String,
    pub service_principal: String,
    pub account_id: String,
    pub key_arn: String,
    pub encryption_context: HashMap<String, String>,
}

#[derive(Default)]
pub struct KmsUsageState {
    records: Vec<KmsUsageRecord>,
}

impl KmsUsageState {
    pub fn records(&self) -> &[KmsUsageRecord] {
        &self.records
    }

    pub fn push(&mut self, record: KmsUsageRecord) {
        self.records.push(record);
    }

    pub fn clear(&mut self) {
        self.records.clear();
    }
}

pub type SharedKmsUsageState = Arc<RwLock<KmsUsageState>>;

/// Hook used by service crates that need to call KMS for encryption /
/// decryption without going through the AWS-shaped HTTP layer.
pub struct KmsServiceHook {
    state: SharedKmsState,
    usage: SharedKmsUsageState,
}

#[derive(Debug)]
pub enum KmsHookError {
    /// Caller supplied a key id / alias / ARN that doesn't resolve to
    /// an existing key (and isn't an AWS-managed alias we auto-create).
    KeyNotFound(String),
    /// Ciphertext envelope is malformed or signed by a key that no
    /// longer exists.
    InvalidCiphertext(String),
}

impl std::fmt::Display for KmsHookError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KeyNotFound(k) => write!(f, "kms key not found: {k}"),
            Self::InvalidCiphertext(msg) => write!(f, "invalid ciphertext: {msg}"),
        }
    }
}

impl std::error::Error for KmsHookError {}

impl KmsServiceHook {
    pub fn new(state: SharedKmsState, usage: SharedKmsUsageState) -> Self {
        Self { state, usage }
    }

    /// Encrypt `plaintext` under `key_id` (raw id, ARN, alias, or
    /// `aws/<service>` AWS-managed alias). Records the call as a
    /// `GenerateDataKey`-shaped usage record and returns the base64
    /// ciphertext envelope.
    pub fn encrypt(
        &self,
        account_id: &str,
        region: &str,
        key_id: &str,
        plaintext: &[u8],
        service_principal: &str,
        encryption_context: HashMap<String, String>,
    ) -> Result<String, KmsHookError> {
        let key_arn = self.resolve_or_provision(account_id, region, key_id, service_principal)?;
        let key_short = key_id_from_arn(&key_arn).to_string();

        // Default to the AWS-shaped binary blob (AES-256-GCM under the
        // per-account master key persisted in `KmsState`). The legacy
        // `fakecloud-kms:<key>:<b64>` textual envelope is still accepted on
        // the decrypt side for back-compat with older snapshots and
        // external callers.
        let master_key_bytes = {
            let mas = self.state.read();
            mas.get(account_id)
                .map(|s| s.master_key_bytes.clone())
                .ok_or_else(|| KmsHookError::KeyNotFound(key_short.clone()))?
        };
        let blob = crate::blob::encode(&master_key_bytes, &key_short, plaintext);
        let ciphertext_b64 = base64::engine::general_purpose::STANDARD.encode(&blob);

        self.usage.write().push(KmsUsageRecord {
            timestamp: Utc::now(),
            operation: "GenerateDataKey".to_string(),
            service_principal: service_principal.to_string(),
            account_id: account_id.to_string(),
            key_arn,
            encryption_context,
        });

        Ok(ciphertext_b64)
    }

    /// Decrypt a previously-`encrypt`-produced base64 ciphertext.
    /// Records the call as a `Decrypt`-shaped usage record.
    pub fn decrypt(
        &self,
        account_id: &str,
        ciphertext_b64: &str,
        service_principal: &str,
        encryption_context: HashMap<String, String>,
    ) -> Result<Vec<u8>, KmsHookError> {
        let envelope_bytes = base64::engine::general_purpose::STANDARD
            .decode(ciphertext_b64)
            .map_err(|e| KmsHookError::InvalidCiphertext(e.to_string()))?;

        // Try AWS-shaped binary blob first using the account's master key;
        // older textual envelopes fall through to the legacy parser below.
        let master_key_bytes = {
            let mas = self.state.read();
            mas.get(account_id)
                .map(|s| s.master_key_bytes.clone())
                .unwrap_or_default()
        };
        let (key_short, plaintext) =
            if let Some(decoded) = crate::blob::decode(&master_key_bytes, &envelope_bytes) {
                (decoded.key_id, decoded.plaintext)
            } else {
                let envelope = String::from_utf8(envelope_bytes)
                    .map_err(|e| KmsHookError::InvalidCiphertext(e.to_string()))?;
                let rest = envelope.strip_prefix("fakecloud-kms:").ok_or_else(|| {
                    KmsHookError::InvalidCiphertext("unrecognized envelope".into())
                })?;
                let (key_short, plaintext_b64) = rest.split_once(':').ok_or_else(|| {
                    KmsHookError::InvalidCiphertext("missing key separator".into())
                })?;

                let plaintext = base64::engine::general_purpose::STANDARD
                    .decode(plaintext_b64)
                    .map_err(|e| KmsHookError::InvalidCiphertext(e.to_string()))?;
                (key_short.to_string(), plaintext)
            };

        let key_arn = {
            let mas = self.state.read();
            let state = mas
                .get(account_id)
                .ok_or_else(|| KmsHookError::KeyNotFound(key_short.clone()))?;
            state
                .keys
                .get(&key_short)
                .map(|k| k.arn.clone())
                .ok_or_else(|| KmsHookError::KeyNotFound(key_short.clone()))?
        };

        self.usage.write().push(KmsUsageRecord {
            timestamp: Utc::now(),
            operation: "Decrypt".to_string(),
            service_principal: service_principal.to_string(),
            account_id: account_id.to_string(),
            key_arn,
            encryption_context,
        });

        Ok(plaintext)
    }

    fn resolve_or_provision(
        &self,
        account_id: &str,
        region: &str,
        key_id: &str,
        service_principal: &str,
    ) -> Result<String, KmsHookError> {
        // Pre-flight read to see if the key resolves cleanly.
        {
            let mas = self.state.read();
            if let Some(state) = mas.get(account_id) {
                if let Some(arn) = resolve_key(state, key_id) {
                    return Ok(arn);
                }
            }
        }

        // AWS-managed aliases (`aws/<service>`) auto-provision on
        // first use. Customer-supplied aliases / IDs that don't
        // resolve are an error.
        let alias = normalize_alias(key_id);
        if !alias.starts_with("aws/") {
            return Err(KmsHookError::KeyNotFound(key_id.to_string()));
        }
        let mut mas = self.state.write();
        let state = mas.get_or_create(account_id);
        // Re-check under the write lock in case a concurrent caller won the race.
        if let Some(arn) = resolve_key(state, key_id) {
            return Ok(arn);
        }
        let key_arn = provision_aws_managed_key(state, region, &alias, service_principal);
        Ok(key_arn)
    }
}

/// Strip the `arn:aws:kms:<region>:<account>:` ARN prefix and return
/// the resource portion (e.g. `key/<id>` or `alias/<name>`). Returns
/// `None` for ARNs that don't have the right shape.
fn strip_kms_arn_prefix(key_id: &str) -> Option<&str> {
    let rest = key_id.strip_prefix("arn:aws:kms:")?;
    // Format after prefix: <region>:<account>:<resource>. Need to skip
    // both `region` and `account` separately so the resource starts
    // cleanly at `key/...` or `alias/...`.
    let (_region, after_region) = rest.split_once(':')?;
    let (_account, resource) = after_region.split_once(':')?;
    Some(resource)
}

/// Resolve `key_id` (raw id, alias name, alias ARN, or key ARN) to the
/// full key ARN if it currently exists in `state`.
fn resolve_key(state: &KmsState, key_id: &str) -> Option<String> {
    if let Some(resource) = strip_kms_arn_prefix(key_id) {
        if let Some(short) = resource.strip_prefix("key/") {
            return state.keys.get(short).map(|k| k.arn.clone());
        }
        if let Some(alias) = resource.strip_prefix("alias/") {
            let full = format!("alias/{alias}");
            if let Some(a) = state.aliases.get(&full) {
                return state.keys.get(&a.target_key_id).map(|k| k.arn.clone());
            }
        }
    }
    if let Some(alias) = key_id.strip_prefix("alias/") {
        let full = format!("alias/{alias}");
        if let Some(a) = state.aliases.get(&full) {
            return state.keys.get(&a.target_key_id).map(|k| k.arn.clone());
        }
    }
    state.keys.get(key_id).map(|k| k.arn.clone())
}

fn normalize_alias(key_id: &str) -> String {
    if let Some(resource) = strip_kms_arn_prefix(key_id) {
        if let Some(alias) = resource.strip_prefix("alias/") {
            return alias.to_string();
        }
    }
    key_id.strip_prefix("alias/").unwrap_or(key_id).to_string()
}

fn provision_aws_managed_key(
    state: &mut KmsState,
    region: &str,
    alias: &str,
    service_principal: &str,
) -> String {
    let key_id = uuid::Uuid::new_v4().to_string();
    let arn = format!(
        "arn:aws:kms:{region}:{account}:key/{key_id}",
        account = state.account_id,
        region = region,
    );
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "Allow access through service",
            "Effect": "Allow",
            "Principal": {"Service": service_principal},
            "Action": ["kms:GenerateDataKey", "kms:Decrypt", "kms:DescribeKey"],
            "Resource": "*"
        }]
    })
    .to_string();
    let key = KmsKey {
        key_id: key_id.clone(),
        arn: arn.clone(),
        creation_date: Utc::now().timestamp() as f64,
        description: format!(
            "Default master key that protects {alias} when no other key is defined"
        ),
        enabled: true,
        key_usage: "ENCRYPT_DECRYPT".to_string(),
        key_spec: "SYMMETRIC_DEFAULT".to_string(),
        key_manager: "AWS".to_string(),
        key_state: "Enabled".to_string(),
        deletion_date: None,
        tags: BTreeMap::new(),
        policy,
        key_rotation_enabled: true,
        origin: "AWS_KMS".to_string(),
        multi_region: false,
        rotations: Vec::new(),
        signing_algorithms: None,
        encryption_algorithms: Some(vec!["SYMMETRIC_DEFAULT".to_string()]),
        mac_algorithms: None,
        custom_key_store_id: None,
        imported_key_material: false,
        imported_material_bytes: None,
        private_key_seed: Vec::new(),
        primary_region: None,
        asymmetric_private_key_der: None,
        asymmetric_public_key_der: None,
    };
    state.keys.insert(key_id.clone(), key);
    let alias_full = format!("alias/{alias}");
    state.aliases.insert(
        alias_full.clone(),
        crate::state::KmsAlias {
            alias_name: alias_full,
            alias_arn: format!(
                "arn:aws:kms:{region}:{account}:alias/{alias}",
                account = state.account_id,
                region = region,
            ),
            target_key_id: key_id,
            creation_date: Utc::now().timestamp() as f64,
        },
    );
    arn
}

fn key_id_from_arn(arn: &str) -> &str {
    arn.rsplit_once('/').map(|(_, k)| k).unwrap_or(arn)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_arn_prefix_skips_region_and_account() {
        assert_eq!(
            strip_kms_arn_prefix("arn:aws:kms:us-east-1:000000000000:key/abc-123"),
            Some("key/abc-123")
        );
        assert_eq!(
            strip_kms_arn_prefix("arn:aws:kms:us-east-1:000000000000:alias/aws/secretsmanager"),
            Some("alias/aws/secretsmanager")
        );
        assert_eq!(strip_kms_arn_prefix("not-an-arn"), None);
        // Missing one of region/account should return None, not a half-stripped resource.
        assert_eq!(strip_kms_arn_prefix("arn:aws:kms:key/abc"), None);
    }

    #[test]
    fn normalize_alias_handles_arns_correctly() {
        assert_eq!(
            normalize_alias("arn:aws:kms:us-east-1:000000000000:alias/aws/secretsmanager"),
            "aws/secretsmanager"
        );
        assert_eq!(normalize_alias("alias/aws/sqs"), "aws/sqs");
        assert_eq!(normalize_alias("aws/s3"), "aws/s3");
    }
}
