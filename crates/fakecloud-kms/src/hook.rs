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

use crate::state::{kms_alias_arn, kms_key_arn, parse_kms_arn, KmsKey, KmsState, SharedKmsState};

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
    /// The key resolves but is disabled / pending deletion, so it can't
    /// be used for a cryptographic operation (mirrors real KMS, which
    /// fails Decrypt with `DisabledException` / `KMSInvalidStateException`).
    KeyDisabled(String),
}

impl std::fmt::Display for KmsHookError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KeyNotFound(k) => write!(f, "kms key not found: {k}"),
            Self::InvalidCiphertext(msg) => write!(f, "invalid ciphertext: {msg}"),
            Self::KeyDisabled(k) => write!(f, "kms key is disabled: {k}"),
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
        let (key_arn, _) =
            self.resolve_or_provision(account_id, region, key_id, service_principal)?;
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
        let (key_short, plaintext) = self.open_envelope(account_id, ciphertext_b64)?;

        let key_arn = {
            let mas = self.state.read();
            let state = mas
                .get(account_id)
                .ok_or_else(|| KmsHookError::KeyNotFound(key_short.clone()))?;
            let key = state
                .keys
                .get(&key_short)
                .ok_or_else(|| KmsHookError::KeyNotFound(key_short.clone()))?;
            // A disabled / pending-deletion key can't decrypt — real KMS
            // rejects the operation, and SSE-KMS consumers (SQS/SNS/...) must
            // surface that rather than returning stale ciphertext as plaintext.
            if !key.enabled {
                return Err(KmsHookError::KeyDisabled(key_short.clone()));
            }
            key.arn.clone()
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

    /// The plaintext inside a ciphertext envelope, read without the KMS
    /// key-state check or a usage record. For introspection only: tests
    /// inspecting stored SSE data see what was encrypted even after the key
    /// was disabled, and the read does not show up in `/_fakecloud/kms/usage`.
    /// `None` when `ciphertext_b64` is not an envelope this account sealed.
    pub fn peek_plaintext(&self, account_id: &str, ciphertext_b64: &str) -> Option<Vec<u8>> {
        self.open_envelope(account_id, ciphertext_b64)
            .ok()
            .map(|(_, plaintext)| plaintext)
    }

    /// Unseal a ciphertext envelope into its key id and plaintext.
    fn open_envelope(
        &self,
        account_id: &str,
        ciphertext_b64: &str,
    ) -> Result<(String, Vec<u8>), KmsHookError> {
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
        if let Some(decoded) = crate::blob::decode(&master_key_bytes, &envelope_bytes) {
            return Ok((decoded.key_id, decoded.plaintext));
        }
        let envelope = String::from_utf8(envelope_bytes)
            .map_err(|e| KmsHookError::InvalidCiphertext(e.to_string()))?;
        let rest = envelope
            .strip_prefix("fakecloud-kms:")
            .ok_or_else(|| KmsHookError::InvalidCiphertext("unrecognized envelope".into()))?;
        let (key_short, plaintext_b64) = rest
            .split_once(':')
            .ok_or_else(|| KmsHookError::InvalidCiphertext("missing key separator".into()))?;
        let plaintext = base64::engine::general_purpose::STANDARD
            .decode(plaintext_b64)
            .map_err(|e| KmsHookError::InvalidCiphertext(e.to_string()))?;
        Ok((key_short.to_string(), plaintext))
    }

    /// Resolve `key_id` to its key ARN, provisioning an AWS-managed
    /// `aws/<service>` key on first use.
    pub fn resolve_key_arn(
        &self,
        account_id: &str,
        region: &str,
        key_id: &str,
        service_principal: &str,
    ) -> Result<String, KmsHookError> {
        self.resolve_or_provision(account_id, region, key_id, service_principal)
            .map(|(arn, _)| arn)
    }

    /// [`Self::resolve_key_arn`], also reporting whether an AWS-managed key
    /// was minted by this call (so a caller can persist KMS state).
    pub fn resolve_key_arn_tracked(
        &self,
        account_id: &str,
        region: &str,
        key_id: &str,
        service_principal: &str,
    ) -> Result<(String, bool), KmsHookError> {
        self.resolve_or_provision(account_id, region, key_id, service_principal)
    }

    /// The ARN of the AWS-managed key for `service` (`alias/aws/<service>`)
    /// in `account_id` and `region`, minted on first use; the flag is `true`
    /// when this call minted it. It is the key `alias/aws/<service>`
    /// resolves to in `region` (see [`Self::aws_managed_key`]).
    pub fn aws_managed_key_arn_tracked(
        &self,
        account_id: &str,
        region: &str,
        service: &str,
        service_principal: &str,
    ) -> (String, bool) {
        self.aws_managed_key(
            account_id,
            region,
            &format!("aws/{service}"),
            service_principal,
        )
    }

    /// Resolve `key_id` to its key ARN; the flag is `true` when an
    /// AWS-managed key had to be minted. Aliases resolve in `region`, or in
    /// the region an alias ARN names.
    fn resolve_or_provision(
        &self,
        account_id: &str,
        region: &str,
        key_id: &str,
        service_principal: &str,
    ) -> Result<(String, bool), KmsHookError> {
        // Pre-flight read to see if the key resolves cleanly.
        {
            let mas = self.state.read();
            if let Some(state) = mas.get(account_id) {
                if let Some(arn) = resolve_key(state, region, key_id) {
                    return Ok((arn, false));
                }
            }
        }

        // AWS-managed aliases (`aws/<service>`) auto-provision on
        // first use, in the region the reference names. Customer-supplied
        // aliases / IDs that don't resolve are an error.
        let alias = normalize_alias(key_id);
        if !alias.starts_with("aws/") {
            return Err(KmsHookError::KeyNotFound(key_id.to_string()));
        }
        let alias_region = parse_kms_arn(key_id).map_or(region, |(r, _, _)| r);
        Ok(self.aws_managed_key(account_id, alias_region, &alias, service_principal))
    }

    /// The AWS-managed key for `alias` (`aws/<service>`) in `account_id` and
    /// `region`, minted on first use. One key exists per account AND region,
    /// and that region's `alias/aws/<service>` alias targets it.
    fn aws_managed_key(
        &self,
        account_id: &str,
        region: &str,
        alias: &str,
        service_principal: &str,
    ) -> (String, bool) {
        let alias_full = format!("alias/{alias}");
        {
            let mas = self.state.read();
            if let Some(arn) = mas
                .get(account_id)
                .and_then(|state| existing_aws_managed_key(state, region, &alias_full))
            {
                return (arn, false);
            }
        }
        let mut mas = self.state.write();
        let state = mas.get_or_create(account_id);
        // Re-check under the write lock in case a concurrent caller won the
        // race; this also records a key/alias pair that is half-recorded.
        ensure_aws_managed_key(state, region, alias, service_principal)
    }
}

/// The cross-service trait form of the hook, for callers that hold KMS state
/// directly (the CloudFormation provisioner, service unit tests). The server
/// wraps the hook in its own adapter that also persists minted keys.
impl fakecloud_core::delivery::KmsHook for KmsServiceHook {
    fn encrypt(
        &self,
        account_id: &str,
        region: &str,
        key_id: &str,
        plaintext: &[u8],
        service_principal: &str,
        encryption_context: HashMap<String, String>,
    ) -> Result<String, String> {
        KmsServiceHook::encrypt(
            self,
            account_id,
            region,
            key_id,
            plaintext,
            service_principal,
            encryption_context,
        )
        .map_err(|e| e.to_string())
    }

    fn decrypt(
        &self,
        account_id: &str,
        ciphertext_b64: &str,
        service_principal: &str,
        encryption_context: HashMap<String, String>,
    ) -> Result<Vec<u8>, String> {
        KmsServiceHook::decrypt(
            self,
            account_id,
            ciphertext_b64,
            service_principal,
            encryption_context,
        )
        .map_err(|e| e.to_string())
    }

    fn resolve_key_arn(
        &self,
        account_id: &str,
        region: &str,
        key_id: &str,
        service_principal: &str,
    ) -> Result<String, String> {
        KmsServiceHook::resolve_key_arn(self, account_id, region, key_id, service_principal)
            .map_err(|e| e.to_string())
    }

    fn aws_managed_key_arn(
        &self,
        account_id: &str,
        region: &str,
        service: &str,
        service_principal: &str,
    ) -> Result<String, String> {
        Ok(self
            .aws_managed_key_arn_tracked(account_id, region, service, service_principal)
            .0)
    }
}

/// Strip the `arn:<partition>:kms:<region>:<account>:` ARN prefix and return
/// the resource portion (e.g. `key/<id>` or `alias/<name>`). Returns
/// `None` for ARNs that don't have the right shape.
fn strip_kms_arn_prefix(key_id: &str) -> Option<&str> {
    parse_kms_arn(key_id).map(|(_region, _account, resource)| resource)
}

/// Resolve `key_id` (raw id, alias name, alias ARN, or key ARN) to the
/// full key ARN if it currently exists in `state`. An alias name resolves in
/// `region`; an alias ARN in the region it names.
fn resolve_key(state: &KmsState, region: &str, key_id: &str) -> Option<String> {
    state.resolve_key_arn(region, key_id).map(str::to_string)
}

/// The ARN of the key the alias `name` targets in `region`.
fn alias_key_arn(state: &KmsState, region: &str, name: &str) -> Option<String> {
    let target = state.alias_target(region, name)?;
    state.keys.get(target).map(|k| k.arn.clone())
}

fn normalize_alias(key_id: &str) -> String {
    if let Some(resource) = strip_kms_arn_prefix(key_id) {
        if let Some(alias) = resource.strip_prefix("alias/") {
            return alias.to_string();
        }
    }
    key_id.strip_prefix("alias/").unwrap_or(key_id).to_string()
}

/// Canonical AWS-managed service aliases (`alias/aws/<service>`). Real
/// AWS pre-creates these in every account/region, so `aws kms list-aliases`
/// returns them on a brand-new account and `data.aws_kms_alias` resolves
/// them. The Terraform acceptance tests for several services
/// (e.g. `aws_dynamodb_table` encryption) read `alias/aws/<service>` via
/// that data source, so they must be listable even before any KMS use.
pub const DEFAULT_AWS_MANAGED_ALIASES: &[&str] = &[
    "aws/dynamodb",
    "aws/s3",
    "aws/sqs",
    "aws/sns",
    "aws/secretsmanager",
    "aws/ssm",
    "aws/rds",
    "aws/lambda",
    "aws/kinesis",
    "aws/logs",
    "aws/ebs",
    "aws/glue",
    "aws/elasticache",
    "aws/backup",
    "aws/es",
    "aws/redshift",
    "aws/xray",
    "aws/elasticfilesystem",
    "aws/cloudtrail",
    "aws/sagemaker",
];

/// Idempotently provision every [`DEFAULT_AWS_MANAGED_ALIASES`] entry that
/// isn't already present in `region`, so `ListAliases` mirrors real AWS.
/// Provisioning a missing alias also mints its backing AWS-managed key,
/// matching the auto-provision-on-first-use path in [`resolve_or_provision`].
pub fn ensure_default_managed_aliases(state: &mut KmsState, region: &str) {
    for alias in DEFAULT_AWS_MANAGED_ALIASES {
        // `aws/<service>` -> `<service>.amazonaws.com`. The principal only
        // shapes the default key policy; the data source asserts on the
        // target key id, not the principal, so an approximate principal is
        // fine here.
        let service = alias.strip_prefix("aws/").unwrap_or(alias);
        let principal = format!("{service}.amazonaws.com");
        ensure_aws_managed_key(state, region, alias, &principal);
    }
}

/// The ARN of the AWS-managed key behind `alias_full` (`alias/aws/<service>`)
/// in `region`, if one exists: the key recorded for the region, else the key
/// the region's alias targets.
fn existing_aws_managed_key(state: &KmsState, region: &str, alias_full: &str) -> Option<String> {
    let slot = crate::state::aws_managed_key_slot(region, alias_full);
    state
        .aws_managed_keys
        .get(&slot)
        .and_then(|id| state.keys.get(id))
        .map(|k| k.arn.clone())
        .or_else(|| alias_key_arn(state, region, alias_full))
}

/// The AWS-managed key for `alias` (`aws/<service>`) in `region`, minting it
/// when the region has none; the flag is `true` when this call minted it.
/// Afterwards the region's `alias/aws/<service>` alias targets the key and
/// [`KmsState::aws_managed_keys`] records it for the region.
fn ensure_aws_managed_key(
    state: &mut KmsState,
    region: &str,
    alias: &str,
    service_principal: &str,
) -> (String, bool) {
    let alias_full = format!("alias/{alias}");
    let (arn, minted) = match existing_aws_managed_key(state, region, &alias_full) {
        Some(arn) => (arn, false),
        None => (
            mint_aws_managed_key(state, region, alias, service_principal),
            true,
        ),
    };
    let key_id = key_id_from_arn(&arn).to_string();
    state.aws_managed_keys.insert(
        crate::state::aws_managed_key_slot(region, &alias_full),
        key_id.clone(),
    );
    if state.alias_target(region, &alias_full) != Some(key_id.as_str()) {
        let creation_date = Utc::now().timestamp() as f64;
        state.insert_alias(
            region,
            crate::state::KmsAlias {
                alias_arn: kms_alias_arn(region, &state.account_id, &alias_full),
                alias_name: alias_full,
                target_key_id: key_id,
                creation_date,
            },
        );
    }
    (arn, minted)
}

/// Mint an AWS-managed key protecting `alias` (`aws/<service>`) in `region`.
fn mint_aws_managed_key(
    state: &mut KmsState,
    region: &str,
    alias: &str,
    service_principal: &str,
) -> String {
    let key_id = uuid::Uuid::new_v4().to_string();
    let arn = kms_key_arn(region, &state.account_id, &key_id);
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
        rotation_period_in_days: None,
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
    state.keys.insert(key_id, key);
    arn
}

fn key_id_from_arn(arn: &str) -> &str {
    arn.rsplit_once('/').map(|(_, k)| k).unwrap_or(arn)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Peeking returns what was sealed even after the key is disabled, and
    /// records no usage; a non-envelope body peeks to `None`.
    #[test]
    fn peek_plaintext_bypasses_key_state_and_usage() {
        let acct = "123456789012";
        let state: SharedKmsState = std::sync::Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new(acct, "us-east-1", ""),
        ));
        let usage = SharedKmsUsageState::default();
        let hook = KmsServiceHook::new(state.clone(), usage.clone());
        let ct = hook
            .encrypt(
                acct,
                "us-east-1",
                "alias/aws/sqs",
                b"hello",
                "sqs.amazonaws.com",
                HashMap::new(),
            )
            .unwrap();
        let recorded = usage.read().records().len();
        for key in state.write().get_or_create(acct).keys.values_mut() {
            key.enabled = false;
        }
        assert!(hook
            .decrypt(acct, &ct, "sqs.amazonaws.com", HashMap::new())
            .is_err());
        assert_eq!(
            hook.peek_plaintext(acct, &ct).as_deref(),
            Some(&b"hello"[..])
        );
        assert_eq!(usage.read().records().len(), recorded);
        assert_eq!(hook.peek_plaintext(acct, "plain text body"), None);
        assert_eq!(hook.peek_plaintext("999999999999", &ct), None);
    }

    #[test]
    fn aws_managed_keys_take_the_partition_of_their_region() {
        let state: SharedKmsState = std::sync::Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "cn-north-1", ""),
        ));
        let hook = KmsServiceHook::new(state, Default::default());
        let arn = hook
            .resolve_key_arn(
                "123456789012",
                "cn-north-1",
                "alias/aws/dynamodb",
                "dynamodb.amazonaws.com",
            )
            .unwrap();
        assert!(
            arn.starts_with("arn:aws-cn:kms:cn-north-1:123456789012:key/"),
            "{arn}"
        );
        // The same key resolves again by its own ARN.
        assert_eq!(
            hook.resolve_key_arn("123456789012", "cn-north-1", &arn, "dynamodb.amazonaws.com")
                .unwrap(),
            arn
        );
    }

    /// AWS-managed keys are resolved per account AND region: us-east-1,
    /// us-west-2 and cn-north-1 get three distinct keys, each in its region's
    /// partition, each reused within its region, and each region's
    /// `alias/aws/<service>` alias resolves to that region's key.
    #[test]
    fn aws_managed_keys_are_minted_per_region() {
        use crate::test_support::{assert_aws_managed_key, kms_hook};
        use fakecloud_core::delivery::aws_managed_kms_key_arn;
        let acct = "123456789012";
        let (state, hook) = kms_hook(acct);
        let h = Some(hook.as_ref());

        let east = aws_managed_kms_key_arn(h, acct, "us-east-1", "timestream").unwrap();
        let west = aws_managed_kms_key_arn(h, acct, "us-west-2", "timestream").unwrap();
        let cn = aws_managed_kms_key_arn(h, acct, "cn-north-1", "timestream").unwrap();
        assert_ne!(east, west);
        assert_ne!(east, cn);
        assert_ne!(west, cn);
        assert!(
            cn.starts_with("arn:aws-cn:kms:cn-north-1:123456789012:key/"),
            "{cn}"
        );
        for (region, arn) in [
            ("us-east-1", &east),
            ("us-west-2", &west),
            ("cn-north-1", &cn),
        ] {
            assert_aws_managed_key(&state, acct, region, arn, "alias/aws/timestream");
            assert_eq!(
                aws_managed_kms_key_arn(h, acct, region, "timestream").as_deref(),
                Some(arn.as_str()),
                "{region} reuses its key"
            );
            assert_eq!(
                &hook
                    .resolve_key_arn(
                        acct,
                        region,
                        "alias/aws/timestream",
                        "timestream.amazonaws.com"
                    )
                    .unwrap(),
                arn,
                "{region}'s alias names {region}'s key"
            );
        }
        // An alias ARN resolves in the region it names, whatever the
        // caller's region.
        assert_eq!(
            hook.resolve_key_arn(
                acct,
                "us-east-1",
                "arn:aws:kms:us-west-2:123456789012:alias/aws/timestream",
                "timestream.amazonaws.com"
            )
            .unwrap(),
            west
        );
        let accounts = state.read();
        let s = accounts.get(acct).unwrap();
        for (region, arn) in [("us-east-1", &east), ("us-west-2", &west)] {
            let alias = s.alias(region, "alias/aws/timestream").unwrap();
            assert_eq!(&s.keys[&alias.target_key_id].arn, arn);
            assert_eq!(
                alias.alias_arn,
                format!("arn:aws:kms:{region}:123456789012:alias/aws/timestream")
            );
        }
        drop(accounts);
        assert_eq!(
            aws_managed_kms_key_arn(None, acct, "us-east-1", "timestream"),
            None
        );
    }

    /// An `aws/<service>` alias ARN naming a region with no key yet mints the
    /// key in that region, not the caller's.
    #[test]
    fn managed_alias_arn_provisions_in_its_own_region() {
        let acct = "123456789012";
        let (state, hook) = crate::test_support::kms_hook(acct);
        let arn = hook
            .resolve_key_arn(
                acct,
                "us-east-1",
                "arn:aws:kms:eu-west-1:123456789012:alias/aws/s3",
                "s3.amazonaws.com",
            )
            .unwrap();
        assert!(arn.starts_with("arn:aws:kms:eu-west-1:"), "{arn}");
        let accounts = state.read();
        let s = accounts.get(acct).unwrap();
        assert!(s.alias("us-east-1", "alias/aws/s3").is_none());
        assert!(s.alias("eu-west-1", "alias/aws/s3").is_some());
    }

    /// A customer alias resolves only in its own region (or through an alias
    /// ARN naming that region).
    #[test]
    fn customer_alias_resolves_only_in_its_region() {
        let acct = "123456789012";
        let (state, hook) = crate::test_support::kms_hook(acct);
        let key_arn = {
            let mut accounts = state.write();
            let s = accounts.get_or_create(acct);
            let arn = mint_aws_managed_key(s, "eu-west-1", "custom", "x.amazonaws.com");
            let alias = crate::state::KmsAlias {
                alias_name: "alias/app".into(),
                alias_arn: kms_alias_arn("eu-west-1", acct, "alias/app"),
                target_key_id: key_id_from_arn(&arn).to_string(),
                creation_date: 0.0,
            };
            s.insert_alias("eu-west-1", alias);
            arn
        };
        let resolve = |region: &str, key: &str| hook.resolve_key_arn(acct, region, key, "p").ok();
        assert_eq!(resolve("eu-west-1", "alias/app"), Some(key_arn.clone()));
        assert_eq!(resolve("us-east-1", "alias/app"), None);
        assert_eq!(
            resolve("us-east-1", "arn:aws:kms:eu-west-1:123456789012:alias/app"),
            Some(key_arn)
        );
        assert_eq!(
            resolve("eu-west-1", "arn:aws:kms:us-east-1:123456789012:alias/app"),
            None
        );
    }

    /// A caller-named key is reported as the key ARN it names: an alias name
    /// or ARN and a bare key id resolve to the target key's ARN, an
    /// AWS-managed alias to that region's AWS-managed key, and a key KMS does
    /// not know (or no hook) is reported as given.
    #[test]
    fn named_keys_resolve_to_their_key_arn() {
        use fakecloud_core::delivery::{kms_key_arn_or_aws_managed, resolve_named_kms_key_arn};
        let acct = "123456789012";
        let (state, hook) = crate::test_support::kms_hook(acct);
        let h = Some(hook.as_ref());
        let target =
            fakecloud_core::delivery::aws_managed_kms_key_arn(h, acct, "us-east-1", "backup")
                .unwrap();
        let target_id = key_id_from_arn(&target).to_string();
        {
            let mut accounts = state.write();
            accounts.get_or_create(acct).insert_alias(
                "us-east-1",
                crate::state::KmsAlias {
                    alias_name: "alias/mine".to_string(),
                    alias_arn: format!("arn:aws:kms:us-east-1:{acct}:alias/mine"),
                    target_key_id: target_id.clone(),
                    creation_date: 0.0,
                },
            );
        }
        let resolve =
            |key: &str, region: &str| resolve_named_kms_key_arn(h, key, acct, region, "rds");
        assert_eq!(resolve("alias/mine", "us-east-1"), target);
        assert_eq!(
            resolve(
                &format!("arn:aws:kms:us-east-1:{acct}:alias/mine"),
                "us-east-1"
            ),
            target
        );
        assert_eq!(resolve(&target_id, "us-east-1"), target);
        assert_eq!(resolve(&target, "us-east-1"), target);

        let rds_west = resolve("alias/aws/rds", "us-west-2");
        crate::test_support::assert_aws_managed_key(
            &state,
            acct,
            "us-west-2",
            &rds_west,
            "alias/aws/rds",
        );
        // An alias ARN naming the resource's own region resolves there; one
        // naming another region is not usable from this region: it is
        // reported as given and mints nothing in that region.
        assert_eq!(
            resolve(
                &format!("arn:aws:kms:us-west-2:{acct}:alias/aws/rds"),
                "us-west-2"
            ),
            rds_west
        );
        let foreign = format!("arn:aws:kms:eu-central-1:{acct}:alias/aws/rds");
        assert_eq!(resolve(&foreign, "us-west-2"), foreign);
        assert!(state
            .read()
            .get(acct)
            .unwrap()
            .alias("eu-central-1", "alias/aws/rds")
            .is_none());

        assert_eq!(resolve("alias/unknown", "us-east-1"), "alias/unknown");
        assert_eq!(
            resolve_named_kms_key_arn(None, "alias/mine", acct, "us-east-1", "rds"),
            "alias/mine"
        );
        // No key named: the AWS-managed key; an empty name counts as none.
        assert_eq!(
            kms_key_arn_or_aws_managed(h, Some(""), acct, "us-west-2", "rds").as_deref(),
            Some(rds_west.as_str())
        );
        assert_eq!(
            kms_key_arn_or_aws_managed(h, Some("alias/mine"), acct, "us-east-1", "rds").as_deref(),
            Some(target.as_str())
        );
        // A customer alias names a key only in its own region: elsewhere it
        // is unresolvable and reported as given.
        assert_eq!(resolve("alias/mine", "us-west-2"), "alias/mine");
    }

    /// A managed alias pre-listed by ListAliases (in the listing region) is
    /// the key that region's services report; other regions mint their own.
    #[test]
    fn pre_listed_managed_alias_serves_its_own_region_only() {
        use fakecloud_core::delivery::aws_managed_kms_key_arn;
        let acct = "123456789012";
        let (state, hook) = crate::test_support::kms_hook(acct);
        {
            let mut accounts = state.write();
            ensure_default_managed_aliases(accounts.get_or_create(acct), "eu-west-1");
        }
        let listed = {
            let accounts = state.read();
            let s = accounts.get(acct).unwrap();
            s.keys[s
                .alias_target("eu-west-1", "alias/aws/elasticfilesystem")
                .unwrap()]
            .arn
            .clone()
        };
        let h = Some(hook.as_ref());
        assert_eq!(
            aws_managed_kms_key_arn(h, acct, "eu-west-1", "elasticfilesystem").as_deref(),
            Some(listed.as_str())
        );
        let east = aws_managed_kms_key_arn(h, acct, "us-east-1", "elasticfilesystem").unwrap();
        assert_ne!(east, listed);
        assert!(east.starts_with("arn:aws:kms:us-east-1:"));
    }

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
        assert_eq!(
            strip_kms_arn_prefix("arn:aws-cn:kms:cn-north-1:000000000000:key/abc-123"),
            Some("key/abc-123")
        );
    }

    #[test]
    fn china_region_managed_key_is_minted_and_resolved_in_aws_cn() {
        let mut state = KmsState::new("000000000000", "cn-north-1");
        let (arn, minted) = ensure_aws_managed_key(
            &mut state,
            "cn-north-1",
            "aws/secretsmanager",
            "secretsmanager.amazonaws.com",
        );
        assert!(minted);
        assert!(
            arn.starts_with("arn:aws-cn:kms:cn-north-1:000000000000:key/"),
            "{arn}"
        );
        assert_eq!(
            state
                .alias("cn-north-1", "alias/aws/secretsmanager")
                .unwrap()
                .alias_arn,
            "arn:aws-cn:kms:cn-north-1:000000000000:alias/aws/secretsmanager"
        );
        assert_eq!(resolve_key(&state, "cn-north-1", &arn), Some(arn.clone()));
        assert_eq!(
            resolve_key(
                &state,
                "us-east-1",
                "arn:aws-cn:kms:cn-north-1:000000000000:alias/aws/secretsmanager"
            ),
            Some(arn)
        );
        assert_eq!(
            resolve_key(&state, "us-east-1", "alias/aws/secretsmanager"),
            None
        );
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
