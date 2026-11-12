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

use fakecloud_core::delivery::{KmsCaller, KmsError, KmsErrorKind};

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

/// Why a hook call failed; see [`fakecloud_core::delivery::KmsError`].
pub type KmsHookError = KmsError;

pub use fakecloud_core::delivery::kms_not_found_message as not_found_message;

/// KMS's refusal of a key that is not `Enabled` (recorded through Lambda):
/// `DisabledException` "<key ARN> is disabled.", `KMSInvalidStateException`
/// "<key ARN> is pending deletion." (or "is <state>.").
fn require_enabled(key: &KmsKey) -> Result<(), KmsError> {
    match key.key_state.as_str() {
        "Enabled" => Ok(()),
        "Disabled" => Err(KmsError::new(
            KmsErrorKind::Disabled,
            format!("{} is disabled.", key.arn),
        )),
        "PendingDeletion" => Err(KmsError::new(
            KmsErrorKind::InvalidState,
            format!("{} is pending deletion.", key.arn),
        )),
        other => Err(KmsError::new(
            KmsErrorKind::InvalidState,
            format!("{} is {other}.", key.arn),
        )),
    }
}

/// A service can only envelope-encrypt under a symmetric encryption key.
/// (The message follows KMS's form for a key whose usage does not allow the
/// operation; not recorded through a service.)
fn require_symmetric_encryption_key(key: &KmsKey, operation: &str) -> Result<(), KmsError> {
    if key.key_usage != "ENCRYPT_DECRYPT" || key.key_spec != "SYMMETRIC_DEFAULT" {
        return Err(KmsError::new(
            KmsErrorKind::InvalidKeyUsage,
            format!(
                "{} key usage is {} which is not valid for {operation}.",
                key.arn, key.key_usage
            ),
        ));
    }
    Ok(())
}

/// The AEAD bytes an encryption context binds into a ciphertext: the same
/// canonical form the KMS API binds, so a ciphertext a service sealed opens
/// with `Decrypt` given that service's context, and with nothing else.
fn context_aad(encryption_context: &HashMap<String, String>) -> Vec<u8> {
    if encryption_context.is_empty() {
        return Vec::new();
    }
    let sorted: BTreeMap<&str, &str> = encryption_context
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    serde_json::to_vec(&sorted).unwrap_or_default()
}

fn invalid_ciphertext() -> KmsError {
    KmsError::new(KmsErrorKind::InvalidCiphertext, "")
}

impl KmsServiceHook {
    pub fn new(state: SharedKmsState, usage: SharedKmsUsageState) -> Self {
        Self { state, usage }
    }

    /// Encrypt `plaintext` under `key_id` (raw id, ARN, alias, or
    /// `aws/<service>` AWS-managed alias), authorized as the service's data
    /// key derivation (`kms:GenerateDataKey`). Records the call and returns
    /// the base64 ciphertext envelope, bound to `encryption_context`.
    pub fn encrypt(
        &self,
        account_id: &str,
        region: &str,
        key_id: &str,
        plaintext: &[u8],
        caller: KmsCaller<'_>,
        encryption_context: HashMap<String, String>,
    ) -> Result<String, KmsError> {
        self.encrypt_with_operation(
            "GenerateDataKey",
            account_id,
            region,
            key_id,
            plaintext,
            caller,
            encryption_context,
        )
    }

    /// [`Self::encrypt`] authorized and recorded as `operation`
    /// (`GenerateDataKey`, or `Encrypt` for SSM, which encrypts parameter
    /// values with `kms:Encrypt`).
    #[allow(clippy::too_many_arguments)]
    pub fn encrypt_with_operation(
        &self,
        operation: &str,
        account_id: &str,
        region: &str,
        key_id: &str,
        plaintext: &[u8],
        caller: KmsCaller<'_>,
        encryption_context: HashMap<String, String>,
    ) -> Result<String, KmsError> {
        let owner = key_owner_account(account_id, key_id);
        let (key_arn, _) = self
            .resolve_or_provision(account_id, region, key_id)
            .map_err(|err| missing_key_refusal(err, caller, &format!("kms:{operation}")))?;
        let key_short = key_id_from_arn(&key_arn).to_string();
        let ciphertext_b64 = {
            let mas = self.state.read();
            let state = mas.get(owner).ok_or_else(|| {
                KmsError::new(
                    KmsErrorKind::NotFound,
                    not_found_message(account_id, region, key_id),
                )
            })?;
            let key = state.keys.get(&key_short).ok_or_else(|| {
                KmsError::new(
                    KmsErrorKind::NotFound,
                    not_found_message(account_id, region, key_id),
                )
            })?;
            authorize(
                key,
                &state.grants,
                account_id,
                caller,
                &format!("kms:{operation}"),
                &encryption_context,
            )?;
            require_enabled(key)?;
            require_symmetric_encryption_key(key, operation)?;
            // Sealed with the material of the key's own account. A key of
            // another account is named by its ARN in the blob, so the
            // decrypting service (which knows only its own account) finds
            // it.
            let embedded = if owner == account_id {
                key_short.as_str()
            } else {
                key_arn.as_str()
            };
            let blob = crate::blob::encode_with_context(
                &state.master_key_bytes,
                embedded,
                plaintext,
                &context_aad(&encryption_context),
            );
            base64::engine::general_purpose::STANDARD.encode(&blob)
        };

        self.usage.write().push(KmsUsageRecord {
            timestamp: Utc::now(),
            operation: operation.to_string(),
            service_principal: caller.service_principal().to_string(),
            account_id: account_id.to_string(),
            key_arn,
            encryption_context,
        });

        Ok(ciphertext_b64)
    }

    /// Decrypt a ciphertext a service sealed with [`Self::encrypt`] (or the
    /// KMS API), which only opens with the encryption context it was sealed
    /// with. Records the call as a `Decrypt`.
    pub fn decrypt(
        &self,
        account_id: &str,
        ciphertext_b64: &str,
        caller: KmsCaller<'_>,
        encryption_context: HashMap<String, String>,
    ) -> Result<Vec<u8>, KmsError> {
        let (key_ref, plaintext) =
            self.open_envelope(account_id, ciphertext_b64, &encryption_context)?;

        let key_arn = {
            let mas = self.state.read();
            // The key a ciphertext names but KMS no longer has: KMS cannot
            // recover the plaintext. A key of another account is named by
            // its ARN and looked up there.
            let owner_state = mas
                .get(key_owner_account(account_id, &key_ref))
                .ok_or_else(invalid_ciphertext)?;
            let key = owner_state
                .keys
                .get(key_id_from_arn(&key_ref))
                .ok_or_else(invalid_ciphertext)?;
            authorize(
                key,
                &owner_state.grants,
                account_id,
                caller,
                "kms:Decrypt",
                &encryption_context,
            )?;
            // A disabled / pending-deletion key can't decrypt: real KMS
            // rejects the operation, and SSE-KMS consumers (SQS/SNS/...) must
            // surface that rather than returning stale ciphertext as plaintext.
            require_enabled(key)?;
            key.arn.clone()
        };

        self.usage.write().push(KmsUsageRecord {
            timestamp: Utc::now(),
            operation: "Decrypt".to_string(),
            service_principal: caller.service_principal().to_string(),
            account_id: account_id.to_string(),
            key_arn,
            encryption_context,
        });

        Ok(plaintext)
    }

    /// Whether `caller` may use `key_id` for every one of `actions` (see
    /// [`fakecloud_core::delivery::KmsHook::authorize_key_use`]); the key
    /// must also be enabled. Returns the key ARN.
    pub fn authorize_key_use(
        &self,
        account_id: &str,
        region: &str,
        key_id: &str,
        caller: KmsCaller<'_>,
        actions: &[&str],
        encryption_context: &HashMap<String, String>,
    ) -> Result<String, KmsError> {
        let owner = key_owner_account(account_id, key_id);
        let (key_arn, _) = self
            .resolve_or_provision(account_id, region, key_id)
            .map_err(|err| {
                missing_key_refusal(
                    err,
                    caller,
                    actions.first().copied().unwrap_or("kms:Decrypt"),
                )
            })?;
        let mas = self.state.read();
        let not_found = || {
            KmsError::new(
                KmsErrorKind::NotFound,
                not_found_message(account_id, region, key_id),
            )
        };
        let owner_state = mas.get(owner).ok_or_else(not_found)?;
        let key = owner_state
            .keys
            .get(key_id_from_arn(&key_arn))
            .ok_or_else(not_found)?;
        for action in actions {
            authorize(
                key,
                &owner_state.grants,
                account_id,
                caller,
                action,
                encryption_context,
            )?;
        }
        require_enabled(key)?;
        Ok(key_arn)
    }

    /// The plaintext inside a ciphertext envelope sealed with
    /// `encryption_context`, read without the KMS key-state check or a usage
    /// record. For introspection only: tests inspecting stored SSE data see
    /// what was encrypted even after the key was disabled, and the read does
    /// not show up in `/_fakecloud/kms/usage`. `None` when `ciphertext_b64`
    /// is not an envelope this account sealed with that context.
    pub fn peek_plaintext(
        &self,
        account_id: &str,
        ciphertext_b64: &str,
        encryption_context: &HashMap<String, String>,
    ) -> Option<Vec<u8>> {
        self.open_envelope(account_id, ciphertext_b64, encryption_context)
            .ok()
            .map(|(_, plaintext)| plaintext)
    }

    /// Unseal a ciphertext envelope sealed with `encryption_context` into
    /// its key id and plaintext. A snapshot from before the hook bound
    /// contexts (see [`KmsState::legacy_context_free_ciphertexts`]) also
    /// opens the context-free ciphertexts it may hold.
    fn open_envelope(
        &self,
        account_id: &str,
        ciphertext_b64: &str,
        encryption_context: &HashMap<String, String>,
    ) -> Result<(String, Vec<u8>), KmsError> {
        let envelope_bytes = base64::engine::general_purpose::STANDARD
            .decode(ciphertext_b64)
            .map_err(|_| invalid_ciphertext())?;

        // Try AWS-shaped binary blob first using the master key of the
        // account whose key sealed it (a blob names another account's key by
        // its ARN); older textual envelopes fall through to the legacy
        // parser below.
        let sealing_account = crate::blob::embedded_key_id(&envelope_bytes)
            .map(|key_ref| key_owner_account(account_id, &key_ref).to_string())
            .unwrap_or_else(|| account_id.to_string());
        let (master_key_bytes, legacy) = {
            let mas = self.state.read();
            mas.get(&sealing_account).map_or((Vec::new(), false), |s| {
                (
                    s.master_key_bytes.clone(),
                    s.legacy_context_free_ciphertexts,
                )
            })
        };
        let aad = context_aad(encryption_context);
        if let Some(decoded) =
            crate::blob::decode_with_context(&master_key_bytes, &envelope_bytes, &aad)
        {
            return Ok((decoded.key_id, decoded.plaintext));
        }
        if legacy && !aad.is_empty() {
            if let Some(decoded) = crate::blob::decode(&master_key_bytes, &envelope_bytes) {
                return Ok((decoded.key_id, decoded.plaintext));
            }
        }
        if crate::blob::decode(&master_key_bytes, &envelope_bytes).is_some() {
            // Sealed by this account under another (or no) context.
            return Err(invalid_ciphertext());
        }
        let envelope = String::from_utf8(envelope_bytes).map_err(|_| invalid_ciphertext())?;
        let rest = envelope
            .strip_prefix("fakecloud-kms:")
            .ok_or_else(invalid_ciphertext)?;
        let (key_short, plaintext_b64) = rest.split_once(':').ok_or_else(invalid_ciphertext)?;
        let plaintext = base64::engine::general_purpose::STANDARD
            .decode(plaintext_b64)
            .map_err(|_| invalid_ciphertext())?;
        Ok((key_short.to_string(), plaintext))
    }

    /// Resolve `key_id` to its key ARN, provisioning an AWS-managed
    /// `aws/<service>` key on first use.
    pub fn resolve_key_arn(
        &self,
        account_id: &str,
        region: &str,
        key_id: &str,
        _service_principal: &str,
    ) -> Result<String, KmsError> {
        self.resolve_or_provision(account_id, region, key_id)
            .map(|(arn, _)| arn)
    }

    /// [`Self::resolve_key_arn`], also reporting whether an AWS-managed key
    /// was minted by this call (so a caller can persist KMS state).
    pub fn resolve_key_arn_tracked(
        &self,
        account_id: &str,
        region: &str,
        key_id: &str,
        _service_principal: &str,
    ) -> Result<(String, bool), KmsError> {
        self.resolve_or_provision(account_id, region, key_id)
    }

    /// The ARN of the AWS-managed key for `service` (`alias/aws/<service>`)
    /// in `account_id` and `region`, minted on first use; the flag is `true`
    /// when this call minted it. It is the key `alias/aws/<service>`
    /// resolves to in `region` (see [`Self::aws_managed_key`]). `None` when
    /// AWS has no AWS-managed key for `service`.
    pub fn aws_managed_key_arn_tracked(
        &self,
        account_id: &str,
        region: &str,
        service: &str,
        _service_principal: &str,
    ) -> Option<(String, bool)> {
        crate::aws_managed::is_aws_managed_service(service)
            .then(|| self.aws_managed_key(account_id, region, &format!("aws/{service}")))
    }

    /// Resolve `key_id` to its key ARN; the flag is `true` when an
    /// AWS-managed key had to be minted. Aliases resolve in `region`, or in
    /// the region an alias ARN names.
    fn resolve_or_provision(
        &self,
        account_id: &str,
        region: &str,
        key_id: &str,
    ) -> Result<(String, bool), KmsError> {
        // A key ARN or alias ARN names the account it lives in (see
        // `key_owner_account`); anything else is the resource account's.
        let owner = key_owner_account(account_id, key_id);
        // Pre-flight read to see if the key resolves cleanly.
        {
            let mas = self.state.read();
            if let Some(state) = mas.get(owner) {
                if let Some(arn) = resolve_key(state, region, key_id) {
                    return Ok((arn, false));
                }
            }
        }

        // An AWS-managed alias (`aws/<service>`) of a service AWS has one
        // for exists from its first use, in the region the reference names.
        // Only the account using it brings it into being: another account's
        // `aws/<service>` alias ARN creates nothing there (an AWS-managed
        // key is never usable across accounts). Any other alias or id that
        // doesn't resolve is an error.
        let alias = normalize_alias(key_id);
        let managed = alias
            .strip_prefix("aws/")
            .is_some_and(crate::aws_managed::is_aws_managed_service);
        if !managed || !names_an_alias(key_id) || owner != account_id {
            return Err(KmsError::new(
                KmsErrorKind::NotFound,
                not_found_message(owner, region, key_id),
            ));
        }
        let alias_region = parse_kms_arn(key_id).map_or(region, |(r, _, _)| r);
        Ok(self.aws_managed_key(account_id, alias_region, &alias))
    }

    /// The AWS-managed key for `alias` (`aws/<service>`) in `account_id` and
    /// `region`, minted on first use. One key exists per account AND region,
    /// and that region's `alias/aws/<service>` alias targets it.
    fn aws_managed_key(&self, account_id: &str, region: &str, alias: &str) -> (String, bool) {
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
        ensure_aws_managed_key(state, region, alias)
    }
}

/// The account whose key `key_id` names: the account of a key ARN or alias
/// ARN, which may be another account than the resource's (a queue or secret
/// in one account encrypted under another account's key); a key id or alias
/// name is the resource account's (`resource_account`).
pub fn key_owner_account<'a>(resource_account: &'a str, key_id: &'a str) -> &'a str {
    parse_kms_arn(key_id)
        .map(|(_, account, _)| account)
        .filter(|account| !account.is_empty())
        .unwrap_or(resource_account)
}

/// Whether `key_ref` names an alias (`alias/...`, an alias ARN, or the bare
/// `aws/<service>` form services use for their default key).
fn names_an_alias(key_ref: &str) -> bool {
    match strip_kms_arn_prefix(key_ref) {
        Some(resource) => resource.starts_with("alias/"),
        None => key_ref.starts_with("alias/") || key_ref.starts_with("aws/"),
    }
}

/// Authorize a fakecloud service's use of `key` through the KMS hook, as
/// AWS evaluates the call.
///
/// - [`KmsCaller::OnBehalfOf`]: the service calls KMS for a caller of its
///   API in `account_id`, so the key policy sees `kms:CallerAccount` and
///   `kms:ViaService` (that service's endpoint in the key's region). Only
///   AWS-managed keys are checked: their policy is AWS's (see
///   [`crate::aws_managed`]) and grants cryptographic use only through the
///   owning service, so S3 uses `aws/s3` but SQS cannot. A customer key of
///   the caller's own account is left to IAM: its policy is evaluated with
///   the calling identity, which the hook does not carry, and under IAM
///   enforcement that identity was already authorized for the service call
///   itself. A customer key of another account is usable only when its key
///   policy allows the caller (the calling identity when the service knows
///   it, else the caller's account) or a grant on the key does (`grants`, the
///   key account's grants), as AWS requires for cross-account use.
/// - [`KmsCaller::Service`]: the service principal acts itself, and only a
///   key policy statement naming it can allow that, so every key's policy
///   is checked (a customer key's default policy grants only the account).
fn authorize(
    key: &KmsKey,
    grants: &[crate::state::KmsGrant],
    account_id: &str,
    caller: KmsCaller<'_>,
    action: &str,
    encryption_context: &HashMap<String, String>,
) -> Result<(), KmsError> {
    let caller_arn = match caller {
        KmsCaller::OnBehalfOf { caller_arn, .. } => Some(caller_arn),
        KmsCaller::Service { .. } => None,
    };
    let region = parse_kms_arn(&key.arn).map_or("us-east-1", |(r, _, _)| r);
    let partition = fakecloud_aws::arn::partition_for(region);
    let root = format!("arn:{partition}:iam::{account_id}:root");
    let (caller, who) = match caller {
        KmsCaller::OnBehalfOf {
            service_principal,
            caller_arn,
        } => {
            let key_account = parse_kms_arn(&key.arn).map(|(_, account, _)| account);
            if key.key_manager != "AWS" && key_account == Some(account_id) {
                return Ok(());
            }
            (
                crate::aws_managed::KeyCaller::ViaService {
                    service_principal,
                    region,
                    account: account_id,
                    caller_arn,
                },
                // KMS names the identity the service acted for (its
                // forwarded credentials).
                format!("User: {}", caller_arn.unwrap_or(&root)),
            )
        }
        KmsCaller::Service {
            service_principal,
            source_arn,
            source_account,
            via_service,
        } => (
            crate::aws_managed::KeyCaller::Service {
                service_principal,
                region,
                source_arn,
                source_account,
                via_service,
            },
            // Recorded (EventBridge delivering to an `aws/sqs` queue): KMS
            // names a service principal without the key.
            format!("User: {service_principal}"),
        ),
    };
    let service_caller = matches!(caller, crate::aws_managed::KeyCaller::Service { .. });
    let context = encryption_context
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    if crate::aws_managed::policy_allows(&key.policy, &key.arn, &caller, action, &context, &[]) {
        return Ok(());
    }
    // A grant on the key lets an identity of the caller's account use it for
    // the operations it covers (AWS principals only; a service principal
    // acting itself is not a grantee).
    if let Some(caller_arn) = caller_arn {
        if key.key_manager != "AWS"
            && grant_allows(
                grants,
                key,
                &grantee_forms(caller_arn, account_id, partition),
                action,
                encryption_context,
            )
        {
            return Ok(());
        }
    }
    let message = if service_caller {
        format!(
            "{who} is not authorized to perform: {action} on this resource because the resource does not exist in this Region, no resource-based policies allow access, or a resource-based policy explicitly denies access"
        )
    } else {
        format!(
            "{who} is not authorized to perform: {action} on resource: {} because no resource-based policy allows the {action} action",
            key.arn
        )
    };
    Err(KmsError::new(KmsErrorKind::AccessDenied, message))
}

/// The grantee principals a grant can name for the calling identity
/// `caller_arn` of `account_id` (`None`: the account itself): the identity,
/// its role for an assumed-role session, and the account root (also named
/// by the bare account id).
fn grantee_forms(caller_arn: Option<&str>, account_id: &str, partition: &str) -> Vec<String> {
    let mut forms = vec![
        format!("arn:{partition}:iam::{account_id}:root"),
        account_id.to_string(),
    ];
    if let Some(arn) = caller_arn {
        forms.push(arn.to_string());
        if let Some((_, rest)) = arn.split_once(":assumed-role/") {
            let role = rest.split('/').next().unwrap_or(rest);
            forms.push(format!("arn:{partition}:iam::{account_id}:role/{role}"));
        }
    }
    forms
}

/// Whether a grant on `key` lets one of `grantees` perform `action`
/// (`kms:GenerateDataKey`, ...) with `encryption_context`: the grant names the
/// key, a grantee, and the operation, and its constraints
/// (`EncryptionContextSubset` / `EncryptionContextEquals`) hold.
fn grant_allows(
    grants: &[crate::state::KmsGrant],
    key: &KmsKey,
    grantees: &[String],
    action: &str,
    encryption_context: &HashMap<String, String>,
) -> bool {
    let operation = action.strip_prefix("kms:").unwrap_or(action);
    let key_id = key_id_from_arn(&key.arn);
    grants.iter().any(|grant| {
        let names_key = grant.key_id == key_id || grant.key_id == key.arn;
        let names_grantee = grantees.contains(&grant.grantee_principal);
        let covers = grant.operations.iter().any(|op| op == operation);
        names_key && names_grantee && covers && grant_constraints_hold(grant, encryption_context)
    })
}

/// Whether `grant`'s encryption-context constraints hold for
/// `encryption_context`.
fn grant_constraints_hold(
    grant: &crate::state::KmsGrant,
    encryption_context: &HashMap<String, String>,
) -> bool {
    let Some(constraints) = &grant.constraints else {
        return true;
    };
    let pairs = |name: &str| -> Option<HashMap<String, String>> {
        constraints.get(name)?.as_object().map(|map| {
            map.iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
                .collect()
        })
    };
    if let Some(subset) = pairs("EncryptionContextSubset") {
        if !subset
            .iter()
            .all(|(k, v)| encryption_context.get(k) == Some(v))
        {
            return false;
        }
    }
    if let Some(equals) = pairs("EncryptionContextEquals") {
        if &equals != encryption_context {
            return false;
        }
    }
    true
}

/// A key reference that resolves to nothing, as KMS answers it for `caller`.
/// An AWS service principal acting for its own resource is not told the key
/// is missing: KMS refuses it exactly as it refuses a key whose policy does
/// not name it (recorded: EventBridge delivering to a queue whose
/// `KmsMasterKeyId` is a missing alias, `eventbridge-sqs-target-failures.json`
/// in the `aws-managed-keys` fixtures). A caller of the account learns the
/// key is missing (`NotFoundException`).
pub fn missing_key_refusal(err: KmsError, caller: KmsCaller<'_>, action: &str) -> KmsError {
    match caller {
        KmsCaller::Service {
            service_principal, ..
        } if err.kind == KmsErrorKind::NotFound => KmsError::new(
            KmsErrorKind::AccessDenied,
            format!(
                "User: {service_principal} is not authorized to perform: {action} on this resource because the resource does not exist in this Region, no resource-based policies allow access, or a resource-based policy explicitly denies access"
            ),
        ),
        _ => err,
    }
}

/// Whether `stored` is a ciphertext KMS sealed: an AWS-shaped blob or the
/// legacy textual envelope. Whether it opens (its key, its context) is
/// decided by decrypting it; this only tells a ciphertext from a value a
/// service stored in plaintext.
fn is_kms_ciphertext(stored: &str) -> bool {
    let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(stored) else {
        return false;
    };
    crate::blob::is_envelope(&bytes) || bytes.starts_with(b"fakecloud-kms:")
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
        caller: KmsCaller<'_>,
        encryption_context: HashMap<String, String>,
    ) -> Result<String, KmsError> {
        KmsServiceHook::encrypt(
            self,
            account_id,
            region,
            key_id,
            plaintext,
            caller,
            encryption_context,
        )
    }

    fn encrypt_with_operation(
        &self,
        operation: &str,
        account_id: &str,
        region: &str,
        key_id: &str,
        plaintext: &[u8],
        caller: KmsCaller<'_>,
        encryption_context: HashMap<String, String>,
    ) -> Result<String, KmsError> {
        KmsServiceHook::encrypt_with_operation(
            self,
            operation,
            account_id,
            region,
            key_id,
            plaintext,
            caller,
            encryption_context,
        )
    }

    fn decrypt(
        &self,
        account_id: &str,
        ciphertext_b64: &str,
        caller: KmsCaller<'_>,
        encryption_context: HashMap<String, String>,
    ) -> Result<Vec<u8>, KmsError> {
        KmsServiceHook::decrypt(self, account_id, ciphertext_b64, caller, encryption_context)
    }

    fn is_ciphertext(&self, _account_id: &str, stored: &str) -> bool {
        is_kms_ciphertext(stored)
    }

    fn authorize_key_use(
        &self,
        account_id: &str,
        region: &str,
        key_id: &str,
        caller: KmsCaller<'_>,
        actions: &[&str],
        encryption_context: &HashMap<String, String>,
    ) -> Result<String, KmsError> {
        KmsServiceHook::authorize_key_use(
            self,
            account_id,
            region,
            key_id,
            caller,
            actions,
            encryption_context,
        )
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
        self.aws_managed_key_arn_tracked(account_id, region, service, service_principal)
            .map(|(arn, _)| arn)
            .ok_or_else(|| format!("AWS has no AWS-managed KMS key for {service}"))
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
pub(crate) fn ensure_aws_managed_key(
    state: &mut KmsState,
    region: &str,
    alias: &str,
) -> (String, bool) {
    let alias_full = format!("alias/{alias}");
    let (arn, minted) = match existing_aws_managed_key(state, region, &alias_full) {
        Some(arn) => (arn, false),
        None => (mint_aws_managed_key(state, region, alias), true),
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

/// Give AWS-managed keys minted by an older fakecloud the policy (and
/// description) AWS gives them (see [`crate::aws_managed::key_policy`]). AWS
/// refuses `PutKeyPolicy` (and every other key-management operation) on
/// AWS-managed keys, so their policy can never differ from AWS's; any other
/// policy found (the generic one fakecloud used to generate, or one set
/// through its `PutKeyPolicy` before that refused AWS-managed keys) is
/// replaced.
pub(crate) fn migrate_aws_managed_key_policies(state: &mut KmsState) {
    // Both the recorded slots and the `alias/aws/<service>` aliases:
    // snapshots from before the slots were recorded reach the key only by
    // its alias.
    let mut slots: std::collections::BTreeSet<(String, String, String)> = state
        .aws_managed_keys
        .iter()
        .filter_map(|(slot, id)| {
            let (region, name) = slot.split_once('/')?;
            let service = crate::aws_managed::alias_service(name)?;
            Some((region.to_string(), service.to_string(), id.clone()))
        })
        .collect();
    for (region, aliases) in &state.aliases {
        for (name, alias) in aliases {
            if let Some(service) = crate::aws_managed::alias_service(name) {
                slots.insert((
                    region.clone(),
                    service.to_string(),
                    alias.target_key_id.clone(),
                ));
            }
        }
    }
    for (region, service, key_id) in slots {
        let account = state.account_id.clone();
        let Some(key) = state.keys.get_mut(&key_id) else {
            continue;
        };
        if key.key_manager != "AWS" {
            continue;
        }
        // A key fakecloud minted for a name AWS has no AWS-managed key for
        // (`aws/logs`, ...) keeps what it has: it was never AWS's.
        if let Some(recorded) = crate::aws_managed::key_policy(&service, &account, &region) {
            if key.policy != recorded {
                key.policy = recorded;
            }
        }
        if let Some(description) = crate::aws_managed::key_description(&service) {
            if key.description != description {
                key.description = description;
            }
        }
    }
}

/// Mint an AWS-managed key protecting `alias` (`aws/<service>`) in `region`.
fn mint_aws_managed_key(state: &mut KmsState, region: &str, alias: &str) -> String {
    let key_id = uuid::Uuid::new_v4().to_string();
    let arn = kms_key_arn(region, &state.account_id, &key_id);
    let service = alias.strip_prefix("aws/").unwrap_or(alias);
    // Callers mint only catalog services (see `resolve_or_provision`); the
    // fallbacks keep a key minted for any other name well-formed.
    let policy = crate::aws_managed::key_policy(service, &state.account_id, region)
        .unwrap_or_else(|| r#"{"Version":"2012-10-17","Statement":[]}"#.to_string());
    let key = KmsKey {
        key_id: key_id.clone(),
        arn: arn.clone(),
        creation_date: Utc::now().timestamp() as f64,
        description: crate::aws_managed::key_description(service).unwrap_or_else(|| {
            format!("Default key that protects my {service} data when no other key is defined")
        }),
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
                KmsCaller::via("sqs.amazonaws.com"),
                HashMap::new(),
            )
            .unwrap();
        let recorded = usage.read().records().len();
        for key in state.write().get_or_create(acct).keys.values_mut() {
            key.enabled = false;
            key.key_state = "Disabled".to_string();
        }
        assert!(hook
            .decrypt(
                acct,
                &ct,
                KmsCaller::via("sqs.amazonaws.com"),
                HashMap::new()
            )
            .is_err());
        assert_eq!(
            hook.peek_plaintext(acct, &ct, &HashMap::new()).as_deref(),
            Some(&b"hello"[..])
        );
        assert_eq!(usage.read().records().len(), recorded);
        assert_eq!(
            hook.peek_plaintext(acct, "plain text body", &HashMap::new()),
            None
        );
        assert_eq!(
            hook.peek_plaintext("999999999999", &ct, &HashMap::new()),
            None
        );
    }

    /// A service principal acting for its resource is refused a missing key
    /// as it is refused a key whose policy does not name it (recorded,
    /// `eventbridge-sqs-target-failures.json`); a caller of the account is
    /// told the key is missing.
    #[test]
    fn a_missing_key_refuses_a_service_principal_as_access_denied() {
        let acct = "123456789012";
        let state: SharedKmsState = std::sync::Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new(acct, "us-east-1", ""),
        ));
        let hook = KmsServiceHook::new(state, SharedKmsUsageState::default());
        let events = KmsCaller::Service {
            service_principal: "events.amazonaws.com",
            source_arn: "arn:aws:events:us-east-1:123456789012:rule/r",
            source_account: acct,
            via_service: Some("sqs.amazonaws.com"),
        };
        let err = hook
            .encrypt(
                acct,
                "us-east-1",
                "alias/fc-does-not-exist",
                b"x",
                events,
                HashMap::new(),
            )
            .unwrap_err();
        assert_eq!(err.kind, KmsErrorKind::AccessDenied);
        assert_eq!(
            err.message,
            "User: events.amazonaws.com is not authorized to perform: kms:GenerateDataKey on this resource because the resource does not exist in this Region, no resource-based policies allow access, or a resource-based policy explicitly denies access"
        );
        let err = hook
            .authorize_key_use(
                acct,
                "us-east-1",
                "alias/fc-does-not-exist",
                events,
                &["kms:GenerateDataKey"],
                &HashMap::new(),
            )
            .unwrap_err();
        assert_eq!(err.kind, KmsErrorKind::AccessDenied);

        let err = hook
            .encrypt(
                acct,
                "us-east-1",
                "alias/fc-does-not-exist",
                b"x",
                KmsCaller::via("sqs.amazonaws.com"),
                HashMap::new(),
            )
            .unwrap_err();
        assert_eq!(err.kind, KmsErrorKind::NotFound);
    }

    /// The policy fakecloud used to give AWS-managed keys before their AWS
    /// policies were recorded.
    const GENERIC_OLD_POLICY: &str = r#"{"Version":"2012-10-17","Statement":[{"Sid":"Allow access through service","Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":["kms:GenerateDataKey","kms:Decrypt","kms:DescribeKey"],"Resource":"*"}]}"#;

    /// A snapshot from before `aws_managed_keys` was recorded reaches its
    /// `aws/lambda` key only through the alias; loading it migrates the
    /// key's policy too.
    #[test]
    fn old_aws_lambda_keys_reached_only_by_alias_are_migrated() {
        let mut state = KmsState::new("123456789012", "us-east-1");
        let arn = ensure_aws_managed_key(&mut state, "us-east-1", "aws/lambda").0;
        let id = key_id_from_arn(&arn).to_string();
        state.keys.get_mut(&id).unwrap().policy = GENERIC_OLD_POLICY.to_string();
        let mut snapshot = serde_json::to_value(&state).unwrap();
        snapshot.as_object_mut().unwrap().remove("aws_managed_keys");
        let loaded: KmsState = serde_json::from_value(snapshot).unwrap();
        assert!(loaded.aws_managed_keys.is_empty());
        assert_eq!(
            loaded.keys[&id].policy,
            crate::aws_managed::key_policy("lambda", "123456789012", "us-east-1").unwrap()
        );
    }

    /// The recorded `aws/lambda` key policy names the account root in the
    /// partition of the key's region.
    #[test]
    fn the_aws_lambda_key_policy_uses_the_region_partition() {
        for (region, partition) in [("cn-north-1", "aws-cn"), ("us-gov-west-1", "aws-us-gov")] {
            let mut state = KmsState::new("123456789012", region);
            let arn = ensure_aws_managed_key(&mut state, region, "aws/lambda").0;
            let policy: serde_json::Value =
                serde_json::from_str(&state.keys[key_id_from_arn(&arn)].policy).unwrap();
            let root = policy["Statement"]
                .as_array()
                .unwrap()
                .iter()
                .find(|st| st["Sid"] == "Allow direct access to key metadata to the account")
                .unwrap();
            assert_eq!(
                root["Principal"]["AWS"],
                format!("arn:{partition}:iam::123456789012:root")
            );
            assert_eq!(
                policy["Statement"][0]["Condition"]["StringEquals"]["kms:ViaService"],
                format!(
                    "lambda.{region}.{}",
                    fakecloud_aws::endpoint::dns_suffix_for_region(region)
                )
            );
        }
    }

    /// Snapshots from before the AWS policies were recorded hold the generic
    /// generated policy and description (or a policy an older fakecloud let
    /// PutKeyPolicy set); loading one gives every AWS-managed key AWS's
    /// policy and description for its service and region, and leaves
    /// customer keys alone.
    #[test]
    fn old_aws_managed_key_policies_are_migrated_on_load() {
        let mut state = KmsState::new("123456789012", "us-east-1");
        let old = ensure_aws_managed_key(&mut state, "us-east-1", "aws/lambda").0;
        let changed = ensure_aws_managed_key(&mut state, "eu-west-1", "aws/lambda").0;
        let sqs = ensure_aws_managed_key(&mut state, "us-east-1", "aws/sqs").0;
        let s3 = ensure_aws_managed_key(&mut state, "us-east-1", "aws/s3").0;
        let custom = r#"{"Version":"2012-10-17","Statement":[]}"#.to_string();
        for arn in [&old, &sqs, &s3] {
            let key = state.keys.get_mut(key_id_from_arn(arn)).unwrap();
            key.policy = GENERIC_OLD_POLICY.to_string();
            key.description = "Default master key that protects aws/x".to_string();
        }
        state
            .keys
            .get_mut(key_id_from_arn(&changed))
            .unwrap()
            .policy = custom.clone();
        // A customer key that happens to carry an old-looking policy.
        let customer = mint_aws_managed_key(&mut state, "us-east-1", "custom");
        {
            let key = state.keys.get_mut(key_id_from_arn(&customer)).unwrap();
            key.key_manager = "CUSTOMER".to_string();
            key.policy = custom.clone();
        }
        let loaded: KmsState =
            serde_json::from_str(&serde_json::to_string(&state).unwrap()).unwrap();
        let policy = |arn: &str| loaded.keys[key_id_from_arn(arn)].policy.clone();
        let acct = "123456789012";
        assert_eq!(
            policy(&old),
            crate::aws_managed::key_policy("lambda", acct, "us-east-1").unwrap()
        );
        assert_eq!(
            policy(&changed),
            crate::aws_managed::key_policy("lambda", acct, "eu-west-1").unwrap()
        );
        assert_eq!(
            policy(&sqs),
            crate::aws_managed::key_policy("sqs", acct, "us-east-1").unwrap()
        );
        assert_eq!(
            policy(&s3),
            crate::aws_managed::key_policy("s3", acct, "us-east-1").unwrap()
        );
        assert_eq!(
            loaded.keys[key_id_from_arn(&s3)].description,
            "Default key that protects my S3 objects when no other key is defined"
        );
        assert_eq!(policy(&customer), custom);
    }

    /// The hook acts through the service that calls it: a service encrypts
    /// and decrypts under its own `aws/<service>` key, another service's
    /// `aws/*` key is refused by that key's policy, and customer keys are
    /// not policy-checked here.
    #[test]
    fn hook_uses_aws_managed_keys_only_through_their_service() {
        let acct = "123456789012";
        let (state, _) = crate::test_support::kms_hook(acct);
        let hook = KmsServiceHook::new(state.clone(), Default::default());
        let ct = hook
            .encrypt(
                acct,
                "us-east-1",
                "alias/aws/s3",
                b"object",
                KmsCaller::via("s3.amazonaws.com"),
                HashMap::new(),
            )
            .unwrap();
        assert_eq!(
            hook.decrypt(
                acct,
                &ct,
                KmsCaller::via("s3.amazonaws.com"),
                HashMap::new()
            )
            .unwrap(),
            b"object"
        );
        let err = hook
            .decrypt(
                acct,
                &ct,
                KmsCaller::via("sqs.amazonaws.com"),
                HashMap::new(),
            )
            .unwrap_err();
        assert_eq!(err.kind, KmsErrorKind::AccessDenied, "{err}");
        let err = hook
            .encrypt(
                acct,
                "us-east-1",
                "alias/aws/s3",
                b"msg",
                KmsCaller::via("sqs.amazonaws.com"),
                HashMap::new(),
            )
            .unwrap_err();
        assert!(
            err.kind == KmsErrorKind::AccessDenied
                && err
                    .message
                    .contains("on resource: arn:aws:kms:us-east-1:123456789012:key/"),
            "{err}"
        );
        // Secrets Manager derives data keys through any region's endpoint.
        hook.encrypt(
            acct,
            "eu-west-1",
            "alias/aws/secretsmanager",
            b"secret",
            KmsCaller::via("secretsmanager.amazonaws.com"),
            HashMap::new(),
        )
        .unwrap();
        // A customer key carries no via-service restriction in the hook.
        let customer = {
            let mut accounts = state.write();
            let s = accounts.get_or_create(acct);
            let arn = mint_aws_managed_key(s, "us-east-1", "custom");
            s.keys.get_mut(key_id_from_arn(&arn)).unwrap().key_manager = "CUSTOMER".into();
            arn
        };
        let ct = hook
            .encrypt(
                acct,
                "us-east-1",
                &customer,
                b"x",
                KmsCaller::via("sqs.amazonaws.com"),
                HashMap::new(),
            )
            .unwrap();
        hook.decrypt(
            acct,
            &ct,
            KmsCaller::via("sns.amazonaws.com"),
            HashMap::new(),
        )
        .unwrap();
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
            let arn = mint_aws_managed_key(s, "eu-west-1", "custom");
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

    /// A region's AWS-managed key, once created, is the key that region's
    /// services report; other regions get their own. A name AWS has no
    /// AWS-managed key for is not created (recorded: `alias/aws/logs`).
    #[test]
    fn aws_managed_keys_exist_per_region_and_only_for_catalog_services() {
        use fakecloud_core::delivery::aws_managed_kms_key_arn;
        let acct = "123456789012";
        let (state, hook) = crate::test_support::kms_hook(acct);
        let h = Some(hook.as_ref());
        let west = aws_managed_kms_key_arn(h, acct, "eu-west-1", "elasticfilesystem").unwrap();
        assert_eq!(
            aws_managed_kms_key_arn(h, acct, "eu-west-1", "elasticfilesystem").as_deref(),
            Some(west.as_str())
        );
        let east = aws_managed_kms_key_arn(h, acct, "us-east-1", "elasticfilesystem").unwrap();
        assert_ne!(east, west);
        assert!(east.starts_with("arn:aws:kms:us-east-1:"));
        assert_eq!(aws_managed_kms_key_arn(h, acct, "us-east-1", "logs"), None);
        let err = KmsServiceHook::new(state.clone(), Default::default())
            .encrypt(
                acct,
                "us-east-1",
                "alias/aws/logs",
                b"x",
                KmsCaller::via("logs.amazonaws.com"),
                HashMap::new(),
            )
            .unwrap_err();
        assert_eq!(
            (err.kind, err.message.as_str()),
            (
                KmsErrorKind::NotFound,
                "Alias arn:aws:kms:us-east-1:123456789012:alias/aws/logs is not found."
            )
        );
        assert!(state
            .read()
            .get(acct)
            .unwrap()
            .alias("us-east-1", "alias/aws/logs")
            .is_none());
    }

    /// KMS's not-found messages name an alias by its ARN and a key by its
    /// key ARN (recorded through SQS, SNS, S3, SSM and Secrets Manager).
    #[test]
    fn not_found_messages_follow_kms() {
        let acct = "123456789012";
        assert_eq!(
            not_found_message(acct, "us-east-1", "alias/fc-does-not-exist"),
            "Alias arn:aws:kms:us-east-1:123456789012:alias/fc-does-not-exist is not found."
        );
        assert_eq!(
            not_found_message(
                acct,
                "us-east-1",
                "arn:aws:kms:us-east-1:123456789012:key/00000000-0000-0000-0000-000000000000"
            ),
            "Key 'arn:aws:kms:us-east-1:123456789012:key/00000000-0000-0000-0000-000000000000' does not exist"
        );
        assert_eq!(
            not_found_message(acct, "eu-west-1", "1234abcd-12ab-34cd-56ef-1234567890ab"),
            "Key 'arn:aws:kms:eu-west-1:123456789012:key/1234abcd-12ab-34cd-56ef-1234567890ab' does not exist"
        );
    }

    /// Hook ciphertexts bind the service's encryption context as KMS
    /// ciphertexts do: another or a missing context does not open them, and
    /// the public KMS `Decrypt` opens them with the same context.
    #[test]
    fn hook_ciphertexts_bind_the_encryption_context() {
        let acct = "123456789012";
        let (state, _) = crate::test_support::kms_hook(acct);
        let hook = KmsServiceHook::new(state.clone(), Default::default());
        let queue = "arn:aws:sqs:us-east-1:123456789012:q";
        let ctx: HashMap<String, String> = [("aws:sqs:arn".to_string(), queue.to_string())].into();
        let ct = hook
            .encrypt(
                acct,
                "us-east-1",
                "alias/aws/sqs",
                b"body",
                KmsCaller::via("sqs.amazonaws.com"),
                ctx.clone(),
            )
            .unwrap();
        let sqs = KmsCaller::via("sqs.amazonaws.com");
        assert_eq!(hook.decrypt(acct, &ct, sqs, ctx.clone()).unwrap(), b"body");
        for other in [
            HashMap::new(),
            [("aws:sqs:arn".to_string(), format!("{queue}2"))].into(),
        ] {
            let err = hook.decrypt(acct, &ct, sqs, other).unwrap_err();
            assert_eq!(err.kind, KmsErrorKind::InvalidCiphertext);
        }
        assert_eq!(hook.peek_plaintext(acct, &ct, &HashMap::new()), None);
        assert_eq!(
            hook.peek_plaintext(acct, &ct, &ctx).as_deref(),
            Some(&b"body"[..])
        );
    }

    /// A snapshot from before contexts were bound may hold context-free
    /// service ciphertexts; they keep decrypting with the service's context.
    /// State created since does not accept them.
    #[test]
    fn context_free_ciphertexts_from_old_snapshots_still_decrypt() {
        let acct = "123456789012";
        let mut fresh = KmsState::new(acct, "us-east-1");
        let arn = ensure_aws_managed_key(&mut fresh, "us-east-1", "aws/sqs").0;
        let key_id = key_id_from_arn(&arn).to_string();
        // What the hook sealed before it bound contexts.
        let old = base64::engine::general_purpose::STANDARD.encode(crate::blob::encode(
            &fresh.master_key_bytes,
            &key_id,
            b"old body",
        ));
        let ctx: HashMap<String, String> = [(
            "aws:sqs:arn".to_string(),
            "arn:aws:sqs:us-east-1:123456789012:q".to_string(),
        )]
        .into();
        let sqs = KmsCaller::via("sqs.amazonaws.com");

        let mut snapshot = serde_json::to_value(&fresh).unwrap();
        snapshot
            .as_object_mut()
            .unwrap()
            .remove("legacy_context_free_ciphertexts");
        let loaded: KmsState = serde_json::from_value(snapshot).unwrap();
        assert!(loaded.legacy_context_free_ciphertexts);
        let shared: SharedKmsState = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new(acct, "us-east-1", ""),
        ));
        *shared.write().get_or_create(acct) = loaded;
        let hook = KmsServiceHook::new(shared.clone(), Default::default());
        assert_eq!(
            hook.decrypt(acct, &old, sqs, ctx.clone()).unwrap(),
            b"old body"
        );
        // And it survives another save and load.
        let again: KmsState =
            serde_json::from_str(&serde_json::to_string(shared.read().get(acct).unwrap()).unwrap())
                .unwrap();
        assert!(again.legacy_context_free_ciphertexts);

        let current: SharedKmsState = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new(acct, "us-east-1", ""),
        ));
        *current.write().get_or_create(acct) =
            serde_json::from_str(&serde_json::to_string(&fresh).unwrap()).unwrap();
        let hook = KmsServiceHook::new(current, Default::default());
        let err = hook.decrypt(acct, &old, sqs, ctx).unwrap_err();
        assert_eq!(err.kind, KmsErrorKind::InvalidCiphertext);
    }

    /// The hook refuses keys that cannot encrypt, with KMS's errors.
    #[test]
    fn hook_reports_unusable_keys() {
        let acct = "123456789012";
        let (state, _) = crate::test_support::kms_hook(acct);
        let hook = KmsServiceHook::new(state.clone(), Default::default());
        let arn = {
            let mut accounts = state.write();
            let s = accounts.get_or_create(acct);
            let arn = mint_aws_managed_key(s, "us-east-1", "custom");
            s.keys.get_mut(key_id_from_arn(&arn)).unwrap().key_manager = "CUSTOMER".into();
            arn
        };
        let set_state = |key_state: &str, usage: &str| {
            let mut accounts = state.write();
            let key = accounts
                .get_or_create(acct)
                .keys
                .get_mut(key_id_from_arn(&arn))
                .unwrap();
            key.key_state = key_state.to_string();
            key.enabled = key_state == "Enabled";
            key.key_usage = usage.to_string();
        };
        let encrypt = || {
            hook.encrypt(
                acct,
                "us-east-1",
                &arn,
                b"x",
                KmsCaller::via("sqs.amazonaws.com"),
                HashMap::new(),
            )
        };
        set_state("Disabled", "ENCRYPT_DECRYPT");
        let err = encrypt().unwrap_err();
        assert_eq!(
            (err.kind, err.message),
            (KmsErrorKind::Disabled, format!("{arn} is disabled."))
        );
        set_state("PendingDeletion", "ENCRYPT_DECRYPT");
        let err = encrypt().unwrap_err();
        assert_eq!(
            (err.kind, err.message),
            (
                KmsErrorKind::InvalidState,
                format!("{arn} is pending deletion.")
            )
        );
        set_state("Enabled", "SIGN_VERIFY");
        assert_eq!(encrypt().unwrap_err().kind, KmsErrorKind::InvalidKeyUsage);
        set_state("Enabled", "ENCRYPT_DECRYPT");
        encrypt().unwrap();
        let err = hook
            .encrypt(
                acct,
                "us-east-1",
                "alias/fc-does-not-exist",
                b"x",
                KmsCaller::via("sqs.amazonaws.com"),
                HashMap::new(),
            )
            .unwrap_err();
        assert_eq!(err.kind, KmsErrorKind::NotFound);
    }

    /// A service acting as its own principal (SES) needs a key policy
    /// statement naming it: `aws/ses` has one, a customer key's default
    /// policy does not.
    #[test]
    fn service_principal_callers_need_a_key_policy_grant() {
        let acct = "123456789012";
        let (state, _) = crate::test_support::kms_hook(acct);
        let hook = KmsServiceHook::new(state.clone(), Default::default());
        let rule = "arn:aws:ses:us-east-1:123456789012:receipt-rule-set/s:receipt-rule/r";
        let ses = KmsCaller::Service {
            service_principal: "ses.amazonaws.com",
            source_arn: rule,
            source_account: acct,
            via_service: None,
        };
        let ctx: HashMap<String, String> = [
            ("aws:ses:source-account".to_string(), acct.to_string()),
            ("aws:ses:rule-name".to_string(), "r".to_string()),
            ("aws:ses:message-id".to_string(), "m".to_string()),
        ]
        .into();
        hook.encrypt(
            acct,
            "us-east-1",
            "alias/aws/ses",
            b"mail",
            ses,
            ctx.clone(),
        )
        .unwrap();
        let customer = {
            let mut accounts = state.write();
            let s = accounts.get_or_create(acct);
            let arn = mint_aws_managed_key(s, "us-east-1", "custom");
            let key = s.keys.get_mut(key_id_from_arn(&arn)).unwrap();
            key.key_manager = "CUSTOMER".into();
            key.policy = format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"arn:aws:iam::{acct}:root"}},"Action":"kms:*","Resource":"*"}}]}}"#
            );
            arn
        };
        let err = hook
            .encrypt(acct, "us-east-1", &customer, b"mail", ses, ctx.clone())
            .unwrap_err();
        assert_eq!(err.kind, KmsErrorKind::AccessDenied);
        // SES's documented customer key policy statement admits it.
        {
            let mut accounts = state.write();
            let key = accounts
                .get_or_create(acct)
                .keys
                .get_mut(key_id_from_arn(&customer))
                .unwrap();
            key.policy = format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"Service":"ses.amazonaws.com"}},"Action":["kms:Encrypt","kms:GenerateDataKey*"],"Resource":"*","Condition":{{"StringEquals":{{"aws:SourceAccount":"{acct}"}},"ArnLike":{{"aws:SourceArn":"arn:aws:ses:*"}}}}}}]}}"#
            );
        }
        hook.encrypt(acct, "us-east-1", &customer, b"mail", ses, ctx)
            .unwrap();
    }

    /// A resource in one account encrypted under a customer key ARN of
    /// another account uses that account's key: allowed when the key policy
    /// grants the resource's account, refused (AccessDenied naming the key)
    /// when it grants only its own. Ciphertexts it seals open from the
    /// resource's account. A bare key id still resolves in the resource's
    /// account.
    #[test]
    fn cross_account_customer_keys_follow_their_key_policy() {
        let a = "111111111111";
        let b = "222222222222";
        let (state, _) = crate::test_support::kms_hook(a);
        let hook = KmsServiceHook::new(state.clone(), Default::default());
        let key_in_b = |grantee: &str| {
            let mut accounts = state.write();
            let s = accounts.get_or_create(b);
            let arn = mint_aws_managed_key(s, "us-east-1", &format!("custom-{grantee}"));
            let key = s.keys.get_mut(key_id_from_arn(&arn)).unwrap();
            key.key_manager = "CUSTOMER".into();
            key.policy = format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"arn:aws:iam::{grantee}:root"}},"Action":"kms:*","Resource":"*"}}]}}"#
            );
            arn
        };
        let shared = key_in_b(a);
        let private = key_in_b(b);
        assert!(shared.contains(":222222222222:key/"), "{shared}");
        let ctx: HashMap<String, String> = [(
            "aws:sqs:arn".to_string(),
            format!("arn:aws:sqs:us-east-1:{a}:q"),
        )]
        .into();
        let sqs = KmsCaller::via("sqs.amazonaws.com");

        let sealed = hook
            .encrypt(a, "us-east-1", &shared, b"hello", sqs, ctx.clone())
            .unwrap();
        assert_eq!(
            hook.decrypt(a, &sealed, sqs, ctx.clone()).unwrap(),
            b"hello".to_vec()
        );
        assert_eq!(
            hook.authorize_key_use(a, "us-east-1", &shared, sqs, &["kms:GenerateDataKey"], &ctx)
                .unwrap(),
            shared
        );

        let err = hook
            .encrypt(a, "us-east-1", &private, b"hello", sqs, ctx.clone())
            .unwrap_err();
        assert_eq!(err.kind, KmsErrorKind::AccessDenied);
        assert_eq!(
            err.message,
            format!(
                "User: arn:aws:iam::{a}:root is not authorized to perform: kms:GenerateDataKey on resource: {private} because no resource-based policy allows the kms:GenerateDataKey action"
            )
        );

        // B's key id alone is not a key of A.
        let err = hook
            .encrypt(a, "us-east-1", key_id_from_arn(&shared), b"x", sqs, ctx)
            .unwrap_err();
        assert_eq!(err.kind, KmsErrorKind::NotFound);
    }

    /// Another account's `aws/<service>` alias ARN brings no key into being
    /// there (AWS-managed keys are never usable across accounts): the use is
    /// refused and the other account holds no key.
    #[test]
    fn another_accounts_aws_managed_alias_creates_nothing() {
        let a = "111111111111";
        let b = "222222222222";
        let (state, _) = crate::test_support::kms_hook(a);
        let hook = KmsServiceHook::new(state.clone(), Default::default());
        let alias = format!("arn:aws:kms:us-east-1:{b}:alias/aws/sqs");
        let ctx = HashMap::new();
        let err = hook
            .encrypt(
                a,
                "us-east-1",
                &alias,
                b"x",
                KmsCaller::via("sqs.amazonaws.com"),
                ctx.clone(),
            )
            .unwrap_err();
        assert_eq!(err.kind, KmsErrorKind::NotFound);
        let events = KmsCaller::Service {
            service_principal: "events.amazonaws.com",
            source_arn: "arn:aws:events:us-east-1:111111111111:rule/r",
            source_account: a,
            via_service: Some("sqs.amazonaws.com"),
        };
        let err = hook
            .authorize_key_use(
                a,
                "us-east-1",
                &alias,
                events,
                &["kms:GenerateDataKey"],
                &ctx,
            )
            .unwrap_err();
        assert_eq!(err.kind, KmsErrorKind::AccessDenied);
        assert!(hook
            .resolve_key_arn_tracked(a, "us-east-1", &alias, "")
            .is_err());
        assert!(state.read().get(b).is_none_or(|s| s.keys.is_empty()));
    }

    /// A cross-account key policy is evaluated against the calling identity
    /// when the service knows it: a statement naming a role admits that
    /// role's sessions and no other identity of the account; a bare account
    /// id names the account's root, admitting any identity of it.
    #[test]
    fn cross_account_key_policies_see_the_calling_identity() {
        let a = "111111111111";
        let b = "222222222222";
        let (state, _) = crate::test_support::kms_hook(a);
        let hook = KmsServiceHook::new(state.clone(), Default::default());
        let key_in_b = |name: &str, principal: &str| {
            let mut accounts = state.write();
            let s = accounts.get_or_create(b);
            let arn = mint_aws_managed_key(s, "us-east-1", name);
            let key = s.keys.get_mut(key_id_from_arn(&arn)).unwrap();
            key.key_manager = "CUSTOMER".into();
            key.policy = format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"kms:*","Resource":"*"}}]}}"#
            );
            arn
        };
        let role_key = key_in_b("role-key", &format!("arn:aws:iam::{a}:role/app"));
        let account_key = key_in_b("account-key", a);
        let ctx = HashMap::new();
        let session = format!("arn:aws:sts::{a}:assumed-role/app/s1");
        let other = format!("arn:aws:iam::{a}:user/other");
        let via = |arn| KmsCaller::via_for("sqs.amazonaws.com", Some(arn));

        hook.encrypt(a, "us-east-1", &role_key, b"x", via(&session), ctx.clone())
            .unwrap();
        let err = hook
            .encrypt(a, "us-east-1", &role_key, b"x", via(&other), ctx.clone())
            .unwrap_err();
        assert_eq!(err.kind, KmsErrorKind::AccessDenied);
        assert!(
            err.message.starts_with(&format!("User: {other} ")),
            "{}",
            err.message
        );
        // Unknown caller: the account root, which the role statement does not
        // name.
        let err = hook
            .encrypt(
                a,
                "us-east-1",
                &role_key,
                b"x",
                KmsCaller::via("sqs.amazonaws.com"),
                ctx.clone(),
            )
            .unwrap_err();
        assert_eq!(err.kind, KmsErrorKind::AccessDenied);

        for caller in [
            via(&session),
            via(&other),
            KmsCaller::via("sqs.amazonaws.com"),
        ] {
            hook.encrypt(a, "us-east-1", &account_key, b"x", caller, ctx.clone())
                .unwrap();
        }
    }

    /// A grant on another account's key lets the grantee (an identity of the
    /// caller's account, or that account) use it through a service for the
    /// operations it covers, within its encryption-context constraints;
    /// without a grant the key policy alone decides.
    #[test]
    fn grants_admit_cross_account_use() {
        let a = "111111111111";
        let b = "222222222222";
        let (state, _) = crate::test_support::kms_hook(a);
        let hook = KmsServiceHook::new(state.clone(), Default::default());
        let key = {
            let mut accounts = state.write();
            let s = accounts.get_or_create(b);
            let arn = mint_aws_managed_key(s, "us-east-1", "granted");
            let key = s.keys.get_mut(key_id_from_arn(&arn)).unwrap();
            key.key_manager = "CUSTOMER".into();
            key.policy = format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"arn:aws:iam::{b}:root"}},"Action":"kms:*","Resource":"*"}}]}}"#
            );
            arn
        };
        let queue = format!("arn:aws:sqs:us-east-1:{a}:q");
        let ctx: HashMap<String, String> = [("aws:sqs:arn".to_string(), queue.clone())].into();
        let caller = KmsCaller::via_for(
            "sqs.amazonaws.com",
            Some("arn:aws:iam::111111111111:user/u"),
        );
        let refused = |hook: &KmsServiceHook, ctx: &HashMap<String, String>| {
            hook.encrypt(a, "us-east-1", &key, b"x", caller, ctx.clone())
                .unwrap_err()
                .kind
        };
        assert_eq!(refused(&hook, &ctx), KmsErrorKind::AccessDenied);

        let grant =
            |grantee: String, operations: &[&str], constraints: Option<serde_json::Value>| {
                crate::state::KmsGrant {
                    grant_id: uuid::Uuid::new_v4().to_string(),
                    grant_token: uuid::Uuid::new_v4().to_string(),
                    key_id: key_id_from_arn(&key).to_string(),
                    grantee_principal: grantee,
                    retiring_principal: None,
                    operations: operations.iter().map(|o| o.to_string()).collect(),
                    constraints,
                    name: None,
                    creation_date: 0.0,
                }
            };
        // A grant that does not cover GenerateDataKey, and one whose context
        // constraint does not hold, admit nothing.
        {
            let mut accounts = state.write();
            let grants = &mut accounts.get_or_create(b).grants;
            grants.push(grant(format!("arn:aws:iam::{a}:root"), &["Decrypt"], None));
            grants.push(grant(
                format!("arn:aws:iam::{a}:root"),
                &["GenerateDataKey"],
                Some(serde_json::json!({"EncryptionContextSubset": {"aws:sqs:arn": "other"}})),
            ));
        }
        assert_eq!(refused(&hook, &ctx), KmsErrorKind::AccessDenied);

        state.write().get_or_create(b).grants.push(grant(
            format!("arn:aws:iam::{a}:root"),
            &["GenerateDataKey", "Decrypt"],
            Some(serde_json::json!({"EncryptionContextSubset": {"aws:sqs:arn": queue}})),
        ));
        let sealed = hook
            .encrypt(a, "us-east-1", &key, b"hello", caller, ctx.clone())
            .unwrap();
        assert_eq!(hook.decrypt(a, &sealed, caller, ctx).unwrap(), b"hello");
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
        let (arn, minted) = ensure_aws_managed_key(&mut state, "cn-north-1", "aws/secretsmanager");
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
