use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_aws::arn::Arn;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_core::validation::*;
use fakecloud_persistence::SnapshotStore;

use crate::state::{
    CustomKeyStore, KeyRotation, KmsAlias, KmsGrant, KmsKey, KmsSnapshot, KmsState, SharedKmsState,
    KMS_SNAPSHOT_SCHEMA_VERSION,
};

const FAKE_ENVELOPE_PREFIX: &str = "fakecloud-kms:";
const IMPORTED_ENVELOPE_PREFIX: &str = "fakecloud-imported:";
const RSA_ENVELOPE_PREFIX: &str = "fakecloud-rsa:";

/// Result of decoding a FakeCloud KMS ciphertext blob. We carry the
/// plaintext as base64 so the two callers that care (`Decrypt` returns
/// it to the client, `ReEncrypt` re-wraps it with a new key) can both
/// hand it straight to the response builder without an extra encode.
pub(crate) struct DecodedCiphertext {
    source_arn: String,
    plaintext_b64: String,
    /// EncryptionAlgorithm to echo on the Decrypt response. `SYMMETRIC_DEFAULT`
    /// for the symmetric AES envelope; the `RSAES_OAEP_*` value for an
    /// asymmetric RSA ciphertext.
    encryption_algorithm: String,
}

const VALID_KEY_SPECS: &[&str] = &[
    "ECC_NIST_P256",
    "ECC_NIST_P384",
    "ECC_NIST_P521",
    "ECC_SECG_P256K1",
    "HMAC_224",
    "HMAC_256",
    "HMAC_384",
    "HMAC_512",
    "ML_DSA_44",
    "ML_DSA_65",
    "ML_DSA_87",
    "RSA_2048",
    "RSA_3072",
    "RSA_4096",
    "SM2",
    "SYMMETRIC_DEFAULT",
];

const VALID_SIGNING_ALGORITHMS: &[&str] = &[
    "RSASSA_PKCS1_V1_5_SHA_256",
    "RSASSA_PKCS1_V1_5_SHA_384",
    "RSASSA_PKCS1_V1_5_SHA_512",
    "RSASSA_PSS_SHA_256",
    "RSASSA_PSS_SHA_384",
    "RSASSA_PSS_SHA_512",
    "ECDSA_SHA_256",
    "ECDSA_SHA_384",
    "ECDSA_SHA_512",
    "ML_DSA_SHAKE_256",
];

/// Single source of truth for supported KMS actions. Referenced by both
/// `supported_actions()` (used by the dispatch layer) and
/// `iam_action_for()` (used by the IAM enforcement layer).
static KMS_ACTIONS: &[&str] = &[
    "CreateKey",
    "DescribeKey",
    "GetKeyLastUsage",
    "ListKeys",
    "EnableKey",
    "DisableKey",
    "ScheduleKeyDeletion",
    "CancelKeyDeletion",
    "Encrypt",
    "Decrypt",
    "ReEncrypt",
    "GenerateDataKey",
    "GenerateDataKeyWithoutPlaintext",
    "GenerateRandom",
    "CreateAlias",
    "DeleteAlias",
    "UpdateAlias",
    "ListAliases",
    "TagResource",
    "UntagResource",
    "ListResourceTags",
    "UpdateKeyDescription",
    "GetKeyPolicy",
    "PutKeyPolicy",
    "ListKeyPolicies",
    "GetKeyRotationStatus",
    "EnableKeyRotation",
    "DisableKeyRotation",
    "RotateKeyOnDemand",
    "ListKeyRotations",
    "Sign",
    "Verify",
    "GetPublicKey",
    "CreateGrant",
    "ListGrants",
    "ListRetirableGrants",
    "RevokeGrant",
    "RetireGrant",
    "GenerateMac",
    "VerifyMac",
    "ReplicateKey",
    "GenerateDataKeyPair",
    "GenerateDataKeyPairWithoutPlaintext",
    "DeriveSharedSecret",
    "GetParametersForImport",
    "ImportKeyMaterial",
    "DeleteImportedKeyMaterial",
    "UpdatePrimaryRegion",
    "CreateCustomKeyStore",
    "DeleteCustomKeyStore",
    "DescribeCustomKeyStores",
    "ConnectCustomKeyStore",
    "DisconnectCustomKeyStore",
    "UpdateCustomKeyStore",
];

/// The DER halves of a generated keypair: `(private_pkcs8, public_spki)`,
/// both `None` for a symmetric or HMAC `KeySpec`.
type KeyMaterial = (Option<Vec<u8>>, Option<Vec<u8>>);

pub struct KmsService {
    state: SharedKmsState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl KmsService {
    pub fn new(state: SharedKmsState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    /// Persist current state as a snapshot. Held across the
    /// clone-serialize-write sequence to prevent stale-last writes,
    /// with serde + file I/O offloaded to the blocking pool.
    async fn save_snapshot(&self) {
        save_kms_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Build a hook that persists the current KMS state when invoked, or
    /// `None` in memory mode (no snapshot store). The CloudFormation provisioner
    /// mutates `state` directly and uses this to write a CFN-provisioned
    /// resource through to disk, the same way a direct mutating API call would.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_kms_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }
}

/// Persist the current KMS state as a snapshot. Offloads the serde +
/// blocking file write to the Tokio blocking pool. Noop when `store` is `None`
/// (memory mode). Shared by `KmsService::save_snapshot` and the CloudFormation
/// provisioner's post-provision persist hook so both route through the same
/// serialize-and-write path.
pub async fn save_kms_snapshot(
    state: &SharedKmsState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = KmsSnapshot {
        schema_version: KMS_SNAPSHOT_SCHEMA_VERSION,
        state: None,
        accounts: Some(state.read().clone()),
    };
    let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        store.save(&bytes)
    })
    .await;
    match join {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(%err, "failed to write kms snapshot"),
        Err(err) => tracing::error!(%err, "kms snapshot task panicked"),
    }
}

#[async_trait]
impl AwsService for KmsService {
    fn service_name(&self) -> &str {
        "kms"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = is_mutating_action(req.action.as_str());
        let result = match req.action.as_str() {
            "CreateKey" => self.create_key(&req),
            "DescribeKey" => self.describe_key(&req),
            "GetKeyLastUsage" => self.get_key_last_usage(&req),
            "ListKeys" => self.list_keys(&req),
            "EnableKey" => self.enable_key(&req),
            "DisableKey" => self.disable_key(&req),
            "ScheduleKeyDeletion" => self.schedule_key_deletion(&req),
            "CancelKeyDeletion" => self.cancel_key_deletion(&req),
            "Encrypt" => self.encrypt(&req),
            "Decrypt" => self.decrypt(&req),
            "ReEncrypt" => self.re_encrypt(&req),
            "GenerateDataKey" => self.generate_data_key(&req),
            "GenerateDataKeyWithoutPlaintext" => self.generate_data_key_without_plaintext(&req),
            "GenerateRandom" => self.generate_random(&req),
            "CreateAlias" => self.create_alias(&req),
            "DeleteAlias" => self.delete_alias(&req),
            "UpdateAlias" => self.update_alias(&req),
            "ListAliases" => self.list_aliases(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListResourceTags" => self.list_resource_tags(&req),
            "UpdateKeyDescription" => self.update_key_description(&req),
            "GetKeyPolicy" => self.get_key_policy(&req),
            "PutKeyPolicy" => self.put_key_policy(&req),
            "ListKeyPolicies" => self.list_key_policies(&req),
            "GetKeyRotationStatus" => self.get_key_rotation_status(&req),
            "EnableKeyRotation" => self.enable_key_rotation(&req),
            "DisableKeyRotation" => self.disable_key_rotation(&req),
            "RotateKeyOnDemand" => self.rotate_key_on_demand(&req),
            "ListKeyRotations" => self.list_key_rotations(&req),
            "Sign" => self.sign(&req),
            "Verify" => self.verify(&req),
            "GetPublicKey" => self.get_public_key(&req),
            "CreateGrant" => self.create_grant(&req),
            "ListGrants" => self.list_grants(&req),
            "ListRetirableGrants" => self.list_retirable_grants(&req),
            "RevokeGrant" => self.revoke_grant(&req),
            "RetireGrant" => self.retire_grant(&req),
            "GenerateMac" => self.generate_mac(&req),
            "VerifyMac" => self.verify_mac(&req),
            "ReplicateKey" => self.replicate_key(&req),
            "GenerateDataKeyPair" => self.generate_data_key_pair(&req),
            "GenerateDataKeyPairWithoutPlaintext" => {
                self.generate_data_key_pair_without_plaintext(&req)
            }
            "DeriveSharedSecret" => self.derive_shared_secret(&req),
            "GetParametersForImport" => self.get_parameters_for_import(&req),
            "ImportKeyMaterial" => self.import_key_material(&req),
            "DeleteImportedKeyMaterial" => self.delete_imported_key_material(&req),
            "UpdatePrimaryRegion" => self.update_primary_region(&req),
            "CreateCustomKeyStore" => self.create_custom_key_store(&req),
            "DeleteCustomKeyStore" => self.delete_custom_key_store(&req),
            "DescribeCustomKeyStores" => self.describe_custom_key_stores(&req),
            "ConnectCustomKeyStore" => self.connect_custom_key_store(&req),
            "DisconnectCustomKeyStore" => self.disconnect_custom_key_store(&req),
            "UpdateCustomKeyStore" => self.update_custom_key_store(&req),
            _ => Err(AwsServiceError::action_not_implemented("kms", &req.action)),
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        KMS_ACTIONS
    }

    fn iam_enforceable(&self) -> bool {
        true
    }

    fn iam_action_for(&self, request: &AwsRequest) -> Option<fakecloud_core::auth::IamAction> {
        let action = KMS_ACTIONS.iter().copied().find(|a| *a == request.action)?;
        let resource = kms_resource_for(action, &self.state, request);
        Some(fakecloud_core::auth::IamAction {
            service: "kms",
            action,
            resource,
        })
    }

    fn resource_tags_for(
        &self,
        resource_arn: &str,
    ) -> Option<std::collections::HashMap<String, String>> {
        if resource_arn == "*" {
            return Some(std::collections::HashMap::new());
        }
        let key_id = resource_arn.rsplit_once(":key/")?.1;
        let account_id = resource_arn.split(':').nth(4).unwrap_or("").to_string();
        let accounts = self.state.read();
        let state = accounts.get(&account_id)?;
        let key = state.keys.get(key_id)?;
        Some(
            key.tags
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        )
    }

    fn request_tags_from(
        &self,
        request: &AwsRequest,
        action: &str,
    ) -> Option<std::collections::HashMap<String, String>> {
        match action {
            "CreateKey" | "TagResource" => {
                let body = request.json_body();
                let mut tags = std::collections::HashMap::new();
                if let Some(arr) = body["Tags"].as_array() {
                    for tag in arr {
                        if let (Some(k), Some(v)) =
                            (tag["TagKey"].as_str(), tag["TagValue"].as_str())
                        {
                            tags.insert(k.to_string(), v.to_string());
                        }
                    }
                }
                Some(tags)
            }
            _ => Some(std::collections::HashMap::new()),
        }
    }
}

/// Parsed + validated inputs for `CreateKey`.
struct CreateKeyInput {
    custom_key_store_id: Option<String>,
    description: String,
    key_usage: String,
    key_spec: String,
    origin: String,
    multi_region: bool,
    policy: Option<String>,
    tags: BTreeMap<String, String>,
}

impl CreateKeyInput {
    fn from_body(body: &Value) -> Result<Self, AwsServiceError> {
        // CreateKey's Smithy contract doesn't declare ValidationException, so
        // map each constraint failure onto the closest declared error.
        recoded("CustomKeyStoreInvalidStateException", || {
            validate_optional_string_length(
                "customKeyStoreId",
                body["CustomKeyStoreId"].as_str(),
                1,
                64,
            )
        })?;
        recoded("UnsupportedOperationException", || {
            validate_optional_string_length("description", body["Description"].as_str(), 0, 8192)
        })?;
        recoded("UnsupportedOperationException", || {
            validate_optional_enum(
                "keyUsage",
                body["KeyUsage"].as_str(),
                &[
                    "SIGN_VERIFY",
                    "ENCRYPT_DECRYPT",
                    "GENERATE_VERIFY_MAC",
                    "KEY_AGREEMENT",
                ],
            )
        })?;
        recoded("UnsupportedOperationException", || {
            validate_optional_enum(
                "origin",
                body["Origin"].as_str(),
                &["AWS_KMS", "EXTERNAL", "AWS_CLOUDHSM", "EXTERNAL_KEY_STORE"],
            )
        })?;
        recoded("MalformedPolicyDocumentException", || {
            validate_optional_string_length("policy", body["Policy"].as_str(), 1, 131072)
        })?;
        recoded("XksKeyInvalidConfigurationException", || {
            validate_optional_string_length("xksKeyId", body["XksKeyId"].as_str(), 1, 64)
        })?;

        let key_spec = body["KeySpec"]
            .as_str()
            .or_else(|| body["CustomerMasterKeySpec"].as_str())
            .unwrap_or("SYMMETRIC_DEFAULT")
            .to_string();
        if !VALID_KEY_SPECS.contains(&key_spec.as_str()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "UnsupportedOperationException",
                format!(
                    "1 validation error detected: Value '{key_spec}' at 'KeySpec' failed to satisfy constraint: Member must satisfy enum value set: {}",
                    fmt_enum_set(&VALID_KEY_SPECS.iter().map(|s| s.to_string()).collect::<Vec<_>>())
                ),
            ));
        }

        let tags: BTreeMap<String, String> = body["Tags"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| {
                        let k = t["TagKey"].as_str()?;
                        let v = t["TagValue"].as_str()?;
                        Some((k.to_string(), v.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(Self {
            custom_key_store_id: body["CustomKeyStoreId"].as_str().map(|s| s.to_string()),
            description: body["Description"].as_str().unwrap_or("").to_string(),
            key_usage: body["KeyUsage"]
                .as_str()
                .unwrap_or("ENCRYPT_DECRYPT")
                .to_string(),
            key_spec,
            origin: body["Origin"].as_str().unwrap_or("AWS_KMS").to_string(),
            multi_region: body["MultiRegion"].as_bool().unwrap_or(false),
            policy: body["Policy"].as_str().map(|s| s.to_string()),
            tags,
        })
    }
}

impl KmsService {
    fn resolve_key_id_for(
        &self,
        account_id: &str,
        region: &str,
        key_id_or_arn: &str,
    ) -> Option<String> {
        let accounts = self.state.read();
        let empty = KmsState::new(account_id, region);
        let state = accounts.get(account_id).unwrap_or(&empty);
        Self::resolve_key_id_with_state(state, key_id_or_arn)
    }

    pub(crate) fn resolve_key_id_with_state(
        state: &crate::state::KmsState,
        key_id_or_arn: &str,
    ) -> Option<String> {
        // Direct key ID
        if state.keys.contains_key(key_id_or_arn) {
            return Some(key_id_or_arn.to_string());
        }

        // ARN for key
        if key_id_or_arn.starts_with("arn:aws:kms:") {
            // Could be key ARN or alias ARN
            if key_id_or_arn.contains(":key/") {
                if let Some(id) = key_id_or_arn.rsplit('/').next() {
                    // Multi-region replicas are stored under a region-scoped
                    // composite key ("{region}:{id}") because the primary and
                    // every replica share the same bare id. Resolve the ARN's
                    // own region first so a DescribeKey on a replica ARN returns
                    // the replica entry, not the primary that also matches `id`.
                    let region = key_id_or_arn.split(':').nth(3).unwrap_or("");
                    let scoped = format!("{region}:{id}");
                    if state.keys.contains_key(&scoped) {
                        return Some(scoped);
                    }
                    if state.keys.contains_key(id) {
                        return Some(id.to_string());
                    }
                }
            }
            // alias ARN: arn:aws:kms:region:account:alias/name
            if key_id_or_arn.contains(":alias/") {
                if let Some(alias_part) = key_id_or_arn.split(':').next_back() {
                    if let Some(alias) = state.aliases.get(alias_part) {
                        return Some(alias.target_key_id.clone());
                    }
                }
            }
        }

        // Alias name
        if key_id_or_arn.starts_with("alias/") {
            if let Some(alias) = state.aliases.get(key_id_or_arn) {
                return Some(alias.target_key_id.clone());
            }
        }

        None
    }

    fn require_key_id(body: &Value) -> Result<String, AwsServiceError> {
        body["KeyId"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "KeyId is required",
                )
            })
    }

    fn resolve_required_key(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<String, AwsServiceError> {
        let key_id_input = Self::require_key_id(body)?;
        self.resolve_key_id_for(&req.account_id, &req.region, &key_id_input)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{key_id_input}' does not exist"),
                )
            })
    }

    /// Generate `(private_pkcs8_der, public_spki_der)` for an asymmetric
    /// `KeySpec`, or `(None, None)` for symmetric / HMAC specs.
    ///
    /// RSA generation is CPU-bound and would otherwise stall the tokio worker
    /// that is driving the request, so on a multi-thread runtime it runs under
    /// `block_in_place`, which hands the worker's other tasks off to a sibling
    /// thread for the duration. `block_in_place` panics on a current-thread
    /// runtime (and there is no runtime at all under `cargo test`), so those
    /// cases call straight through.
    fn generate_asymmetric_material(key_spec: &str) -> Result<KeyMaterial, AwsServiceError> {
        fn generate(key_spec: &str) -> Result<KeyMaterial, AwsServiceError> {
            if let Some((p, k)) = asym::generate_keypair(key_spec).map_err(|e| {
                AwsServiceError::aws_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "KMSInternalException",
                    format!("failed to generate asymmetric key: {e}"),
                )
            })? {
                return Ok((Some(p), Some(k)));
            }
            if let Some((p, k)) = asym_ecdsa::generate_keypair(key_spec).map_err(|e| {
                AwsServiceError::aws_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "KMSInternalException",
                    format!("failed to generate ecdsa key: {e}"),
                )
            })? {
                return Ok((Some(p), Some(k)));
            }
            Ok((None, None))
        }

        let multi_thread = matches!(
            tokio::runtime::Handle::try_current().map(|h| h.runtime_flavor()),
            Ok(tokio::runtime::RuntimeFlavor::MultiThread)
        );
        if multi_thread {
            tokio::task::block_in_place(|| generate(key_spec))
        } else {
            generate(key_spec)
        }
    }

    fn create_key(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let input = CreateKeyInput::from_body(&req.json_body())?;
        // Reject incompatible KeySpec/KeyUsage combinations (e.g. an HMAC spec
        // with ENCRYPT_DECRYPT) before minting a key no operation can use.
        validate_key_spec_usage(&input.key_spec, &input.key_usage)?;

        // Mint the key material *before* taking the state lock. RSA generation
        // is CPU-bound (seconds, and far worse for RSA-4096 in a debug build);
        // doing it under `state.write()` stalled every other KMS request —
        // including symmetric CreateKey and Encrypt — behind one RSA key, and
        // blocked the tokio worker on top of that. It depends only on
        // `key_spec`, so it does not need the lock at all.
        let (asym_priv, asym_pub) = Self::generate_asymmetric_material(&input.key_spec)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let key_id = if input.multi_region {
            format!("mrk-{}", Uuid::new_v4().as_simple())
        } else {
            Uuid::new_v4().to_string()
        };

        let arn = format!(
            "arn:aws:kms:{}:{}:key/{}",
            req.region, state.account_id, key_id
        );
        let now = Utc::now().timestamp() as f64;

        let signing_algs = if input.key_usage == "SIGN_VERIFY" {
            signing_algorithms_for_key_spec(&input.key_spec)
        } else {
            None
        };
        let encryption_algs = encryption_algorithms_for_key(&input.key_usage, &input.key_spec);
        let mac_algs = if input.key_usage == "GENERATE_VERIFY_MAC" {
            mac_algorithms_for_key_spec(&input.key_spec)
        } else {
            None
        };

        let key_policy = input
            .policy
            .unwrap_or_else(|| default_key_policy(&state.account_id));

        // Refuse asymmetric specs we cannot really generate keys for
        // rather than store a no-DER key that would later fall through
        // to a fake-bytes Sign/Verify path. SM2 currently has no
        // pure-Rust impl wired in.
        let is_asymmetric = input.key_spec.starts_with("ECC_")
            || input.key_spec.starts_with("RSA_")
            || input.key_spec == "SM2";
        if is_asymmetric && asym_priv.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "UnsupportedOperationException",
                format!(
                    "KeySpec '{}' is not supported by this fakecloud build; \
                     no fake-signature fallback is provided",
                    input.key_spec
                ),
            ));
        }

        // A key created with `Origin=EXTERNAL` has no key material yet, so AWS
        // returns it disabled in the `PendingImport` state until
        // ImportKeyMaterial runs. fakecloud previously created every key
        // Enabled, which the Terraform `aws_kms_external_key` resource caught
        // as `enabled = true` where it expected `false`.
        let pending_import = input.origin == "EXTERNAL";
        let key = KmsKey {
            key_id: key_id.clone(),
            arn: arn.clone(),
            creation_date: now,
            description: input.description,
            enabled: !pending_import,
            key_usage: input.key_usage,
            key_spec: input.key_spec,
            key_manager: "CUSTOMER".to_string(),
            key_state: if pending_import {
                "PendingImport".to_string()
            } else {
                "Enabled".to_string()
            },
            deletion_date: None,
            tags: input.tags,
            policy: key_policy,
            key_rotation_enabled: false,
            rotation_period_in_days: None,
            origin: input.origin,
            multi_region: input.multi_region,
            rotations: Vec::new(),
            signing_algorithms: signing_algs,
            encryption_algorithms: encryption_algs,
            mac_algorithms: mac_algs,
            custom_key_store_id: input.custom_key_store_id,
            imported_key_material: false,
            imported_material_bytes: None,
            private_key_seed: rand_bytes(32),
            primary_region: None,
            asymmetric_private_key_der: asym_priv,
            asymmetric_public_key_der: asym_pub,
        };

        let metadata = key_metadata_json(&key, &state.account_id);
        state.keys.insert(key_id, key);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "KeyMetadata": metadata })).unwrap(),
        ))
    }

    fn describe_key(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id_input = body["KeyId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "KeyId is required",
            )
        })?;

        let accounts = self.state.read();
        let empty = KmsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        // Check key policy for Deny rules
        let resolved = Self::resolve_key_id_with_state(state, key_id_input).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id_input}' does not exist"),
            )
        })?;

        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id_input}' does not exist"),
            )
        })?;

        // Check policy for Deny on DescribeKey
        check_policy_deny(key, "kms:DescribeKey")?;

        let metadata = key_metadata_json(key, &state.account_id);
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "KeyMetadata": metadata })).unwrap(),
        ))
    }

    fn get_key_last_usage(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id_input = body["KeyId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "KeyId is required",
            )
        })?;

        let accounts = self.state.read();
        let empty = KmsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        let resolved = Self::resolve_key_id_with_state(state, key_id_input).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id_input}' does not exist"),
            )
        })?;
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id_input}' does not exist"),
            )
        })?;

        // KMS started tracking on the key's creation date. We don't yet
        // record per-op timestamps, so KeyLastUsage is omitted — the AWS
        // spec explicitly allows this when no tracked op has run.
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "KeyId": key.key_id,
                "KeyCreationDate": key.creation_date,
                "TrackingStartDate": key.creation_date,
            }))
            .unwrap(),
        ))
    }

    fn list_keys(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        // ListKeys only declares InvalidMarkerException / KMSInternal /
        // DependencyTimeout; map both Limit and Marker shape failures onto
        // InvalidMarkerException so the probe sees a declared error.
        recoded("InvalidMarkerException", || {
            validate_optional_json_range("limit", &body["Limit"], 1, 1000)
        })?;
        recoded("InvalidMarkerException", || {
            validate_optional_string_length("marker", body["Marker"].as_str(), 1, 320)
        })?;

        let limit = body["Limit"].as_i64().unwrap_or(1000) as usize;
        let marker = body["Marker"].as_str();

        let accounts = self.state.read();
        let empty = KmsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let all_keys: Vec<Value> = state
            .keys
            .values()
            .map(|k| {
                json!({
                    "KeyId": k.key_id,
                    "KeyArn": k.arn,
                })
            })
            .collect();

        // Resume at the first key whose KeyId is strictly greater than the
        // marker. all_keys is in KeyId order (BTreeMap-backed) and the marker is
        // the last KeyId of the previous page, so if that key was deleted between
        // pages we still advance to the next one instead of falling back to 0 and
        // restarting the listing (which an exact-match lookup did).
        let start = match marker {
            Some(m) => all_keys
                .iter()
                .position(|k| k["KeyId"].as_str() > Some(m))
                .unwrap_or(all_keys.len()),
            None => 0,
        };

        let page = &all_keys[start..all_keys.len().min(start + limit)];
        let truncated = start + limit < all_keys.len();

        let mut result = json!({
            "Keys": page,
            "Truncated": truncated,
        });

        if truncated {
            if let Some(last) = page.last() {
                result["NextMarker"] = last["KeyId"].clone();
            }
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&result).unwrap(),
        ))
    }

    fn enable_key(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resolved = self.resolve_required_key(req, &body)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let key = state.keys.get_mut(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;
        // EnableKey is only valid from Enabled/Disabled. A key that is
        // PendingDeletion / PendingImport / Unavailable must not be silently
        // resurrected to Enabled — AWS returns KMSInvalidStateException (you
        // must CancelKeyDeletion / import material first).
        require_enable_disable_transition(key)?;
        key.enabled = true;
        key.key_state = "Enabled".to_string();

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn disable_key(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resolved = self.resolve_required_key(req, &body)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let key = state.keys.get_mut(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;
        require_enable_disable_transition(key)?;
        key.enabled = false;
        key.key_state = "Disabled".to_string();

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn schedule_key_deletion(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resolved = self.resolve_required_key(req, &body)?;
        let pending_days = body["PendingWindowInDays"].as_i64().unwrap_or(30);

        // AWS enforces a 7..=30 day waiting period; anything outside that
        // range is a ValidationException. Previously any value (including 0
        // or negative) was accepted verbatim and echoed back.
        if !(7..=30).contains(&pending_days) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "1 validation error detected: Value '{pending_days}' at 'pendingWindowInDays' failed to satisfy constraint: Member must have value less than or equal to 30 and greater than or equal to 7"
                ),
            ));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let key = state.keys.get_mut(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;
        let deletion_date =
            Utc::now().timestamp() as f64 + (pending_days as f64 * 24.0 * 60.0 * 60.0);
        key.key_state = "PendingDeletion".to_string();
        key.enabled = false;
        key.deletion_date = Some(deletion_date);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "KeyId": key.key_id,
                "DeletionDate": deletion_date,
                "KeyState": "PendingDeletion",
                "PendingWindowInDays": pending_days,
            }))
            .unwrap(),
        ))
    }

    fn cancel_key_deletion(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resolved = self.resolve_required_key(req, &body)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let key = state.keys.get_mut(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;
        key.key_state = "Disabled".to_string();
        key.deletion_date = None;

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "KeyId": key.key_id,
            }))
            .unwrap(),
        ))
    }

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;

        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, &key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Invalid keyId {key_id}"),
                )
            })?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let key = state.keys.get_mut(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;

        fakecloud_core::tags::apply_tags(&mut key.tags, &body, "Tags", "TagKey", "TagValue")
            .map_err(|f| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!("{f} must be a list"),
                )
            })?;

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;

        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, &key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Invalid keyId {key_id}"),
                )
            })?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let key = state.keys.get_mut(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;

        fakecloud_core::tags::remove_tags(&mut key.tags, &body, "TagKeys").map_err(|f| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!("{f} must be a list"),
            )
        })?;

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_resource_tags(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;

        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, &key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Invalid keyId {key_id}"),
                )
            })?;

        let accounts = self.state.read();
        let empty = KmsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;
        let tags = fakecloud_core::tags::tags_to_json(&key.tags, "TagKey", "TagValue");

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "Tags": tags,
                "Truncated": false,
            }))
            .unwrap(),
        ))
    }

    fn update_key_description(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resolved = self.resolve_required_key(req, &body)?;
        let description = body["Description"].as_str().unwrap_or("").to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let key = state.keys.get_mut(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;
        key.description = description;

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn get_key_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;

        // For key policy operations, aliases should not work
        if key_id.starts_with("alias/") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Invalid keyId {key_id}"),
            ));
        }

        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, &key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{key_id}' does not exist"),
                )
            })?;

        let accounts = self.state.read();
        let empty = KmsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "Policy": key.policy,
            }))
            .unwrap(),
        ))
    }

    fn put_key_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;

        // For key policy operations, aliases should not work
        if key_id.starts_with("alias/") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Invalid keyId {key_id}"),
            ));
        }

        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, &key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{key_id}' does not exist"),
                )
            })?;

        let policy = body["Policy"].as_str().unwrap_or("").to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let key = state.keys.get_mut(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;
        key.policy = policy;

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_key_policies(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let _resolved = self.resolve_required_key(req, &body)?;

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "PolicyNames": ["default"],
                "Truncated": false,
            }))
            .unwrap(),
        ))
    }

    fn get_key_rotation_status(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;

        // Real KMS resolves alias/* and alias-ARNs identically to a key id.
        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, &key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{key_id}' does not exist"),
                )
            })?;

        let accounts = self.state.read();
        let empty = KmsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;

        let mut result = json!({
            "KeyRotationEnabled": key.key_rotation_enabled,
        });
        // AWS echoes the configured cadence (default 365) only while rotation
        // is enabled.
        if key.key_rotation_enabled {
            result["RotationPeriodInDays"] = json!(key.rotation_period_in_days.unwrap_or(365));
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&result).unwrap(),
        ))
    }

    fn enable_key_rotation(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;

        // Real KMS resolves alias/* and alias-ARNs identically to a key id
        // here. Earlier code rejected `alias/*` outright, breaking IaC
        // configs that reference keys by alias.
        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, &key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{key_id}' does not exist"),
                )
            })?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let key = state.keys.get_mut(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;
        // Rotation cannot be toggled on a key pending deletion / import;
        // AWS rejects with KMSInvalidStateException (Disabled -> DisabledException).
        require_usable_key_state(key)?;
        // RotationPeriodInDays is optional; AWS validates 90..=2560 and
        // defaults to 365. Persist it so GetKeyRotationStatus echoes it back
        // instead of dropping it (bug-audit 2026-06-20, 1.24).
        if let Some(period) = body.get("RotationPeriodInDays").and_then(|v| v.as_i64()) {
            if !(90..=2560).contains(&period) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!("RotationPeriodInDays must be between 90 and 2560, got {period}"),
                ));
            }
            key.rotation_period_in_days = Some(period as i32);
        }
        key.key_rotation_enabled = true;

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn disable_key_rotation(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;

        // Real KMS resolves alias/* and alias-ARNs identically to a key id.
        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, &key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{key_id}' does not exist"),
                )
            })?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let key = state.keys.get_mut(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;
        require_usable_key_state(key)?;
        key.key_rotation_enabled = false;

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn rotate_key_on_demand(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resolved = self.resolve_required_key(req, &body)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let key = state.keys.get_mut(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;
        require_usable_key_state(key)?;

        let rotation = KeyRotation {
            key_id: key.key_id.clone(),
            rotation_date: Utc::now().timestamp() as f64,
            rotation_type: "ON_DEMAND".to_string(),
        };
        key.rotations.push(rotation);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "KeyId": key.key_id,
            }))
            .unwrap(),
        ))
    }

    fn list_key_rotations(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resolved = self.resolve_required_key(req, &body)?;
        validate_optional_json_range("limit", &body["Limit"], 1, 1000)?;
        let limit = body["Limit"].as_i64().unwrap_or(1000) as usize;
        let marker = body["Marker"].as_str();

        let accounts = self.state.read();
        let empty = KmsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;

        let start_index = if let Some(marker) = marker {
            marker.parse::<usize>().unwrap_or(0)
        } else {
            0
        };

        let rotations: Vec<Value> = key
            .rotations
            .iter()
            .skip(start_index)
            .take(limit)
            .map(|r| {
                json!({
                    "KeyId": r.key_id,
                    "RotationDate": r.rotation_date,
                    "RotationType": r.rotation_type,
                })
            })
            .collect();

        let total_after_start = key.rotations.len().saturating_sub(start_index);
        let truncated = total_after_start > limit;

        let mut response = json!({
            "Rotations": rotations,
            "Truncated": truncated,
        });

        if truncated {
            response["NextMarker"] = json!((start_index + limit).to_string());
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&response).unwrap(),
        ))
    }

    fn replicate_key(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;
        let replica_region = body["ReplicaRegion"].as_str().unwrap_or("").to_string();

        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, &key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{key_id}' does not exist"),
                )
            })?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Clone the source key once and drop the borrow — the replica reuses
        // every field except the region-dependent ones.
        let source_key = state
            .keys
            .get(&resolved)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "KMSInternalException",
                    "Key state became inconsistent",
                )
            })?
            .clone();
        let account_id = state.account_id.clone();
        // The primary key's region is whatever region its ARN was minted in
        // (the request region at CreateKey time), not the server default.
        let source_region = source_key
            .arn
            .split(':')
            .nth(3)
            .unwrap_or(&state.region)
            .to_string();

        let replica_arn = format!(
            "arn:aws:kms:{}:{}:key/{}",
            replica_region, account_id, source_key.key_id
        );

        let metadata = json!({
            "KeyId": source_key.key_id,
            "Arn": replica_arn,
            "AWSAccountId": account_id,
            "CreationDate": source_key.creation_date,
            "Description": source_key.description,
            "Enabled": source_key.enabled,
            "KeyUsage": source_key.key_usage,
            "KeySpec": source_key.key_spec,
            "CustomerMasterKeySpec": source_key.key_spec,
            "KeyManager": source_key.key_manager,
            "KeyState": source_key.key_state,
            "Origin": source_key.origin,
            "MultiRegion": true,
            "MultiRegionConfiguration": {
                "MultiRegionKeyType": "REPLICA",
                "PrimaryKey": {
                    "Arn": source_key.arn,
                    "Region": source_region,
                },
                "ReplicaKeys": [],
            },
        });

        let replica_storage_key = format!("{}:{}", replica_region, source_key.key_id);
        let source_policy = source_key.policy.clone();
        let replica_key = KmsKey {
            arn: replica_arn,
            deletion_date: None,
            key_rotation_enabled: false,
            rotation_period_in_days: None,
            multi_region: true,
            rotations: Vec::new(),
            custom_key_store_id: None,
            imported_key_material: false,
            imported_material_bytes: None,
            private_key_seed: rand_bytes(32),
            // Record which region holds the primary so a later DescribeKey on
            // this replica renders MultiRegionKeyType=REPLICA (and the correct
            // primary region/ARN). Leaving this None made the stored replica
            // look like a PRIMARY on read-back.
            primary_region: Some(source_region.clone()),
            ..source_key
        };

        state.keys.insert(replica_storage_key, replica_key);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "ReplicaKeyMetadata": metadata,
                "ReplicaPolicy": source_policy,
            }))
            .unwrap(),
        ))
    }

    fn update_primary_region(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;
        let primary_region = body["PrimaryRegion"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "PrimaryRegion is required",
                )
            })?
            .to_string();

        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, &key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{key_id}' does not exist"),
                )
            })?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let account_id = state.account_id.clone();
        let key = state.keys.get_mut(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        if !key.multi_region {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "UnsupportedOperationException",
                format!("Key '{}' is not a multi-Region key", key.arn),
            ));
        }
        key.primary_region = Some(primary_region.clone());
        // Update the ARN to reflect the new region
        key.arn = format!(
            "arn:aws:kms:{}:{}:key/{}",
            primary_region, account_id, key.key_id
        );

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }
}

#[path = "asym.rs"]
pub(crate) mod asym;
#[path = "asym_ecdsa.rs"]
pub(crate) mod asym_ecdsa;
#[path = "mac.rs"]
pub(crate) mod mac;
#[path = "service_aliases.rs"]
mod service_aliases;
#[path = "service_crypto.rs"]
mod service_crypto;
#[path = "service_custom_store.rs"]
mod service_custom_store;
#[path = "service_grants.rs"]
mod service_grants;

#[path = "helpers.rs"]
mod helpers;
pub(crate) use helpers::*;

#[path = "provisioner.rs"]
pub mod provisioner;

#[cfg(test)]
#[path = "service_tests.rs"]
mod tests;
