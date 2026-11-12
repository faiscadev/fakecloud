use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_aws::arn::{partition_for, partition_of, Arn};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_core::validation::*;
use fakecloud_persistence::SnapshotStore;

use crate::state::{
    kms_alias_arn, kms_key_arn, parse_kms_arn, CustomKeyStore, KeyRotation, KmsAlias, KmsGrant,
    KmsKey, KmsSnapshot, KmsState, SharedKmsState, KMS_SNAPSHOT_SCHEMA_VERSION,
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
    /// Service Quotas, for the customer managed key quota once the user
    /// switched its enforcement on. Without one nothing is enforced.
    quota_provider: Option<Arc<dyn fakecloud_core::quota::QuotaProvider>>,
}

impl KmsService {
    pub fn new(state: SharedKmsState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
            quota_provider: None,
        }
    }

    /// Attach Service Quotas so the customer managed key quota is enforced
    /// once the user switches it on.
    pub fn with_quota_provider(
        mut self,
        provider: Option<Arc<dyn fakecloud_core::quota::QuotaProvider>>,
    ) -> Self {
        self.quota_provider = provider;
        self
    }

    /// The enforced customer managed key limit of `region`. Resolved before
    /// the KMS lock is taken.
    fn key_limit(&self, account_id: &str, region: &str) -> Option<usize> {
        crate::quota::enforced_key_limit(self.quota_provider.as_ref(), account_id, region)
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

    async fn handle(&self, mut req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // A request using another account's key is served in that account
        // (its key, its grants); the calling principal stays the caller.
        if let Some(owner) = self.cross_account_key_owner(&req) {
            req.account_id = owner;
        }
        let materialized = self.materialize_aws_managed_keys(&req);
        let mutates = materialized || is_mutating_action(req.action.as_str());
        if let Some(err) = self.aws_managed_key_refusal(&req) {
            if materialized {
                self.save_snapshot().await;
            }
            return Err(err);
        }
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
        if materialized
            || (mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()))
        {
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

    /// `ReEncrypt` is authorized as AWS authorizes it: `kms:ReEncryptFrom`
    /// on the source key and `kms:ReEncryptTo` on the destination key.
    fn iam_actions_for(&self, request: &AwsRequest) -> Vec<fakecloud_core::auth::IamAction> {
        if request.action == "ReEncrypt" {
            let [source, destination] = re_encrypt_resources(&self.state, request);
            return vec![
                fakecloud_core::auth::IamAction {
                    service: "kms",
                    action: "ReEncryptFrom",
                    resource: source,
                },
                fakecloud_core::auth::IamAction {
                    service: "kms",
                    action: "ReEncryptTo",
                    resource: destination,
                },
            ];
        }
        self.iam_action_for(request).into_iter().collect()
    }

    /// `kms:CallerAccount` (the calling principal's account) on every
    /// request, and `kms:EncryptionContext:<key>` /
    /// `kms:EncryptionContextKeys` on the operations that take an
    /// encryption context (for `ReEncrypt`, the source context on
    /// `kms:ReEncryptFrom` and the destination context on
    /// `kms:ReEncryptTo`). Key policies condition on these (the AWS-managed
    /// keys' policies all require `kms:CallerAccount`). `kms:ViaService` is
    /// never set here: a request reaching the KMS API directly did not come
    /// through another service.
    fn iam_condition_keys_for(
        &self,
        request: &AwsRequest,
        action: &fakecloud_core::auth::IamAction,
    ) -> BTreeMap<String, Vec<String>> {
        let mut out = BTreeMap::new();
        out.insert(
            "kms:calleraccount".to_string(),
            vec![request.account_id.clone()],
        );
        let body = request.json_body();
        let field = match action.action {
            "ReEncryptFrom" => "SourceEncryptionContext",
            "ReEncryptTo" => "DestinationEncryptionContext",
            _ => "EncryptionContext",
        };
        if body.get(field).is_some() {
            let context = crate::aws_managed::encryption_context_map(&body[field]);
            out.insert(
                "kms:encryptioncontextkeys".to_string(),
                context.keys().cloned().collect(),
            );
            for (k, v) in context {
                out.insert(format!("kms:encryptioncontext:{k}"), vec![v]);
            }
        }
        out
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
        Self::resolve_key_id_with_state(state, region, key_id_or_arn)
    }

    /// Resolve a key id, key ARN, alias name or alias ARN to the stored key
    /// id; see [`KmsState::resolve_key_id`].
    pub(crate) fn resolve_key_id_with_state(
        state: &crate::state::KmsState,
        region: &str,
        key_id_or_arn: &str,
    ) -> Option<String> {
        state.resolve_key_id(region, key_id_or_arn)
    }

    /// Resolve `target` (a key id or key ARN) to the stored id of the key an
    /// alias in `region` may point at. An alias and its key share a region,
    /// so a key elsewhere does not resolve; for a multi-region key the
    /// replica in `region` stands in for the primary.
    pub(crate) fn resolve_alias_target(
        state: &crate::state::KmsState,
        region: &str,
        target: &str,
    ) -> Option<String> {
        let resolved = Self::resolve_key_id_with_state(state, region, target)?;
        let key = state.keys.get(&resolved)?;
        if parse_kms_arn(&key.arn).map(|(r, _, _)| r) == Some(region) {
            return Some(resolved);
        }
        let replica = format!("{region}:{}", key.key_id);
        state.keys.contains_key(&replica).then_some(replica)
    }

    /// Create the AWS-managed key behind every `alias/aws/<service>` the
    /// request names (by name or alias ARN) that does not exist yet, as AWS
    /// does on first use: recorded, a direct `DescribeKey`, `Encrypt` or
    /// `GenerateDataKey` on a predefined alias that `ListAliases` showed
    /// without a target (`alias/aws/dynamodb`), or on an unlisted one
    /// (`alias/aws/sns`), creates the key, which then refuses the
    /// cryptographic call. A name AWS has no AWS-managed key for
    /// (`alias/aws/logs`) stays `NotFoundException`. The key-policy
    /// operations take no alias (`InvalidArnException`) and the alias
    /// operations name aliases, not keys, so neither creates one. Returns
    /// whether a key was created.
    fn materialize_aws_managed_keys(&self, req: &AwsRequest) -> bool {
        if matches!(
            req.action.as_str(),
            "GetKeyPolicy"
                | "PutKeyPolicy"
                | "ListKeyPolicies"
                | "CreateAlias"
                | "UpdateAlias"
                | "DeleteAlias"
                | "ListAliases"
                | "CreateKey"
        ) {
            return false;
        }
        let body = req.json_body();
        let mut minted = false;
        for field in ["KeyId", "DestinationKeyId", "SourceKeyId"] {
            let Some(key_ref) = body[field].as_str() else {
                continue;
            };
            let (region, account, resource) = match parse_kms_arn(key_ref) {
                Some((r, a, res)) => (r.to_string(), a.to_string(), res),
                None => (req.region.clone(), req.account_id.clone(), key_ref),
            };
            let Some(service) = resource
                .strip_prefix("alias/aws/")
                .filter(|s| crate::aws_managed::is_aws_managed_service(s))
            else {
                continue;
            };
            // AWS-managed keys belong to the caller's own account.
            if account != req.account_id {
                continue;
            }
            let alias = format!("alias/aws/{service}");
            let exists = self
                .state
                .read()
                .get(&account)
                .is_some_and(|state| state.alias(&region, &alias).is_some());
            if !exists {
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(&account);
                minted |=
                    crate::hook::ensure_aws_managed_key(state, &region, &format!("aws/{service}"))
                        .1;
            }
        }
        minted
    }

    /// The account owning the key a request names when it is another
    /// account than the caller's, for the operations AWS allows on a key of
    /// another account (recorded in the KMS developer guide, "Allowing users
    /// in other accounts to use a KMS key"): the cryptographic operations,
    /// `DescribeKey` and `GetPublicKey` naming the key by its key ARN or
    /// alias ARN, and `CreateGrant`, `GetKeyRotationStatus`, `ListGrants`,
    /// `RevokeGrant` and `RetireGrant` naming it by its key ARN. A `Decrypt`
    /// naming no key uses the key its ciphertext names, a `RetireGrant`
    /// naming its grant by `GrantToken` the account holding that grant, and a
    /// `ReEncrypt` is served in another account only when its source and
    /// destination keys are both there (with keys in different accounts it
    /// stays in the caller's, and `re_encrypt` opens the ciphertext and seals
    /// the new one each in its own key's account). A key id or alias name is always the caller's own
    /// account's. Whether the caller may use the key is decided by IAM
    /// enforcement at dispatch (key policy, identity policies and grants).
    fn cross_account_key_owner(&self, req: &AwsRequest) -> Option<String> {
        let caller = req.principal.as_ref()?.account_id.as_str();
        let alias_arn_allowed = match req.action.as_str() {
            "Encrypt"
            | "Decrypt"
            | "ReEncrypt"
            | "GenerateDataKey"
            | "GenerateDataKeyWithoutPlaintext"
            | "GenerateDataKeyPair"
            | "GenerateDataKeyPairWithoutPlaintext"
            | "Sign"
            | "Verify"
            | "GenerateMac"
            | "VerifyMac"
            | "DeriveSharedSecret"
            | "GetPublicKey"
            | "DescribeKey" => true,
            "CreateGrant"
            | "GetKeyRotationStatus"
            | "ListGrants"
            | "RevokeGrant"
            | "RetireGrant" => false,
            _ => return None,
        };
        let body = req.json_body();
        // A RetireGrant naming its grant by token is served where the grant
        // is (the account of its key).
        if req.action == "RetireGrant" {
            if let Some(token) = body["GrantToken"].as_str() {
                let accounts = self.state.read();
                let (account, _) = grant_by_token(&accounts, req, token)?;
                return (account != caller).then(|| account.to_string());
            }
        }
        let from_ciphertext = || body["CiphertextBlob"].as_str().and_then(ciphertext_key_ref);
        let key_refs: Vec<String> = match req.action.as_str() {
            "ReEncrypt" => {
                let source = body["SourceKeyId"]
                    .as_str()
                    .map(str::to_string)
                    .or_else(from_ciphertext);
                let destination = body["DestinationKeyId"].as_str().map(str::to_string);
                vec![source?, destination?]
            }
            "Decrypt" => vec![body["KeyId"]
                .as_str()
                .map(str::to_string)
                .or_else(from_ciphertext)?],
            _ => vec![body["KeyId"].as_str()?.to_string()],
        };
        let accounts = self.state.read();
        let mut owner = None;
        for key_ref in &key_refs {
            let (_, account, resource) = parse_kms_arn(key_ref)?;
            let names_key = resource.starts_with("key/")
                || (alias_arn_allowed && resource.starts_with("alias/"));
            if !names_key || account == caller || owner.is_some_and(|o| o != account) {
                return None;
            }
            accounts
                .get(account)?
                .resolve_key_id(&req.region, key_ref)?;
            owner = Some(account);
        }
        owner.map(str::to_string)
    }

    /// AWS refuses the key-management operations below on AWS-managed keys
    /// to every caller (recorded against `alias/aws/lambda`'s key): an
    /// AWS-managed key's policy grants account principals only
    /// `Describe*` / `Get*` / `List*` / `RevokeGrant` (plus cryptographic use
    /// through the owning service), and it cannot be changed, so IAM
    /// policies cannot grant more. The cryptographic operations are left to
    /// each operation: AWS allows them conditionally (through the service,
    /// or for `aws/lambda` decrypts with a function encryption context).
    fn aws_managed_key_refusal(&self, req: &AwsRequest) -> Option<AwsServiceError> {
        let field = match req.action.as_str() {
            "PutKeyPolicy"
            | "ScheduleKeyDeletion"
            | "CancelKeyDeletion"
            | "EnableKey"
            | "DisableKey"
            | "EnableKeyRotation"
            | "DisableKeyRotation"
            | "RotateKeyOnDemand"
            | "TagResource"
            | "UntagResource"
            | "UpdateKeyDescription"
            | "CreateGrant"
            | "ImportKeyMaterial"
            | "DeleteImportedKeyMaterial"
            | "ReplicateKey"
            | "UpdatePrimaryRegion" => "KeyId",
            "CreateAlias" | "UpdateAlias" => "TargetKeyId",
            _ => return None,
        };
        let body = req.json_body();
        let key_ref = body[field].as_str()?;
        // PutKeyPolicy refuses an alias as its KeyId on its own.
        if req.action == "PutKeyPolicy" && key_ref.contains("alias/") {
            return None;
        }
        let account = parse_kms_arn(key_ref)
            .map(|(_, account, _)| account.to_string())
            .filter(|a| !a.is_empty())
            .unwrap_or_else(|| req.account_id.clone());
        let key_arn = {
            let accounts = self.state.read();
            let state = accounts.get(&account)?;
            let id = Self::resolve_key_id_with_state(state, &req.region, key_ref)?;
            let key = state.keys.get(&id)?;
            if key.key_manager != "AWS" {
                return None;
            }
            key.arn.clone()
        };
        Some(Self::key_policy_denied(
            req,
            &format!("kms:{}", req.action),
            &key_arn,
        ))
    }

    /// AWS's refusal of `action` on `key_arn` when no key policy statement
    /// allows the caller (recorded: `tests/fixtures/aws-managed-key-refusals.txt`
    /// and `tests/fixtures/aws-managed-keys/direct-crypto.txt`). AWS appends a
    /// console link with a per-request authorization id, which has no meaning
    /// outside AWS and is left out.
    fn key_policy_denied(req: &AwsRequest, action: &str, key_arn: &str) -> AwsServiceError {
        let caller = req.principal.as_ref().map_or_else(
            || Arn::global_in(&req.region, "iam", &req.account_id, "root").to_string(),
            |p| p.arn.clone(),
        );
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "AccessDeniedException",
            format!(
                "User: {caller} is not authorized to perform: {action} on resource: {key_arn} because no resource-based policy allows the {action} action"
            ),
        )
    }

    /// Authorize a direct KMS API call's use of `key` for `action`
    /// (`kms:Encrypt`, `kms:ReEncryptFrom`, ...). AWS-managed keys carry
    /// AWS's own key policy (see [`crate::aws_managed`]), which grants the
    /// cryptographic operations only through the owning service
    /// (`kms:ViaService`), so a direct call is refused unless a statement
    /// without that condition allows it (the `aws/lambda` key lets account
    /// principals decrypt their functions' ciphertexts). The policy itself
    /// decides: it is evaluated with the request's `kms:CallerAccount` and
    /// encryption context, and no `kms:ViaService`, since the caller came to
    /// KMS directly. Customer keys are governed by IAM enforcement at
    /// dispatch (`--iam`), as before.
    pub(super) fn authorize_direct_key_use(
        req: &AwsRequest,
        key: &KmsKey,
        action: &str,
        encryption_context: &Value,
        extra: &[(&str, &str)],
    ) -> Result<(), AwsServiceError> {
        if key.key_manager != "AWS" {
            return Ok(());
        }
        let root;
        let principal = match req.principal.as_ref() {
            Some(p) => p,
            None => {
                root = crate::aws_managed::account_root(&req.account_id, &req.region);
                &root
            }
        };
        let context = crate::aws_managed::encryption_context_map(encryption_context);
        // The caller's own account, not the key's: an AWS-managed key's
        // policy admits only its own account's principals.
        let caller = crate::aws_managed::KeyCaller::Direct {
            principal,
            account: caller_account(req),
        };
        if crate::aws_managed::policy_allows(
            &key.policy,
            &key.arn,
            &caller,
            action,
            &context,
            extra,
        ) {
            Ok(())
        } else {
            Err(Self::key_policy_denied(req, action, &key.arn))
        }
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
        let key_limit = self.key_limit(&req.account_id, &req.region);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::quota::check_new_key(state, &req.region, key_limit)?;

        let key_id = if input.multi_region {
            format!("mrk-{}", Uuid::new_v4().as_simple())
        } else {
            Uuid::new_v4().to_string()
        };

        let arn = kms_key_arn(&req.region, &state.account_id, &key_id);
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
            .unwrap_or_else(|| default_key_policy(&state.account_id, &arn));

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

        // Check key policy for Deny rules. An unknown alias (recorded:
        // `alias/aws/logs`) is named by its ARN.
        let resolved = Self::resolve_key_id_with_state(state, &req.region, key_id_input)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    crate::hook::not_found_message(&req.account_id, &req.region, key_id_input),
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

        let resolved = Self::resolve_key_id_with_state(state, &req.region, key_id_input)
            .ok_or_else(|| {
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

        // GetKeyPolicy takes a key id or key ARN only. Recorded against
        // `alias/aws/s3` (tests/fixtures/aws-managed-keys/direct-crypto.txt).
        if key_id.starts_with("alias/") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidArnException",
                "Key Aliases are not supported for this operation.",
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

        // An AWS-managed key's policy is AWS's (see `crate::aws_managed`),
        // and the account's principals may read it (`kms:Get*`).
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
        let replica_limit = self.key_limit(&req.account_id, &replica_region);

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

        let replica_arn = kms_key_arn(&replica_region, &account_id, &source_key.key_id);

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

        // The replica is a customer managed key of its own Region.
        if !state.keys.contains_key(&replica_storage_key) {
            crate::quota::check_new_key(state, &replica_region, replica_limit)?;
        }
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
        key.arn = kms_key_arn(&primary_region, &account_id, &key.key_id);

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
