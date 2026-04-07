use std::collections::HashMap;

use async_trait::async_trait;
use base64::Engine;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_aws::arn::Arn;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{CustomKeyStore, KeyRotation, KmsAlias, KmsGrant, KmsKey, SharedKmsState};

const FAKE_ENVELOPE_PREFIX: &str = "fakecloud-kms:";

const VALID_KEY_SPECS: &[&str] = &[
    "ECC_NIST_P256",
    "ECC_NIST_P384",
    "ECC_NIST_P521",
    "ECC_SECG_P256K1",
    "HMAC_224",
    "HMAC_256",
    "HMAC_384",
    "HMAC_512",
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
];

pub struct KmsService {
    state: SharedKmsState,
}

impl KmsService {
    pub fn new(state: SharedKmsState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl AwsService for KmsService {
    fn service_name(&self) -> &str {
        "kms"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match req.action.as_str() {
            "CreateKey" => self.create_key(&req),
            "DescribeKey" => self.describe_key(&req),
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
        }
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            "CreateKey",
            "DescribeKey",
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
        ]
    }
}

fn default_key_policy(account_id: &str) -> String {
    serde_json::to_string(&json!({
        "Version": "2012-10-17",
        "Id": "key-default-1",
        "Statement": [
            {
                "Sid": "Enable IAM User Permissions",
                "Effect": "Allow",
                "Principal": {"AWS": Arn::global("iam", account_id, "root").to_string()},
                "Action": "kms:*",
                "Resource": "*",
            }
        ],
    }))
    .unwrap()
}

fn signing_algorithms_for_key_spec(key_spec: &str) -> Option<Vec<String>> {
    match key_spec {
        "RSA_2048" | "RSA_3072" | "RSA_4096" => Some(vec![
            "RSASSA_PKCS1_V1_5_SHA_256".into(),
            "RSASSA_PKCS1_V1_5_SHA_384".into(),
            "RSASSA_PKCS1_V1_5_SHA_512".into(),
            "RSASSA_PSS_SHA_256".into(),
            "RSASSA_PSS_SHA_384".into(),
            "RSASSA_PSS_SHA_512".into(),
        ]),
        "ECC_NIST_P256" | "ECC_SECG_P256K1" => Some(vec!["ECDSA_SHA_256".into()]),
        "ECC_NIST_P384" => Some(vec!["ECDSA_SHA_384".into()]),
        "ECC_NIST_P521" => Some(vec!["ECDSA_SHA_512".into()]),
        _ => None,
    }
}

fn encryption_algorithms_for_key(key_usage: &str, key_spec: &str) -> Option<Vec<String>> {
    if key_usage == "ENCRYPT_DECRYPT" {
        match key_spec {
            "SYMMETRIC_DEFAULT" => Some(vec!["SYMMETRIC_DEFAULT".into()]),
            "RSA_2048" | "RSA_3072" | "RSA_4096" => {
                Some(vec!["RSAES_OAEP_SHA_1".into(), "RSAES_OAEP_SHA_256".into()])
            }
            _ => None,
        }
    } else {
        None
    }
}

fn mac_algorithms_for_key_spec(key_spec: &str) -> Option<Vec<String>> {
    match key_spec {
        "HMAC_224" => Some(vec!["HMAC_SHA_224".into()]),
        "HMAC_256" => Some(vec!["HMAC_SHA_256".into()]),
        "HMAC_384" => Some(vec!["HMAC_SHA_384".into()]),
        "HMAC_512" => Some(vec!["HMAC_SHA_512".into()]),
        _ => None,
    }
}

fn rand_bytes(n: usize) -> Vec<u8> {
    (0..n)
        .map(|_| {
            let u = Uuid::new_v4();
            u.as_bytes()[0]
        })
        .collect()
}

impl KmsService {
    fn resolve_key_id(&self, key_id_or_arn: &str) -> Option<String> {
        let state = self.state.read();
        Self::resolve_key_id_with_state(&state, key_id_or_arn)
    }

    fn resolve_key_id_with_state(
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

    fn resolve_required_key(&self, body: &Value) -> Result<String, AwsServiceError> {
        let key_id_input = Self::require_key_id(body)?;
        self.resolve_key_id(&key_id_input).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id_input}' does not exist"),
            )
        })
    }

    fn create_key(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        validate_optional_string_length(
            "customKeyStoreId",
            body["CustomKeyStoreId"].as_str(),
            1,
            64,
        )?;

        validate_optional_string_length("description", body["Description"].as_str(), 0, 8192)?;
        validate_optional_enum(
            "keyUsage",
            body["KeyUsage"].as_str(),
            &[
                "SIGN_VERIFY",
                "ENCRYPT_DECRYPT",
                "GENERATE_VERIFY_MAC",
                "KEY_AGREEMENT",
            ],
        )?;
        validate_optional_enum(
            "origin",
            body["Origin"].as_str(),
            &["AWS_KMS", "EXTERNAL", "AWS_CLOUDHSM", "EXTERNAL_KEY_STORE"],
        )?;
        validate_optional_string_length("policy", body["Policy"].as_str(), 1, 131072)?;
        validate_optional_string_length("xksKeyId", body["XksKeyId"].as_str(), 1, 64)?;

        let custom_key_store_id = body["CustomKeyStoreId"].as_str().map(|s| s.to_string());
        let description = body["Description"].as_str().unwrap_or("").to_string();
        let key_usage = body["KeyUsage"]
            .as_str()
            .unwrap_or("ENCRYPT_DECRYPT")
            .to_string();
        let key_spec = body["KeySpec"]
            .as_str()
            .or_else(|| body["CustomerMasterKeySpec"].as_str())
            .unwrap_or("SYMMETRIC_DEFAULT")
            .to_string();
        let origin = body["Origin"].as_str().unwrap_or("AWS_KMS").to_string();
        let multi_region = body["MultiRegion"].as_bool().unwrap_or(false);
        let policy = body["Policy"].as_str().map(|s| s.to_string());

        // Validate key spec
        if !VALID_KEY_SPECS.contains(&key_spec.as_str()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "1 validation error detected: Value '{}' at 'KeySpec' failed to satisfy constraint: Member must satisfy enum value set: {}",
                    key_spec, fmt_enum_set(&VALID_KEY_SPECS.iter().map(|s| s.to_string()).collect::<Vec<_>>())
                ),
            ));
        }

        let mut state = self.state.write();

        let key_id = if multi_region {
            format!("mrk-{}", Uuid::new_v4().as_simple())
        } else {
            Uuid::new_v4().to_string()
        };

        let arn = format!(
            "arn:aws:kms:{}:{}:key/{}",
            state.region, state.account_id, key_id
        );
        let now = Utc::now().timestamp() as f64;

        let tags: HashMap<String, String> = body["Tags"]
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

        let signing_algs = if key_usage == "SIGN_VERIFY" {
            signing_algorithms_for_key_spec(&key_spec)
        } else {
            None
        };

        let encryption_algs = encryption_algorithms_for_key(&key_usage, &key_spec);

        let mac_algs = if key_usage == "GENERATE_VERIFY_MAC" {
            mac_algorithms_for_key_spec(&key_spec)
        } else {
            None
        };

        let default_policy = default_key_policy(&state.account_id);
        let key_policy = policy.unwrap_or(default_policy);

        let key = KmsKey {
            key_id: key_id.clone(),
            arn: arn.clone(),
            creation_date: now,
            description,
            enabled: true,
            key_usage,
            key_spec,
            key_manager: "CUSTOMER".to_string(),
            key_state: "Enabled".to_string(),
            deletion_date: None,
            tags,
            policy: key_policy,
            key_rotation_enabled: false,
            origin,
            multi_region,
            rotations: Vec::new(),
            signing_algorithms: signing_algs,
            encryption_algorithms: encryption_algs,
            mac_algorithms: mac_algs,
            custom_key_store_id,
            imported_key_material: false,
            imported_material_bytes: None,
            private_key_seed: rand_bytes(32),
            primary_region: None,
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

        let state = self.state.read();

        // Check key policy for Deny rules
        let resolved = Self::resolve_key_id_with_state(&state, key_id_input).ok_or_else(|| {
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

    fn list_keys(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        validate_optional_json_range("limit", &body["Limit"], 1, 1000)?;
        validate_optional_string_length("marker", body["Marker"].as_str(), 1, 320)?;

        let limit = body["Limit"].as_i64().unwrap_or(1000) as usize;
        let marker = body["Marker"].as_str();

        let state = self.state.read();
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

        let start = if let Some(m) = marker {
            all_keys
                .iter()
                .position(|k| k["KeyId"].as_str() == Some(m))
                .map(|pos| pos + 1)
                .unwrap_or(0)
        } else {
            0
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
        let resolved = self.resolve_required_key(&body)?;

        let mut state = self.state.write();
        let key = state.keys.get_mut(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;
        key.enabled = true;
        key.key_state = "Enabled".to_string();

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn disable_key(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resolved = self.resolve_required_key(&body)?;

        let mut state = self.state.write();
        let key = state.keys.get_mut(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;
        key.enabled = false;
        key.key_state = "Disabled".to_string();

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn schedule_key_deletion(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resolved = self.resolve_required_key(&body)?;
        let pending_days = body["PendingWindowInDays"].as_i64().unwrap_or(30);

        let mut state = self.state.write();
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
        let resolved = self.resolve_required_key(&body)?;

        let mut state = self.state.write();
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

    fn encrypt(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;
        let plaintext_b64 = body["Plaintext"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Plaintext is required",
            )
        })?;

        // Decode the plaintext to check length
        let plaintext_bytes = base64::engine::general_purpose::STANDARD
            .decode(plaintext_b64)
            .unwrap_or_default();

        if plaintext_bytes.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "1 validation error detected: Value at 'plaintext' failed to satisfy constraint: Member must have length greater than or equal to 1",
            ));
        }

        if plaintext_bytes.len() > 4096 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "1 validation error detected: Value at 'plaintext' failed to satisfy constraint: Member must have length less than or equal to 4096",
            ));
        }

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let state = self.state.read();
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;
        if !key.enabled {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DisabledException",
                format!("Key '{}' is disabled", key.arn),
            ));
        }

        // When imported key material is present, XOR plaintext with imported material
        // to produce a deterministic ciphertext that depends on the imported key.
        let envelope = if let Some(ref material) = key.imported_material_bytes {
            let xored: Vec<u8> = plaintext_bytes
                .iter()
                .enumerate()
                .map(|(i, b)| b ^ material[i % material.len()])
                .collect();
            let xored_b64 = base64::engine::general_purpose::STANDARD.encode(&xored);
            format!("fakecloud-imported:{}:{xored_b64}", key.key_id)
        } else {
            // Fake encryption: prefix + key_id + ":" + base64(plaintext_bytes)
            format!("{FAKE_ENVELOPE_PREFIX}{}:{plaintext_b64}", key.key_id)
        };
        let ciphertext_b64 = base64::engine::general_purpose::STANDARD.encode(envelope.as_bytes());

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "CiphertextBlob": ciphertext_b64,
                "KeyId": key.arn,
                "EncryptionAlgorithm": "SYMMETRIC_DEFAULT",
            }))
            .unwrap(),
        ))
    }

    fn decrypt(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let ciphertext_b64 = body["CiphertextBlob"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "CiphertextBlob is required",
            )
        })?;

        let ciphertext_bytes = base64::engine::general_purpose::STANDARD
            .decode(ciphertext_b64)
            .map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidCiphertextException",
                    "The ciphertext is invalid",
                )
            })?;

        let envelope = String::from_utf8(ciphertext_bytes).map_err(|_| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidCiphertextException",
                "The ciphertext is invalid",
            )
        })?;

        const IMPORTED_PREFIX: &str = "fakecloud-imported:";

        if let Some(rest) = envelope.strip_prefix(IMPORTED_PREFIX) {
            // Imported key material envelope: XOR to recover plaintext
            let (key_id, xored_b64) = rest.split_once(':').ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidCiphertextException",
                    "The ciphertext is invalid",
                )
            })?;

            let xored_bytes = base64::engine::general_purpose::STANDARD
                .decode(xored_b64)
                .map_err(|_| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidCiphertextException",
                        "The ciphertext is invalid",
                    )
                })?;

            let state = self.state.read();
            let key = state.keys.get(key_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{key_id}' does not exist"),
                )
            })?;

            let material = key.imported_material_bytes.as_ref().ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidCiphertextException",
                    "Key material has been deleted",
                )
            })?;

            let plaintext_bytes: Vec<u8> = xored_bytes
                .iter()
                .enumerate()
                .map(|(i, b)| b ^ material[i % material.len()])
                .collect();
            let plaintext_b64 = base64::engine::general_purpose::STANDARD.encode(&plaintext_bytes);

            Ok(AwsResponse::json(
                StatusCode::OK,
                serde_json::to_string(&json!({
                    "Plaintext": plaintext_b64,
                    "KeyId": key.arn,
                    "EncryptionAlgorithm": "SYMMETRIC_DEFAULT",
                }))
                .unwrap(),
            ))
        } else if let Some(rest) = envelope.strip_prefix(FAKE_ENVELOPE_PREFIX) {
            let (key_id, plaintext_b64) = rest.split_once(':').ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidCiphertextException",
                    "The ciphertext is invalid",
                )
            })?;

            let state = self.state.read();
            let key = state.keys.get(key_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{key_id}' does not exist"),
                )
            })?;

            Ok(AwsResponse::json(
                StatusCode::OK,
                serde_json::to_string(&json!({
                    "Plaintext": plaintext_b64,
                    "KeyId": key.arn,
                    "EncryptionAlgorithm": "SYMMETRIC_DEFAULT",
                }))
                .unwrap(),
            ))
        } else {
            Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidCiphertextException",
                "The ciphertext is not a valid FakeCloud KMS ciphertext",
            ))
        }
    }

    fn re_encrypt(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let ciphertext_b64 = body["CiphertextBlob"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "CiphertextBlob is required",
            )
        })?;
        let dest_key_id = body["DestinationKeyId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "DestinationKeyId is required",
            )
        })?;

        // Decrypt
        let ciphertext_bytes = base64::engine::general_purpose::STANDARD
            .decode(ciphertext_b64)
            .map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidCiphertextException",
                    "The ciphertext is invalid",
                )
            })?;

        let envelope = String::from_utf8(ciphertext_bytes).map_err(|_| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidCiphertextException",
                "The ciphertext is invalid",
            )
        })?;

        const IMPORTED_PREFIX: &str = "fakecloud-imported:";

        // Extract source key ID and plaintext from either envelope format
        let (source_key_id, plaintext_b64) =
            if let Some(rest) = envelope.strip_prefix(IMPORTED_PREFIX) {
                let (kid, xored_b64) = rest.split_once(':').ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidCiphertextException",
                        "The ciphertext is invalid",
                    )
                })?;
                let xored_bytes = base64::engine::general_purpose::STANDARD
                    .decode(xored_b64)
                    .map_err(|_| {
                        AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "InvalidCiphertextException",
                            "The ciphertext is invalid",
                        )
                    })?;
                let state = self.state.read();
                let key = state.keys.get(kid).ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "NotFoundException",
                        format!("Key '{kid}' does not exist"),
                    )
                })?;
                let material = key.imported_material_bytes.as_ref().ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidCiphertextException",
                        "Key material has been deleted",
                    )
                })?;
                let plaintext_bytes: Vec<u8> = xored_bytes
                    .iter()
                    .enumerate()
                    .map(|(i, b)| b ^ material[i % material.len()])
                    .collect();
                (
                    kid.to_string(),
                    base64::engine::general_purpose::STANDARD.encode(&plaintext_bytes),
                )
            } else if let Some(rest) = envelope.strip_prefix(FAKE_ENVELOPE_PREFIX) {
                let (kid, pt) = rest.split_once(':').ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidCiphertextException",
                        "The ciphertext is invalid",
                    )
                })?;
                (kid.to_string(), pt.to_string())
            } else {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidCiphertextException",
                    "The ciphertext is invalid",
                ));
            };

        let state = self.state.read();

        let source_key = state.keys.get(source_key_id.as_str()).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{source_key_id}' does not exist"),
            )
        })?;
        let source_arn = source_key.arn.clone();

        // Resolve destination
        let dest_resolved =
            Self::resolve_key_id_with_state(&state, dest_key_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{dest_key_id}' does not exist"),
                )
            })?;

        let dest_key = state.keys.get(&dest_resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;

        // Re-encrypt with destination key
        let new_envelope = if let Some(ref material) = dest_key.imported_material_bytes {
            let plaintext_bytes = base64::engine::general_purpose::STANDARD
                .decode(&plaintext_b64)
                .unwrap_or_default();
            let xored: Vec<u8> = plaintext_bytes
                .iter()
                .enumerate()
                .map(|(i, b)| b ^ material[i % material.len()])
                .collect();
            let xored_b64 = base64::engine::general_purpose::STANDARD.encode(&xored);
            format!("fakecloud-imported:{}:{xored_b64}", dest_key.key_id)
        } else {
            format!("{FAKE_ENVELOPE_PREFIX}{}:{plaintext_b64}", dest_key.key_id)
        };
        let new_ciphertext_b64 =
            base64::engine::general_purpose::STANDARD.encode(new_envelope.as_bytes());

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "CiphertextBlob": new_ciphertext_b64,
                "KeyId": dest_key.arn,
                "SourceKeyId": source_arn,
                "SourceEncryptionAlgorithm": "SYMMETRIC_DEFAULT",
                "DestinationEncryptionAlgorithm": "SYMMETRIC_DEFAULT",
            }))
            .unwrap(),
        ))
    }

    fn generate_data_key(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let state = self.state.read();
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;
        if !key.enabled {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DisabledException",
                format!("Key '{}' is disabled", key.arn),
            ));
        }

        let num_bytes = data_key_size_from_body(&body)?;

        let data_key_bytes: Vec<u8> = rand_bytes(num_bytes);
        let plaintext_b64 = base64::engine::general_purpose::STANDARD.encode(&data_key_bytes);

        // Encrypt the data key
        let envelope = format!("{FAKE_ENVELOPE_PREFIX}{}:{plaintext_b64}", key.key_id);
        let ciphertext_b64 = base64::engine::general_purpose::STANDARD.encode(envelope.as_bytes());

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "Plaintext": plaintext_b64,
                "CiphertextBlob": ciphertext_b64,
                "KeyId": key.arn,
            }))
            .unwrap(),
        ))
    }

    fn generate_data_key_without_plaintext(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let state = self.state.read();
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;
        if !key.enabled {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DisabledException",
                format!("Key '{}' is disabled", key.arn),
            ));
        }

        let num_bytes = data_key_size_from_body(&body)?;
        let data_key_bytes: Vec<u8> = rand_bytes(num_bytes);
        let plaintext_b64 = base64::engine::general_purpose::STANDARD.encode(&data_key_bytes);
        let envelope = format!("{FAKE_ENVELOPE_PREFIX}{}:{plaintext_b64}", key.key_id);
        let ciphertext_b64 = base64::engine::general_purpose::STANDARD.encode(envelope.as_bytes());

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "CiphertextBlob": ciphertext_b64,
                "KeyId": key.arn,
            }))
            .unwrap(),
        ))
    }

    fn generate_random(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        // CustomKeyStoreId is accepted for API compatibility but has no effect on
        // random number generation in this emulator.
        validate_optional_string_length(
            "customKeyStoreId",
            body["CustomKeyStoreId"].as_str(),
            1,
            64,
        )?;

        let num_bytes = body["NumberOfBytes"].as_u64().unwrap_or(32) as usize;

        validate_range_i64("numberOfBytes", num_bytes as i64, 1, 1024)?;

        let random_bytes = rand_bytes(num_bytes);
        let b64 = base64::engine::general_purpose::STANDARD.encode(&random_bytes);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "Plaintext": b64,
            }))
            .unwrap(),
        ))
    }

    fn create_alias(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let alias_name = body["AliasName"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "AliasName is required",
                )
            })?
            .to_string();
        let target_key_id = body["TargetKeyId"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "TargetKeyId is required",
                )
            })?
            .to_string();

        // Validate prefix
        if !alias_name.starts_with("alias/") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Invalid identifier",
            ));
        }

        // Check for reserved aliases
        if alias_name.starts_with("alias/aws/") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "",
            ));
        }

        // Check for restricted characters
        let alias_suffix = &alias_name["alias/".len()..];
        if alias_suffix.contains(':') {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!("{alias_name} contains invalid characters for an alias"),
            ));
        }

        // Check regex pattern
        let valid_chars = alias_name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '/' || c == '_' || c == '-' || c == ':');
        if !valid_chars {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "1 validation error detected: Value '{}' at 'aliasName' failed to satisfy constraint: Member must satisfy regular expression pattern: ^[a-zA-Z0-9:/_-]+$",
                    alias_name
                ),
            ));
        }

        // Check if target is an alias
        if target_key_id.starts_with("alias/") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Aliases must refer to keys. Not aliases",
            ));
        }

        let resolved = self.resolve_key_id(&target_key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{target_key_id}' does not exist"),
            )
        })?;

        let mut state = self.state.write();

        if state.aliases.contains_key(&alias_name) {
            let alias_arn = format!(
                "arn:aws:kms:{}:{}:{}",
                state.region, state.account_id, alias_name
            );
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AlreadyExistsException",
                format!("An alias with the name {alias_arn} already exists"),
            ));
        }

        let alias_arn = format!(
            "arn:aws:kms:{}:{}:{}",
            state.region, state.account_id, alias_name
        );

        state.aliases.insert(
            alias_name.clone(),
            KmsAlias {
                alias_name,
                alias_arn,
                target_key_id: resolved,
                creation_date: Utc::now().timestamp() as f64,
            },
        );

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn delete_alias(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let alias_name = body["AliasName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "AliasName is required",
            )
        })?;

        if !alias_name.starts_with("alias/") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Invalid identifier",
            ));
        }

        let mut state = self.state.write();
        if state.aliases.remove(alias_name).is_none() {
            let alias_arn = format!(
                "arn:aws:kms:{}:{}:{}",
                state.region, state.account_id, alias_name
            );
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Alias {alias_arn} is not found."),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn update_alias(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let alias_name = body["AliasName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "AliasName is required",
            )
        })?;
        let target_key_id = body["TargetKeyId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "TargetKeyId is required",
            )
        })?;

        let resolved = self.resolve_key_id(target_key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{target_key_id}' does not exist"),
            )
        })?;

        let mut state = self.state.write();
        let alias = state.aliases.get_mut(alias_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Alias '{alias_name}' does not exist"),
            )
        })?;

        alias.target_key_id = resolved;

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_aliases(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        validate_optional_json_range("limit", &body["Limit"], 1, 100)?;
        validate_optional_string_length("marker", body["Marker"].as_str(), 1, 320)?;

        if !body["KeyId"].is_null() && !body["KeyId"].is_string() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "KeyId must be a string",
            ));
        }
        validate_optional_string_length("keyId", body["KeyId"].as_str(), 1, 2048)?;

        let key_id_filter = body["KeyId"].as_str();

        let state = self.state.read();

        // Resolve key_id_filter to actual key ID if needed
        let resolved_filter =
            key_id_filter.and_then(|kid| Self::resolve_key_id_with_state(&state, kid));

        let aliases: Vec<Value> = state
            .aliases
            .values()
            .filter(|a| match (&resolved_filter, key_id_filter) {
                (Some(r), _) => a.target_key_id == *r,
                (None, Some(_)) => false,
                (None, None) => true,
            })
            .map(|a| {
                json!({
                    "AliasName": a.alias_name,
                    "AliasArn": a.alias_arn,
                    "TargetKeyId": a.target_key_id,
                })
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "Aliases": aliases,
                "Truncated": false,
            }))
            .unwrap(),
        ))
    }

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Invalid keyId {key_id}"),
            )
        })?;

        let mut state = self.state.write();
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

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Invalid keyId {key_id}"),
            )
        })?;

        let mut state = self.state.write();
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

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Invalid keyId {key_id}"),
            )
        })?;

        let state = self.state.read();
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
        let resolved = self.resolve_required_key(&body)?;
        let description = body["Description"].as_str().unwrap_or("").to_string();

        let mut state = self.state.write();
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

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let state = self.state.read();
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

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let policy = body["Policy"].as_str().unwrap_or("").to_string();

        let mut state = self.state.write();
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
        let _resolved = self.resolve_required_key(&body)?;

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

        // Aliases should fail for rotation operations
        if key_id.starts_with("alias/") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Invalid keyId {key_id}"),
            ));
        }

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let state = self.state.read();
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
                "KeyRotationEnabled": key.key_rotation_enabled,
            }))
            .unwrap(),
        ))
    }

    fn enable_key_rotation(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;

        if key_id.starts_with("alias/") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Invalid keyId {key_id}"),
            ));
        }

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let mut state = self.state.write();
        let key = state.keys.get_mut(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;
        key.key_rotation_enabled = true;

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn disable_key_rotation(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;

        if key_id.starts_with("alias/") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Invalid keyId {key_id}"),
            ));
        }

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let mut state = self.state.write();
        let key = state.keys.get_mut(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;
        key.key_rotation_enabled = false;

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn rotate_key_on_demand(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resolved = self.resolve_required_key(&body)?;

        let mut state = self.state.write();
        let key = state.keys.get_mut(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;

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
        let resolved = self.resolve_required_key(&body)?;
        validate_optional_json_range("limit", &body["Limit"], 1, 1000)?;
        let limit = body["Limit"].as_i64().unwrap_or(1000) as usize;
        let marker = body["Marker"].as_str();

        let state = self.state.read();
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

    fn sign(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;
        let message_b64 = body["Message"].as_str().unwrap_or("");
        let signing_algorithm = body["SigningAlgorithm"].as_str().unwrap_or("");

        // Validate message
        let message_bytes = base64::engine::general_purpose::STANDARD
            .decode(message_b64)
            .unwrap_or_default();

        if message_bytes.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "1 validation error detected: Value at 'Message' failed to satisfy constraint: Member must have length greater than or equal to 1",
            ));
        }

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let state = self.state.read();
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;

        // Validate key usage
        if key.key_usage != "SIGN_VERIFY" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "1 validation error detected: Value '{}' at 'KeyId' failed to satisfy constraint: Member must point to a key with usage: 'SIGN_VERIFY'",
                    resolved
                ),
            ));
        }

        // Validate signing algorithm against key's supported algorithms
        let valid_algs = key.signing_algorithms.as_deref().unwrap_or(&[]);
        if !valid_algs.iter().any(|a| a == signing_algorithm) {
            let set: Vec<String> = if valid_algs.is_empty() {
                VALID_SIGNING_ALGORITHMS
                    .iter()
                    .map(|s| s.to_string())
                    .collect()
            } else {
                valid_algs.to_vec()
            };
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "1 validation error detected: Value '{}' at 'SigningAlgorithm' failed to satisfy constraint: Member must satisfy enum value set: {}",
                    signing_algorithm, fmt_enum_set(&set)
                ),
            ));
        }

        // Generate a fake signature
        let sig_data = format!(
            "fakecloud-sig:{}:{}:{}",
            key.key_id, signing_algorithm, message_b64
        );
        let signature_b64 = base64::engine::general_purpose::STANDARD.encode(sig_data.as_bytes());

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "Signature": signature_b64,
                "SigningAlgorithm": signing_algorithm,
                "KeyId": key.arn,
            }))
            .unwrap(),
        ))
    }

    fn verify(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;
        let message_b64 = body["Message"].as_str().unwrap_or("");
        let signature_b64 = body["Signature"].as_str().unwrap_or("");
        let signing_algorithm = body["SigningAlgorithm"].as_str().unwrap_or("");

        // Validate message
        let message_bytes = base64::engine::general_purpose::STANDARD
            .decode(message_b64)
            .unwrap_or_default();

        if message_bytes.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "1 validation error detected: Value at 'Message' failed to satisfy constraint: Member must have length greater than or equal to 1",
            ));
        }

        // Validate signature
        let sig_bytes = base64::engine::general_purpose::STANDARD
            .decode(signature_b64)
            .unwrap_or_default();
        if sig_bytes.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "1 validation error detected: Value at 'Signature' failed to satisfy constraint: Member must have length greater than or equal to 1",
            ));
        }

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let state = self.state.read();
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;

        // Validate key usage
        if key.key_usage != "SIGN_VERIFY" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "1 validation error detected: Value '{}' at 'KeyId' failed to satisfy constraint: Member must point to a key with usage: 'SIGN_VERIFY'",
                    resolved
                ),
            ));
        }

        // Validate signing algorithm
        let valid_algs = key.signing_algorithms.as_deref().unwrap_or(&[]);
        if !valid_algs.iter().any(|a| a == signing_algorithm) {
            let set: Vec<String> = if valid_algs.is_empty() {
                VALID_SIGNING_ALGORITHMS
                    .iter()
                    .map(|s| s.to_string())
                    .collect()
            } else {
                valid_algs.to_vec()
            };
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "1 validation error detected: Value '{}' at 'SigningAlgorithm' failed to satisfy constraint: Member must satisfy enum value set: {}",
                    signing_algorithm, fmt_enum_set(&set)
                ),
            ));
        }

        // Check if signature matches
        let expected_sig_data = format!(
            "fakecloud-sig:{}:{}:{}",
            key.key_id, signing_algorithm, message_b64
        );
        let expected_signature_b64 =
            base64::engine::general_purpose::STANDARD.encode(expected_sig_data.as_bytes());

        let signature_valid = signature_b64 == expected_signature_b64;

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "SignatureValid": signature_valid,
                "SigningAlgorithm": signing_algorithm,
                "KeyId": key.arn,
            }))
            .unwrap(),
        ))
    }

    fn get_public_key(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let state = self.state.read();
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;

        // Generate a fake DER-encoded public key
        let fake_public_key = generate_fake_public_key(&key.key_spec);
        let public_key_b64 = base64::engine::general_purpose::STANDARD.encode(&fake_public_key);

        let mut response = json!({
            "KeyId": key.arn,
            "KeySpec": key.key_spec,
            "KeyUsage": key.key_usage,
            "PublicKey": public_key_b64,
            "CustomerMasterKeySpec": key.key_spec,
        });

        if let Some(ref signing_algs) = key.signing_algorithms {
            response["SigningAlgorithms"] = json!(signing_algs);
        }
        if let Some(ref enc_algs) = key.encryption_algorithms {
            response["EncryptionAlgorithms"] = json!(enc_algs);
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&response).unwrap(),
        ))
    }

    fn create_grant(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let grantee_principal = body["GranteePrincipal"].as_str().unwrap_or("").to_string();
        let retiring_principal = body["RetiringPrincipal"].as_str().map(|s| s.to_string());
        let operations: Vec<String> = body["Operations"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let constraints = if body["Constraints"].is_null() {
            None
        } else {
            Some(body["Constraints"].clone())
        };
        let name = body["Name"].as_str().map(|s| s.to_string());

        let grant_id = Uuid::new_v4().to_string();
        let grant_token = Uuid::new_v4().to_string();

        let mut state = self.state.write();
        state.grants.push(KmsGrant {
            grant_id: grant_id.clone(),
            grant_token: grant_token.clone(),
            key_id: resolved,
            grantee_principal,
            retiring_principal,
            operations,
            constraints,
            name,
            creation_date: Utc::now().timestamp() as f64,
        });

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "GrantId": grant_id,
                "GrantToken": grant_token,
            }))
            .unwrap(),
        ))
    }

    fn list_grants(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let grant_id_filter = body["GrantId"].as_str();

        let state = self.state.read();
        let grants: Vec<Value> = state
            .grants
            .iter()
            .filter(|g| g.key_id == resolved)
            .filter(|g| {
                if let Some(gid) = grant_id_filter {
                    g.grant_id == gid
                } else {
                    true
                }
            })
            .map(grant_to_json)
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "Grants": grants,
                "Truncated": false,
            }))
            .unwrap(),
        ))
    }

    fn list_retirable_grants(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        validate_required("RetiringPrincipal", &body["RetiringPrincipal"])?;
        let retiring_principal = body["RetiringPrincipal"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "RetiringPrincipal must be a string",
            )
        })?;
        validate_string_length("retiringPrincipal", retiring_principal, 1, 256)?;
        validate_optional_json_range("limit", &body["Limit"], 1, 1000)?;
        validate_optional_string_length("marker", body["Marker"].as_str(), 1, 320)?;

        let limit = body["Limit"].as_i64().unwrap_or(1000) as usize;
        let marker = body["Marker"].as_str();

        let state = self.state.read();
        let all_grants: Vec<Value> = state
            .grants
            .iter()
            .filter(|g| {
                g.retiring_principal
                    .as_deref()
                    .is_some_and(|rp| rp == retiring_principal)
            })
            .map(grant_to_json)
            .collect();

        let start = if let Some(m) = marker {
            all_grants
                .iter()
                .position(|g| g["GrantId"].as_str() == Some(m))
                .map(|pos| pos + 1)
                .unwrap_or(0)
        } else {
            0
        };

        let page = &all_grants[start..all_grants.len().min(start + limit)];
        let truncated = start + limit < all_grants.len();

        let mut result = json!({
            "Grants": page,
            "Truncated": truncated,
        });

        if truncated {
            if let Some(last) = page.last() {
                result["NextMarker"] = last["GrantId"].clone();
            }
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&result).unwrap(),
        ))
    }

    fn revoke_grant(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;
        let grant_id = body["GrantId"].as_str().unwrap_or("");

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let mut state = self.state.write();
        let idx = state
            .grants
            .iter()
            .position(|g| g.key_id == resolved && g.grant_id == grant_id);

        match idx {
            Some(i) => {
                state.grants.remove(i);
                Ok(AwsResponse::json(StatusCode::OK, "{}"))
            }
            None => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Grant ID {grant_id} not found"),
            )),
        }
    }

    fn retire_grant(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let grant_token = body["GrantToken"].as_str();
        let grant_id = body["GrantId"].as_str();
        let key_id = body["KeyId"].as_str();

        let mut state = self.state.write();

        let idx = if let Some(token) = grant_token {
            state.grants.iter().position(|g| g.grant_token == token)
        } else if let (Some(kid), Some(gid)) = (key_id, grant_id) {
            let resolved = Self::resolve_key_id_with_state(&state, kid);
            resolved.and_then(|r| {
                state
                    .grants
                    .iter()
                    .position(|g| g.key_id == r && g.grant_id == gid)
            })
        } else {
            None
        };

        match idx {
            Some(i) => {
                state.grants.remove(i);
                Ok(AwsResponse::json(StatusCode::OK, "{}"))
            }
            None => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                "Grant not found",
            )),
        }
    }

    fn generate_mac(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;
        let mac_algorithm = body["MacAlgorithm"].as_str().unwrap_or("").to_string();
        let message_b64 = body["Message"].as_str().unwrap_or("");

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let state = self.state.read();
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;

        // Validate key usage
        if key.key_usage != "GENERATE_VERIFY_MAC" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidKeyUsageException",
                format!("Key '{}' is not a GENERATE_VERIFY_MAC key", key.arn),
            ));
        }

        // Validate key spec supports MAC
        if key.mac_algorithms.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidKeyUsageException",
                format!("Key '{}' does not support MAC operations", key.arn),
            ));
        }

        // Generate fake MAC
        let mac_data = format!(
            "fakecloud-mac:{}:{}:{}",
            key.key_id, mac_algorithm, message_b64
        );
        let mac_b64 = base64::engine::general_purpose::STANDARD.encode(mac_data.as_bytes());

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "Mac": mac_b64,
                "KeyId": key.key_id,
                "MacAlgorithm": mac_algorithm,
            }))
            .unwrap(),
        ))
    }

    fn verify_mac(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;
        let mac_algorithm = body["MacAlgorithm"].as_str().unwrap_or("").to_string();
        let message_b64 = body["Message"].as_str().unwrap_or("");
        let mac_b64 = body["Mac"].as_str().unwrap_or("");

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let state = self.state.read();
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;

        // Validate key usage
        if key.key_usage != "GENERATE_VERIFY_MAC" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidKeyUsageException",
                format!("Key '{}' is not a GENERATE_VERIFY_MAC key", key.arn),
            ));
        }

        // Check if MAC matches
        let expected_mac_data = format!(
            "fakecloud-mac:{}:{}:{}",
            key.key_id, mac_algorithm, message_b64
        );
        let expected_mac_b64 =
            base64::engine::general_purpose::STANDARD.encode(expected_mac_data.as_bytes());

        let mac_valid = mac_b64 == expected_mac_b64;

        if !mac_valid {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "KMSInvalidMacException",
                "MAC verification failed",
            ));
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "KeyId": key.key_id,
                "MacAlgorithm": mac_algorithm,
                "MacValid": true,
            }))
            .unwrap(),
        ))
    }

    fn replicate_key(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;
        let replica_region = body["ReplicaRegion"].as_str().unwrap_or("").to_string();

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let mut state = self.state.write();

        // Clone all needed data from source key first to avoid borrow issues
        let source_key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;
        let source_key_id = source_key.key_id.clone();
        let source_arn = source_key.arn.clone();
        let source_creation_date = source_key.creation_date;
        let source_description = source_key.description.clone();
        let source_enabled = source_key.enabled;
        let source_key_usage = source_key.key_usage.clone();
        let source_key_spec = source_key.key_spec.clone();
        let source_key_manager = source_key.key_manager.clone();
        let source_key_state = source_key.key_state.clone();
        let source_origin = source_key.origin.clone();
        let source_tags = source_key.tags.clone();
        let source_policy = source_key.policy.clone();
        let source_signing_algorithms = source_key.signing_algorithms.clone();
        let source_encryption_algorithms = source_key.encryption_algorithms.clone();
        let source_mac_algorithms = source_key.mac_algorithms.clone();
        let account_id = state.account_id.clone();
        let source_region = state.region.clone();

        let replica_arn = format!(
            "arn:aws:kms:{}:{}:key/{}",
            replica_region, account_id, source_key_id
        );

        let metadata = json!({
            "KeyId": source_key_id,
            "Arn": replica_arn,
            "AWSAccountId": account_id,
            "CreationDate": source_creation_date,
            "Description": source_description,
            "Enabled": source_enabled,
            "KeyUsage": source_key_usage,
            "KeySpec": source_key_spec,
            "CustomerMasterKeySpec": source_key_spec,
            "KeyManager": source_key_manager,
            "KeyState": source_key_state,
            "Origin": source_origin,
            "MultiRegion": true,
            "MultiRegionConfiguration": {
                "MultiRegionKeyType": "REPLICA",
                "PrimaryKey": {
                    "Arn": source_arn,
                    "Region": source_region,
                },
                "ReplicaKeys": [],
            },
        });

        let replica_key = KmsKey {
            key_id: source_key_id.clone(),
            arn: replica_arn,
            creation_date: source_creation_date,
            description: source_description,
            enabled: source_enabled,
            key_usage: source_key_usage,
            key_spec: source_key_spec,
            key_manager: source_key_manager,
            key_state: source_key_state,
            deletion_date: None,
            tags: source_tags,
            policy: source_policy.clone(),
            key_rotation_enabled: false,
            origin: source_origin,
            multi_region: true,
            rotations: Vec::new(),
            signing_algorithms: source_signing_algorithms,
            encryption_algorithms: source_encryption_algorithms,
            mac_algorithms: source_mac_algorithms,
            custom_key_store_id: None,
            imported_key_material: false,
            imported_material_bytes: None,
            private_key_seed: rand_bytes(32),
            primary_region: None,
        };

        let replica_storage_key = format!("{}:{}", replica_region, source_key_id);
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

    fn generate_data_key_pair(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;
        let key_pair_spec = body["KeyPairSpec"]
            .as_str()
            .unwrap_or("RSA_2048")
            .to_string();

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let state = self.state.read();
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;
        if !key.enabled {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DisabledException",
                format!("Key '{}' is disabled", key.arn),
            ));
        }

        let public_key_bytes = generate_fake_public_key(&key_pair_spec);
        let private_key_bytes = rand_bytes(256);
        let public_key_b64 = base64::engine::general_purpose::STANDARD.encode(&public_key_bytes);
        let private_plaintext_b64 =
            base64::engine::general_purpose::STANDARD.encode(&private_key_bytes);

        // Encrypt private key
        let envelope = format!(
            "{FAKE_ENVELOPE_PREFIX}{}:{private_plaintext_b64}",
            key.key_id
        );
        let private_ciphertext_b64 =
            base64::engine::general_purpose::STANDARD.encode(envelope.as_bytes());

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "KeyId": key.arn,
                "KeyPairSpec": key_pair_spec,
                "PublicKey": public_key_b64,
                "PrivateKeyPlaintext": private_plaintext_b64,
                "PrivateKeyCiphertextBlob": private_ciphertext_b64,
            }))
            .unwrap(),
        ))
    }

    fn generate_data_key_pair_without_plaintext(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;
        let key_pair_spec = body["KeyPairSpec"]
            .as_str()
            .unwrap_or("RSA_2048")
            .to_string();

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let state = self.state.read();
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;
        if !key.enabled {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DisabledException",
                format!("Key '{}' is disabled", key.arn),
            ));
        }

        let public_key_bytes = generate_fake_public_key(&key_pair_spec);
        let private_key_bytes = rand_bytes(256);
        let public_key_b64 = base64::engine::general_purpose::STANDARD.encode(&public_key_bytes);
        let private_plaintext_b64 =
            base64::engine::general_purpose::STANDARD.encode(&private_key_bytes);

        let envelope = format!(
            "{FAKE_ENVELOPE_PREFIX}{}:{private_plaintext_b64}",
            key.key_id
        );
        let private_ciphertext_b64 =
            base64::engine::general_purpose::STANDARD.encode(envelope.as_bytes());

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "KeyId": key.arn,
                "KeyPairSpec": key_pair_spec,
                "PublicKey": public_key_b64,
                "PrivateKeyCiphertextBlob": private_ciphertext_b64,
            }))
            .unwrap(),
        ))
    }

    fn derive_shared_secret(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;
        let _key_agreement_algorithm = body["KeyAgreementAlgorithm"]
            .as_str()
            .unwrap_or("ECDH")
            .to_string();
        let _public_key = body["PublicKey"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "PublicKey is required",
            )
        })?;

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let state = self.state.read();
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;

        if !key.enabled {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DisabledException",
                format!("Key '{}' is disabled", key.arn),
            ));
        }

        // Key must be asymmetric (KEY_AGREEMENT usage)
        if key.key_usage != "KEY_AGREEMENT" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidKeyUsageException",
                format!(
                    "Key '{}' usage is '{}', not KEY_AGREEMENT",
                    key.arn, key.key_usage
                ),
            ));
        }

        // Deterministic shared secret: SHA-256(private_key_seed || public_key_bytes)
        // Both parties using the correct keys will derive the same result.
        let public_key_bytes = base64::engine::general_purpose::STANDARD
            .decode(_public_key)
            .unwrap_or_default();

        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(&key.private_key_seed);
        hasher.update(&public_key_bytes);
        let shared_secret_bytes = hasher.finalize();
        let shared_secret_b64 =
            base64::engine::general_purpose::STANDARD.encode(shared_secret_bytes);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "KeyId": key.arn,
                "SharedSecret": shared_secret_b64,
                "KeyAgreementAlgorithm": "ECDH",
                "KeyOrigin": key.origin,
            }))
            .unwrap(),
        ))
    }

    fn get_parameters_for_import(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let state = self.state.read();
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "KMSInternalException",
                "Key state became inconsistent",
            )
        })?;

        if key.origin != "EXTERNAL" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "UnsupportedOperationException",
                format!("Key '{}' origin is '{}', not EXTERNAL", key.arn, key.origin),
            ));
        }

        let import_token_bytes = rand_bytes(64);
        let import_token_b64 =
            base64::engine::general_purpose::STANDARD.encode(&import_token_bytes);
        let public_key_bytes = generate_fake_public_key("RSA_2048");
        let public_key_b64 = base64::engine::general_purpose::STANDARD.encode(&public_key_bytes);

        // Valid for 24 hours
        let parameters_valid_to = Utc::now().timestamp() as f64 + 86400.0;

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "KeyId": key.arn,
                "ImportToken": import_token_b64,
                "PublicKey": public_key_b64,
                "ParametersValidTo": parameters_valid_to,
            }))
            .unwrap(),
        ))
    }

    fn import_key_material(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;

        let _import_token = body["ImportToken"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "ImportToken is required",
            )
        })?;

        let encrypted_key_material = body["EncryptedKeyMaterial"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "EncryptedKeyMaterial is required",
            )
        })?;

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let mut state = self.state.write();
        let key = state.keys.get_mut(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        if key.origin != "EXTERNAL" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "UnsupportedOperationException",
                format!("Key '{}' origin is '{}', not EXTERNAL", key.arn, key.origin),
            ));
        }

        // Store the imported material bytes for use in encrypt/decrypt.
        // In real AWS, the material is unwrapped with the import RSA key.
        // Here we treat the EncryptedKeyMaterial as the raw key (base64-decoded).
        let material_bytes = base64::engine::general_purpose::STANDARD
            .decode(encrypted_key_material)
            .map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "EncryptedKeyMaterial is not valid base64",
                )
            })?;
        if material_bytes.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "EncryptedKeyMaterial must not be empty",
            ));
        }
        key.imported_key_material = true;
        key.imported_material_bytes = Some(material_bytes);
        key.enabled = true;
        key.key_state = "Enabled".to_string();

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn delete_imported_key_material(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let mut state = self.state.write();
        let key = state.keys.get_mut(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        if key.origin != "EXTERNAL" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "UnsupportedOperationException",
                format!("Key '{}' origin is '{}', not EXTERNAL", key.arn, key.origin),
            ));
        }

        key.imported_key_material = false;
        key.imported_material_bytes = None;
        key.enabled = false;
        key.key_state = "PendingImport".to_string();

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
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

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let mut state = self.state.write();
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

    fn create_custom_key_store(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let name = body["CustomKeyStoreName"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "CustomKeyStoreName is required",
                )
            })?
            .to_string();

        validate_string_length("customKeyStoreName", &name, 1, 256)?;

        let store_type = body["CustomKeyStoreType"]
            .as_str()
            .unwrap_or("AWS_CLOUDHSM")
            .to_string();

        validate_optional_enum(
            "customKeyStoreType",
            Some(store_type.as_str()),
            &["AWS_CLOUDHSM", "EXTERNAL_KEY_STORE"],
        )?;

        let mut state = self.state.write();

        // Name must be unique
        if state
            .custom_key_stores
            .values()
            .any(|s| s.custom_key_store_name == name)
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "CustomKeyStoreNameInUseException",
                format!("Custom key store name '{name}' is already in use"),
            ));
        }

        let store_id = format!("cks-{}", Uuid::new_v4().as_simple());
        let now = Utc::now().timestamp() as f64;

        let store = CustomKeyStore {
            custom_key_store_id: store_id.clone(),
            custom_key_store_name: name,
            custom_key_store_type: store_type,
            cloud_hsm_cluster_id: body["CloudHsmClusterId"].as_str().map(|s| s.to_string()),
            trust_anchor_certificate: body["TrustAnchorCertificate"]
                .as_str()
                .map(|s| s.to_string()),
            connection_state: "DISCONNECTED".to_string(),
            creation_date: now,
            xks_proxy_uri_endpoint: body["XksProxyUriEndpoint"].as_str().map(|s| s.to_string()),
            xks_proxy_uri_path: body["XksProxyUriPath"].as_str().map(|s| s.to_string()),
            xks_proxy_vpc_endpoint_service_name: body["XksProxyVpcEndpointServiceName"]
                .as_str()
                .map(|s| s.to_string()),
            xks_proxy_connectivity: body["XksProxyConnectivity"].as_str().map(|s| s.to_string()),
        };

        state.custom_key_stores.insert(store_id.clone(), store);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "CustomKeyStoreId": store_id })).unwrap(),
        ))
    }

    fn delete_custom_key_store(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let store_id = body["CustomKeyStoreId"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "CustomKeyStoreId is required",
                )
            })?
            .to_string();

        let mut state = self.state.write();

        let store = state.custom_key_stores.get(&store_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "CustomKeyStoreNotFoundException",
                format!("Custom key store '{store_id}' does not exist"),
            )
        })?;

        if store.connection_state == "CONNECTED" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "CustomKeyStoreHasCMKsException",
                "Cannot delete a connected custom key store. Disconnect it first.",
            ));
        }

        state.custom_key_stores.remove(&store_id);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn describe_custom_key_stores(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length(
            "customKeyStoreName",
            body["CustomKeyStoreName"].as_str(),
            1,
            256,
        )?;
        validate_optional_json_range("limit", &body["Limit"], 1, 1000)?;
        validate_optional_string_length("marker", body["Marker"].as_str(), 1, 1024)?;

        let filter_id = body["CustomKeyStoreId"].as_str();
        let filter_name = body["CustomKeyStoreName"].as_str();
        let limit = body["Limit"].as_i64().unwrap_or(1000) as usize;
        let marker = body["Marker"].as_str();

        let state = self.state.read();

        let mut stores: Vec<&CustomKeyStore> = state
            .custom_key_stores
            .values()
            .filter(|s| {
                if let Some(id) = filter_id {
                    return s.custom_key_store_id == id;
                }
                if let Some(name) = filter_name {
                    return s.custom_key_store_name == name;
                }
                true
            })
            .collect();

        stores.sort_by(|a, b| a.custom_key_store_id.cmp(&b.custom_key_store_id));

        // If filtering by ID and not found, return error
        if let Some(id) = filter_id {
            if stores.is_empty() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "CustomKeyStoreNotFoundException",
                    format!("Custom key store '{id}' does not exist"),
                ));
            }
        }

        let start = marker
            .and_then(|m| {
                stores
                    .iter()
                    .position(|s| s.custom_key_store_id == m)
                    .map(|p| p + 1)
            })
            .unwrap_or(0);

        let page: Vec<_> = stores.iter().skip(start).take(limit).collect();
        let truncated = start + page.len() < stores.len();

        let entries: Vec<Value> = page.iter().map(|s| custom_key_store_json(s)).collect();

        let mut resp = json!({ "CustomKeyStores": entries, "Truncated": truncated });
        if truncated {
            if let Some(last) = page.last() {
                resp["NextMarker"] = json!(last.custom_key_store_id);
            }
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&resp).unwrap(),
        ))
    }

    fn connect_custom_key_store(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let store_id = body["CustomKeyStoreId"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "CustomKeyStoreId is required",
                )
            })?
            .to_string();

        let mut state = self.state.write();

        let store = state.custom_key_stores.get_mut(&store_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "CustomKeyStoreNotFoundException",
                format!("Custom key store '{store_id}' does not exist"),
            )
        })?;

        store.connection_state = "CONNECTED".to_string();

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn disconnect_custom_key_store(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let store_id = body["CustomKeyStoreId"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "CustomKeyStoreId is required",
                )
            })?
            .to_string();

        let mut state = self.state.write();

        let store = state.custom_key_stores.get_mut(&store_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "CustomKeyStoreNotFoundException",
                format!("Custom key store '{store_id}' does not exist"),
            )
        })?;

        store.connection_state = "DISCONNECTED".to_string();

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn update_custom_key_store(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let store_id = body["CustomKeyStoreId"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "CustomKeyStoreId is required",
                )
            })?
            .to_string();

        let mut state = self.state.write();

        // Check uniqueness of new name before borrowing store mutably
        if let Some(new_name) = body["NewCustomKeyStoreName"].as_str() {
            if state
                .custom_key_stores
                .values()
                .any(|s| s.custom_key_store_name == new_name && s.custom_key_store_id != store_id)
            {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "CustomKeyStoreNameInUseException",
                    format!("Custom key store name '{new_name}' is already in use"),
                ));
            }
        }

        let store = state.custom_key_stores.get_mut(&store_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "CustomKeyStoreNotFoundException",
                format!("Custom key store '{store_id}' does not exist"),
            )
        })?;

        if let Some(new_name) = body["NewCustomKeyStoreName"].as_str() {
            store.custom_key_store_name = new_name.to_string();
        }
        if let Some(v) = body["CloudHsmClusterId"].as_str() {
            store.cloud_hsm_cluster_id = Some(v.to_string());
        }
        if let Some(v) = body["KeyStorePassword"].as_str() {
            // In a real implementation this would update the password;
            // we just accept it silently.
            let _ = v;
        }
        if let Some(v) = body["XksProxyUriEndpoint"].as_str() {
            store.xks_proxy_uri_endpoint = Some(v.to_string());
        }
        if let Some(v) = body["XksProxyUriPath"].as_str() {
            store.xks_proxy_uri_path = Some(v.to_string());
        }
        if let Some(v) = body["XksProxyVpcEndpointServiceName"].as_str() {
            store.xks_proxy_vpc_endpoint_service_name = Some(v.to_string());
        }
        if let Some(v) = body["XksProxyConnectivity"].as_str() {
            store.xks_proxy_connectivity = Some(v.to_string());
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }
}

fn custom_key_store_json(store: &CustomKeyStore) -> Value {
    let mut obj = json!({
        "CustomKeyStoreId": store.custom_key_store_id,
        "CustomKeyStoreName": store.custom_key_store_name,
        "CustomKeyStoreType": store.custom_key_store_type,
        "ConnectionState": store.connection_state,
        "CreationDate": store.creation_date,
    });
    if let Some(ref v) = store.cloud_hsm_cluster_id {
        obj["CloudHsmClusterId"] = json!(v);
    }
    if let Some(ref v) = store.trust_anchor_certificate {
        obj["TrustAnchorCertificate"] = json!(v);
    }
    if let Some(ref v) = store.xks_proxy_uri_endpoint {
        obj["XksProxyConfiguration"] = json!({});
        obj["XksProxyConfiguration"]["UriEndpoint"] = json!(v);
        if let Some(ref p) = store.xks_proxy_uri_path {
            obj["XksProxyConfiguration"]["UriPath"] = json!(p);
        }
        if let Some(ref c) = store.xks_proxy_connectivity {
            obj["XksProxyConfiguration"]["Connectivity"] = json!(c);
        }
        if let Some(ref s) = store.xks_proxy_vpc_endpoint_service_name {
            obj["XksProxyConfiguration"]["VpcEndpointServiceName"] = json!(s);
        }
    }
    obj
}

fn key_metadata_json(key: &KmsKey, account_id: &str) -> Value {
    let mut meta = json!({
        "KeyId": key.key_id,
        "Arn": key.arn,
        "AWSAccountId": account_id,
        "CreationDate": key.creation_date,
        "Description": key.description,
        "Enabled": key.enabled,
        "KeyUsage": key.key_usage,
        "KeySpec": key.key_spec,
        "CustomerMasterKeySpec": key.key_spec,
        "KeyManager": key.key_manager,
        "KeyState": key.key_state,
        "Origin": key.origin,
        "MultiRegion": key.multi_region,
    });

    if let Some(ref enc_algs) = key.encryption_algorithms {
        meta["EncryptionAlgorithms"] = json!(enc_algs);
    }
    if let Some(ref sig_algs) = key.signing_algorithms {
        meta["SigningAlgorithms"] = json!(sig_algs);
    }
    if let Some(ref mac_algs) = key.mac_algorithms {
        meta["MacAlgorithms"] = json!(mac_algs);
    }
    if let Some(dd) = key.deletion_date {
        meta["DeletionDate"] = json!(dd);
    }
    if let Some(ref cks_id) = key.custom_key_store_id {
        meta["CustomKeyStoreId"] = json!(cks_id);
    }

    if key.multi_region {
        // Add MultiRegionConfiguration for primary keys
        meta["MultiRegionConfiguration"] = json!({
            "MultiRegionKeyType": "PRIMARY",
            "PrimaryKey": {
                "Arn": key.arn,
                "Region": key.arn.split(':').nth(3).unwrap_or("us-east-1"),
            },
            "ReplicaKeys": [],
        });
    }

    meta
}

fn fmt_enum_set(items: &[String]) -> String {
    let inner: Vec<String> = items.iter().map(|s| format!("'{s}'")).collect();
    format!("[{}]", inner.join(", "))
}

fn grant_to_json(grant: &KmsGrant) -> Value {
    let mut v = json!({
        "KeyId": grant.key_id,
        "GrantId": grant.grant_id,
        "GranteePrincipal": grant.grantee_principal,
        "Operations": grant.operations,
        "IssuingAccount": format!("arn:aws:iam::root"),
        "CreationDate": grant.creation_date,
    });

    if let Some(ref rp) = grant.retiring_principal {
        v["RetiringPrincipal"] = json!(rp);
    }
    if let Some(ref c) = grant.constraints {
        v["Constraints"] = c.clone();
    }
    if let Some(ref n) = grant.name {
        v["Name"] = json!(n);
    }

    v
}

fn data_key_size_from_body(body: &Value) -> Result<usize, AwsServiceError> {
    let key_spec = body["KeySpec"].as_str();
    let number_of_bytes = body["NumberOfBytes"].as_u64();

    match (key_spec, number_of_bytes) {
        (Some(_), Some(_)) => Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "KeySpec and NumberOfBytes are mutually exclusive",
        )),
        (Some("AES_256"), None) => Ok(32),
        (Some("AES_128"), None) => Ok(16),
        (Some(spec), None) => Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!("1 validation error detected: Value '{spec}' at 'keySpec' failed to satisfy constraint: Member must satisfy enum value set: [AES_256, AES_128]"),
        )),
        (None, Some(n)) => {
            if n > 1024 {
                Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!("1 validation error detected: Value '{n}' at 'numberOfBytes' failed to satisfy constraint: Member must have value less than or equal to 1024"),
                ))
            } else {
                Ok(n as usize)
            }
        }
        (None, None) => Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "KeySpec or NumberOfBytes is required",
        )),
    }
}

fn generate_fake_public_key(key_spec: &str) -> Vec<u8> {
    // Return a minimal but valid-looking DER-encoded SubjectPublicKeyInfo
    // This is a fake RSA 2048-bit public key structure for testing
    match key_spec {
        "RSA_2048" | "RSA_3072" | "RSA_4096" => {
            // A minimal ASN.1 DER structure for RSA public key
            let mut key = vec![
                0x30, 0x82, 0x01, 0x22, // SEQUENCE, length 290
                0x30, 0x0d, // SEQUENCE, length 13
                0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01,
                0x01, // OID rsaEncryption
                0x05, 0x00, // NULL
                0x03, 0x82, 0x01, 0x0f, // BIT STRING, length 271
                0x00, // unused bits
                0x30, 0x82, 0x01, 0x0a, // SEQUENCE, length 266
                0x02, 0x82, 0x01, 0x01, // INTEGER, length 257
            ];
            // Fake modulus (257 bytes: 0x00 + 256 bytes of random-looking data)
            key.push(0x00);
            key.extend_from_slice(&rand_bytes(256));
            // Exponent
            key.extend_from_slice(&[0x02, 0x03, 0x01, 0x00, 0x01]); // 65537
            key
        }
        "ECC_NIST_P256" | "ECC_SECG_P256K1" => {
            // Minimal EC public key for P-256
            let mut key = vec![
                0x30, 0x59, // SEQUENCE, length 89
                0x30, 0x13, // SEQUENCE, length 19
                0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01, // OID ecPublicKey
                0x06, 0x08, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07, // OID prime256v1
                0x03, 0x42, // BIT STRING, length 66
                0x00, // unused bits
                0x04, // uncompressed point
            ];
            key.extend_from_slice(&rand_bytes(64)); // x and y coordinates
            key
        }
        "ECC_NIST_P384" => {
            let mut key = vec![
                0x30, 0x76, // SEQUENCE, length 118
                0x30, 0x10, // SEQUENCE, length 16
                0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01, // OID ecPublicKey
                0x06, 0x05, 0x2b, 0x81, 0x04, 0x00, 0x22, // OID secp384r1
                0x03, 0x62, // BIT STRING, length 98
                0x00, // unused bits
                0x04, // uncompressed point
            ];
            key.extend_from_slice(&rand_bytes(96)); // x and y coordinates
            key
        }
        "ECC_NIST_P521" => {
            let mut key = vec![
                0x30, 0x81, 0x9b, // SEQUENCE, length 155
                0x30, 0x10, // SEQUENCE, length 16
                0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01, // OID ecPublicKey
                0x06, 0x05, 0x2b, 0x81, 0x04, 0x00, 0x23, // OID secp521r1
                0x03, 0x81, 0x86, // BIT STRING, length 134
                0x00, // unused bits
                0x04, // uncompressed point
            ];
            key.extend_from_slice(&rand_bytes(132)); // x and y coordinates
            key
        }
        _ => rand_bytes(32),
    }
}

fn check_policy_deny(key: &KmsKey, action: &str) -> Result<(), AwsServiceError> {
    // Parse the policy and check for Deny statements
    let policy: Value = match serde_json::from_str(&key.policy) {
        Ok(v) => v,
        Err(_) => return Ok(()), // If policy can't be parsed, allow
    };

    let statements = match policy["Statement"].as_array() {
        Some(s) => s,
        None => return Ok(()),
    };

    for stmt in statements {
        let effect = stmt["Effect"].as_str().unwrap_or("");
        if !effect.eq_ignore_ascii_case("deny") {
            continue;
        }

        // Check Resource - only deny if resource is "*"
        let resource = &stmt["Resource"];
        let resource_matches = if let Some(r) = resource.as_str() {
            r == "*"
        } else if let Some(arr) = resource.as_array() {
            arr.iter().any(|r| r.as_str() == Some("*"))
        } else {
            false
        };

        if !resource_matches {
            continue;
        }

        // Check Action
        let actions = if let Some(a) = stmt["Action"].as_str() {
            vec![a.to_string()]
        } else if let Some(arr) = stmt["Action"].as_array() {
            arr.iter()
                .filter_map(|a| a.as_str().map(|s| s.to_string()))
                .collect()
        } else {
            continue;
        };

        for policy_action in &actions {
            if action_matches(policy_action, action) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "AccessDeniedException",
                    format!(
                        "User is not authorized to perform: {} on resource: {}",
                        action, key.arn
                    ),
                ));
            }
        }
    }

    Ok(())
}

fn action_matches(policy_action: &str, requested_action: &str) -> bool {
    if policy_action == "kms:*" {
        return true;
    }
    if policy_action == requested_action {
        return true;
    }
    // Wildcard matching: "kms:Describe*" matches "kms:DescribeKey"
    if let Some(prefix) = policy_action.strip_suffix('*') {
        if requested_action.starts_with(prefix) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::RwLock;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_service() -> KmsService {
        let state: SharedKmsState = Arc::new(RwLock::new(crate::state::KmsState::new(
            "123456789012",
            "us-east-1",
        )));
        KmsService::new(state)
    }

    fn make_request(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "kms".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test-id".to_string(),
            headers: http::HeaderMap::new(),
            query_params: HashMap::new(),
            body: serde_json::to_vec(&body).unwrap().into(),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
        }
    }

    fn create_key(svc: &KmsService) -> String {
        let req = make_request("CreateKey", json!({}));
        let resp = svc.create_key(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        body["KeyMetadata"]["KeyId"].as_str().unwrap().to_string()
    }

    #[test]
    fn list_keys_pagination_no_duplicates() {
        let svc = make_service();
        let mut all_key_ids: Vec<String> = Vec::new();
        for _ in 0..5 {
            all_key_ids.push(create_key(&svc));
        }

        let mut collected_ids: Vec<String> = Vec::new();
        let mut marker: Option<String> = None;

        loop {
            let mut body = json!({ "Limit": 2 });
            if let Some(ref m) = marker {
                body["Marker"] = json!(m);
            }
            let req = make_request("ListKeys", body);
            let resp = svc.list_keys(&req).unwrap();
            let resp_body: Value = serde_json::from_slice(&resp.body).unwrap();

            for key in resp_body["Keys"].as_array().unwrap() {
                collected_ids.push(key["KeyId"].as_str().unwrap().to_string());
            }

            if resp_body["Truncated"].as_bool().unwrap_or(false) {
                marker = resp_body["NextMarker"].as_str().map(|s| s.to_string());
            } else {
                break;
            }
        }

        // Verify no duplicates
        let mut deduped = collected_ids.clone();
        deduped.sort();
        deduped.dedup();
        assert_eq!(
            collected_ids.len(),
            deduped.len(),
            "pagination produced duplicate keys"
        );

        // Verify all keys returned
        for kid in &all_key_ids {
            assert!(
                collected_ids.contains(kid),
                "key {kid} missing from paginated results"
            );
        }
    }

    #[test]
    fn list_retirable_grants_pagination() {
        let svc = make_service();
        let key_id = create_key(&svc);
        let retiring = "arn:aws:iam::123456789012:user/retiring-user";

        // Create 5 grants with the same retiring principal
        for i in 0..5 {
            let req = make_request(
                "CreateGrant",
                json!({
                    "KeyId": key_id,
                    "GranteePrincipal": format!("arn:aws:iam::123456789012:user/grantee-{i}"),
                    "RetiringPrincipal": retiring,
                    "Operations": ["Encrypt"]
                }),
            );
            svc.create_grant(&req).unwrap();
        }

        let mut collected_ids: Vec<String> = Vec::new();
        let mut marker: Option<String> = None;

        loop {
            let mut body = json!({
                "RetiringPrincipal": retiring,
                "Limit": 2
            });
            if let Some(ref m) = marker {
                body["Marker"] = json!(m);
            }
            let req = make_request("ListRetirableGrants", body);
            let resp = svc.list_retirable_grants(&req).unwrap();
            let resp_body: Value = serde_json::from_slice(&resp.body).unwrap();

            for grant in resp_body["Grants"].as_array().unwrap() {
                collected_ids.push(grant["GrantId"].as_str().unwrap().to_string());
            }

            if resp_body["Truncated"].as_bool().unwrap_or(false) {
                marker = resp_body["NextMarker"].as_str().map(|s| s.to_string());
            } else {
                break;
            }
        }

        // Verify no duplicates
        let mut deduped = collected_ids.clone();
        deduped.sort();
        deduped.dedup();
        assert_eq!(
            collected_ids.len(),
            deduped.len(),
            "pagination produced duplicate grants"
        );

        // All 5 grants returned
        assert_eq!(collected_ids.len(), 5, "expected 5 grants total");
    }

    fn create_key_with_opts(svc: &KmsService, body: Value) -> String {
        let req = make_request("CreateKey", body);
        let resp = svc.create_key(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        body["KeyMetadata"]["KeyId"].as_str().unwrap().to_string()
    }

    #[test]
    fn generate_data_key_pair_returns_all_fields() {
        let svc = make_service();
        let key_id = create_key(&svc);

        let req = make_request(
            "GenerateDataKeyPair",
            json!({ "KeyId": key_id, "KeyPairSpec": "RSA_2048" }),
        );
        let resp = svc.generate_data_key_pair(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();

        assert!(body["PublicKey"].as_str().is_some());
        assert!(body["PrivateKeyPlaintext"].as_str().is_some());
        assert!(body["PrivateKeyCiphertextBlob"].as_str().is_some());
        assert_eq!(body["KeyPairSpec"].as_str().unwrap(), "RSA_2048");
        assert!(body["KeyId"].as_str().unwrap().contains(":key/"));
    }

    #[test]
    fn generate_data_key_pair_disabled_key_fails() {
        let svc = make_service();
        let key_id = create_key(&svc);

        let disable_req = make_request("DisableKey", json!({ "KeyId": key_id }));
        svc.disable_key(&disable_req).unwrap();

        let req = make_request(
            "GenerateDataKeyPair",
            json!({ "KeyId": key_id, "KeyPairSpec": "RSA_2048" }),
        );
        assert!(svc.generate_data_key_pair(&req).is_err());
    }

    #[test]
    fn generate_data_key_pair_without_plaintext_omits_private_plaintext() {
        let svc = make_service();
        let key_id = create_key(&svc);

        let req = make_request(
            "GenerateDataKeyPairWithoutPlaintext",
            json!({ "KeyId": key_id, "KeyPairSpec": "ECC_NIST_P256" }),
        );
        let resp = svc.generate_data_key_pair_without_plaintext(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();

        assert!(body["PublicKey"].as_str().is_some());
        assert!(body["PrivateKeyCiphertextBlob"].as_str().is_some());
        assert!(body.get("PrivateKeyPlaintext").is_none());
        assert_eq!(body["KeyPairSpec"].as_str().unwrap(), "ECC_NIST_P256");
    }

    #[test]
    fn derive_shared_secret_success() {
        let svc = make_service();
        let key_id = create_key_with_opts(
            &svc,
            json!({
                "KeyUsage": "KEY_AGREEMENT",
                "KeySpec": "ECC_NIST_P256"
            }),
        );

        let fake_pub = base64::engine::general_purpose::STANDARD.encode(b"fake-public-key");
        let req = make_request(
            "DeriveSharedSecret",
            json!({
                "KeyId": key_id,
                "KeyAgreementAlgorithm": "ECDH",
                "PublicKey": fake_pub
            }),
        );
        let resp = svc.derive_shared_secret(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();

        assert!(body["SharedSecret"].as_str().is_some());
        assert!(body["KeyId"].as_str().unwrap().contains(":key/"));
        assert_eq!(body["KeyAgreementAlgorithm"].as_str().unwrap(), "ECDH");
    }

    #[test]
    fn derive_shared_secret_wrong_usage_fails() {
        let svc = make_service();
        let key_id = create_key(&svc); // Default is ENCRYPT_DECRYPT

        let fake_pub = base64::engine::general_purpose::STANDARD.encode(b"fake-public-key");
        let req = make_request(
            "DeriveSharedSecret",
            json!({
                "KeyId": key_id,
                "KeyAgreementAlgorithm": "ECDH",
                "PublicKey": fake_pub
            }),
        );
        assert!(svc.derive_shared_secret(&req).is_err());
    }

    #[test]
    fn get_parameters_for_import_success() {
        let svc = make_service();
        let key_id = create_key_with_opts(&svc, json!({ "Origin": "EXTERNAL" }));

        let req = make_request(
            "GetParametersForImport",
            json!({ "KeyId": key_id, "WrappingAlgorithm": "RSAES_OAEP_SHA_256", "WrappingKeySpec": "RSA_2048" }),
        );
        let resp = svc.get_parameters_for_import(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();

        assert!(body["ImportToken"].as_str().is_some());
        assert!(body["PublicKey"].as_str().is_some());
        assert!(body["ParametersValidTo"].as_f64().is_some());
        assert!(body["KeyId"].as_str().unwrap().contains(":key/"));
    }

    #[test]
    fn get_parameters_for_import_non_external_fails() {
        let svc = make_service();
        let key_id = create_key(&svc); // Default origin is AWS_KMS

        let req = make_request("GetParametersForImport", json!({ "KeyId": key_id }));
        assert!(svc.get_parameters_for_import(&req).is_err());
    }

    #[test]
    fn import_key_material_lifecycle() {
        let svc = make_service();
        let key_id = create_key_with_opts(&svc, json!({ "Origin": "EXTERNAL" }));

        let fake_token = base64::engine::general_purpose::STANDARD.encode(b"token");
        let fake_material = base64::engine::general_purpose::STANDARD.encode(b"material");

        // Import
        let req = make_request(
            "ImportKeyMaterial",
            json!({
                "KeyId": key_id,
                "ImportToken": fake_token,
                "EncryptedKeyMaterial": fake_material,
                "ExpirationModel": "KEY_MATERIAL_DOES_NOT_EXPIRE"
            }),
        );
        svc.import_key_material(&req).unwrap();

        // Key should be enabled
        {
            let state = svc.state.read();
            let key = state.keys.get(&key_id).unwrap();
            assert!(key.imported_key_material);
            assert!(key.enabled);
        }

        // Delete imported material
        let req = make_request("DeleteImportedKeyMaterial", json!({ "KeyId": key_id }));
        svc.delete_imported_key_material(&req).unwrap();

        // Key should be disabled and pending import
        {
            let state = svc.state.read();
            let key = state.keys.get(&key_id).unwrap();
            assert!(!key.imported_key_material);
            assert!(!key.enabled);
            assert_eq!(key.key_state, "PendingImport");
        }
    }

    #[test]
    fn import_key_material_non_external_fails() {
        let svc = make_service();
        let key_id = create_key(&svc);

        let fake_token = base64::engine::general_purpose::STANDARD.encode(b"token");
        let fake_material = base64::engine::general_purpose::STANDARD.encode(b"material");

        let req = make_request(
            "ImportKeyMaterial",
            json!({
                "KeyId": key_id,
                "ImportToken": fake_token,
                "EncryptedKeyMaterial": fake_material
            }),
        );
        assert!(svc.import_key_material(&req).is_err());
    }

    #[test]
    fn delete_imported_key_material_non_external_fails() {
        let svc = make_service();
        let key_id = create_key(&svc);

        let req = make_request("DeleteImportedKeyMaterial", json!({ "KeyId": key_id }));
        assert!(svc.delete_imported_key_material(&req).is_err());
    }

    #[test]
    fn update_primary_region_success() {
        let svc = make_service();
        let key_id = create_key_with_opts(&svc, json!({ "MultiRegion": true }));

        let req = make_request(
            "UpdatePrimaryRegion",
            json!({ "KeyId": key_id, "PrimaryRegion": "eu-west-1" }),
        );
        svc.update_primary_region(&req).unwrap();

        let state = svc.state.read();
        let key = state.keys.get(&key_id).unwrap();
        assert_eq!(key.primary_region.as_deref(), Some("eu-west-1"));
        assert!(key.arn.contains("eu-west-1"));
    }

    #[test]
    fn update_primary_region_non_multi_region_fails() {
        let svc = make_service();
        let key_id = create_key(&svc); // Not multi-region

        let req = make_request(
            "UpdatePrimaryRegion",
            json!({ "KeyId": key_id, "PrimaryRegion": "eu-west-1" }),
        );
        assert!(svc.update_primary_region(&req).is_err());
    }

    #[test]
    fn custom_key_store_lifecycle() {
        let svc = make_service();

        // Create
        let req = make_request(
            "CreateCustomKeyStore",
            json!({
                "CustomKeyStoreName": "my-store",
                "CloudHsmClusterId": "cluster-1234",
                "TrustAnchorCertificate": "cert-data",
                "KeyStorePassword": "password123"
            }),
        );
        let resp = svc.create_custom_key_store(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let store_id = body["CustomKeyStoreId"].as_str().unwrap().to_string();
        assert!(store_id.starts_with("cks-"));

        // Describe
        let req = make_request(
            "DescribeCustomKeyStores",
            json!({ "CustomKeyStoreId": store_id }),
        );
        let resp = svc.describe_custom_key_stores(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let stores = body["CustomKeyStores"].as_array().unwrap();
        assert_eq!(stores.len(), 1);
        assert_eq!(
            stores[0]["CustomKeyStoreName"].as_str().unwrap(),
            "my-store"
        );
        assert_eq!(
            stores[0]["ConnectionState"].as_str().unwrap(),
            "DISCONNECTED"
        );
        assert_eq!(
            stores[0]["CloudHsmClusterId"].as_str().unwrap(),
            "cluster-1234"
        );

        // Connect
        let req = make_request(
            "ConnectCustomKeyStore",
            json!({ "CustomKeyStoreId": store_id }),
        );
        svc.connect_custom_key_store(&req).unwrap();

        // Verify connected
        let req = make_request(
            "DescribeCustomKeyStores",
            json!({ "CustomKeyStoreId": store_id }),
        );
        let resp = svc.describe_custom_key_stores(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["CustomKeyStores"][0]["ConnectionState"]
                .as_str()
                .unwrap(),
            "CONNECTED"
        );

        // Cannot delete when connected
        let req = make_request(
            "DeleteCustomKeyStore",
            json!({ "CustomKeyStoreId": store_id }),
        );
        assert!(svc.delete_custom_key_store(&req).is_err());

        // Disconnect
        let req = make_request(
            "DisconnectCustomKeyStore",
            json!({ "CustomKeyStoreId": store_id }),
        );
        svc.disconnect_custom_key_store(&req).unwrap();

        // Update name
        let req = make_request(
            "UpdateCustomKeyStore",
            json!({
                "CustomKeyStoreId": store_id,
                "NewCustomKeyStoreName": "renamed-store"
            }),
        );
        svc.update_custom_key_store(&req).unwrap();

        // Verify update
        let req = make_request(
            "DescribeCustomKeyStores",
            json!({ "CustomKeyStoreId": store_id }),
        );
        let resp = svc.describe_custom_key_stores(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["CustomKeyStores"][0]["CustomKeyStoreName"]
                .as_str()
                .unwrap(),
            "renamed-store"
        );

        // Delete
        let req = make_request(
            "DeleteCustomKeyStore",
            json!({ "CustomKeyStoreId": store_id }),
        );
        svc.delete_custom_key_store(&req).unwrap();

        // Describe all should return empty
        let req = make_request("DescribeCustomKeyStores", json!({}));
        let resp = svc.describe_custom_key_stores(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["CustomKeyStores"].as_array().unwrap().is_empty());
    }

    #[test]
    fn custom_key_store_duplicate_name_fails() {
        let svc = make_service();

        let req = make_request(
            "CreateCustomKeyStore",
            json!({ "CustomKeyStoreName": "dup-store" }),
        );
        svc.create_custom_key_store(&req).unwrap();

        let req = make_request(
            "CreateCustomKeyStore",
            json!({ "CustomKeyStoreName": "dup-store" }),
        );
        assert!(svc.create_custom_key_store(&req).is_err());
    }

    #[test]
    fn describe_custom_key_store_not_found() {
        let svc = make_service();

        let req = make_request(
            "DescribeCustomKeyStores",
            json!({ "CustomKeyStoreId": "cks-nonexistent" }),
        );
        assert!(svc.describe_custom_key_stores(&req).is_err());
    }

    #[test]
    fn delete_nonexistent_custom_key_store_fails() {
        let svc = make_service();

        let req = make_request(
            "DeleteCustomKeyStore",
            json!({ "CustomKeyStoreId": "cks-nonexistent" }),
        );
        assert!(svc.delete_custom_key_store(&req).is_err());
    }

    #[test]
    fn connect_nonexistent_custom_key_store_fails() {
        let svc = make_service();

        let req = make_request(
            "ConnectCustomKeyStore",
            json!({ "CustomKeyStoreId": "cks-nonexistent" }),
        );
        assert!(svc.connect_custom_key_store(&req).is_err());
    }

    #[test]
    fn describe_custom_key_stores_by_name() {
        let svc = make_service();

        let req = make_request(
            "CreateCustomKeyStore",
            json!({ "CustomKeyStoreName": "store-a" }),
        );
        svc.create_custom_key_store(&req).unwrap();

        let req = make_request(
            "CreateCustomKeyStore",
            json!({ "CustomKeyStoreName": "store-b" }),
        );
        svc.create_custom_key_store(&req).unwrap();

        // Filter by name
        let req = make_request(
            "DescribeCustomKeyStores",
            json!({ "CustomKeyStoreName": "store-a" }),
        );
        let resp = svc.describe_custom_key_stores(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let stores = body["CustomKeyStores"].as_array().unwrap();
        assert_eq!(stores.len(), 1);
        assert_eq!(stores[0]["CustomKeyStoreName"].as_str().unwrap(), "store-a");
    }

    #[test]
    fn update_custom_key_store_name_conflict() {
        let svc = make_service();

        let req = make_request(
            "CreateCustomKeyStore",
            json!({ "CustomKeyStoreName": "store-x" }),
        );
        svc.create_custom_key_store(&req).unwrap();

        let req = make_request(
            "CreateCustomKeyStore",
            json!({ "CustomKeyStoreName": "store-y" }),
        );
        let resp = svc.create_custom_key_store(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let store_y_id = body["CustomKeyStoreId"].as_str().unwrap().to_string();

        // Try to rename store-y to store-x
        let req = make_request(
            "UpdateCustomKeyStore",
            json!({
                "CustomKeyStoreId": store_y_id,
                "NewCustomKeyStoreName": "store-x"
            }),
        );
        assert!(svc.update_custom_key_store(&req).is_err());
    }

    #[test]
    fn derive_shared_secret_is_deterministic() {
        let svc = make_service();
        let key_id = create_key_with_opts(
            &svc,
            json!({
                "KeyUsage": "KEY_AGREEMENT",
                "KeySpec": "ECC_NIST_P256"
            }),
        );

        let pub_key = base64::engine::general_purpose::STANDARD.encode(b"counterparty-public-key");
        let req = make_request(
            "DeriveSharedSecret",
            json!({
                "KeyId": key_id,
                "KeyAgreementAlgorithm": "ECDH",
                "PublicKey": pub_key
            }),
        );

        let resp1 = svc.derive_shared_secret(&req).unwrap();
        let body1: Value = serde_json::from_slice(&resp1.body).unwrap();
        let secret1 = body1["SharedSecret"].as_str().unwrap().to_string();

        // Same inputs must produce the same shared secret
        let resp2 = svc.derive_shared_secret(&req).unwrap();
        let body2: Value = serde_json::from_slice(&resp2.body).unwrap();
        let secret2 = body2["SharedSecret"].as_str().unwrap().to_string();

        assert_eq!(secret1, secret2, "DeriveSharedSecret must be deterministic");

        // Different public key must produce a different shared secret
        let other_pub = base64::engine::general_purpose::STANDARD.encode(b"different-public-key");
        let req2 = make_request(
            "DeriveSharedSecret",
            json!({
                "KeyId": key_id,
                "KeyAgreementAlgorithm": "ECDH",
                "PublicKey": other_pub
            }),
        );
        let resp3 = svc.derive_shared_secret(&req2).unwrap();
        let body3: Value = serde_json::from_slice(&resp3.body).unwrap();
        let secret3 = body3["SharedSecret"].as_str().unwrap().to_string();
        assert_ne!(
            secret1, secret3,
            "Different public keys must yield different shared secrets"
        );
    }

    #[test]
    fn imported_key_material_encrypt_decrypt_roundtrip() {
        let svc = make_service();
        let key_id = create_key_with_opts(&svc, json!({ "Origin": "EXTERNAL" }));

        let fake_token = base64::engine::general_purpose::STANDARD.encode(b"token");
        let material = b"my-secret-aes-key-material!12345";
        let fake_material = base64::engine::general_purpose::STANDARD.encode(material);

        // Import key material
        let req = make_request(
            "ImportKeyMaterial",
            json!({
                "KeyId": key_id,
                "ImportToken": fake_token,
                "EncryptedKeyMaterial": fake_material,
            }),
        );
        svc.import_key_material(&req).unwrap();

        // Encrypt
        let plaintext = b"Hello imported key!";
        let plaintext_b64 = base64::engine::general_purpose::STANDARD.encode(plaintext);
        let req = make_request(
            "Encrypt",
            json!({ "KeyId": key_id, "Plaintext": plaintext_b64 }),
        );
        let resp = svc.encrypt(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let ciphertext = body["CiphertextBlob"].as_str().unwrap().to_string();

        // Verify ciphertext uses the imported envelope
        let ct_bytes = base64::engine::general_purpose::STANDARD
            .decode(&ciphertext)
            .unwrap();
        let envelope = String::from_utf8(ct_bytes).unwrap();
        assert!(
            envelope.starts_with("fakecloud-imported:"),
            "Imported key should use fakecloud-imported envelope"
        );

        // Decrypt
        let req = make_request("Decrypt", json!({ "CiphertextBlob": ciphertext }));
        let resp = svc.decrypt(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let decrypted_b64 = body["Plaintext"].as_str().unwrap();
        let decrypted = base64::engine::general_purpose::STANDARD
            .decode(decrypted_b64)
            .unwrap();
        assert_eq!(
            decrypted, plaintext,
            "Decrypt must recover the original plaintext"
        );
    }

    #[test]
    fn imported_key_material_decrypt_fails_after_deletion() {
        let svc = make_service();
        let key_id = create_key_with_opts(&svc, json!({ "Origin": "EXTERNAL" }));

        let fake_token = base64::engine::general_purpose::STANDARD.encode(b"token");
        let fake_material =
            base64::engine::general_purpose::STANDARD.encode(b"some-key-material-32bytes!!");

        // Import and encrypt
        svc.import_key_material(&make_request(
            "ImportKeyMaterial",
            json!({
                "KeyId": key_id,
                "ImportToken": fake_token,
                "EncryptedKeyMaterial": fake_material,
            }),
        ))
        .unwrap();

        let plaintext_b64 = base64::engine::general_purpose::STANDARD.encode(b"secret");
        let resp = svc
            .encrypt(&make_request(
                "Encrypt",
                json!({ "KeyId": key_id, "Plaintext": plaintext_b64 }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let ciphertext = body["CiphertextBlob"].as_str().unwrap().to_string();

        // Delete imported material
        svc.delete_imported_key_material(&make_request(
            "DeleteImportedKeyMaterial",
            json!({ "KeyId": key_id }),
        ))
        .unwrap();

        // Re-import to re-enable the key but material bytes are gone for old ciphertext path
        // Actually, after deletion the key is disabled, so decrypt will fail with DisabledException
        let result = svc.decrypt(&make_request(
            "Decrypt",
            json!({ "CiphertextBlob": ciphertext }),
        ));
        assert!(
            result.is_err(),
            "Decrypt should fail after key material deletion"
        );
    }

    #[test]
    fn list_keys_rejects_non_integer_limit() {
        let svc = make_service();
        // String value should fail validation
        let req = make_request("ListKeys", json!({ "Limit": "abc" }));
        let result = svc.list_keys(&req);
        assert!(result.is_err(), "non-integer Limit should be rejected");
    }

    #[test]
    fn list_keys_rejects_large_unsigned_limit() {
        let svc = make_service();
        // Value larger than i64::MAX should fail validation
        let req = make_request("ListKeys", json!({ "Limit": u64::MAX }));
        let result = svc.list_keys(&req);
        assert!(result.is_err(), "large unsigned Limit should be rejected");
    }

    #[test]
    fn list_keys_rejects_out_of_range_limit() {
        let svc = make_service();
        let req = make_request("ListKeys", json!({ "Limit": 0 }));
        let result = svc.list_keys(&req);
        assert!(result.is_err(), "Limit=0 should be rejected");

        let req = make_request("ListKeys", json!({ "Limit": 1001 }));
        let result = svc.list_keys(&req);
        assert!(result.is_err(), "Limit=1001 should be rejected");
    }

    #[test]
    fn enable_key_with_nonexistent_id_returns_error() {
        let svc = make_service();
        // Manually insert a resolved key ID into the state, then remove it to simulate
        // a race condition where resolve_required_key succeeds but get_mut fails
        let key_id = create_key(&svc);

        // Delete the key from state directly to simulate inconsistency
        svc.state.write().keys.remove(&key_id);

        let req = make_request("EnableKey", json!({ "KeyId": key_id }));
        let result = svc.enable_key(&req);
        assert!(result.is_err(), "Should return error for missing key");
    }

    #[test]
    fn disable_key_with_nonexistent_id_returns_error() {
        let svc = make_service();
        let key_id = create_key(&svc);
        svc.state.write().keys.remove(&key_id);

        let req = make_request("DisableKey", json!({ "KeyId": key_id }));
        let result = svc.disable_key(&req);
        assert!(result.is_err(), "Should return error for missing key");
    }

    #[test]
    fn tag_resource_with_nonexistent_key_returns_error() {
        let svc = make_service();
        let key_id = create_key(&svc);
        svc.state.write().keys.remove(&key_id);

        let req = make_request(
            "TagResource",
            json!({ "KeyId": key_id, "Tags": [{"TagKey": "k", "TagValue": "v"}] }),
        );
        let result = svc.tag_resource(&req);
        assert!(result.is_err(), "Should return error for missing key");
    }

    #[test]
    fn cancel_key_deletion_re_enables_key() {
        let svc = make_service();
        let key_id = create_key(&svc);

        // Schedule deletion
        let req = make_request(
            "ScheduleKeyDeletion",
            json!({ "KeyId": key_id, "PendingWindowInDays": 7 }),
        );
        let resp = svc.schedule_key_deletion(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["KeyState"].as_str().unwrap(), "PendingDeletion");

        // Verify key is pending deletion
        {
            let state = svc.state.read();
            let key = state.keys.get(&key_id).unwrap();
            assert_eq!(key.key_state, "PendingDeletion");
            assert!(!key.enabled);
            assert!(key.deletion_date.is_some());
        }

        // Cancel deletion
        let req = make_request("CancelKeyDeletion", json!({ "KeyId": key_id }));
        let resp = svc.cancel_key_deletion(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["KeyId"].as_str().unwrap(), key_id);

        // Key should be disabled (not enabled) with no deletion date
        {
            let state = svc.state.read();
            let key = state.keys.get(&key_id).unwrap();
            assert_eq!(key.key_state, "Disabled");
            assert!(key.deletion_date.is_none());
        }

        // Re-enable the key
        let req = make_request("EnableKey", json!({ "KeyId": key_id }));
        svc.enable_key(&req).unwrap();

        {
            let state = svc.state.read();
            let key = state.keys.get(&key_id).unwrap();
            assert!(key.enabled);
            assert_eq!(key.key_state, "Enabled");
        }
    }

    #[test]
    fn key_rotation_lifecycle() {
        let svc = make_service();
        let key_id = create_key(&svc);

        // Initially rotation is disabled
        let req = make_request("GetKeyRotationStatus", json!({ "KeyId": key_id }));
        let resp = svc.get_key_rotation_status(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(!body["KeyRotationEnabled"].as_bool().unwrap());

        // Enable rotation
        let req = make_request("EnableKeyRotation", json!({ "KeyId": key_id }));
        svc.enable_key_rotation(&req).unwrap();

        let req = make_request("GetKeyRotationStatus", json!({ "KeyId": key_id }));
        let resp = svc.get_key_rotation_status(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["KeyRotationEnabled"].as_bool().unwrap());

        // Disable rotation
        let req = make_request("DisableKeyRotation", json!({ "KeyId": key_id }));
        svc.disable_key_rotation(&req).unwrap();

        let req = make_request("GetKeyRotationStatus", json!({ "KeyId": key_id }));
        let resp = svc.get_key_rotation_status(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(!body["KeyRotationEnabled"].as_bool().unwrap());
    }

    #[test]
    fn rotate_key_on_demand_and_list_rotations() {
        let svc = make_service();
        let key_id = create_key(&svc);

        // No rotations initially
        let req = make_request("ListKeyRotations", json!({ "KeyId": key_id }));
        let resp = svc.list_key_rotations(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["Rotations"].as_array().unwrap().is_empty());

        // Rotate on demand
        let req = make_request("RotateKeyOnDemand", json!({ "KeyId": key_id }));
        let resp = svc.rotate_key_on_demand(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["KeyId"].as_str().unwrap(), key_id);

        // Rotate again
        let req = make_request("RotateKeyOnDemand", json!({ "KeyId": key_id }));
        svc.rotate_key_on_demand(&req).unwrap();

        // List rotations
        let req = make_request("ListKeyRotations", json!({ "KeyId": key_id }));
        let resp = svc.list_key_rotations(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let rotations = body["Rotations"].as_array().unwrap();
        assert_eq!(rotations.len(), 2);
        assert_eq!(rotations[0]["RotationType"].as_str().unwrap(), "ON_DEMAND");
        assert_eq!(rotations[0]["KeyId"].as_str().unwrap(), key_id);
        assert!(rotations[0]["RotationDate"].as_f64().is_some());
    }

    #[test]
    fn key_policy_get_put_list() {
        let svc = make_service();
        let key_id = create_key(&svc);

        // Get default policy
        let req = make_request("GetKeyPolicy", json!({ "KeyId": key_id }));
        let resp = svc.get_key_policy(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let policy_str = body["Policy"].as_str().unwrap();
        assert!(policy_str.contains("Enable IAM User Permissions"));

        // Put custom policy
        let custom_policy = r#"{"Version":"2012-10-17","Statement":[]}"#;
        let req = make_request(
            "PutKeyPolicy",
            json!({ "KeyId": key_id, "Policy": custom_policy }),
        );
        svc.put_key_policy(&req).unwrap();

        // Get updated policy
        let req = make_request("GetKeyPolicy", json!({ "KeyId": key_id }));
        let resp = svc.get_key_policy(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Policy"].as_str().unwrap(), custom_policy);

        // List key policies always returns ["default"]
        let req = make_request("ListKeyPolicies", json!({ "KeyId": key_id }));
        let resp = svc.list_key_policies(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let names = body["PolicyNames"].as_array().unwrap();
        assert_eq!(names.len(), 1);
        assert_eq!(names[0].as_str().unwrap(), "default");
    }

    #[test]
    fn grant_create_list_revoke() {
        let svc = make_service();
        let key_id = create_key(&svc);

        // Create a grant
        let req = make_request(
            "CreateGrant",
            json!({
                "KeyId": key_id,
                "GranteePrincipal": "arn:aws:iam::123456789012:user/alice",
                "Operations": ["Encrypt", "Decrypt"],
                "Name": "test-grant"
            }),
        );
        let resp = svc.create_grant(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let grant_id = body["GrantId"].as_str().unwrap().to_string();
        let grant_token = body["GrantToken"].as_str().unwrap().to_string();
        assert!(!grant_id.is_empty());
        assert!(!grant_token.is_empty());

        // List grants
        let req = make_request("ListGrants", json!({ "KeyId": key_id }));
        let resp = svc.list_grants(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let grants = body["Grants"].as_array().unwrap();
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0]["GrantId"].as_str().unwrap(), grant_id);
        assert_eq!(
            grants[0]["GranteePrincipal"].as_str().unwrap(),
            "arn:aws:iam::123456789012:user/alice"
        );
        assert_eq!(grants[0]["Operations"].as_array().unwrap().len(), 2);

        // Revoke the grant
        let req = make_request(
            "RevokeGrant",
            json!({ "KeyId": key_id, "GrantId": grant_id }),
        );
        svc.revoke_grant(&req).unwrap();

        // List grants should be empty
        let req = make_request("ListGrants", json!({ "KeyId": key_id }));
        let resp = svc.list_grants(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["Grants"].as_array().unwrap().is_empty());
    }

    #[test]
    fn grant_retire_by_token() {
        let svc = make_service();
        let key_id = create_key(&svc);

        let req = make_request(
            "CreateGrant",
            json!({
                "KeyId": key_id,
                "GranteePrincipal": "arn:aws:iam::123456789012:user/bob",
                "RetiringPrincipal": "arn:aws:iam::123456789012:user/admin",
                "Operations": ["Encrypt"]
            }),
        );
        let resp = svc.create_grant(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let grant_token = body["GrantToken"].as_str().unwrap().to_string();

        // Retire by token
        let req = make_request("RetireGrant", json!({ "GrantToken": grant_token }));
        svc.retire_grant(&req).unwrap();

        // Verify grant is gone
        let req = make_request("ListGrants", json!({ "KeyId": key_id }));
        let resp = svc.list_grants(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["Grants"].as_array().unwrap().is_empty());
    }

    #[test]
    fn grant_retire_by_key_and_grant_id() {
        let svc = make_service();
        let key_id = create_key(&svc);

        let req = make_request(
            "CreateGrant",
            json!({
                "KeyId": key_id,
                "GranteePrincipal": "arn:aws:iam::123456789012:user/charlie",
                "Operations": ["Decrypt"]
            }),
        );
        let resp = svc.create_grant(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let grant_id = body["GrantId"].as_str().unwrap().to_string();

        // Retire by key ID + grant ID
        let req = make_request(
            "RetireGrant",
            json!({ "KeyId": key_id, "GrantId": grant_id }),
        );
        svc.retire_grant(&req).unwrap();

        // Verify grant is gone
        let req = make_request("ListGrants", json!({ "KeyId": key_id }));
        let resp = svc.list_grants(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["Grants"].as_array().unwrap().is_empty());
    }

    #[test]
    fn sign_verify_roundtrip() {
        let svc = make_service();
        let key_id = create_key_with_opts(
            &svc,
            json!({ "KeyUsage": "SIGN_VERIFY", "KeySpec": "RSA_2048" }),
        );

        let message = b"data to sign";
        let message_b64 = base64::engine::general_purpose::STANDARD.encode(message);

        // Sign
        let req = make_request(
            "Sign",
            json!({
                "KeyId": key_id,
                "Message": message_b64,
                "SigningAlgorithm": "RSASSA_PKCS1_V1_5_SHA_256"
            }),
        );
        let resp = svc.sign(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let signature = body["Signature"].as_str().unwrap().to_string();
        assert!(!signature.is_empty());
        assert_eq!(
            body["SigningAlgorithm"].as_str().unwrap(),
            "RSASSA_PKCS1_V1_5_SHA_256"
        );

        // Verify with correct signature
        let req = make_request(
            "Verify",
            json!({
                "KeyId": key_id,
                "Message": message_b64,
                "Signature": signature,
                "SigningAlgorithm": "RSASSA_PKCS1_V1_5_SHA_256"
            }),
        );
        let resp = svc.verify(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["SignatureValid"].as_bool().unwrap());

        // Verify with wrong signature should return false
        let wrong_sig = base64::engine::general_purpose::STANDARD.encode(b"wrong-signature-data");
        let req = make_request(
            "Verify",
            json!({
                "KeyId": key_id,
                "Message": message_b64,
                "Signature": wrong_sig,
                "SigningAlgorithm": "RSASSA_PKCS1_V1_5_SHA_256"
            }),
        );
        let resp = svc.verify(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(!body["SignatureValid"].as_bool().unwrap());
    }

    #[test]
    fn sign_with_ecc_key() {
        let svc = make_service();
        let key_id = create_key_with_opts(
            &svc,
            json!({ "KeyUsage": "SIGN_VERIFY", "KeySpec": "ECC_NIST_P256" }),
        );

        let message_b64 = base64::engine::general_purpose::STANDARD.encode(b"ecc data");
        let req = make_request(
            "Sign",
            json!({
                "KeyId": key_id,
                "Message": message_b64,
                "SigningAlgorithm": "ECDSA_SHA_256"
            }),
        );
        let resp = svc.sign(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["Signature"].as_str().is_some());
        assert_eq!(body["SigningAlgorithm"].as_str().unwrap(), "ECDSA_SHA_256");
    }

    #[test]
    fn sign_wrong_key_usage_fails() {
        let svc = make_service();
        let key_id = create_key(&svc); // ENCRYPT_DECRYPT

        let message_b64 = base64::engine::general_purpose::STANDARD.encode(b"test");
        let req = make_request(
            "Sign",
            json!({
                "KeyId": key_id,
                "Message": message_b64,
                "SigningAlgorithm": "RSASSA_PKCS1_V1_5_SHA_256"
            }),
        );
        assert!(svc.sign(&req).is_err());
    }

    #[test]
    fn generate_random_various_lengths() {
        let svc = make_service();

        for num_bytes in [1, 16, 32, 64, 256, 1024] {
            let req = make_request("GenerateRandom", json!({ "NumberOfBytes": num_bytes }));
            let resp = svc.generate_random(&req).unwrap();
            let body: Value = serde_json::from_slice(&resp.body).unwrap();
            let b64 = body["Plaintext"].as_str().unwrap();
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(b64)
                .unwrap();
            assert_eq!(
                decoded.len(),
                num_bytes as usize,
                "GenerateRandom({num_bytes}) returned wrong length"
            );
        }
    }

    #[test]
    fn generate_random_zero_bytes_fails() {
        let svc = make_service();
        let req = make_request("GenerateRandom", json!({ "NumberOfBytes": 0 }));
        assert!(svc.generate_random(&req).is_err());
    }

    #[test]
    fn generate_random_too_many_bytes_fails() {
        let svc = make_service();
        let req = make_request("GenerateRandom", json!({ "NumberOfBytes": 1025 }));
        assert!(svc.generate_random(&req).is_err());
    }

    #[test]
    fn generate_mac_verify_mac_roundtrip() {
        let svc = make_service();
        let key_id = create_key_with_opts(
            &svc,
            json!({ "KeyUsage": "GENERATE_VERIFY_MAC", "KeySpec": "HMAC_256" }),
        );

        let message_b64 = base64::engine::general_purpose::STANDARD.encode(b"mac message");

        // Generate MAC
        let req = make_request(
            "GenerateMac",
            json!({
                "KeyId": key_id,
                "Message": message_b64,
                "MacAlgorithm": "HMAC_SHA_256"
            }),
        );
        let resp = svc.generate_mac(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let mac = body["Mac"].as_str().unwrap().to_string();
        assert!(!mac.is_empty());

        // Verify MAC
        let req = make_request(
            "VerifyMac",
            json!({
                "KeyId": key_id,
                "Message": message_b64,
                "Mac": mac,
                "MacAlgorithm": "HMAC_SHA_256"
            }),
        );
        let resp = svc.verify_mac(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["MacValid"].as_bool().unwrap());
    }

    #[test]
    fn verify_mac_wrong_mac_fails() {
        let svc = make_service();
        let key_id = create_key_with_opts(
            &svc,
            json!({ "KeyUsage": "GENERATE_VERIFY_MAC", "KeySpec": "HMAC_256" }),
        );

        let message_b64 = base64::engine::general_purpose::STANDARD.encode(b"msg");
        let wrong_mac = base64::engine::general_purpose::STANDARD.encode(b"wrong-mac");

        let req = make_request(
            "VerifyMac",
            json!({
                "KeyId": key_id,
                "Message": message_b64,
                "Mac": wrong_mac,
                "MacAlgorithm": "HMAC_SHA_256"
            }),
        );
        assert!(svc.verify_mac(&req).is_err());
    }

    #[test]
    fn generate_mac_wrong_key_usage_fails() {
        let svc = make_service();
        let key_id = create_key(&svc); // ENCRYPT_DECRYPT

        let message_b64 = base64::engine::general_purpose::STANDARD.encode(b"msg");
        let req = make_request(
            "GenerateMac",
            json!({
                "KeyId": key_id,
                "Message": message_b64,
                "MacAlgorithm": "HMAC_SHA_256"
            }),
        );
        assert!(svc.generate_mac(&req).is_err());
    }

    #[test]
    fn re_encrypt_between_keys() {
        let svc = make_service();
        let key_a = create_key(&svc);
        let key_b = create_key(&svc);

        // Encrypt with key A
        let plaintext = b"re-encrypt test data";
        let plaintext_b64 = base64::engine::general_purpose::STANDARD.encode(plaintext);
        let req = make_request(
            "Encrypt",
            json!({ "KeyId": key_a, "Plaintext": plaintext_b64 }),
        );
        let resp = svc.encrypt(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let ciphertext_a = body["CiphertextBlob"].as_str().unwrap().to_string();

        // Re-encrypt from key A to key B
        let req = make_request(
            "ReEncrypt",
            json!({
                "CiphertextBlob": ciphertext_a,
                "DestinationKeyId": key_b
            }),
        );
        let resp = svc.re_encrypt(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let ciphertext_b = body["CiphertextBlob"].as_str().unwrap().to_string();
        assert_ne!(ciphertext_a, ciphertext_b);
        assert!(body["KeyId"].as_str().unwrap().contains(&key_b));
        assert!(body["SourceKeyId"].as_str().unwrap().contains(&key_a));

        // Decrypt with key B (the ciphertext is self-describing in fakecloud)
        let req = make_request("Decrypt", json!({ "CiphertextBlob": ciphertext_b }));
        let resp = svc.decrypt(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let decrypted_b64 = body["Plaintext"].as_str().unwrap();
        let decrypted = base64::engine::general_purpose::STANDARD
            .decode(decrypted_b64)
            .unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn update_alias_points_to_different_key() {
        let svc = make_service();
        let key_a = create_key(&svc);
        let key_b = create_key(&svc);

        // Create alias pointing to key A
        let req = make_request(
            "CreateAlias",
            json!({ "AliasName": "alias/switchable", "TargetKeyId": key_a }),
        );
        svc.create_alias(&req).unwrap();

        // Verify alias points to key A
        {
            let state = svc.state.read();
            let alias = state.aliases.get("alias/switchable").unwrap();
            assert_eq!(alias.target_key_id, key_a);
        }

        // Update alias to point to key B
        let req = make_request(
            "UpdateAlias",
            json!({ "AliasName": "alias/switchable", "TargetKeyId": key_b }),
        );
        svc.update_alias(&req).unwrap();

        // Verify alias now points to key B
        {
            let state = svc.state.read();
            let alias = state.aliases.get("alias/switchable").unwrap();
            assert_eq!(alias.target_key_id, key_b);
        }
    }

    #[test]
    fn update_key_description_changes_description() {
        let svc = make_service();
        let key_id = create_key(&svc);

        // Initially empty description
        {
            let state = svc.state.read();
            let key = state.keys.get(&key_id).unwrap();
            assert_eq!(key.description, "");
        }

        // Update description
        let req = make_request(
            "UpdateKeyDescription",
            json!({ "KeyId": key_id, "Description": "new description" }),
        );
        svc.update_key_description(&req).unwrap();

        {
            let state = svc.state.read();
            let key = state.keys.get(&key_id).unwrap();
            assert_eq!(key.description, "new description");
        }

        // Update again
        let req = make_request(
            "UpdateKeyDescription",
            json!({ "KeyId": key_id, "Description": "updated again" }),
        );
        svc.update_key_description(&req).unwrap();

        {
            let state = svc.state.read();
            let key = state.keys.get(&key_id).unwrap();
            assert_eq!(key.description, "updated again");
        }
    }

    #[test]
    fn get_public_key_for_asymmetric_key() {
        let svc = make_service();

        // RSA signing key
        let key_id = create_key_with_opts(
            &svc,
            json!({ "KeyUsage": "SIGN_VERIFY", "KeySpec": "RSA_2048" }),
        );

        let req = make_request("GetPublicKey", json!({ "KeyId": key_id }));
        let resp = svc.get_public_key(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();

        assert!(body["PublicKey"].as_str().is_some());
        assert_eq!(body["KeySpec"].as_str().unwrap(), "RSA_2048");
        assert_eq!(body["KeyUsage"].as_str().unwrap(), "SIGN_VERIFY");
        assert!(body["SigningAlgorithms"].as_array().is_some());
        assert!(body["KeyId"].as_str().unwrap().contains(":key/"));

        // ECC key
        let ecc_key_id = create_key_with_opts(
            &svc,
            json!({ "KeyUsage": "SIGN_VERIFY", "KeySpec": "ECC_NIST_P256" }),
        );

        let req = make_request("GetPublicKey", json!({ "KeyId": ecc_key_id }));
        let resp = svc.get_public_key(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["PublicKey"].as_str().is_some());
        assert_eq!(body["KeySpec"].as_str().unwrap(), "ECC_NIST_P256");
    }
}
