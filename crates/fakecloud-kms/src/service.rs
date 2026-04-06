use std::collections::HashMap;

use async_trait::async_trait;
use base64::Engine;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{KeyRotation, KmsAlias, KmsGrant, KmsKey, SharedKmsState};

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
        ]
    }
}

fn body_json(req: &AwsRequest) -> Value {
    serde_json::from_slice(&req.body).unwrap_or(Value::Null)
}

fn default_key_policy(account_id: &str) -> String {
    serde_json::to_string(&json!({
        "Version": "2012-10-17",
        "Id": "key-default-1",
        "Statement": [
            {
                "Sid": "Enable IAM User Permissions",
                "Effect": "Allow",
                "Principal": {"AWS": format!("arn:aws:iam::{account_id}:root")},
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
        let body = body_json(req);

        validate_optional_string_length(
            "customKeyStoreId",
            body["CustomKeyStoreId"].as_str(),
            1,
            64,
        )?;

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
        };

        let metadata = key_metadata_json(&key, &state.account_id);
        state.keys.insert(key_id, key);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "KeyMetadata": metadata })).unwrap(),
        ))
    }

    fn describe_key(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
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
        let body = body_json(req);

        validate_optional_range_i64("limit", body["Limit"].as_i64(), 1, 1000)?;

        let limit = body["Limit"].as_u64().unwrap_or(1000) as usize;
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
        let body = body_json(req);
        let resolved = self.resolve_required_key(&body)?;

        let mut state = self.state.write();
        let key = state.keys.get_mut(&resolved).unwrap();
        key.enabled = true;
        key.key_state = "Enabled".to_string();

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn disable_key(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let resolved = self.resolve_required_key(&body)?;

        let mut state = self.state.write();
        let key = state.keys.get_mut(&resolved).unwrap();
        key.enabled = false;
        key.key_state = "Disabled".to_string();

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn schedule_key_deletion(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let resolved = self.resolve_required_key(&body)?;
        let pending_days = body["PendingWindowInDays"].as_i64().unwrap_or(30);

        let mut state = self.state.write();
        let key = state.keys.get_mut(&resolved).unwrap();
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
        let body = body_json(req);
        let resolved = self.resolve_required_key(&body)?;

        let mut state = self.state.write();
        let key = state.keys.get_mut(&resolved).unwrap();
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
        let body = body_json(req);
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
        let key = state.keys.get(&resolved).unwrap();
        if !key.enabled {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DisabledException",
                format!("Key '{}' is disabled", key.arn),
            ));
        }

        // Fake encryption: prefix + key_id + ":" + base64(plaintext_bytes)
        let envelope = format!("{FAKE_ENVELOPE_PREFIX}{}:{plaintext_b64}", key.key_id);
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
        let body = body_json(req);
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

        if !envelope.starts_with(FAKE_ENVELOPE_PREFIX) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidCiphertextException",
                "The ciphertext is not a valid FakeCloud KMS ciphertext",
            ));
        }

        let rest = &envelope[FAKE_ENVELOPE_PREFIX.len()..];
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
    }

    fn re_encrypt(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
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

        if !envelope.starts_with(FAKE_ENVELOPE_PREFIX) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidCiphertextException",
                "The ciphertext is invalid",
            ));
        }

        let rest = &envelope[FAKE_ENVELOPE_PREFIX.len()..];
        let (source_key_id, plaintext_b64) = rest.split_once(':').ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidCiphertextException",
                "The ciphertext is invalid",
            )
        })?;

        let state = self.state.read();

        let source_key = state.keys.get(source_key_id).ok_or_else(|| {
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

        let dest_key = state.keys.get(&dest_resolved).unwrap();

        // Re-encrypt with destination key
        let new_envelope = format!("{FAKE_ENVELOPE_PREFIX}{}:{plaintext_b64}", dest_key.key_id);
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
        let body = body_json(req);
        let key_id = Self::require_key_id(&body)?;

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let state = self.state.read();
        let key = state.keys.get(&resolved).unwrap();
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
        let body = body_json(req);
        let key_id = Self::require_key_id(&body)?;

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let state = self.state.read();
        let key = state.keys.get(&resolved).unwrap();
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
        let body = body_json(req);

        // CustomKeyStoreId is accepted for API compatibility but has no effect on
        // random number generation in this emulator.
        validate_optional_string_length(
            "customKeyStoreId",
            body["CustomKeyStoreId"].as_str(),
            1,
            64,
        )?;

        let num_bytes = body["NumberOfBytes"].as_u64().unwrap_or(32) as usize;

        if num_bytes > 1024 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "1 validation error detected: Value '{num_bytes}' at 'numberOfBytes' failed to satisfy constraint: Member must have value less than or equal to 1024"
                ),
            ));
        }

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
        let body = body_json(req);
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
        let body = body_json(req);
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
        let body = body_json(req);
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
        let body = body_json(req);

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
        let body = body_json(req);
        let key_id = Self::require_key_id(&body)?;

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Invalid keyId {key_id}"),
            )
        })?;

        let tags = body["Tags"].as_array();

        let mut state = self.state.write();
        let key = state.keys.get_mut(&resolved).unwrap();
        if let Some(tags) = tags {
            for tag in tags {
                if let (Some(k), Some(v)) = (tag["TagKey"].as_str(), tag["TagValue"].as_str()) {
                    key.tags.insert(k.to_string(), v.to_string());
                }
            }
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let key_id = Self::require_key_id(&body)?;

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Invalid keyId {key_id}"),
            )
        })?;

        let tag_keys = body["TagKeys"].as_array();

        let mut state = self.state.write();
        let key = state.keys.get_mut(&resolved).unwrap();
        if let Some(tag_keys) = tag_keys {
            for tag_key in tag_keys {
                if let Some(k) = tag_key.as_str() {
                    key.tags.remove(k);
                }
            }
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_resource_tags(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let key_id = Self::require_key_id(&body)?;

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Invalid keyId {key_id}"),
            )
        })?;

        let state = self.state.read();
        let key = state.keys.get(&resolved).unwrap();
        let mut sorted_tags: Vec<(&String, &String)> = key.tags.iter().collect();
        sorted_tags.sort_by_key(|(k, _)| (*k).clone());
        let tags: Vec<Value> = sorted_tags
            .iter()
            .map(|(k, v)| {
                json!({
                    "TagKey": k,
                    "TagValue": v,
                })
            })
            .collect();

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
        let body = body_json(req);
        let resolved = self.resolve_required_key(&body)?;
        let description = body["Description"].as_str().unwrap_or("").to_string();

        let mut state = self.state.write();
        let key = state.keys.get_mut(&resolved).unwrap();
        key.description = description;

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn get_key_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
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
        let key = state.keys.get(&resolved).unwrap();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "Policy": key.policy,
            }))
            .unwrap(),
        ))
    }

    fn put_key_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
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
        let key = state.keys.get_mut(&resolved).unwrap();
        key.policy = policy;

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_key_policies(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
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
        let body = body_json(req);
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
        let key = state.keys.get(&resolved).unwrap();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "KeyRotationEnabled": key.key_rotation_enabled,
            }))
            .unwrap(),
        ))
    }

    fn enable_key_rotation(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
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
        let key = state.keys.get_mut(&resolved).unwrap();
        key.key_rotation_enabled = true;

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn disable_key_rotation(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
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
        let key = state.keys.get_mut(&resolved).unwrap();
        key.key_rotation_enabled = false;

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn rotate_key_on_demand(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let resolved = self.resolve_required_key(&body)?;

        let mut state = self.state.write();
        let key = state.keys.get_mut(&resolved).unwrap();

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
        let body = body_json(req);
        let resolved = self.resolve_required_key(&body)?;
        let limit = body["Limit"].as_u64().unwrap_or(1000) as usize;
        let marker = body["Marker"].as_str();

        let state = self.state.read();
        let key = state.keys.get(&resolved).unwrap();

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
        let body = body_json(req);
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
        let key = state.keys.get(&resolved).unwrap();

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
        let body = body_json(req);
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
        let key = state.keys.get(&resolved).unwrap();

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
        let body = body_json(req);
        let key_id = Self::require_key_id(&body)?;

        let resolved = self.resolve_key_id(&key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let state = self.state.read();
        let key = state.keys.get(&resolved).unwrap();

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
        let body = body_json(req);
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
        let body = body_json(req);
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
        let body = body_json(req);

        validate_required("RetiringPrincipal", &body["RetiringPrincipal"])?;
        let retiring_principal = body["RetiringPrincipal"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "RetiringPrincipal must be a string",
            )
        })?;
        validate_optional_range_i64("limit", body["Limit"].as_i64(), 1, 1000)?;

        let limit = body["Limit"].as_u64().unwrap_or(1000) as usize;
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
        let body = body_json(req);
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
        let body = body_json(req);
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
        let body = body_json(req);
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
        let key = state.keys.get(&resolved).unwrap();

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
        let body = body_json(req);
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
        let key = state.keys.get(&resolved).unwrap();

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
        let body = body_json(req);
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
        let source_key = state.keys.get(&resolved).unwrap();
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
}
