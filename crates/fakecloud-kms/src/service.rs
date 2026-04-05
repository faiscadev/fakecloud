use std::collections::HashMap;

use async_trait::async_trait;
use base64::Engine;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::{KmsAlias, KmsKey, SharedKmsState};

const FAKE_ENVELOPE_PREFIX: &str = "fakecloud-kms:";

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
            "Encrypt" => self.encrypt(&req),
            "Decrypt" => self.decrypt(&req),
            "GenerateDataKey" => self.generate_data_key(&req),
            "GenerateDataKeyWithoutPlaintext" => self.generate_data_key_without_plaintext(&req),
            "CreateAlias" => self.create_alias(&req),
            "DeleteAlias" => self.delete_alias(&req),
            "ListAliases" => self.list_aliases(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListResourceTags" => self.list_resource_tags(&req),
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
            "Encrypt",
            "Decrypt",
            "GenerateDataKey",
            "GenerateDataKeyWithoutPlaintext",
            "CreateAlias",
            "DeleteAlias",
            "ListAliases",
            "TagResource",
            "UntagResource",
            "ListResourceTags",
        ]
    }
}

fn body_json(req: &AwsRequest) -> Value {
    serde_json::from_slice(&req.body).unwrap_or(Value::Null)
}

impl KmsService {
    fn resolve_key_id(&self, key_id_or_arn: &str) -> Option<String> {
        let state = self.state.read();

        // Direct key ID
        if state.keys.contains_key(key_id_or_arn) {
            return Some(key_id_or_arn.to_string());
        }

        // ARN
        if key_id_or_arn.starts_with("arn:aws:kms:") {
            if let Some(id) = key_id_or_arn.rsplit('/').next() {
                if state.keys.contains_key(id) {
                    return Some(id.to_string());
                }
            }
        }

        // Alias
        if key_id_or_arn.starts_with("alias/") {
            if let Some(alias) = state.aliases.get(key_id_or_arn) {
                return Some(alias.target_key_id.clone());
            }
        }

        None
    }

    fn create_key(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let description = body["Description"].as_str().unwrap_or("").to_string();
        let key_usage = body["KeyUsage"]
            .as_str()
            .unwrap_or("ENCRYPT_DECRYPT")
            .to_string();
        let key_spec = body["KeySpec"]
            .as_str()
            .unwrap_or("SYMMETRIC_DEFAULT")
            .to_string();

        let key_id = Uuid::new_v4().to_string();
        let mut state = self.state.write();
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
        };

        let metadata = key_metadata_json(&key);
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

        let resolved = self.resolve_key_id(key_id_input).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id_input}' does not exist"),
            )
        })?;

        let state = self.state.read();
        let key = state.keys.get(&resolved).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id_input}' does not exist"),
            )
        })?;

        let metadata = key_metadata_json(key);
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "KeyMetadata": metadata })).unwrap(),
        ))
    }

    fn list_keys(&self, _req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let keys: Vec<Value> = state
            .keys
            .values()
            .map(|k| {
                json!({
                    "KeyId": k.key_id,
                    "KeyArn": k.arn,
                })
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "Keys": keys,
                "Truncated": false,
            }))
            .unwrap(),
        ))
    }

    fn enable_key(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let key_id = body["KeyId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "KeyId is required",
            )
        })?;

        let resolved = self.resolve_key_id(key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let mut state = self.state.write();
        let key = state.keys.get_mut(&resolved).unwrap();
        key.enabled = true;
        key.key_state = "Enabled".to_string();

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn disable_key(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let key_id = body["KeyId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "KeyId is required",
            )
        })?;

        let resolved = self.resolve_key_id(key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let mut state = self.state.write();
        let key = state.keys.get_mut(&resolved).unwrap();
        key.enabled = false;
        key.key_state = "Disabled".to_string();

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn schedule_key_deletion(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let key_id = body["KeyId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "KeyId is required",
            )
        })?;
        let pending_days = body["PendingWindowInDays"].as_i64().unwrap_or(30);

        let resolved = self.resolve_key_id(key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

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

    fn encrypt(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let key_id = body["KeyId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "KeyId is required",
            )
        })?;
        let plaintext_b64 = body["Plaintext"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Plaintext is required",
            )
        })?;

        let resolved = self.resolve_key_id(key_id).ok_or_else(|| {
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

        // Fake encryption: prefix + key_id + ":" + plaintext
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

    fn generate_data_key(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let key_id = body["KeyId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "KeyId is required",
            )
        })?;

        let resolved = self.resolve_key_id(key_id).ok_or_else(|| {
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

        // Generate a fake 32-byte data key
        let data_key_bytes: Vec<u8> = (0..32).map(|_| rand_byte()).collect();
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
        let key_id = body["KeyId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "KeyId is required",
            )
        })?;

        let resolved = self.resolve_key_id(key_id).ok_or_else(|| {
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

        let data_key_bytes: Vec<u8> = (0..32).map(|_| rand_byte()).collect();
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

        let resolved = self.resolve_key_id(&target_key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{target_key_id}' does not exist"),
            )
        })?;

        let mut state = self.state.write();

        if state.aliases.contains_key(&alias_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AlreadyExistsException",
                format!("Alias '{alias_name}' already exists"),
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

        let mut state = self.state.write();
        if state.aliases.remove(alias_name).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Alias '{alias_name}' does not exist"),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_aliases(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let key_id_filter = body["KeyId"].as_str();

        let state = self.state.read();
        let aliases: Vec<Value> = state
            .aliases
            .values()
            .filter(|a| {
                if let Some(kid) = key_id_filter {
                    a.target_key_id == kid
                } else {
                    true
                }
            })
            .map(|a| {
                json!({
                    "AliasName": a.alias_name,
                    "AliasArn": a.alias_arn,
                    "TargetKeyId": a.target_key_id,
                    "CreationDate": a.creation_date,
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
        let key_id = body["KeyId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "KeyId is required",
            )
        })?;
        let tags = body["Tags"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Tags is required",
            )
        })?;

        let resolved = self.resolve_key_id(key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let mut state = self.state.write();
        let key = state.keys.get_mut(&resolved).unwrap();
        for tag in tags {
            if let (Some(k), Some(v)) = (tag["TagKey"].as_str(), tag["TagValue"].as_str()) {
                key.tags.insert(k.to_string(), v.to_string());
            }
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let key_id = body["KeyId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "KeyId is required",
            )
        })?;
        let tag_keys = body["TagKeys"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "TagKeys is required",
            )
        })?;

        let resolved = self.resolve_key_id(key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let mut state = self.state.write();
        let key = state.keys.get_mut(&resolved).unwrap();
        for tag_key in tag_keys {
            if let Some(k) = tag_key.as_str() {
                key.tags.remove(k);
            }
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_resource_tags(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let key_id = body["KeyId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "KeyId is required",
            )
        })?;

        let resolved = self.resolve_key_id(key_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Key '{key_id}' does not exist"),
            )
        })?;

        let state = self.state.read();
        let key = state.keys.get(&resolved).unwrap();
        let tags: Vec<Value> = key
            .tags
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
}

fn key_metadata_json(key: &KmsKey) -> Value {
    let mut meta = json!({
        "KeyId": key.key_id,
        "Arn": key.arn,
        "CreationDate": key.creation_date,
        "Description": key.description,
        "Enabled": key.enabled,
        "KeyUsage": key.key_usage,
        "KeySpec": key.key_spec,
        "KeyManager": key.key_manager,
        "KeyState": key.key_state,
        "Origin": "AWS_KMS",
        "EncryptionAlgorithms": ["SYMMETRIC_DEFAULT"],
    });
    if let Some(dd) = key.deletion_date {
        meta["DeletionDate"] = json!(dd);
    }
    meta
}

/// Simple pseudo-random byte generator using UUID (no extra deps needed).
fn rand_byte() -> u8 {
    let u = Uuid::new_v4();
    u.as_bytes()[0]
}
