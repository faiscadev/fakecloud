use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::{Secret, SecretVersion, SharedSecretsManagerState};

pub struct SecretsManagerService {
    state: SharedSecretsManagerState,
}

impl SecretsManagerService {
    pub fn new(state: SharedSecretsManagerState) -> Self {
        Self { state }
    }

    fn create_secret(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "Name is required",
                )
            })?
            .to_string();

        let mut state = self.state.write();

        if state.secrets.contains_key(&name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceExistsException",
                format!("The operation failed because the secret {name} already exists."),
            ));
        }

        let arn = format!(
            "arn:aws:secretsmanager:{}:{}:secret:{}-{}",
            state.region,
            state.account_id,
            name,
            &uuid::Uuid::new_v4().to_string()[..6]
        );

        let version_id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now();

        let secret_string = body["SecretString"].as_str().map(|s| s.to_string());
        let secret_binary = body["SecretBinary"].as_str().and_then(base64_decode);

        let description = body["Description"].as_str().unwrap_or("").to_string();
        let kms_key_id = body["KmsKeyId"].as_str().map(|s| s.to_string());

        let tags = body["Tags"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| {
                        let key = t["Key"].as_str()?;
                        let value = t["Value"].as_str()?;
                        Some((key.to_string(), value.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let version = SecretVersion {
            version_id: version_id.clone(),
            secret_string,
            secret_binary,
            stages: vec!["AWSCURRENT".to_string()],
            created_at: now,
        };

        let mut versions = std::collections::HashMap::new();
        versions.insert(version_id.clone(), version);

        let secret = Secret {
            name: name.clone(),
            arn: arn.clone(),
            description,
            kms_key_id,
            versions,
            current_version_id: version_id.clone(),
            tags,
            deleted: false,
            deletion_date: None,
            created_at: now,
            last_changed_at: now,
            last_accessed_at: None,
        };

        state.secrets.insert(name.clone(), secret);

        let response = json!({
            "ARN": arn,
            "Name": name,
            "VersionId": version_id,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn get_secret_value(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let secret_id = body["SecretId"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "SecretId is required",
                )
            })?
            .to_string();

        let mut state = self.state.write();
        let secret = self.find_secret_mut(&mut state, &secret_id)?;

        if secret.deleted {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequestException",
                "You can't perform this operation on the secret because it was marked for deletion.",
            ));
        }

        secret.last_accessed_at = Some(Utc::now());

        // Determine which version to return
        let version_id = body["VersionId"]
            .as_str()
            .map(|s| s.to_string())
            .or_else(|| {
                body["VersionStage"].as_str().and_then(|stage| {
                    secret
                        .versions
                        .iter()
                        .find(|(_, v)| v.stages.contains(&stage.to_string()))
                        .map(|(id, _)| id.clone())
                })
            })
            .unwrap_or_else(|| secret.current_version_id.clone());

        let version = secret.versions.get(&version_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Secrets Manager can't find the specified secret version: {version_id}"),
            )
        })?;

        let mut response = json!({
            "ARN": secret.arn,
            "Name": secret.name,
            "VersionId": version.version_id,
            "VersionStages": version.stages,
            "CreatedDate": version.created_at.timestamp_millis() as f64 / 1000.0,
        });

        if let Some(ref s) = version.secret_string {
            response["SecretString"] = json!(s);
        }
        if let Some(ref b) = version.secret_binary {
            response["SecretBinary"] = json!(base64_encode(b));
        }

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn put_secret_value(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let secret_id = body["SecretId"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "SecretId is required",
                )
            })?
            .to_string();

        let mut state = self.state.write();
        let secret = self.find_secret_mut(&mut state, &secret_id)?;

        if secret.deleted {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequestException",
                "You can't perform this operation on the secret because it was marked for deletion.",
            ));
        }

        let now = Utc::now();
        let version_id = body["ClientRequestToken"]
            .as_str()
            .map(|s| s.to_string())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let secret_string = body["SecretString"].as_str().map(|s| s.to_string());
        let secret_binary = body["SecretBinary"].as_str().and_then(base64_decode);

        let version_stages: Vec<String> = body["VersionStages"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_else(|| vec!["AWSCURRENT".to_string()]);

        // Move AWSCURRENT from old version to AWSPREVIOUS
        if version_stages.contains(&"AWSCURRENT".to_string()) {
            let old_version_id = secret.current_version_id.clone();
            if let Some(old_version) = secret.versions.get_mut(&old_version_id) {
                old_version.stages.retain(|s| s != "AWSCURRENT");
                if !old_version.stages.contains(&"AWSPREVIOUS".to_string()) {
                    old_version.stages.push("AWSPREVIOUS".to_string());
                }
            }
            // Remove AWSPREVIOUS from any other version
            for (id, v) in secret.versions.iter_mut() {
                if *id != old_version_id {
                    v.stages.retain(|s| s != "AWSPREVIOUS");
                }
            }
        }

        let version = SecretVersion {
            version_id: version_id.clone(),
            secret_string,
            secret_binary,
            stages: version_stages,
            created_at: now,
        };

        secret.versions.insert(version_id.clone(), version);
        secret.current_version_id = version_id.clone();
        secret.last_changed_at = now;

        let response = json!({
            "ARN": secret.arn,
            "Name": secret.name,
            "VersionId": version_id,
            "VersionStages": ["AWSCURRENT"],
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn update_secret(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let secret_id = body["SecretId"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "SecretId is required",
                )
            })?
            .to_string();

        let mut state = self.state.write();
        let secret = self.find_secret_mut(&mut state, &secret_id)?;

        if secret.deleted {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequestException",
                "You can't perform this operation on the secret because it was marked for deletion.",
            ));
        }

        if let Some(desc) = body["Description"].as_str() {
            secret.description = desc.to_string();
        }
        if let Some(kms) = body["KmsKeyId"].as_str() {
            secret.kms_key_id = Some(kms.to_string());
        }

        // If SecretString or SecretBinary is provided, create a new version
        let secret_string = body["SecretString"].as_str().map(|s| s.to_string());
        let secret_binary = body["SecretBinary"].as_str().and_then(base64_decode);

        let version_id = if secret_string.is_some() || secret_binary.is_some() {
            let vid = body["ClientRequestToken"]
                .as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let now = Utc::now();

            // Move AWSCURRENT -> AWSPREVIOUS on old version
            let old_vid = secret.current_version_id.clone();
            if let Some(old_v) = secret.versions.get_mut(&old_vid) {
                old_v.stages.retain(|s| s != "AWSCURRENT");
                if !old_v.stages.contains(&"AWSPREVIOUS".to_string()) {
                    old_v.stages.push("AWSPREVIOUS".to_string());
                }
            }

            let version = SecretVersion {
                version_id: vid.clone(),
                secret_string,
                secret_binary,
                stages: vec!["AWSCURRENT".to_string()],
                created_at: now,
            };
            secret.versions.insert(vid.clone(), version);
            secret.current_version_id = vid.clone();
            secret.last_changed_at = now;
            vid
        } else {
            secret.current_version_id.clone()
        };

        let response = json!({
            "ARN": secret.arn,
            "Name": secret.name,
            "VersionId": version_id,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn delete_secret(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let secret_id = body["SecretId"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "SecretId is required",
                )
            })?
            .to_string();

        let force_delete = body["ForceDeleteWithoutRecovery"]
            .as_bool()
            .unwrap_or(false);
        let recovery_window = body["RecoveryWindowInDays"].as_i64().unwrap_or(30);

        let mut state = self.state.write();

        if force_delete {
            let secret = self.find_secret_mut(&mut state, &secret_id)?;
            let arn = secret.arn.clone();
            let name = secret.name.clone();
            let deletion_date = Utc::now();
            state.secrets.remove(&name);
            let response = json!({
                "ARN": arn,
                "Name": name,
                "DeletionDate": deletion_date.timestamp_millis() as f64 / 1000.0,
            });
            return Ok(AwsResponse::json(StatusCode::OK, response.to_string()));
        }

        let secret = self.find_secret_mut(&mut state, &secret_id)?;

        if secret.deleted {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequestException",
                "You can't perform this operation on the secret because it was already scheduled for deletion.",
            ));
        }

        let now = Utc::now();
        let deletion_date = now + chrono::Duration::days(recovery_window);
        secret.deleted = true;
        secret.deletion_date = Some(deletion_date);

        let response = json!({
            "ARN": secret.arn,
            "Name": secret.name,
            "DeletionDate": deletion_date.timestamp_millis() as f64 / 1000.0,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn restore_secret(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let secret_id = body["SecretId"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "SecretId is required",
                )
            })?
            .to_string();

        let mut state = self.state.write();
        let secret = self.find_secret_mut(&mut state, &secret_id)?;

        if !secret.deleted {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequestException",
                "You can't perform this operation on the secret because it was not marked for deletion.",
            ));
        }

        secret.deleted = false;
        secret.deletion_date = None;

        let response = json!({
            "ARN": secret.arn,
            "Name": secret.name,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn describe_secret(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let secret_id = body["SecretId"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "SecretId is required",
                )
            })?
            .to_string();

        let state = self.state.read();
        let secret = self.find_secret_ref(&state, &secret_id)?;

        let mut version_ids_to_stages: serde_json::Map<String, Value> = serde_json::Map::new();
        for (vid, version) in &secret.versions {
            version_ids_to_stages.insert(vid.clone(), json!(version.stages));
        }

        let mut response = json!({
            "ARN": secret.arn,
            "Name": secret.name,
            "Description": secret.description,
            "CreatedDate": secret.created_at.timestamp_millis() as f64 / 1000.0,
            "LastChangedDate": secret.last_changed_at.timestamp_millis() as f64 / 1000.0,
            "VersionIdsToStages": version_ids_to_stages,
            "Tags": secret.tags.iter().map(|(k, v)| json!({"Key": k, "Value": v})).collect::<Vec<_>>(),
        });

        if let Some(ref kms) = secret.kms_key_id {
            response["KmsKeyId"] = json!(kms);
        }
        if secret.deleted {
            response["DeletedDate"] = json!(secret
                .deletion_date
                .map(|d| d.timestamp_millis() as f64 / 1000.0));
        }
        if let Some(accessed) = secret.last_accessed_at {
            response["LastAccessedDate"] = json!(accessed.timestamp_millis() as f64 / 1000.0);
        }

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn list_secrets(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let max_results = body["MaxResults"].as_i64().unwrap_or(100) as usize;
        let next_token = body["NextToken"].as_str();

        let state = self.state.read();

        let mut secrets: Vec<&Secret> = state.secrets.values().collect();
        secrets.sort_by(|a, b| a.name.cmp(&b.name));

        // Simple pagination with name-based token
        let start_idx = if let Some(token) = next_token {
            secrets.iter().position(|s| s.name == token).unwrap_or(0)
        } else {
            0
        };

        let page: Vec<Value> = secrets
            .iter()
            .skip(start_idx)
            .take(max_results)
            .map(|s| {
                let mut version_ids_to_stages: serde_json::Map<String, Value> =
                    serde_json::Map::new();
                for (vid, version) in &s.versions {
                    version_ids_to_stages.insert(vid.clone(), json!(version.stages));
                }

                let mut entry = json!({
                    "ARN": s.arn,
                    "Name": s.name,
                    "Description": s.description,
                    "CreatedDate": s.created_at.timestamp_millis() as f64 / 1000.0,
                    "LastChangedDate": s.last_changed_at.timestamp_millis() as f64 / 1000.0,
                    "SecretVersionsToStages": version_ids_to_stages,
                    "Tags": s.tags.iter().map(|(k, v)| json!({"Key": k, "Value": v})).collect::<Vec<_>>(),
                });

                if let Some(ref kms) = s.kms_key_id {
                    entry["KmsKeyId"] = json!(kms);
                }
                if s.deleted {
                    entry["DeletedDate"] = json!(
                        s.deletion_date
                            .map(|d| d.timestamp_millis() as f64 / 1000.0)
                    );
                }

                entry
            })
            .collect();

        let has_more = start_idx + max_results < secrets.len();
        let mut response = json!({
            "SecretList": page,
        });
        if has_more {
            if let Some(next) = secrets.get(start_idx + max_results) {
                response["NextToken"] = json!(next.name);
            }
        }

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let secret_id = body["SecretId"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "SecretId is required",
                )
            })?
            .to_string();

        let tags: Vec<(String, String)> = body["Tags"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| {
                        let key = t["Key"].as_str()?;
                        let value = t["Value"].as_str()?;
                        Some((key.to_string(), value.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let mut state = self.state.write();
        let secret = self.find_secret_mut(&mut state, &secret_id)?;

        for (k, v) in tags {
            secret.tags.insert(k, v);
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let secret_id = body["SecretId"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "SecretId is required",
                )
            })?
            .to_string();

        let tag_keys: Vec<String> = body["TagKeys"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let mut state = self.state.write();
        let secret = self.find_secret_mut(&mut state, &secret_id)?;

        for key in &tag_keys {
            secret.tags.remove(key);
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_secret_version_ids(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let secret_id = body["SecretId"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "SecretId is required",
                )
            })?
            .to_string();

        let state = self.state.read();
        let secret = self.find_secret_ref(&state, &secret_id)?;

        let versions: Vec<Value> = secret
            .versions
            .values()
            .map(|v| {
                json!({
                    "VersionId": v.version_id,
                    "VersionStages": v.stages,
                    "CreatedDate": v.created_at.timestamp_millis() as f64 / 1000.0,
                })
            })
            .collect();

        let response = json!({
            "ARN": secret.arn,
            "Name": secret.name,
            "Versions": versions,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    /// Find a secret by name or ARN (mutable).
    fn find_secret_mut<'a>(
        &self,
        state: &'a mut crate::state::SecretsManagerState,
        secret_id: &str,
    ) -> Result<&'a mut Secret, AwsServiceError> {
        // Try by name first, then by ARN
        if state.secrets.contains_key(secret_id) {
            return Ok(state.secrets.get_mut(secret_id).unwrap());
        }

        // Search by ARN
        for secret in state.secrets.values_mut() {
            if secret.arn == secret_id {
                return Ok(secret);
            }
        }

        Err(AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            "Secrets Manager can't find the specified secret. (Service: SecretsManager, Status Code: 404)",
        ))
    }

    /// Find a secret by name or ARN (immutable).
    fn find_secret_ref<'a>(
        &self,
        state: &'a crate::state::SecretsManagerState,
        secret_id: &str,
    ) -> Result<&'a Secret, AwsServiceError> {
        if let Some(secret) = state.secrets.get(secret_id) {
            return Ok(secret);
        }

        for secret in state.secrets.values() {
            if secret.arn == secret_id {
                return Ok(secret);
            }
        }

        Err(AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            "Secrets Manager can't find the specified secret. (Service: SecretsManager, Status Code: 404)",
        ))
    }
}

#[async_trait]
impl AwsService for SecretsManagerService {
    fn service_name(&self) -> &str {
        "secretsmanager"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match req.action.as_str() {
            "CreateSecret" => self.create_secret(&req),
            "GetSecretValue" => self.get_secret_value(&req),
            "PutSecretValue" => self.put_secret_value(&req),
            "UpdateSecret" => self.update_secret(&req),
            "DeleteSecret" => self.delete_secret(&req),
            "RestoreSecret" => self.restore_secret(&req),
            "DescribeSecret" => self.describe_secret(&req),
            "ListSecrets" => self.list_secrets(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListSecretVersionIds" => self.list_secret_version_ids(&req),
            "GetResourcePolicy" => Ok(AwsResponse::json(
                StatusCode::OK,
                r#"{"ARN":null,"Name":null}"#,
            )),
            _ => Err(AwsServiceError::action_not_implemented(
                "secretsmanager",
                &req.action,
            )),
        }
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            "CreateSecret",
            "GetSecretValue",
            "PutSecretValue",
            "UpdateSecret",
            "DeleteSecret",
            "RestoreSecret",
            "DescribeSecret",
            "ListSecrets",
            "TagResource",
            "UntagResource",
            "ListSecretVersionIds",
        ]
    }
}

fn base64_decode(input: &str) -> Option<Vec<u8>> {
    let table = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut buf = Vec::new();
    let mut bits: u32 = 0;
    let mut count = 0;
    for &b in input.as_bytes() {
        if b == b'=' || b == b'\n' || b == b'\r' {
            continue;
        }
        let val = table.iter().position(|&c| c == b)? as u32;
        bits = (bits << 6) | val;
        count += 1;
        if count == 4 {
            buf.push((bits >> 16) as u8);
            buf.push((bits >> 8) as u8);
            buf.push(bits as u8);
            bits = 0;
            count = 0;
        }
    }
    match count {
        2 => {
            bits <<= 12;
            buf.push((bits >> 16) as u8);
        }
        3 => {
            bits <<= 6;
            buf.push((bits >> 16) as u8);
            buf.push((bits >> 8) as u8);
        }
        _ => {}
    }
    Some(buf)
}

fn base64_encode(input: &[u8]) -> String {
    let table = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::new();
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        result.push(table[((triple >> 18) & 0x3F) as usize] as char);
        result.push(table[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(table[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(table[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::SecretsManagerState;
    use bytes::Bytes;
    use http::{HeaderMap, Method};
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_state() -> SharedSecretsManagerState {
        Arc::new(RwLock::new(SecretsManagerState::new(
            "123456789012",
            "us-east-1",
        )))
    }

    fn make_request(action: &str, body: &str) -> AwsRequest {
        AwsRequest {
            service: "secretsmanager".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test-request-id".to_string(),
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: Bytes::from(body.to_string()),
            path_segments: vec![],
            raw_path: "/".to_string(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
        }
    }

    #[tokio::test]
    async fn test_create_and_get_secret() {
        let state = make_state();
        let svc = SecretsManagerService::new(state);

        let req = make_request(
            "CreateSecret",
            r#"{"Name": "test/secret", "SecretString": "mysecretvalue"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Name"], "test/secret");
        assert!(body["ARN"].as_str().unwrap().contains("test/secret"));

        let req = make_request("GetSecretValue", r#"{"SecretId": "test/secret"}"#);
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["SecretString"], "mysecretvalue");
    }

    #[tokio::test]
    async fn test_put_secret_value_creates_version() {
        let state = make_state();
        let svc = SecretsManagerService::new(state);

        let req = make_request(
            "CreateSecret",
            r#"{"Name": "versioned", "SecretString": "v1"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            "PutSecretValue",
            r#"{"SecretId": "versioned", "SecretString": "v2"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Name"], "versioned");

        // Get should return v2
        let req = make_request("GetSecretValue", r#"{"SecretId": "versioned"}"#);
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["SecretString"], "v2");
    }

    #[tokio::test]
    async fn test_delete_and_restore_secret() {
        let state = make_state();
        let svc = SecretsManagerService::new(state);

        let req = make_request(
            "CreateSecret",
            r#"{"Name": "deleteme", "SecretString": "value"}"#,
        );
        svc.handle(req).await.unwrap();

        // Delete (soft)
        let req = make_request("DeleteSecret", r#"{"SecretId": "deleteme"}"#);
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["DeletionDate"].as_f64().is_some());

        // GetSecretValue should fail
        let req = make_request("GetSecretValue", r#"{"SecretId": "deleteme"}"#);
        assert!(svc.handle(req).await.is_err());

        // Restore
        let req = make_request("RestoreSecret", r#"{"SecretId": "deleteme"}"#);
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // GetSecretValue should work again
        let req = make_request("GetSecretValue", r#"{"SecretId": "deleteme"}"#);
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["SecretString"], "value");
    }

    #[tokio::test]
    async fn test_list_secrets() {
        let state = make_state();
        let svc = SecretsManagerService::new(state);

        for name in &["alpha", "beta", "gamma"] {
            let req = make_request(
                "CreateSecret",
                &format!(r#"{{"Name": "{name}", "SecretString": "val"}}"#),
            );
            svc.handle(req).await.unwrap();
        }

        let req = make_request("ListSecrets", "{}");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["SecretList"].as_array().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn test_tags() {
        let state = make_state();
        let svc = SecretsManagerService::new(state);

        let req = make_request(
            "CreateSecret",
            r#"{"Name": "tagged", "SecretString": "val"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            "TagResource",
            r#"{"SecretId": "tagged", "Tags": [{"Key": "env", "Value": "prod"}]}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request("DescribeSecret", r#"{"SecretId": "tagged"}"#);
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let tags = body["Tags"].as_array().unwrap();
        assert!(tags
            .iter()
            .any(|t| t["Key"] == "env" && t["Value"] == "prod"));

        let req = make_request(
            "UntagResource",
            r#"{"SecretId": "tagged", "TagKeys": ["env"]}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request("DescribeSecret", r#"{"SecretId": "tagged"}"#);
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["Tags"].as_array().unwrap().is_empty());
    }
}
