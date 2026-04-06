use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{RotationRules, Secret, SecretVersion, SharedSecretsManagerState};

pub struct SecretsManagerService {
    state: SharedSecretsManagerState,
}

impl SecretsManagerService {
    pub fn new(state: SharedSecretsManagerState) -> Self {
        Self { state }
    }

    fn create_secret(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        validate_required("Name", &body["Name"])?;
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
        validate_string_length("name", &name, 1, 512)?;
        validate_optional_string_length(
            "clientRequestToken",
            body["ClientRequestToken"].as_str(),
            32,
            64,
        )?;
        validate_optional_string_length("description", body["Description"].as_str(), 0, 2048)?;
        validate_optional_string_length("kmsKeyId", body["KmsKeyId"].as_str(), 0, 2048)?;
        validate_optional_string_length("secretString", body["SecretString"].as_str(), 1, 65536)?;

        let mut state = self.state.write();

        let secret_string = body["SecretString"].as_str().map(|s| s.to_string());
        let secret_binary = body["SecretBinary"].as_str().and_then(base64_decode);
        let has_value = secret_string.is_some() || secret_binary.is_some();

        let client_request_token = body["ClientRequestToken"].as_str().map(|s| s.to_string());

        // Check for existing secret (idempotency)
        if let Some(existing) = state.secrets.get(&name) {
            if let Some(ref token) = client_request_token {
                // Check if same token was used
                if existing.versions.contains_key(token) {
                    let version = &existing.versions[token];
                    // Check if the value matches
                    if version.secret_string == secret_string
                        && version.secret_binary == secret_binary
                    {
                        // Idempotent: return same result
                        let mut response = json!({
                            "ARN": existing.arn,
                            "Name": existing.name,
                            "VersionId": token,
                        });
                        if !has_value {
                            response.as_object_mut().unwrap().remove("VersionId");
                        }
                        return Ok(AwsResponse::json(StatusCode::OK, response.to_string()));
                    } else {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "ResourceExistsException",
                            format!(
                                "You can't use ClientRequestToken {token} because that value is already in use for a version of secret {}.",
                                existing.arn
                            ),
                        ));
                    }
                }
            }
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceExistsException",
                format!("The operation failed because the secret {name} already exists."),
            ));
        }

        let region = &req.region;
        let account_id = &req.account_id;
        let arn = format!(
            "arn:aws:secretsmanager:{}:{}:secret:{}-{}",
            region,
            account_id,
            name,
            &uuid::Uuid::new_v4().to_string()[..6]
        );

        let now = Utc::now();

        let description = body["Description"].as_str().map(|s| s.to_string());
        let kms_key_id = body["KmsKeyId"].as_str().map(|s| s.to_string());

        let tags = parse_tags(&body["Tags"]);

        let (versions, current_version_id, version_id_for_response) = if has_value {
            let vid = client_request_token.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let version = SecretVersion {
                version_id: vid.clone(),
                secret_string,
                secret_binary,
                stages: vec!["AWSCURRENT".to_string()],
                created_at: now,
            };
            let mut versions = std::collections::HashMap::new();
            versions.insert(vid.clone(), version);
            (versions, Some(vid.clone()), Some(vid))
        } else {
            (std::collections::HashMap::new(), None, None)
        };

        let tags_ever_set = !tags.is_empty();
        let secret = Secret {
            name: name.clone(),
            arn: arn.clone(),
            description,
            kms_key_id,
            versions,
            current_version_id,
            tags,
            tags_ever_set,
            deleted: false,
            deletion_date: None,
            created_at: now,
            last_changed_at: now,
            last_accessed_at: None,
            rotation_enabled: None,
            rotation_lambda_arn: None,
            rotation_rules: None,
            last_rotated_at: None,
            resource_policy: None,
        };

        state.secrets.insert(name.clone(), secret);

        let mut response = json!({
            "ARN": arn,
            "Name": name,
        });
        if let Some(vid) = version_id_for_response {
            response["VersionId"] = json!(vid);
        }

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn get_secret_value(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let secret_id = require_secret_id(&body)?;
        validate_optional_string_length("versionId", body["VersionId"].as_str(), 32, 64)?;
        validate_optional_string_length("versionStage", body["VersionStage"].as_str(), 1, 256)?;

        let mut state = self.state.write();
        let secret = self.find_secret_mut(&mut state, &secret_id)?;

        if secret.deleted {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequestException",
                "You can't perform this operation on the secret because it was marked for deletion.",
            ));
        }

        let requested_stage = body["VersionStage"].as_str().unwrap_or("AWSCURRENT");

        // Determine which version to return
        let version_id = body["VersionId"]
            .as_str()
            .map(|s| s.to_string())
            .or_else(|| {
                secret
                    .versions
                    .iter()
                    .find(|(_, v)| v.stages.contains(&requested_stage.to_string()))
                    .map(|(id, _)| id.clone())
            });

        let version_id = match version_id {
            Some(vid) => vid,
            None => {
                // No versions exist
                return Err(AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ResourceNotFoundException",
                    format!(
                        "Secrets Manager can't find the specified secret value for staging label: {requested_stage}"
                    ),
                ));
            }
        };

        let version = secret.versions.get(&version_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!(
                    "Secrets Manager can't find the specified secret value for VersionId: {version_id}"
                ),
            )
        })?;

        // If VersionStage is specified with VersionId, verify they match
        if body["VersionId"].as_str().is_some() {
            if let Some(stage) = body["VersionStage"].as_str() {
                if !version.stages.contains(&stage.to_string()) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "ResourceNotFoundException",
                        "You provided a VersionStage that is not associated to the provided VersionId.",
                    ));
                }
            }
        }

        // Only set last_accessed_at on successful retrieval
        secret.last_accessed_at = Some(Utc::now());

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
        let secret_id = require_secret_id(&body)?;
        validate_optional_string_length(
            "clientRequestToken",
            body["ClientRequestToken"].as_str(),
            32,
            64,
        )?;
        validate_optional_string_length("secretString", body["SecretString"].as_str(), 1, 65536)?;

        let secret_string = body["SecretString"].as_str().map(|s| s.to_string());
        let secret_binary = body["SecretBinary"].as_str().and_then(base64_decode);

        // Validate that either SecretString or SecretBinary is provided
        if secret_string.is_none() && secret_binary.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequestException",
                "You must provide either SecretString or SecretBinary.",
            ));
        }

        let mut state = self.state.write();
        let secret = match self.find_secret_mut(&mut state, &secret_id) {
            Ok(s) => s,
            Err(_) => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ResourceNotFoundException",
                    "Secrets Manager can't find the specified secret.",
                ));
            }
        };

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

        // Check for idempotent request (same token, same value)
        if let Some(existing_version) = secret.versions.get(&version_id) {
            if existing_version.secret_string == secret_string
                && existing_version.secret_binary == secret_binary
            {
                // Idempotent: return existing version
                let response = json!({
                    "ARN": secret.arn,
                    "Name": secret.name,
                    "VersionId": version_id,
                    "VersionStages": existing_version.stages,
                });
                return Ok(AwsResponse::json(StatusCode::OK, response.to_string()));
            } else {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceExistsException",
                    format!(
                        "You can't use ClientRequestToken {version_id} because that value is already in use for a version of secret {}.",
                        secret.arn
                    ),
                ));
            }
        }

        let mut version_stages: Vec<String> = body["VersionStages"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_else(|| vec!["AWSCURRENT".to_string()]);

        // If this is the first version with a value, add AWSCURRENT to stages
        let has_current = secret
            .versions
            .values()
            .any(|v| v.stages.contains(&"AWSCURRENT".to_string()));
        if !has_current && !version_stages.contains(&"AWSCURRENT".to_string()) {
            version_stages.push("AWSCURRENT".to_string());
        }

        // Move AWSCURRENT from old version to AWSPREVIOUS if new version has AWSCURRENT
        if version_stages.contains(&"AWSCURRENT".to_string()) {
            if let Some(ref old_vid) = secret.current_version_id.clone() {
                if let Some(old_version) = secret.versions.get_mut(old_vid) {
                    old_version.stages.retain(|s| s != "AWSCURRENT");
                    if !old_version.stages.contains(&"AWSPREVIOUS".to_string()) {
                        old_version.stages.push("AWSPREVIOUS".to_string());
                    }
                }
                // Remove AWSPREVIOUS from any other version
                for (id, v) in secret.versions.iter_mut() {
                    if id != old_vid {
                        v.stages.retain(|s| s != "AWSPREVIOUS");
                    }
                }
            }
            secret.current_version_id = Some(version_id.clone());
        }

        // Remove custom stages from other versions that have them
        for stage in &version_stages {
            if stage == "AWSCURRENT" || stage == "AWSPREVIOUS" {
                continue;
            }
            for v in secret.versions.values_mut() {
                v.stages.retain(|s| s != stage);
            }
        }

        // Remove versions with no stages
        secret.versions.retain(|_, v| !v.stages.is_empty());

        let version = SecretVersion {
            version_id: version_id.clone(),
            secret_string,
            secret_binary,
            stages: version_stages.clone(),
            created_at: now,
        };

        secret.versions.insert(version_id.clone(), version);
        secret.last_changed_at = now;

        let response = json!({
            "ARN": secret.arn,
            "Name": secret.name,
            "VersionId": version_id,
            "VersionStages": version_stages,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn update_secret(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let secret_id = require_secret_id(&body)?;
        validate_optional_string_length(
            "clientRequestToken",
            body["ClientRequestToken"].as_str(),
            32,
            64,
        )?;
        validate_optional_string_length("description", body["Description"].as_str(), 0, 2048)?;
        validate_optional_string_length("kmsKeyId", body["KmsKeyId"].as_str(), 0, 2048)?;
        validate_optional_string_length("secretString", body["SecretString"].as_str(), 1, 65536)?;

        let mut state = self.state.write();
        let secret = match self.find_secret_mut(&mut state, &secret_id) {
            Ok(s) => s,
            Err(_) => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ResourceNotFoundException",
                    "Secrets Manager can't find the specified secret.",
                ));
            }
        };

        if secret.deleted {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequestException",
                "You can't perform this operation on the secret because it was marked for deletion.",
            ));
        }

        if let Some(desc) = body["Description"].as_str() {
            secret.description = Some(desc.to_string());
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

            // Check for idempotent request
            if let Some(existing_version) = secret.versions.get(&vid) {
                if existing_version.secret_string == secret_string
                    && existing_version.secret_binary == secret_binary
                {
                    // Idempotent
                    let response = json!({
                        "ARN": secret.arn,
                        "Name": secret.name,
                        "VersionId": vid,
                    });
                    return Ok(AwsResponse::json(StatusCode::OK, response.to_string()));
                } else {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ResourceExistsException",
                        format!(
                            "You can't use ClientRequestToken {vid} because that value is already in use for a version of secret {}.",
                            secret.arn
                        ),
                    ));
                }
            }

            let now = Utc::now();

            // Move AWSCURRENT -> AWSPREVIOUS on old version
            if let Some(ref old_vid) = secret.current_version_id.clone() {
                if let Some(old_v) = secret.versions.get_mut(old_vid) {
                    old_v.stages.retain(|s| s != "AWSCURRENT");
                    if !old_v.stages.contains(&"AWSPREVIOUS".to_string()) {
                        old_v.stages.push("AWSPREVIOUS".to_string());
                    }
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
            secret.current_version_id = Some(vid.clone());
            secret.last_changed_at = now;
            Some(vid)
        } else {
            secret.last_changed_at = Utc::now();
            None
        };

        let mut response = json!({
            "ARN": secret.arn,
            "Name": secret.name,
        });
        if let Some(vid) = version_id {
            response["VersionId"] = json!(vid);
        }

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn delete_secret(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let secret_id = require_secret_id(&body)?;

        let force_delete = body["ForceDeleteWithoutRecovery"]
            .as_bool()
            .unwrap_or(false);
        let recovery_window = body.get("RecoveryWindowInDays").and_then(|v| v.as_i64());

        // Validate recovery window range first (AWS validates this before the conflict check)
        if let Some(days) = recovery_window {
            if !(7..=30).contains(&days) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "An error occurred (InvalidParameterException) when calling the DeleteSecret operation: RecoveryWindowInDays value must be between 7 and 30 days (inclusive).",
                ));
            }
        }

        // Validate: can't use both force delete and recovery window
        if force_delete && recovery_window.is_some() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "An error occurred (InvalidParameterException) when calling the DeleteSecret operation: You can't use ForceDeleteWithoutRecovery in conjunction with RecoveryWindowInDays.",
            ));
        }

        let mut state = self.state.write();

        if force_delete {
            // Force delete: if secret doesn't exist, create a fake response
            match self.find_secret_mut(&mut state, &secret_id) {
                Ok(secret) => {
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
                Err(_) => {
                    // For force delete of non-existent secret, AWS returns success
                    let arn = format!(
                        "arn:aws:secretsmanager:{}:{}:secret:{}-{}",
                        req.region,
                        req.account_id,
                        secret_id,
                        &uuid::Uuid::new_v4().to_string()[..6]
                    );
                    let deletion_date = Utc::now();
                    let response = json!({
                        "ARN": arn,
                        "Name": secret_id,
                        "DeletionDate": deletion_date.timestamp_millis() as f64 / 1000.0,
                    });
                    return Ok(AwsResponse::json(StatusCode::OK, response.to_string()));
                }
            }
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
        let days = recovery_window.unwrap_or(30);
        let deletion_date = now + chrono::Duration::days(days);
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
        let secret_id = require_secret_id(&body)?;

        let mut state = self.state.write();
        let secret = self.find_secret_mut(&mut state, &secret_id)?;

        // AWS allows restoring a secret that is not deleted (no-op)
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
        let secret_id = require_secret_id(&body)?;

        let state = self.state.read();
        let secret = self.find_secret_ref(&state, &secret_id)?;

        let mut response = json!({
            "ARN": secret.arn,
            "Name": secret.name,
            "CreatedDate": secret.created_at.timestamp_millis() as f64 / 1000.0,
            "LastChangedDate": secret.last_changed_at.timestamp_millis() as f64 / 1000.0,
        });

        if !secret.versions.is_empty() {
            let mut version_ids_to_stages: serde_json::Map<String, Value> = serde_json::Map::new();
            for (vid, version) in &secret.versions {
                version_ids_to_stages.insert(vid.clone(), json!(version.stages));
            }
            response["VersionIdsToStages"] = Value::Object(version_ids_to_stages);
        }

        if let Some(ref desc) = secret.description {
            if !desc.is_empty() {
                response["Description"] = json!(desc);
            }
        }

        if secret.tags_ever_set || !secret.tags.is_empty() {
            response["Tags"] = json!(tags_to_json(&secret.tags));
        }

        if let Some(ref kms) = secret.kms_key_id {
            response["KmsKeyId"] = json!(kms);
        }
        if secret.deleted {
            response["DeletedDate"] = json!(secret
                .deletion_date
                .map(|d| d.timestamp_millis() as f64 / 1000.0));
        }
        if let Some(rotation_enabled) = secret.rotation_enabled {
            response["RotationEnabled"] = json!(rotation_enabled);
        }
        if let Some(ref lambda_arn) = secret.rotation_lambda_arn {
            response["RotationLambdaARN"] = json!(lambda_arn);
        }
        if let Some(ref rules) = secret.rotation_rules {
            let mut rules_json = json!({});
            if let Some(days) = rules.automatically_after_days {
                rules_json["AutomaticallyAfterDays"] = json!(days);
            }
            response["RotationRules"] = rules_json;
        }
        if let Some(last_rotated) = secret.last_rotated_at {
            response["LastRotatedDate"] = json!(last_rotated.timestamp_millis() as f64 / 1000.0);
        }
        // Calculate NextRotationDate if rotation is enabled
        if secret.rotation_enabled == Some(true) {
            if let Some(ref rules) = secret.rotation_rules {
                if let Some(days) = rules.automatically_after_days {
                    let base = secret.last_rotated_at.unwrap_or(secret.created_at);
                    let next = base + chrono::Duration::days(days);
                    response["NextRotationDate"] = json!(next.timestamp_millis() as f64 / 1000.0);
                }
            }
        }

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn list_secrets(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        validate_optional_string_length("nextToken", body["NextToken"].as_str(), 1, 4096)?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(100) as usize;
        let next_token = body["NextToken"].as_str();
        let filters = body["Filters"].as_array();
        let include_deleted = body["IncludePlannedDeletion"].as_bool().unwrap_or(false);

        // Validate filters
        if let Some(filters) = filters {
            for filter in filters {
                let key = filter["Key"].as_str().unwrap_or("");
                let values = filter["Values"].as_array();

                if key.is_empty() {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterException",
                        "Invalid filter key",
                    ));
                }

                let valid_keys = [
                    "all",
                    "name",
                    "tag-key",
                    "description",
                    "tag-value",
                    "owning-service",
                    "primary-region",
                ];
                if !valid_keys.contains(&key) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ValidationException",
                        format!(
                            "1 validation error detected: Value '{}' at 'filters.1.member.key' failed to satisfy constraint: Member must satisfy enum value set: [all, name, tag-key, description, tag-value]",
                            key
                        ),
                    ));
                }

                if values.is_none() || values.unwrap().is_empty() {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterException",
                        format!("Invalid filter values for key: {key}"),
                    ));
                }
            }
        }

        let state = self.state.read();

        let mut secrets: Vec<&Secret> = state
            .secrets
            .values()
            .filter(|s| {
                // Exclude deleted unless IncludePlannedDeletion
                if s.deleted && !include_deleted {
                    return false;
                }

                if let Some(filters) = filters {
                    for filter in filters {
                        let key = filter["Key"].as_str().unwrap_or("");
                        let values: Vec<&str> = filter["Values"]
                            .as_array()
                            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
                            .unwrap_or_default();

                        let matches = match key {
                            "name" => filter_name(s, &values),
                            "description" => filter_description(s, &values),
                            "tag-key" => filter_tag_key(s, &values),
                            "tag-value" => filter_tag_value(s, &values),
                            "all" => filter_all(s, &values),
                            "owning-service" => false,
                            "primary-region" => false,
                            _ => true,
                        };

                        if !matches {
                            return false;
                        }
                    }
                }
                true
            })
            .collect();
        secrets.sort_by(|a, b| a.created_at.cmp(&b.created_at));

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
                let mut entry = json!({
                    "ARN": s.arn,
                    "Name": s.name,
                    "CreatedDate": s.created_at.timestamp_millis() as f64 / 1000.0,
                    "LastChangedDate": s.last_changed_at.timestamp_millis() as f64 / 1000.0,
                });

                if !s.versions.is_empty() {
                    let mut version_ids_to_stages: serde_json::Map<String, Value> =
                        serde_json::Map::new();
                    for (vid, version) in &s.versions {
                        version_ids_to_stages.insert(vid.clone(), json!(version.stages));
                    }
                    entry["SecretVersionsToStages"] = Value::Object(version_ids_to_stages);
                }

                if let Some(ref desc) = s.description {
                    if !desc.is_empty() {
                        entry["Description"] = json!(desc);
                    }
                }

                if s.tags_ever_set || !s.tags.is_empty() {
                    entry["Tags"] = json!(tags_to_json(&s.tags));
                }

                if let Some(ref kms) = s.kms_key_id {
                    entry["KmsKeyId"] = json!(kms);
                }
                if s.deleted {
                    entry["DeletedDate"] = json!(s
                        .deletion_date
                        .map(|d| d.timestamp_millis() as f64 / 1000.0));
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
        let secret_id = require_secret_id(&body)?;

        let new_tags = parse_tags(&body["Tags"]);

        let mut state = self.state.write();
        let secret = self.find_secret_mut(&mut state, &secret_id)?;

        if !new_tags.is_empty() {
            secret.tags_ever_set = true;
        }
        for (k, v) in new_tags {
            // Update existing tag or add new one
            if let Some(existing) = secret.tags.iter_mut().find(|(ek, _)| *ek == k) {
                existing.1 = v;
            } else {
                secret.tags.push((k, v));
            }
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let secret_id = require_secret_id(&body)?;

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

        secret.tags.retain(|(k, _)| !tag_keys.contains(k));

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_secret_version_ids(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let secret_id = require_secret_id(&body)?;
        validate_optional_string_length("nextToken", body["NextToken"].as_str(), 1, 4096)?;

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

    fn get_random_password(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let length = body["PasswordLength"].as_i64().unwrap_or(32) as usize;

        if length < 4 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "InvalidParameterException",
            ));
        }
        if length > 4096 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                "InvalidParameterValue",
            ));
        }

        let exclude_lowercase = body["ExcludeLowercase"].as_bool().unwrap_or(false);
        let exclude_uppercase = body["ExcludeUppercase"].as_bool().unwrap_or(false);
        let exclude_numbers = body["ExcludeNumbers"].as_bool().unwrap_or(false);
        let exclude_punctuation = body["ExcludePunctuation"].as_bool().unwrap_or(false);
        let include_space = body["IncludeSpace"].as_bool().unwrap_or(false);
        let require_each = body["RequireEachIncludedType"].as_bool().unwrap_or(true);
        let exclude_chars = body["ExcludeCharacters"].as_str().unwrap_or("").to_string();

        let lowercase = "abcdefghijklmnopqrstuvwxyz";
        let uppercase = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        let digits = "0123456789";
        let punctuation = "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~";

        let mut char_pool = String::new();
        let mut required_chars: Vec<String> = Vec::new();

        if !exclude_lowercase {
            let filtered: String = lowercase
                .chars()
                .filter(|c| !exclude_chars.contains(*c))
                .collect();
            if !filtered.is_empty() {
                required_chars.push(filtered.clone());
                char_pool.push_str(&filtered);
            }
        }
        if !exclude_uppercase {
            let filtered: String = uppercase
                .chars()
                .filter(|c| !exclude_chars.contains(*c))
                .collect();
            if !filtered.is_empty() {
                required_chars.push(filtered.clone());
                char_pool.push_str(&filtered);
            }
        }
        if !exclude_numbers {
            let filtered: String = digits
                .chars()
                .filter(|c| !exclude_chars.contains(*c))
                .collect();
            if !filtered.is_empty() {
                required_chars.push(filtered.clone());
                char_pool.push_str(&filtered);
            }
        }
        if !exclude_punctuation {
            let filtered: String = punctuation
                .chars()
                .filter(|c| !exclude_chars.contains(*c))
                .collect();
            if !filtered.is_empty() {
                required_chars.push(filtered.clone());
                char_pool.push_str(&filtered);
            }
        }
        if include_space && !exclude_chars.contains(' ') {
            char_pool.push(' ');
        }

        if char_pool.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "InvalidParameterException",
            ));
        }

        let pool_bytes: Vec<char> = char_pool.chars().collect();
        let mut password = String::with_capacity(length);

        // Use simple random generation
        if require_each {
            // First, ensure at least one character from each required category
            for category in &required_chars {
                let chars: Vec<char> = category.chars().collect();
                let idx = simple_random() % chars.len();
                password.push(chars[idx]);
            }
            if include_space && !exclude_chars.contains(' ') {
                password.push(' ');
            }
        }

        // Fill the rest randomly
        while password.len() < length {
            let idx = simple_random() % pool_bytes.len();
            password.push(pool_bytes[idx]);
        }

        // Shuffle the password (Fisher-Yates)
        let mut chars: Vec<char> = password.chars().collect();
        for i in (1..chars.len()).rev() {
            let j = simple_random() % (i + 1);
            chars.swap(i, j);
        }
        let password: String = chars.into_iter().take(length).collect();

        let response = json!({
            "RandomPassword": password,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn rotate_secret(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let secret_id = require_secret_id(&body)?;

        // Validate ClientRequestToken
        if let Some(token) = body["ClientRequestToken"].as_str() {
            if token.len() < 32 || token.len() > 64 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "ClientRequestToken must be 32-64 characters long.",
                ));
            }
        }

        // Validate RotationLambdaARN
        if let Some(arn) = body["RotationLambdaARN"].as_str() {
            if arn.len() > 2048 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "RotationLambdaARN length must be less than or equal to 2048.",
                ));
            }
        }

        // Validate RotationRules
        if let Some(rules) = body["RotationRules"].as_object() {
            if let Some(days) = rules.get("AutomaticallyAfterDays").and_then(|v| v.as_i64()) {
                if !(1..=1000).contains(&days) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterException",
                        "RotationRules.AutomaticallyAfterDays must be within 1-1000.",
                    ));
                }
            }
        }

        let mut state = self.state.write();
        let secret = self.find_secret_mut(&mut state, &secret_id)?;

        if secret.deleted {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequestException",
                "You can't perform this operation on the secret because it was marked for deletion.",
            ));
        }

        // Set rotation config
        if let Some(lambda_arn) = body["RotationLambdaARN"].as_str() {
            secret.rotation_lambda_arn = Some(lambda_arn.to_string());
        }

        if let Some(rules) = body["RotationRules"].as_object() {
            let days = rules.get("AutomaticallyAfterDays").and_then(|v| v.as_i64());
            secret.rotation_rules = Some(RotationRules {
                automatically_after_days: days,
            });
        }

        secret.rotation_enabled = Some(true);
        let now = Utc::now();
        secret.last_rotated_at = Some(now);
        secret.last_changed_at = now;

        let version_id = body["ClientRequestToken"]
            .as_str()
            .map(|s| s.to_string())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let has_lambda =
            body["RotationLambdaARN"].as_str().is_some() || secret.rotation_lambda_arn.is_some();

        // If the secret has a value, perform rotation
        if let Some(current_vid) = secret.current_version_id.clone() {
            let current_value = secret.versions.get(&current_vid).cloned();

            if let Some(cv) = current_value {
                if has_lambda {
                    // With Lambda: create AWSPENDING version for Lambda to process
                    let version = SecretVersion {
                        version_id: version_id.clone(),
                        secret_string: cv.secret_string.clone(),
                        secret_binary: cv.secret_binary.clone(),
                        stages: vec!["AWSPENDING".to_string()],
                        created_at: now,
                    };
                    secret.versions.insert(version_id.clone(), version);
                } else {
                    // Without Lambda: simple rotation - new version becomes AWSCURRENT
                    // Move old version to AWSPREVIOUS
                    if let Some(old_v) = secret.versions.get_mut(&current_vid) {
                        old_v.stages.retain(|s| s != "AWSCURRENT");
                        if !old_v.stages.contains(&"AWSPREVIOUS".to_string()) {
                            old_v.stages.push("AWSPREVIOUS".to_string());
                        }
                    }
                    let version = SecretVersion {
                        version_id: version_id.clone(),
                        secret_string: cv.secret_string.clone(),
                        secret_binary: cv.secret_binary.clone(),
                        stages: vec!["AWSCURRENT".to_string()],
                        created_at: now,
                    };
                    secret.versions.insert(version_id.clone(), version);
                    secret.current_version_id = Some(version_id.clone());
                }
            }
        }

        let response = json!({
            "ARN": secret.arn,
            "Name": secret.name,
            "VersionId": version_id,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn cancel_rotate_secret(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let secret_id = require_secret_id(&body)?;

        let mut state = self.state.write();
        let secret = self.find_secret_mut(&mut state, &secret_id)?;

        if secret.deleted {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequestException",
                "You can't perform this operation on the secret because it was marked for deletion.",
            ));
        }

        if secret.rotation_enabled != Some(true) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequestException",
                "You can't cancel rotation for a secret that does not have rotation enabled.",
            ));
        }

        secret.rotation_enabled = Some(false);

        let response = json!({
            "ARN": secret.arn,
            "Name": secret.name,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn update_secret_version_stage(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let secret_id = require_secret_id(&body)?;
        let version_stage = body["VersionStage"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "VersionStage is required",
                )
            })?
            .to_string();
        validate_string_length("versionStage", &version_stage, 1, 256)?;
        validate_optional_string_length(
            "removeFromVersionId",
            body["RemoveFromVersionId"].as_str(),
            32,
            64,
        )?;
        validate_optional_string_length(
            "moveToVersionId",
            body["MoveToVersionId"].as_str(),
            32,
            64,
        )?;

        let move_to = body["MoveToVersionId"].as_str().map(|s| s.to_string());
        let remove_from = body["RemoveFromVersionId"].as_str().map(|s| s.to_string());

        let mut state = self.state.write();
        let secret = self.find_secret_mut(&mut state, &secret_id)?;

        // Validate: if moving AWSCURRENT, must specify RemoveFromVersionId
        if version_stage == "AWSCURRENT" && move_to.is_some() && remove_from.is_none() {
            // Find the version that currently has AWSCURRENT
            let current_holder = secret
                .versions
                .iter()
                .find(|(_, v)| v.stages.contains(&"AWSCURRENT".to_string()))
                .map(|(id, _)| id.clone());

            if let Some(current_vid) = current_holder {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    format!(
                        "The parameter RemoveFromVersionId can't be empty. Staging label AWSCURRENT is currently attached to version {current_vid}, so you must explicitly reference that version in RemoveFromVersionId."
                    ),
                ));
            }
        }

        // Remove stage from specified version
        if let Some(ref remove_vid) = remove_from {
            if let Some(version) = secret.versions.get_mut(remove_vid) {
                version.stages.retain(|s| s != &version_stage);
                // If moving AWSCURRENT away, add AWSPREVIOUS and remove from others
                if version_stage == "AWSCURRENT" {
                    // Remove AWSPREVIOUS from all other versions first
                    for (id, v) in secret.versions.iter_mut() {
                        if id != remove_vid {
                            v.stages.retain(|s| s != "AWSPREVIOUS");
                        }
                    }
                    // Now add AWSPREVIOUS to the version losing AWSCURRENT
                    if let Some(v) = secret.versions.get_mut(remove_vid) {
                        if !v.stages.contains(&"AWSPREVIOUS".to_string()) {
                            v.stages.push("AWSPREVIOUS".to_string());
                        }
                    }
                }
            }
        }

        // Add stage to specified version
        if let Some(ref move_vid) = move_to {
            if let Some(version) = secret.versions.get_mut(move_vid) {
                if !version.stages.contains(&version_stage) {
                    version.stages.push(version_stage.clone());
                }
            }
            // Update current_version_id if we moved AWSCURRENT
            if version_stage == "AWSCURRENT" {
                secret.current_version_id = Some(move_vid.clone());
            }
        }

        let response = json!({
            "ARN": secret.arn,
            "Name": secret.name,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn batch_get_secret_value(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        validate_optional_string_length("nextToken", body["NextToken"].as_str(), 1, 4096)?;
        let secret_id_list = body["SecretIdList"].as_array();
        let filters = body["Filters"].as_array();
        let max_results = body.get("MaxResults").and_then(|v| v.as_i64());

        // Validate: can't use both SecretIdList and Filters
        if secret_id_list.is_some() && filters.is_some() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "Either 'SecretIdList' or 'Filters' must be provided, but not both.",
            ));
        }

        // Validate: MaxResults requires Filters
        if max_results.is_some() && filters.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "'Filters' not specified. 'Filters' must also be specified when 'MaxResults' is provided.",
            ));
        }

        let state = self.state.read();
        let mut secret_values: Vec<Value> = Vec::new();
        let mut errors: Vec<Value> = Vec::new();

        if let Some(id_list) = secret_id_list {
            for id_val in id_list {
                let sid = id_val.as_str().unwrap_or("");
                match self.find_secret_ref(&state, sid) {
                    Ok(secret) => {
                        if secret.deleted {
                            errors.push(json!({
                                "SecretId": sid,
                                "ErrorCode": "InvalidRequestException",
                                "Message": "Secret is currently marked deleted. Secret can be recovered with RestoreSecret. Secret is currently marked deleted.",
                            }));
                        } else if let Some(ref current_vid) = secret.current_version_id {
                            if let Some(version) = secret.versions.get(current_vid) {
                                let mut entry = json!({
                                    "ARN": secret.arn,
                                    "Name": secret.name,
                                    "VersionId": version.version_id,
                                    "VersionStages": version.stages,
                                    "CreatedDate": version.created_at.timestamp_millis() as f64 / 1000.0,
                                });
                                if let Some(ref s) = version.secret_string {
                                    entry["SecretString"] = json!(s);
                                }
                                if let Some(ref b) = version.secret_binary {
                                    entry["SecretBinary"] = json!(base64_encode(b));
                                }
                                secret_values.push(entry);
                            } else {
                                errors.push(json!({
                                    "SecretId": sid,
                                    "ErrorCode": "ResourceNotFoundException",
                                    "Message": "Secrets Manager can't find the specified secret.",
                                }));
                            }
                        } else {
                            errors.push(json!({
                                "SecretId": sid,
                                "ErrorCode": "ResourceNotFoundException",
                                "Message": "Secrets Manager can't find the specified secret.",
                            }));
                        }
                    }
                    Err(_) => {
                        errors.push(json!({
                            "SecretId": sid,
                            "ErrorCode": "ResourceNotFoundException",
                            "Message": "Secrets Manager can't find the specified secret.",
                        }));
                    }
                }
            }
        } else if let Some(filters) = filters {
            // Get secrets matching filters
            let matching: Vec<&Secret> = state
                .secrets
                .values()
                .filter(|s| {
                    if s.deleted {
                        return false;
                    }
                    for filter in filters {
                        let key = filter["Key"].as_str().unwrap_or("");
                        let values: Vec<&str> = filter["Values"]
                            .as_array()
                            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
                            .unwrap_or_default();
                        let matches = match key {
                            "name" => filter_name(s, &values),
                            "description" => filter_description(s, &values),
                            "tag-key" => filter_tag_key(s, &values),
                            "tag-value" => filter_tag_value(s, &values),
                            "all" => filter_all(s, &values),
                            _ => true,
                        };
                        if !matches {
                            return false;
                        }
                    }
                    true
                })
                .collect();

            let limit = max_results.unwrap_or(100) as usize;
            let mut no_value_found = false;
            let mut matching = matching;
            matching.sort_by(|a, b| a.name.cmp(&b.name));

            for secret in matching.iter().take(limit) {
                if let Some(ref current_vid) = secret.current_version_id {
                    if let Some(version) = secret.versions.get(current_vid) {
                        let mut entry = json!({
                            "ARN": secret.arn,
                            "Name": secret.name,
                            "VersionId": version.version_id,
                            "VersionStages": version.stages,
                            "CreatedDate": version.created_at.timestamp_millis() as f64 / 1000.0,
                        });
                        if let Some(ref s) = version.secret_string {
                            entry["SecretString"] = json!(s);
                        }
                        if let Some(ref b) = version.secret_binary {
                            entry["SecretBinary"] = json!(base64_encode(b));
                        }
                        secret_values.push(entry);
                    } else {
                        no_value_found = true;
                    }
                } else {
                    no_value_found = true;
                }
            }

            if no_value_found && secret_values.is_empty() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ResourceNotFoundException",
                    "Secrets Manager can't find the specified secret.",
                ));
            }
        }

        let mut response = json!({
            "SecretValues": secret_values,
            "Errors": errors,
        });

        // Remove empty arrays
        if errors.is_empty() {
            response.as_object_mut().unwrap().remove("Errors");
        }

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn get_resource_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let secret_id = require_secret_id(&body)?;

        let state = self.state.read();
        let secret = self.find_secret_ref(&state, &secret_id)?;

        let mut response = json!({
            "ARN": secret.arn,
            "Name": secret.name,
        });

        if let Some(ref policy) = secret.resource_policy {
            response["ResourcePolicy"] = json!(policy);
        }

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn validate_resource_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        validate_optional_string_length("secretId", body["SecretId"].as_str(), 1, 2048)?;
        validate_required("ResourcePolicy", &body["ResourcePolicy"])?;
        validate_optional_string_length(
            "resourcePolicy",
            body["ResourcePolicy"].as_str(),
            1,
            20480,
        )?;
        let response = json!({
            "PolicyValidationPassed": true,
            "ValidationErrors": [],
        });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn put_resource_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let secret_id = require_secret_id(&body)?;
        validate_required("ResourcePolicy", &body["ResourcePolicy"])?;
        validate_optional_string_length(
            "resourcePolicy",
            body["ResourcePolicy"].as_str(),
            1,
            20480,
        )?;
        let policy = body["ResourcePolicy"].as_str().map(|s| s.to_string());

        let mut state = self.state.write();
        let secret = self.find_secret_mut(&mut state, &secret_id)?;
        secret.resource_policy = policy;

        let response = json!({
            "ARN": secret.arn,
            "Name": secret.name,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn delete_resource_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let secret_id = require_secret_id(&body)?;

        let mut state = self.state.write();
        let secret = self.find_secret_mut(&mut state, &secret_id)?;
        secret.resource_policy = None;

        let response = json!({
            "ARN": secret.arn,
            "Name": secret.name,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn replicate_secret_to_regions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let secret_id = require_secret_id(&body)?;

        let state = self.state.read();
        let secret = self.find_secret_ref(&state, &secret_id)?;

        let response = json!({
            "ARN": secret.arn,
            "ReplicationStatus": [],
        });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn remove_regions_from_replication(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let secret_id = require_secret_id(&body)?;

        let state = self.state.read();
        let secret = self.find_secret_ref(&state, &secret_id)?;

        let response = json!({
            "ARN": secret.arn,
            "ReplicationStatus": [],
        });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn stop_replication_to_replica(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let secret_id = require_secret_id(&body)?;

        let state = self.state.read();
        let secret = self.find_secret_ref(&state, &secret_id)?;

        let response = json!({
            "ARN": secret.arn,
        });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    /// Find a secret by name, full ARN, or partial ARN (mutable).
    fn find_secret_mut<'a>(
        &self,
        state: &'a mut crate::state::SecretsManagerState,
        secret_id: &str,
    ) -> Result<&'a mut Secret, AwsServiceError> {
        let key = self.find_secret_key(state, secret_id)?;
        Ok(state.secrets.get_mut(&key).unwrap())
    }

    fn find_secret_key(
        &self,
        state: &crate::state::SecretsManagerState,
        secret_id: &str,
    ) -> Result<String, AwsServiceError> {
        if state.secrets.contains_key(secret_id) {
            return Ok(secret_id.to_string());
        }

        for secret in state.secrets.values() {
            if secret.arn == secret_id {
                return Ok(secret.name.clone());
            }
        }

        if secret_id.starts_with("arn:aws:secretsmanager:") {
            for secret in state.secrets.values() {
                if secret.arn.starts_with(secret_id) {
                    return Ok(secret.name.clone());
                }
            }
        }

        Err(AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            "Secrets Manager can't find the specified secret.",
        ))
    }

    /// Find a secret by name, full ARN, or partial ARN (immutable).
    fn find_secret_ref<'a>(
        &self,
        state: &'a crate::state::SecretsManagerState,
        secret_id: &str,
    ) -> Result<&'a Secret, AwsServiceError> {
        if let Some(secret) = state.secrets.get(secret_id) {
            return Ok(secret);
        }

        // Search by full ARN
        for secret in state.secrets.values() {
            if secret.arn == secret_id {
                return Ok(secret);
            }
        }

        // Search by partial ARN
        if secret_id.starts_with("arn:aws:secretsmanager:") {
            for secret in state.secrets.values() {
                if secret.arn.starts_with(secret_id) {
                    return Ok(secret);
                }
            }
        }

        Err(AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            "Secrets Manager can't find the specified secret.",
        ))
    }
}

fn require_secret_id(body: &Value) -> Result<String, AwsServiceError> {
    let id = body["SecretId"].as_str().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterException",
            "SecretId is required",
        )
    })?;
    validate_string_length("secretId", id, 1, 2048)?;
    Ok(id.to_string())
}

fn parse_tags(tags_val: &Value) -> Vec<(String, String)> {
    tags_val
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
        .unwrap_or_default()
}

fn tags_to_json(tags: &[(String, String)]) -> Vec<Value> {
    tags.iter()
        .map(|(k, v)| json!({"Key": k, "Value": v}))
        .collect()
}

/// Split text into words for secret name filtering.
/// Splits on special characters (/ - _ + = . @) and camelCase.
/// If multiple different special characters are present, doesn't split.
/// Spaces are always split on first.
fn split_words(text: &str) -> Vec<String> {
    // First split on whitespace, then apply word splitting to each part
    let mut all_words = Vec::new();
    for space_part in text.split_whitespace() {
        all_words.extend(split_words_no_space(space_part));
    }
    all_words
}

fn split_words_no_space(text: &str) -> Vec<String> {
    let special_chars = ['/', '-', '_', '+', '=', '.', '@'];

    // Check if text is just a special char
    if text.len() == 1 && special_chars.contains(&text.chars().next().unwrap_or(' ')) {
        return vec![];
    }

    // Find which special chars are present
    let present: Vec<char> = special_chars
        .iter()
        .filter(|&&c| text.contains(c))
        .copied()
        .collect();

    if present.len() > 1 {
        // Multiple different special chars: don't split
        return vec![text.to_string()];
    }

    if present.len() == 1 {
        let ch = present[0];
        let parts: Vec<&str> = text.split(ch).filter(|s| !s.is_empty()).collect();
        let mut result = Vec::new();
        for part in parts {
            result.extend(split_by_uppercase(part));
        }
        return result;
    }

    // No special chars: split by uppercase
    split_by_uppercase(text)
}

/// Split a string by the pattern: a non-lowercase char followed by one or more lowercase chars.
/// Equivalent to Python regex: re.split(r"([^a-z][a-z]+)", s)
fn split_by_uppercase(text: &str) -> Vec<String> {
    // Implement the equivalent of Python's re.split(r"([^a-z][a-z]+)", text)
    // re.split with capturing group returns: [before, match, between, match, ..., after]
    let chars: Vec<char> = text.chars().collect();
    let mut words = Vec::new();
    let mut last_end = 0;
    let mut i = 0;

    while i < chars.len() {
        // Try to find pattern: [^a-z][a-z]+
        if !chars[i].is_ascii_lowercase()
            && i + 1 < chars.len()
            && chars[i + 1].is_ascii_lowercase()
        {
            // Text before this match (between previous match end and this match start)
            if i > last_end {
                let between: String = chars[last_end..i].iter().collect();
                let trimmed = between.trim().to_string();
                if !trimmed.is_empty() {
                    words.push(trimmed);
                }
            }

            // The match itself
            let start = i;
            i += 2;
            while i < chars.len() && chars[i].is_ascii_lowercase() {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            let trimmed = word.trim().to_string();
            if !trimmed.is_empty() {
                words.push(trimmed);
            }
            last_end = i;
        } else {
            i += 1;
        }
    }

    // Text after last match
    if last_end < chars.len() {
        let after: String = chars[last_end..].iter().collect();
        let trimmed = after.trim().to_string();
        if !trimmed.is_empty() {
            words.push(trimmed);
        }
    }

    words
}

/// Match a pattern against a value.
/// - match_prefix=true: simple prefix match on the full string
/// - match_prefix=false: split both into words, all pattern words must prefix-match some value word
fn match_pattern(pattern: &str, value: &str, match_prefix: bool, case_sensitive: bool) -> bool {
    if match_prefix {
        if case_sensitive {
            value.starts_with(pattern)
        } else {
            value.to_lowercase().starts_with(&pattern.to_lowercase())
        }
    } else {
        let mut pattern_words = split_words(pattern);
        if pattern_words.is_empty() {
            return false;
        }
        let mut value_words = split_words(value);
        if !case_sensitive {
            pattern_words = pattern_words.iter().map(|w| w.to_lowercase()).collect();
            value_words = value_words.iter().map(|w| w.to_lowercase()).collect();
        }
        for pw in &pattern_words {
            if !value_words.iter().any(|vw| vw.starts_with(pw.as_str())) {
                return false;
            }
        }
        true
    }
}

/// The main matcher: check patterns against a list of strings.
/// Supports negation (!pattern), prefix matching, and case sensitivity.
fn matcher(patterns: &[&str], strings: &[&str], match_prefix: bool, case_sensitive: bool) -> bool {
    // First check negated patterns
    for pattern in patterns.iter().filter(|p| p.starts_with('!')) {
        let inner = &pattern[1..];
        for s in strings {
            if !match_pattern(inner, s, match_prefix, case_sensitive) {
                return true;
            }
        }
    }

    // Then check positive patterns
    for pattern in patterns.iter().filter(|p| !p.starts_with('!')) {
        for s in strings {
            if match_pattern(pattern, s, match_prefix, case_sensitive) {
                return true;
            }
        }
    }
    false
}

/// Name filter: prefix match, case sensitive
fn filter_name(secret: &Secret, values: &[&str]) -> bool {
    matcher(values, &[secret.name.as_str()], true, true)
}

/// Description filter: word match, case insensitive
fn filter_description(secret: &Secret, values: &[&str]) -> bool {
    match secret.description.as_deref() {
        Some(desc) if !desc.is_empty() => matcher(values, &[desc], false, false),
        _ => false,
    }
}

/// Tag key filter: prefix match, case sensitive
fn filter_tag_key(secret: &Secret, values: &[&str]) -> bool {
    if secret.tags.is_empty() {
        return false;
    }
    let keys: Vec<&str> = secret.tags.iter().map(|(k, _)| k.as_str()).collect();
    matcher(values, &keys, true, true)
}

/// Tag value filter: prefix match, case sensitive
fn filter_tag_value(secret: &Secret, values: &[&str]) -> bool {
    if secret.tags.is_empty() {
        return false;
    }
    let vals: Vec<&str> = secret.tags.iter().map(|(_, v)| v.as_str()).collect();
    matcher(values, &vals, true, true)
}

/// All filter: word match, case insensitive, across all fields
fn filter_all(secret: &Secret, values: &[&str]) -> bool {
    let mut attributes: Vec<&str> = vec![secret.name.as_str()];
    if let Some(ref desc) = secret.description {
        if !desc.is_empty() {
            attributes.push(desc.as_str());
        }
    }
    for (k, v) in &secret.tags {
        attributes.push(k.as_str());
        attributes.push(v.as_str());
    }
    matcher(values, &attributes, false, false)
}

fn simple_random() -> usize {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let s = RandomState::new();
    let mut hasher = s.build_hasher();
    hasher.write_usize(0);
    hasher.finish() as usize
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
            "GetRandomPassword" => self.get_random_password(&req),
            "RotateSecret" => self.rotate_secret(&req),
            "CancelRotateSecret" => self.cancel_rotate_secret(&req),
            "UpdateSecretVersionStage" => self.update_secret_version_stage(&req),
            "BatchGetSecretValue" => self.batch_get_secret_value(&req),
            "GetResourcePolicy" => self.get_resource_policy(&req),
            "PutResourcePolicy" => self.put_resource_policy(&req),
            "DeleteResourcePolicy" => self.delete_resource_policy(&req),
            "ValidateResourcePolicy" => self.validate_resource_policy(&req),
            "ReplicateSecretToRegions" => self.replicate_secret_to_regions(&req),
            "RemoveRegionsFromReplication" => self.remove_regions_from_replication(&req),
            "StopReplicationToReplica" => self.stop_replication_to_replica(&req),
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
            "GetRandomPassword",
            "RotateSecret",
            "CancelRotateSecret",
            "UpdateSecretVersionStage",
            "BatchGetSecretValue",
            "GetResourcePolicy",
            "PutResourcePolicy",
            "DeleteResourcePolicy",
            "ValidateResourcePolicy",
            "ReplicateSecretToRegions",
            "RemoveRegionsFromReplication",
            "StopReplicationToReplica",
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
    async fn test_create_secret_without_value() {
        let state = make_state();
        let svc = SecretsManagerService::new(state);

        let req = make_request("CreateSecret", r#"{"Name": "empty-secret"}"#);
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Name"], "empty-secret");
        assert!(body.get("VersionId").is_none());
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
        // Tags should be empty list after untagging all (but key present since tags were set)
        assert_eq!(body["Tags"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_get_random_password() {
        let state = make_state();
        let svc = SecretsManagerService::new(state);

        let req = make_request("GetRandomPassword", "{}");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["RandomPassword"].as_str().unwrap().len(), 32);
    }

    #[tokio::test]
    async fn test_replication_ops_return_arn() {
        let state = make_state();
        let svc = SecretsManagerService::new(state);

        let req = make_request(
            "CreateSecret",
            r#"{"Name": "repl-secret", "SecretString": "val"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        let create_body: Value = serde_json::from_slice(&resp.body).unwrap();
        let expected_arn = create_body["ARN"].as_str().unwrap();

        for action in &[
            "ReplicateSecretToRegions",
            "RemoveRegionsFromReplication",
            "StopReplicationToReplica",
        ] {
            let req = make_request(action, r#"{"SecretId": "repl-secret"}"#);
            let resp = svc.handle(req).await.unwrap();
            let body: Value = serde_json::from_slice(&resp.body).unwrap();
            assert_eq!(
                body["ARN"].as_str().unwrap(),
                expected_arn,
                "{action} should return the secret's actual ARN"
            );
        }
    }

    #[tokio::test]
    async fn test_secret_id_length_validation() {
        let state = make_state();
        let svc = SecretsManagerService::new(state);

        // SecretId too long (> 2048)
        let long_id = "x".repeat(2049);
        let req = make_request("GetSecretValue", &format!(r#"{{"SecretId": "{long_id}"}}"#));
        match svc.handle(req).await {
            Err(e) => assert!(e.to_string().contains("ValidationException")),
            Ok(_) => panic!("expected ValidationException"),
        }
    }

    #[tokio::test]
    async fn test_name_length_validation() {
        let state = make_state();
        let svc = SecretsManagerService::new(state);

        // Name too long (> 512)
        let long_name = "x".repeat(513);
        let req = make_request(
            "CreateSecret",
            &format!(r#"{{"Name": "{long_name}", "SecretString": "val"}}"#),
        );
        match svc.handle(req).await {
            Err(e) => assert!(e.to_string().contains("ValidationException")),
            Ok(_) => panic!("expected ValidationException"),
        }
    }

    #[tokio::test]
    async fn test_next_token_length_validation() {
        let state = make_state();
        let svc = SecretsManagerService::new(state);

        // NextToken too long (> 4096)
        let long_token = "x".repeat(4097);
        let req = make_request(
            "ListSecrets",
            &format!(r#"{{"NextToken": "{long_token}"}}"#),
        );
        match svc.handle(req).await {
            Err(e) => assert!(e.to_string().contains("ValidationException")),
            Ok(_) => panic!("expected ValidationException"),
        }
    }

    #[tokio::test]
    async fn test_client_request_token_length_validation() {
        let state = make_state();
        let svc = SecretsManagerService::new(state);

        // ClientRequestToken too short (< 32)
        let req = make_request(
            "CreateSecret",
            r#"{"Name": "test", "SecretString": "val", "ClientRequestToken": "short"}"#,
        );
        match svc.handle(req).await {
            Err(e) => assert!(e.to_string().contains("ValidationException")),
            Ok(_) => panic!("expected ValidationException"),
        }
    }
}
