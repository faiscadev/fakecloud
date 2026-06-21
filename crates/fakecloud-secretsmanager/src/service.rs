use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use tokio::sync::Mutex as AsyncMutex;

use fakecloud_aws::arn::Arn;
use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_core::validation::*;
use fakecloud_persistence::SnapshotStore;

use crate::state::{
    RotationRules, Secret, SecretVersion, SecretsManagerSnapshot, SecretsManagerState,
    SharedSecretsManagerState, SECRETSMANAGER_SNAPSHOT_SCHEMA_VERSION,
};

/// Information needed to invoke the rotation Lambda after releasing state lock.
struct RotationInvocation {
    lambda_arn: String,
    secret_id: String,
    client_request_token: String,
}

/// Result of an idempotency check against an existing
/// `ClientRequestToken` / version id.
pub(crate) enum VersionIdempotency {
    /// The version id isn't in the secret yet — this is a fresh write.
    NotFound,
    /// The version id exists and stores the exact same payload we're
    /// about to write — callers should return the existing version as
    /// a successful no-op response.
    Match,
    /// The version id exists but stores a different payload — AWS
    /// surfaces this as a `ResourceExistsException`.
    Conflict,
}

pub struct SecretsManagerService {
    state: SharedSecretsManagerState,
    delivery_bus: Option<Arc<DeliveryBus>>,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
    kms_hook: Option<Arc<dyn fakecloud_core::delivery::KmsHook>>,
}

impl SecretsManagerService {
    pub fn new(state: SharedSecretsManagerState) -> Self {
        Self {
            state,
            delivery_bus: None,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
            kms_hook: None,
        }
    }

    pub fn with_delivery(mut self, delivery_bus: Arc<DeliveryBus>) -> Self {
        self.delivery_bus = Some(delivery_bus);
        self
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    pub fn with_kms_hook(mut self, hook: Arc<dyn fakecloud_core::delivery::KmsHook>) -> Self {
        self.kms_hook = Some(hook);
        self
    }

    fn maybe_encrypt_secret_string(
        &self,
        account_id: &str,
        region: &str,
        secret_arn: &str,
        kms_key_id: Option<&str>,
        plaintext: Option<String>,
    ) -> Option<String> {
        let pt = plaintext?;
        let (Some(hook), Some(key)) = (&self.kms_hook, kms_key_id) else {
            return Some(pt);
        };
        let key = if key.is_empty() {
            "aws/secretsmanager"
        } else {
            key
        };
        let mut ctx = HashMap::new();
        ctx.insert(
            "aws:secretsmanager:secretArn".to_string(),
            secret_arn.to_string(),
        );
        match hook.encrypt(
            account_id,
            region,
            key,
            pt.as_bytes(),
            "secretsmanager.amazonaws.com",
            ctx,
        ) {
            Ok(ciphertext) => Some(ciphertext),
            Err(err) => {
                tracing::warn!(
                    secret_arn = %secret_arn,
                    error = %err,
                    "KMS encrypt failed for secret; storing plaintext"
                );
                Some(pt)
            }
        }
    }

    fn maybe_decrypt_secret_string(
        &self,
        account_id: &str,
        secret_arn: &str,
        kms_key_id: Option<&str>,
        stored: Option<&str>,
    ) -> Option<String> {
        let stored = stored?;
        let (Some(hook), Some(_)) = (&self.kms_hook, kms_key_id) else {
            return Some(stored.to_string());
        };
        let mut ctx = HashMap::new();
        ctx.insert(
            "aws:secretsmanager:secretArn".to_string(),
            secret_arn.to_string(),
        );
        match hook.decrypt(account_id, stored, "secretsmanager.amazonaws.com", ctx) {
            Ok(bytes) => Some(String::from_utf8_lossy(&bytes).to_string()),
            Err(_) => Some(stored.to_string()),
        }
    }

    /// Persist current state as a snapshot. Held across the
    /// clone-serialize-write sequence to prevent stale-last writes,
    /// with serde + file I/O offloaded to the blocking pool.
    async fn save_snapshot(&self) {
        save_secretsmanager_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Build a hook that persists the current Secrets Manager state when
    /// invoked, or `None` in memory mode (no snapshot store). The
    /// CloudFormation provisioner mutates `state` directly and uses this to
    /// write a CFN-provisioned secret through to disk, the same way a direct
    /// mutating API call would.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_secretsmanager_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    fn create_secret(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let input = CreateSecretInput::from_body(&req.json_body())?;
        let has_value = input.secret_string.is_some() || input.secret_binary.is_some();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if let Some(existing) = state.secrets.get(&input.name) {
            if let Some(ref token) = input.client_request_token {
                let existing_plaintext = existing.versions.get(token).and_then(|v| {
                    self.maybe_decrypt_secret_string(
                        &req.account_id,
                        &existing.arn,
                        existing.kms_key_id.as_deref(),
                        v.secret_string.as_deref(),
                    )
                });
                match check_secret_version_idempotency(
                    &existing.versions,
                    token,
                    existing_plaintext,
                    &input.secret_string,
                    &input.secret_binary,
                ) {
                    VersionIdempotency::Match => {
                        let mut response = json!({
                            "ARN": existing.arn,
                            "Name": existing.name,
                            "VersionId": token,
                        });
                        if !has_value {
                            response.as_object_mut().unwrap().remove("VersionId");
                        }
                        return Ok(AwsResponse::ok_json(response));
                    }
                    VersionIdempotency::Conflict => {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "ResourceExistsException",
                            format!(
                                "You can't use ClientRequestToken {token} because that value is already in use for a version of secret {}.",
                                existing.arn
                            ),
                        ));
                    }
                    VersionIdempotency::NotFound => {}
                }
            }
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceExistsException",
                format!(
                    "The operation failed because the secret {} already exists.",
                    input.name
                ),
            ));
        }

        let arn = format!(
            "arn:aws:secretsmanager:{}:{}:secret:{}-{}",
            req.region,
            req.account_id,
            input.name,
            &uuid::Uuid::new_v4().to_string()[..6]
        );

        let now = Utc::now();

        let (versions, current_version_id, version_id_for_response) = if has_value {
            let vid = input
                .client_request_token
                .clone()
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let stored_string = self.maybe_encrypt_secret_string(
                &req.account_id,
                &req.region,
                &arn,
                input.kms_key_id.as_deref(),
                input.secret_string,
            );
            let version = SecretVersion {
                version_id: vid.clone(),
                secret_string: stored_string,
                secret_binary: input.secret_binary,
                stages: vec!["AWSCURRENT".to_string()],
                created_at: now,
            };
            let mut versions = std::collections::BTreeMap::new();
            versions.insert(vid.clone(), version);
            (versions, Some(vid.clone()), Some(vid))
        } else {
            (std::collections::BTreeMap::new(), None, None)
        };

        let tags_ever_set = !input.tags.is_empty();
        let secret = Secret {
            name: input.name.clone(),
            arn: arn.clone(),
            description: input.description,
            kms_key_id: input.kms_key_id,
            versions,
            current_version_id,
            tags: input.tags,
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
            replica_regions: Vec::new(),
        };

        state.secrets.insert(input.name.clone(), secret);

        let mut response = json!({
            "ARN": arn,
            "Name": input.name,
        });
        if let Some(vid) = version_id_for_response {
            response["VersionId"] = json!(vid);
        }

        Ok(AwsResponse::ok_json(response))
    }

    fn get_secret_value(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let secret_id = require_secret_id(&body)?;
        validate_optional_string_length("versionId", body["VersionId"].as_str(), 32, 64)?;
        validate_optional_string_length("versionStage", body["VersionStage"].as_str(), 1, 256)?;

        // Resolve owning account from an ARN form. Cross-account
        // GetSecretValue then evaluates `secret.resource_policy` via
        // the IAM evaluator before returning the value.
        let owner_account = secret_owner_account(&secret_id, &req.account_id);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&owner_account);
        let secret = self.find_secret_mut(state, &secret_id)?;
        if owner_account != req.account_id {
            let policy_doc = secret.resource_policy.as_deref().unwrap_or("");
            let secret_arn = secret.arn.clone();
            if !resource_policy_allows(policy_doc, &req.account_id, &secret_arn) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::FORBIDDEN,
                    "AccessDeniedException",
                    "User is not authorized to perform: secretsmanager:GetSecretValue on the requested resource",
                ));
            }
        }

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

        let kms_for_decrypt = secret.kms_key_id.clone();
        let arn_for_decrypt = secret.arn.clone();
        if let Some(ref s) = version.secret_string {
            let plaintext = self
                .maybe_decrypt_secret_string(
                    &req.account_id,
                    &arn_for_decrypt,
                    kms_for_decrypt.as_deref(),
                    Some(s.as_str()),
                )
                .unwrap_or_else(|| s.clone());
            response["SecretString"] = json!(plaintext);
        }
        if let Some(ref b) = version.secret_binary {
            response["SecretBinary"] = json!(base64_encode(b));
        }

        Ok(AwsResponse::ok_json(response))
    }

    fn put_secret_value(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let secret = match self.find_secret_mut(state, &secret_id) {
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

        let existing_plaintext = secret.versions.get(&version_id).and_then(|v| {
            self.maybe_decrypt_secret_string(
                &req.account_id,
                &secret.arn,
                secret.kms_key_id.as_deref(),
                v.secret_string.as_deref(),
            )
        });
        match check_secret_version_idempotency(
            &secret.versions,
            &version_id,
            existing_plaintext,
            &secret_string,
            &secret_binary,
        ) {
            VersionIdempotency::Match => {
                let existing_stages = secret.versions[&version_id].stages.clone();
                return Ok(AwsResponse::ok_json(json!({
                    "ARN": secret.arn,
                    "Name": secret.name,
                    "VersionId": version_id,
                    "VersionStages": existing_stages,
                })));
            }
            VersionIdempotency::Conflict => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceExistsException",
                    format!(
                        "You can't use ClientRequestToken {version_id} because that value is already in use for a version of secret {}.",
                        secret.arn
                    ),
                ));
            }
            VersionIdempotency::NotFound => {}
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

        let kms_key_for_enc = secret.kms_key_id.clone();
        let arn_for_enc = secret.arn.clone();
        let stored_secret_string = self.maybe_encrypt_secret_string(
            &req.account_id,
            &req.region,
            &arn_for_enc,
            kms_key_for_enc.as_deref(),
            secret_string,
        );
        let version = SecretVersion {
            version_id: version_id.clone(),
            secret_string: stored_secret_string,
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

        Ok(AwsResponse::ok_json(response))
    }

    fn update_secret(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let secret = match self.find_secret_mut(state, &secret_id) {
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

            let existing_plaintext = secret.versions.get(&vid).and_then(|v| {
                self.maybe_decrypt_secret_string(
                    &req.account_id,
                    &secret.arn,
                    secret.kms_key_id.as_deref(),
                    v.secret_string.as_deref(),
                )
            });
            match check_secret_version_idempotency(
                &secret.versions,
                &vid,
                existing_plaintext,
                &secret_string,
                &secret_binary,
            ) {
                VersionIdempotency::Match => {
                    return Ok(AwsResponse::ok_json(json!({
                        "ARN": secret.arn,
                        "Name": secret.name,
                        "VersionId": vid,
                    })));
                }
                VersionIdempotency::Conflict => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ResourceExistsException",
                        format!(
                            "You can't use ClientRequestToken {vid} because that value is already in use for a version of secret {}.",
                            secret.arn
                        ),
                    ));
                }
                VersionIdempotency::NotFound => {}
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

        Ok(AwsResponse::ok_json(response))
    }

    fn delete_secret(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if force_delete {
            // Force delete: if secret doesn't exist, create a fake response
            match self.find_secret_mut(state, &secret_id) {
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
                    return Ok(AwsResponse::ok_json(response));
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
                    return Ok(AwsResponse::ok_json(response));
                }
            }
        }

        let secret = self.find_secret_mut(state, &secret_id)?;

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

        Ok(AwsResponse::ok_json(response))
    }

    fn restore_secret(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let secret_id = require_secret_id(&body)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let secret = self.find_secret_mut(state, &secret_id)?;

        // AWS allows restoring a secret that is not deleted (no-op)
        secret.deleted = false;
        secret.deletion_date = None;

        let response = json!({
            "ARN": secret.arn,
            "Name": secret.name,
        });

        Ok(AwsResponse::ok_json(response))
    }

    fn describe_secret(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let secret_id = require_secret_id(&body)?;

        let accounts = self.state.read();
        let empty = SecretsManagerState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let secret = self.find_secret_ref(state, &secret_id)?;

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
        if !secret.replica_regions.is_empty() {
            response["ReplicationStatus"] = replication_status_json(&secret.replica_regions);
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

        Ok(AwsResponse::ok_json(response))
    }

    fn list_secrets(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("nextToken", body["NextToken"].as_str(), 1, 4096)?;
        validate_optional_range_i64("maxResults", body["MaxResults"].as_i64(), 1, 100)?;
        validate_optional_enum("sortBy", body["SortBy"].as_str(), &["name", "created-date"])?;
        validate_optional_enum("sortOrder", body["SortOrder"].as_str(), &["asc", "desc"])?;
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

        let accounts = self.state.read();
        let empty = SecretsManagerState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

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
        secrets.sort_by_key(|a| a.created_at);

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
                // AWS always echoes `Description` and `SecretVersionsToStages`
                // on every SecretListEntry. The documented `@examples` for
                // ListSecrets relies on the fields being present even when
                // empty, and SDK consumers index into them unconditionally.
                let mut version_ids_to_stages: serde_json::Map<String, Value> =
                    serde_json::Map::new();
                for (vid, version) in &s.versions {
                    version_ids_to_stages.insert(vid.clone(), json!(version.stages));
                }
                let mut entry = json!({
                    "ARN": s.arn,
                    "Name": s.name,
                    "CreatedDate": s.created_at.timestamp_millis() as f64 / 1000.0,
                    "LastChangedDate": s.last_changed_at.timestamp_millis() as f64 / 1000.0,
                    "Description": s.description.clone().unwrap_or_default(),
                    "SecretVersionsToStages": Value::Object(version_ids_to_stages),
                });

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

        Ok(AwsResponse::ok_json(response))
    }

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let secret_id = require_secret_id(&body)?;

        let new_tags = parse_tags(&body["Tags"]);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let secret = self.find_secret_mut(state, &secret_id)?;

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
        let body = req.json_body();
        let secret_id = require_secret_id(&body)?;

        let tag_keys: Vec<String> = body["TagKeys"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let secret = self.find_secret_mut(state, &secret_id)?;

        secret.tags.retain(|(k, _)| !tag_keys.contains(k));

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_secret_version_ids(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let secret_id = require_secret_id(&body)?;
        validate_optional_string_length("nextToken", body["NextToken"].as_str(), 1, 4096)?;

        let accounts = self.state.read();
        let empty = SecretsManagerState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let secret = self.find_secret_ref(state, &secret_id)?;

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

        Ok(AwsResponse::ok_json(response))
    }

    fn get_random_password(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
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
        validate_optional_string_length(
            "excludeCharacters",
            body["ExcludeCharacters"].as_str(),
            0,
            4096,
        )?;
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

        Ok(AwsResponse::ok_json(response))
    }

    fn rotate_secret(
        &self,
        req: &AwsRequest,
    ) -> Result<(AwsResponse, Option<RotationInvocation>), AwsServiceError> {
        let body = req.json_body();
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let secret = self.find_secret_mut(state, &secret_id)?;

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
        let lambda_arn = secret.rotation_lambda_arn.clone();

        // If the secret has a value, perform rotation
        let mut invocation = None;
        if let Some(current_vid) = secret.current_version_id.clone() {
            let current_value = secret.versions.get(&current_vid).cloned();

            if let Some(cv) = current_value {
                if has_lambda {
                    // With Lambda: do NOT pre-create the AWSPENDING version. The
                    // rotation Lambda is responsible for putting the new value via
                    // PutSecretValue with VersionStages=[AWSPENDING] during the
                    // createSecret step (matching real AWS Secrets Manager behavior).

                    // Schedule Lambda invocation
                    if let Some(ref arn) = lambda_arn {
                        invocation = Some(RotationInvocation {
                            lambda_arn: arn.clone(),
                            secret_id: secret.arn.clone(),
                            client_request_token: version_id.clone(),
                        });
                    }
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

        Ok((AwsResponse::ok_json(response), invocation))
    }

    fn cancel_rotate_secret(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let secret_id = require_secret_id(&body)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let secret = self.find_secret_mut(state, &secret_id)?;

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

        Ok(AwsResponse::ok_json(response))
    }

    fn update_secret_version_stage(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let secret = self.find_secret_mut(state, &secret_id)?;

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

        Ok(AwsResponse::ok_json(response))
    }

    fn batch_get_secret_value(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
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

        let accounts = self.state.read();
        let empty = SecretsManagerState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let mut secret_values: Vec<Value> = Vec::new();
        let mut errors: Vec<Value> = Vec::new();

        if let Some(id_list) = secret_id_list {
            for id_val in id_list {
                let sid = id_val.as_str().unwrap_or("");
                match self.find_secret_ref(state, sid) {
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
                                    // Decrypt the same way GetSecretValue does;
                                    // pushing the raw stored value leaked
                                    // ciphertext when a KMS hook + KmsKeyId were
                                    // configured (bug-audit 2026-06-20, 1.10).
                                    let plaintext = self
                                        .maybe_decrypt_secret_string(
                                            &req.account_id,
                                            &secret.arn,
                                            secret.kms_key_id.as_deref(),
                                            Some(s.as_str()),
                                        )
                                        .unwrap_or_else(|| s.clone());
                                    entry["SecretString"] = json!(plaintext);
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
                            // Decrypt like GetSecretValue; the raw stored value
                            // is ciphertext under a configured KMS hook (1.10).
                            let plaintext = self
                                .maybe_decrypt_secret_string(
                                    &req.account_id,
                                    &secret.arn,
                                    secret.kms_key_id.as_deref(),
                                    Some(s.as_str()),
                                )
                                .unwrap_or_else(|| s.clone());
                            entry["SecretString"] = json!(plaintext);
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

        Ok(AwsResponse::ok_json(response))
    }

    fn get_resource_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let secret_id = require_secret_id(&body)?;

        let accounts = self.state.read();
        let empty = SecretsManagerState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let secret = self.find_secret_ref(state, &secret_id)?;

        // Real AWS omits ResourcePolicy when none is attached; terraform
        // provider and the SDK choke on an empty-string policy.
        let mut response = json!({
            "ARN": secret.arn,
            "Name": secret.name,
        });
        if let Some(ref policy) = secret.resource_policy {
            response["ResourcePolicy"] = json!(policy);
        }

        Ok(AwsResponse::ok_json(response))
    }

    fn validate_resource_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("secretId", body["SecretId"].as_str(), 1, 2048)?;
        validate_required("ResourcePolicy", &body["ResourcePolicy"])?;
        let policy_str = body["ResourcePolicy"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "ResourcePolicy must be a string",
            )
        })?;
        validate_string_length("resourcePolicy", policy_str, 1, 20480)?;

        // If SecretId is provided, verify the secret exists
        if let Some(secret_id) = body["SecretId"].as_str() {
            let accounts = self.state.read();
            let empty = SecretsManagerState::new(&req.account_id, &req.region);
            let state = accounts.get(&req.account_id).unwrap_or(&empty);
            self.find_secret_key(state, secret_id)?;
        }

        let response = json!({
            "PolicyValidationPassed": true,
            "ValidationErrors": [],
        });
        Ok(AwsResponse::ok_json(response))
    }

    fn put_resource_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let secret_id = require_secret_id(&body)?;
        validate_required("ResourcePolicy", &body["ResourcePolicy"])?;
        validate_optional_string_length(
            "resourcePolicy",
            body["ResourcePolicy"].as_str(),
            1,
            20480,
        )?;
        let policy = body["ResourcePolicy"].as_str().map(|s| s.to_string());

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let secret = self.find_secret_mut(state, &secret_id)?;
        secret.resource_policy = policy;

        let response = json!({
            "ARN": secret.arn,
            "Name": secret.name,
        });

        Ok(AwsResponse::ok_json(response))
    }

    fn delete_resource_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let secret_id = require_secret_id(&body)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let secret = self.find_secret_mut(state, &secret_id)?;
        secret.resource_policy = None;

        let response = json!({
            "ARN": secret.arn,
            "Name": secret.name,
        });

        Ok(AwsResponse::ok_json(response))
    }

    fn replicate_secret_to_regions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let secret_id = require_secret_id(&body)?;
        // AddReplicaRegions[].Region — the regions to replicate into.
        let add_regions: Vec<String> = body["AddReplicaRegions"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|r| r["Region"].as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let secret = self.find_secret_mut(state, &secret_id)?;
        for region in add_regions {
            if !secret.replica_regions.contains(&region) {
                secret.replica_regions.push(region);
            }
        }
        let response = json!({
            "ARN": secret.arn,
            "ReplicationStatus": replication_status_json(&secret.replica_regions),
        });
        Ok(AwsResponse::ok_json(response))
    }

    fn remove_regions_from_replication(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let secret_id = require_secret_id(&body)?;
        let remove_regions: Vec<String> = body["RemoveReplicaRegions"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|r| r.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let secret = self.find_secret_mut(state, &secret_id)?;
        secret
            .replica_regions
            .retain(|r| !remove_regions.contains(r));
        let response = json!({
            "ARN": secret.arn,
            "ReplicationStatus": replication_status_json(&secret.replica_regions),
        });
        Ok(AwsResponse::ok_json(response))
    }

    fn stop_replication_to_replica(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let secret_id = require_secret_id(&body)?;

        let accounts = self.state.read();
        let empty = SecretsManagerState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let secret = self.find_secret_ref(state, &secret_id)?;

        let response = json!({
            "ARN": secret.arn,
        });
        Ok(AwsResponse::ok_json(response))
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

/// Persist the current Secrets Manager state as a snapshot. Offloads the
/// serialization and blocking file write to the Tokio blocking pool. Noop when
/// `store` is `None` (memory mode). Shared by
/// `SecretsManagerService::save_snapshot` and
/// the CloudFormation provisioner's post-provision persist hook so both route
/// through the same serialize-and-write path.
pub async fn save_secretsmanager_snapshot(
    state: &SharedSecretsManagerState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = SecretsManagerSnapshot {
        schema_version: SECRETSMANAGER_SNAPSHOT_SCHEMA_VERSION,
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
        Ok(Err(err)) => tracing::error!(%err, "failed to write secretsmanager snapshot"),
        Err(err) => tracing::error!(%err, "secretsmanager snapshot task panicked"),
    }
}

/// Parsed + validated inputs for `CreateSecret`.
struct CreateSecretInput {
    name: String,
    client_request_token: Option<String>,
    description: Option<String>,
    kms_key_id: Option<String>,
    secret_string: Option<String>,
    secret_binary: Option<Vec<u8>>,
    tags: Vec<(String, String)>,
}

impl CreateSecretInput {
    fn from_body(body: &Value) -> Result<Self, AwsServiceError> {
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

        Ok(Self {
            name,
            client_request_token: body["ClientRequestToken"].as_str().map(|s| s.to_string()),
            description: body["Description"].as_str().map(|s| s.to_string()),
            kms_key_id: body["KmsKeyId"].as_str().map(|s| s.to_string()),
            secret_string: body["SecretString"].as_str().map(|s| s.to_string()),
            secret_binary: body["SecretBinary"].as_str().and_then(base64_decode),
            tags: parse_tags(&body["Tags"]),
        })
    }
}

#[async_trait]
impl AwsService for SecretsManagerService {
    fn service_name(&self) -> &str {
        "secretsmanager"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = is_mutating_action(req.action.as_str());
        let result = match req.action.as_str() {
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
            "RotateSecret" => {
                let (response, invocation) = self.rotate_secret(&req)?;
                if let Some(inv) = invocation {
                    if let Some(ref bus) = self.delivery_bus {
                        let bus = bus.clone();
                        // AWS invokes the rotation Lambda asynchronously for each step.
                        tokio::spawn(async move {
                            for step in &["createSecret", "setSecret", "testSecret", "finishSecret"]
                            {
                                let payload = serde_json::json!({
                                    "SecretId": inv.secret_id,
                                    "ClientRequestToken": inv.client_request_token,
                                    "Step": step,
                                });
                                let payload_str = payload.to_string();
                                match bus.invoke_lambda(&inv.lambda_arn, &payload_str).await {
                                    Some(Ok(_)) => {}
                                    Some(Err(e)) => {
                                        tracing::warn!(
                                            step = step,
                                            error = %e,
                                            "rotation Lambda invocation failed"
                                        );
                                    }
                                    None => {
                                        tracing::warn!(
                                            lambda_arn = %inv.lambda_arn,
                                            step = step,
                                            "rotation Lambda delivery not configured; \
                                             Lambda invocation skipped"
                                        );
                                        break;
                                    }
                                }
                            }
                        });
                    }
                }
                Ok(response)
            }
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
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result.map_err(remap_validation_error)
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

#[path = "service_helpers.rs"]
mod service_helpers;
pub(crate) use service_helpers::*;

/// The shared `fakecloud_core::validation` helpers raise
/// `ValidationException`, but the Secrets Manager Smithy model does not
/// declare `ValidationException` on any operation — its declared input
/// error is `InvalidParameterException`. Translate at the dispatcher
/// boundary so the wire-level error code matches real AWS without
/// duplicating every validator.
/// Build the `ReplicationStatus` array from a secret's replica regions.
/// Each replica reports `InSync`, matching a healthy replication.
fn replication_status_json(regions: &[String]) -> Value {
    Value::Array(
        regions
            .iter()
            .map(|r| {
                json!({
                    "Region": r,
                    "Status": "InSync",
                    "StatusMessage": "Replication succeeded",
                })
            })
            .collect(),
    )
}

fn remap_validation_error(err: AwsServiceError) -> AwsServiceError {
    match err {
        AwsServiceError::AwsError {
            status,
            code,
            message,
            extra_fields,
            headers,
        } if code == "ValidationException" => AwsServiceError::AwsError {
            status,
            code: "InvalidParameterException".to_string(),
            message,
            extra_fields,
            headers,
        },
        other => other,
    }
}

/// Extract the owning account-id from an `arn:aws:secretsmanager:...:ACCOUNT:secret:...`
/// secret id. Returns `caller_account` when the input is a bare name
/// or a same-account ARN.
fn secret_owner_account(secret_id: &str, caller_account: &str) -> String {
    if !secret_id.starts_with("arn:aws:secretsmanager:") {
        return caller_account.to_string();
    }
    let parts: Vec<&str> = secret_id.splitn(7, ':').collect();
    if parts.len() < 5 {
        return caller_account.to_string();
    }
    let account = parts[4];
    if account.is_empty() {
        caller_account.to_string()
    } else {
        account.to_string()
    }
}

/// Evaluate a Secrets Manager resource policy against a cross-account
/// caller. Empty policy implicitly denies (real AWS behaviour). Uses
/// the shared IAM evaluator so the same policy semantics apply
/// service-wide.
fn resource_policy_allows(policy_doc: &str, caller_account: &str, secret_arn: &str) -> bool {
    if policy_doc.is_empty() {
        return false;
    }
    use fakecloud_core::auth::{Principal, PrincipalType};
    use fakecloud_iam::evaluator::{evaluate, EvalRequest, PolicyDocument};
    let doc = PolicyDocument::parse(policy_doc);
    let principal_arn = Arn::global("iam", caller_account, "root").to_string();
    let principal = Principal {
        arn: principal_arn.clone(),
        user_id: principal_arn.clone(),
        account_id: caller_account.to_string(),
        principal_type: PrincipalType::User,
        source_identity: None,
        tags: None,
    };
    let req = EvalRequest {
        principal: &principal,
        action: "secretsmanager:GetSecretValue".to_string(),
        resource: secret_arn.to_string(),
        context: Default::default(),
    };
    matches!(
        evaluate(&[doc], &req),
        fakecloud_iam::evaluator::Decision::Allow
    )
}

#[cfg(test)]
#[path = "service_tests.rs"]
mod tests;
