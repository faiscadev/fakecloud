//! `GetSecretValue`'s read path, shared by the Secrets Manager API and by
//! other services that resolve a secret on a caller's behalf (ECS task
//! `secrets[].valueFrom`).

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use http::StatusCode;

use fakecloud_core::delivery::KmsHook;
use fakecloud_core::service::AwsServiceError;

use crate::service::secret_recovery_window_elapsed;
use crate::service::{resource_policy_allows, secret_not_found, secret_owner_account};
use crate::state::SharedSecretsManagerState;

/// One secret version's value, as `GetSecretValue` returns it. The
/// `secret_string` is plaintext: a value stored KMS-encrypted is decrypted.
#[derive(Debug, Clone)]
pub struct SecretValue {
    pub arn: String,
    pub name: String,
    pub version_id: String,
    pub version_stages: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub secret_string: Option<String>,
    pub secret_binary: Option<Vec<u8>>,
}

/// Read a secret's value the way `GetSecretValue` does, as `caller_account`.
///
/// `secret_id` is a name (looked up in `caller_account` and `caller_region`,
/// the region of the request, task or build resolving it), a full ARN or a
/// partial ARN (looked up in the ARN's account and region, which is how ECS
/// and CodeBuild reach a secret in another region; the Secrets Manager API
/// itself refuses an ARN of another region before calling this).
/// `version_id` / `version_stage` select the version; with neither,
/// the `AWSCURRENT` version is read. A cross-account read needs the secret's
/// resource policy to allow the caller. Marks the secret as accessed.
///
/// `caller_arn` is the identity reading it (an IAM user or assumed role),
/// whom KMS names if the secret's key refuses Secrets Manager; `None` when
/// the reader acts as the account (a service resolving a secret for a task).
#[allow(clippy::too_many_arguments)]
pub fn read_secret_value(
    state: &SharedSecretsManagerState,
    kms_hook: Option<&dyn KmsHook>,
    caller_account: &str,
    caller_region: &str,
    secret_id: &str,
    version_id: Option<&str>,
    version_stage: Option<&str>,
    caller_arn: Option<&str>,
) -> Result<SecretValue, AwsServiceError> {
    let owner_account = secret_owner_account(secret_id, caller_account);
    let owner_region = if fakecloud_aws::arn::arn_resource(secret_id, "secretsmanager").is_some() {
        fakecloud_aws::arn::region_of(secret_id).unwrap_or(caller_region)
    } else {
        caller_region
    };
    let (value, kms_key_id) = {
        let mut accounts = state.write();
        let state = accounts
            .regional_get_mut(&owner_account, owner_region)
            .ok_or_else(secret_not_found)?;
        let key = state
            .secret_key(secret_id)
            .filter(|key| {
                !state
                    .secrets
                    .get(key)
                    .is_some_and(|s| secret_recovery_window_elapsed(s, Utc::now()))
            })
            .ok_or_else(secret_not_found)?;
        let secret = state.secrets.get_mut(&key).ok_or_else(secret_not_found)?;

        if owner_account != caller_account {
            let policy_doc = secret.resource_policy.as_deref().unwrap_or("");
            if !resource_policy_allows(policy_doc, caller_account, &secret.arn) {
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

        let requested_stage = version_stage.unwrap_or("AWSCURRENT");
        let resolved_id = match version_id {
            Some(id) => id.to_string(),
            None => secret
                .versions
                .iter()
                .find(|(_, v)| v.stages.iter().any(|s| s == requested_stage))
                .map(|(id, _)| id.clone())
                .ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ResourceNotFoundException",
                        format!(
                            "Secrets Manager can't find the specified secret value for staging label: {requested_stage}"
                        ),
                    )
                })?,
        };
        let version = secret.versions.get(&resolved_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!(
                    "Secrets Manager can't find the specified secret value for VersionId: {resolved_id}"
                ),
            )
        })?;
        if version_id.is_some() {
            if let Some(stage) = version_stage {
                if !version.stages.iter().any(|s| s == stage) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ResourceNotFoundException",
                        "You provided a VersionStage that is not associated to the provided VersionId.",
                    ));
                }
            }
        }

        let value = SecretValue {
            arn: secret.arn.clone(),
            name: secret.name.clone(),
            version_id: version.version_id.clone(),
            version_stages: version.stages.clone(),
            created_at: version.created_at,
            secret_string: version.secret_string.clone(),
            secret_binary: version.secret_binary.clone(),
        };
        secret.last_accessed_at = Some(Utc::now());
        (value, secret.kms_key_id.clone())
    };

    let secret_string = value
        .secret_string
        .map(|stored| {
            decrypt_secret_string(
                kms_hook,
                &owner_account,
                &value.arn,
                &value.version_id,
                kms_key_id.as_deref(),
                stored,
                caller_arn,
            )
        })
        .transpose()?;
    Ok(SecretValue {
        secret_string,
        ..value
    })
}

/// Decrypt a stored `SecretString`. A secret written while a KMS hook was
/// wired is stored as ciphertext, under its `KmsKeyId` or, with none, the
/// AWS-managed `aws/secretsmanager` key; anything else (no hook, or a value
/// that is not a ciphertext, such as one stored before KMS was wired or
/// before secrets without a key were encrypted) is returned as stored. A
/// KMS refusal fails the read.
#[allow(clippy::too_many_arguments)]
pub(crate) fn decrypt_secret_string(
    kms_hook: Option<&dyn KmsHook>,
    account_id: &str,
    secret_arn: &str,
    version_id: &str,
    _kms_key_id: Option<&str>,
    stored: String,
    caller_arn: Option<&str>,
) -> Result<String, AwsServiceError> {
    let Some(hook) = kms_hook else {
        return Ok(stored);
    };
    match hook.decrypt(
        account_id,
        &stored,
        fakecloud_core::delivery::KmsCaller::via_for("secretsmanager.amazonaws.com", caller_arn),
        secret_encryption_context(secret_arn, version_id),
    ) {
        Ok(bytes) => Ok(String::from_utf8_lossy(&bytes).to_string()),
        Err(err) if err.kind == fakecloud_core::delivery::KmsErrorKind::InvalidCiphertext => {
            Ok(stored)
        }
        Err(err) => Err(secrets_manager_kms_error(&err, true)),
    }
}

/// The plaintext of a secret version's stored `SecretString`, decrypted for a
/// service that reads it for a resource (CodeBuild resolving a build's
/// environment), with that resource's role: an identity of the account. A
/// value KMS refuses reads as empty, as the resource would get nothing.
pub fn plaintext_secret_string(
    kms_hook: Option<&dyn KmsHook>,
    account_id: &str,
    secret_arn: &str,
    version_id: &str,
    stored: String,
) -> String {
    decrypt_secret_string(
        kms_hook, account_id, secret_arn, version_id, None, stored, None,
    )
    .unwrap_or_else(|err| {
        tracing::warn!(secret_arn, error = %err.message(), "secret value not readable");
        String::new()
    })
}

/// Seal `plaintext` as version `version_id` of secret `secret_arn` (in
/// `account_id` and `region`) for a service writing it for a resource
/// (CloudFormation provisioning `AWS::SecretsManager::Secret`), as
/// `CreateSecret` / `PutSecretValue` store it: under `kms_key_id`, or the
/// AWS-managed `aws/secretsmanager` key when none is set. Returned as is
/// when no KMS hook is wired.
pub fn seal_secret_string(
    kms_hook: Option<&dyn KmsHook>,
    account_id: &str,
    region: &str,
    secret_arn: &str,
    version_id: &str,
    kms_key_id: Option<&str>,
    plaintext: String,
) -> Result<String, AwsServiceError> {
    crate::service::encrypt_secret_string(
        kms_hook,
        account_id,
        region,
        secret_arn,
        version_id,
        kms_key_id,
        Some(plaintext.clone()),
        None,
    )
    .map(|sealed| sealed.unwrap_or(plaintext))
}

/// The encryption context Secrets Manager binds a secret version to (Secrets
/// Manager user guide, "Encryption context").
pub(crate) fn secret_encryption_context(
    secret_arn: &str,
    version_id: &str,
) -> HashMap<String, String> {
    [
        ("SecretARN".to_string(), secret_arn.to_string()),
        ("SecretVersionId".to_string(), version_id.to_string()),
    ]
    .into()
}

/// Secrets Manager's error for a failed KMS call. Recorded (CreateSecret):
/// a key policy refusal is 400 `AccessDeniedException` "Access to KMS is not
/// allowed", and a missing key 400 `InvalidParameterException` "The
/// operation failed because of an invalid KMS key: <KMS message>". A key
/// KMS will not use (disabled, pending deletion, wrong usage) is the
/// declared `EncryptionFailure` (writes) / `DecryptionFailure` (reads), with
/// KMS's message (not recorded).
pub(crate) fn secrets_manager_kms_error(
    err: &fakecloud_core::delivery::KmsError,
    decrypting: bool,
) -> AwsServiceError {
    use fakecloud_core::delivery::KmsErrorKind;
    match err.kind {
        KmsErrorKind::AccessDenied => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "AccessDeniedException",
            "Access to KMS is not allowed",
        ),
        KmsErrorKind::NotFound if !decrypting => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterException",
            format!(
                "The operation failed because of an invalid KMS key: {}",
                err.relayed_message()
            ),
        ),
        KmsErrorKind::Internal => AwsServiceError::aws_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalServiceError",
            err.relayed_message(),
        ),
        _ => AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            if decrypting {
                "DecryptionFailure"
            } else {
                "EncryptionFailure"
            },
            err.relayed_message(),
        ),
    }
}
