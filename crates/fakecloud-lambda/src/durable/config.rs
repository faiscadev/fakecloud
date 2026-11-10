//! A function's `DurableConfig`: validation on `CreateFunction` /
//! `UpdateFunctionConfiguration`, and the customer managed KMS key checks.
//!
//! Messages are the ones Lambda answered in the recordings
//! (`tests/fixtures/durable/aws-round3*.jsonl`).

use http::StatusCode;
use serde_json::Value;

use fakecloud_core::delivery::{KmsHook, KmsKeyUnusable};
use fakecloud_core::service::AwsServiceError;

use super::model::normalize_durable_config;
use super::validate::invalid_parameter;

const MAX_EXECUTION_TIMEOUT: i64 = 31_622_400;
const MAX_RETENTION_DAYS: i64 = 90;
const KMS_KEY_PATTERN: &str = "(arn:(aws[a-zA-Z-]*)?:[a-z0-9-.]+:.*)|()";

/// The service principal that encrypts durable execution data.
pub(crate) const LAMBDA_PRINCIPAL: &str = "lambda.amazonaws.com";

fn violation(value: &str, member: &str, constraint: String) -> String {
    format!(
        "Value '{value}' at 'durableConfig.{member}' failed to satisfy constraint: {constraint}"
    )
}

/// The model constraints of `DurableConfig`.
fn check_bounds(cfg: &Value) -> Result<(), AwsServiceError> {
    let mut errors = Vec::new();
    let mut range = |key: &str, member: &str, max: i64| {
        if let Some(n) = cfg.get(key).and_then(Value::as_i64) {
            if n < 1 {
                errors.push(violation(
                    &n.to_string(),
                    member,
                    "Member must have value greater than or equal to 1".to_string(),
                ));
            } else if n > max {
                errors.push(violation(
                    &n.to_string(),
                    member,
                    format!("Member must have value less than or equal to {max}"),
                ));
            }
        }
    };
    range(
        "ExecutionTimeout",
        "executionTimeout",
        MAX_EXECUTION_TIMEOUT,
    );
    range(
        "RetentionPeriodInDays",
        "retentionPeriodInDays",
        MAX_RETENTION_DAYS,
    );
    if let Some(k) = cfg.get("KMSKeyArn").and_then(Value::as_str) {
        if !(k.is_empty() || is_arn(k)) {
            errors.push(violation(
                k,
                "kMSKeyArn",
                format!("Member must satisfy regular expression pattern: {KMS_KEY_PATTERN}"),
            ));
        }
    }
    if errors.is_empty() {
        return Ok(());
    }
    let count = if errors.len() == 1 {
        "1 validation error detected".to_string()
    } else {
        format!("{} validation errors detected", errors.len())
    };
    Err(AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ValidationException",
        format!("{count}: {}", errors.join("; ")),
    ))
}

/// `arn:(aws[a-zA-Z-]*)?:[a-z0-9-.]+:.*`
fn is_arn(k: &str) -> bool {
    let Some(rest) = k.strip_prefix("arn:") else {
        return false;
    };
    let Some((partition, rest)) = rest.split_once(':') else {
        return false;
    };
    let partition_ok = partition.is_empty()
        || partition
            .strip_prefix("aws")
            .is_some_and(|p| p.chars().all(|c| c.is_ascii_alphabetic() || c == '-'));
    let Some((service, _)) = rest.split_once(':') else {
        return false;
    };
    partition_ok
        && !service.is_empty()
        && service
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.')
}

/// The `DurableConfig` a function is created with.
pub(crate) fn validate_create(cfg: &Value) -> Result<Value, AwsServiceError> {
    check_bounds(cfg)?;
    if cfg.get("ExecutionTimeout").is_none_or(Value::is_null) {
        return Err(invalid_parameter(
            "You cannot create a function with a durable configuration without an executionTimeout",
        ));
    }
    Ok(normalize(cfg))
}

/// The `DurableConfig` after an update: the members given replace the
/// stored ones (an empty `KMSKeyArn` removes the key), the others stay.
pub(crate) fn validate_update(
    existing: Option<&Value>,
    cfg: &Value,
) -> Result<Value, AwsServiceError> {
    check_bounds(cfg)?;
    let Some(existing) = existing else {
        return Err(invalid_parameter(
            "You cannot add a durable configuration to a function that was originally created with no durable configuration",
        ));
    };
    let mut merged = existing.as_object().cloned().unwrap_or_default();
    if let Some(update) = cfg.as_object() {
        for (k, v) in update {
            if !v.is_null() {
                merged.insert(k.clone(), v.clone());
            }
        }
    }
    Ok(normalize(&Value::Object(merged)))
}

fn normalize(cfg: &Value) -> Value {
    let mut v = normalize_durable_config(cfg);
    if v.get("KMSKeyArn").and_then(Value::as_str) == Some("") {
        if let Some(m) = v.as_object_mut() {
            m.remove("KMSKeyArn");
        }
    }
    v
}

/// The encryption context Lambda uses for a function's durable data.
pub(crate) fn encryption_context(function_arn: &str) -> std::collections::HashMap<String, String> {
    [(
        "aws:lambda:FunctionArn".to_string(),
        function_arn.to_string(),
    )]
    .into_iter()
    .collect()
}

/// Lambda's checks when a function's `DurableConfig` names a customer
/// managed key: the key exists, is enabled and its key policy lets the
/// Lambda service principal derive and decrypt data keys for the function.
pub(crate) fn check_configured_key(
    hook: Option<&dyn KmsHook>,
    account_id: &str,
    region: &str,
    function_arn: &str,
    cfg: &Value,
) -> Result<(), AwsServiceError> {
    let (Some(hook), Some(key)) = (hook, cfg.get("KMSKeyArn").and_then(Value::as_str)) else {
        return Ok(());
    };
    match hook.check_service_key_use(
        account_id,
        region,
        key,
        LAMBDA_PRINCIPAL,
        function_arn,
        &["kms:GenerateDataKey", "kms:Decrypt"],
        &encryption_context(function_arn),
    ) {
        Ok(_) => Ok(()),
        Err(KmsKeyUnusable::NotFound) => Err(invalid_parameter(format!(
            "Lambda was unable to configure your durable function because the specified KMS key {key} was not found. Please check your KMS key settings. KMS Exception: NotFoundException KMS Message: Key '{key}' does not exist"
        ))),
        Err(KmsKeyUnusable::AccessDenied) => Err(AwsServiceError::aws_error(
            StatusCode::FORBIDDEN,
            "AccessDeniedException",
            format!(
                "Lambda was unable to configure your durable function because KMS access was denied for key {key}. Please ensure your KMS key policy allows access to Lambda."
            ),
        )),
        Err(KmsKeyUnusable::Disabled) => Err(invalid_parameter(format!(
            "Lambda was unable to configure your durable function because the specified KMS key {key} is disabled. Please check your KMS key settings. KMS Exception: DescribeKey KMS Message: Key is disabled"
        ))),
        Err(KmsKeyUnusable::InvalidState(state)) => Err(invalid_parameter(format!(
            "Lambda was unable to configure your durable function because the specified KMS key {key} is in an invalid state. Please check your KMS key settings. KMS Exception: DescribeKey KMS Message: Key is in {state} state"
        ))),
    }
}

/// The error an operation that reads or writes an execution's data answers
/// when the execution's customer managed key is no longer usable (recorded:
/// `GetDurableExecution` / `GetDurableExecutionHistory` with data,
/// `StopDurableExecution`, `SendDurableExecutionCallbackSuccess`).
pub(crate) fn unusable_key_error(key: &str, reason: &KmsKeyUnusable) -> AwsServiceError {
    let (code, why, kms_exception, kms_message) = match reason {
        KmsKeyUnusable::Disabled => (
            "KMSDisabledException",
            "the KMS key used is disabled",
            "DisabledException",
            format!("{key} is disabled."),
        ),
        KmsKeyUnusable::InvalidState(state) => (
            "KMSInvalidStateException",
            "the KMS key's state is not valid",
            "KMSInvalidStateException",
            if state == "PendingDeletion" {
                format!("{key} is pending deletion.")
            } else {
                format!("{key} is {state}.")
            },
        ),
        KmsKeyUnusable::NotFound => (
            "KMSNotFoundException",
            "the KMS key was not found",
            "NotFoundException",
            format!("Key '{key}' does not exist"),
        ),
        KmsKeyUnusable::AccessDenied => (
            "KMSAccessDeniedException",
            "access to the KMS key was denied",
            "AccessDeniedException",
            format!("Access to {key} is denied."),
        ),
    };
    AwsServiceError::aws_error(
        StatusCode::BAD_GATEWAY,
        code,
        format!(
            "Lambda was unable to access your execution data because {why}. Check the KMS key configured in the function's durable config. KMS Exception: {kms_exception} KMS Message: {kms_message}"
        ),
    )
}

/// Whether the execution's customer managed key can still protect its
/// data (state only: callers reading data need the key enabled).
pub(crate) fn execution_key_error(
    hook: Option<&dyn KmsHook>,
    account_id: &str,
    region: &str,
    exec: &super::model::DurableExecution,
) -> Option<AwsServiceError> {
    let (Some(hook), Some(key)) = (hook, exec.kms_key_arn.as_deref()) else {
        return None;
    };
    let function_arn = exec
        .function_arn
        .rsplit_once(':')
        .map(|(f, _)| f)
        .unwrap_or(&exec.function_arn);
    hook.check_service_key_use(
        account_id,
        region,
        key,
        LAMBDA_PRINCIPAL,
        function_arn,
        &[],
        &encryption_context(function_arn),
    )
    .err()
    .map(|reason| unusable_key_error(key, &reason))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn msg(r: Result<Value, AwsServiceError>) -> (String, String) {
        let e = r.expect_err("rejected");
        (e.code().to_string(), e.message())
    }

    #[test]
    fn create_rejections_match_the_recordings() {
        for (cfg, code, message) in [
            (json!({}), "InvalidParameterValueException", "You cannot create a function with a durable configuration without an executionTimeout"),
            (json!({"RetentionPeriodInDays": 7}), "InvalidParameterValueException", "You cannot create a function with a durable configuration without an executionTimeout"),
            (json!({"ExecutionTimeout": 0}), "ValidationException", "1 validation error detected: Value '0' at 'durableConfig.executionTimeout' failed to satisfy constraint: Member must have value greater than or equal to 1"),
            (json!({"ExecutionTimeout": 31622401}), "ValidationException", "1 validation error detected: Value '31622401' at 'durableConfig.executionTimeout' failed to satisfy constraint: Member must have value less than or equal to 31622400"),
            (json!({"ExecutionTimeout": 60, "RetentionPeriodInDays": 0}), "ValidationException", "1 validation error detected: Value '0' at 'durableConfig.retentionPeriodInDays' failed to satisfy constraint: Member must have value greater than or equal to 1"),
            (json!({"ExecutionTimeout": 60, "RetentionPeriodInDays": 91}), "ValidationException", "1 validation error detected: Value '91' at 'durableConfig.retentionPeriodInDays' failed to satisfy constraint: Member must have value less than or equal to 90"),
            (json!({"ExecutionTimeout": 60, "KMSKeyArn": "not-an-arn"}), "ValidationException", "1 validation error detected: Value 'not-an-arn' at 'durableConfig.kMSKeyArn' failed to satisfy constraint: Member must satisfy regular expression pattern: (arn:(aws[a-zA-Z-]*)?:[a-z0-9-.]+:.*)|()"),
        ] {
            assert_eq!(msg(validate_create(&cfg)), (code.to_string(), message.to_string()), "{cfg}");
        }
        assert_eq!(
            validate_create(&json!({"ExecutionTimeout": 31622400})).unwrap(),
            json!({"ExecutionTimeout": 31622400, "RetentionPeriodInDays": 14})
        );
        assert_eq!(
            validate_create(&json!({"ExecutionTimeout": 60, "KMSKeyArn": ""})).unwrap(),
            json!({"ExecutionTimeout": 60, "RetentionPeriodInDays": 14})
        );
    }

    #[test]
    fn data_errors_match_the_recordings() {
        let key = "arn:aws:kms:us-east-1:454591548454:key/dbb3c25f-6044-4bed-b43b-3853a423e931";
        let e = unusable_key_error(key, &KmsKeyUnusable::Disabled);
        assert_eq!(
            (e.status(), e.code()),
            (StatusCode::BAD_GATEWAY, "KMSDisabledException")
        );
        assert_eq!(e.message(), format!("Lambda was unable to access your execution data because the KMS key used is disabled. Check the KMS key configured in the function's durable config. KMS Exception: DisabledException KMS Message: {key} is disabled."));
        let e = unusable_key_error(key, &KmsKeyUnusable::InvalidState("PendingDeletion".into()));
        assert_eq!(e.code(), "KMSInvalidStateException");
        assert_eq!(e.message(), format!("Lambda was unable to access your execution data because the KMS key's state is not valid. Check the KMS key configured in the function's durable config. KMS Exception: KMSInvalidStateException KMS Message: {key} is pending deletion."));
    }

    #[test]
    fn updates_merge_and_refuse_plain_functions() {
        let existing = json!({"ExecutionTimeout": 600, "RetentionPeriodInDays": 3});
        assert_eq!(
            validate_update(Some(&existing), &json!({})).unwrap(),
            existing
        );
        assert_eq!(
            validate_update(Some(&existing), &json!({"ExecutionTimeout": 900})).unwrap(),
            json!({"ExecutionTimeout": 900, "RetentionPeriodInDays": 3})
        );
        assert_eq!(
            validate_update(Some(&existing), &json!({"RetentionPeriodInDays": 5})).unwrap(),
            json!({"ExecutionTimeout": 600, "RetentionPeriodInDays": 5})
        );
        assert_eq!(
            msg(validate_update(
                Some(&existing),
                &json!({"ExecutionTimeout": 0})
            ))
            .0,
            "ValidationException"
        );
        assert_eq!(
            msg(validate_update(None, &json!({"ExecutionTimeout": 60}))),
            (
                "InvalidParameterValueException".to_string(),
                "You cannot add a durable configuration to a function that was originally created with no durable configuration".to_string()
            )
        );
    }
}
