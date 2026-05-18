//! CloudWatch Logs-specific input validators.
//!
//! These mirror the generic helpers in [`fakecloud_core::validation`] but emit
//! `InvalidParameterException` instead of `ValidationException`, matching the
//! errors declared in the `com.amazonaws.cloudwatchlogs` Smithy model. The
//! distinction matters for conformance: clients using `aws-sdk-cloudwatchlogs`
//! expect to deserialize `InvalidParameterException`, and `ValidationException`
//! is not part of the service's error union.

use fakecloud_core::service::AwsServiceError;
use http::StatusCode;

fn invalid_parameter(message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidParameterException",
        message.into(),
    )
}

/// Validate a string's length is within [min, max].
pub fn validate_string_length(
    field: &str,
    value: &str,
    min: usize,
    max: usize,
) -> Result<(), AwsServiceError> {
    let len = value.len();
    if len < min || len > max {
        return Err(invalid_parameter(format!(
            "Value at '{}' failed to satisfy constraint: \
             Member must have length between {} and {}",
            field, min, max,
        )));
    }
    Ok(())
}

/// Validate an integer is within [min, max].
pub fn validate_range_i64(
    field: &str,
    value: i64,
    min: i64,
    max: i64,
) -> Result<(), AwsServiceError> {
    if value < min || value > max {
        return Err(invalid_parameter(format!(
            "Value '{}' at '{}' failed to satisfy constraint: \
             Member must have value between {} and {}",
            value, field, min, max,
        )));
    }
    Ok(())
}

/// Validate a string is one of the allowed enum values.
pub fn validate_enum(field: &str, value: &str, allowed: &[&str]) -> Result<(), AwsServiceError> {
    if !allowed.contains(&value) {
        return Err(invalid_parameter(format!(
            "Value '{}' at '{}' failed to satisfy constraint: \
             Member must satisfy enum value set: [{}]",
            value,
            field,
            allowed.join(", "),
        )));
    }
    Ok(())
}

/// Validate that a required field is present (not null/missing).
pub fn validate_required(field: &str, value: &serde_json::Value) -> Result<(), AwsServiceError> {
    if value.is_null() {
        return Err(invalid_parameter(format!("{} is required", field)));
    }
    Ok(())
}

/// Validate an optional string's length if present.
pub fn validate_optional_string_length(
    field: &str,
    value: Option<&str>,
    min: usize,
    max: usize,
) -> Result<(), AwsServiceError> {
    if let Some(v) = value {
        validate_string_length(field, v, min, max)?;
    }
    Ok(())
}

/// Validate an optional integer range if present.
pub fn validate_optional_range_i64(
    field: &str,
    value: Option<i64>,
    min: i64,
    max: i64,
) -> Result<(), AwsServiceError> {
    if let Some(v) = value {
        validate_range_i64(field, v, min, max)?;
    }
    Ok(())
}

/// Validate an optional enum value if present.
pub fn validate_optional_enum_value(
    field: &str,
    value: &serde_json::Value,
    allowed: &[&str],
) -> Result<(), AwsServiceError> {
    if value.is_null() {
        return Ok(());
    }
    let s = value.as_str().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "SerializationException",
            format!("Value for '{}' must be a string", field),
        )
    })?;
    validate_enum(field, s, allowed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_use_invalid_parameter_exception() {
        let err = validate_string_length("kmsKeyId", "", 1, 256).unwrap_err();
        assert_eq!(err.code(), "InvalidParameterException");

        let err = validate_range_i64("limit", 0, 1, 50).unwrap_err();
        assert_eq!(err.code(), "InvalidParameterException");

        let err = validate_enum("kind", "bad", &["good"]).unwrap_err();
        assert_eq!(err.code(), "InvalidParameterException");

        let err = validate_required("logGroupName", &serde_json::Value::Null).unwrap_err();
        assert_eq!(err.code(), "InvalidParameterException");
    }

    #[test]
    fn optional_helpers_passthrough_on_none() {
        assert!(validate_optional_string_length("x", None, 1, 5).is_ok());
        assert!(validate_optional_range_i64("y", None, 1, 5).is_ok());
        assert!(validate_optional_enum_value("z", &serde_json::Value::Null, &["a", "b"]).is_ok());
    }

    #[test]
    fn enum_value_rejects_non_string() {
        let err =
            validate_optional_enum_value("kind", &serde_json::json!(123), &["a"]).unwrap_err();
        assert_eq!(err.code(), "SerializationException");
    }
}
