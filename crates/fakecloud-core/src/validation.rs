//! Generic input validation helpers for AWS-compatible error responses.
//!
//! All validators return `AwsServiceError` with `ValidationException` and 400 status.

use crate::service::AwsServiceError;
use http::StatusCode;

/// Validate a string's length is within [min, max].
pub fn validate_string_length(
    field: &str,
    value: &str,
    min: usize,
    max: usize,
) -> Result<(), AwsServiceError> {
    let len = value.len();
    if len < min || len > max {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!(
                "Value at '{}' failed to satisfy constraint: \
                 Member must have length between {} and {}",
                field, min, max,
            ),
        ));
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
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!(
                "Value '{}' at '{}' failed to satisfy constraint: \
                 Member must have value between {} and {}",
                value, field, min, max,
            ),
        ));
    }
    Ok(())
}

/// Validate a string is one of the allowed enum values.
pub fn validate_enum(field: &str, value: &str, allowed: &[&str]) -> Result<(), AwsServiceError> {
    if !allowed.contains(&value) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!(
                "Value '{}' at '{}' failed to satisfy constraint: \
                 Member must satisfy enum value set: [{}]",
                value,
                field,
                allowed.join(", "),
            ),
        ));
    }
    Ok(())
}

/// Validate that a required field is present (not null/missing).
pub fn validate_required(field: &str, value: &serde_json::Value) -> Result<(), AwsServiceError> {
    if value.is_null() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!("{} is required", field),
        ));
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
pub fn validate_optional_enum(
    field: &str,
    value: Option<&str>,
    allowed: &[&str],
) -> Result<(), AwsServiceError> {
    if let Some(v) = value {
        validate_enum(field, v, allowed)?;
    }
    Ok(())
}
