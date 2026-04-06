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

/// Validate an optional numeric JSON value is within [min, max].
///
/// Unlike `validate_optional_range_i64`, this takes a raw `serde_json::Value` reference
/// so it can detect non-null, non-integer values (e.g. strings, large unsigned numbers)
/// that would silently become `None` via `as_i64()`.
pub fn validate_optional_json_range(
    field: &str,
    value: &serde_json::Value,
    min: i64,
    max: i64,
) -> Result<(), AwsServiceError> {
    if value.is_null() {
        return Ok(());
    }
    let n = value.as_i64().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!(
                "Value at '{}' failed to satisfy constraint: \
                 Member must have value between {} and {}",
                field, min, max,
            ),
        )
    })?;
    validate_range_i64(field, n, min, max)
}

/// Parse a string as an i64 for range validation, returning a `ValidationException`
/// when the value is present but not a valid integer.
///
/// Use this instead of `.parse::<i64>().ok()` + `validate_optional_range_i64` to
/// catch non-numeric strings that would silently fall through to defaults.
pub fn parse_optional_i64_param(
    field: &str,
    value: Option<&str>,
) -> Result<Option<i64>, AwsServiceError> {
    match value {
        None => Ok(None),
        Some(s) => {
            let n = s.parse::<i64>().map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!(
                        "Value '{}' at '{}' failed to satisfy constraint: \
                         Member must be a number",
                        s, field,
                    ),
                )
            })?;
            Ok(Some(n))
        }
    }
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

/// Validate an optional enum from a JSON value, rejecting non-string types.
///
/// Unlike [`validate_optional_enum`], this takes a raw [`serde_json::Value`] so it can
/// distinguish between a missing/null field (ok to skip) and a non-string value (error).
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
    fn validate_optional_json_range_rejects_non_integer() {
        let val = serde_json::json!("abc");
        let result = validate_optional_json_range("limit", &val, 1, 100);
        assert!(result.is_err());
    }

    #[test]
    fn validate_optional_json_range_rejects_large_unsigned() {
        let val = serde_json::json!(u64::MAX);
        let result = validate_optional_json_range("limit", &val, 1, 1000);
        assert!(result.is_err());
    }

    #[test]
    fn validate_optional_json_range_allows_null() {
        let val = serde_json::Value::Null;
        let result = validate_optional_json_range("limit", &val, 1, 100);
        assert!(result.is_ok());
    }

    #[test]
    fn validate_optional_json_range_validates_range() {
        let val = serde_json::json!(0);
        let result = validate_optional_json_range("limit", &val, 1, 100);
        assert!(result.is_err());

        let val = serde_json::json!(50);
        let result = validate_optional_json_range("limit", &val, 1, 100);
        assert!(result.is_ok());
    }

    #[test]
    fn parse_optional_i64_param_rejects_non_numeric() {
        let result = parse_optional_i64_param("maxItems", Some("abc"));
        assert!(result.is_err());
    }

    #[test]
    fn parse_optional_i64_param_allows_none() {
        let result = parse_optional_i64_param("maxItems", None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn parse_optional_i64_param_parses_valid_number() {
        let result = parse_optional_i64_param("maxItems", Some("42"));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(42));
    }
}
