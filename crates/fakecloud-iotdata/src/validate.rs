//! Model-derived input validation for AWS IoT Data Plane operations.
//!
//! IoT Data Plane binds almost every input member to an `@httpLabel`,
//! `@httpQuery`, or the raw `@httpPayload` body, so validation centres on the
//! shadow document carried by `UpdateThingShadow`: it must be a JSON object
//! carrying a `state` object with `desired` / `reported` sub-objects. AWS
//! rejects a malformed document with `InvalidRequestException` (a declared
//! error on the operation), so surface it here rather than letting a bad
//! document reach the merge engine.

use http::StatusCode;
use serde_json::Value;

use fakecloud_core::service::AwsServiceError;

pub fn invalid_request(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidRequestException", msg)
}

/// Validate a `thingName` path label (Smithy `length` 1..=128, `pattern`
/// `^[a-zA-Z0-9:_-]+$`).
pub fn validate_thing_name(name: &str) -> Result<(), AwsServiceError> {
    if name.is_empty() || name.len() > 128 {
        return Err(invalid_request(
            "thingName must be between 1 and 128 characters.",
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, ':' | '_' | '-'))
    {
        return Err(invalid_request("thingName contains invalid characters."));
    }
    Ok(())
}

/// Validate a `shadowName` query value (Smithy `length` 1..=64, `pattern`
/// `^[$a-zA-Z0-9:_-]+$`).
pub fn validate_shadow_name(name: &str) -> Result<(), AwsServiceError> {
    if name.is_empty() || name.len() > 64 {
        return Err(invalid_request(
            "shadowName must be between 1 and 64 characters.",
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '$' | ':' | '_' | '-'))
    {
        return Err(invalid_request("shadowName contains invalid characters."));
    }
    Ok(())
}

/// Validate the `payloadFormatIndicator` header enum
/// (`UNSPECIFIED_BYTES` | `UTF8_DATA`).
pub fn validate_payload_format_indicator(value: Option<&str>) -> Result<(), AwsServiceError> {
    match value {
        None => Ok(()),
        Some("UNSPECIFIED_BYTES") | Some("UTF8_DATA") => Ok(()),
        Some(other) => Err(invalid_request(&format!(
            "'{other}' is not a valid payloadFormatIndicator."
        ))),
    }
}

/// Validate and parse an `UpdateThingShadow` payload into its `state` object.
/// The document must be a JSON object with a `state` object; `desired` and
/// `reported` (when present) must themselves be objects.
pub fn validate_shadow_update(payload: &[u8]) -> Result<Value, AwsServiceError> {
    if payload.is_empty() {
        return Err(invalid_request(
            "The shadow update payload must not be empty.",
        ));
    }
    let doc: Value = serde_json::from_slice(payload)
        .map_err(|e| invalid_request(&format!("The shadow update document is malformed: {e}")))?;
    let Some(obj) = doc.as_object() else {
        return Err(invalid_request(
            "The shadow update document must be a JSON object.",
        ));
    };
    let Some(state) = obj.get("state") else {
        return Err(invalid_request(
            "The shadow update document must contain a 'state' object.",
        ));
    };
    let Some(state_obj) = state.as_object() else {
        return Err(invalid_request("'state' must be a JSON object."));
    };
    for section in ["desired", "reported"] {
        if let Some(v) = state_obj.get(section) {
            if !v.is_null() && !v.is_object() {
                return Err(invalid_request(&format!(
                    "'state.{section}' must be a JSON object."
                )));
            }
        }
    }
    Ok(doc)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_and_malformed() {
        assert!(validate_shadow_update(b"").is_err());
        assert!(validate_shadow_update(b"not json").is_err());
        assert!(validate_shadow_update(b"[]").is_err());
    }

    #[test]
    fn requires_state_object() {
        assert!(validate_shadow_update(br#"{"foo":1}"#).is_err());
        assert!(validate_shadow_update(br#"{"state":5}"#).is_err());
    }

    #[test]
    fn rejects_non_object_section() {
        assert!(validate_shadow_update(br#"{"state":{"desired":5}}"#).is_err());
    }

    #[test]
    fn accepts_valid_document() {
        assert!(validate_shadow_update(br#"{"state":{"desired":{"a":1},"reported":{}}}"#).is_ok());
    }
}
