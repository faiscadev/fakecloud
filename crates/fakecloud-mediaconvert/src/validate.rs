//! Model-derived input validation for AWS Elemental MediaConvert operations.
//!
//! Rejects requests that violate the Smithy contract (missing required members,
//! out-of-set enum values, out-of-range integers) with `BadRequestException` --
//! the code every MediaConvert operation declares -- so the conformance suite's
//! negative variants land on a declared error rather than a routing miss or a
//! 500. Body member names are the restJson1 `@jsonName` (camelCase) wire names.

use http::StatusCode;
use serde_json::Value;

use fakecloud_core::service::AwsServiceError;

pub const PRICING_PLAN: &[&str] = &["ON_DEMAND", "RESERVED"];
pub const QUEUE_STATUS: &[&str] = &["ACTIVE", "PAUSED"];
pub const BILLING_TAGS_SOURCE: &[&str] = &["QUEUE", "PRESET", "JOB_TEMPLATE", "JOB"];
pub const ACCELERATION_MODE: &[&str] = &["DISABLED", "ENABLED", "PREFERRED"];
pub const ORDER: &[&str] = &["ASCENDING", "DESCENDING"];
pub const COMMITMENT: &[&str] = &["ONE_YEAR"];
pub const RENEWAL_TYPE: &[&str] = &["AUTO_RENEW", "EXPIRE"];
pub const STATUS_UPDATE_INTERVAL: &[&str] = &[
    "SECONDS_10",
    "SECONDS_12",
    "SECONDS_15",
    "SECONDS_20",
    "SECONDS_30",
    "SECONDS_60",
    "SECONDS_120",
    "SECONDS_180",
    "SECONDS_240",
    "SECONDS_300",
    "SECONDS_360",
    "SECONDS_420",
    "SECONDS_480",
    "SECONDS_540",
    "SECONDS_600",
];

fn bad_request(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "BadRequestException", msg)
}

/// A present, non-null required member.
fn require<'a>(b: &'a Value, field: &str) -> Result<&'a Value, AwsServiceError> {
    match b.get(field) {
        Some(Value::Null) | None => Err(bad_request(&format!(
            "The request failed because it is missing the required parameter '{field}'."
        ))),
        Some(Value::String(s)) if s.is_empty() => {
            Err(bad_request(&format!("'{field}' must not be empty.")))
        }
        Some(v) => Ok(v),
    }
}

/// An optional string member is one of `allowed` (case-sensitive), if present.
fn enum_opt(b: &Value, field: &str, allowed: &[&str]) -> Result<(), AwsServiceError> {
    if let Some(v) = b.get(field).and_then(Value::as_str) {
        if !allowed.contains(&v) {
            return Err(bad_request(&format!(
                "'{field}' must be one of [{}], got '{v}'.",
                allowed.join(", ")
            )));
        }
    }
    Ok(())
}

/// An optional integer member is within `[min, max]`, if present.
fn int_range(b: &Value, field: &str, min: i64, max: i64) -> Result<(), AwsServiceError> {
    if let Some(v) = b.get(field) {
        if !v.is_null() {
            let n = v
                .as_i64()
                .ok_or_else(|| bad_request(&format!("'{field}' must be an integer.")))?;
            if n < min || n > max {
                return Err(bad_request(&format!(
                    "'{field}' must be between {min} and {max}, got {n}."
                )));
            }
        }
    }
    Ok(())
}

/// Validate the nested `reservationPlanSettings` object when present.
fn validate_reservation_plan(b: &Value) -> Result<(), AwsServiceError> {
    if let Some(rp) = b.get("reservationPlanSettings") {
        if !rp.is_null() {
            require(rp, "commitment")?;
            require(rp, "renewalType")?;
            require(rp, "reservedSlots")?;
            enum_opt(rp, "commitment", COMMITMENT)?;
            enum_opt(rp, "renewalType", RENEWAL_TYPE)?;
        }
    }
    Ok(())
}

/// Validate an operation's already-parsed JSON body against the model. Members
/// bound to `@httpLabel` / `@httpQuery` are validated in the handler (they are
/// not part of the JSON body).
pub fn validate_input(action: &str, body: &Value) -> Result<(), AwsServiceError> {
    match action {
        "CreateQueue" => {
            require(body, "name")?;
            enum_opt(body, "pricingPlan", PRICING_PLAN)?;
            enum_opt(body, "status", QUEUE_STATUS)?;
            validate_reservation_plan(body)?;
        }
        "UpdateQueue" => {
            enum_opt(body, "status", QUEUE_STATUS)?;
            validate_reservation_plan(body)?;
        }
        "CreatePreset" => {
            require(body, "name")?;
            require(body, "settings")?;
        }
        "CreateJobTemplate" => {
            require(body, "name")?;
            require(body, "settings")?;
            int_range(body, "priority", -50, 50)?;
            enum_opt(body, "statusUpdateInterval", STATUS_UPDATE_INTERVAL)?;
            validate_acceleration(body)?;
        }
        "UpdateJobTemplate" => {
            int_range(body, "priority", -50, 50)?;
            enum_opt(body, "statusUpdateInterval", STATUS_UPDATE_INTERVAL)?;
            validate_acceleration(body)?;
        }
        "CreateJob" => {
            require(body, "role")?;
            require(body, "settings")?;
            int_range(body, "priority", -50, 50)?;
            enum_opt(body, "billingTagsSource", BILLING_TAGS_SOURCE)?;
            enum_opt(body, "statusUpdateInterval", STATUS_UPDATE_INTERVAL)?;
            validate_acceleration(body)?;
        }
        "PutPolicy" => {
            require(body, "policy")?;
        }
        "TagResource" => {
            require(body, "arn")?;
            require(body, "tags")?;
        }
        "AssociateCertificate" => {
            require(body, "arn")?;
        }
        "CreateResourceShare" => {
            require(body, "jobId")?;
            require(body, "supportCaseId")?;
        }
        "StartJobsQuery" => {
            int_range(body, "maxResults", 1, 20)?;
            enum_opt(body, "order", ORDER)?;
        }
        _ => {}
    }
    Ok(())
}

fn validate_acceleration(b: &Value) -> Result<(), AwsServiceError> {
    if let Some(acc) = b.get("accelerationSettings") {
        if !acc.is_null() {
            require(acc, "mode")?;
            enum_opt(acc, "mode", ACCELERATION_MODE)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn create_queue_requires_name() {
        assert!(validate_input("CreateQueue", &json!({})).is_err());
        assert!(validate_input("CreateQueue", &json!({ "name": "q" })).is_ok());
    }

    #[test]
    fn create_queue_rejects_bad_pricing_plan() {
        assert!(validate_input(
            "CreateQueue",
            &json!({ "name": "q", "pricingPlan": "NOPE" })
        )
        .is_err());
        assert!(validate_input(
            "CreateQueue",
            &json!({ "name": "q", "pricingPlan": "RESERVED",
                     "reservationPlanSettings": {"commitment":"ONE_YEAR","renewalType":"EXPIRE","reservedSlots":1} })
        )
        .is_ok());
    }

    #[test]
    fn create_job_requires_role_and_settings() {
        assert!(validate_input("CreateJob", &json!({ "role": "r" })).is_err());
        assert!(validate_input("CreateJob", &json!({ "settings": {} })).is_err());
        assert!(validate_input("CreateJob", &json!({ "role": "r", "settings": {} })).is_ok());
    }

    #[test]
    fn create_job_rejects_out_of_range_priority() {
        assert!(validate_input(
            "CreateJob",
            &json!({ "role": "r", "settings": {}, "priority": 99 })
        )
        .is_err());
        assert!(validate_input(
            "CreateJob",
            &json!({ "role": "r", "settings": {}, "priority": 10 })
        )
        .is_ok());
    }

    #[test]
    fn create_preset_requires_settings() {
        assert!(validate_input("CreatePreset", &json!({ "name": "p" })).is_err());
        assert!(validate_input("CreatePreset", &json!({ "name": "p", "settings": {} })).is_ok());
    }

    #[test]
    fn tag_resource_requires_arn_and_tags() {
        assert!(validate_input("TagResource", &json!({ "arn": "a" })).is_err());
        assert!(validate_input("TagResource", &json!({ "arn": "a", "tags": {"k":"v"} })).is_ok());
    }
}
