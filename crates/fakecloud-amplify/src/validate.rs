//! Model-derived input validation for AWS Amplify operations.
//!
//! Rejects requests that violate the Smithy contract (missing required
//! members, out-of-set enum values, over-length strings, oversized maps) with
//! `BadRequestException` -- the code every Amplify operation declares -- so the
//! conformance suite's negative variants land on a declared error rather than a
//! routing miss or a 500. Body member names are the restJson1 member names
//! (camelCase; Amplify declares no `@jsonName` overrides).

use http::StatusCode;
use serde_json::Value;

use fakecloud_core::service::AwsServiceError;

pub const PLATFORM: &[&str] = &["WEB", "WEB_DYNAMIC", "WEB_COMPUTE"];
pub const STAGE: &[&str] = &[
    "PRODUCTION",
    "BETA",
    "DEVELOPMENT",
    "EXPERIMENTAL",
    "PULL_REQUEST",
];
pub const JOB_TYPE: &[&str] = &["RELEASE", "RETRY", "MANUAL", "WEB_HOOK"];
pub const SOURCE_URL_TYPE: &[&str] = &["ZIP", "BUCKET_PREFIX"];
pub const CERTIFICATE_TYPE: &[&str] = &["AMPLIFY_MANAGED", "CUSTOM"];
pub const CACHE_CONFIG_TYPE: &[&str] = &["AMPLIFY_MANAGED", "AMPLIFY_MANAGED_NO_COOKIES"];
pub const BUILD_COMPUTE_TYPE: &[&str] = &["STANDARD_8GB", "LARGE_16GB", "XLARGE_72GB"];

fn bad_request(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "BadRequestException", msg)
}

/// A present, non-null, non-empty required member.
fn require<'a>(b: &'a Value, field: &str) -> Result<&'a Value, AwsServiceError> {
    match b.get(field) {
        Some(Value::Null) | None => Err(bad_request(&format!("{field} is required."))),
        Some(Value::String(s)) if s.is_empty() => {
            Err(bad_request(&format!("{field} must not be empty.")))
        }
        Some(v) => Ok(v),
    }
}

/// An optional string member is one of `allowed` (case-sensitive), if present.
fn enum_opt(b: &Value, field: &str, allowed: &[&str]) -> Result<(), AwsServiceError> {
    if let Some(v) = b.get(field).and_then(Value::as_str) {
        if !allowed.contains(&v) {
            return Err(bad_request(&format!(
                "{field} must be one of [{}], got '{v}'.",
                allowed.join(", ")
            )));
        }
    }
    Ok(())
}

/// An optional nested-object member's `type` field is a valid enum, if present.
fn nested_enum(b: &Value, field: &str, allowed: &[&str]) -> Result<(), AwsServiceError> {
    if let Some(obj) = b.get(field) {
        // The nested config's discriminator is a required member per the model;
        // a present config with a bad/absent type is rejected.
        enum_opt(obj, "type", allowed)?;
    }
    Ok(())
}

/// An optional string member is at most `max` UTF-8 bytes, if present.
fn max_len(b: &Value, field: &str, max: usize) -> Result<(), AwsServiceError> {
    if let Some(v) = b.get(field).and_then(Value::as_str) {
        if v.len() > max {
            return Err(bad_request(&format!(
                "{field} must be at most {max} characters, got {}.",
                v.len()
            )));
        }
    }
    Ok(())
}

/// An optional string member, if present, is non-empty (min length 1). Amplify
/// declares `^(?s).+$` (at least one character) on several members.
fn non_empty(b: &Value, field: &str) -> Result<(), AwsServiceError> {
    if let Some("") = b.get(field).and_then(Value::as_str) {
        return Err(bad_request(&format!("{field} must not be empty.")));
    }
    Ok(())
}

/// An optional map member has at most `max` entries, if present.
fn max_map(b: &Value, field: &str, max: usize) -> Result<(), AwsServiceError> {
    if let Some(m) = b.get(field).and_then(Value::as_object) {
        if m.len() > max {
            return Err(bad_request(&format!(
                "{field} must have at most {max} entries, got {}.",
                m.len()
            )));
        }
    }
    Ok(())
}

fn validate_app_fields(body: &Value) -> Result<(), AwsServiceError> {
    enum_opt(body, "platform", PLATFORM)?;
    nested_enum(body, "cacheConfig", CACHE_CONFIG_TYPE)?;
    nested_field_enum(body, "jobConfig", "buildComputeType", BUILD_COMPUTE_TYPE)?;
    max_len(body, "name", 255)?;
    max_len(body, "description", 1000)?;
    max_len(body, "repository", 1000)?;
    max_len(body, "computeRoleArn", 1000)?;
    max_len(body, "iamServiceRoleArn", 1000)?;
    max_len(body, "oauthToken", 1000)?;
    max_len(body, "accessToken", 255)?;
    max_len(body, "basicAuthCredentials", 2000)?;
    max_len(body, "buildSpec", 25000)?;
    max_len(body, "customHeaders", 25000)?;
    max_map(body, "tags", 50)?;
    // Members whose pattern (`^(?s).+$`) forbids an empty value.
    non_empty(body, "accessToken")?;
    non_empty(body, "buildSpec")?;
    Ok(())
}

/// A nested object's named field is a valid enum, if the object and field are
/// present. (`jobConfig.buildComputeType`, `certificateSettings.type`, ...)
fn nested_field_enum(
    b: &Value,
    obj_field: &str,
    inner: &str,
    allowed: &[&str],
) -> Result<(), AwsServiceError> {
    if let Some(obj) = b.get(obj_field) {
        enum_opt(obj, inner, allowed)?;
    }
    Ok(())
}

fn validate_branch_fields(body: &Value) -> Result<(), AwsServiceError> {
    enum_opt(body, "stage", STAGE)?;
    max_len(body, "branchName", 255)?;
    max_len(body, "description", 1000)?;
    max_len(body, "framework", 255)?;
    max_len(body, "basicAuthCredentials", 2000)?;
    max_len(body, "buildSpec", 25000)?;
    max_len(body, "ttl", 32)?;
    max_len(body, "displayName", 255)?;
    max_len(body, "pullRequestEnvironmentName", 20)?;
    max_len(body, "backendEnvironmentArn", 1000)?;
    max_len(body, "computeRoleArn", 1000)?;
    max_map(body, "tags", 50)?;
    non_empty(body, "buildSpec")?;
    Ok(())
}

/// Validate an operation's already-parsed JSON body against the model. Members
/// bound to `@httpLabel` / `@httpQuery` are validated in the handler (they are
/// not part of the JSON body).
pub fn validate_input(action: &str, body: &Value) -> Result<(), AwsServiceError> {
    match action {
        "CreateApp" => {
            require(body, "name")?;
            validate_app_fields(body)?;
        }
        "UpdateApp" => {
            validate_app_fields(body)?;
        }
        "CreateBranch" => {
            require(body, "branchName")?;
            validate_branch_fields(body)?;
        }
        "UpdateBranch" => {
            validate_branch_fields(body)?;
        }
        "CreateDomainAssociation" => {
            require(body, "domainName")?;
            require(body, "subDomainSettings")?;
            nested_field_enum(body, "certificateSettings", "type", CERTIFICATE_TYPE)?;
        }
        "UpdateDomainAssociation" => {
            nested_field_enum(body, "certificateSettings", "type", CERTIFICATE_TYPE)?;
        }
        "CreateWebhook" => {
            require(body, "branchName")?;
            max_len(body, "description", 1000)?;
        }
        "CreateBackendEnvironment" => {
            require(body, "environmentName")?;
        }
        "StartJob" => {
            require(body, "jobType")?;
            enum_opt(body, "jobType", JOB_TYPE)?;
            max_len(body, "jobReason", 255)?;
            max_len(body, "commitId", 255)?;
            max_len(body, "commitMessage", 10000)?;
        }
        "StartDeployment" => {
            enum_opt(body, "sourceUrlType", SOURCE_URL_TYPE)?;
        }
        "TagResource" => {
            require(body, "tags")?;
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn create_app_requires_name() {
        assert!(validate_input("CreateApp", &json!({})).is_err());
        assert!(validate_input("CreateApp", &json!({ "name": "app" })).is_ok());
    }

    #[test]
    fn create_app_rejects_bad_platform() {
        assert!(validate_input("CreateApp", &json!({ "name": "a", "platform": "NOPE" })).is_err());
        assert!(validate_input("CreateApp", &json!({ "name": "a", "platform": "WEB" })).is_ok());
    }

    #[test]
    fn create_app_rejects_too_long_name() {
        let long = "a".repeat(256);
        assert!(validate_input("CreateApp", &json!({ "name": long })).is_err());
    }

    #[test]
    fn create_app_rejects_bad_cache_config() {
        assert!(validate_input(
            "CreateApp",
            &json!({ "name": "a", "cacheConfig": { "type": "BAD" } })
        )
        .is_err());
        assert!(validate_input(
            "CreateApp",
            &json!({ "name": "a", "jobConfig": { "buildComputeType": "STANDARD_8GB" } })
        )
        .is_ok());
    }

    #[test]
    fn start_job_requires_and_validates_job_type() {
        assert!(validate_input("StartJob", &json!({})).is_err());
        assert!(validate_input("StartJob", &json!({ "jobType": "WAT" })).is_err());
        assert!(validate_input("StartJob", &json!({ "jobType": "RELEASE" })).is_ok());
    }

    #[test]
    fn create_domain_requires_subdomain_settings() {
        assert!(
            validate_input("CreateDomainAssociation", &json!({ "domainName": "x.com" })).is_err()
        );
        assert!(validate_input(
            "CreateDomainAssociation",
            &json!({ "domainName": "x.com", "subDomainSettings": [] })
        )
        .is_ok());
    }
}
