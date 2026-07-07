//! Model-derived input validation for Amazon MWAA operations.
//!
//! Rejects requests that violate the Smithy contract (missing required members,
//! out-of-set enum values, malformed environment names) with
//! `ValidationException` (the code every operation declares) so the conformance
//! suite's negative variants land on a declared error rather than a routing miss
//! or a 500. Body member names are the restJson1 member names (PascalCase --
//! MWAA declares no `jsonName` overrides).

use http::StatusCode;
use serde_json::Value;

use fakecloud_core::service::AwsServiceError;

pub const WEBSERVER_ACCESS_MODE: &[&str] = &["PRIVATE_ONLY", "PUBLIC_ONLY", "PUBLIC_AND_PRIVATE"];
pub const ENDPOINT_MANAGEMENT: &[&str] = &["CUSTOMER", "SERVICE"];
pub const WORKER_REPLACEMENT_STRATEGY: &[&str] = &["FORCED", "GRACEFUL"];
pub const LOGGING_LEVEL: &[&str] = &["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"];
pub const REST_API_METHOD: &[&str] = &["GET", "PUT", "POST", "PATCH", "DELETE"];

fn validation(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", msg)
}

/// A present, non-null, non-empty required member.
fn require<'a>(b: &'a Value, field: &str) -> Result<&'a Value, AwsServiceError> {
    match b.get(field) {
        Some(Value::Null) | None => Err(validation(&format!("{field} is required."))),
        Some(Value::String(s)) if s.is_empty() => {
            Err(validation(&format!("{field} must not be empty.")))
        }
        Some(v) => Ok(v),
    }
}

/// An optional string member is one of `allowed` (case-sensitive, as AWS
/// declares these enums), when present.
fn enum_opt(b: &Value, field: &str, allowed: &[&str]) -> Result<(), AwsServiceError> {
    if let Some(v) = b.get(field).and_then(Value::as_str) {
        if !allowed.contains(&v) {
            return Err(validation(&format!(
                "{field} must be one of [{}], got '{v}'.",
                allowed.join(", ")
            )));
        }
    }
    Ok(())
}

fn validate_logging_input(b: &Value) -> Result<(), AwsServiceError> {
    let Some(lc) = b.get("LoggingConfiguration").and_then(Value::as_object) else {
        return Ok(());
    };
    for module in [
        "DagProcessingLogs",
        "SchedulerLogs",
        "WebserverLogs",
        "WorkerLogs",
        "TaskLogs",
    ] {
        if let Some(m) = lc.get(module) {
            // ModuleLoggingConfigurationInput requires Enabled + LogLevel.
            require(m, "Enabled")?;
            require(m, "LogLevel")?;
            enum_opt(m, "LogLevel", LOGGING_LEVEL)?;
        }
    }
    Ok(())
}

/// Validate an operation's already-parsed JSON body against the model.
pub fn validate_input(action: &str, body: &Value) -> Result<(), AwsServiceError> {
    match action {
        "CreateEnvironment" => {
            require(body, "ExecutionRoleArn")?;
            require(body, "SourceBucketArn")?;
            require(body, "DagS3Path")?;
            require(body, "NetworkConfiguration")?;
            enum_opt(body, "WebserverAccessMode", WEBSERVER_ACCESS_MODE)?;
            enum_opt(body, "EndpointManagement", ENDPOINT_MANAGEMENT)?;
            validate_logging_input(body)?;
        }
        "UpdateEnvironment" => {
            enum_opt(body, "WebserverAccessMode", WEBSERVER_ACCESS_MODE)?;
            enum_opt(
                body,
                "WorkerReplacementStrategy",
                WORKER_REPLACEMENT_STRATEGY,
            )?;
            validate_logging_input(body)?;
        }
        "TagResource" => {
            require(body, "Tags")?;
        }
        "PublishMetrics" => {
            require(body, "MetricData")?;
        }
        "InvokeRestApi" => {
            require(body, "Path")?;
            require(body, "Method")?;
            enum_opt(body, "Method", REST_API_METHOD)?;
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
    fn create_requires_execution_role() {
        let err = validate_input("CreateEnvironment", &json!({})).unwrap_err();
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn create_accepts_full_body() {
        let body = json!({
            "ExecutionRoleArn": "arn:aws:iam::000000000000:role/mwaa",
            "SourceBucketArn": "arn:aws:s3:::my-bucket",
            "DagS3Path": "dags",
            "NetworkConfiguration": { "SubnetIds": ["subnet-a", "subnet-b"] },
            "WebserverAccessMode": "PUBLIC_ONLY"
        });
        assert!(validate_input("CreateEnvironment", &body).is_ok());
    }

    #[test]
    fn rejects_bad_access_mode() {
        let body = json!({
            "ExecutionRoleArn": "r", "SourceBucketArn": "b", "DagS3Path": "d",
            "NetworkConfiguration": {}, "WebserverAccessMode": "NOPE"
        });
        assert!(validate_input("CreateEnvironment", &body).is_err());
    }

    #[test]
    fn invoke_rest_api_requires_path_and_method() {
        assert!(validate_input("InvokeRestApi", &json!({ "Method": "GET" })).is_err());
        assert!(validate_input(
            "InvokeRestApi",
            &json!({ "Path": "/dags", "Method": "WAT" })
        )
        .is_err());
        assert!(validate_input(
            "InvokeRestApi",
            &json!({ "Path": "/dags", "Method": "GET" })
        )
        .is_ok());
    }
}
