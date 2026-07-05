//! Model-derived input validation for Amazon MQ operations.
//!
//! Rejects requests that violate the Smithy contract (missing required members,
//! out-of-set enum values, out-of-range page sizes) with `BadRequestException`
//! (the code every operation declares) so the conformance suite's negative
//! variants land on a declared error rather than a routing miss or a 500. Body
//! member names are the restJson1 `jsonName`s (camelCase).

use http::StatusCode;
use serde_json::Value;

use fakecloud_core::service::AwsServiceError;

fn bad_request(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "BadRequestException", msg)
}

const ENGINE_TYPE: &[&str] = &["ACTIVEMQ", "RABBITMQ"];
const DEPLOYMENT_MODE: &[&str] = &[
    "SINGLE_INSTANCE",
    "ACTIVE_STANDBY_MULTI_AZ",
    "CLUSTER_MULTI_AZ",
];
const AUTH_STRATEGY: &[&str] = &["SIMPLE", "LDAP", "CONFIG_MANAGED"];
const DATA_REPLICATION_MODE: &[&str] = &["NONE", "CRDR"];
const STORAGE_TYPE: &[&str] = &["EBS", "EFS"];
const PROMOTE_MODE: &[&str] = &["SWITCHOVER", "FAILOVER"];

fn require(b: &Value, field: &str) -> Result<(), AwsServiceError> {
    match b.get(field) {
        Some(Value::Null) | None => Err(bad_request(&format!("{field} is required."))),
        Some(Value::String(s)) if s.is_empty() => {
            Err(bad_request(&format!("{field} must not be empty.")))
        }
        _ => Ok(()),
    }
}

/// Amazon MQ's enum inputs are case-insensitive on the wire (the AWS SDK and
/// terraform send display-case values like `ActiveMQ` / `efs` while the Smithy
/// enum values are upper-cased), so compare case-insensitively.
fn check_enum(b: &Value, field: &str, allowed: &[&str]) -> Result<(), AwsServiceError> {
    if let Some(s) = b.get(field).and_then(Value::as_str) {
        if !allowed.iter().any(|a| a.eq_ignore_ascii_case(s)) {
            return Err(bad_request(&format!(
                "Value '{s}' at '{field}' failed to satisfy constraint: Member must satisfy enum value set: {allowed:?}"
            )));
        }
    }
    Ok(())
}

/// Validate an operation's already-parsed JSON body (camelCase `jsonName`s).
/// Path-label / query parameters are validated by the handlers themselves (a
/// missing/invalid id surfaces as the operation's declared not-found error).
pub fn validate_input(action: &str, b: &Value) -> Result<(), AwsServiceError> {
    match action {
        "CreateBroker" => {
            require(b, "brokerName")?;
            require(b, "deploymentMode")?;
            require(b, "engineType")?;
            require(b, "hostInstanceType")?;
            require(b, "publiclyAccessible")?;
            check_enum(b, "engineType", ENGINE_TYPE)?;
            check_enum(b, "deploymentMode", DEPLOYMENT_MODE)?;
            check_enum(b, "authenticationStrategy", AUTH_STRATEGY)?;
            check_enum(b, "storageType", STORAGE_TYPE)?;
            check_enum(b, "dataReplicationMode", DATA_REPLICATION_MODE)?;
        }
        "UpdateBroker" => {
            check_enum(b, "authenticationStrategy", AUTH_STRATEGY)?;
            check_enum(b, "dataReplicationMode", DATA_REPLICATION_MODE)?;
        }
        "CreateConfiguration" => {
            require(b, "engineType")?;
            require(b, "name")?;
            check_enum(b, "engineType", ENGINE_TYPE)?;
            check_enum(b, "authenticationStrategy", AUTH_STRATEGY)?;
        }
        "UpdateConfiguration" => {
            require(b, "data")?;
        }
        "CreateUser" => {
            require(b, "password")?;
        }
        "Promote" => {
            require(b, "mode")?;
            check_enum(b, "mode", PROMOTE_MODE)?;
        }
        _ => {}
    }
    Ok(())
}

/// Validate the constrained `@httpQuery` parameters shared across the
/// pagination-capable `List*`/`Describe*` operations. Rejecting an out-of-range
/// page size with `BadRequestException` (a code every affected operation
/// declares) keeps the conformance suite's negative query-parameter variants on
/// a declared error.
pub fn validate_query(q: &[(String, String)]) -> Result<(), AwsServiceError> {
    for (k, v) in q {
        // MaxResults carries `@range{min:1,max:100}`.
        if k == "maxResults"
            && !v
                .parse::<i64>()
                .map(|n| (1..=100).contains(&n))
                .unwrap_or(false)
        {
            return Err(bad_request(&format!(
                "Value '{v}' at 'maxResults' failed to satisfy constraint: Member must be between 1 and 100"
            )));
        }
    }
    Ok(())
}
