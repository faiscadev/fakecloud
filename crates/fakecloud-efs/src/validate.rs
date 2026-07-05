//! Model-derived input validation for EFS operations.
//!
//! Rejects requests that violate the Smithy contract (missing required members,
//! out-of-set enum values, empty required lists) with `BadRequest` (the code
//! every affected operation declares) so the conformance suite's negative
//! variants land on a declared error rather than a routing miss or a 500.

use http::StatusCode;
use serde_json::Value;

use fakecloud_core::service::AwsServiceError;

fn bad_request(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "BadRequest", msg)
}

const PERFORMANCE_MODE: &[&str] = &["generalPurpose", "maxIO"];
const THROUGHPUT_MODE: &[&str] = &["bursting", "provisioned", "elastic"];
const BACKUP_STATUS: &[&str] = &["ENABLED", "ENABLING", "DISABLED", "DISABLING"];
const RESOURCE_ID_TYPE: &[&str] = &["LONG_ID", "SHORT_ID"];
const IP_ADDRESS_TYPE: &[&str] = &["IPV4_ONLY", "IPV6_ONLY", "DUAL_STACK"];
const REPLICATION_OVERWRITE_PROTECTION: &[&str] = &["ENABLED", "DISABLED", "REPLICATING"];

fn require(b: &Value, field: &str) -> Result<(), AwsServiceError> {
    match b.get(field) {
        Some(Value::Null) | None => Err(bad_request(&format!("{field} is required."))),
        Some(Value::String(s)) if s.is_empty() => {
            Err(bad_request(&format!("{field} must not be empty.")))
        }
        _ => Ok(()),
    }
}

fn require_nonempty_array(b: &Value, field: &str) -> Result<(), AwsServiceError> {
    match b.get(field) {
        Some(Value::Array(a)) if !a.is_empty() => Ok(()),
        Some(Value::Array(_)) => Err(bad_request(&format!("{field} must not be empty."))),
        _ => Err(bad_request(&format!("{field} is required."))),
    }
}

fn check_len(b: &Value, field: &str, min: usize, max: usize) -> Result<(), AwsServiceError> {
    if let Some(s) = b.get(field).and_then(Value::as_str) {
        let len = s.chars().count();
        if len < min || len > max {
            return Err(bad_request(&format!(
                "Value at '{field}' failed to satisfy constraint: Member must have length between {min} and {max}"
            )));
        }
    }
    Ok(())
}

fn check_enum(b: &Value, field: &str, allowed: &[&str]) -> Result<(), AwsServiceError> {
    if let Some(v) = b.get(field) {
        if let Some(s) = v.as_str() {
            if !allowed.contains(&s) {
                return Err(bad_request(&format!(
                    "Value '{s}' at '{field}' failed to satisfy constraint: Member must satisfy enum value set: {allowed:?}"
                )));
            }
        }
    }
    Ok(())
}

/// Validate an operation's already-parsed JSON body. Path-label / query
/// parameters are validated by the handlers themselves (a missing/invalid id
/// surfaces as the operation's declared not-found error).
pub fn validate_input(action: &str, b: &Value) -> Result<(), AwsServiceError> {
    match action {
        "CreateFileSystem" => {
            require(b, "CreationToken")?;
            check_len(b, "CreationToken", 1, 64)?;
            check_enum(b, "PerformanceMode", PERFORMANCE_MODE)?;
            check_enum(b, "ThroughputMode", THROUGHPUT_MODE)?;
        }
        "CreateMountTarget" => {
            require(b, "FileSystemId")?;
            require(b, "SubnetId")?;
            check_enum(b, "IpAddressType", IP_ADDRESS_TYPE)?;
        }
        "CreateAccessPoint" => {
            require(b, "ClientToken")?;
            require(b, "FileSystemId")?;
        }
        "CreateReplicationConfiguration" => {
            require_nonempty_array(b, "Destinations")?;
        }
        "CreateTags" => {
            require_nonempty_array(b, "Tags")?;
        }
        "DeleteTags" => {
            require_nonempty_array(b, "TagKeys")?;
        }
        "TagResource" => {
            require_nonempty_array(b, "Tags")?;
        }
        "PutLifecycleConfiguration" => {
            require(b, "LifecyclePolicies")?;
        }
        "PutBackupPolicy" => {
            require(b, "BackupPolicy")?;
            if let Some(p) = b.get("BackupPolicy") {
                check_enum(p, "Status", BACKUP_STATUS)?;
            }
        }
        "PutFileSystemPolicy" => {
            require(b, "Policy")?;
        }
        "PutAccountPreferences" => {
            require(b, "ResourceIdType")?;
            check_enum(b, "ResourceIdType", RESOURCE_ID_TYPE)?;
        }
        "UpdateFileSystem" => {
            check_enum(b, "ThroughputMode", THROUGHPUT_MODE)?;
        }
        "UpdateFileSystemProtection" => {
            check_enum(
                b,
                "ReplicationOverwriteProtection",
                REPLICATION_OVERWRITE_PROTECTION,
            )?;
        }
        _ => {}
    }
    Ok(())
}

/// Validate the constrained `@httpQuery` parameters shared across the
/// pagination-capable `Describe*`/`List*` operations. Rejecting an
/// out-of-range page size or an out-of-length pagination token with
/// `BadRequest` (a code every affected operation declares) keeps the
/// conformance suite's negative query-parameter variants on a declared error.
pub fn validate_query(q: &[(String, String)]) -> Result<(), AwsServiceError> {
    for (k, v) in q {
        match k.as_str() {
            // MaxItems / MaxResults carry `@range{min:1}`.
            "MaxItems" | "MaxResults" if !v.parse::<i64>().map(|n| n >= 1).unwrap_or(false) => {
                return Err(bad_request(&format!(
                    "Value '{v}' at '{k}' failed to satisfy constraint: Member must have value greater than or equal to 1"
                )));
            }
            // Marker / NextToken carry `@length{min:1,max:128}`.
            "Marker" | "NextToken" if v.is_empty() || v.chars().count() > 128 => {
                return Err(bad_request(&format!(
                    "Value at '{k}' failed to satisfy constraint: Member must have length between 1 and 128"
                )));
            }
            // CreationToken carries `@length{min:1,max:64}`.
            "CreationToken" if v.is_empty() || v.chars().count() > 64 => {
                return Err(bad_request(
                    "Value at 'CreationToken' failed to satisfy constraint: Member must have length between 1 and 64",
                ));
            }
            _ => {}
        }
    }
    Ok(())
}
