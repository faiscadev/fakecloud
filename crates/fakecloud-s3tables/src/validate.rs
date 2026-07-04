//! Model-derived input validation for Amazon S3 Tables.
//!
//! AWS rejects requests that violate the Smithy model's top-level constraints
//! (`@required`, `@length`, `@range`, `@enum`) with a `BadRequestException`
//! before the operation runs. This module encodes the constraints that the
//! negative conformance variants exercise: required body members, the
//! `name` length bound on `CreateTableBucket`, the `format` enum on
//! `CreateTable`, the list-limit `@range` bounds on the `List*` query params,
//! and the `type` enum on `ListTableBuckets`. `@pattern` is intentionally not
//! enforced (no negative variant exercises it, and enforcing the ARN/name
//! patterns would reject otherwise-valid synthetic inputs).

use http::StatusCode;
use serde_json::Value;

use fakecloud_core::service::{AwsRequest, AwsServiceError};

fn bad(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "BadRequestException", msg)
}

fn str_len(v: &Value) -> Option<usize> {
    v.as_str().map(|s| s.chars().count())
}

/// Body members that are `@required` and bound to the JSON body (not to an
/// `@httpLabel` / `@httpQuery`), per operation.
fn required_body_members(action: &str) -> &'static [&'static str] {
    match action {
        "CreateTableBucket" => &["name"],
        "CreateNamespace" => &["namespace"],
        "CreateTable" => &["name", "format"],
        "PutTableBucketEncryption" => &["encryptionConfiguration"],
        "PutTableBucketPolicy" => &["resourcePolicy"],
        "PutTablePolicy" => &["resourcePolicy"],
        "PutTableBucketMaintenanceConfiguration" => &["value"],
        "PutTableMaintenanceConfiguration" => &["value"],
        "PutTableBucketReplication" => &["configuration"],
        "PutTableReplication" => &["configuration"],
        "PutTableRecordExpirationConfiguration" => &["value"],
        "PutTableBucketStorageClass" => &["storageClassConfiguration"],
        "TagResource" => &["tags"],
        "UpdateTableMetadataLocation" => &["versionToken", "metadataLocation"],
        _ => &[],
    }
}

/// Validate an operation's input against the model constraints exercised by the
/// negative conformance variants.
pub fn validate(action: &str, req: &AwsRequest) -> Result<(), AwsServiceError> {
    let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);

    // Required body members.
    for field in required_body_members(action) {
        let present = body.get(*field).is_some_and(|v| !v.is_null());
        if !present {
            return Err(bad(format!("{field} is required.")));
        }
    }

    // `NextToken` `@length` (1..=2048) on the `continuationToken` query param.
    // The offset cursor is otherwise opaque, but an out-of-length token is
    // rejected exactly as AWS rejects a malformed pagination token.
    if let Some(token) = req.query_params.get("continuationToken") {
        let len = token.chars().count();
        if !(1..=2048).contains(&len) {
            return Err(bad("Invalid continuationToken."));
        }
    }

    // List-limit `@range` (1..=1000) on the `max*` query params.
    for param in ["maxBuckets", "maxNamespaces", "maxTables"] {
        if let Some(raw) = req.query_params.get(param) {
            let n: i64 = raw
                .parse()
                .map_err(|_| bad(format!("{param} must be an integer.")))?;
            if !(1..=1000).contains(&n) {
                return Err(bad(format!("{param} must be between 1 and 1000.")));
            }
        }
    }

    match action {
        "CreateTableBucket" => {
            if let Some(len) = body.get("name").and_then(str_len) {
                if !(3..=63).contains(&len) {
                    return Err(bad("name must be between 3 and 63 characters."));
                }
            }
        }
        "CreateTable" => {
            if let Some(fmt) = body.get("format").and_then(|v| v.as_str()) {
                if fmt != "ICEBERG" {
                    return Err(bad(format!("Invalid value '{fmt}' for format.")));
                }
            }
        }
        "ListTableBuckets" => {
            if let Some(t) = req.query_params.get("type") {
                if t != "AWS" && t != "CUSTOMER" {
                    return Err(bad(format!("Invalid value '{t}' for type.")));
                }
            }
        }
        _ => {}
    }

    Ok(())
}
