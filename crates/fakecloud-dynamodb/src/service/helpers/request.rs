//! dynamodb helpers `request` concerns (audit-2026-05-19).

use super::*;

/// Actions that mutate DynamoDB state and therefore require a snapshot
/// write after success. Kept in sync with the dispatch table above.
///
/// For PartiQL ops (`ExecuteStatement`, `BatchExecuteStatement`,
/// `ExecuteTransaction`) call [`is_mutating_request`] instead — those
/// can carry read-only SELECTs and forcing a snapshot per call wastes
/// I/O.
pub(crate) fn is_mutating_action(action: &str) -> bool {
    matches!(
        action,
        "CreateTable"
            | "DeleteTable"
            | "UpdateTable"
            | "PutItem"
            | "DeleteItem"
            | "UpdateItem"
            | "BatchWriteItem"
            | "TagResource"
            | "UntagResource"
            | "TransactWriteItems"
            | "UpdateTimeToLive"
            | "PutResourcePolicy"
            | "DeleteResourcePolicy"
            | "CreateBackup"
            | "DeleteBackup"
            | "RestoreTableFromBackup"
            | "RestoreTableToPointInTime"
            | "UpdateContinuousBackups"
            | "CreateGlobalTable"
            | "UpdateGlobalTable"
            | "UpdateGlobalTableSettings"
            | "UpdateTableReplicaAutoScaling"
            | "EnableKinesisStreamingDestination"
            | "DisableKinesisStreamingDestination"
            | "UpdateKinesisStreamingDestination"
            | "UpdateContributorInsights"
            | "ExportTableToPointInTime"
            | "ImportTable"
    )
}

/// True when a PartiQL statement is a write (INSERT / UPDATE / DELETE
/// / UPSERT / MERGE). DynamoDB rejects `CREATE` / `DROP` PartiQL, so
/// only the data-modification verbs are interesting.
fn partiql_is_write(stmt: &str) -> bool {
    let trimmed = stmt.trim_start();
    // Strip any leading SQL comments (`-- ...` or `/* ... */`).
    let trimmed = strip_sql_comments(trimmed);
    let kw = trimmed
        .split(|c: char| c.is_whitespace())
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    matches!(
        kw.as_str(),
        "INSERT" | "UPDATE" | "DELETE" | "UPSERT" | "MERGE"
    )
}

fn strip_sql_comments(s: &str) -> &str {
    let s = s.trim_start();
    if let Some(rest) = s.strip_prefix("--") {
        if let Some(nl) = rest.find('\n') {
            return strip_sql_comments(&rest[nl + 1..]);
        }
        return "";
    }
    if let Some(rest) = s.strip_prefix("/*") {
        if let Some(end) = rest.find("*/") {
            return strip_sql_comments(&rest[end + 2..]);
        }
        return "";
    }
    s
}

/// Whether a specific request mutates state — extends
/// [`is_mutating_action`] with body inspection for PartiQL ops so
/// read-only SELECTs don't trigger snapshot writes.
pub(crate) fn is_mutating_request(action: &str, body: &Value) -> bool {
    if is_mutating_action(action) {
        return true;
    }
    match action {
        "ExecuteStatement" => body
            .get("Statement")
            .and_then(Value::as_str)
            .map(partiql_is_write)
            .unwrap_or(true),
        "ExecuteTransaction" => body
            .get("TransactStatements")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter().any(|s| {
                    s.get("Statement")
                        .and_then(Value::as_str)
                        .map(partiql_is_write)
                        .unwrap_or(true)
                })
            })
            .unwrap_or(true),
        "BatchExecuteStatement" => body
            .get("Statements")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter().any(|s| {
                    s.get("Statement")
                        .and_then(Value::as_str)
                        .map(partiql_is_write)
                        .unwrap_or(true)
                })
            })
            .unwrap_or(true),
        _ => false,
    }
}

// ── Helper functions ────────────────────────────────────────────────────

pub(crate) fn require_str<'a>(body: &'a Value, field: &str) -> Result<&'a str, AwsServiceError> {
    require_str_with_code(body, field, "ValidationException")
}

/// Like [`require_str`] but emits a caller-chosen wire error code.
///
/// Several DynamoDB ops (e.g. CreateBackup, DescribeContinuousBackups,
/// UpdateContinuousBackups, ExportTableToPointInTime,
/// RestoreTableToPointInTime, ListBackups, ListImports) do not declare
/// `ValidationException` in their Smithy `errors:` list. Use this helper
/// to surface a declared shape (e.g. `TableNotFoundException`) for the
/// missing-required-field path on those ops.
pub(crate) fn require_str_with_code<'a>(
    body: &'a Value,
    field: &str,
    code: &str,
) -> Result<&'a str, AwsServiceError> {
    body[field].as_str().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            code,
            format!("{field} is required"),
        )
    })
}

pub(crate) fn require_object(
    body: &Value,
    field: &str,
) -> Result<HashMap<String, AttributeValue>, AwsServiceError> {
    let obj = body[field].as_object().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!("{field} is required"),
        )
    })?;
    Ok(obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
}
