//! dynamodb helpers `table_lookup` concerns (audit-2026-05-19).

use super::*;

/// AWS DDB ops accept either a bare table name or an ARN of the form
/// `arn:aws:dynamodb:REGION:ACCOUNT:table/NAME[/index/...|/stream/...|/backup/...]`
/// in `TableName`. Real DynamoDB normalizes both transparently; the
/// SDKs send ARNs in cross-account scenarios. Strip everything past
/// `:table/` and any sub-resource segment so callers can use one path.
pub(crate) fn resolve_table_name(input: &str) -> &str {
    if let Some(rest) = input.strip_prefix("arn:aws:dynamodb:") {
        if let Some(after_table) = rest.split(":table/").nth(1) {
            // Drop any /index/<n>, /stream/<n>, /backup/<n> suffix.
            return after_table.split('/').next().unwrap_or(after_table);
        }
    }
    input
}

pub(crate) fn get_table<'a>(
    tables: &'a BTreeMap<String, DynamoTable>,
    name: &str,
) -> Result<&'a DynamoTable, AwsServiceError> {
    get_table_with_code(tables, name, "ResourceNotFoundException")
}

/// Variant of [`get_table`] that emits a caller-chosen wire error code.
///
/// Smithy declares different "table missing" shapes for different ops:
/// most read/write paths declare `ResourceNotFoundException`, while the
/// backup, continuous-backup, and export/restore paths declare
/// `TableNotFoundException` instead. Passing the wrong shape makes the
/// strict-mode conformance probe reject the response.
pub(crate) fn get_table_with_code<'a>(
    tables: &'a BTreeMap<String, DynamoTable>,
    name: &str,
    code: &str,
) -> Result<&'a DynamoTable, AwsServiceError> {
    let resolved = resolve_table_name(name);
    tables.get(resolved).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            code,
            format!("Requested resource not found: Table: {resolved} not found"),
        )
    })
}

pub(crate) fn get_table_mut<'a>(
    tables: &'a mut BTreeMap<String, DynamoTable>,
    name: &str,
) -> Result<&'a mut DynamoTable, AwsServiceError> {
    get_table_mut_with_code(tables, name, "ResourceNotFoundException")
}

pub(crate) fn get_table_mut_with_code<'a>(
    tables: &'a mut BTreeMap<String, DynamoTable>,
    name: &str,
    code: &str,
) -> Result<&'a mut DynamoTable, AwsServiceError> {
    let resolved = resolve_table_name(name).to_string();
    tables.get_mut(&resolved).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            code,
            format!("Requested resource not found: Table: {resolved} not found"),
        )
    })
}

pub(crate) fn find_table_by_arn<'a>(
    tables: &'a BTreeMap<String, DynamoTable>,
    arn: &str,
) -> Result<&'a DynamoTable, AwsServiceError> {
    tables.values().find(|t| t.arn == arn).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ResourceNotFoundException",
            format!("Requested resource not found: {arn}"),
        )
    })
}

pub(crate) fn find_table_by_arn_mut<'a>(
    tables: &'a mut BTreeMap<String, DynamoTable>,
    arn: &str,
) -> Result<&'a mut DynamoTable, AwsServiceError> {
    tables.values_mut().find(|t| t.arn == arn).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ResourceNotFoundException",
            format!("Requested resource not found: {arn}"),
        )
    })
}
