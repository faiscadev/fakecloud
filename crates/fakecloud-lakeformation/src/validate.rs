//! Model-derived input validation for AWS Lake Formation.
//!
//! Encodes the Smithy model's top-level `@required` / `@length` / `@range` /
//! `@enum` constraints so out-of-range, bad-enum, too-long/short, or omitted
//! inputs are rejected with a 4xx before the operation runs, matching how the
//! real service validates. Every Lake Formation input member is bound to the
//! JSON body (restJson1, no `@httpLabel`/`@httpQuery` members), so the source
//! is implicit. Rules are generated from `aws-models/lakeformation.json`.

use http::StatusCode;
use serde_json::Value;

use fakecloud_core::service::{AwsRequest, AwsServiceError};

/// A single input-constraint rule from the Smithy model.
pub enum Rule {
    Required(&'static str),
    LenMin(&'static str, usize),
    LenMax(&'static str, usize),
    IntMin(&'static str, f64),
    IntMax(&'static str, f64),
    Enum(&'static str, &'static [&'static str]),
}

fn bad(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidInputException", msg)
}

fn value_len(v: &Value) -> Option<usize> {
    match v {
        Value::String(s) => Some(s.chars().count()),
        Value::Array(a) => Some(a.len()),
        Value::Object(o) => Some(o.len()),
        _ => None,
    }
}

fn as_f64(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

/// Validate an operation's request body against the model constraints.
pub fn validate(action: &str, req: &AwsRequest) -> Result<(), AwsServiceError> {
    let rules = input_rules(action);
    if rules.is_empty() {
        return Ok(());
    }
    let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
    let raw = |field: &str| -> Option<Value> { body.get(field).cloned().filter(|v| !v.is_null()) };

    for rule in rules {
        match rule {
            Rule::Required(f) => {
                if raw(f).is_none() {
                    return Err(bad(format!("{f} is required.")));
                }
            }
            Rule::LenMin(f, n) => {
                if let Some(len) = raw(f).as_ref().and_then(value_len) {
                    if len < n {
                        return Err(bad(format!("{f} is shorter than the minimum length.")));
                    }
                }
            }
            Rule::LenMax(f, n) => {
                if let Some(len) = raw(f).as_ref().and_then(value_len) {
                    if len > n {
                        return Err(bad(format!("{f} exceeds the maximum length.")));
                    }
                }
            }
            Rule::IntMin(f, n) => {
                if let Some(x) = raw(f).as_ref().and_then(as_f64) {
                    if x < n {
                        return Err(bad(format!("{f} is below the minimum value.")));
                    }
                }
            }
            Rule::IntMax(f, n) => {
                if let Some(x) = raw(f).as_ref().and_then(as_f64) {
                    if x > n {
                        return Err(bad(format!("{f} exceeds the maximum value.")));
                    }
                }
            }
            Rule::Enum(f, vals) => {
                if let Some(Value::String(s)) = raw(f) {
                    if !vals.contains(&s.as_str()) {
                        return Err(bad(format!("{f} is not a valid value.")));
                    }
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
const ENUM_0: &[&str] = &["READ", "READWRITE"];
const ENUM_1: &[&str] = &["FOREIGN", "ALL"];
const ENUM_2: &[&str] = &[
    "CATALOG",
    "DATABASE",
    "TABLE",
    "DATA_LOCATION",
    "LF_TAG",
    "LF_TAG_POLICY",
    "LF_TAG_POLICY_DATABASE",
    "LF_TAG_POLICY_TABLE",
    "LF_NAMED_TAG_EXPRESSION",
];
const ENUM_3: &[&str] = &["COMPACTION", "GARBAGE_COLLECTION", "ALL"];
const ENUM_4: &[&str] = &["ALL", "COMPLETED", "ACTIVE", "COMMITTED", "ABORTED"];
const ENUM_5: &[&str] = &["READ_AND_WRITE", "READ_ONLY"];
const ENUM_6: &[&str] = &["ENABLED", "DISABLED"];

fn input_rules(action: &str) -> Vec<Rule> {
    match action {
        "AddLFTagsToResource" => vec![
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
            Rule::Required("Resource"),
            Rule::Required("LFTags"),
        ],
        "AssumeDecoratedRoleWithSAML" => vec![
            Rule::Required("SAMLAssertion"),
            Rule::LenMin("SAMLAssertion", 4),
            Rule::Required("RoleArn"),
            Rule::Required("PrincipalArn"),
            Rule::IntMin("DurationSeconds", 900.0),
            Rule::IntMax("DurationSeconds", 43200.0),
        ],
        "BatchGrantPermissions" => vec![
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
            Rule::Required("Entries"),
        ],
        "BatchRevokePermissions" => vec![
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
            Rule::Required("Entries"),
        ],
        "CancelTransaction" => vec![
            Rule::Required("TransactionId"),
            Rule::LenMin("TransactionId", 1),
            Rule::LenMax("TransactionId", 255),
        ],
        "CommitTransaction" => vec![
            Rule::Required("TransactionId"),
            Rule::LenMin("TransactionId", 1),
            Rule::LenMax("TransactionId", 255),
        ],
        "CreateDataCellsFilter" => vec![Rule::Required("TableData")],
        "CreateLFTag" => vec![
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
            Rule::Required("TagKey"),
            Rule::LenMin("TagKey", 1),
            Rule::LenMax("TagKey", 128),
            Rule::Required("TagValues"),
        ],
        "CreateLFTagExpression" => vec![
            Rule::Required("Name"),
            Rule::LenMin("Name", 1),
            Rule::LenMax("Name", 255),
            Rule::LenMax("Description", 2048),
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
            Rule::Required("Expression"),
        ],
        "CreateLakeFormationIdentityCenterConfiguration" => {
            vec![Rule::LenMin("CatalogId", 1), Rule::LenMax("CatalogId", 255)]
        }
        "CreateLakeFormationOptIn" => vec![Rule::Required("Principal"), Rule::Required("Resource")],
        "DeleteDataCellsFilter" => vec![
            Rule::LenMin("TableCatalogId", 1),
            Rule::LenMax("TableCatalogId", 255),
            Rule::LenMin("DatabaseName", 1),
            Rule::LenMax("DatabaseName", 255),
            Rule::LenMin("TableName", 1),
            Rule::LenMax("TableName", 255),
            Rule::LenMin("Name", 1),
            Rule::LenMax("Name", 255),
        ],
        "DeleteLFTag" => vec![
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
            Rule::Required("TagKey"),
            Rule::LenMin("TagKey", 1),
            Rule::LenMax("TagKey", 128),
        ],
        "DeleteLFTagExpression" => vec![
            Rule::Required("Name"),
            Rule::LenMin("Name", 1),
            Rule::LenMax("Name", 255),
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
        ],
        "DeleteLakeFormationIdentityCenterConfiguration" => {
            vec![Rule::LenMin("CatalogId", 1), Rule::LenMax("CatalogId", 255)]
        }
        "DeleteLakeFormationOptIn" => vec![Rule::Required("Principal"), Rule::Required("Resource")],
        "DeleteObjectsOnCancel" => vec![
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
            Rule::Required("DatabaseName"),
            Rule::LenMin("DatabaseName", 1),
            Rule::LenMax("DatabaseName", 255),
            Rule::Required("TableName"),
            Rule::LenMin("TableName", 1),
            Rule::LenMax("TableName", 255),
            Rule::Required("TransactionId"),
            Rule::LenMin("TransactionId", 1),
            Rule::LenMax("TransactionId", 255),
            Rule::Required("Objects"),
        ],
        "DeregisterResource" => vec![Rule::Required("ResourceArn")],
        "DescribeLakeFormationIdentityCenterConfiguration" => {
            vec![Rule::LenMin("CatalogId", 1), Rule::LenMax("CatalogId", 255)]
        }
        "DescribeResource" => vec![Rule::Required("ResourceArn")],
        "DescribeTransaction" => vec![
            Rule::Required("TransactionId"),
            Rule::LenMin("TransactionId", 1),
            Rule::LenMax("TransactionId", 255),
        ],
        "ExtendTransaction" => vec![
            Rule::LenMin("TransactionId", 1),
            Rule::LenMax("TransactionId", 255),
        ],
        "GetDataCellsFilter" => vec![
            Rule::Required("TableCatalogId"),
            Rule::LenMin("TableCatalogId", 1),
            Rule::LenMax("TableCatalogId", 255),
            Rule::Required("DatabaseName"),
            Rule::LenMin("DatabaseName", 1),
            Rule::LenMax("DatabaseName", 255),
            Rule::Required("TableName"),
            Rule::LenMin("TableName", 1),
            Rule::LenMax("TableName", 255),
            Rule::Required("Name"),
            Rule::LenMin("Name", 1),
            Rule::LenMax("Name", 255),
        ],
        "GetDataLakeSettings" => vec![Rule::LenMin("CatalogId", 1), Rule::LenMax("CatalogId", 255)],
        "GetEffectivePermissionsForPath" => vec![
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
            Rule::Required("ResourceArn"),
            Rule::IntMin("MaxResults", 1.0),
            Rule::IntMax("MaxResults", 1000.0),
        ],
        "GetLFTag" => vec![
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
            Rule::Required("TagKey"),
            Rule::LenMin("TagKey", 1),
            Rule::LenMax("TagKey", 128),
        ],
        "GetLFTagExpression" => vec![
            Rule::Required("Name"),
            Rule::LenMin("Name", 1),
            Rule::LenMax("Name", 255),
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
        ],
        "GetQueryState" => vec![Rule::Required("QueryId"), Rule::LenMax("QueryId", 36)],
        "GetQueryStatistics" => vec![Rule::Required("QueryId"), Rule::LenMax("QueryId", 36)],
        "GetResourceLFTags" => vec![
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
            Rule::Required("Resource"),
        ],
        "GetTableObjects" => vec![
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
            Rule::Required("DatabaseName"),
            Rule::LenMin("DatabaseName", 1),
            Rule::LenMax("DatabaseName", 255),
            Rule::Required("TableName"),
            Rule::LenMin("TableName", 1),
            Rule::LenMax("TableName", 255),
            Rule::LenMin("TransactionId", 1),
            Rule::LenMax("TransactionId", 255),
            Rule::LenMax("PartitionPredicate", 2048),
            Rule::IntMin("MaxResults", 1.0),
            Rule::IntMax("MaxResults", 1000.0),
            Rule::LenMax("NextToken", 4096),
        ],
        "GetTemporaryDataLocationCredentials" => vec![
            Rule::IntMin("DurationSeconds", 900.0),
            Rule::IntMax("DurationSeconds", 43200.0),
            Rule::Enum("CredentialsScope", ENUM_0),
        ],
        "GetTemporaryGluePartitionCredentials" => vec![
            Rule::Required("TableArn"),
            Rule::Required("Partition"),
            Rule::IntMin("DurationSeconds", 900.0),
            Rule::IntMax("DurationSeconds", 43200.0),
        ],
        "GetTemporaryGlueTableCredentials" => vec![
            Rule::Required("TableArn"),
            Rule::IntMin("DurationSeconds", 900.0),
            Rule::IntMax("DurationSeconds", 43200.0),
        ],
        "GetWorkUnitResults" => vec![
            Rule::Required("QueryId"),
            Rule::LenMax("QueryId", 36),
            Rule::Required("WorkUnitId"),
            Rule::IntMin("WorkUnitId", 0.0),
            Rule::Required("WorkUnitToken"),
            Rule::LenMin("WorkUnitToken", 1),
        ],
        "GetWorkUnits" => vec![Rule::Required("QueryId"), Rule::LenMax("QueryId", 36)],
        "GrantPermissions" => vec![
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
            Rule::Required("Principal"),
            Rule::Required("Resource"),
            Rule::Required("Permissions"),
        ],
        "ListDataCellsFilter" => vec![
            Rule::IntMin("MaxResults", 1.0),
            Rule::IntMax("MaxResults", 1000.0),
        ],
        "ListLFTagExpressions" => vec![
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
            Rule::IntMin("MaxResults", 1.0),
            Rule::IntMax("MaxResults", 1000.0),
        ],
        "ListLFTags" => vec![
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
            Rule::Enum("ResourceShareType", ENUM_1),
            Rule::IntMin("MaxResults", 1.0),
            Rule::IntMax("MaxResults", 1000.0),
        ],
        "ListLakeFormationOptIns" => vec![
            Rule::IntMin("MaxResults", 1.0),
            Rule::IntMax("MaxResults", 1000.0),
        ],
        "ListPermissions" => vec![
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
            Rule::Enum("ResourceType", ENUM_2),
            Rule::IntMin("MaxResults", 1.0),
            Rule::IntMax("MaxResults", 1000.0),
            Rule::LenMin("IncludeRelated", 1),
            Rule::LenMax("IncludeRelated", 5),
        ],
        "ListResources" => vec![
            Rule::IntMin("MaxResults", 1.0),
            Rule::IntMax("MaxResults", 1000.0),
        ],
        "ListTableStorageOptimizers" => vec![
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
            Rule::Required("DatabaseName"),
            Rule::LenMin("DatabaseName", 1),
            Rule::LenMax("DatabaseName", 255),
            Rule::Required("TableName"),
            Rule::LenMin("TableName", 1),
            Rule::LenMax("TableName", 255),
            Rule::Enum("StorageOptimizerType", ENUM_3),
            Rule::IntMin("MaxResults", 1.0),
            Rule::IntMax("MaxResults", 1000.0),
        ],
        "ListTransactions" => vec![
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
            Rule::Enum("StatusFilter", ENUM_4),
            Rule::IntMin("MaxResults", 1.0),
            Rule::IntMax("MaxResults", 1000.0),
            Rule::LenMax("NextToken", 4096),
        ],
        "PutDataLakeSettings" => vec![
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
            Rule::Required("DataLakeSettings"),
        ],
        "RegisterResource" => vec![Rule::Required("ResourceArn")],
        "RemoveLFTagsFromResource" => vec![
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
            Rule::Required("Resource"),
            Rule::Required("LFTags"),
        ],
        "RevokePermissions" => vec![
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
            Rule::Required("Principal"),
            Rule::Required("Resource"),
            Rule::Required("Permissions"),
        ],
        "SearchDatabasesByLFTags" => vec![
            Rule::IntMin("MaxResults", 1.0),
            Rule::IntMax("MaxResults", 100.0),
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
            Rule::Required("Expression"),
        ],
        "SearchTablesByLFTags" => vec![
            Rule::IntMin("MaxResults", 1.0),
            Rule::IntMax("MaxResults", 100.0),
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
            Rule::Required("Expression"),
        ],
        "StartQueryPlanning" => vec![
            Rule::Required("QueryPlanningContext"),
            Rule::Required("QueryString"),
            Rule::LenMin("QueryString", 1),
        ],
        "StartTransaction" => vec![Rule::Enum("TransactionType", ENUM_5)],
        "UpdateDataCellsFilter" => vec![Rule::Required("TableData")],
        "UpdateLFTag" => vec![
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
            Rule::Required("TagKey"),
            Rule::LenMin("TagKey", 1),
            Rule::LenMax("TagKey", 128),
        ],
        "UpdateLFTagExpression" => vec![
            Rule::Required("Name"),
            Rule::LenMin("Name", 1),
            Rule::LenMax("Name", 255),
            Rule::LenMax("Description", 2048),
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
            Rule::Required("Expression"),
        ],
        "UpdateLakeFormationIdentityCenterConfiguration" => vec![
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
            Rule::Enum("ApplicationStatus", ENUM_6),
        ],
        "UpdateResource" => vec![Rule::Required("RoleArn"), Rule::Required("ResourceArn")],
        "UpdateTableObjects" => vec![
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
            Rule::Required("DatabaseName"),
            Rule::LenMin("DatabaseName", 1),
            Rule::LenMax("DatabaseName", 255),
            Rule::Required("TableName"),
            Rule::LenMin("TableName", 1),
            Rule::LenMax("TableName", 255),
            Rule::LenMin("TransactionId", 1),
            Rule::LenMax("TransactionId", 255),
            Rule::Required("WriteOperations"),
        ],
        "UpdateTableStorageOptimizer" => vec![
            Rule::LenMin("CatalogId", 1),
            Rule::LenMax("CatalogId", 255),
            Rule::Required("DatabaseName"),
            Rule::LenMin("DatabaseName", 1),
            Rule::LenMax("DatabaseName", 255),
            Rule::Required("TableName"),
            Rule::LenMin("TableName", 1),
            Rule::LenMax("TableName", 255),
            Rule::Required("StorageOptimizerConfig"),
        ],
        _ => Vec::new(),
    }
}
