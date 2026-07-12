//! Model-derived input validation for AWS AppSync operations.
//!
//! Rejects requests that violate the Smithy contract (missing required body
//! members, out-of-set enum values) with `BadRequestException` -- the error
//! code every AppSync operation declares -- so the conformance suite's negative
//! variants land on a declared error rather than a routing miss or a 500. Path
//! label required members are validated during routing (an empty label rejects
//! before dispatch); this pass covers the JSON *body* members and enums.

use http::StatusCode;
use serde_json::Value;

use fakecloud_core::service::AwsServiceError;

pub const AUTH_TYPES: &[&str] = &[
    "API_KEY",
    "AWS_IAM",
    "AMAZON_COGNITO_USER_POOLS",
    "OPENID_CONNECT",
    "AWS_LAMBDA",
];

pub const DATA_SOURCE_TYPES: &[&str] = &[
    "AWS_LAMBDA",
    "AMAZON_DYNAMODB",
    "AMAZON_ELASTICSEARCH",
    "NONE",
    "HTTP",
    "RELATIONAL_DATABASE",
    "AMAZON_OPENSEARCH_SERVICE",
    "AMAZON_EVENTBRIDGE",
    "AMAZON_BEDROCK_RUNTIME",
];

pub const TYPE_DEF_FORMAT: &[&str] = &["SDL", "JSON"];
pub const GRAPHQL_API_TYPE: &[&str] = &["GRAPHQL", "MERGED"];
pub const VISIBILITY: &[&str] = &["GLOBAL", "PRIVATE"];
pub const INTROSPECTION_CONFIG: &[&str] = &["ENABLED", "DISABLED"];
pub const OWNERSHIP: &[&str] = &["CURRENT_ACCOUNT", "OTHER_ACCOUNTS"];
pub const RESOLVER_KIND: &[&str] = &["UNIT", "PIPELINE"];
pub const API_CACHING_BEHAVIOR: &[&str] = &[
    "FULL_REQUEST_CACHING",
    "PER_RESOLVER_CACHING",
    "OPERATION_LEVEL_CACHING",
];
pub const API_CACHE_TYPE: &[&str] = &[
    "T2_SMALL",
    "T2_MEDIUM",
    "R4_LARGE",
    "R4_XLARGE",
    "R4_2XLARGE",
    "R4_4XLARGE",
    "R4_8XLARGE",
    "SMALL",
    "MEDIUM",
    "LARGE",
    "XLARGE",
    "LARGE_2X",
    "LARGE_4X",
    "LARGE_8X",
    "LARGE_12X",
];

fn bad(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "BadRequestException", msg)
}

/// A present, non-null required body member.
fn require<'a>(b: &'a Value, field: &str) -> Result<&'a Value, AwsServiceError> {
    match b.get(field) {
        Some(Value::Null) | None => Err(bad(&format!("{field} is required."))),
        Some(v) => Ok(v),
    }
}

fn enum_opt(b: &Value, field: &str, allowed: &[&str]) -> Result<(), AwsServiceError> {
    if let Some(v) = b.get(field).and_then(Value::as_str) {
        if !allowed.contains(&v) {
            return Err(bad(&format!(
                "{field} must be one of [{}], got '{v}'.",
                allowed.join(", ")
            )));
        }
    }
    Ok(())
}

fn enum_req(b: &Value, field: &str, allowed: &[&str]) -> Result<(), AwsServiceError> {
    require(b, field)?;
    enum_opt(b, field, allowed)
}

/// Reject a present string body member whose character length is outside
/// `[min, max]`.
fn len_opt(b: &Value, field: &str, min: usize, max: usize) -> Result<(), AwsServiceError> {
    if let Some(s) = b.get(field).and_then(Value::as_str) {
        let len = s.chars().count();
        if len < min || len > max {
            return Err(bad(&format!(
                "{field} length {len} is outside the allowed range [{min}, {max}]."
            )));
        }
    }
    Ok(())
}

/// Reject a present integer body member outside `[min, max]`.
fn range_opt(b: &Value, field: &str, min: i64, max: i64) -> Result<(), AwsServiceError> {
    if let Some(v) = b.get(field).and_then(Value::as_i64) {
        if v < min || v > max {
            return Err(bad(&format!(
                "{field} value {v} is outside the allowed range [{min}, {max}]."
            )));
        }
    }
    Ok(())
}

/// Validate the request query string for an operation: the common `maxResults`
/// (0..=25) and `nextToken` (>=1 char) bounds plus per-op query enums. Returns
/// `BadRequestException` on violation, which every AppSync op declares.
pub fn validate_query(action: &str, q: &[(String, String)]) -> Result<(), AwsServiceError> {
    for (k, v) in q {
        match k.as_str() {
            "maxResults" => match v.parse::<i64>() {
                Ok(n) if !(0..=25).contains(&n) => {
                    return Err(bad(&format!(
                        "maxResults value {n} is outside the allowed range [0, 25]."
                    )));
                }
                Ok(_) => {}
                Err(_) if !v.is_empty() => {
                    return Err(bad("maxResults must be an integer."));
                }
                Err(_) => {}
            },
            "nextToken" if v.is_empty() || v.chars().count() > 65536 => {
                return Err(bad(
                    "nextToken length is outside the allowed range [1, 65536].",
                ));
            }
            "owner" if action == "ListGraphqlApis" && !OWNERSHIP.contains(&v.as_str()) => {
                return Err(bad(&format!("owner must be one of {OWNERSHIP:?}.")));
            }
            "apiType" if action == "ListGraphqlApis" && !GRAPHQL_API_TYPE.contains(&v.as_str()) => {
                return Err(bad(&format!(
                    "apiType must be one of {GRAPHQL_API_TYPE:?}."
                )));
            }
            "format"
                if matches!(action, "ListTypesByAssociation" | "ListTypes" | "GetType")
                    && !TYPE_DEF_FORMAT.contains(&v.as_str()) =>
            {
                return Err(bad(&format!("format must be one of {TYPE_DEF_FORMAT:?}.")));
            }
            _ => {}
        }
    }
    Ok(())
}

/// Validate an operation's already-parsed JSON body against the model. Only the
/// JSON-body members are checked here; path labels are enforced in routing.
pub fn validate_input(action: &str, body: &Value) -> Result<(), AwsServiceError> {
    match action {
        "CreateGraphqlApi" => {
            require(body, "name")?;
            enum_req(body, "authenticationType", AUTH_TYPES)?;
            enum_opt(body, "apiType", GRAPHQL_API_TYPE)?;
            enum_opt(body, "visibility", VISIBILITY)?;
            enum_opt(body, "introspectionConfig", INTROSPECTION_CONFIG)?;
            range_opt(body, "queryDepthLimit", 0, 75)?;
            range_opt(body, "resolverCountLimit", 0, 10000)?;
        }
        "UpdateGraphqlApi" => {
            require(body, "name")?;
            enum_req(body, "authenticationType", AUTH_TYPES)?;
            enum_opt(body, "introspectionConfig", INTROSPECTION_CONFIG)?;
            range_opt(body, "queryDepthLimit", 0, 75)?;
            range_opt(body, "resolverCountLimit", 0, 10000)?;
        }
        "CreateApi" => {
            require(body, "name")?;
            len_opt(body, "name", 1, 50)?;
            require(body, "eventConfig")?;
        }
        "UpdateApi" => {
            require(body, "name")?;
            len_opt(body, "name", 1, 50)?;
            require(body, "eventConfig")?;
        }
        "CreateApiCache" | "UpdateApiCache" => {
            require(body, "ttl")?;
            enum_req(body, "apiCachingBehavior", API_CACHING_BEHAVIOR)?;
            enum_req(body, "type", API_CACHE_TYPE)?;
        }
        "CreateChannelNamespace" => {
            require(body, "name")?;
        }
        "CreateDataSource" => {
            require(body, "name")?;
            enum_req(body, "type", DATA_SOURCE_TYPES)?;
        }
        "UpdateDataSource" => {
            enum_req(body, "type", DATA_SOURCE_TYPES)?;
        }
        "CreateDomainName" => {
            require(body, "domainName")?;
            len_opt(body, "domainName", 1, 253)?;
            require(body, "certificateArn")?;
            len_opt(body, "certificateArn", 20, 2048)?;
            len_opt(body, "description", 0, 255)?;
        }
        "UpdateDomainName" => {
            len_opt(body, "description", 0, 255)?;
        }
        "CreateFunction" => {
            require(body, "name")?;
            require(body, "dataSourceName")?;
        }
        "UpdateFunction" => {
            require(body, "name")?;
            require(body, "dataSourceName")?;
        }
        "CreateResolver" => {
            require(body, "fieldName")?;
            enum_opt(body, "kind", RESOLVER_KIND)?;
        }
        "UpdateResolver" => {
            enum_opt(body, "kind", RESOLVER_KIND)?;
        }
        "CreateType" => {
            require(body, "definition")?;
            enum_req(body, "format", TYPE_DEF_FORMAT)?;
        }
        "UpdateType" => {
            enum_req(body, "format", TYPE_DEF_FORMAT)?;
        }
        "EvaluateCode" => {
            require(body, "runtime")?;
            require(body, "code")?;
            len_opt(body, "code", 1, 32768)?;
            require(body, "context")?;
            len_opt(body, "context", 2, 28000)?;
        }
        "EvaluateMappingTemplate" => {
            require(body, "template")?;
            len_opt(body, "template", 2, 65536)?;
            require(body, "context")?;
            len_opt(body, "context", 2, 28000)?;
        }
        "PutGraphqlApiEnvironmentVariables" => {
            require(body, "environmentVariables")?;
        }
        "StartSchemaCreation" => {
            require(body, "definition")?;
        }
        "StartSchemaMerge" => {
            require(body, "mergedApiIdentifier")?;
        }
        "TagResource" => {
            require(body, "tags")?;
        }
        "AssociateApi" => {
            require(body, "apiId")?;
        }
        "AssociateMergedGraphqlApi" => {
            require(body, "mergedApiIdentifier")?;
        }
        "AssociateSourceGraphqlApi" => {
            require(body, "sourceApiIdentifier")?;
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
    fn create_graphql_api_requires_name_and_auth() {
        assert!(validate_input("CreateGraphqlApi", &json!({})).is_err());
        assert!(validate_input("CreateGraphqlApi", &json!({ "name": "x" })).is_err());
        assert!(validate_input(
            "CreateGraphqlApi",
            &json!({ "name": "x", "authenticationType": "BOGUS" })
        )
        .is_err());
        assert!(validate_input(
            "CreateGraphqlApi",
            &json!({ "name": "x", "authenticationType": "API_KEY" })
        )
        .is_ok());
    }

    #[test]
    fn create_data_source_enum() {
        assert!(
            validate_input("CreateDataSource", &json!({ "name": "d", "type": "NONE" })).is_ok()
        );
        assert!(
            validate_input("CreateDataSource", &json!({ "name": "d", "type": "WRONG" })).is_err()
        );
    }

    #[test]
    fn evaluate_code_requires_members() {
        assert!(validate_input("EvaluateCode", &json!({ "runtime": {}, "code": "x" })).is_err());
        assert!(validate_input(
            "EvaluateCode",
            &json!({ "runtime": {}, "code": "x", "context": "{}" })
        )
        .is_ok());
    }
}
