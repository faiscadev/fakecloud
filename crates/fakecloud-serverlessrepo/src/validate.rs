//! Model-derived input validation for AWS Serverless Application Repository
//! operations.
//!
//! Rejects requests that violate the Smithy contract (missing required members)
//! with `BadRequestException` -- a code every SAR operation declares -- so the
//! conformance suite's negative variants land on a declared error rather than a
//! routing miss or a 500. Body member names are the restJson1 `@jsonName`
//! (camelCase) wire names; members bound to `@httpLabel` / `@httpQuery` are
//! validated in the handler.

use http::StatusCode;
use serde_json::Value;

use fakecloud_core::service::AwsServiceError;

fn bad_request(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "BadRequestException", msg)
}

/// A present, non-null, non-empty required member.
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

/// A required list member that must be present and non-empty.
fn require_list<'a>(b: &'a Value, field: &str) -> Result<&'a Vec<Value>, AwsServiceError> {
    match require(b, field)?.as_array() {
        Some(a) => Ok(a),
        None => Err(bad_request(&format!("'{field}' must be a list."))),
    }
}

/// Validate an operation's already-parsed JSON body against the model.
pub fn validate_input(action: &str, body: &Value) -> Result<(), AwsServiceError> {
    match action {
        "CreateApplication" => {
            require(body, "author")?;
            require(body, "description")?;
            require(body, "name")?;
        }
        "CreateCloudFormationChangeSet" => {
            require(body, "stackName")?;
        }
        "PutApplicationPolicy" => {
            let statements = require_list(body, "statements")?;
            for st in statements {
                require_list(st, "actions")?;
                require_list(st, "principals")?;
            }
        }
        "UnshareApplication" => {
            require(body, "organizationId")?;
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
    fn create_application_requires_core_fields() {
        assert!(validate_input("CreateApplication", &json!({})).is_err());
        assert!(validate_input(
            "CreateApplication",
            &json!({ "author": "a", "description": "d" })
        )
        .is_err());
        assert!(validate_input(
            "CreateApplication",
            &json!({ "author": "a", "description": "d", "name": "n" })
        )
        .is_ok());
    }

    #[test]
    fn put_policy_requires_statement_fields() {
        assert!(validate_input("PutApplicationPolicy", &json!({})).is_err());
        assert!(validate_input(
            "PutApplicationPolicy",
            &json!({ "statements": [{ "principals": ["*"] }] })
        )
        .is_err());
        assert!(validate_input(
            "PutApplicationPolicy",
            &json!({ "statements": [{ "actions": ["Deploy"], "principals": ["*"] }] })
        )
        .is_ok());
    }

    #[test]
    fn change_set_requires_stack_name() {
        assert!(validate_input("CreateCloudFormationChangeSet", &json!({})).is_err());
        assert!(validate_input(
            "CreateCloudFormationChangeSet",
            &json!({ "stackName": "s" })
        )
        .is_ok());
    }

    #[test]
    fn unshare_requires_org() {
        assert!(validate_input("UnshareApplication", &json!({})).is_err());
        assert!(validate_input("UnshareApplication", &json!({ "organizationId": "o-1" })).is_ok());
    }
}
