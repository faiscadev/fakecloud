//! Model-derived input validation for AWS FIS operations.
//!
//! Rejects requests that violate the Smithy contract (missing required members,
//! out-of-set enum values) with `ValidationException` (the code every mutating
//! operation declares) so the conformance suite's negative variants land on a
//! declared error rather than a routing miss or a 500. Body member names are the
//! restJson1 member names (camelCase -- FIS declares no `jsonName` overrides).

use http::StatusCode;
use serde_json::Value;

use fakecloud_core::service::AwsServiceError;

pub const SAFETY_LEVER_STATUS_INPUT: &[&str] = &["disengaged", "engaged"];
pub const ACTIONS_MODE: &[&str] = &["skip-all", "run-all"];
pub const ACCOUNT_TARGETING: &[&str] = &["single-account", "multi-account"];
pub const EMPTY_TARGET_RESOLUTION_MODE: &[&str] = &["fail", "skip"];

fn validation(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", msg)
}

/// A present, non-null required member (empty string / empty collection are
/// treated as absent for a `min:1` member).
fn require<'a>(b: &'a Value, field: &str) -> Result<&'a Value, AwsServiceError> {
    match b.get(field) {
        Some(Value::Null) | None => Err(validation(&format!("{field} is a required field."))),
        Some(Value::String(s)) if s.is_empty() => {
            Err(validation(&format!("{field} must not be empty.")))
        }
        Some(v) => Ok(v),
    }
}

/// A present string member's length is within `[min, max]` (character count),
/// per the member's Smithy `@length` constraint.
fn str_len(b: &Value, field: &str, min: usize, max: usize) -> Result<(), AwsServiceError> {
    if let Some(s) = b.get(field).and_then(Value::as_str) {
        let len = s.chars().count();
        if len < min || len > max {
            return Err(validation(&format!(
                "{field} length {len} is outside the valid range [{min}, {max}]."
            )));
        }
    }
    Ok(())
}

/// An optional string member is one of `allowed` (case-sensitive), when present.
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

fn validate_experiment_options(b: &Value) -> Result<(), AwsServiceError> {
    if let Some(opts) = b.get("experimentOptions") {
        enum_opt(opts, "accountTargeting", ACCOUNT_TARGETING)?;
        enum_opt(
            opts,
            "emptyTargetResolutionMode",
            EMPTY_TARGET_RESOLUTION_MODE,
        )?;
    }
    Ok(())
}

/// Validate an operation's already-parsed JSON body against the model.
pub fn validate_input(action: &str, body: &Value) -> Result<(), AwsServiceError> {
    match action {
        "CreateExperimentTemplate" => {
            require(body, "clientToken")?;
            require(body, "description")?;
            require(body, "stopConditions")?;
            require(body, "actions")?;
            require(body, "roleArn")?;
            // Model `@length` constraints on the scalar string members.
            str_len(body, "clientToken", 1, 1024)?;
            str_len(body, "description", 0, 512)?;
            str_len(body, "roleArn", 20, 2048)?;
            // Each stop condition requires a `source`.
            if let Some(conds) = body.get("stopConditions").and_then(Value::as_array) {
                for c in conds {
                    require(c, "source")?;
                }
            }
            // Each action requires an `actionId`.
            if let Some(actions) = body.get("actions").and_then(Value::as_object) {
                for (_name, a) in actions {
                    require(a, "actionId")?;
                }
            }
            // Each target requires `resourceType` + `selectionMode`.
            if let Some(targets) = body.get("targets").and_then(Value::as_object) {
                for (_name, t) in targets {
                    require(t, "resourceType")?;
                    require(t, "selectionMode")?;
                }
            }
            validate_experiment_options(body)?;
        }
        "UpdateExperimentTemplate" => {
            str_len(body, "description", 0, 512)?;
            str_len(body, "roleArn", 20, 2048)?;
            validate_experiment_options(body)?;
        }
        "StartExperiment" => {
            require(body, "clientToken")?;
            require(body, "experimentTemplateId")?;
            if let Some(opts) = body.get("experimentOptions") {
                enum_opt(opts, "actionsMode", ACTIONS_MODE)?;
            }
        }
        "CreateTargetAccountConfiguration" | "UpdateTargetAccountConfiguration" => {
            // roleArn is required on create; on update it is optional. The
            // handler distinguishes; both share the accountId + templateId
            // labels validated at the route.
            if action == "CreateTargetAccountConfiguration" {
                require(body, "roleArn")?;
            }
            str_len(body, "roleArn", 20, 2048)?;
        }
        "UpdateSafetyLeverState" => {
            let state = require(body, "state")?;
            require(state, "status")?;
            require(state, "reason")?;
            enum_opt(state, "status", SAFETY_LEVER_STATUS_INPUT)?;
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

    fn full_create() -> Value {
        json!({
            "clientToken": "tok-1",
            "description": "template",
            "stopConditions": [{ "source": "none" }],
            "actions": { "a1": { "actionId": "aws:fis:wait" } },
            "roleArn": "arn:aws:iam::000000000000:role/fis"
        })
    }

    #[test]
    fn create_accepts_full_body() {
        assert!(validate_input("CreateExperimentTemplate", &full_create()).is_ok());
    }

    #[test]
    fn create_requires_role_arn() {
        let mut b = full_create();
        b.as_object_mut().unwrap().remove("roleArn");
        assert!(validate_input("CreateExperimentTemplate", &b).is_err());
    }

    #[test]
    fn create_requires_action_id_per_action() {
        let b = json!({
            "clientToken": "t", "description": "d",
            "stopConditions": [{ "source": "none" }],
            "actions": { "a1": { "description": "no id" } },
            "roleArn": "arn:aws:iam::000000000000:role/fis"
        });
        assert!(validate_input("CreateExperimentTemplate", &b).is_err());
    }

    #[test]
    fn start_requires_template_id() {
        assert!(validate_input("StartExperiment", &json!({ "clientToken": "t" })).is_err());
        assert!(validate_input(
            "StartExperiment",
            &json!({ "clientToken": "t", "experimentTemplateId": "EXT1" })
        )
        .is_ok());
    }

    #[test]
    fn safety_lever_status_enum_enforced() {
        assert!(validate_input(
            "UpdateSafetyLeverState",
            &json!({ "state": { "status": "nope", "reason": "r" } })
        )
        .is_err());
        assert!(validate_input(
            "UpdateSafetyLeverState",
            &json!({ "state": { "status": "engaged", "reason": "r" } })
        )
        .is_ok());
    }

    #[test]
    fn tag_requires_tags() {
        assert!(validate_input("TagResource", &json!({})).is_err());
    }
}
