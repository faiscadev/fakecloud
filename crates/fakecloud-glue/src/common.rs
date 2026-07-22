//! Shared helpers for Glue handlers: error constructors, input parsing, and a
//! generic JSON-backed named-resource store used by the control-plane families
//! (crawlers, triggers, workflows, connections, etc.).
//!
//! Resources are persisted as the raw create/update input `serde_json::Value`,
//! then echoed back on read filtered to the set of fields valid for the
//! resource's output shape. This keeps responses real (the data the caller
//! actually sent round-trips) while never emitting a field the Smithy output
//! shape doesn't declare.

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Map, Value};

use fakecloud_core::service::AwsServiceError;

pub(crate) fn invalid_input(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidInputException", msg.into())
}

pub(crate) fn missing(field: &str) -> AwsServiceError {
    invalid_input(format!("Missing required field: {field}"))
}

pub(crate) fn entity_not_found(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "EntityNotFoundException",
        msg.into(),
    )
}

pub(crate) fn already_exists(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "AlreadyExistsException",
        msg.into(),
    )
}

/// Require a string member, erroring with `InvalidInputException` when absent
/// or not a string. Used so both negative variants (missing field) and
/// positive variants without the field resolve to a declared error.
pub(crate) fn req_str<'a>(body: &'a Value, field: &str) -> Result<&'a str, AwsServiceError> {
    body.get(field)
        .and_then(|v| v.as_str())
        .ok_or_else(|| missing(field))
}

/// Require a member to be present (any JSON type), erroring otherwise.
pub(crate) fn req_present<'a>(body: &'a Value, field: &str) -> Result<&'a Value, AwsServiceError> {
    match body.get(field) {
        Some(v) if !v.is_null() => Ok(v),
        _ => Err(missing(field)),
    }
}

/// Current epoch seconds as an f64 — the wire form Glue uses for `Timestamp`
/// members in awsJson1.1.
pub(crate) fn now_ts() -> f64 {
    Utc::now().timestamp() as f64
}

/// Settle a still-`RUNNING` async run/task to a terminal state when it is read,
/// so poll-until-done loops terminate instead of hanging forever. Glue's real
/// runs finish asynchronously; the emulator has no background worker, so
/// `StartJobRun` completes synchronously and every other `Start*Run` op must
/// likewise reach a terminal state. Doing it on read (rather than at start)
/// keeps the resource's initially persisted state `RUNNING`, so a `Stop*`/
/// `Cancel*` call issued before the first poll still finds a running resource.
///
/// `status_field` is the member carrying the lifecycle state (`Status` for most
/// runs, `State` for crawler/blueprint runs). When a transition happens and
/// `completed_field` is `Some`, that timestamp member is stamped with the
/// current time. Returns `true` when a transition occurred (so callers can
/// attach op-specific completion fields such as `ResultIds`).
pub(crate) fn settle_run_status(
    run: &mut Value,
    status_field: &str,
    terminal: &str,
    completed_field: Option<&str>,
) -> bool {
    let running = matches!(
        run.get(status_field).and_then(Value::as_str),
        Some("RUNNING") | Some("STARTING")
    );
    if !running {
        return false;
    }
    if let Some(obj) = run.as_object_mut() {
        obj.insert(status_field.to_string(), json!(terminal));
        if let Some(cf) = completed_field {
            obj.insert(cf.to_string(), json!(now_ts()));
        }
    }
    true
}

/// Copy the entries of `src` whose keys appear in `allowed` into a fresh
/// object. Lets handlers echo the real stored input while guaranteeing the
/// response carries only fields the Smithy output shape declares.
pub(crate) fn pick(src: &Value, allowed: &[&str]) -> Map<String, Value> {
    let mut out = Map::new();
    if let Some(obj) = src.as_object() {
        for key in allowed {
            if let Some(v) = obj.get(*key) {
                if !v.is_null() {
                    out.insert((*key).to_string(), v.clone());
                }
            }
        }
    }
    out
}

/// Build an object from `pick` plus extra key/value pairs (generated metadata
/// such as timestamps, status, ids).
pub(crate) fn entity(src: &Value, allowed: &[&str], extra: Vec<(&str, Value)>) -> Value {
    let mut obj = pick(src, allowed);
    for (k, v) in extra {
        obj.insert(k.to_string(), v);
    }
    Value::Object(obj)
}

/// Deterministic-ish fake ARN for a resource.
pub(crate) fn resource_arn(account: &str, region: &str, kind: &str, name: &str) -> String {
    format!("arn:aws:glue:{region}:{account}:{kind}/{name}")
}

/// Paginate a Glue list op using the request's `MaxResults`/`NextToken`.
///
/// Uses the offset-token scheme in [`fakecloud_core::pagination::paginate`]:
/// the token is the numeric offset of the next page. When `MaxResults` is
/// absent the full remaining list (from any incoming offset) is returned in a
/// single page with no continuation token — matching how AWS returns an
/// unbounded page when the caller omits a page size.
pub(crate) fn paginate_body(
    body: &Value,
    items: Vec<Value>,
) -> Result<(Vec<Value>, Option<String>), AwsServiceError> {
    let token = body.get("NextToken").and_then(|v| v.as_str());
    let max = body
        .get("MaxResults")
        .and_then(|v| v.as_u64())
        .map(|n| n as usize)
        .unwrap_or(usize::MAX);
    // A malformed NextToken is rejected with InvalidInputException rather than
    // silently restarting at page 0 (which can loop a client forever).
    fakecloud_core::pagination::paginate_checked(&items, token, max)
        .map_err(|_| invalid_input("Invalid value for NextToken."))
}

/// A small UUID-ish identifier (32 hex chars). Glue uses these for run ids,
/// transform ids, etc.
pub(crate) fn new_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// A hyphenated UUID (36 chars). Required where the Smithy shape enforces
/// `@length(min:36)`, e.g. `SchemaVersionIdString`.
pub(crate) fn new_uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Build an `ErrorDetail` Smithy shape (`{ErrorCode, ErrorMessage}`).
pub(crate) fn error_detail(code: &str, message: impl Into<String>) -> Value {
    json!({ "ErrorCode": code, "ErrorMessage": message.into() })
}

/// Validate the top-level input fields of a request against the generated
/// Smithy constraint table (`@length`, `@range`, enum membership). Returns
/// `InvalidInputException` on the first violation. AWS performs this validation
/// server-side before any business logic, so doing it here keeps fakecloud
/// behaviourally faithful rather than silently accepting malformed input.
pub(crate) fn validate_constraints(action: &str, body: &Value) -> Result<(), AwsServiceError> {
    for c in crate::constraints::constraints_for(action) {
        let Some(v) = body.get(c.field) else { continue };
        if v.is_null() {
            continue;
        }
        if let Some(s) = v.as_str() {
            let len = s.chars().count() as u64;
            if let Some(min) = c.len_min {
                if len < min {
                    return Err(invalid_input(format!(
                        "{} below minimum length {min}",
                        c.field
                    )));
                }
            }
            if let Some(max) = c.len_max {
                if len > max {
                    return Err(invalid_input(format!(
                        "{} exceeds maximum length {max}",
                        c.field
                    )));
                }
            }
            if !c.enum_values.is_empty() && !c.enum_values.contains(&s) {
                return Err(invalid_input(format!(
                    "{} is not a valid enum value",
                    c.field
                )));
            }
        } else if let Some(n) = v.as_i64() {
            if let Some(min) = c.range_min {
                if n < min {
                    return Err(invalid_input(format!("{} below minimum {min}", c.field)));
                }
            }
            if let Some(max) = c.range_max {
                if n > max {
                    return Err(invalid_input(format!("{} exceeds maximum {max}", c.field)));
                }
            }
        }
    }
    Ok(())
}
