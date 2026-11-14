//! Validation of `CheckpointDurableExecution` updates.
//!
//! The service checks a checkpoint in four passes, each of which rejects the
//! whole request before any update is applied:
//!
//! 1. Deserialization: a member of the wrong JSON type answers
//!    `SerializationException`, naming only the first such member. Members
//!    are read in alphabetical order.
//! 2. The model: `ValidationException` listing every constraint violation
//!    (at most ten), in a fixed member order, with each update named by its
//!    1-based position.
//! 3. The request as a whole: one EXECUTION update, last; parents that are
//!    CONTEXT operations; no operation updated twice.
//! 4. Each update against the operation it touches: unchanged metadata,
//!    payload sizes, the actions its type allows, its status transition.
//!
//! The messages were recorded from `CheckpointDurableExecution` in
//! us-east-1 (see `tests/fixtures/durable`).

use std::collections::HashMap;
use std::sync::LazyLock;

use http::StatusCode;
use regex::Regex;
use serde_json::Value;

use fakecloud_core::service::AwsServiceError;

use super::model::{DurableExecution, OpStatus, OpType};

pub(crate) fn invalid_parameter(message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidParameterValueException",
        message,
    )
}

fn serialization_error(message: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "SerializationException", message)
}

// --- pass 1: deserialization ----------------------------------------------

enum Schema {
    Str,
    Int,
    Bool,
    List(&'static Schema),
    Struct(&'static [(&'static str, Schema)]),
}

static ERROR_SCHEMA: [(&str, Schema); 4] = [
    ("ErrorData", Schema::Str),
    ("ErrorMessage", Schema::Str),
    ("ErrorType", Schema::Str),
    ("StackTrace", Schema::List(&Schema::Str)),
];

/// `OperationUpdate`, members in alphabetical order (the order the service
/// reads them in).
static UPDATE_SCHEMA: [(&str, Schema); 13] = [
    ("Action", Schema::Str),
    (
        "CallbackOptions",
        Schema::Struct(&[
            ("HeartbeatTimeoutSeconds", Schema::Int),
            ("TimeoutSeconds", Schema::Int),
        ]),
    ),
    (
        "ChainedInvokeOptions",
        Schema::Struct(&[
            ("FunctionName", Schema::Str),
            ("TenantId", Schema::Str),
            ("XAmznTraceId", Schema::Str),
        ]),
    ),
    (
        "ContextOptions",
        Schema::Struct(&[("ReplayChildren", Schema::Bool)]),
    ),
    ("Error", Schema::Struct(&ERROR_SCHEMA)),
    ("Id", Schema::Str),
    ("Name", Schema::Str),
    ("ParentId", Schema::Str),
    ("Payload", Schema::Str),
    (
        "StepOptions",
        Schema::Struct(&[("NextAttemptDelaySeconds", Schema::Int)]),
    ),
    ("SubType", Schema::Str),
    ("Type", Schema::Str),
    (
        "WaitOptions",
        Schema::Struct(&[("WaitSeconds", Schema::Int)]),
    ),
];

const OBJECT_NOT_EXPECTED: &str = "Start of structure or map found where not expected.";
const ARRAY_NOT_EXPECTED: &str = "Start of list found where not expected";

fn token_name(v: &Value) -> &'static str {
    match v {
        Value::Bool(true) => "TRUE_VALUE",
        Value::Bool(false) => "FALSE_VALUE",
        Value::Number(_) => "NUMBER_VALUE",
        _ => "STRING_VALUE",
    }
}

fn first_error(v: &Value, schema: &Schema) -> Option<String> {
    if v.is_null() {
        return None;
    }
    match schema {
        Schema::Str | Schema::Int | Schema::Bool => {
            if v.is_array() {
                return Some(ARRAY_NOT_EXPECTED.to_string());
            }
            if v.is_object() {
                return Some(OBJECT_NOT_EXPECTED.to_string());
            }
            match schema {
                Schema::Str if !v.is_string() => Some(format!(
                    "{} can not be converted to a String",
                    token_name(v)
                )),
                Schema::Int if !v.is_number() => Some(format!(
                    "{} can not be converted to an Integer",
                    token_name(v)
                )),
                // The service also reads a string as a Boolean.
                Schema::Bool if v.is_number() => {
                    Some("NUMBER_VALUE can not be converted to a Boolean".to_string())
                }
                _ => None,
            }
        }
        Schema::List(inner) => match v {
            Value::Array(items) => items.iter().find_map(|i| first_error(i, inner)),
            Value::Object(_) => Some(OBJECT_NOT_EXPECTED.to_string()),
            _ => Some("Expected list or null".to_string()),
        },
        Schema::Struct(members) => match v {
            // A list where a structure is expected is read as an empty one.
            Value::Array(_) => None,
            Value::Object(m) => members
                .iter()
                .find_map(|(name, s)| m.get(*name).and_then(|mv| first_error(mv, s))),
            _ => Some("Expected null".to_string()),
        },
    }
}

fn validate_serialization(updates: &[Value]) -> Result<(), AwsServiceError> {
    for u in updates {
        if let Some(msg) = first_error(u, &Schema::Struct(&UPDATE_SCHEMA)) {
            return Err(serialization_error(&msg));
        }
    }
    Ok(())
}

// --- pass 2: the model ------------------------------------------------------

const MAX_DURATION_SECONDS: i64 = 31_622_400;
const MAX_CALLBACK_TIMEOUT_SECONDS: i64 = 99_999_999;
const MAX_REPORTED_ERRORS: usize = 10;

static ID_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^(?:[a-zA-Z0-9-_]+)$").unwrap());
static NAME_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^(?:[\x20-\x7E]+)$").unwrap());
static TENANT_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(?:[a-zA-Z0-9\._:\/=+\-@ ]+)$").unwrap());
static FUNCTION_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(?:(arn:(aws[a-zA-Z-]*)?:lambda:)?((eusc-)?[a-z]{2}((-gov)|(-iso([a-z]?)))?-[a-z]+-\d{1}:)?(\d{12}:)?(function:)?([a-zA-Z0-9-_\.]+)(:(\$LATEST(\.PUBLISHED)?|[a-zA-Z0-9-_]+))?)$").unwrap()
});

const ID_PATTERN: &str = "[a-zA-Z0-9-_]+";
const NAME_PATTERN: &str = "[\\x20-\\x7E]+";
const TENANT_PATTERN: &str = "[a-zA-Z0-9\\._:\\/=+\\-@ ]+";
const FUNCTION_PATTERN: &str = "(arn:(aws[a-zA-Z-]*)?:lambda:)?((eusc-)?[a-z]{2}((-gov)|(-iso([a-z]?)))?-[a-z]+-\\d{1}:)?(\\d{12}:)?(function:)?([a-zA-Z0-9-_\\.]+)(:(\\$LATEST(\\.PUBLISHED)?|[a-zA-Z0-9-_]+))?";

/// The `OperationType` values the service's model accepts, in the order its
/// enum violation message lists them.
const TYPE_ENUM_MESSAGE: &str =
    "[DISTRIBUTED_MAP, CALLBACK, STEP, CONTEXT, WAIT, CHAINED_INVOKE, EXECUTION]";

struct StrConstraint {
    required: bool,
    min: usize,
    max: usize,
    pattern: Option<(&'static LazyLock<Regex>, &'static str)>,
}

fn str_errors(v: &str, path: &str, c: &StrConstraint, out: &mut Vec<String>) {
    let at = format!("Value '{v}' at '{path}' failed to satisfy constraint:");
    let len = v.chars().count();
    if len < c.min {
        out.push(format!(
            "{at} Member must have length greater than or equal to {}",
            c.min
        ));
    }
    if len > c.max {
        out.push(format!(
            "{at} Member must have length less than or equal to {}",
            c.max
        ));
    }
    // The service checks the pattern even when the length check fails.
    if let Some((re, src)) = c.pattern {
        if !re.is_match(v) {
            out.push(format!(
                "{at} Member must satisfy regular expression pattern: {src}"
            ));
        }
    }
}

/// The integer the service reads for a JSON number: fraction dropped,
/// clamped to the 32-bit range.
pub(crate) fn service_integer(v: &Value) -> Option<i64> {
    let n = v.as_f64()?;
    if !n.is_finite() {
        return None;
    }
    Some(n.trunc().clamp(i32::MIN as f64, i32::MAX as f64) as i64)
}

fn num_errors(v: &Value, path: &str, min: i64, max: i64, out: &mut Vec<String>) {
    let Some(n) = service_integer(v) else { return };
    let at = format!("Value '{n}' at '{path}' failed to satisfy constraint:");
    if n < min {
        out.push(format!(
            "{at} Member must have value greater than or equal to {min}"
        ));
    } else if n > max {
        out.push(format!(
            "{at} Member must have value less than or equal to {max}"
        ));
    }
}

fn check_str(
    container: Option<&Value>,
    key: &str,
    path: &str,
    c: &StrConstraint,
    out: &mut Vec<String>,
) {
    let Some(container) = container.filter(|v| v.is_object()) else {
        return;
    };
    match container.get(key).filter(|v| !v.is_null()) {
        None => {
            if c.required {
                out.push(format!(
                    "Value null at '{path}' failed to satisfy constraint: Member must not be null"
                ));
            }
        }
        Some(Value::String(s)) => str_errors(s, path, c, out),
        Some(_) => {}
    }
}

fn check_num(
    container: Option<&Value>,
    key: &str,
    path: &str,
    min: i64,
    max: i64,
    out: &mut Vec<String>,
) {
    let Some(container) = container.filter(|v| v.is_object()) else {
        return;
    };
    if let Some(v) = container.get(key).filter(|v| v.is_number()) {
        num_errors(v, path, min, max, out);
    }
}

const OPTIONAL_ID: StrConstraint = StrConstraint {
    required: false,
    min: 1,
    max: 64,
    pattern: None,
};

fn validate_model(updates: &[Value]) -> Result<(), AwsServiceError> {
    let mut errors = Vec::new();
    for (i, u) in updates.iter().enumerate() {
        let p = |m: &str| format!("updates.{}.member.{m}", i + 1);
        let top = Some(u);
        let id = StrConstraint {
            pattern: Some((&ID_RE, ID_PATTERN)),
            ..OPTIONAL_ID
        };
        check_str(
            u.get("ChainedInvokeOptions"),
            "FunctionName",
            &p("chainedInvokeOptions.functionName"),
            &StrConstraint {
                required: true,
                min: 1,
                max: 256,
                pattern: Some((&FUNCTION_RE, FUNCTION_PATTERN)),
            },
            &mut errors,
        );
        check_str(
            u.get("ChainedInvokeOptions"),
            "TenantId",
            &p("chainedInvokeOptions.tenantId"),
            &StrConstraint {
                required: false,
                min: 1,
                max: 256,
                pattern: Some((&TENANT_RE, TENANT_PATTERN)),
            },
            &mut errors,
        );
        check_str(
            u.get("ChainedInvokeOptions"),
            "XAmznTraceId",
            &p("chainedInvokeOptions.xAmznTraceId"),
            &StrConstraint {
                required: false,
                min: 0,
                max: 8192,
                pattern: None,
            },
            &mut errors,
        );
        check_num(
            u.get("CallbackOptions"),
            "TimeoutSeconds",
            &p("callbackOptions.timeoutSeconds"),
            0,
            MAX_CALLBACK_TIMEOUT_SECONDS,
            &mut errors,
        );
        check_num(
            u.get("CallbackOptions"),
            "HeartbeatTimeoutSeconds",
            &p("callbackOptions.heartbeatTimeoutSeconds"),
            0,
            MAX_CALLBACK_TIMEOUT_SECONDS,
            &mut errors,
        );
        match u.get("Type").filter(|v| !v.is_null()) {
            None if u.is_object() => errors.push(format!(
                "Value null at '{}' failed to satisfy constraint: Member must not be null",
                p("type")
            )),
            Some(Value::String(t)) if OpType::parse(t).is_none() && t != "DISTRIBUTED_MAP" => {
                errors.push(format!(
                    "Value '{t}' at '{}' failed to satisfy constraint: Member must satisfy enum value set: {TYPE_ENUM_MESSAGE}",
                    p("type")
                ))
            }
            _ => {}
        }
        check_str(top, "ParentId", &p("parentId"), &id, &mut errors);
        check_num(
            u.get("WaitOptions"),
            "WaitSeconds",
            &p("waitOptions.waitSeconds"),
            1,
            MAX_DURATION_SECONDS,
            &mut errors,
        );
        check_num(
            u.get("StepOptions"),
            "NextAttemptDelaySeconds",
            &p("stepOptions.nextAttemptDelaySeconds"),
            1,
            MAX_DURATION_SECONDS,
            &mut errors,
        );
        check_str(
            top,
            "Name",
            &p("name"),
            &StrConstraint {
                required: false,
                min: 1,
                max: 256,
                pattern: Some((&NAME_RE, NAME_PATTERN)),
            },
            &mut errors,
        );
        check_str(
            top,
            "Action",
            &p("action"),
            &StrConstraint {
                required: true,
                min: 0,
                max: usize::MAX,
                pattern: None,
            },
            &mut errors,
        );
        check_str(
            top,
            "SubType",
            &p("subType"),
            &StrConstraint {
                required: false,
                min: 1,
                max: 32,
                pattern: Some((&ID_RE, ID_PATTERN)),
            },
            &mut errors,
        );
        check_str(
            top,
            "Id",
            &p("id"),
            &StrConstraint {
                required: true,
                ..id
            },
            &mut errors,
        );
    }
    if errors.is_empty() {
        return Ok(());
    }
    errors.truncate(MAX_REPORTED_ERRORS);
    let count = if errors.len() == 1 {
        "1 validation error detected".to_string()
    } else {
        format!("{} validation errors detected", errors.len())
    };
    Err(AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ValidationException",
        format!("{count}: {}", errors.join("; ")),
    ))
}

// --- passes 3 and 4 ---------------------------------------------------------

/// The error object and STEP/CONTEXT output limit.
const MAX_CHECKPOINT_PAYLOAD_BYTES: usize = 262_144;
const MAX_CHAINED_INVOKE_INPUT_BYTES: usize = 1_048_576;
const MAX_EXECUTION_OUTPUT_BYTES: usize = 6_291_456;

const INCONSISTENT: &str =
    " value in OperationUpdate is inconsistent with existing operation. Check for non-determinism.";

fn str_field<'a>(u: &'a Value, key: &str) -> Option<&'a str> {
    u.get(key).and_then(Value::as_str)
}

fn present(u: &Value, key: &str) -> bool {
    u.get(key).is_some_and(|v| !v.is_null())
}

/// Validate the updates of one checkpoint against the execution.
pub(crate) fn validate_updates(
    updates: &[Value],
    exec: &DurableExecution,
) -> Result<(), AwsServiceError> {
    if updates.is_empty() {
        return Ok(());
    }
    validate_serialization(updates)?;
    validate_model(updates)?;

    let executions = updates
        .iter()
        .filter(|u| str_field(u, "Type") == Some("EXECUTION"))
        .count();
    if executions > 1 {
        return Err(invalid_parameter(
            "Cannot checkpoint multiple EXECUTION updates.",
        ));
    }
    if executions == 1 && str_field(&updates[updates.len() - 1], "Type") != Some("EXECUTION") {
        return Err(invalid_parameter(
            "EXECUTION checkpoint must be the last update.",
        ));
    }

    // Parents and duplicates.
    let mut started: HashMap<&str, &Value> = HashMap::new();
    let mut last_seen: HashMap<&str, &Value> = HashMap::new();
    for u in updates {
        let id = str_field(u, "Id").unwrap_or_default();
        if let Some(prev) = last_seen.get(id) {
            let prev_type = str_field(prev, "Type");
            let allowed = matches!(prev_type, Some("STEP") | Some("CONTEXT"))
                && str_field(prev, "Action") == Some("START")
                && str_field(u, "Action") != Some("START");
            if !allowed {
                return Err(invalid_parameter(
                    "Cannot update the same operation twice in a single request.",
                ));
            }
        }
        if let Some(parent) = str_field(u, "ParentId") {
            let parent_type = match started.get(parent) {
                Some(p) => str_field(p, "Type").and_then(OpType::parse),
                None => exec.op(parent).map(|o| o.op_type),
            };
            if parent_type != Some(OpType::Context) {
                return Err(invalid_parameter("Invalid parent operation id."));
            }
        }
        if str_field(u, "Action") == Some("START") {
            started.insert(id, u);
        }
        last_seen.insert(id, u);
    }

    // Each update against the operation it touches. Earlier updates of the
    // same batch count: a START followed by SUCCEED closes what it opened.
    let mut batch_status: HashMap<&str, OpStatus> = HashMap::new();
    for u in updates {
        let id = str_field(u, "Id").unwrap_or_default();
        let existing = exec.op(id);
        validate_metadata(u, existing)?;
        validate_payload_sizes(u)?;
        let op_type = match str_field(u, "Type").and_then(OpType::parse) {
            Some(t) => t,
            None => return Err(invalid_parameter("Invalid operation type.")),
        };
        let action = str_field(u, "Action").unwrap_or_default();
        if !allowed_actions(op_type).contains(&action) {
            return Err(invalid_parameter(
                "Invalid action for the given operation type.",
            ));
        }
        let status = batch_status
            .get(id)
            .copied()
            .or_else(|| existing.map(|o| o.status));
        validate_transition(op_type, action, u, status)?;
        let next = match (op_type, action) {
            (_, "START") => OpStatus::Started,
            (_, "SUCCEED") => OpStatus::Succeeded,
            (_, "FAIL") => OpStatus::Failed,
            (_, "RETRY") => OpStatus::Pending,
            _ => OpStatus::Cancelled,
        };
        batch_status.insert(id, next);
    }
    Ok(())
}

fn validate_metadata(
    u: &Value,
    existing: Option<&super::model::Operation>,
) -> Result<(), AwsServiceError> {
    let Some(op) = existing else { return Ok(()) };
    let checks: [(&str, Option<&str>, Option<&str>); 4] = [
        ("Type", str_field(u, "Type"), Some(op.op_type.as_str())),
        ("SubType", str_field(u, "SubType"), op.sub_type.as_deref()),
        ("Name", str_field(u, "Name"), op.name.as_deref()),
        (
            "ParentId",
            str_field(u, "ParentId"),
            op.parent_id.as_deref(),
        ),
    ];
    for (field, update_value, op_value) in checks {
        if let Some(v) = update_value {
            if Some(v) != op_value {
                return Err(invalid_parameter(format!("{field}{INCONSISTENT}")));
            }
        }
    }
    Ok(())
}

fn validate_payload_sizes(u: &Value) -> Result<(), AwsServiceError> {
    if present(u, "Error") && u.to_string().len() > MAX_CHECKPOINT_PAYLOAD_BYTES {
        return Err(invalid_parameter(format!(
            "Error object size must be less than or equal to {MAX_CHECKPOINT_PAYLOAD_BYTES} bytes."
        )));
    }
    let limit = match str_field(u, "Type") {
        Some("STEP") => Some((MAX_CHECKPOINT_PAYLOAD_BYTES, "STEP output payload")),
        Some("CONTEXT") => Some((MAX_CHECKPOINT_PAYLOAD_BYTES, "CONTEXT output payload")),
        Some("CHAINED_INVOKE") => Some((
            MAX_CHAINED_INVOKE_INPUT_BYTES,
            "CHAINED_INVOKE input payload",
        )),
        Some("EXECUTION") => Some((MAX_EXECUTION_OUTPUT_BYTES, "Execution output payload")),
        _ => None,
    };
    if let (Some((max, what)), Some(p)) = (limit, str_field(u, "Payload")) {
        if p.len() > max {
            return Err(invalid_parameter(format!(
                "{what} size must be less than or equal to {max} bytes."
            )));
        }
    }
    Ok(())
}

fn allowed_actions(t: OpType) -> &'static [&'static str] {
    match t {
        OpType::Step => &["START", "FAIL", "RETRY", "SUCCEED"],
        OpType::Context => &["START", "FAIL", "SUCCEED"],
        OpType::Wait => &["START", "CANCEL"],
        // The service completes callbacks and chained invokes itself.
        OpType::Callback | OpType::ChainedInvoke => &["START"],
        OpType::Execution => &["SUCCEED", "FAIL"],
    }
}

fn validate_transition(
    t: OpType,
    action: &str,
    u: &Value,
    status: Option<OpStatus>,
) -> Result<(), AwsServiceError> {
    let fail_with_payload = action == "FAIL" && present(u, "Payload");
    let succeed_with_error = action == "SUCCEED" && present(u, "Error");
    match t {
        OpType::Callback => {
            if status.is_some() {
                return Err(invalid_parameter(
                    "Cannot start a CALLBACK operation that already exists.",
                ));
            }
        }
        OpType::ChainedInvoke => {
            if status.is_some() {
                return Err(invalid_parameter(
                    "Cannot start a CHAINED_INVOKE operation that already exists.",
                ));
            }
            if !present(u, "ChainedInvokeOptions") {
                return Err(invalid_parameter(
                    "Update for CHAINED_INVOKE operation requires ChainedInvokeOptions.",
                ));
            }
        }
        OpType::Execution => {
            if succeed_with_error {
                return Err(invalid_parameter(
                    "Cannot provide an Error for SUCCEED action.",
                ));
            }
            if fail_with_payload {
                return Err(invalid_parameter(
                    "Cannot provide a Payload for FAIL action.",
                ));
            }
        }
        OpType::Context => match action {
            "START" => {
                if status.is_some() {
                    return Err(invalid_parameter(
                        "Cannot start a CONTEXT operation that already exists.",
                    ));
                }
            }
            _ => {
                if status.is_some_and(|s| s != OpStatus::Started) {
                    return Err(invalid_parameter("Invalid current CONTEXT state to close."));
                }
                if fail_with_payload {
                    return Err(invalid_parameter(
                        "Cannot provide a Payload for FAIL action.",
                    ));
                }
                if succeed_with_error {
                    return Err(invalid_parameter(
                        "Cannot provide an Error for SUCCEED action.",
                    ));
                }
            }
        },
        OpType::Step => match action {
            "START" => {
                if status.is_some_and(|s| s != OpStatus::Ready) {
                    return Err(invalid_parameter("Invalid current STEP state to start."));
                }
            }
            "RETRY" => {
                if status.is_some_and(|s| !matches!(s, OpStatus::Started | OpStatus::Ready)) {
                    return Err(invalid_parameter("Invalid current STEP state to retry."));
                }
                if !present(u, "StepOptions") {
                    return Err(invalid_parameter(
                        "RETRY action for STEP operation requires StepOptions.",
                    ));
                }
                if present(u, "Error") && present(u, "Payload") {
                    return Err(invalid_parameter(
                        "Cannot provide both an Error and Payload for RETRY action.",
                    ));
                }
            }
            _ => {
                if status.is_some_and(|s| !matches!(s, OpStatus::Started | OpStatus::Ready)) {
                    return Err(invalid_parameter("Invalid current STEP state to close."));
                }
                if fail_with_payload {
                    return Err(invalid_parameter(
                        "Cannot provide a Payload for FAIL action.",
                    ));
                }
                if succeed_with_error {
                    return Err(invalid_parameter(
                        "Cannot provide an Error for SUCCEED action.",
                    ));
                }
            }
        },
        OpType::Wait => match action {
            "START" => {
                if status.is_some() {
                    return Err(invalid_parameter(
                        "Cannot start a WAIT operation that already exists.",
                    ));
                }
                if !present(u, "WaitOptions") {
                    return Err(invalid_parameter(
                        "Update for WAIT operation requires WaitOptions.",
                    ));
                }
            }
            _ => {
                if status != Some(OpStatus::Started) {
                    return Err(invalid_parameter(
                        "Cannot cancel a WAIT operation that does not exist or has already completed.",
                    ));
                }
            }
        },
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable::model::{OpStatus, OpType, Operation};
    use chrono::Utc;
    use serde_json::json;

    fn exec_with(ops: Vec<Operation>) -> DurableExecution {
        DurableExecution {
            arn: "arn".into(),
            id: "x".into(),
            name: "n".into(),
            function_name: "f".into(),
            function_arn: "f".into(),
            version: "$LATEST".into(),
            status: "RUNNING".into(),
            input_payload: "{}".into(),
            result: None,
            error: None,
            start_timestamp: Utc::now(),
            end_timestamp: None,
            execution_timeout: 60,
            retention_days: 1,
            kms_key_arn: None,
            trace_id: String::new(),
            tenant_id: None,
            operations: ops,
            events: vec![],
            invocation: None,
            dirty: vec![],
            last_updated_ids: vec![],
            wake: false,
            retry: None,
            parent: None,
        }
    }

    fn msg(r: Result<(), AwsServiceError>) -> (String, String) {
        let e = r.expect_err("expected a rejection");
        (e.code().to_string(), e.message())
    }

    #[test]
    fn recorded_messages_for_rejected_updates() {
        let mut step = Operation::new("s1", OpType::Step, OpStatus::Started, Utc::now());
        step.name = Some("probe-step".into());
        step.sub_type = Some("Step".into());
        let wait = Operation::new("w1", OpType::Wait, OpStatus::Started, Utc::now());
        let e = exec_with(vec![step, wait]);
        let cases: Vec<(Value, &str, &str)> = vec![
            (json!([{"Id":"s1","Type":"STEP","Action":"SUCCEED","Error":{"ErrorMessage":"x"}}]), "InvalidParameterValueException", "Cannot provide an Error for SUCCEED action."),
            (json!([{"Id":"s2","Type":"STEP","Action":"RETRY"}]), "InvalidParameterValueException", "RETRY action for STEP operation requires StepOptions."),
            (json!([{"Id":"w0","Type":"WAIT","Action":"START"}]), "InvalidParameterValueException", "Update for WAIT operation requires WaitOptions."),
            (json!([{"Id":"w0","Type":"WAIT","Action":"START","WaitOptions":{"WaitSeconds":0}}]), "ValidationException", "1 validation error detected: Value '0' at 'updates.1.member.waitOptions.waitSeconds' failed to satisfy constraint: Member must have value greater than or equal to 1"),
            (json!([{"Id":"w1","Type":"WAIT","Action":"START","WaitOptions":{"WaitSeconds":60}}]), "InvalidParameterValueException", "Cannot start a WAIT operation that already exists."),
            (json!([{"Id":"c1","Type":"CALLBACK","Action":"SUCCEED"}]), "InvalidParameterValueException", "Invalid action for the given operation type."),
            (json!([{"Id":"x1","ParentId":"nope","Type":"STEP","Action":"START"}]), "InvalidParameterValueException", "Invalid parent operation id."),
            (json!([{"Id":"x2","ParentId":"s1","Type":"STEP","Action":"START"}]), "InvalidParameterValueException", "Invalid parent operation id."),
            (json!([{"Id":"x3","Type":"WAIT","Action":"START","WaitOptions":{"WaitSeconds":5}},{"Id":"x3","Type":"WAIT","Action":"CANCEL"}]), "InvalidParameterValueException", "Cannot update the same operation twice in a single request."),
            (json!([{"Id":"s1","Type":"STEP","Action":"START","Name":"other"}]), "InvalidParameterValueException", "Name value in OperationUpdate is inconsistent with existing operation. Check for non-determinism."),
            (json!([{"Id":"e1","Type":"EXECUTION","Action":"SUCCEED"},{"Id":"x4","Type":"STEP","Action":"START"}]), "InvalidParameterValueException", "EXECUTION checkpoint must be the last update."),
            (json!([{"Id":"x5","Type":"BOGUS","Action":"START"}]), "ValidationException", "1 validation error detected: Value 'BOGUS' at 'updates.1.member.type' failed to satisfy constraint: Member must satisfy enum value set: [DISTRIBUTED_MAP, CALLBACK, STEP, CONTEXT, WAIT, CHAINED_INVOKE, EXECUTION]"),
        ];
        for (updates, code, message) in cases {
            let (c, m) = msg(validate_updates(updates.as_array().unwrap(), &e));
            assert_eq!((c.as_str(), m.as_str()), (code, message), "{updates}");
        }
    }

    #[test]
    fn accepted_batches() {
        let e = exec_with(vec![Operation::new(
            "k1",
            OpType::Context,
            OpStatus::Started,
            Utc::now(),
        )]);
        for updates in [
            json!([{"Id":"s","Type":"STEP","Action":"START"},{"Id":"s","Type":"STEP","Action":"SUCCEED","Payload":"1"}]),
            json!([{"Id":"k2","Type":"CONTEXT","Action":"START"},{"Id":"k2s","ParentId":"k2","Type":"STEP","Action":"START"}]),
            json!([{"Id":"in","ParentId":"k1","Type":"WAIT","Action":"START","WaitOptions":{"WaitSeconds":1}}]),
            json!([{"Id":"c","Type":"CALLBACK","Action":"START","CallbackOptions":{"TimeoutSeconds":0}}]),
            json!([{"Id":"e","Type":"EXECUTION","Action":"SUCCEED","Payload":"\"big\""}]),
        ] {
            validate_updates(updates.as_array().unwrap(), &e).unwrap_or_else(|err| {
                panic!("{updates} rejected: {err:?}");
            });
        }
    }

    #[test]
    fn wrong_json_types_are_serialization_errors() {
        let e = exec_with(vec![]);
        let (c, m) = msg(validate_updates(
            &[json!({"Id":"s","Type":"WAIT","Action":"START","WaitOptions":{"WaitSeconds":"5"}})],
            &e,
        ));
        assert_eq!(c, "SerializationException");
        assert_eq!(m, "STRING_VALUE can not be converted to an Integer");
        let (_, m) = msg(validate_updates(
            &[json!({"Id":["x"],"Type":"STEP","Action":"START"})],
            &e,
        ));
        assert_eq!(m, "Start of list found where not expected");
    }

    #[test]
    fn model_errors_list_every_violation_in_service_order() {
        let e = exec_with(vec![]);
        let (_, m) = msg(validate_updates(
            &[json!({"Id":"bad id","Type":"STEP","Action":"START","Name":""})],
            &e,
        ));
        assert_eq!(
            m,
            "3 validation errors detected: Value '' at 'updates.1.member.name' failed to satisfy constraint: Member must have length greater than or equal to 1; Value '' at 'updates.1.member.name' failed to satisfy constraint: Member must satisfy regular expression pattern: [\\x20-\\x7E]+; Value 'bad id' at 'updates.1.member.id' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9-_]+"
        );
    }
}
