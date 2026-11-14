//! The durable execution state machine.
//!
//! Everything here is synchronous and takes the current time as an
//! argument, so the transitions are unit-testable without a container
//! runtime or a clock. The async driver in [`super::runner`] calls in here
//! under the Lambda state lock.
//!
//! Behavior follows what AWS was recorded doing (`tests/fixtures/durable`):
//! the history event shapes, the operation statuses each update produces,
//! when the function is re-invoked, how failed invocations are retried, and
//! how timeouts surface.

use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Map, Value};

use fakecloud_core::service::AwsServiceError;

use super::model::{
    error_wrapper, new_callback_id, new_checkpoint_token, payload_wrapper, ts, ActiveInvocation,
    CallbackState, ChainedInvokeState, DeferredOutcome, DurableExecution, InvocationRetry,
    OpStatus, OpType, Operation, ParentLink, RetryKind, TimeFormat, FAILED, RUNNING, STOPPED,
    SUCCEEDED, TIMED_OUT,
};
use super::validate::{invalid_parameter, service_integer, validate_updates};

/// The invocation payload is a Lambda request; past this size the
/// operations continue on `GetDurableExecutionState` pages instead.
const MAX_INITIAL_STATE_BYTES: usize = 5 * 1024 * 1024;
/// `MaxItems` default and ceiling of the paginated reads.
pub(crate) const DEFAULT_PAGE: usize = 100;
pub(crate) const MAX_PAGE: usize = 1000;

pub(crate) fn invalid_token() -> AwsServiceError {
    invalid_parameter("Invalid checkpoint token")
}

/// What creating an execution needs.
pub(crate) struct NewExecution {
    pub arn: String,
    pub id: String,
    pub name: String,
    pub function_name: String,
    pub function_arn: String,
    pub version: String,
    pub input: String,
    pub execution_timeout: i64,
    pub retention_days: i64,
    pub kms_key_arn: Option<String>,
    pub trace_id: String,
    pub tenant_id: Option<String>,
    pub parent: Option<ParentLink>,
}

/// The identity a history event carries for its operation.
struct Subject {
    id: String,
    name: Option<String>,
    sub_type: Option<String>,
    parent_id: Option<String>,
}

impl Subject {
    fn of(op: &Operation) -> Subject {
        Subject {
            id: op.id.clone(),
            name: op.name.clone(),
            sub_type: op.sub_type.clone(),
            parent_id: op.parent_id.clone(),
        }
    }
}

fn push_event(
    exec: &mut DurableExecution,
    at: DateTime<Utc>,
    event_type: &str,
    subject: Option<&Subject>,
    details: Value,
) {
    let mut m = Map::new();
    m.insert("EventType".into(), json!(event_type));
    m.insert("EventId".into(), json!(exec.events.len() + 1));
    m.insert("EventTimestamp".into(), ts(at, TimeFormat::Seconds));
    if let Some(s) = subject {
        m.insert("Id".into(), json!(s.id));
        if let Some(n) = &s.name {
            m.insert("Name".into(), json!(n));
        }
        if let Some(t) = &s.sub_type {
            m.insert("SubType".into(), json!(t));
        }
        if let Some(p) = &s.parent_id {
            m.insert("ParentId".into(), json!(p));
        }
    }
    m.insert(format!("{event_type}Details"), details);
    exec.events.push(Value::Object(m));
}

/// Create a running execution with its EXECUTION operation and
/// `ExecutionStarted` event. The first invocation is due at once.
pub(crate) fn create_execution(p: NewExecution, now: DateTime<Utc>) -> DurableExecution {
    let mut op = Operation::new(&p.id, OpType::Execution, OpStatus::Started, now);
    op.name = Some(p.name.clone());
    op.input_payload = Some(p.input.clone());
    let subject = Subject::of(&op);
    let mut exec = DurableExecution {
        arn: p.arn,
        id: p.id.clone(),
        name: p.name,
        function_name: p.function_name,
        function_arn: p.function_arn,
        version: p.version,
        status: RUNNING.to_string(),
        input_payload: p.input.clone(),
        result: None,
        error: None,
        start_timestamp: now,
        end_timestamp: None,
        execution_timeout: p.execution_timeout,
        retention_days: p.retention_days,
        kms_key_arn: p.kms_key_arn,
        trace_id: p.trace_id,
        tenant_id: p.tenant_id,
        operations: vec![op],
        events: Vec::new(),
        invocation: None,
        dirty: vec![p.id],
        last_updated_ids: Vec::new(),
        wake: true,
        retry: None,
        parent: p.parent,
    };
    let details = json!({
        "Input": payload_wrapper(Some(&p.input)),
        "ExecutionTimeout": exec.execution_timeout,
    });
    push_event(&mut exec, now, "ExecutionStarted", Some(&subject), details);
    exec
}

/// The operations handed to the function: every descendant of a closed
/// CONTEXT that did not ask to replay its children is left out, since the
/// SDK replays such a context from its own result.
/// Each operation comes with its position in the operation log, which never
/// changes (the log only grows) and so keys pagination.
pub(crate) fn visible_operations(exec: &DurableExecution) -> Vec<(usize, &Operation)> {
    let prunes = |op: &Operation| {
        op.op_type == OpType::Context
            && matches!(op.status, OpStatus::Succeeded | OpStatus::Failed)
            && op.replay_children != Some(true)
    };
    let by_id: std::collections::HashMap<&str, &Operation> =
        exec.operations.iter().map(|o| (o.id.as_str(), o)).collect();
    exec.operations
        .iter()
        .enumerate()
        .filter(|(_, op)| {
            let mut current = op.parent_id.as_deref();
            let mut hops = 0;
            while let Some(pid) = current {
                hops += 1;
                if hops > exec.operations.len() {
                    break;
                }
                let Some(parent) = by_id.get(pid) else { break };
                if prunes(parent) {
                    return false;
                }
                current = parent.parent_id.as_deref();
            }
            true
        })
        .collect()
}

/// An opaque pagination marker naming the last item returned by a stable
/// key, so items added or removed between pages neither repeat nor go
/// missing (an offset would shift).
pub(crate) fn encode_marker<K: serde::Serialize>(after: &K) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(json!({ "v": 1, "after": after }).to_string())
}

pub(crate) fn decode_marker<K: serde::de::DeserializeOwned>(
    marker: &str,
) -> Result<K, AwsServiceError> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(marker)
        .ok()
        .and_then(|raw| serde_json::from_slice::<Value>(&raw).ok())
        .filter(|v| v["v"] == 1)
        .and_then(|v| serde_json::from_value(v["after"].clone()).ok())
        .ok_or_else(|| invalid_parameter("Invalid marker"))
}

/// One page of `items`, each with its sort key: ascending by key, or
/// descending. A marker resumes after the key it names, wherever that item
/// is now (or would be).
pub(crate) fn page_by_key<K, T>(
    mut items: Vec<(K, T)>,
    descending: bool,
    marker: Option<&str>,
    max_items: usize,
) -> Result<(Vec<T>, Option<String>), AwsServiceError>
where
    K: Ord + Clone + serde::Serialize + serde::de::DeserializeOwned,
{
    items.sort_by(|a, b| a.0.cmp(&b.0));
    if descending {
        items.reverse();
    }
    if let Some(m) = marker.filter(|m| !m.is_empty()) {
        let after: K = decode_marker(m)?;
        items.retain(|(k, _)| if descending { *k < after } else { *k > after });
    }
    let limit = if max_items == 0 {
        DEFAULT_PAGE
    } else {
        max_items.min(MAX_PAGE)
    };
    let more = items.len() > limit;
    items.truncate(limit);
    let next = more
        .then(|| items.last().map(|(k, _)| encode_marker(k)))
        .flatten();
    Ok((items.into_iter().map(|(_, t)| t).collect(), next))
}

/// Start an invocation: mint its checkpoint token and build the payload
/// the function receives.
pub(crate) fn begin_invocation(
    exec: &mut DurableExecution,
    request_id: &str,
    now: DateTime<Utc>,
) -> Value {
    // A retried invocation sees the operations its failed attempt was
    // told about, plus anything that changed since.
    let mut updated = if exec.retry.is_some() {
        exec.last_updated_ids.clone()
    } else {
        Vec::new()
    };
    for id in exec.dirty.drain(..) {
        if !updated.contains(&id) {
            updated.push(id);
        }
    }
    exec.last_updated_ids = updated.clone();
    exec.wake = false;
    let token = new_checkpoint_token();
    exec.invocation = Some(ActiveInvocation {
        request_id: request_id.to_string(),
        start: now,
        token: Some(token.clone()),
        previous_token: None,
        previous_client_token: None,
        previous_response: None,
    });

    let visible = visible_operations(exec);
    let mut ops = Vec::new();
    let mut size = 0usize;
    let mut next_marker = None;
    let mut last_index = None;
    for (index, op) in visible {
        let wire = op.to_wire(TimeFormat::Millis);
        size += wire.to_string().len();
        if size > MAX_INITIAL_STATE_BYTES {
            if let Some(last) = last_index {
                // The rest is read with GetDurableExecutionState.
                next_marker = Some(encode_marker(&last));
                break;
            }
        }
        ops.push(wire);
        last_index = Some(index);
    }
    let mut state = Map::new();
    state.insert("Operations".into(), Value::Array(ops));
    if let Some(m) = next_marker {
        state.insert("NextMarker".into(), json!(m));
    }
    json!({
        "DurableExecutionArn": exec.arn,
        "CheckpointToken": token,
        "InitialExecutionState": Value::Object(state),
        "UpdatedOperationIds": updated,
    })
}

fn require_token(exec: &DurableExecution, token: &str) -> Result<(), AwsServiceError> {
    let current = exec
        .invocation
        .as_ref()
        .filter(|_| exec.is_running())
        .and_then(|i| i.token.as_deref());
    if current == Some(token) && !token.is_empty() {
        Ok(())
    } else {
        Err(invalid_token())
    }
}

/// `GetDurableExecutionState`: one page of the replay state.
pub(crate) fn state_page(
    exec: &DurableExecution,
    token: &str,
    marker: Option<&str>,
    max_items: usize,
) -> Result<Value, AwsServiceError> {
    require_token(exec, token)?;
    let items: Vec<(usize, Value)> = visible_operations(exec)
        .into_iter()
        .map(|(i, o)| (i, o.to_wire(TimeFormat::Seconds)))
        .collect();
    let (ops, next) = page_by_key(items, false, marker, max_items)?;
    let mut m = Map::new();
    m.insert("Operations".into(), Value::Array(ops));
    if let Some(n) = next {
        m.insert("NextMarker".into(), json!(n));
    }
    Ok(Value::Object(m))
}

/// `CheckpointDurableExecution`.
pub(crate) fn checkpoint(
    exec: &mut DurableExecution,
    token: &str,
    client_token: Option<&str>,
    updates: &[Value],
    now: DateTime<Utc>,
) -> Result<Value, AwsServiceError> {
    if let Err(e) = require_token(exec, token) {
        // A retry of the last successful checkpoint (same token, same
        // ClientToken) gets that checkpoint's answer again.
        let replay = exec.invocation.as_ref().and_then(|i| {
            let same = client_token.is_some()
                && i.previous_token.as_deref() == Some(token)
                && i.previous_client_token.as_deref() == client_token;
            same.then(|| i.previous_response.clone()).flatten()
        });
        return replay.ok_or(e);
    }
    validate_updates(updates, exec)?;
    for u in updates {
        apply_update(exec, u, now);
    }
    let finished = !exec.is_running();
    let ops: Vec<Value> = std::mem::take(&mut exec.dirty)
        .iter()
        .filter_map(|id| exec.op(id))
        .map(|o| o.to_wire(TimeFormat::Seconds))
        .collect();
    // `wake` stays set: the SDK may already have suspended the branch an
    // operation the service just changed belongs to, so the function runs
    // again once it answers PENDING (at worst one extra replay).
    let mut m = Map::new();
    let next_token = (!finished).then(new_checkpoint_token);
    if let Some(t) = &next_token {
        m.insert("CheckpointToken".into(), json!(t));
    }
    m.insert("NewExecutionState".into(), json!({ "Operations": ops }));
    let response = Value::Object(m);
    if let Some(inv) = exec.invocation.as_mut() {
        inv.previous_token = Some(token.to_string());
        inv.previous_client_token = client_token.map(str::to_string);
        inv.previous_response = Some(response.clone());
        inv.token = next_token;
    }
    Ok(response)
}

fn s<'a>(u: &'a Value, key: &str) -> Option<&'a str> {
    u.get(key).and_then(Value::as_str)
}

fn int_option(u: &Value, container: &str, key: &str) -> Option<i64> {
    u.get(container)
        .and_then(|c| c.get(key))
        .and_then(service_integer)
}

/// Record a new operation from its first update.
fn open_operation(exec: &mut DurableExecution, u: &Value, op_type: OpType, now: DateTime<Utc>) {
    let mut op = Operation::new(
        s(u, "Id").unwrap_or_default(),
        op_type,
        OpStatus::Started,
        now,
    );
    op.parent_id = s(u, "ParentId").map(str::to_string);
    op.name = s(u, "Name").map(str::to_string);
    op.sub_type = s(u, "SubType").map(str::to_string);
    let subject = Subject::of(&op);
    let id = op.id.clone();
    match op_type {
        OpType::Step => {
            exec.operations.push(op);
            push_event(exec, now, "StepStarted", Some(&subject), json!({}));
        }
        OpType::Context => {
            exec.operations.push(op);
            push_event(exec, now, "ContextStarted", Some(&subject), json!({}));
        }
        OpType::Wait => {
            let secs = int_option(u, "WaitOptions", "WaitSeconds").unwrap_or(1);
            let end = now + Duration::seconds(secs);
            op.wait_seconds = Some(secs);
            op.scheduled_end = Some(end);
            exec.operations.push(op);
            push_event(
                exec,
                now,
                "WaitStarted",
                Some(&subject),
                json!({ "Duration": secs, "ScheduledEndTimestamp": ts(end, TimeFormat::Seconds) }),
            );
        }
        OpType::Callback => {
            let timeout = int_option(u, "CallbackOptions", "TimeoutSeconds").unwrap_or(0);
            let heartbeat =
                int_option(u, "CallbackOptions", "HeartbeatTimeoutSeconds").unwrap_or(0);
            let callback_id = new_callback_id(&exec.arn, &exec.version);
            op.callback = Some(CallbackState {
                id: callback_id.clone(),
                timeout_seconds: timeout,
                heartbeat_timeout_seconds: heartbeat,
                deadline: (timeout > 0).then(|| now + Duration::seconds(timeout)),
                heartbeat_deadline: (heartbeat > 0).then(|| now + Duration::seconds(heartbeat)),
            });
            exec.operations.push(op);
            let mut d = Map::new();
            d.insert("CallbackId".into(), json!(callback_id));
            if timeout > 0 {
                d.insert("Timeout".into(), json!(timeout));
            }
            if heartbeat > 0 {
                d.insert("HeartbeatTimeout".into(), json!(heartbeat));
            }
            push_event(
                exec,
                now,
                "CallbackStarted",
                Some(&subject),
                Value::Object(d),
            );
        }
        OpType::ChainedInvoke => {
            let options = u.get("ChainedInvokeOptions");
            op.chained = Some(ChainedInvokeState {
                function_name: options
                    .and_then(|o| s(o, "FunctionName"))
                    .unwrap_or_default()
                    .to_string(),
                tenant_id: options.and_then(|o| s(o, "TenantId")).map(str::to_string),
                trace_header: options
                    .and_then(|o| s(o, "XAmznTraceId"))
                    .filter(|t| !t.is_empty())
                    .map(str::to_string),
                payload: s(u, "Payload").map(str::to_string),
                dispatched: false,
                child_execution_arn: None,
                executed_version: None,
                deferred: None,
            });
            // `ChainedInvokeStarted` is recorded when the service dispatches
            // the call, once the invoked version is known.
            exec.operations.push(op);
        }
        OpType::Execution => {}
    }
    exec.mark_dirty(&id);
}

/// Apply one validated update.
fn apply_update(exec: &mut DurableExecution, u: &Value, now: DateTime<Utc>) {
    let Some(op_type) = s(u, "Type").and_then(OpType::parse) else {
        return;
    };
    let action = s(u, "Action").unwrap_or_default();
    let payload = s(u, "Payload").map(str::to_string);
    let error = u.get("Error").filter(|e| !e.is_null()).cloned();

    if op_type == OpType::Execution {
        // An EXECUTION update always targets the execution itself, whatever
        // id the SDK used (its oversized-result path invents one).
        match action {
            "SUCCEED" => finish(exec, SUCCEEDED, payload, None, now),
            _ => finish(
                exec,
                FAILED,
                None,
                Some(error.unwrap_or_else(|| json!({}))),
                now,
            ),
        }
        return;
    }

    let id = s(u, "Id").unwrap_or_default().to_string();
    let exists = exec.op(&id).is_some();
    if action == "START" {
        if exists {
            // Only a READY step starts again: its next attempt.
            if let Some(op) = exec.op_mut(&id) {
                op.status = OpStatus::Started;
                op.next_attempt = None;
            }
            let subject = exec.op(&id).map(Subject::of);
            push_event(exec, now, "StepStarted", subject.as_ref(), json!({}));
            exec.mark_dirty(&id);
        } else {
            open_operation(exec, u, op_type, now);
        }
        return;
    }
    if !exists {
        // Steps and contexts may be closed without a separate START.
        open_operation(exec, u, op_type, now);
    }
    let retry_delay = int_option(u, "StepOptions", "NextAttemptDelaySeconds");
    let replay_children = u
        .get("ContextOptions")
        .and_then(|c| c.get("ReplayChildren"))
        .and_then(|v| match v {
            Value::Bool(b) => Some(*b),
            Value::String(s) => Some(s.eq_ignore_ascii_case("true")),
            _ => None,
        })
        == Some(true);
    let Some(op) = exec.op_mut(&id) else { return };
    let (event, details) = match (op_type, action) {
        (OpType::Step, "SUCCEED") | (OpType::Step, "FAIL") => {
            op.attempt += 1;
            op.end = Some(now);
            op.next_attempt = None;
            let mut retry = Map::new();
            retry.insert("CurrentAttempt".into(), json!(op.attempt));
            if let Some(d) = retry_delay {
                retry.insert("NextAttemptDelaySeconds".into(), json!(d));
            }
            if action == "SUCCEED" {
                op.status = OpStatus::Succeeded;
                op.result = payload.clone();
                (
                    "StepSucceeded",
                    json!({ "Result": payload_wrapper(payload.as_deref()), "RetryDetails": retry }),
                )
            } else {
                let e = error.unwrap_or_else(|| json!({}));
                op.status = OpStatus::Failed;
                op.error = Some(e.clone());
                (
                    "StepFailed",
                    json!({ "Error": error_wrapper(&e), "RetryDetails": retry }),
                )
            }
        }
        (OpType::Step, "RETRY") => {
            let delay = retry_delay.unwrap_or(1);
            op.attempt += 1;
            op.status = OpStatus::Pending;
            op.next_attempt = Some(now + Duration::seconds(delay));
            let retry = json!({ "CurrentAttempt": op.attempt, "NextAttemptDelaySeconds": delay });
            match error {
                Some(e) => {
                    op.error = Some(e.clone());
                    (
                        "StepFailed",
                        json!({ "Error": error_wrapper(&e), "RetryDetails": retry }),
                    )
                }
                None => {
                    op.result = payload.clone();
                    (
                        "StepSucceeded",
                        json!({ "Result": payload_wrapper(payload.as_deref()), "RetryDetails": retry }),
                    )
                }
            }
        }
        (OpType::Context, "SUCCEED") => {
            op.status = OpStatus::Succeeded;
            op.end = Some(now);
            op.replay_children = Some(replay_children);
            op.result = payload.clone();
            (
                "ContextSucceeded",
                json!({ "Result": payload_wrapper(payload.as_deref()) }),
            )
        }
        (OpType::Context, "FAIL") => {
            let e = error.unwrap_or_else(|| json!({}));
            op.status = OpStatus::Failed;
            op.end = Some(now);
            op.replay_children = Some(replay_children);
            op.error = Some(e.clone());
            ("ContextFailed", json!({ "Error": error_wrapper(&e) }))
        }
        (OpType::Wait, "CANCEL") => {
            op.status = OpStatus::Cancelled;
            op.end = Some(now);
            ("WaitCancelled", json!({}))
        }
        _ => return,
    };
    let subject = Subject::of(op);
    push_event(exec, now, event, Some(&subject), details);
    exec.mark_dirty(&id);
}

/// Move a running execution to a terminal status.
pub(crate) fn finish(
    exec: &mut DurableExecution,
    status: &str,
    result: Option<String>,
    error: Option<Value>,
    now: DateTime<Utc>,
) {
    if !exec.is_running() {
        return;
    }
    exec.status = status.to_string();
    exec.end_timestamp = Some(now);
    exec.result = result.clone();
    exec.error = error.clone();
    exec.wake = false;
    exec.retry = None;
    let exec_id = exec.id.clone();
    let op_status = match status {
        SUCCEEDED => OpStatus::Succeeded,
        FAILED => OpStatus::Failed,
        TIMED_OUT => OpStatus::TimedOut,
        _ => OpStatus::Stopped,
    };
    let subject = exec.op_mut(&exec_id).map(|op| {
        op.status = op_status;
        op.end = Some(now);
        op.result = result.clone();
        op.error = error.clone();
        Subject::of(op)
    });
    let (event, details) = match status {
        SUCCEEDED => (
            "ExecutionSucceeded",
            json!({ "Result": payload_wrapper(result.as_deref()) }),
        ),
        FAILED => (
            "ExecutionFailed",
            json!({ "Error": error_wrapper(error.as_ref().unwrap_or(&json!({}))) }),
        ),
        TIMED_OUT => ("ExecutionTimedOut", json!({})),
        _ => (
            "ExecutionStopped",
            json!({ "Error": error_wrapper(error.as_ref().unwrap_or(&json!({}))) }),
        ),
    };
    push_event(exec, now, event, subject.as_ref(), details);
}

/// How an invocation ended.
#[derive(Debug, Clone)]
pub(crate) enum InvocationOutcome {
    /// The function returned this value.
    Output(Value),
    /// The invocation itself failed; the value is an `ErrorObject`.
    FunctionError(Value),
}

/// Whether something is still in flight that will move the execution
/// along without the function returning a result.
fn has_pending_work(exec: &DurableExecution) -> bool {
    exec.wake
        || exec.operations.iter().any(|op| match op.op_type {
            OpType::Wait | OpType::Callback | OpType::ChainedInvoke => {
                op.status == OpStatus::Started
            }
            OpType::Step => matches!(op.status, OpStatus::Pending | OpStatus::Ready),
            _ => false,
        })
}

fn invocation_completed(
    exec: &mut DurableExecution,
    inv: &ActiveInvocation,
    error: Option<&Value>,
    now: DateTime<Utc>,
) {
    let mut d = Map::new();
    d.insert("StartTimestamp".into(), ts(inv.start, TimeFormat::Seconds));
    d.insert("EndTimestamp".into(), ts(now, TimeFormat::Seconds));
    d.insert("RequestId".into(), json!(inv.request_id));
    if let Some(e) = error {
        d.insert("Error".into(), error_wrapper(e));
    }
    push_event(exec, now, "InvocationCompleted", None, Value::Object(d));
}

/// The delay before retry number `failures` (1-based). A failing function
/// backs off along the Fibonacci sequence; a function that answers PENDING
/// with nothing pending doubles its delay.
pub(crate) fn retry_delay(kind: RetryKind, failures: u32) -> i64 {
    match kind {
        RetryKind::PendingWithoutWork => 1i64 << failures.saturating_sub(1).min(20),
        RetryKind::FunctionError => {
            let (mut a, mut b) = (1i64, 1i64);
            for _ in 1..failures.min(40) {
                let next = a + b;
                a = b;
                b = next;
            }
            a
        }
    }
}

fn invocation_failed(
    exec: &mut DurableExecution,
    kind: RetryKind,
    error: Value,
    now: DateTime<Utc>,
) {
    let failures = match &exec.retry {
        Some(r) if r.kind == kind => r.failures + 1,
        _ => 1,
    };
    let at = now + Duration::seconds(retry_delay(kind, failures));
    if at >= exec.deadline() {
        // No attempt left before the execution would time out.
        finish(exec, FAILED, None, Some(error), now);
    } else {
        exec.retry = Some(InvocationRetry { failures, at, kind });
    }
}

/// Record the end of the invocation `request_id`. Returns true when the
/// function must run again at once.
pub(crate) fn complete_invocation(
    exec: &mut DurableExecution,
    request_id: &str,
    outcome: InvocationOutcome,
    now: DateTime<Utc>,
) -> bool {
    let Some(inv) = exec.invocation.take_if(|i| i.request_id == request_id) else {
        return false;
    };
    if !exec.is_running() {
        // Finished through a checkpoint, stopped or timed out meanwhile.
        invocation_completed(exec, &inv, None, now);
        return false;
    }
    let output = match outcome {
        InvocationOutcome::FunctionError(e) => {
            invocation_completed(exec, &inv, Some(&e), now);
            apply_deferred_chained(exec, now);
            invocation_failed(exec, RetryKind::FunctionError, e, now);
            return false;
        }
        InvocationOutcome::Output(v) => v,
    };
    let status = output.get("Status").and_then(Value::as_str);
    match status {
        Some("SUCCEEDED") => {
            invocation_completed(exec, &inv, None, now);
            let result = match output.get("Result") {
                Some(Value::String(r)) => Some(r.clone()),
                None | Some(Value::Null) => None,
                Some(other) => Some(other.to_string()),
            };
            finish(exec, SUCCEEDED, result, None, now);
            false
        }
        Some("FAILED") => {
            invocation_completed(exec, &inv, None, now);
            let e = output
                .get("Error")
                .filter(|e| !e.is_null())
                .cloned()
                .unwrap_or_else(|| json!({}));
            finish(exec, FAILED, None, Some(e), now);
            false
        }
        Some("PENDING") if has_pending_work(exec) => {
            invocation_completed(exec, &inv, None, now);
            apply_deferred_chained(exec, now);
            exec.retry = None;
            exec.wake
        }
        Some("PENDING") => {
            let e = json!({
                "ErrorType": "InvalidParameterValueException",
                "ErrorMessage": "Cannot return PENDING status with no pending operations.",
            });
            invocation_completed(exec, &inv, Some(&e), now);
            invocation_failed(exec, RetryKind::PendingWithoutWork, e, now);
            false
        }
        _ => {
            let e = json!({
                "ErrorType": "InvalidParameterValueException",
                "ErrorMessage": "Invalid Status in invocation output.",
            });
            invocation_completed(exec, &inv, Some(&e), now);
            finish(exec, FAILED, None, Some(e), now);
            false
        }
    }
}

/// Advance the timers of a running execution: its own timeout, elapsed
/// waits, step retries that are due, callback timeouts. Returns true when
/// anything changed.
pub(crate) fn tick(exec: &mut DurableExecution, now: DateTime<Utc>) -> bool {
    if !exec.is_running() {
        return false;
    }
    if now >= exec.deadline() {
        let deadline = exec.deadline();
        let e = json!({
            "ErrorMessage": format!("Execution timed out after {} seconds.", exec.execution_timeout),
        });
        finish(exec, TIMED_OUT, None, Some(e), deadline);
        return true;
    }
    let mut changed = Vec::new();
    for op in exec.operations.iter_mut() {
        if op.status != OpStatus::Started && op.status != OpStatus::Pending {
            continue;
        }
        match op.op_type {
            OpType::Wait if op.status == OpStatus::Started => {
                if let Some(end) = op.scheduled_end.filter(|e| *e <= now) {
                    op.status = OpStatus::Succeeded;
                    op.end = Some(end);
                    changed.push((
                        op.id.clone(),
                        end,
                        "WaitSucceeded",
                        json!({ "Duration": op.wait_seconds }),
                    ));
                }
            }
            OpType::Step if op.status == OpStatus::Pending => {
                if op.next_attempt.is_some_and(|t| t <= now) {
                    op.status = OpStatus::Ready;
                    changed.push((op.id.clone(), now, "", Value::Null));
                }
            }
            OpType::Callback if op.status == OpStatus::Started => {
                let Some(cb) = &op.callback else { continue };
                let timed_out = cb
                    .deadline
                    .filter(|d| *d <= now)
                    .map(|d| (d, "Callback.Timeout"));
                let heartbeat = cb
                    .heartbeat_deadline
                    .filter(|d| *d <= now)
                    .map(|d| (d, "Callback.Heartbeat"));
                let first = match (timed_out, heartbeat) {
                    (Some(a), Some(b)) => Some(if b.0 < a.0 { b } else { a }),
                    (a, b) => a.or(b),
                };
                if let Some((at, kind)) = first {
                    let e = json!({ "ErrorType": kind });
                    op.status = OpStatus::TimedOut;
                    op.end = Some(at);
                    op.error = Some(e.clone());
                    changed.push((
                        op.id.clone(),
                        at,
                        "CallbackTimedOut",
                        json!({ "Error": error_wrapper(&e) }),
                    ));
                }
            }
            _ => {}
        }
    }
    let any = !changed.is_empty();
    for (id, at, event, details) in changed {
        if !event.is_empty() {
            let subject = exec.op(&id).map(Subject::of);
            push_event(exec, at, event, subject.as_ref(), details);
        }
        exec.mark_dirty(&id);
        exec.wake = true;
    }
    any
}

/// How an external caller completed a callback.
pub(crate) enum CallbackOutcome {
    Succeeded(Option<String>),
    Failed(Value),
}

/// `SendDurableExecutionCallbackSuccess` / `Failure`. Returns false when
/// the callback is no longer open.
pub(crate) fn complete_callback(
    exec: &mut DurableExecution,
    callback_id: &str,
    outcome: CallbackOutcome,
    now: DateTime<Utc>,
) -> bool {
    if !exec.is_running() {
        return false;
    }
    let Some(op) = exec.operations.iter_mut().find(|o| {
        o.status == OpStatus::Started && o.callback.as_ref().is_some_and(|c| c.id == callback_id)
    }) else {
        return false;
    };
    op.end = Some(now);
    let (event, details) = match outcome {
        CallbackOutcome::Succeeded(result) => {
            op.status = OpStatus::Succeeded;
            op.result = result.clone();
            (
                "CallbackSucceeded",
                json!({ "Result": payload_wrapper(result.as_deref()) }),
            )
        }
        CallbackOutcome::Failed(e) => {
            op.status = OpStatus::Failed;
            op.error = Some(e.clone());
            ("CallbackFailed", json!({ "Error": error_wrapper(&e) }))
        }
    };
    let subject = Subject::of(op);
    let id = op.id.clone();
    push_event(exec, now, event, Some(&subject), details);
    exec.mark_dirty(&id);
    exec.wake = true;
    true
}

/// `SendDurableExecutionCallbackHeartbeat`. Returns false when the
/// callback is no longer open.
pub(crate) fn heartbeat(
    exec: &mut DurableExecution,
    callback_id: &str,
    now: DateTime<Utc>,
) -> bool {
    if !exec.is_running() {
        return false;
    }
    let Some(cb) = exec
        .operations
        .iter_mut()
        .filter(|o| o.status == OpStatus::Started)
        .find_map(|o| o.callback.as_mut().filter(|c| c.id == callback_id))
    else {
        return false;
    };
    if cb.heartbeat_timeout_seconds > 0 {
        cb.heartbeat_deadline = Some(now + Duration::seconds(cb.heartbeat_timeout_seconds));
    }
    true
}

/// `StopDurableExecution`: stop a running execution, or report when a
/// finished one ended.
pub(crate) fn stop(
    exec: &mut DurableExecution,
    error: Option<Value>,
    now: DateTime<Utc>,
) -> DateTime<Utc> {
    if !exec.is_running() {
        return exec.end_timestamp.unwrap_or(now);
    }
    finish(
        exec,
        STOPPED,
        None,
        Some(error.unwrap_or_else(|| json!({}))),
        now,
    );
    now
}

/// Record that the service dispatched a chained invoke.
pub(crate) fn chained_dispatched(
    exec: &mut DurableExecution,
    op_id: &str,
    executed_version: Option<&str>,
    child_execution_arn: Option<&str>,
) {
    let Some(op) = exec.op_mut(op_id) else { return };
    let start = op.start;
    let Some(ch) = op.chained.as_mut() else {
        return;
    };
    ch.dispatched = true;
    ch.executed_version = executed_version.map(str::to_string);
    ch.child_execution_arn = child_execution_arn.map(str::to_string);
    let mut d = Map::new();
    d.insert("FunctionName".into(), json!(ch.function_name));
    if let Some(t) = &ch.tenant_id {
        d.insert("TenantId".into(), json!(t));
    }
    d.insert("Input".into(), payload_wrapper(ch.payload.as_deref()));
    if let Some(v) = executed_version {
        d.insert("ExecutedVersion".into(), json!(v));
    }
    if let Some(a) = child_execution_arn {
        d.insert("DurableExecutionArn".into(), json!(a));
    }
    let subject = Subject::of(op);
    push_event(
        exec,
        start,
        "ChainedInvokeStarted",
        Some(&subject),
        Value::Object(d),
    );
}

/// Record the child execution of a chained invoke dispatched again after a
/// restart (its `ChainedInvokeStarted` event already exists).
pub(crate) fn set_chained_child(exec: &mut DurableExecution, op_id: &str, child: &str) {
    if let Some(ch) = exec.op_mut(op_id).and_then(|o| o.chained.as_mut()) {
        ch.child_execution_arn = Some(child.to_string());
    }
}

/// How a chained invoke ended.
pub(crate) struct ChainedOutcome {
    pub status: OpStatus,
    pub result: Option<String>,
    pub error: Option<Value>,
}

/// Complete a chained-invoke operation. Returns false when it is not open.
///
/// A call that returns while the parent's invocation is still running is
/// recorded when that invocation ends: the parent learns of it on its next
/// invocation, and its history shows `ChainedInvokeSucceeded` (or `Failed`,
/// ...) after `InvocationCompleted`, as on AWS.
pub(crate) fn complete_chained(
    exec: &mut DurableExecution,
    op_id: &str,
    outcome: ChainedOutcome,
    now: DateTime<Utc>,
) -> bool {
    if !exec.is_running() {
        return false;
    }
    let in_invocation = exec.invocation.is_some();
    let Some(op) = exec
        .op_mut(op_id)
        .filter(|o| o.op_type == OpType::ChainedInvoke && o.status == OpStatus::Started)
    else {
        return false;
    };
    if in_invocation {
        let Some(ch) = op.chained.as_mut().filter(|c| c.deferred.is_none()) else {
            return false;
        };
        ch.deferred = Some(DeferredOutcome {
            status: outcome.status,
            result: outcome.result,
            error: outcome.error,
        });
        return true;
    }
    apply_chained(exec, op_id, outcome, now);
    true
}

/// Record the chained invokes that returned during the invocation that just
/// ended.
pub(crate) fn apply_deferred_chained(exec: &mut DurableExecution, now: DateTime<Utc>) {
    let deferred: Vec<(String, DeferredOutcome)> = exec
        .operations
        .iter_mut()
        .filter_map(|o| {
            let d = o.chained.as_mut()?.deferred.take()?;
            Some((o.id.clone(), d))
        })
        .collect();
    for (id, d) in deferred {
        apply_chained(
            exec,
            &id,
            ChainedOutcome {
                status: d.status,
                result: d.result,
                error: d.error,
            },
            now,
        );
    }
}

fn apply_chained(
    exec: &mut DurableExecution,
    op_id: &str,
    outcome: ChainedOutcome,
    now: DateTime<Utc>,
) {
    let Some(op) = exec
        .op_mut(op_id)
        .filter(|o| o.op_type == OpType::ChainedInvoke && o.status == OpStatus::Started)
    else {
        return;
    };
    op.status = outcome.status;
    op.end = Some(now);
    op.result = outcome.result.clone();
    op.error = outcome.error.clone();
    let err = outcome.error.unwrap_or_else(|| json!({}));
    let (event, details) = match outcome.status {
        OpStatus::Succeeded => (
            "ChainedInvokeSucceeded",
            json!({ "Result": payload_wrapper(outcome.result.as_deref()) }),
        ),
        OpStatus::TimedOut => (
            "ChainedInvokeTimedOut",
            json!({ "Error": error_wrapper(&err) }),
        ),
        OpStatus::Stopped => (
            "ChainedInvokeStopped",
            json!({ "Error": error_wrapper(&err) }),
        ),
        _ => (
            "ChainedInvokeFailed",
            json!({ "Error": error_wrapper(&err) }),
        ),
    };
    let subject = Subject::of(op);
    let id = op.id.clone();
    push_event(exec, now, event, Some(&subject), details);
    exec.mark_dirty(&id);
    exec.wake = true;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable::model::{callback_execution_arn, now as now_ms};

    const ARN: &str = "arn:aws:lambda:us-east-1:123456789012:function:f:$LATEST/durable-execution/n/11111111-1111-3111-8111-111111111111";

    fn t0() -> DateTime<Utc> {
        DateTime::from_timestamp_millis(1_794_322_688_966).unwrap()
    }

    fn new_exec(timeout: i64) -> DurableExecution {
        create_execution(
            NewExecution {
                arn: ARN.into(),
                id: "11111111-1111-3111-8111-111111111111".into(),
                name: "n".into(),
                function_name: "f".into(),
                function_arn: "arn:aws:lambda:us-east-1:123456789012:function:f:$LATEST".into(),
                version: "$LATEST".into(),
                input: "{\"n\":21}".into(),
                execution_timeout: timeout,
                retention_days: 1,
                kms_key_arn: None,
                trace_id: "Root=1-x".into(),
                tenant_id: None,
                parent: None,
            },
            t0(),
        )
    }

    fn token(exec: &DurableExecution) -> String {
        exec.invocation.as_ref().unwrap().token.clone().unwrap()
    }

    fn ck(exec: &mut DurableExecution, updates: Value, at: DateTime<Utc>) -> Value {
        let t = token(exec);
        checkpoint(exec, &t, None, updates.as_array().unwrap(), at).unwrap()
    }

    fn event_types(exec: &DurableExecution) -> Vec<String> {
        exec.events
            .iter()
            .map(|e| e["EventType"].as_str().unwrap().to_string())
            .collect()
    }

    #[test]
    fn first_invocation_payload_matches_the_recorded_envelope() {
        let mut e = new_exec(600);
        assert!(e.wake);
        let env = begin_invocation(&mut e, "req-1", t0());
        assert_eq!(env["DurableExecutionArn"], ARN);
        assert_eq!(
            env["InitialExecutionState"]["Operations"],
            json!([{
                "Id": "11111111-1111-3111-8111-111111111111",
                "Name": "n",
                "Type": "EXECUTION",
                "StartTimestamp": 1_794_322_688_966i64,
                "Status": "STARTED",
                "ExecutionDetails": {"InputPayload": "{\"n\":21}"}
            }])
        );
        assert_eq!(
            env["UpdatedOperationIds"],
            json!(["11111111-1111-3111-8111-111111111111"])
        );
        assert!(env["InitialExecutionState"].get("NextMarker").is_none());
        assert!(!e.wake);
    }

    #[test]
    fn step_wait_replay_cycle() {
        let mut e = new_exec(600);
        begin_invocation(&mut e, "r1", t0());
        let t1 = t0() + Duration::milliseconds(1149);
        let r = ck(
            &mut e,
            json!([{"Id":"c4ca4238a0b92382","Type":"STEP","Action":"START","Name":"double","SubType":"Step"},
                   {"Id":"c4ca4238a0b92382","Type":"STEP","Action":"SUCCEED","Name":"double","SubType":"Step","Payload":"42"}]),
            t1,
        );
        assert_eq!(
            r["NewExecutionState"]["Operations"],
            json!([{"EndTimestamp":1_794_322_690.115,"Id":"c4ca4238a0b92382","Name":"double","StartTimestamp":1_794_322_690.115,"Status":"SUCCEEDED","StepDetails":{"Attempt":1,"Result":"42"},"SubType":"Step","Type":"STEP"}])
        );
        assert!(r["CheckpointToken"].is_string());
        let t2 = t1 + Duration::milliseconds(141);
        let r = ck(
            &mut e,
            json!([{"Id":"c81e728d9d4c2f63","Type":"WAIT","Action":"START","Name":"pause","SubType":"Wait","WaitOptions":{"WaitSeconds":3}}]),
            t2,
        );
        assert_eq!(
            r["NewExecutionState"]["Operations"][0]["WaitDetails"]["ScheduledEndTimestamp"],
            json!(1_794_322_693.256)
        );
        let again = complete_invocation(
            &mut e,
            "r1",
            InvocationOutcome::Output(json!({"Status":"PENDING"})),
            t2,
        );
        assert!(!again, "nothing is due yet");
        assert!(e.retry.is_none() && e.is_running());
        // Before the wait ends nothing changes; at its end it succeeds.
        assert!(!tick(&mut e, t2 + Duration::seconds(1)));
        assert!(tick(&mut e, t2 + Duration::seconds(3)));
        assert!(e.wake);
        let env = begin_invocation(&mut e, "r2", t2 + Duration::seconds(3));
        assert_eq!(env["UpdatedOperationIds"], json!(["c81e728d9d4c2f63"]));
        let ops = env["InitialExecutionState"]["Operations"]
            .as_array()
            .unwrap();
        assert_eq!(ops.len(), 3);
        assert_eq!(ops[2]["Status"], "SUCCEEDED");
        assert_eq!(ops[2]["EndTimestamp"], json!(1_794_322_693_256i64));
        complete_invocation(
            &mut e,
            "r2",
            InvocationOutcome::Output(json!({"Status":"SUCCEEDED","Result":"{\"doubled\":42}"})),
            t2 + Duration::seconds(3),
        );
        assert_eq!(e.status, SUCCEEDED);
        assert_eq!(e.result.as_deref(), Some("{\"doubled\":42}"));
        assert_eq!(
            event_types(&e),
            [
                "ExecutionStarted",
                "StepStarted",
                "StepSucceeded",
                "WaitStarted",
                "InvocationCompleted",
                "WaitSucceeded",
                "InvocationCompleted",
                "ExecutionSucceeded"
            ]
        );
        assert_eq!(
            e.events[2]["StepSucceededDetails"],
            json!({"Result":{"Truncated":false,"Payload":"42"},"RetryDetails":{"CurrentAttempt":1}})
        );
        assert_eq!(e.events[5]["WaitSucceededDetails"], json!({"Duration":3}));
        assert_eq!(e.events.last().unwrap()["Id"], e.id.as_str());
    }

    #[test]
    fn tokens_rotate_and_stale_tokens_are_refused() {
        let mut e = new_exec(600);
        begin_invocation(&mut e, "r1", t0());
        let first = token(&e);
        checkpoint(&mut e, &first, Some("ct"), &[], t0()).unwrap();
        let second = token(&e);
        assert_ne!(first, second);
        // The same retried request answers the same.
        assert!(checkpoint(&mut e, &first, Some("ct"), &[], t0()).is_ok());
        let err = checkpoint(&mut e, &first, Some("other"), &[], t0()).unwrap_err();
        assert_eq!(err.message(), "Invalid checkpoint token");
        assert!(state_page(&e, &first, None, 0).is_err());
        assert!(state_page(&e, &second, None, 0).is_ok());
        assert_eq!(
            checkpoint(&mut e, "AAAA", None, &[], t0())
                .unwrap_err()
                .message(),
            "Invalid checkpoint token"
        );
    }

    #[test]
    fn callbacks_complete_and_reinvoke() {
        let mut e = new_exec(600);
        begin_invocation(&mut e, "r1", t0());
        let r = ck(
            &mut e,
            json!([{"Id":"ctx","Type":"CONTEXT","Action":"START","Name":"approval","SubType":"WaitForCallback"},
                   {"Id":"cb","ParentId":"ctx","Type":"CALLBACK","Action":"START","SubType":"Callback","CallbackOptions":{"TimeoutSeconds":600,"HeartbeatTimeoutSeconds":120}}]),
            t0(),
        );
        let ops = r["NewExecutionState"]["Operations"].as_array().unwrap();
        assert_eq!(ops[0]["ContextDetails"], json!({}));
        let callback_id = ops[1]["CallbackDetails"]["CallbackId"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(callback_execution_arn(&callback_id).as_deref(), Some(ARN));
        assert_eq!(
            e.events[2]["CallbackStartedDetails"],
            json!({"CallbackId": callback_id, "Timeout": 600, "HeartbeatTimeout": 120})
        );
        complete_invocation(
            &mut e,
            "r1",
            InvocationOutcome::Output(json!({"Status":"PENDING"})),
            t0(),
        );
        assert!(!e.wake);
        assert!(heartbeat(&mut e, &callback_id, t0()));
        assert!(complete_callback(
            &mut e,
            &callback_id,
            CallbackOutcome::Succeeded(Some("{\"approved\":true}".into())),
            t0() + Duration::seconds(3)
        ));
        assert!(e.wake);
        // A second completion is refused (CallbackTimeoutException).
        assert!(!complete_callback(
            &mut e,
            &callback_id,
            CallbackOutcome::Failed(json!({})),
            t0()
        ));
        assert!(!heartbeat(&mut e, &callback_id, t0()));
        let env = begin_invocation(&mut e, "r2", t0() + Duration::seconds(3));
        assert_eq!(env["UpdatedOperationIds"], json!(["cb"]));
        let cb = &env["InitialExecutionState"]["Operations"][2];
        assert_eq!(cb["CallbackDetails"]["Result"], "{\"approved\":true}");
    }

    #[test]
    fn callback_timeouts_record_the_aws_error_types() {
        for (opts, kind) in [
            (json!({"TimeoutSeconds":2}), "Callback.Timeout"),
            (
                json!({"TimeoutSeconds":60,"HeartbeatTimeoutSeconds":2}),
                "Callback.Heartbeat",
            ),
        ] {
            let mut e = new_exec(600);
            begin_invocation(&mut e, "r1", t0());
            ck(
                &mut e,
                json!([{"Id":"cb","Type":"CALLBACK","Action":"START","CallbackOptions":opts}]),
                t0(),
            );
            complete_invocation(
                &mut e,
                "r1",
                InvocationOutcome::Output(json!({"Status":"PENDING"})),
                t0(),
            );
            assert!(tick(&mut e, t0() + Duration::seconds(2)));
            let ev = e.events.last().unwrap();
            assert_eq!(ev["EventType"], "CallbackTimedOut");
            assert_eq!(
                ev["CallbackTimedOutDetails"]["Error"]["Payload"],
                json!({"ErrorType": kind})
            );
            assert_eq!(e.op("cb").unwrap().status, OpStatus::TimedOut);
            assert!(e.wake);
        }
    }

    #[test]
    fn step_retry_moves_to_ready_when_due() {
        let mut e = new_exec(600);
        begin_invocation(&mut e, "r1", t0());
        let r = ck(
            &mut e,
            json!([{"Id":"s1","Type":"STEP","Action":"START","Name":"probe-step","SubType":"Step"}]),
            t0(),
        );
        assert_eq!(
            r["NewExecutionState"]["Operations"][0]["StepDetails"],
            json!({"Attempt":0})
        );
        let r = ck(
            &mut e,
            json!([{"Id":"s1","Type":"STEP","Action":"RETRY","Error":{"ErrorMessage":"try again","ErrorType":"Oops"},"StepOptions":{"NextAttemptDelaySeconds":1}}]),
            t0(),
        );
        let op = &r["NewExecutionState"]["Operations"][0];
        assert_eq!(op["Status"], "PENDING");
        assert_eq!(
            op["StepDetails"],
            json!({"Attempt":1,"NextAttemptTimestamp":1_794_322_689.966})
        );
        complete_invocation(
            &mut e,
            "r1",
            InvocationOutcome::Output(json!({"Status":"PENDING"})),
            t0(),
        );
        assert!(tick(&mut e, t0() + Duration::seconds(1)));
        assert_eq!(e.op("s1").unwrap().status, OpStatus::Ready);
        assert!(e.wake);
        begin_invocation(&mut e, "r2", t0() + Duration::seconds(1));
        ck(
            &mut e,
            json!([{"Id":"s1","Type":"STEP","Action":"START"},{"Id":"s1","Type":"STEP","Action":"SUCCEED","Payload":"\"ok\""}]),
            t0(),
        );
        let op = e.op("s1").unwrap();
        assert_eq!((op.status, op.attempt), (OpStatus::Succeeded, 2));
    }

    #[test]
    fn pending_with_nothing_pending_backs_off_then_fails() {
        let mut e = new_exec(15);
        let mut now = t0();
        let mut invocations = 0;
        while e.is_running() {
            begin_invocation(&mut e, &format!("r{invocations}"), now);
            invocations += 1;
            complete_invocation(
                &mut e,
                &format!("r{}", invocations - 1),
                InvocationOutcome::Output(json!({"Status":"PENDING"})),
                now,
            );
            if let Some(r) = &e.retry {
                now = r.at;
            }
        }
        // Recorded: four invocations, 1s/2s/4s apart, then FAILED.
        assert_eq!(invocations, 4);
        assert_eq!(e.status, FAILED);
        assert_eq!(
            e.error,
            Some(
                json!({"ErrorType":"InvalidParameterValueException","ErrorMessage":"Cannot return PENDING status with no pending operations."})
            )
        );
        let completed: Vec<_> = e
            .events
            .iter()
            .filter(|ev| ev["EventType"] == "InvocationCompleted")
            .collect();
        assert_eq!(completed.len(), 4);
        assert!(completed
            .iter()
            .all(|ev| ev["InvocationCompletedDetails"]["Error"].is_object()));
    }

    #[test]
    fn crashing_invocations_retry_on_a_fibonacci_schedule() {
        assert_eq!(
            (1..=6)
                .map(|n| retry_delay(RetryKind::FunctionError, n))
                .collect::<Vec<_>>(),
            [1, 1, 2, 3, 5, 8]
        );
        let mut e = new_exec(15);
        let mut now = t0();
        let mut invocations = 0;
        while e.is_running() {
            let id = format!("r{invocations}");
            begin_invocation(&mut e, &id, now);
            invocations += 1;
            complete_invocation(
                &mut e,
                &id,
                InvocationOutcome::FunctionError(
                    json!({"ErrorType":"Error","ErrorMessage":"crashed invocation"}),
                ),
                now,
            );
            if let Some(r) = &e.retry {
                now = r.at;
            }
        }
        // Recorded: six invocations, then FAILED with the function's error.
        assert_eq!(invocations, 6);
        assert_eq!(
            e.error,
            Some(json!({"ErrorType":"Error","ErrorMessage":"crashed invocation"}))
        );
    }

    #[test]
    fn invalid_output_fails_at_once() {
        let mut e = new_exec(15);
        begin_invocation(&mut e, "r", t0());
        complete_invocation(
            &mut e,
            "r",
            InvocationOutcome::Output(json!({"hello":1})),
            t0(),
        );
        assert_eq!(e.status, FAILED);
        assert_eq!(
            e.error.as_ref().unwrap()["ErrorMessage"],
            "Invalid Status in invocation output."
        );
    }

    #[test]
    fn execution_timeout_and_stop() {
        let mut e = new_exec(15);
        assert!(tick(&mut e, t0() + Duration::seconds(15)));
        assert_eq!(e.status, TIMED_OUT);
        assert_eq!(
            e.error,
            Some(json!({"ErrorMessage":"Execution timed out after 15 seconds."}))
        );
        assert_eq!(
            e.events.last().unwrap()["ExecutionTimedOutDetails"],
            json!({})
        );
        assert_eq!(e.end_timestamp, Some(t0() + Duration::seconds(15)));
        // Stopping a finished execution reports its end and changes nothing.
        assert_eq!(stop(&mut e, None, now_ms()), t0() + Duration::seconds(15));
        assert_eq!(e.status, TIMED_OUT);

        let mut e = new_exec(600);
        let at = t0() + Duration::seconds(2);
        assert_eq!(
            stop(
                &mut e,
                Some(json!({"ErrorMessage":"operator stop","ErrorType":"Manual"})),
                at
            ),
            at
        );
        assert_eq!(e.status, STOPPED);
        assert_eq!(
            e.events.last().unwrap()["ExecutionStoppedDetails"],
            json!({"Error":{"Truncated":false,"Payload":{"ErrorMessage":"operator stop","ErrorType":"Manual"}}})
        );
    }

    #[test]
    fn closed_contexts_hide_their_children_unless_replaying() {
        let mut e = new_exec(600);
        begin_invocation(&mut e, "r1", t0());
        ck(
            &mut e,
            json!([{"Id":"k1","Type":"CONTEXT","Action":"START"},
                   {"Id":"k1s","ParentId":"k1","Type":"STEP","Action":"START"},
                   {"Id":"k1s","ParentId":"k1","Type":"STEP","Action":"SUCCEED","Payload":"1"}]),
            t0(),
        );
        assert_eq!(visible_operations(&e).len(), 3);
        ck(
            &mut e,
            json!([{"Id":"k1","Type":"CONTEXT","Action":"SUCCEED","Payload":"\"done\""}]),
            t0(),
        );
        assert_eq!(visible_operations(&e).len(), 2);
        let ctx = e.op("k1").unwrap().to_wire(TimeFormat::Seconds);
        assert_eq!(
            ctx["ContextDetails"],
            json!({"ReplayChildren": false, "Result": "\"done\""})
        );
    }

    #[test]
    fn execution_update_completes_the_execution_and_withdraws_the_token() {
        let mut e = new_exec(600);
        begin_invocation(&mut e, "r1", t0());
        let r = ck(
            &mut e,
            json!([{"Id":"execution-result-1","Type":"EXECUTION","Action":"SUCCEED","Payload":"\"big\""}]),
            t0(),
        );
        assert!(r.get("CheckpointToken").is_none());
        assert_eq!(e.status, SUCCEEDED);
        complete_invocation(
            &mut e,
            "r1",
            InvocationOutcome::Output(json!({"Status":"SUCCEEDED","Result":""})),
            t0(),
        );
        assert_eq!(e.result.as_deref(), Some("\"big\""));
        assert_eq!(
            event_types(&e)
                .iter()
                .filter(|t| t.as_str() == "ExecutionSucceeded")
                .count(),
            1
        );
    }

    /// The event types of a recorded history (`tests/fixtures/durable`).
    fn recorded_event_types(label: &str) -> Vec<String> {
        let fixture = include_str!("../../tests/fixtures/durable/aws-round2.jsonl");
        let line = fixture
            .lines()
            .map(|l| serde_json::from_str::<Value>(l).unwrap())
            .find(|r| r["label"] == label && r["status"] == 200)
            .unwrap_or_else(|| panic!("{label} not recorded"));
        let body: Value = serde_json::from_str(line["body"].as_str().unwrap()).unwrap();
        body["Events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["EventType"].as_str().unwrap().to_string())
            .collect()
    }

    /// A chained invoke whose target returns while the parent invocation is
    /// still running is recorded after that invocation's
    /// `InvocationCompleted`, matching the AWS recordings, whether it
    /// succeeds or fails.
    #[test]
    fn chained_invoke_history_matches_the_recordings() {
        for (label, status, error) in [
            ("chain-unqualified-history", OpStatus::Succeeded, None),
            (
                "chainfail-history",
                OpStatus::Failed,
                Some(json!({"ErrorType":"Error","ErrorMessage":"target exploded"})),
            ),
        ] {
            let mut e = new_exec(600);
            begin_invocation(&mut e, "r1", t0());
            ck(
                &mut e,
                json!([{"Id":"ci","Type":"CHAINED_INVOKE","Action":"START","Name":"call-target","SubType":"ChainedInvoke","Payload":"{\"hello\":\"world\"}","ChainedInvokeOptions":{"FunctionName":"target"}}]),
                t0(),
            );
            chained_dispatched(&mut e, "ci", Some("$LATEST"), None);
            // The target returns before the parent's invocation has ended.
            assert!(complete_chained(
                &mut e,
                "ci",
                ChainedOutcome {
                    status,
                    result: (status == OpStatus::Succeeded).then(|| "{\"echo\":{}}".to_string()),
                    error: error.clone(),
                },
                t0()
            ));
            assert_eq!(e.op("ci").unwrap().status, OpStatus::Started);
            let again = complete_invocation(
                &mut e,
                "r1",
                InvocationOutcome::Output(json!({"Status":"PENDING"})),
                t0(),
            );
            assert!(again, "the returned call needs a replay");
            assert_eq!(e.op("ci").unwrap().status, status);
            begin_invocation(&mut e, "r2", t0());
            complete_invocation(
                &mut e,
                "r2",
                InvocationOutcome::Output(json!({"Status":"SUCCEEDED","Result":"{}"})),
                t0(),
            );
            assert_eq!(event_types(&e), recorded_event_types(label), "{label}");
        }
    }

    /// A target that returns after the parent suspended is recorded at once.
    #[test]
    fn chained_invoke_completing_after_the_invocation_is_recorded_at_once() {
        let mut e = new_exec(600);
        begin_invocation(&mut e, "r1", t0());
        ck(
            &mut e,
            json!([{"Id":"ci","Type":"CHAINED_INVOKE","Action":"START","ChainedInvokeOptions":{"FunctionName":"target"}}]),
            t0(),
        );
        chained_dispatched(&mut e, "ci", Some("$LATEST"), None);
        assert!(!complete_invocation(
            &mut e,
            "r1",
            InvocationOutcome::Output(json!({"Status":"PENDING"})),
            t0()
        ));
        assert!(complete_chained(
            &mut e,
            "ci",
            ChainedOutcome {
                status: OpStatus::Succeeded,
                result: Some("1".into()),
                error: None
            },
            t0()
        ));
        assert_eq!(
            e.events.last().unwrap()["EventType"],
            "ChainedInvokeSucceeded"
        );
        assert!(e.wake);
    }

    #[test]
    fn a_change_delivered_mid_invocation_still_reinvokes_on_pending() {
        let mut e = new_exec(600);
        begin_invocation(&mut e, "r1", t0());
        let r = ck(
            &mut e,
            json!([{"Id":"cb","Type":"CALLBACK","Action":"START"}]),
            t0(),
        );
        let id = r["NewExecutionState"]["Operations"][0]["CallbackDetails"]["CallbackId"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(complete_callback(
            &mut e,
            &id,
            CallbackOutcome::Succeeded(None),
            t0()
        ));
        // The SDK's next checkpoint hands the completed callback back...
        let r = ck(&mut e, json!([]), t0());
        assert_eq!(
            r["NewExecutionState"]["Operations"][0]["Status"],
            "SUCCEEDED"
        );
        // ...but if the branch had already suspended, PENDING must not strand it.
        assert!(complete_invocation(
            &mut e,
            "r1",
            InvocationOutcome::Output(json!({"Status":"PENDING"})),
            t0()
        ));
    }

    #[test]
    fn huge_markers_do_not_overflow() {
        let items: Vec<(usize, u32)> = (1..=3).map(|i| (i as usize, i)).collect();
        let marker = encode_marker(&usize::MAX);
        let (p, next) = page_by_key(items, false, Some(&marker), 2).unwrap();
        assert!(p.is_empty() && next.is_none());
        let mut e = new_exec(600);
        begin_invocation(&mut e, "r1", t0());
        let t = token(&e);
        let page = state_page(&e, &t, Some(&marker), 5).unwrap();
        assert_eq!(page["Operations"], json!([]));
    }

    #[test]
    fn pages_walk_forward_and_backward_by_key() {
        let items = || (1..=5u32).map(|i| (i, i)).collect::<Vec<_>>();
        let (p, m) = page_by_key(items(), false, None, 2).unwrap();
        assert_eq!(p, [1, 2]);
        let (p, m) = page_by_key(items(), false, m.as_deref(), 2).unwrap();
        assert_eq!(p, [3, 4]);
        let (p, m) = page_by_key(items(), false, m.as_deref(), 2).unwrap();
        assert_eq!((p, m), (vec![5], None));
        let (p, m) = page_by_key(items(), true, None, 2).unwrap();
        assert_eq!(p, [5, 4]);
        // An item removed and one added before the marker's key keep the
        // next page in place.
        let mut changed = items();
        changed.retain(|(k, _)| *k != 4);
        changed.push((0, 0));
        let (p, _) = page_by_key(changed, true, m.as_deref(), 2).unwrap();
        assert_eq!(p, [3, 2]);
    }
}
