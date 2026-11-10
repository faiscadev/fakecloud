//! Durable execution state and its wire encodings.
//!
//! One [`DurableExecution`] is kept per execution in the Lambda state (and so
//! in the Lambda snapshot). It owns the operation log the durable SDK replays
//! from, the history events `GetDurableExecutionHistory` reports, and the
//! bookkeeping the scheduler needs (the in-flight invocation, its checkpoint
//! token, invocation retry backoff).
//!
//! Timestamps travel in two encodings, both recorded from AWS: epoch
//! milliseconds (integers) inside the invocation payload handed to the
//! function, and epoch seconds (fractional numbers) in every API response.

use base64::Engine as _;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

/// `ExecutionStatus` of a durable execution.
pub(crate) const RUNNING: &str = "RUNNING";
pub(crate) const SUCCEEDED: &str = "SUCCEEDED";
pub(crate) const FAILED: &str = "FAILED";
pub(crate) const TIMED_OUT: &str = "TIMED_OUT";
pub(crate) const STOPPED: &str = "STOPPED";

/// `RetentionPeriodInDays` applied when the function's `DurableConfig`
/// leaves it out (CreateFunction reports 14).
pub(crate) const DEFAULT_RETENTION_DAYS: i64 = 14;

/// `OperationType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OpType {
    Execution,
    Context,
    Step,
    Wait,
    Callback,
    ChainedInvoke,
}

impl OpType {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            OpType::Execution => "EXECUTION",
            OpType::Context => "CONTEXT",
            OpType::Step => "STEP",
            OpType::Wait => "WAIT",
            OpType::Callback => "CALLBACK",
            OpType::ChainedInvoke => "CHAINED_INVOKE",
        }
    }

    pub(crate) fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "EXECUTION" => OpType::Execution,
            "CONTEXT" => OpType::Context,
            "STEP" => OpType::Step,
            "WAIT" => OpType::Wait,
            "CALLBACK" => OpType::Callback,
            "CHAINED_INVOKE" => OpType::ChainedInvoke,
            _ => return None,
        })
    }
}

/// `OperationStatus`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OpStatus {
    Started,
    Pending,
    Ready,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
    Stopped,
}

impl OpStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            OpStatus::Started => "STARTED",
            OpStatus::Pending => "PENDING",
            OpStatus::Ready => "READY",
            OpStatus::Succeeded => "SUCCEEDED",
            OpStatus::Failed => "FAILED",
            OpStatus::Cancelled => "CANCELLED",
            OpStatus::TimedOut => "TIMED_OUT",
            OpStatus::Stopped => "STOPPED",
        }
    }

    pub(crate) fn is_terminal(self) -> bool {
        matches!(
            self,
            OpStatus::Succeeded
                | OpStatus::Failed
                | OpStatus::Cancelled
                | OpStatus::TimedOut
                | OpStatus::Stopped
        )
    }
}

/// A callback operation's id and timers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CallbackState {
    pub id: String,
    /// `CallbackOptions.TimeoutSeconds`; 0 is no timeout.
    #[serde(default)]
    pub timeout_seconds: i64,
    /// `CallbackOptions.HeartbeatTimeoutSeconds`; 0 is no heartbeat timeout.
    #[serde(default)]
    pub heartbeat_timeout_seconds: i64,
    #[serde(default)]
    pub deadline: Option<DateTime<Utc>>,
    #[serde(default)]
    pub heartbeat_deadline: Option<DateTime<Utc>>,
}

/// What a chained-invoke operation calls and how far that call has gone.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChainedInvokeState {
    /// `ChainedInvokeOptions.FunctionName` as the SDK sent it.
    pub function_name: String,
    #[serde(default)]
    pub tenant_id: Option<String>,
    /// The input handed to the invoked function.
    #[serde(default)]
    pub payload: Option<String>,
    /// Set once the service has dispatched the call, so a restart or a
    /// second scheduler pass never invokes it twice.
    #[serde(default)]
    pub dispatched: bool,
    /// The child execution, when the invoked function is itself durable.
    #[serde(default)]
    pub child_execution_arn: Option<String>,
    #[serde(default)]
    pub executed_version: Option<String>,
}

/// One entry of the operation log.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Operation {
    pub id: String,
    #[serde(default)]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    pub op_type: OpType,
    #[serde(default)]
    pub sub_type: Option<String>,
    pub start: DateTime<Utc>,
    #[serde(default)]
    pub end: Option<DateTime<Utc>>,
    pub status: OpStatus,
    /// `ExecutionDetails.InputPayload` (EXECUTION only).
    #[serde(default)]
    pub input_payload: Option<String>,
    #[serde(default)]
    pub result: Option<String>,
    #[serde(default)]
    pub error: Option<Value>,
    /// `ContextDetails.ReplayChildren`, recorded from the closing update.
    #[serde(default)]
    pub replay_children: Option<bool>,
    /// `StepDetails.Attempt`.
    #[serde(default)]
    pub attempt: i64,
    #[serde(default)]
    pub next_attempt: Option<DateTime<Utc>>,
    #[serde(default)]
    pub wait_seconds: Option<i64>,
    #[serde(default)]
    pub scheduled_end: Option<DateTime<Utc>>,
    #[serde(default)]
    pub callback: Option<CallbackState>,
    #[serde(default)]
    pub chained: Option<ChainedInvokeState>,
}

/// How a timestamp is rendered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TimeFormat {
    /// Epoch milliseconds, as in the invocation payload.
    Millis,
    /// Epoch seconds with a fractional part, as in API responses.
    Seconds,
}

pub(crate) fn ts(dt: DateTime<Utc>, format: TimeFormat) -> Value {
    match format {
        TimeFormat::Millis => json!(dt.timestamp_millis()),
        TimeFormat::Seconds => json!(dt.timestamp_millis() as f64 / 1000.0),
    }
}

/// The current time, truncated to the millisecond every timestamp the
/// service reports carries.
pub(crate) fn now() -> DateTime<Utc> {
    truncate_ms(Utc::now())
}

pub(crate) fn truncate_ms(dt: DateTime<Utc>) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(dt.timestamp_millis()).unwrap_or(dt)
}

impl Operation {
    pub(crate) fn new(
        id: &str,
        op_type: OpType,
        status: OpStatus,
        start: DateTime<Utc>,
    ) -> Operation {
        Operation {
            id: id.to_string(),
            parent_id: None,
            name: None,
            op_type,
            sub_type: None,
            start,
            end: None,
            status,
            input_payload: None,
            result: None,
            error: None,
            replay_children: None,
            attempt: 0,
            next_attempt: None,
            wait_seconds: None,
            scheduled_end: None,
            callback: None,
            chained: None,
        }
    }

    /// The `Operation` shape, with timestamps in `format`.
    pub(crate) fn to_wire(&self, format: TimeFormat) -> Value {
        let mut m = Map::new();
        m.insert("Id".into(), json!(self.id));
        if let Some(p) = &self.parent_id {
            m.insert("ParentId".into(), json!(p));
        }
        if let Some(n) = &self.name {
            m.insert("Name".into(), json!(n));
        }
        m.insert("Type".into(), json!(self.op_type.as_str()));
        if let Some(s) = &self.sub_type {
            m.insert("SubType".into(), json!(s));
        }
        m.insert("StartTimestamp".into(), ts(self.start, format));
        if let Some(e) = self.end {
            m.insert("EndTimestamp".into(), ts(e, format));
        }
        m.insert("Status".into(), json!(self.status.as_str()));
        match self.op_type {
            OpType::Execution => {
                m.insert(
                    "ExecutionDetails".into(),
                    json!({ "InputPayload": self.input_payload.clone().unwrap_or_default() }),
                );
            }
            OpType::Context => {
                let mut d = Map::new();
                if let Some(rc) = self.replay_children {
                    d.insert("ReplayChildren".into(), json!(rc));
                }
                self.insert_outcome(&mut d);
                m.insert("ContextDetails".into(), Value::Object(d));
            }
            OpType::Step => {
                let mut d = Map::new();
                d.insert("Attempt".into(), json!(self.attempt));
                if matches!(self.status, OpStatus::Pending | OpStatus::Ready) {
                    if let Some(n) = self.next_attempt {
                        d.insert("NextAttemptTimestamp".into(), ts(n, format));
                    }
                }
                self.insert_outcome(&mut d);
                m.insert("StepDetails".into(), Value::Object(d));
            }
            OpType::Wait => {
                let mut d = Map::new();
                if let Some(e) = self.scheduled_end {
                    d.insert("ScheduledEndTimestamp".into(), ts(e, format));
                }
                m.insert("WaitDetails".into(), Value::Object(d));
            }
            OpType::Callback => {
                let mut d = Map::new();
                if let Some(cb) = &self.callback {
                    d.insert("CallbackId".into(), json!(cb.id));
                }
                self.insert_outcome(&mut d);
                m.insert("CallbackDetails".into(), Value::Object(d));
            }
            OpType::ChainedInvoke => {
                let mut d = Map::new();
                self.insert_outcome(&mut d);
                m.insert("ChainedInvokeDetails".into(), Value::Object(d));
            }
        }
        Value::Object(m)
    }

    /// `Result` / `Error` of a finished operation. A step that is waiting
    /// for its next attempt reports neither.
    fn insert_outcome(&self, d: &mut Map<String, Value>) {
        if !self.status.is_terminal() {
            return;
        }
        if let Some(r) = &self.result {
            d.insert("Result".into(), json!(r));
        }
        if let Some(e) = &self.error {
            d.insert("Error".into(), e.clone());
        }
    }
}

/// The invocation the service is running for an execution right now.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActiveInvocation {
    pub request_id: String,
    pub start: DateTime<Utc>,
    /// The only checkpoint token currently accepted. `None` once the
    /// execution has finished through a checkpoint.
    #[serde(default)]
    pub token: Option<String>,
    /// The token the last successful checkpoint consumed, and that
    /// checkpoint's `ClientToken` and response: a retried request carrying
    /// both gets the same answer instead of `Invalid checkpoint token`.
    #[serde(default)]
    pub previous_token: Option<String>,
    #[serde(default)]
    pub previous_client_token: Option<String>,
    #[serde(default)]
    pub previous_response: Option<Value>,
}

/// Backoff state for re-running an invocation that failed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InvocationRetry {
    /// Failed invocations so far in this streak.
    pub failures: u32,
    /// When the next attempt runs.
    pub at: DateTime<Utc>,
    /// Which backoff schedule applies.
    pub kind: RetryKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetryKind {
    /// The function itself failed (threw, crashed, timed out).
    FunctionError,
    /// The function answered PENDING with nothing pending.
    PendingWithoutWork,
}

/// The parent operation a chained-invoke child execution reports to.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParentLink {
    pub execution_arn: String,
    pub operation_id: String,
}

/// A durable execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurableExecution {
    pub arn: String,
    /// The EXECUTION operation id (the execution's UUID).
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub name: String,
    pub function_name: String,
    /// The function ARN qualified with the version the execution is
    /// pinned to.
    pub function_arn: String,
    #[serde(default)]
    pub version: String,
    /// `ExecutionStatus`.
    pub status: String,
    #[serde(default)]
    pub input_payload: String,
    #[serde(default)]
    pub result: Option<String>,
    #[serde(default)]
    pub error: Option<Value>,
    #[serde(alias = "started_at")]
    pub start_timestamp: DateTime<Utc>,
    #[serde(default, alias = "stopped_at")]
    pub end_timestamp: Option<DateTime<Utc>>,
    #[serde(default)]
    pub execution_timeout: i64,
    #[serde(default)]
    pub retention_days: i64,
    #[serde(default)]
    pub kms_key_arn: Option<String>,
    #[serde(default)]
    pub trace_id: String,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub operations: Vec<Operation>,
    /// History events, as reported with `IncludeExecutionData=true`.
    #[serde(default, alias = "history")]
    pub events: Vec<Value>,
    #[serde(default)]
    pub invocation: Option<ActiveInvocation>,
    /// Operations changed since the running invocation last heard about
    /// them (through a checkpoint response or the invocation payload).
    #[serde(default)]
    pub dirty: Vec<String>,
    /// `UpdatedOperationIds` of the last invocation, reused when that
    /// invocation is retried.
    #[serde(default)]
    pub last_updated_ids: Vec<String>,
    /// Set when the service changed an operation and the function has to
    /// run again to observe it.
    #[serde(default)]
    pub wake: bool,
    #[serde(default)]
    pub retry: Option<InvocationRetry>,
    #[serde(default)]
    pub parent: Option<ParentLink>,
}

impl DurableExecution {
    pub(crate) fn is_running(&self) -> bool {
        self.status == RUNNING
    }

    pub(crate) fn op(&self, id: &str) -> Option<&Operation> {
        self.operations.iter().find(|o| o.id == id)
    }

    pub(crate) fn op_mut(&mut self, id: &str) -> Option<&mut Operation> {
        self.operations.iter_mut().find(|o| o.id == id)
    }

    pub(crate) fn mark_dirty(&mut self, id: &str) {
        if !self.dirty.iter().any(|d| d == id) {
            self.dirty.push(id.to_string());
        }
    }

    /// When the execution times out.
    pub(crate) fn deadline(&self) -> DateTime<Utc> {
        self.start_timestamp + chrono::Duration::seconds(self.execution_timeout.max(1))
    }

    /// `DurableConfig` as the execution reports it.
    pub(crate) fn durable_config_json(&self) -> Value {
        let mut m = Map::new();
        if let Some(k) = &self.kms_key_arn {
            m.insert("KMSKeyArn".into(), json!(k));
        }
        m.insert("ExecutionTimeout".into(), json!(self.execution_timeout));
        m.insert("RetentionPeriodInDays".into(), json!(self.retention_days));
        Value::Object(m)
    }

    /// The `Execution` list entry.
    pub(crate) fn summary_json(&self) -> Value {
        let mut m = Map::new();
        m.insert("DurableExecutionArn".into(), json!(self.arn));
        m.insert("DurableExecutionName".into(), json!(self.name));
        m.insert("FunctionArn".into(), json!(self.function_arn));
        m.insert("Status".into(), json!(self.status));
        m.insert(
            "StartTimestamp".into(),
            ts(self.start_timestamp, TimeFormat::Seconds),
        );
        if let Some(e) = self.end_timestamp {
            m.insert("EndTimestamp".into(), ts(e, TimeFormat::Seconds));
        }
        if let Some(k) = &self.kms_key_arn {
            m.insert("KMSKeyArn".into(), json!(k));
        }
        Value::Object(m)
    }

    /// The `GetDurableExecution` response.
    pub(crate) fn detail_json(&self, include_data: bool) -> Value {
        let mut m = Map::new();
        m.insert("DurableExecutionArn".into(), json!(self.arn));
        m.insert("DurableExecutionName".into(), json!(self.name));
        m.insert("FunctionArn".into(), json!(self.function_arn));
        m.insert("Status".into(), json!(self.status));
        m.insert(
            "StartTimestamp".into(),
            ts(self.start_timestamp, TimeFormat::Seconds),
        );
        if let Some(e) = self.end_timestamp {
            m.insert("EndTimestamp".into(), ts(e, TimeFormat::Seconds));
        }
        if self.is_running() {
            m.insert(
                "ExpectedExecutionTimeoutTimestamp".into(),
                ts(self.deadline(), TimeFormat::Seconds),
            );
        }
        m.insert("Version".into(), json!(self.version));
        m.insert(
            "TraceHeader".into(),
            json!({ "XAmznTraceId": self.trace_id }),
        );
        m.insert("DurableConfig".into(), self.durable_config_json());
        m.insert("ExecutionDataIncluded".into(), json!(include_data));
        if include_data {
            m.insert("InputPayload".into(), json!(self.input_payload));
            if let Some(r) = &self.result {
                m.insert("Result".into(), json!(r));
            }
            if let Some(e) = &self.error {
                m.insert("Error".into(), e.clone());
            }
        }
        Value::Object(m)
    }
}

/// A function's `DurableConfig` as Lambda stores and reports it: the
/// retention period defaults to 14 days.
pub(crate) fn normalize_durable_config(cfg: &Value) -> Value {
    let mut m = cfg.as_object().cloned().unwrap_or_default();
    m.retain(|_, v| !v.is_null());
    m.entry("RetentionPeriodInDays")
        .or_insert_with(|| json!(DEFAULT_RETENTION_DAYS));
    Value::Object(m)
}

/// `{"Truncated": false, "Payload": p}`, the payload wrapper of history
/// event details. An absent payload leaves only the flag.
pub(crate) fn payload_wrapper(payload: Option<&str>) -> Value {
    let mut m = Map::new();
    m.insert("Truncated".into(), json!(false));
    if let Some(p) = payload {
        m.insert("Payload".into(), json!(p));
    }
    Value::Object(m)
}

/// `{"Truncated": false, "Payload": error}`.
pub(crate) fn error_wrapper(error: &Value) -> Value {
    json!({ "Truncated": false, "Payload": error })
}

/// A history event with its payload wrappers emptied, as reported without
/// `IncludeExecutionData`: every `{Truncated, Payload}` wrapper becomes
/// `{"Truncated": true}`.
pub(crate) fn strip_event_data(event: &Value) -> Value {
    fn strip(v: &Value) -> Value {
        match v {
            Value::Object(m) => {
                if m.contains_key("Truncated") {
                    return json!({ "Truncated": true });
                }
                Value::Object(m.iter().map(|(k, v)| (k.clone(), strip(v))).collect())
            }
            other => other.clone(),
        }
    }
    strip(event)
}

/// `{function_arn}:{version}/durable-execution/{name}/{id}`.
pub(crate) fn execution_arn(
    unqualified_function_arn: &str,
    version: &str,
    name: &str,
    id: &str,
) -> String {
    format!("{unqualified_function_arn}:{version}/durable-execution/{name}/{id}")
}

/// The parts of a durable execution ARN: (function ARN without qualifier,
/// version, name, id).
pub(crate) fn parse_execution_arn(arn: &str) -> Option<(&str, &str, &str, &str)> {
    let (function_part, rest) = arn.split_once("/durable-execution/")?;
    let (name, id) = rest.split_once('/')?;
    let (function_arn, version) = function_part.rsplit_once(':')?;
    if name.is_empty() || id.is_empty() || id.contains('/') || !function_arn.contains(":function:")
    {
        return None;
    }
    Some((function_arn, version, name, id))
}

/// A fresh execution id. AWS mints version-3 (name-based) UUIDs; the bytes
/// here are random with the version nibble set to match.
pub(crate) fn new_execution_id() -> String {
    let mut bytes = *uuid::Uuid::new_v4().as_bytes();
    bytes[6] = (bytes[6] & 0x0f) | 0x30;
    uuid::Uuid::from_bytes(bytes).to_string()
}

/// An X-Ray trace header for a new execution.
pub(crate) fn new_trace_header(start: DateTime<Utc>) -> String {
    let a = uuid::Uuid::new_v4().simple().to_string();
    let b = uuid::Uuid::new_v4().simple().to_string();
    format!(
        "Root=1-{:08x}-{};Parent={};Sampled=0",
        start.timestamp(),
        &a[..24],
        &b[..16]
    )
}

/// An opaque checkpoint token (`^[A-Za-z0-9+/]+={0,2}$`).
pub(crate) fn new_checkpoint_token() -> String {
    let mut bytes = Vec::with_capacity(96);
    bytes.extend_from_slice(&[0x02, 0x01]);
    while bytes.len() < 96 {
        bytes.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
    }
    bytes.truncate(96);
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

// --- callback ids --------------------------------------------------------
//
// AWS callback ids are an unpadded base64 version byte (1) followed by a
// CBOR indefinite-length map of three text entries: `e` the execution ARN,
// `i` a UUID, `r` `<version>:g-<UUID>`. The service resolves a callback by
// decoding the execution from it, so an id naming no live execution answers
// `CallbackTimeoutException`, and one that does not decode answers
// `Invalid callback id`.

fn cbor_text(out: &mut Vec<u8>, s: &str) {
    let len = s.len();
    if len < 24 {
        out.push(0x60 + len as u8);
    } else if len < 256 {
        out.push(0x78);
        out.push(len as u8);
    } else {
        out.push(0x79);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    }
    out.extend_from_slice(s.as_bytes());
}

/// Mint a callback id for an operation of `execution_arn`.
pub(crate) fn new_callback_id(execution_arn: &str, version: &str) -> String {
    let mut out = vec![0x01, 0xbf];
    cbor_text(&mut out, "e");
    cbor_text(&mut out, execution_arn);
    cbor_text(&mut out, "i");
    cbor_text(&mut out, &uuid::Uuid::new_v4().to_string());
    cbor_text(&mut out, "r");
    cbor_text(&mut out, &format!("{version}:g-{}", uuid::Uuid::new_v4()));
    out.push(0xff);
    base64::engine::general_purpose::STANDARD_NO_PAD.encode(out)
}

/// The execution ARN a callback id names, or `None` when it does not
/// decode as one.
pub(crate) fn callback_execution_arn(callback_id: &str) -> Option<String> {
    let trimmed = callback_id.trim_end_matches('=');
    let bytes = base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(trimmed)
        .ok()?;
    if bytes.len() < 3 || bytes[0] != 0x01 || bytes[1] != 0xbf {
        return None;
    }
    let mut pos = 2;
    let read_text = |pos: &mut usize| -> Option<String> {
        let head = *bytes.get(*pos)?;
        *pos += 1;
        let len = match head {
            0x60..=0x77 => (head - 0x60) as usize,
            0x78 => {
                let l = *bytes.get(*pos)? as usize;
                *pos += 1;
                l
            }
            0x79 => {
                let l = u16::from_be_bytes([*bytes.get(*pos)?, *bytes.get(*pos + 1)?]) as usize;
                *pos += 2;
                l
            }
            _ => return None,
        };
        let s = std::str::from_utf8(bytes.get(*pos..*pos + len)?).ok()?;
        *pos += len;
        Some(s.to_string())
    };
    let mut arn = None;
    loop {
        if *bytes.get(pos)? == 0xff {
            break;
        }
        let key = read_text(&mut pos)?;
        let value = read_text(&mut pos)?;
        if key == "e" {
            arn = Some(value);
        }
    }
    arn.filter(|a| parse_execution_arn(a).is_some())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_config_defaults_the_retention_period() {
        assert_eq!(
            normalize_durable_config(&json!({"ExecutionTimeout": 15})),
            json!({"ExecutionTimeout": 15, "RetentionPeriodInDays": 14})
        );
        assert_eq!(
            normalize_durable_config(
                &json!({"ExecutionTimeout": 600, "RetentionPeriodInDays": 1, "KMSKeyArn": null})
            ),
            json!({"ExecutionTimeout": 600, "RetentionPeriodInDays": 1})
        );
    }

    #[test]
    fn callback_id_round_trips_and_matches_the_aws_layout() {
        let arn = "arn:aws:lambda:us-east-1:123456789012:function:f:$LATEST/durable-execution/n/0b9a1f2e-1111-3222-8333-444455556666";
        let id = new_callback_id(arn, "$LATEST");
        // Unpadded standard base64 with the version-1 CBOR map prefix AWS
        // ids start with (`Ab9h...`).
        assert!(id.starts_with("Ab9h"), "{id}");
        assert!(!id.ends_with('='));
        assert_eq!(callback_execution_arn(&id).as_deref(), Some(arn));
    }

    #[test]
    fn callback_id_decodes_a_recorded_aws_id() {
        let aws = "Ab9hZXiCYXJuOmF3czpsYW1iZGE6dXMtZWFzdC0xOjQ1NDU5MTU0ODQ1NDpmdW5jdGlvbjpmYy1kdXJhYmxlLXJlYzokTEFURVNUL2R1cmFibGUtZXhlY3V0aW9uL2NiLW9rL2QyZjE4Y2E1LTdjYjgtM2U4MC1iOGE1LTZkNjE4YTg0ODBhMmFpeCQyM2QxZDMzOC1jNjIyLTQ1MmItYTcyZS04OWIzOTRmNzlhMjNhcnguJExBVEVTVDpnLWI5ZjcyNTUzLWNkNmEtNGJiYy05ZDZlLTJlNmE3MDdkM2U2Yv8";
        assert_eq!(
            callback_execution_arn(aws).as_deref(),
            Some("arn:aws:lambda:us-east-1:454591548454:function:fc-durable-rec:$LATEST/durable-execution/cb-ok/d2f18ca5-7cb8-3e80-b8a5-6d618a8480a2")
        );
    }

    #[test]
    fn junk_callback_ids_do_not_decode() {
        for bad in ["bm9wZQ==", "AAAA", "", "Ab9h"] {
            assert_eq!(callback_execution_arn(bad), None, "{bad}");
        }
    }

    #[test]
    fn execution_arn_parses_back() {
        let arn = execution_arn(
            "arn:aws:lambda:us-east-1:123456789012:function:f",
            "$LATEST",
            "n",
            "id-1",
        );
        assert_eq!(
            parse_execution_arn(&arn),
            Some((
                "arn:aws:lambda:us-east-1:123456789012:function:f",
                "$LATEST",
                "n",
                "id-1"
            ))
        );
        assert_eq!(
            parse_execution_arn("arn:aws:lambda:us-east-1:1:durable-execution/x"),
            None
        );
    }

    #[test]
    fn timestamps_render_as_millis_and_seconds() {
        let t = DateTime::from_timestamp_millis(1_794_322_690_115).unwrap();
        assert_eq!(ts(t, TimeFormat::Millis), json!(1_794_322_690_115i64));
        assert_eq!(ts(t, TimeFormat::Seconds), json!(1_794_322_690.115));
    }

    #[test]
    fn checkpoint_tokens_match_the_modeled_pattern() {
        let t = new_checkpoint_token();
        assert!(t
            .trim_end_matches('=')
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'+' || b == b'/'));
        assert!(t.len() <= 2048);
        assert_ne!(t, new_checkpoint_token());
    }

    #[test]
    fn stripped_events_keep_only_truncation_flags() {
        let e = json!({"EventType":"StepSucceeded","StepSucceededDetails":{"Result":{"Truncated":false,"Payload":"42"},"RetryDetails":{"CurrentAttempt":1}}});
        assert_eq!(
            strip_event_data(&e),
            json!({"EventType":"StepSucceeded","StepSucceededDetails":{"Result":{"Truncated":true},"RetryDetails":{"CurrentAttempt":1}}})
        );
    }
}
